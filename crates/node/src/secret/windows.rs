//! Key files on Windows (`crate::secret`). A key file is created by CreateFileW with an absolute
//! security descriptor whose DACL grants full access to the process token's default owner, who
//! owns the file the process creates, and to LocalSystem, and no one else, protected so it inherits
//! nothing from its directory (`SE_DACL_PROTECTED`; SECURITY_INFORMATION's
//! PROTECTED_DACL_SECURITY_INFORMATION, "The DACL cannot inherit access control entries (ACEs)",
//! Microsoft Learn). A key file is read only once its DACL, read from the open file's handle with
//! GetKernelObjectSecurity ("the calling process must have been granted READ_CONTROL access when
//! the handle was opened", Microsoft Learn; GENERIC_READ includes it), lets no one but its owner and
//! LocalSystem have anything: each entry that allows anything allows the file's owner or
//! LocalSystem, as hyper-seal's check of a key file reads it (vendor/hyper-seal `file_windows.rs`).
//! No DACL, which allows everyone everything, or an entry of a kind this does not read, is not.
#![allow(unsafe_code)]

use std::fs::File;
use std::os::windows::ffi::OsStrExt as _;
use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _, OwnedHandle};
use std::path::Path;
use std::ptr::null_mut;

use windows_sys::Win32::Foundation::{GENERIC_WRITE, HANDLE, INVALID_HANDLE_VALUE};
use windows_sys::Win32::Security::{
    ACCESS_ALLOWED_ACE, ACE_HEADER, ACL, ACL_REVISION, ACL_SIZE_INFORMATION, AclSizeInformation,
    AddAccessAllowedAce, CreateWellKnownSid, DACL_SECURITY_INFORMATION, EqualSid, GetAce,
    GetAclInformation, GetKernelObjectSecurity, GetLengthSid, GetSecurityDescriptorDacl,
    GetSecurityDescriptorOwner, GetTokenInformation, InitializeAcl, InitializeSecurityDescriptor,
    IsWellKnownSid, OWNER_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID, SE_DACL_PROTECTED,
    SECURITY_ATTRIBUTES, SECURITY_DESCRIPTOR, SECURITY_MAX_SID_SIZE, SetSecurityDescriptorControl,
    SetSecurityDescriptorDacl, TOKEN_OWNER, TOKEN_QUERY, TokenOwner, WinLocalSystemSid,
};
use windows_sys::Win32::Storage::FileSystem::{
    CREATE_NEW, CreateFileW, FILE_ALL_ACCESS, FILE_ATTRIBUTE_NORMAL,
};
use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

/// `SECURITY_DESCRIPTOR_REVISION` (winnt.h).
const SECURITY_DESCRIPTOR_REVISION: u32 = 1;
/// `ACCESS_ALLOWED_ACE_TYPE` and `ACCESS_DENIED_ACE_TYPE` (winnt.h).
const ACCESS_ALLOWED_ACE_TYPE: u8 = 0;
const ACCESS_DENIED_ACE_TYPE: u8 = 1;

fn failed() -> std::io::Error {
    std::io::Error::last_os_error()
}

fn malformed(what: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, what.to_owned())
}

/// A buffer of at least `bytes` bytes aligned for the SIDs, ACLs and descriptors it holds, which
/// the system reads in words.
fn aligned(bytes: u32) -> Result<Vec<u64>, std::io::Error> {
    let words = usize::try_from(bytes)
        .map_err(|_| malformed("a security buffer past this machine's address space"))?
        .div_ceil(8);
    Ok(vec![0u64; words])
}

/// The process token's default owner, the owner of every file the process creates, as a SID in a
/// buffer of its own.
fn token_owner() -> Result<Vec<u64>, std::io::Error> {
    let mut token: HANDLE = null_mut();
    // SAFETY: GetCurrentProcess returns a pseudo-handle that needs no closing; OpenProcessToken
    // writes the token's handle to `token` and returns nonzero on success.
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
        return Err(failed());
    }
    // SAFETY: `token` is the handle OpenProcessToken opened for this process, owned here alone and
    // closed once as `OwnedHandle` drops.
    let token = unsafe { OwnedHandle::from_raw_handle(token.cast()) };
    let mut needed = 0u32;
    // SAFETY: a null buffer of no length asks only for the length the information needs, which the
    // call writes to `needed`; it fails with ERROR_INSUFFICIENT_BUFFER, which the next call answers.
    unsafe {
        GetTokenInformation(
            token.as_raw_handle().cast(),
            TokenOwner,
            null_mut(),
            0,
            &mut needed,
        )
    };
    let mut information = aligned(needed)?;
    // SAFETY: `information` holds at least `needed` bytes, aligned for TOKEN_OWNER and the SID the
    // call writes after it.
    if unsafe {
        GetTokenInformation(
            token.as_raw_handle().cast(),
            TokenOwner,
            information.as_mut_ptr().cast(),
            needed,
            &mut needed,
        )
    } == 0
    {
        return Err(failed());
    }
    // SAFETY: the call wrote a TOKEN_OWNER at the buffer's start, aligned as `u64`s are, whose SID
    // pointer points into the same buffer, which lives until the SID is copied out below.
    let owner: PSID = unsafe { (*information.as_ptr().cast::<TOKEN_OWNER>()).Owner };
    copy_sid(owner)
}

/// The SID `sid` points to, copied into a buffer of its own.
fn copy_sid(sid: PSID) -> Result<Vec<u64>, std::io::Error> {
    // SAFETY: `sid` points to a valid SID, whose length GetLengthSid reads from its header.
    let length = unsafe { GetLengthSid(sid) };
    let mut copy = aligned(length)?;
    let bytes = usize::try_from(length).map_err(|_| malformed("a SID's length"))?;
    // SAFETY: `sid` holds `bytes` bytes and `copy` at least as many, and the two do not overlap.
    unsafe { std::ptr::copy_nonoverlapping(sid.cast::<u8>(), copy.as_mut_ptr().cast(), bytes) };
    Ok(copy)
}

/// LocalSystem's SID.
fn local_system() -> Result<Vec<u64>, std::io::Error> {
    let mut size = SECURITY_MAX_SID_SIZE;
    let mut sid = aligned(size)?;
    // SAFETY: `sid` holds `size` bytes, the most any SID takes, and CreateWellKnownSid writes one
    // there and its length to `size`; a well-known SID needs no domain.
    if unsafe {
        CreateWellKnownSid(
            WinLocalSystemSid,
            null_mut(),
            sid.as_mut_ptr().cast(),
            &mut size,
        )
    } == 0
    {
        return Err(failed());
    }
    Ok(sid)
}

/// A new file at `path`, created with a protected DACL granting its owner and LocalSystem full
/// access and no one else: refused where a file is there already.
pub(super) fn create(path: &Path) -> Result<File, std::io::Error> {
    let owner = token_owner()?;
    let system = local_system()?;
    let sid_bytes = |sid: &Vec<u64>| -> u32 {
        // SAFETY: each buffer holds a valid SID, written by the system above.
        unsafe { GetLengthSid(sid.as_ptr().cast_mut().cast()) }
    };
    // An entry's bytes beside its SID, whose first word the structure counts in `SidStart`.
    let ace = std::mem::size_of::<ACCESS_ALLOWED_ACE>()
        .checked_sub(std::mem::size_of::<u32>())
        .and_then(|bytes| u32::try_from(bytes).ok())
        .ok_or_else(|| malformed("an ACE's size"))?;
    let acl_bytes = u32::try_from(std::mem::size_of::<ACL>())
        .ok()
        .and_then(|header| header.checked_add(ace.checked_mul(2)?))
        .and_then(|bytes| {
            bytes
                .checked_add(sid_bytes(&owner))?
                .checked_add(sid_bytes(&system))
        })
        .ok_or_else(|| malformed("a key file's ACL"))?;
    let mut acl = aligned(acl_bytes)?;
    let acl_ptr: *mut ACL = acl.as_mut_ptr().cast();
    // SAFETY: `acl` holds `acl_bytes` bytes, word-aligned, which InitializeAcl makes an empty ACL of
    // and each AddAccessAllowedAce appends one entry to, within the length counted for both.
    let built = unsafe {
        InitializeAcl(acl_ptr, acl_bytes, ACL_REVISION) != 0
            && AddAccessAllowedAce(
                acl_ptr,
                ACL_REVISION,
                FILE_ALL_ACCESS,
                owner.as_ptr().cast_mut().cast(),
            ) != 0
            && AddAccessAllowedAce(
                acl_ptr,
                ACL_REVISION,
                FILE_ALL_ACCESS,
                system.as_ptr().cast_mut().cast(),
            ) != 0
    };
    if !built {
        return Err(failed());
    }
    let mut descriptor = SECURITY_DESCRIPTOR::default();
    let descriptor_ptr: PSECURITY_DESCRIPTOR = (&raw mut descriptor).cast();
    // SAFETY: `descriptor` is an absolute security descriptor on this frame, initialized here, whose
    // DACL points to `acl`, which outlives the CreateFileW call that reads it.
    let described = unsafe {
        InitializeSecurityDescriptor(descriptor_ptr, SECURITY_DESCRIPTOR_REVISION) != 0
            && SetSecurityDescriptorDacl(descriptor_ptr, 1, acl_ptr, 0) != 0
            && SetSecurityDescriptorControl(descriptor_ptr, SE_DACL_PROTECTED, SE_DACL_PROTECTED)
                != 0
    };
    if !described {
        return Err(failed());
    }
    let attributes = SECURITY_ATTRIBUTES {
        nLength: u32::try_from(std::mem::size_of::<SECURITY_ATTRIBUTES>())
            .map_err(|_| malformed("SECURITY_ATTRIBUTES' size"))?,
        lpSecurityDescriptor: descriptor_ptr,
        bInheritHandle: 0,
    };
    let wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    // SAFETY: `wide` is a NUL-terminated UTF-16 path and `attributes` points to the descriptor
    // above, both alive for the call; CREATE_NEW fails where a file exists, so no file another
    // made is opened.
    let handle = unsafe {
        CreateFileW(
            wide.as_ptr(),
            GENERIC_WRITE,
            0,
            &attributes,
            CREATE_NEW,
            FILE_ATTRIBUTE_NORMAL,
            null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(failed());
    }
    // SAFETY: `handle` is the file CreateFileW just opened, owned by the File alone from here.
    Ok(unsafe { File::from_raw_handle(handle.cast()) })
}

/// Whether `file` is its owner's alone: each entry of its DACL that allows anything allows its
/// owner or LocalSystem.
pub(super) fn owner_only(file: &File) -> Result<bool, std::io::Error> {
    let handle = file.as_raw_handle().cast();
    let asked = OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION;
    let mut needed = 0u32;
    // SAFETY: a null descriptor of no length asks only for the length needed, which the call
    // writes to `needed`; `handle` is the open file's, alive for the call.
    unsafe { GetKernelObjectSecurity(handle, asked, null_mut(), 0, &mut needed) };
    let mut buffer = aligned(needed)?;
    let descriptor: PSECURITY_DESCRIPTOR = buffer.as_mut_ptr().cast();
    // SAFETY: `buffer` holds at least `needed` bytes, word-aligned, for the descriptor the call
    // writes; `handle` is as above.
    if unsafe { GetKernelObjectSecurity(handle, asked, descriptor, needed, &mut needed) } == 0 {
        return Err(failed());
    }
    let (mut owner, mut defaulted): (PSID, i32) = (null_mut(), 0);
    // SAFETY: `descriptor` is the valid descriptor just read; the owner's SID points into it.
    if unsafe { GetSecurityDescriptorOwner(descriptor, &mut owner, &mut defaulted) } == 0 {
        return Err(failed());
    }
    let (mut present, mut dacl): (i32, *mut ACL) = (0, null_mut());
    // SAFETY: as above; the DACL, where present, points into the descriptor.
    if unsafe { GetSecurityDescriptorDacl(descriptor, &mut present, &mut dacl, &mut defaulted) }
        == 0
    {
        return Err(failed());
    }
    if present == 0 || dacl.is_null() || owner.is_null() {
        return Ok(false);
    }
    let mut size = ACL_SIZE_INFORMATION::default();
    let size_bytes = u32::try_from(std::mem::size_of::<ACL_SIZE_INFORMATION>())
        .map_err(|_| malformed("ACL_SIZE_INFORMATION's size"))?;
    // SAFETY: `dacl` is the descriptor's valid ACL and `size` the structure the class names.
    if unsafe { GetAclInformation(dacl, (&raw mut size).cast(), size_bytes, AclSizeInformation) }
        == 0
    {
        return Err(failed());
    }
    for index in 0..size.AceCount {
        let mut ace: *mut core::ffi::c_void = null_mut();
        // SAFETY: `index` is below the ACL's count, and GetAce points `ace` at that entry inside
        // the descriptor.
        if unsafe { GetAce(dacl, index, &mut ace) } == 0 || ace.is_null() {
            return Err(failed());
        }
        // SAFETY: every entry begins with an ACE_HEADER, inside the descriptor.
        let kind = unsafe { std::ptr::read_unaligned(ace.cast::<ACE_HEADER>()) }.AceType;
        match kind {
            ACCESS_ALLOWED_ACE_TYPE => {
                // SAFETY: an allow entry is an ACCESS_ALLOWED_ACE whose SID begins at SidStart,
                // inside the descriptor.
                let sid: PSID =
                    unsafe { (&raw mut (*ace.cast::<ACCESS_ALLOWED_ACE>()).SidStart).cast() };
                // SAFETY: both SIDs are valid and inside the descriptor read above.
                let allowed = unsafe {
                    EqualSid(sid, owner) != 0 || IsWellKnownSid(sid, WinLocalSystemSid) != 0
                };
                if !allowed {
                    return Ok(false);
                }
            }
            // A deny entry takes access away and grants none.
            ACCESS_DENIED_ACE_TYPE => {}
            _ => return Ok(false),
        }
    }
    Ok(true)
}
