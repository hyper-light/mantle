//! Windows: the capacity of a disk or volume opened by its device path, from
//! IOCTL_DISK_GET_LENGTH_INFO, since such a handle has no file size; the sector sizes of the
//! volume a file is on, from GetFileInformationByHandleEx's FileStorageInfo class; and the write
//! cache of the device under a file, from IOCTL_STORAGE_QUERY_PROPERTY's
//! StorageDeviceWriteCacheProperty.
#![allow(unsafe_code)]

use std::fs::File;
use std::io;
use std::os::windows::io::AsRawHandle;

use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE};
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, FILE_FLAG_BACKUP_SEMANTICS, FILE_SHARE_READ, FILE_SHARE_WRITE, FILE_STORAGE_INFO,
    FileStorageInfo, GetFileInformationByHandleEx, GetFinalPathNameByHandleW, OPEN_EXISTING,
    VOLUME_NAME_GUID,
};
use windows_sys::Win32::System::IO::DeviceIoControl;
use windows_sys::Win32::System::Ioctl::{
    GET_LENGTH_INFORMATION, IOCTL_DISK_GET_LENGTH_INFO, IOCTL_STORAGE_QUERY_PROPERTY,
    PropertyStandardQuery, STORAGE_PROPERTY_QUERY, STORAGE_WRITE_CACHE_PROPERTY,
    StorageDeviceWriteCacheProperty,
};

/// What a volume GUID path begins with: `\\?\Volume{` and the GUID that ends at its brace
/// (Naming a Volume, Microsoft Learn).
const VOLUME_GUID_PREFIX: &str = r"\\?\Volume{";

/// A handle this module opened, closed on drop.
struct Owned(HANDLE);

impl Drop for Owned {
    fn drop(&mut self) {
        // SAFETY: `self.0` is a valid handle CreateFileW returned, which this value owns; this is
        // its one close. A failed close leaves nothing to undo.
        let _closed = unsafe { CloseHandle(self.0) };
    }
}

/// The bytes of the STORAGE_WRITE_CACHE_PROPERTY the device under `file` answers: the device
/// itself where `file` is a node opened by its device path, else the volume `file` is on, opened
/// by its volume GUID path with no access rights, which CreateFileW allows "to query device
/// attributes without accessing a device" (CreateFileW, Physical Disks and Volumes, Microsoft
/// Learn), so without privilege. `None` where no device answers: a file on no local volume, a
/// volume spanning several disks, or a driver that does not report the property.
pub(crate) fn write_cache_property(file: &File, node: bool) -> Option<Vec<u8>> {
    if node {
        return query_write_cache(file.as_raw_handle());
    }
    let volume = volume_of(file)?;
    query_write_cache(volume.0)
}

/// The volume `file` is on, opened by the volume GUID path GetFinalPathNameByHandleW gives for
/// the open file with VOLUME_NAME_GUID, without its trailing separator: with it the path names
/// the volume's root directory, without it the volume (Naming a Volume, Microsoft Learn).
fn volume_of(file: &File) -> Option<Owned> {
    let handle = file.as_raw_handle();
    // SAFETY: the handle is `file`'s and outlives the call; a buffer of no characters asks only
    // for the size the path needs, its terminator included (GetFinalPathNameByHandleW).
    let needed =
        unsafe { GetFinalPathNameByHandleW(handle, std::ptr::null_mut(), 0, VOLUME_NAME_GUID) };
    let mut path = vec![0u16; usize::try_from(needed).ok().filter(|&n| n > 0)?];
    // SAFETY: the handle is `file`'s; `path` is writable for the `needed` characters the first
    // call asked for. A path that grew between the calls returns a larger size, refused below.
    let written =
        unsafe { GetFinalPathNameByHandleW(handle, path.as_mut_ptr(), needed, VOLUME_NAME_GUID) };
    if written == 0 || written >= needed {
        return None;
    }
    let path = String::from_utf16(path.get(..usize::try_from(written).ok()?)?).ok()?;
    let rest = path.strip_prefix(VOLUME_GUID_PREFIX)?;
    let guid = rest.get(..rest.find('}')?)?;
    let volume: Vec<u16> = format!("{VOLUME_GUID_PREFIX}{guid}}}")
        .encode_utf16()
        .chain(std::iter::once(0))
        .collect();
    // SAFETY: `volume` is NUL-terminated and outlives the call; no security attributes or
    // template are passed. No access rights are asked.
    let opened = unsafe {
        CreateFileW(
            volume.as_ptr(),
            0,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            std::ptr::null(),
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS,
            std::ptr::null_mut(),
        )
    };
    if opened == INVALID_HANDLE_VALUE || opened.is_null() {
        return None;
    }
    Some(Owned(opened))
}

/// The bytes the device behind `device` answers StorageDeviceWriteCacheProperty with, as many as
/// it returned of one STORAGE_WRITE_CACHE_PROPERTY; `None` where the request fails.
fn query_write_cache(device: HANDLE) -> Option<Vec<u8>> {
    let request = STORAGE_PROPERTY_QUERY {
        PropertyId: StorageDeviceWriteCacheProperty,
        QueryType: PropertyStandardQuery,
        AdditionalParameters: [0],
    };
    let mut out = vec![0u8; std::mem::size_of::<STORAGE_WRITE_CACHE_PROPERTY>()];
    let mut returned = 0u32;
    let in_len = u32::try_from(std::mem::size_of::<STORAGE_PROPERTY_QUERY>()).ok()?;
    let out_len = u32::try_from(out.len()).ok()?;
    // SAFETY: `device` is a live handle for the call; the input is a STORAGE_PROPERTY_QUERY of
    // `in_len` bytes and the output is writable for `out_len` bytes; without an OVERLAPPED the
    // call completes before it returns, so both buffers outlive it.
    let ok = unsafe {
        DeviceIoControl(
            device,
            IOCTL_STORAGE_QUERY_PROPERTY,
            (&raw const request).cast(),
            in_len,
            out.as_mut_ptr().cast(),
            out_len,
            &mut returned,
            std::ptr::null_mut(),
        )
    };
    if ok == 0 {
        return None;
    }
    out.truncate(usize::try_from(returned).ok()?);
    Some(out)
}

/// The sector sizes of the volume `file` is on (FILE_STORAGE_INFO, Windows 8 and Server 2012 on).
pub(crate) fn storage_info(file: &File) -> io::Result<FILE_STORAGE_INFO> {
    let mut info = FILE_STORAGE_INFO {
        LogicalBytesPerSector: 0,
        PhysicalBytesPerSectorForAtomicity: 0,
        PhysicalBytesPerSectorForPerformance: 0,
        FileSystemEffectivePhysicalBytesPerSectorForAtomicity: 0,
        Flags: 0,
        ByteOffsetForSectorAlignment: 0,
        ByteOffsetForPartitionAlignment: 0,
    };
    let size = u32::try_from(std::mem::size_of::<FILE_STORAGE_INFO>())
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    // SAFETY: the handle is `file`'s and outlives the call; the output is a writable
    // FILE_STORAGE_INFO of `size` bytes, the structure the FileStorageInfo class fills, and the
    // call is synchronous, so the buffer outlives it.
    let ok = unsafe {
        GetFileInformationByHandleEx(
            file.as_raw_handle(),
            FileStorageInfo,
            (&raw mut info).cast(),
            size,
        )
    };
    if ok == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(info)
}

pub(crate) fn len(file: &File) -> io::Result<u64> {
    let mut info = GET_LENGTH_INFORMATION { Length: 0 };
    let mut returned = 0u32;
    let size = u32::try_from(std::mem::size_of::<GET_LENGTH_INFORMATION>())
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    // SAFETY: the handle is `file`'s and outlives the call; the output is a writable
    // GET_LENGTH_INFORMATION of `size` bytes; without an OVERLAPPED the call completes before
    // it returns, so the buffer outlives it too.
    let ok = unsafe {
        DeviceIoControl(
            file.as_raw_handle(),
            IOCTL_DISK_GET_LENGTH_INFO,
            std::ptr::null(),
            0,
            (&raw mut info).cast(),
            size,
            &mut returned,
            std::ptr::null_mut(),
        )
    };
    if ok == 0 {
        return Err(io::Error::last_os_error());
    }
    u64::try_from(info.Length).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "the device reports a negative length",
        )
    })
}
