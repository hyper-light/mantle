//! macOS: the block sizes the storage device under a file's volume publishes in the I/O Registry,
//! read without opening the device.
//!
//! fstatfs(2) names the device the volume is mounted from (`f_mntfromname`, `/dev/disk3s5`).
//! IOBSDNameMatching and IOServiceGetMatchingService find the device's IOMedia object by that BSD
//! name, and IORegistryEntrySearchCFProperty, searching the object and then its parents in the
//! service plane, finds the "Physical Block Size" and "Logical Block Size" its storage device
//! publishes (IOKitLib.h; IOStorageDeviceCharacteristics.h). An APFS volume's media sits on a
//! synthesized container disk, whose parents lead to the physical store. DKIOCGETPHYSICALBLOCKSIZE
//! answers from the same property found by the same search (IOMediaBSDClient.cpp), but only on a
//! disk node, which is root's and the operator group's, mode 0640; the registry is read by any
//! user (mantle research/02 §3.2–3.5).
#![allow(unsafe_code)]

use std::ffi::{CStr, CString, c_char, c_void};
use std::fs::File;
use std::io;

type CFTypeRef = *const c_void;
type CFIndex = isize;
type CFTypeID = usize;
type MachPort = u32;
type IoObject = MachPort;

/// `kCFStringEncodingUTF8` (CFString.h).
const UTF8: u32 = 0x0800_0100;
/// `kCFNumberSInt64Type` (CFNumber.h).
const SINT64: CFIndex = 4;
/// `kIORegistryIterateRecursively` (IOKitKeys.h): the search goes on past the entry it starts at.
const ITERATE_RECURSIVELY: u32 = 0x1;
/// `kIORegistryIterateParents` (IOKitKeys.h): the search goes through the entry's parents, up to
/// the storage device that publishes the sizes, in place of its children.
const ITERATE_PARENTS: u32 = 0x2;
/// `kIOMainPortDefault`, "a synonym for NULL" (IOKitLib.h).
const MAIN_PORT: MachPort = 0;
/// IOBSDNameMatching's options: "No options are currently defined" (IOKitLib.h).
const NO_OPTIONS: u32 = 0;
/// `kIOServicePlane` (IOKitKeys.h).
const SERVICE_PLANE: &CStr = c"IOService";
/// `kIOPropertyPhysicalBlockSizeKey` (IOStorageDeviceCharacteristics.h).
const PHYSICAL_BLOCK_SIZE: &CStr = c"Physical Block Size";
/// `kIOPropertyLogicalBlockSizeKey` (IOStorageDeviceCharacteristics.h).
const LOGICAL_BLOCK_SIZE: &CStr = c"Logical Block Size";
/// What `f_mntfromname` begins with for a volume mounted from a device: the device's node in
/// /dev. A volume mounted from elsewhere, a network share or devfs, names no device (statfs(2)).
const DEV: &str = "/dev/";

#[link(name = "CoreFoundation", kind = "framework")]
unsafe extern "C" {
    fn CFStringCreateWithCString(alloc: CFTypeRef, s: *const c_char, encoding: u32) -> CFTypeRef;
    fn CFRelease(cf: CFTypeRef);
    fn CFGetTypeID(cf: CFTypeRef) -> CFTypeID;
    fn CFNumberGetTypeID() -> CFTypeID;
    fn CFNumberGetValue(number: CFTypeRef, kind: CFIndex, out: *mut c_void) -> u8;
}

#[link(name = "IOKit", kind = "framework")]
unsafe extern "C" {
    fn IOBSDNameMatching(port: MachPort, options: u32, bsd_name: *const c_char) -> CFTypeRef;
    fn IOServiceGetMatchingService(port: MachPort, matching: CFTypeRef) -> IoObject;
    fn IORegistryEntrySearchCFProperty(
        entry: IoObject,
        plane: *const c_char,
        key: CFTypeRef,
        alloc: CFTypeRef,
        options: u32,
    ) -> CFTypeRef;
    fn IOObjectRelease(object: IoObject) -> i32;
}

/// The block sizes a storage device publishes; each `None` where it publishes none.
#[derive(Default)]
pub(super) struct Sizes {
    /// "Physical Block Size".
    pub(super) physical: Option<u32>,
    /// "Logical Block Size".
    pub(super) logical: Option<u32>,
}

/// The block sizes the storage device under `file`'s volume publishes; none where the volume is
/// mounted from no device or the registry holds no entry for it.
pub(super) fn sizes(file: &File) -> io::Result<Sizes> {
    let stat = rustix::fs::fstatfs(file)?;
    let Some(entry) = bsd_name(&stat.f_mntfromname).and_then(|name| media(&name)) else {
        return Ok(Sizes::default());
    };
    Ok(Sizes {
        physical: number(&entry, PHYSICAL_BLOCK_SIZE),
        logical: number(&entry, LOGICAL_BLOCK_SIZE),
    })
}

/// The BSD name of the device a volume is mounted from (`disk3s5` of `/dev/disk3s5`).
fn bsd_name(from: &[c_char]) -> Option<CString> {
    let bytes: Vec<u8> = from
        .iter()
        .take_while(|&&c| c != 0)
        .map(|&c| u8::from_ne_bytes(c.to_ne_bytes()))
        .collect();
    let name = std::str::from_utf8(&bytes).ok()?.strip_prefix(DEV)?;
    CString::new(name).ok()
}

/// A Core Foundation object this module owns (a Create or Copy result), released on drop.
struct Owned(CFTypeRef);

impl Owned {
    /// Takes a Create or Copy result; null, which owns nothing, is `None`.
    fn new(cf: CFTypeRef) -> Option<Self> {
        (!cf.is_null()).then_some(Self(cf))
    }
}

impl Drop for Owned {
    fn drop(&mut self) {
        // SAFETY: `self.0` is a non-null object a Create or Copy call gave this module, which
        // holds its one reference; this is that reference's one release.
        unsafe { CFRelease(self.0) }
    }
}

/// A registry entry this module holds a reference to, released on drop.
struct Entry(IoObject);

impl Drop for Entry {
    fn drop(&mut self) {
        // SAFETY: `self.0` is a non-zero object IOServiceGetMatchingService returned, whose one
        // reference this module holds; this releases it. A failed release leaves nothing to undo.
        let _released = unsafe { IOObjectRelease(self.0) };
    }
}

/// The registry entry of the media named `name`.
fn media(name: &CStr) -> Option<Entry> {
    // SAFETY: `name` is a NUL-terminated string that outlives the call. IOBSDNameMatching returns
    // a new dictionary or null; IOServiceGetMatchingService consumes the one reference to the
    // dictionary whatever it returns ("One reference is always consumed by this function",
    // IOKitLib.h), so nothing is left here to release.
    let service = unsafe {
        let matching = IOBSDNameMatching(MAIN_PORT, NO_OPTIONS, name.as_ptr());
        if matching.is_null() {
            return None;
        }
        IOServiceGetMatchingService(MAIN_PORT, matching)
    };
    (service != 0).then_some(Entry(service))
}

/// The number `key` names on `entry` or its nearest parent that has it, where it fits 32 bits.
fn number(entry: &Entry, key: &CStr) -> Option<u32> {
    // SAFETY: `key` is a NUL-terminated UTF-8 string that outlives the call; the result is a new
    // string or null, which `Owned` takes.
    let key =
        Owned::new(unsafe { CFStringCreateWithCString(std::ptr::null(), key.as_ptr(), UTF8) })?;
    // SAFETY: `entry` holds a live registry object, the plane's name is NUL-terminated and `key`
    // is a live CFString; the result is a new reference or null, which `Owned` takes.
    let value = Owned::new(unsafe {
        IORegistryEntrySearchCFProperty(
            entry.0,
            SERVICE_PLANE.as_ptr(),
            key.0,
            std::ptr::null(),
            ITERATE_RECURSIVELY | ITERATE_PARENTS,
        )
    })?;
    // SAFETY: `value` is a live Core Foundation object; asking its type reads nothing else.
    if unsafe { CFGetTypeID(value.0) != CFNumberGetTypeID() } {
        return None;
    }
    let mut out: i64 = 0;
    // SAFETY: `value` is a live CFNumber, its type checked above, and `out` is the i64 that
    // kCFNumberSInt64Type writes, live for the call.
    let exact = unsafe { CFNumberGetValue(value.0, SINT64, (&raw mut out).cast()) };
    if exact == 0 {
        return None;
    }
    u32::try_from(out).ok()
}
