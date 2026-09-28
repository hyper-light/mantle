# 02 — OS Storage APIs: Finding the Real Device Under a Data Directory and Writing Bytes Durably (Linux, macOS, Windows)

**Status:** research input for mantle's storage-node bootstrap (device detection and I/O-strategy selection) and for the durable-write path of the chunk store and the metadata log. This is not a design decision record. It is the OS-API ground truth that the recommendations in `03-io-and-persistence.md` have to be implemented against.
**Compiled:** 2026-09-28.
**Scope:** Linux, macOS and Windows on x86_64 and aarch64. For a given data directory, it covers:
- which filesystem and which physical or virtual device(s) back it;
- what those devices report: block sizes, alignment, write cache and FUA, rotational status or seek penalty, zoned status, and identity strings;
- the exact semantics of each OS's durability, preallocation, cache-bypass and asynchronous-I/O primitives, including the privileges they need and how they fail.

---

## 0. How to read this document

- **Evidence labels.** Every non-trivial claim has one of these labels.
  - **[doc]** Official documentation: kernel.org `Documentation/`, man7.org man pages, Apple developer documentation and Apple open-source man pages and headers, or Microsoft Learn.
  - **[src]** Read directly in primary source code (the Linux kernel; Apple's xnu, IOStorageFamily, IOKitUser and hfs), with the file and line cited in §8.
  - **[obs]** Observed on the research machine: macOS 26.4.1 (build 25E253), Apple Silicon, internal "APPLE SSD AP8192Z", APFS. This shows how *that* machine behaves and nothing more.
  - **[ms-meta]** A numeric value that Microsoft Learn does not print. It was read from Microsoft's metadata-generated Rust bindings `windows-sys` 0.61.2 (the `microsoft/windows-rs` project). These are Microsoft's own bindings, but they are not on learn.microsoft.com.
  - **Inference:** our own reasoning from the cited material. The source does not say it.
  - **UNVERIFIED:** we could not confirm this from an allowed primary source. Do not rely on it without testing.
- **Quotes** in "double quotes" are verbatim from the source. HTML, roff and PDF were converted to text and line wrapping was joined; `[sic]` marks a typo in the original.
- **Version pins.** Everything was read at these revisions:
  - Linux: tag `v7.3-rc5` (commit `72d3fcf802c4`, 2026-09-27). Any "since Linux X.Y" claim comes from the man pages, or we checked it by fetching the file at the release tags.
  - man pages: man7.org, *Linux man-pages 6.19* (2026-02-08). The io_uring pages are liburing's, mirrored by man7.org from an upstream snapshot dated 2026-08-04.
  - Apple (github.com/apple-oss-distributions): `xnu-12377.121.6`, `IOStorageFamily-337.100.1`, `IOKitUser-100231.100.18.0.1`, `hfs-715.100.10`. Also the macOS SDK 26.4 headers from the Command Line Tools.
  - Microsoft: learn.microsoft.com pages, retrieved 2026-09-28.
- **Source keys** such as (L1), (M3), (A8) and (W20) refer to the source index in §8.

---

## 1. Executive summary

1. **Mapping a path to its hardware** is mechanical on all three OSes, but it has blind spots that need explicit handling:
   - **Linux:** `stat.st_dev` → `/sys/dev/block/MAJ:MIN` → resolve the link. A partition's parent directory is its disk. `slaves/` leads to the components of dm/md devices, and `loop/backing_file` leads to a loop device's backing file. Filesystems with an anonymous `st_dev` (btrfs, overlayfs directories, tmpfs, NFS, FUSE, OpenZFS) have no sysfs node, so fall back to `/proc/self/mountinfo` or filesystem-specific ioctls.
   - **macOS:** `statfs.f_mntfromname` → BSD name → `IOBSDNameMatching` → walk *parents* in the service plane. This walk reaches through the APFS container to the physical store.
   - **Windows:** `GetVolumePathNameW` → volume GUID path → `IOCTL_VOLUME_GET_VOLUME_DISK_EXTENTS` → `\\.\PhysicalDriveN` → `IOCTL_STORAGE_QUERY_PROPERTY`. (§2.1, §3.1–3.2, §4.1–4.2)
2. **"Rotational", "seek penalty" and "Medium Type" are driver policy, not measurements.** Examples:
   - Linux `sd` assumes rotational unless the device's Block Device Characteristics VPD page reports rotation rate 1.
   - `virtio_blk` *always* reports rotational.
   - Since Linux 6.11, drivers must opt in to rotational, so the default is non-rotational.
   - macOS "Medium Type" is optional.
   - Windows seek-penalty and write-cache queries can fail or be unsupported.

   Treat all of these as hints and confirm them by measurement. (§2.5, §3.3, §4.2)
3. **Facts you can trust:**
   - the logical block size, and on Linux the direct-I/O alignment (`statx(STATX_DIOALIGN)`, 6.1+);
   - the zoned model (`host-managed`/`host-aware`);
   - the kernel's *own* view of the write cache and FUA, which determines what flushes the kernel will send;
   - the filesystem type;
   - whether the filesystem is local or on the network;
   - whether it is RAM-backed. (§6.2)
4. **Durability semantics differ in ways that matter:**
   - **Linux:** `fsync`/`fdatasync` flush the device's volatile cache. A new name needs `fsync` on the directory. After an `fsync` error, a later `fsync` can return 0 even though the data never reached disk. `sync_file_range` is not a durability primitive.
   - **macOS:** `fsync` does **not** make the drive flush. Only `F_FULLFSYNC` does; `F_BARRIERFSYNC` orders writes without guaranteeing durability. xnu treats `O_DSYNC` as `O_SYNC`.
   - **Windows:** `FILE_FLAG_WRITE_THROUGH` + `FILE_FLAG_NO_BUFFERING` asks the device to write through its cache (the FUA mechanism), and not all hardware supports it. `FlushFileBuffers` sends `IRP_MJ_FLUSH_BUFFERS`, which reaches the device cache. `NtFlushBuffersFileEx(FLUSH_FLAGS_FILE_DATA_SYNC_ONLY)` is the `fdatasync` equivalent (NTFS only). (§2.9, §3.7–3.8, §4.6)
5. **Linux's cheapest durable write** is `O_DIRECT` + `O_DSYNC`/`RWF_DSYNC` (**not** `O_SYNC`/`RWF_SYNC`) that purely overwrites already-*written* extents inside EOF, on an iomap filesystem (ext4, XFS). On btrfs, copy-on-write means an overwrite allocates new blocks, so this path applies at most to NOCOW files (Inference). When the device supports FUA, or has no volatile cache, each such write is a single FUA write with no cache flush afterwards. Two consequences for preallocation:
   - A plain `fallocate` creates *unwritten* extents. The first write to each one needs a metadata conversion at completion, and that disables this fast path.
   - `FALLOC_FL_WRITE_ZEROES` (Linux 6.17: ext4 and block devices; XFS on current mainline) creates *written* zeroed extents instead. (§2.10–2.11)
6. **On Windows, extending a file is synchronous, and NTFS zero-fills** everything between the valid data length (VDL) and the write offset. `SetFileValidData` avoids the zero-fill but needs `SE_MANAGE_VOLUME_NAME`, and it can expose stale disk contents. (§4.7)
7. **Privileges:**
   - **Linux:** sysfs is world-readable. `BLK*` ioctls need access to the device node.
   - **macOS:** the IORegistry is readable without privileges. `/dev/disk*` is `root:operator 0640`, so `DKIOC*` ioctls effectively need root.
   - **Windows:** the documentation is inconsistent. `IOCTL_STORAGE_QUERY_PROPERTY` is `FILE_ANY_ACCESS`, and Learn says a handle opened with zero access can query attributes. But opening a disk or volume is documented to require admin, and the Advanced Format guide says the alignment query "Requires elevated privilege". Whether it works unprivileged is **UNVERIFIED**. The unprivileged per-handle queries (`FileStorageInfo` and `FileFsSectorSizeInformation`; the latter includes `SSINFO_FLAGS_NO_SEEK_PENALTY` and TRIM) are documented to work for any caller. (§4.3–4.4)
8. **io_uring availability is a runtime question:**
   - `ENOSYS` means the kernel was built without it.
   - `EPERM` means `kernel.io_uring_disabled` blocks it (6.6+), or an LSM or seccomp policy does.
   - `IORING_REGISTER_PROBE` (5.6+) reports which opcodes are supported.
   - The man page states that `IORING_OP_FSYNC` does **not** wait for writes that are still in flight. Order writes before an fsync with `IOSQE_IO_LINK` (or `IOSQE_IO_DRAIN`). (§2.13)
9. **Whether APFS overwrites data in place or copies on write for non-cloned files is UNVERIFIED.** Apple's own documents conflict. Measure it with `F_LOG2PHYS_EXT`. (§3.10)

---

## 2. Linux

### 2.1 From a path to its filesystem and its block device(s)

**Step A: identify the filesystem.**
- `statfs(2)`/`fstatfs(2)` → `f_type`, a filesystem magic number. The table is in §2.12. [doc] (M7)
- `statx(2)` with `STATX_MNT_ID` (0x1000, since 5.8) returns `stx_mnt_id`. The man page says this "is the same number reported by name_to_handle_at(2) and corresponds to the number in the first field in one of the records in /proc/self/mountinfo". `STATX_MNT_ID_UNIQUE` (0x4000, since 6.8) returns a unique ID that "is guaranteed to not be reused while the system is running". [doc] (M1)
- `/proc/self/mountinfo` fields [doc] (M11):
  - field (3): "major:minor: the value of st_dev for files on this filesystem (see stat(2))";
  - field (9): "filesystem type: the filesystem type in the form 'type[.subtype]'";
  - field (10): "mount source: filesystem-specific information or 'none'";
  - field (11): super options.

  Matching `stx_mnt_id` against field (1) is exact. Matching on the longest mount-point prefix breaks under bind mounts and stacked mounts (Inference).

**Step B: map `st_dev` into sysfs.**
- `/sys/dev` (KernelVersion 2.6.26): "The /sys/dev tree provides a method to look up the sysfs path for a device using the information returned from stat(2). There are two directories, 'block' and 'char', beneath /sys/dev containing symbolic links with names of the form '<major>:<minor>'. These links point to the corresponding sysfs path for the given device." [doc] (L3)
- Split `st_dev` with `major(3)`/`minor(3)` from `makedev(3)`. [doc] (M17)
- Rules from `sysfs-rules.rst` that affect how the walk is coded [doc] (L4):
  - "Symlinks pointing to /sys/devices must always be resolved to their real target".
  - "Never depend on a specific parent device position in the devpath, or the chain of parent devices... You must always request the parent device you are looking for by its subsystem value. You need to walk up the chain until you find the device that matches the expected subsystem."
  - The "device" link may only be used to find the parent in `/sys/devices`.
  - "The converted block subsystem at /sys/class/block ... will contain the links for disks and partitions at the same level, never in a hierarchy."

**Step C: go from a partition to its whole disk.**
- A partition's directory has a `partition` attribute (the partition number). Partitions also have `start`, `size`, `ro`, `alignment_offset`, `discard_alignment`, `stat` and `inflight`. [src] (L7 `block/partitions/core.c:208–215`)
- The ABI documents partitions as `/sys/block/<disk>/<partition>/…` [doc] (L1). So once the path is resolved, the whole disk is the partition's parent directory.
- `queue/` exists only on the whole disk (Inference from the ABI layout in L1).

**Step D: descend through stacked devices.**
- Every gendisk gets a `holders/` directory and a `slaves/` directory [src] (L8 `block/genhd.c:519–526`). `bd_link_disk_holder` populates them. For example, "/sys/block/dm-0/slaves/sda --> /sys/block/sda" and "/sys/block/sda/holders/dm-0 --> /sys/block/dm-0" [src] (L6 `block/holder.c:43–50`).
- Both device-mapper and md call `bd_link_disk_holder` [src] (`drivers/md/dm.c:761, 2643`; `drivers/md/md.c:2633`).
- **dm:** `/sys/block/dm-<num>/dm/name`, `dm/uuid` ("DM-UUID or empty string"), and `dm/suspended` [doc] (L10). The DM-UUID prefixes that identify LVM, dm-crypt and multipath devices are userspace conventions. The kernel docs do not define them (**UNVERIFIED**).
- **md:** `/sys/block/mdX/md/{level, raid_disks, chunk_size, component_size, array_state, dev-XXX/…}` [doc] (L11).
- **loop:** `/sys/block/loopX/loop/{backing_file, offset, sizelimit, autoclear, partscan, dio}`. `backing_file` is "The path of the backing file that the loop device maps its data blocks to", and `dio` shows "if direct IO is being used to access backing file" [doc] (L9). Recurse on the backing file, with a depth limit (Inference).
- **Limits of stacked devices:**
  - Features are OR-ed together: `t->features |= (b->features & BLK_FEAT_INHERIT_MASK)`. The inherited mask is write cache, FUA, rotational, stable writes, zoned and one bcache flag [src] (L22 `block/blk-settings.c:786`; L25 `include/linux/blkdev.h:363–366`). So one rotational member makes the whole stack report rotational.
  - The logical and physical block sizes and `io_min` take the maximum over the members, and `io_opt` takes their least common multiple [src] (L22 `blk-settings.c:852–860`).

**Step E: handle `st_dev` with major 0 (anonymous device, so no `/sys/dev/block` entry).**
- **tmpfs/ramfs:** RAM-backed, with no device.
- **overlayfs:** "While directories will report an st_dev from the overlay-filesystem, non-directory objects may report an st_dev from the lower filesystem or upper filesystem that is providing the object." [doc] (L39) Also see the `volatile` option in §2.9.
- **btrfs:**
  - `BTRFS_IOC_FS_INFO` and `BTRFS_IOC_DEV_INFO` are defined in `include/uapi/linux/btrfs.h:1198–1200`. Their handlers in `fs/btrfs/ioctl.c` perform no capability check, so they work unprivileged, and `DEV_INFO` copies out the device path [src] (L46).
  - A `devices` kobject exists under `/sys/fs/btrfs/<FSID>/` [src] (L46 `fs/btrfs/sysfs.c:2266`). There is no ABI document for it in the kernel tree, so its stability is **UNVERIFIED**.
  - That btrfs gives each subvolume an anonymous `st_dev` is widely stated but **UNVERIFIED** here.
- **NFS/CIFS/SMB3/FUSE/9p/Ceph/OpenZFS:** classify by `f_type` and mountinfo (§2.12), then measure.

**Step F: classify each leaf device.**
- If the leaf resolves under `/sys/devices/virtual/block/…`, it has no parent device. The driver core says: "If we have no parent, we live in "virtual"." [src] (L26 `drivers/base/core.c:3325`) This covers dm, md, loop, zram, brd, nbd and similar devices.
- Otherwise walk the parent chain and read the basename of each ancestor's `subsystem` link. The kernel names are:
  - `virtio` [src `drivers/virtio/virtio.c:460`]
  - `vmbus` (Hyper-V) [src `drivers/hv/vmbus_drv.c:1009`]
  - `xen` [src `drivers/xen/xenbus/xenbus_probe_frontend.c:163`]
  - `usb` [src `drivers/usb/core/driver.c:2081`]
  - `mmc` [src `drivers/mmc/core/bus.c:221`]
  - `nvme` (class) [src `drivers/nvme/host/core.c:143`]

### 2.2 Recognizing device types

| Kernel name | Driver | Block major (devices.txt) | Robust signature | Notes |
|---|---|---|---|---|
| `nvmeXnY`. With native multipath, the paths `nvmeXcYnZ` are hidden. | NVMe | Dynamic. Major 259 is "Block Extended Major … Used dynamically to hold additional partition minor numbers" [doc] (L12) | The disk's parent is a class-`nvme` controller. A multipath head's parent is the `nvme-subsystem` device [src] (L14 `core.c:4311`, `multipath.c:804`). `/sys/class/nvme/nvmeX/transport` is one of "pcie", "tcp", "rdma", "fc", "loop" [doc] (L13) | tcp/rdma/fc transports mean NVMe over Fabrics, i.e. network-attached block storage. Hidden path devices have `/sys/block/<disk>/hidden` = 1, "Used for the underlying components of multipath devices" [doc] (L1) |
| `sdX` | SCSI disk (`sd`) | 8, 65–71, 128–135 "SCSI disk devices" [doc] (L12) | A `scsi` ancestor. SATA disks behind libata report vendor `"ATA     "` [src] (L17 `libata-scsi.c:2104, 2230`). USB mass storage has a `usb` ancestor, Hyper-V disks a `vmbus` ancestor, virtio-scsi disks a `virtio` ancestor | One name covers SATA, SAS, USB, iSCSI, FC and paravirtual SCSI |
| `vdX` | virtio-blk | Dynamic (`register_blkdev(0, "virtblk")`) [src] (L18 `:1703`) | A `virtio` ancestor. Has `serial` and `cache_type` attributes [src] (L18) | `rotational` is always 1 [src] (§2.5) |
| `xvdX` | Xen blkfront | 202 "Xen Virtual Block Device" [doc] (L12) | A `xen` ancestor | |
| `mmcblkN[pM]` | MMC/SD/eMMC | 179 "MMC block devices" [doc] (L12) | `mmc` bus. The device has `name`, `type`, `manfid` and `oemid` attributes [doc] (L28) | |
| `zramN` | zram | Dynamic | `/sys/devices/virtual/block`. "Statistics for individual zram devices are exported through sysfs nodes at /sys/block/zram<id>/" [doc] (L28) | RAM-backed and compressed |
| `ramN` | brd | 1 "RAM disk" [doc] (L12); named `"ram%d"` [src] (`drivers/block/brd.c:322`) | virtual | RAM-backed |
| `loopN` | loop | 7 "Loopback devices" [doc] (L12) | `loop/` subdirectory [doc] (L9) | Backed by a file |
| `nbdN` | NBD | 43 "Network block devices" [doc] (L12) | `pid` and `backend` attributes [src] (L20 `nbd.c:246–263`) | Network |
| `rbdN` | Ceph RBD | Dynamic | `/sys/bus/rbd/devices/<dev-id>/{pool,name,image_id,…}` [doc] (L28) | Network |
| `mdN` | md RAID | 9 "Metadisk (RAID) devices" [doc] (L12) | `md/` and `slaves/` | Stacked |
| `dm-N` | device-mapper | Dynamic | `dm/` and `slaves/` | Stacked (LVM, crypt, multipath and others) |
| `pmemN` | NVDIMM | Dynamic | Sets `BLK_FEAT_DAX` when mappable. Sets `BLK_FEAT_FUA` only if persistence can be guaranteed; otherwise it warns "unable to guarantee persistence of writes" [src] (L28 `drivers/nvdimm/pmem.c:508–517`) | Byte-addressable |
| `ublkbN` | ublk | Dynamic | "ublk block device (/dev/ublkb*) is added by ublk driver" [doc] (L28) | Implemented in userspace |

Inference: do not classify by major number alone, because the NVMe, virtio, dm, zram and rbd majors are dynamic.

### 2.3 Drive identity through sysfs (all world-readable)

- **NVMe controller:** `/sys/class/nvme/nvmeX/{model, serial, firmware_rev}` "Shows the model, serial number, or firmware revision string of the NVMe controller, as reported in the Identify Controller data structure" (KernelVersion 4.5). The controller also has `cntlid`, `transport`, `subsysnqn`, `numa_node`, `queue_count` and `sqsize` [doc] (L13).
- **NVMe subsystem:** `/sys/class/nvme-subsystem/nvme-subsysX/{model, serial, firmware_rev, subsysnqn, iopolicy}` (KernelVersion 4.15) [doc] (L13).
- **NVMe namespace:** `/sys/block/nvmeXnY/{wwid, uuid, eui, nguid, nsid, csi, metadata_bytes, nuse}` [doc] (L13).
- All of these are `S_IRUGO`, i.e. readable by everyone [src] (L14 `sysfs.c:461–473`).
- **SCSI:** `/sys/block/sdX/device/{vendor, model, rev}` are printed as `%.8s`, `%.16s` and `%.4s`. The same directory has `type`, `scsi_level`, `wwid`, `serial`, `queue_depth` and more [src] (L15 `scsi_sysfs.c:650–655`).
- **SCSI disk class** (`/sys/class/scsi_disk/H:C:T:L/`): `cache_type`, `FUA`, `protection_type`, `provisioning_mode`, `zeroing_mode`, `max_write_same_blocks`, `zoned_cap` and more [src] (L16 `sd.c:759–778`).
  - `zoned_cap` returns "host-managed" (for ZBC-type devices), "host-aware", "drive-managed" or "none" [src] (L16 `sd.c:711–724`).
  - The value comes from the ZONED field of VPD page B1: `(vpd->data[8] >> 4) & 3`. The kernel logs "Drive-managed SMR disk" when that field is 2 [src] (L16 `sd.c:3498–3514`).
  - For SATA disks, libata *emulates* VPD page B1. It takes the rotation rate from IDENTIFY word 217 and the zoned capabilities from IDENTIFY word 69, bits 1:0 [src] (L17 `libata-scsi.c:2389–2405`; `include/linux/ata.h:991–994`).
  - So a **drive-managed SMR** disk that honestly reports itself is detectable on Linux, but only here. `queue/zoned` shows "none" for such disks (§2.4).
  - There is no ABI document for `zoned_cap`, so its stability is **UNVERIFIED**. Many drive-managed SMR drives may not report the field at all (**UNVERIFIED**).
- **virtio-blk:** `serial` and `cache_type` ("write through" or "write back") [src] (L18).
- **Removability:** `/sys/block/<disk>/removable` reflects `GENHD_FL_REMOVABLE` [src] (L8 `genhd.c:1034–1041`). The device-level `/sys/devices/…/removable` is "removable", "fixed" or "unknown", and "Currently this is only supported by USB … and PCI" [doc] (L27).

### 2.4 `/sys/block/<disk>/queue/*`: exact semantics

The primary document is `Documentation/ABI/stable/sysfs-block` (L1). `Documentation/block/queue-sysfs.rst` carried the same text until it was removed in v5.17 (it was last present in v5.16). The removal commit `208e4f9c0028` says: "This has been replaced by Documentation/ABI/stable/sysfs-block, which is the correct place for sysfs documentation." (L2)

Read-only attributes are mode 0444 and read-write ones 0644 [src] (L23 `blk-sysfs.c:585–609`). So reading needs no privileges, and writing needs root (Inference from the modes).

| Attribute | RO/RW | ABI "Date" | Semantics ([doc] L1, verbatim where quoted) | Notes for mantle |
|---|---|---|---|---|
| `rotational` | RW | Jan 2009 | "This file is used to stat if the device is of rotational type or non-rotational type." | Driver policy (§2.5). Admins and udev can rewrite it |
| `logical_block_size` | RO | May 2009 | "This is the smallest unit the storage device can address. It is typically 512 bytes." | The minimum O_DIRECT granularity for raw block devices |
| `hw_sector_size` | RO | Jan 2008 | "This is the hardware sector size of the device, in bytes." | The source says it is a "legacy alias for logical_block_size" [src] (L23 `:668–672`) |
| `physical_block_size` | RO | May 2009 | "This is the smallest unit a physical storage device can write atomically. It is usually the same as the logical block size but may be bigger. One example is SATA drives with 4KB sectors that expose a 512-byte logical block size… For stacked block devices the physical_block_size variable contains the maximum physical_block_size of the component devices." | The write size that avoids read-modify-write |
| `minimum_io_size` | RO | Apr 2009 | "…the smallest request the device can perform without incurring a performance penalty. For disk drives this is often the physical block size. For RAID arrays it is often the stripe chunk size. A properly aligned multiple of minimum_io_size is the preferred request size for workloads where a high number of I/O operations is desired." | |
| `optimal_io_size` | RO | Apr 2009 | "…the device's preferred unit for sustained I/O. This is rarely reported for disk drives. For RAID arrays it is usually the stripe width or the internal track size… If no optimal I/O size is reported this file contains 0." | Often 0. The source rounds it down to the physical block size [src] (L22 `blk-settings.c:376`) |
| `max_sectors_kb` | RW | Sep 2004 | "…the maximum number of kilobytes that the block layer will allow for a filesystem request. Must be smaller than or equal to the maximum size allowed by the hardware. Write 0 to use default kernel settings." | Bigger requests get split |
| `max_hw_sectors_kb` | RO | Sep 2004 | "…the maximum number of kilobytes supported in a single data transfer." | |
| `nr_requests` | RW | Jul 2003 | "…how many requests may be allocated in the block layer. Noted this value only represents the quantity for a single blk_mq_tags instance. The actual number for the entire device depends on the hardware queue count, whether elevator is enabled, and whether tags are shared." | Not the device's queue depth |
| `write_cache` | RW | Apr 2016 | "When read, this file will display whether the device has write back caching enabled or not. It will return "write back" for the former case, and "write through" for the latter. Writing to this file can change the kernels view of the device, but it doesn't alter the device state. This means that it might not be safe to toggle the setting from "write back" to "write through", since that will also eliminate cache flushes issued by the kernel." | Writing also accepts "none" [src] (L23 `:557–583`). This is exactly what decides whether the kernel sends flushes |
| `fua` | RO | May 2018 | "Whether or not the block driver supports the FUA flag for write requests. FUA stands for Force Unit Access. If the FUA flag is set that means that write requests must bypass the volatile cache of the storage device." | When 0, FUA requests are emulated with a flush after the write (§2.9) |
| `dax` | RO | Jun 2016 | "…whether the device supports Direct Access (DAX), used by CPU-addressable storage to bypass the pagecache. It shows '1' if true, '0' if not." | |
| `zoned` | RO | Sep 2016 | "…"none" for regular block devices and "host-aware" or "host-managed" for zoned block devices… These standards also define the "drive-managed" zone model. However, since drive-managed zoned block devices do not support zone commands, they will be treated as regular block devices and zoned will report "none"." | For drive-managed SMR see `zoned_cap` (§2.3) |
| `chunk_sectors` | RO | Sep 2016 | "…For a RAID device (dm-raid), chunk_sectors indicates the size in 512B sectors of the RAID volume stripe segment. For a zoned block device, either host-aware or host-managed, chunk_sectors indicates the size in 512B sectors of the zones of the device, with the eventual exception of the last zone of the device which may be smaller." | Zone size |
| `nr_zones`, `max_open_zones`, `max_active_zones`, `zone_append_max_bytes`, `zone_write_granularity` | RO | 2018–2021 | Zone count; open and active zone limits ("If this value is 0, there is no limit"); zone-append size; "the alignment constraint, in bytes, for write operations in sequential zones" | Constraints you *must* obey on host-managed devices |
| `zoned_qd1_writes` | RW | Jan 2026 | "…write operations to a zoned block device are being handled using a single issuer context… at a maximum queue depth of 1… For rotational zoned block devices (e.g. SMR HDDs) the default value is 1." | New in recent kernels |
| `discard_granularity` | RO | May 2011 | "…the size of the internal allocation unit in bytes if reported by the device. Otherwise the discard_granularity will be set to match the device's physical block size. A discard_granularity of 0 means that the device does not support discard functionality." | |
| `discard_max_bytes` / `discard_max_hw_bytes` | RW / RO | 2011 / 2015 | "While discard_max_hw_bytes is the hardware limit for the device, this setting is the software limit…" / "A discard_max_hw_bytes value of 0 means that the device does not support discard functionality." | |
| `discard_zeroes_data` | RO | May 2011 | "Will always return 0. Don't rely on any specific behavior for discards, and don't read this file." | |
| `write_zeroes_max_bytes` | RO | Nov 2016 | "If write_zeroes_max_bytes is 0, write zeroes is not supported by the device." | |
| `write_zeroes_unmap_max_hw_bytes` / `…_max_bytes` | RO / RW | Jan 2025 | "…whether a device supports zeroing data in a specified block range without incurring the cost of physically writing zeroes to the media… a device may fall back to physically writing zeroes…" | Relevant to `FALLOC_FL_WRITE_ZEROES` (§2.11) |
| `io_poll` / `io_poll_delay` | RW | 2015 / 2016 | "When read, this file shows whether polling is enabled (1) or disabled (0)…" / "…now fixed to -1, which is classic polling… <deprecated>" | Needed for `IORING_SETUP_IOPOLL` (§2.13) |
| `scheduler` | RW | Oct 2004 | "…the current and available IO schedulers for this block device. The currently active IO scheduler will be enclosed in [] brackets…" | |
| `dma_alignment` | RO | May 2022 | "Reports the alignment that user space addresses must have to be used for raw block device access with O_DIRECT and other driver specific passthrough mechanisms." | |
| `virt_boundary_mask` | RO | Apr 2021 | "…I/O requests to this device will be split between segments wherever either the memory address of the end of the previous segment or the memory address of the beginning of the current segment is not aligned to virt_boundary_mask + 1 bytes." | Matters for vectored or registered-buffer I/O |
| `atomic_write_max_bytes`, `atomic_write_unit_min_bytes`, `atomic_write_unit_max_bytes`, `atomic_write_boundary_bytes` | RO | Feb 2024 | Limits for hardware atomic (untorn) writes, e.g. "the smallest block which can be written atomically…" | **Doc path error:** the ABI lists them as `/sys/block/<disk>/atomic_write_*`, but the source registers them as *queue* attributes, `/sys/block/<disk>/queue/atomic_write_*` [src] (L23 `:635–639`) |
| `independent_access_ranges/N/{sector,nr_sectors}` | RO | Oct 2021 | "…the device is capable of executing requests targeting different sector ranges in parallel. For instance, single LUN multi-actuator hard-disks will have an independent_access_ranges directory…" | Detects multi-actuator HDDs |
| `stable_writes` | RW | Sep 2020 | "'1' if memory must not be modified while it is being used in a write request to this device…" | |
| `max_segments`, `max_segment_size`, `max_discard_segments`, `max_integrity_segments` | RO | 2010–2017 | Limits on DMA scatter/gather lists | |
| `max_write_streams`, `write_stream_granularity` | RO | Nov 2024 | Write streams; granularity is "the size that should be discarded or overwritten together to avoid write amplification in the device" | |
| `read_ahead_kb`, `iostats`, `add_random`, `nomerges`, `rq_affinity`, `wbt_lat_usec`, `io_timeout`, `async_depth`, `crypto/` | various | various | Tunables and inline-encryption capabilities | |

Per-disk attributes outside `queue/` [doc] (L1):
- `alignment_offset`: "how many bytes the beginning of the device is offset from the disk's natural alignment".
- `discard_alignment`.
- `diskseq`: "a monotonically increasing number assigned to every drive".
- `inflight`.
- `stat`: 17 fields, including flush count and time.
- `hidden`.
- `partscan`.

### 2.5 The driver policy behind `rotational`, `write_cache` and `fua`

Since v6.11, the non-rotational flag lives in the queue limits, and drivers must *opt in* to rotational. Commit `bd4a633b6f7c` ("block: move the nonrot flag to queue_limits") says: "Use the chance to switch to defaulting to non-rotational and require the driver to opt into rotational… There are some other drivers that unconditionally set the rotational flag to keep the existing behavior as they arguably can be used on rotational devices even if that is probably not their main use today (e.g. virtio_blk and drbd)." [src] (L24; we confirmed it is in v6.11 and not in v6.10.)

| Driver | Rotational | Write cache and FUA | Source |
|---|---|---|---|
| `sd` (SATA/SAS/USB/iSCSI/…) | "set the default to rotational. All non-rotational devices support the block characteristics VPD page, which will cause this to be updated correctly and any device which doesn't support it should be treated as rotational." Cleared only when the VPD B1 MEDIUM ROTATION RATE is 1 | Write cache set from the mode-page WCE bit. FUA only if the device reports DPOFUA | [src] L16 `sd.c:3818–3824`, `:3498–3503`, `:205–215`, `:3231` |
| NVMe | Only if the namespace reports the NSFEAT rotational bit (`NVME_NS_ROTATIONAL`). Since v6.13; we confirmed it is absent in v6.12 | Write cache and FUA if the controller reports VWC present and the namespace does not report VWC-not-present | [src] L14 `core.c:1708`, `:2476–2482` |
| virtio-blk | **Always** (`.features = BLK_FEAT_ROTATIONAL`) | Write cache per `VIRTIO_BLK_F_CONFIG_WCE`, or, if WCE isn't configurable, `VIRTIO_BLK_F_FLUSH` ("If WCE is not configurable and flush is not available, assume no writeback cache is in use."). **Never FUA**: the driver does not set `BLK_FEAT_FUA` | [src] L18 `:1442`, `:1070–1087` |
| xen-blkfront | Not set, so non-rotational on 6.11+ (Inference from the flag default) | Write cache if the backend has `feature_flush`. FUA if it has `feature_fua` | [src] L21 `:962–966` |
| loop | Only if the backing block device is rotational | Write cache if the backing file has `fsync` | [src] L19 `:992–996` |
| nbd | If the server sets `NBD_FLAG_ROTATIONAL` | Write cache if `NBD_FLAG_SEND_FLUSH`. FUA if `NBD_FLAG_SEND_FUA` | [src] L20 `:335–352` |
| dm | OR of the members | Write cache + FUA if the table supports flush | [src] L22 `dm-table.c:2058` |
| zram, brd | Not set, so non-rotational | — | [src] (no `BLK_FEAT_ROTATIONAL` in `zram_drv.c` or `brd.c`) |

Consequences (Inference):
- `rotational=1` on `vdX` tells you nothing, and `rotational=0` on any virtual disk says nothing about the host's medium.
- `write_cache=write through` on virtio means the hypervisor offered neither WCE nor FLUSH. The guest then sends *no* flushes, and durability depends entirely on the host's cache mode (**UNVERIFIED** for any particular hypervisor).
- A USB bridge that does not pass VPD page B1 through makes an SSD look rotational.

### 2.6 Block-device ioctls (these need an fd on the device node)

`include/uapi/linux/fs.h` [src] (L29):

| ioctl | Encoding | Returns |
|---|---|---|
| `BLKSSZGET` | `_IO(0x12,104)` | logical block size |
| `BLKIOMIN` | `_IO(0x12,120)` | minimum I/O size |
| `BLKIOOPT` | `_IO(0x12,121)` | optimal I/O size |
| `BLKALIGNOFF` | `_IO(0x12,122)` | alignment offset |
| `BLKPBSZGET` | `_IO(0x12,123)` | physical block size |
| `BLKDISCARDZEROES` | `_IO(0x12,124)` | discard-zeroes flag |
| `BLKROTATIONAL` | `_IO(0x12,126)` | rotational flag |
| `BLKGETSIZE64` | `_IOR(0x12,114,size_t)` | device size in bytes |
| `BLKGETDISKSEQ` | `_IOR(0x12,128,__u64)` | disk sequence number |

The open(2) man page points to `BLKSSZGET` for the logical block size [doc] (M2). Opening `/dev/sdX` needs permission on the device node, which is distribution policy (**UNVERIFIED**). The sysfs attributes give the same information without privileges (§2.4).

### 2.7 `statx(2)`: direct-I/O alignment and atomic-write limits

Masks from `include/uapi/linux/stat.h:203–221` [src] (L30). Versions from the man page [doc] (M1).

| Mask | Value | Since | Fields |
|---|---|---|---|
| `STATX_MNT_ID` | 0x00001000 | 5.8 | `stx_mnt_id` |
| `STATX_DIOALIGN` | 0x00002000 | 6.1 | `stx_dio_mem_align`, `stx_dio_offset_align` |
| `STATX_MNT_ID_UNIQUE` | 0x00004000 | 6.8 | unique `stx_mnt_id` |
| `STATX_SUBVOL` | 0x00008000 | 6.10 | `stx_subvol` (btrfs, bcachefs) |
| `STATX_WRITE_ATOMIC` | 0x00010000 | 6.11 | `stx_atomic_write_unit_min/max/max_opt`, `stx_atomic_write_segments_max` |
| `STATX_DIO_READ_ALIGN` | 0x00020000 | 6.14 | `stx_dio_read_offset_align` |

Field semantics [doc] (M1):
- `stx_dio_mem_align`: "The alignment (in bytes) required for user memory buffers for direct I/O (O_DIRECT) on this file, or 0 if direct I/O is not supported on this file."
- `stx_dio_offset_align`: "The alignment (in bytes) required for file offsets and I/O segment lengths for direct I/O (O_DIRECT) on this file, or 0 if direct I/O is not supported on this file. This will only be nonzero if stx_dio_mem_align is nonzero, and vice versa."
- `stx_dio_read_offset_align`: "…for direct I/O reads… If zero, the limit in stx_dio_offset_align applies for reads as well. If non-zero, this value must be smaller than or equal to stx_dio_offset_align…". The version note reads "STATX_DIO_READ_ALIGN (stx_dio_offset_align) is supported by xfs on regular files since Linux 6.14". The field name in that sentence appears to be a typo for `stx_dio_read_offset_align` [sic].
- Support: "STATX_DIOALIGN … is supported on block devices since Linux 6.1. The support on regular files varies by filesystem; it is supported by ext4, f2fs, and xfs since Linux 6.1."
  - In v7.3-rc5, the NFS client also reports it. It was added in **v6.18**: we found it absent at v6.17 and present at v6.18 [src] (L44).
  - btrfs does not report it in v7.3-rc5 [src] (no `dio_mem_align` in `fs/btrfs/inode.c`).
- Atomic writes: "The minimum and maximum sizes (in bytes) supported for direct I/O (O_DIRECT) on the file to be written with torn-write protection. These values are each guaranteed to be a power-of-2." Supported "on block devices since Linux 6.11" and on regular files by "xfs and ext4 since Linux 6.13". `STATX_ATTR_WRITE_ATOMIC` (since 6.11) means "The file supports torn-write protection."
- `STATX_ATTR_DAX` (since 5.8): "The file is in the DAX (cpu direct access) state."

Use: request the mask, then check `stx_mask` for the bits the kernel actually returned. Query the actual file, or a probe file in the same directory, because support "varies by filesystem" (Inference).

### 2.8 `O_DIRECT`

From open(2) [doc] (M2):

- **The flag:** "Try to minimize cache effects of the I/O to and from this file… The O_DIRECT flag on its own makes an effort to transfer data synchronously, but does not give the guarantees of the O_SYNC flag that data and necessary metadata are transferred. To guarantee synchronous I/O, O_SYNC must be used in addition to O_DIRECT."
- **Alignment:** "The O_DIRECT flag may impose alignment restrictions on the length and address of user-space buffers and the file offset of I/Os. In Linux alignment restrictions vary by filesystem and kernel version and might be absent entirely. The handling of misaligned O_DIRECT I/Os also varies; they can either fail with EINVAL or fall back to buffered I/O."
- **Querying alignment:** "Since Linux 6.1, O_DIRECT support and alignment restrictions for a file can be queried using statx(2), using the STATX_DIOALIGN flag." `XFS_IOC_DIOINFO` also exists, but "STATX_DIOALIGN should be used instead when it is available."
- **Historical rules:** "In Linux 2.4, most filesystems based on block devices require that the file offset and the length and memory address of all I/O segments be multiples of the filesystem block size (typically 4096 bytes). In Linux 2.6.0, this was relaxed to the logical block size of the block device (typically 512 bytes)."
- **fork(2):** "O_DIRECT I/Os should never be run concurrently with the fork(2) system call, if the memory buffer is a private mapping… Failure to do so can result in data corruption…". This does not apply to `MAP_SHARED` memory or memory marked `MADV_DONTFORK`.
- **Unsupported filesystems:** `EINVAL`, "The filesystem does not support the O_DIRECT flag." Also: "Some filesystems may not implement the flag, in which case open() fails with the error EINVAL if it is used."
- **Mixing:** "Applications should avoid mixing O_DIRECT and normal I/O to the same file, and especially to overlapping byte regions in the same file… Likewise, applications should avoid mixing mmap(2) of files with direct I/O to the same files."
- **NFS:** "O_DIRECT I/O will bypass the page cache only on the client; the server may still cache the I/O. The client asks the server to make the I/O synchronous… Some servers may also be configured to lie to clients about the I/O having reached stable storage… The Linux NFS client places no alignment restrictions on O_DIRECT I/O."
- The man page's summary: "treat use of O_DIRECT as a performance option which is disabled by default."

Other facts:
- **tmpfs** accepts `O_DIRECT` since **v6.6**, via commit `e88e0d366f9c` "tmpfs: trivial support for direct IO", merged through `v6.6-vfs.tmpfs`. The commit says: "if tmpfs is to stand in for a more sophisticated filesystem, it can be helpful for tmpfs to support O_DIRECT" [src] (L43). So on older kernels, opening tmpfs with `O_DIRECT` fails with `EINVAL` (Inference from the man page's general rule).
- ext4, XFS and btrfs implement direct I/O through `iomap_dio_rw`; ext4 and XFS set `FMODE_CAN_ODIRECT` [src] (L49). This is why the iomap FUA path in §2.10 applies to them. On btrfs, COW overwrites need completion work, so the fast path is not expected except for NOCOW files (Inference).

### 2.9 Durability primitives

**fsync and fdatasync** [doc] (M3):
- "fsync() transfers ("flushes") all modified in-core data of … the file referred to by the file descriptor fd to the disk device … so that all changed information can be retrieved even if the system crashes or is rebooted. This includes writing through or flushing a disk cache if present. The call blocks until the device reports that the transfer has completed."
- "Calling fsync() does not necessarily ensure that the entry in the directory containing the file has also reached disk. For that an explicit fsync() on a file descriptor for the directory is also needed."
- "fdatasync() is similar to fsync(), but does not flush modified metadata unless that metadata is needed in order to allow a subsequent data retrieval to be correctly handled… a change to the file size (st_size, as made by say ftruncate(2)), would require a metadata flush."
- The man page warns that "The fsync() implementations in older kernels and lesser used filesystems do not know how to flush disk caches."

**O_DSYNC and O_SYNC** [doc] (M2):
- O_DSYNC: "By the time write(2) (and similar) return, the output data has been transferred to the underlying hardware, along with any file metadata that would be required to retrieve that data (i.e., as though each write(2) was followed by a call to fdatasync(2))."
- O_SYNC: "…the output data and associated file metadata have been transferred to the underlying hardware (i.e., as though each write(2) was followed by a call to fsync(2))."
- History: "Before Linux 2.6.33, Linux implemented only the O_SYNC flag… O_SYNC was actually implemented as the equivalent of O_DSYNC", and "Linux implements O_SYNC and O_DSYNC, but not O_RSYNC."
- The per-write versions `RWF_DSYNC` and `RWF_SYNC` for `pwritev2` exist since 4.7: "Provide a per-write equivalent of the O_DSYNC open(2) flag … its effect applies only to the data range written by the system call." [doc] (M9)

**How the block layer turns these into device commands** [doc] (L5):
- "The REQ_PREFLUSH flag … will make sure the volatile cache of the storage device has been flushed before the actual I/O operation is started."
- "The REQ_FUA flag … will make sure that I/O completion for this request is only signaled after the data has been committed to non-volatile storage."
- "For devices that do not support volatile write caches … the block layer completes empty REQ_PREFLUSH requests before entering the driver and strips off the REQ_PREFLUSH and REQ_FUA bits from requests that have a payload."
- For blk-mq drivers with a write cache but no FUA: "else a REQ_OP_FLUSH request is sent by the block layer after the completion of the write request for bio submissions with the REQ_FUA bit set."

**Names, directories and renames:**
- rename(2): "If newpath already exists, it will be atomically replaced, so that there is no point at which another process attempting to access newpath will find it missing." [doc] (M8) This is atomicity, not durability. The directory still needs `fsync` (M3).
- `O_TMPFILE` + `linkat(2)` (since 3.11) lets you create a fully written file and then link it into the namespace atomically. Supporting filesystems include ext2/3/4, tmpfs, XFS (3.15) and btrfs (3.16) [doc] (M2).
- Overlayfs documentation: "On traditional local filesystems with a single journal (e.g. ext4, xfs), fsync on a file also persists the parent directory changes, because they are usually modified in the same transaction" [doc] (L39). This describes filesystem behavior; it is not an API guarantee. Inference: mantle should still `fsync` the directory.
- The ext4 `auto_da_alloc` heuristic forces a new file's blocks out before a rename commits in the replace-via-rename pattern [doc] (L36). It is a heuristic for "broken applications". Do not depend on it.

**What an fsync error means** (this is load-bearing):
- fsync(2) EIO: "Since Linux 4.13, errors from write-back will be reported to all file descriptors that might have written the data which triggered the error… Other filesystems (e.g., most local filesystems) will report errors to all file descriptors that were open on the file when the error was recorded." [doc] (M3)
- vfs.rst: "After an error has been reported on one request, subsequent requests on the same file descriptor should return 0, unless further writeback errors have occurred since the previous file synchronization." [doc] (L37) The mechanism is `errseq_t` (L38).
- Inference: **never retry `fsync` and treat success as durability.** Treat the region as lost, rewrite it from mantle's own copy, or crash and recover.
- `syncfs(2)`: "In mainline kernel versions prior to Linux 5.8, syncfs() will fail only when passed a bad file descriptor (EBADF). Since Linux 5.8, syncfs() will also report an error if one or more inodes failed to be written back since the last syncfs() call." Also: "Linux waits for I/O completions, and thus sync() or syncfs() provide the same guarantees as fsync() called on every file in the system or filesystem respectively." [doc] (M10)

**sync_file_range is not a durability primitive.** Quoted verbatim [doc] (M4):

> "This system call is extremely dangerous and should not be used in portable programs. None of these operations writes out the file's metadata. Therefore, unless the application is strictly performing overwrites of already-instantiated disk blocks, there are no guarantees that the data will be available after a crash. There is no user interface to know if a write is purely an overwrite. On filesystems using copy-on-write semantics (e.g., btrfs) an overwrite of existing allocated blocks is impossible. When writing into preallocated space, many filesystems also require calls into the block allocator, which this system call does not sync out to disk. This system call does not flush disk write caches and thus does not provide any data integrity on systems with volatile disk write caches."

The one legitimate use is `SYNC_FILE_RANGE_WRITE` as a writeback hint ("asynchronous flush-to-disk… not suitable for data integrity operations"). `RWF_DONTCACHE` (6.14) is similar: it starts writeback, prunes the page cache, and "is a hint, or best effort" [doc] (M9).

**Mount-level traps:**
- **Overlay `volatile`:** "all forms of sync calls to the upper filesystem are omitted." After a writeback error, "all sync functions will return an error" permanently [doc] (L39). Detect it in the super options in mountinfo and refuse to run a durable mode on it (Inference).
- **ext4 `barrier=0`/`nobarrier`:** "Write barriers enforce proper on-disk ordering of journal commits, making volatile disk write caches safe to use, at some performance penalty. If your disks are battery-backed in one way or another, disabling barriers may safely improve performance." [doc] (L36)
- **XFS** removed `barrier`/`nobarrier` in v4.19 [doc] (L48).

### 2.10 The FUA fast path: exact conditions

`fs/iomap/direct-io.c` [src] (L34), which ext4, XFS and btrfs use for direct I/O (§2.8):

1. For an `O_DSYNC`/`O_SYNC` write, the kernel sets `IOMAP_DIO_NEED_SYNC`. If the write is **not** `IOCB_SYNC`, i.e. it is `O_DSYNC`/`RWF_DSYNC` only, it also sets `IOMAP_DIO_WRITE_THROUGH`: "For datasync only writes, we optimistically try using WRITE_THROUGH for this IO. This flag requires either FUA writes through the device's write cache, or a normal write to a device without a volatile write cache." (`:759–771`)
2. For each mapped extent: "Use a FUA write if we need datasync semantics and this is a pure overwrite that doesn't require any metadata updates. This allows us to avoid cache flushes on I/O completion." In code, the write gets `REQ_FUA` only if all of the following hold (`:489–502`):
   - no completion work is needed (the extent is not `IOMAP_UNWRITTEN`, not `IOMAP_F_NEW` and not `IOMAP_F_SHARED`);
   - the extent is not `IOMAP_F_DIRTY` ("The inode will have uncommitted metadata needed to access any data written. fdatasync is required to commit these changes" [doc] (L35));
   - the device has no write cache, or supports FUA.
3. "If all the writes we issued were already written through to the media, we don't need to flush the cache on IO completion." Otherwise the kernel runs `generic_write_sync` at completion, which is an fdatasync or fsync including the cache flush (`:845–858`, `:150–153`).
4. Documentation drift: the iomap operations document says "IOCB_SYNC: … In the case of pure overwrites, the I/O may be issued with FUA enabled" [doc] (L35). The code only takes the FUA path when `IOCB_SYNC` is *not* set. We follow the code (Inference).

**Torn-write protection.** `RWF_ATOMIC` (since 6.11) [doc] (M9):
- "Torn-write protection means that for a power or any other hardware failure, all or none of the data from the write will be stored, but never a mix of old and new data."
- Size and alignment: "The total write length must be power-of-2 and must be sized in the range [stx_atomic_write_unit_min, stx_atomic_write_unit_max]. The write must be at a naturally-aligned offset within the file with respect to the total write length."
- It needs `O_DIRECT`: "Torn-write protection only works with O_DIRECT flag…"
- For durability it also needs sync: "To guarantee consistency from the write between a file's in-core state with the storage device, O_SYNC or O_DSYNC must be specified."

### 2.11 `fallocate(2)`, unwritten extents, and `FALLOC_FL_WRITE_ZEROES`

Mode bits from `include/uapi/linux/falloc.h` [src] (L31):

| Flag | Value |
|---|---|
| `FALLOC_FL_ALLOCATE_RANGE` | 0x00 |
| `FALLOC_FL_KEEP_SIZE` | 0x01 |
| `FALLOC_FL_PUNCH_HOLE` | 0x02 |
| `FALLOC_FL_NO_HIDE_STALE` | 0x04 (reserved codepoint) |
| `FALLOC_FL_COLLAPSE_RANGE` | 0x08 |
| `FALLOC_FL_ZERO_RANGE` | 0x10 |
| `FALLOC_FL_INSERT_RANGE` | 0x20 |
| `FALLOC_FL_UNSHARE_RANGE` | 0x40 |
| `FALLOC_FL_WRITE_ZEROES` | 0x80 |

Semantics from fallocate(2) [doc] (M5):
- **Default mode (0):** "allocates the disk space within the range … Any subregion within the range … that did not contain data before the call will be initialized to zero… After a successful call, subsequent writes into the range … are guaranteed not to fail because of lack of disk space." Also: "Because allocation is done in block size chunks, fallocate() may allocate a larger range of disk space than was specified."
- **`FALLOC_FL_KEEP_SIZE`:** the file size is not changed. "Preallocating zeroed blocks beyond the end of the file in this manner is useful for optimizing append workloads."
- **`FALLOC_FL_ZERO_RANGE`** (since 3.15; XFS 3.15, ext4 extent files 3.15, SMB3 3.17, btrfs 4.16): "Zeroing is done within the filesystem preferably by converting the range into unwritten extents. This approach means that the specified range will not be physically zeroed out on the device (except for partial blocks at the either end of the range), and I/O is (otherwise) required only to update metadata."
- **`FALLOC_FL_PUNCH_HOLE`** (since 2.6.38) "must be ORed with FALLOC_FL_KEEP_SIZE". Supported on XFS (2.6.38), ext4 (3.0), btrfs (3.7), tmpfs (3.5) and gfs2 (4.16).
- **`FALLOC_FL_UNSHARE_RANGE`:** "shared file data extents will be made private to the file to guarantee that a subsequent write will not fail due to lack of space."
- **Errors:**
  - `EOPNOTSUPP`: the filesystem does not support the mode.
  - `ENOSYS`: no fallocate at all.
  - `EINVAL`: bad or misaligned collapse or insert range.
  - `EBADF`: not opened for writing.
- **`posix_fallocate(3)`:** when the filesystem lacks fallocate, glibc emulates it. "The emulation is inefficient", and it has races where "concurrent writes from another thread or process could be overwritten with null bytes." [doc] (M6)

**Unwritten extents and the cost of converting them:**
- ext4 on-disk format: "If the value of this field is <= 32768, the extent is initialized. If the value of the field is > 32768, the extent is uninitialized and the actual extent length is ee_len - 32768." [doc] (L36 ifork)
- iomap: "IOMAP_UNWRITTEN: The file range maps to specific space on the storage device, but the space has not yet been initialized… Reads from this type of mapping will return zeroes to the caller. For a write or writeback operation, the ioend should update the mapping to MAPPED." The `end_io` hook "should perform post-write conversions of unwritten extent mappings" [doc] (L35).
- In the direct-I/O path, an unwritten extent sets `IOMAP_DIO_UNWRITTEN` and `need_zeroout`, and it forces completion work. That excludes the FUA fast path (§2.10) [src] (L34 `:451–453`, `:494–502`).
- So the first `O_DSYNC` write into a preallocated but unwritten region costs a metadata transaction plus a cache flush, instead of one FUA write (Inference from the cited code and `generic_write_sync`).
- ext4's `dioread_nolock` option "will allocate uninitialized extent before buffer write and convert the extent to initialized after IO completes" [doc] (L36). Note that the ext4 document still describes the default as `dioread_lock`. We have not verified whether that is current, so it is **UNVERIFIED**.

**`FALLOC_FL_WRITE_ZEROES`: preallocation with written extents.**
- falloc.h: "zeroes a specified file range in such a way that subsequent writes to that range do not require further changes to the file mapping metadata. This flag is beneficial for subsequent pure overwriting within this range… filesystems that always require out-of-place writes should not support this flag… This flag cannot be specified in conjunction with the FALLOC_FL_KEEP_SIZE." [src] (L31)
- Commit `7bd43cc79cab`: "we can use this command to quickly preallocate a real all-zero file with written extents… should greatly improve overwrite performance on certain filesystems." It points users to `queue/write_zeroes_unmap_max_hw_bytes` to see whether the device accelerates the operation [src] (L32).
- Versions, checked at release tags [src] (L32):
  - The flag and the ext4 and block-device support first appear in **v6.17**.
  - **XFS** support (`xfs_falloc_write_zeroes`) is present in v7.3-rc5 but absent in v7.2. That means it is expected in 7.3.
  - btrfs does not support it, because it is copy-on-write.
- The fallocate(2) page in man-pages 6.19 does not document this flag yet.

**Verifying the result.** `FS_IOC_FIEMAP` reports `FIEMAP_EXTENT_UNWRITTEN`: "the extent is allocated but its data has not been initialized. This indicates the extent's data will be all zero if read through the filesystem but the contents are undefined if read directly from the device." [doc] (L40)

### 2.12 `statfs` `f_type` magic numbers

Values from `include/uapi/linux/magic.h` [src] (L33); the statfs(2) list agrees [doc] (M7).

| Filesystem | Constant | Value |
|---|---|---|
| tmpfs | `TMPFS_MAGIC` | 0x01021994 |
| ramfs | `RAMFS_MAGIC` | 0x858458f6 |
| NFS | `NFS_SUPER_MAGIC` | 0x6969 |
| overlayfs | `OVERLAYFS_SUPER_MAGIC` | 0x794c7630 |
| btrfs | `BTRFS_SUPER_MAGIC` | 0x9123683E |
| XFS | `XFS_SUPER_MAGIC` | 0x58465342 ("XFSB") |
| ext2/ext3/ext4 | `EXT4_SUPER_MAGIC` (= EXT2 = EXT3) | 0xEF53 |
| f2fs | `F2FS_SUPER_MAGIC` | 0xF2F52010 |
| bcachefs | `BCACHEFS_SUPER_MAGIC` | 0xca451a4e |
| FUSE (including virtiofs, which is built on FUSE: `virtio_fs.c:1604` calls `fuse_fill_super_common`, which calls `fuse_sb_defaults`, which sets `sb->s_magic = FUSE_SUPER_MAGIC` [src] `fs/fuse/inode.c:1602, 1724–1738`) | `FUSE_SUPER_MAGIC` | 0x65735546 |
| CIFS client, SMB1 dialect | `CIFS_SUPER_MAGIC` | 0xFF534D42 [src] `fs/smb/client/smb1ops.c:1179` |
| CIFS client, SMB2/3 dialects | `SMB2_SUPER_MAGIC` | 0xFE534D42 [src] `fs/smb/client/smb2ops.c:3135` |
| Legacy smbfs | `SMB_SUPER_MAGIC` | 0x517B |
| 9p (v9fs) | `V9FS_MAGIC` | 0x01021997 |
| Ceph | `CEPH_SUPER_MAGIC` | 0x00c36400 |
| exFAT | `EXFAT_SUPER_MAGIC` | 0x2011BAB0 |
| FAT (msdos/vfat) | `MSDOS_SUPER_MAGIC` | 0x4d44 |
| NTFS (legacy driver, per the man page) | `NTFS_SB_MAGIC` | 0x5346544e. The value reported by `ntfs3` is **UNVERIFIED** |
| zonefs | `ZONEFS_MAGIC` | 0x5a4f4653 |
| OpenZFS (out of tree) | `ZFS_SUPER_MAGIC` | 0x2fc12fc1. From OpenZFS `include/sys/fs/zfs.h:1561` (O1), which is outside the owner's list of allowed sources |

Notes:
- ext2, ext3 and ext4 share one magic number. Use mountinfo field (9) to tell them apart.
- `f_type` has type `__fsword_t`. Compare the low 32 bits, because values such as 0xFF534D42 do not fit in a signed 32-bit integer (Inference).

### 2.13 io_uring: versions, privileges and runtime detection

| Feature | Since | Source |
|---|---|---|
| `io_uring_setup/enter/register`, the opcodes `NOP, READV, WRITEV, FSYNC (IORING_FSYNC_DATASYNC), READ_FIXED, WRITE_FIXED, POLL_ADD, POLL_REMOVE`, the setup flags `IORING_SETUP_IOPOLL/SQPOLL/SQ_AFF`, and `IORING_REGISTER_BUFFERS/FILES` | 5.1 | [src] `include/uapi/linux/io_uring.h` at v5.1; absent at v5.0 |
| `IORING_OP_SYNC_FILE_RANGE` and `IOSQE_IO_DRAIN` | 5.2 | [doc] M14 |
| `IOSQE_IO_LINK` | 5.3 | [doc] M14 |
| `IORING_OP_READ/WRITE`, `FALLOCATE`, `OPENAT`, `CLOSE`, `STATX` and `IORING_REGISTER_PROBE` | 5.6 | [doc] M14, M15 |
| `IORING_FEAT_SQPOLL_NONFIXED`, `RENAMEAT` and `UNLINKAT` | 5.11 | [doc] M13, M14 |
| `MKDIRAT`, `SYMLINKAT` and `LINKAT` | 5.15 | [doc] M14 |
| `IORING_OP_FTRUNCATE` | 6.9 | [doc] M14 |
| `IORING_OP_WRITEV_FIXED` | 6.15 | [doc] M14 |
| `kernel.io_uring_disabled` / `io_uring_group` sysctls | 6.6 | [doc] L41; we confirmed absent at v6.5 and present at v6.6 |

Key semantics:
- **SQPOLL privileges:** "Before version 5.11 of the Linux kernel, to successfully use this feature, the application must register a set of files … In version 5.11 and later, it is no longer necessary to register files… 5.11 also allows using this as non-root, if the user has the CAP_SYS_NICE capability. In 5.13 this requirement was also relaxed, and no special privileges are needed for SQPOLL in newer kernels." [doc] (M13)
- **IOPOLL:** "usable only on a file descriptor opened using the O_DIRECT flag… the storage device must be configured for polling… For NVMe devices, the nvme driver must be loaded with the poll_queues parameter set…" [doc] (M13). Check `queue/io_poll` (§2.4).
- **Registered buffers:** "locked in memory and charged against the user's RLIMIT_MEMLOCK resource limit… there is a size limit of 1GiB per buffer… the buffers must be anonymous, non-file-backed memory… Note that before 5.13 registering buffers would wait for the ring to idle." [doc] (M15)
- **`IORING_OP_FSYNC` does not wait for earlier writes:** "an application which places a write I/O followed by an fsync in the submission queue cannot expect the fsync to apply to the write. The two operations execute in parallel… To enforce ordering one may utilize linked SQEs, IOSQE_IO_DRAIN or wait for the arrival of CQEs…" [doc] (M14). With `IOSQE_IO_LINK`, "A chain of SQEs will be broken if any request in that chain ends in error… the remaining unstarted part of the chain will be terminated and completed with -ECANCELED" (5.3).
- **Per-I/O sync flags:** `IORING_OP_WRITE`/`WRITEV` carry `rw_flags`. We infer that `RWF_DSYNC` and `RWF_ATOMIC` apply there as they do for `pwritev2`; the man page defers to `pwritev2` semantics (M14). Treat this as **UNVERIFIED** until tested.

**Detecting whether io_uring is usable:**
- `ENOSYS`: the kernel was built without `CONFIG_IO_URING`. The syscalls are `COND_SYSCALL(io_uring_setup)` and friends, which route to `sys_ni_syscall`, which returns `-ENOSYS` [src] (L42 `kernel/sys_ni.c:51–53`). The Kconfig entry is `bool "Enable IO uring support" if EXPERT`, `default y` [src] (L42 `init/Kconfig:1960`).
- `EPERM`: "/proc/sys/kernel/io_uring_disabled has the value 2, or it has the value 1 and the calling process does not hold the CAP_SYS_ADMIN capability or is not a member of /proc/sys/kernel/io_uring_group." [doc] (M13) The sysctl semantics are: "0 All processes can create io_uring instances as normal… 1 io_uring creation is disabled (io_uring_setup() will fail with -EPERM) for unprivileged processes not in the io_uring_group group… 2 io_uring creation is disabled for all processes." [doc] (L41) The check is implemented in `io_uring_allowed()`, which then consults the LSM hook [src] (L42 `io_uring/io_uring.c:3114`).
  - What errno an LSM denial produces is **UNVERIFIED**.
  - Container seccomp profiles can also block these syscalls. Which errno they return depends on the runtime and is **UNVERIFIED**.
- After setup succeeds, read `io_uring_params.features` (`IORING_FEAT_*`) and call `IORING_REGISTER_PROBE`: "If the flags field has IO_URING_OP_SUPPORTED set, then this opcode is supported on the running kernel. Available since 5.6." [doc] (M15)
- Error convention: the liburing man page says "On error, a negative error code is returned. The caller should not rely on errno variable." (M13). That describes the liburing wrapper. A raw `syscall(2)` call returns -1 and sets errno (Inference; standard syscall ABI).

### 2.14 Virtualization hints from DMI

`/sys/class/dmi/id/{sys_vendor, product_name, board_vendor, bios_vendor, chassis_vendor}` are mode 0444. `product_serial` and `product_uuid` are 0400, so root only [src] (L47 `drivers/firmware/dmi-id.c:41–58`). The strings come from firmware. Mapping particular strings to particular hypervisors is **UNVERIFIED**; build that table empirically.

---

## 3. macOS

### 3.1 From a path to its filesystem and BSD device

- **`statfs(2)`/`fstatfs(2)`** (64-bit-inode layout) [doc] (A3):
  - `f_bsize` is the "fundamental file system block size".
  - `f_iosize` is the "optimal transfer block size".
  - The struct also has `f_fstypename`, `f_mntonname` ("directory on which mounted"), `f_mntfromname` ("mounted filesystem") and `f_flags`.
  - "Fields that are undefined for a particular file system are set to -1."
  - Flags include `MNT_LOCAL` ("File system is stored locally") and `MNT_RDONLY`.
- **Flag values** [src] (A7 `mount.h`): `MNT_RDONLY` 0x1, `MNT_SYNCHRONOUS` 0x2, `MNT_LOCAL` 0x00001000, `MNT_ROOTFS` 0x00004000, `MNT_DONTBROWSE` 0x00100000, `MNT_JOURNALED` 0x00800000, `MNT_SNAPSHOT` 0x40000000.
- **Observed on the research machine** [obs]:
  - `statfs` on `/Users/adalundhe/Projects/mantle` returned `f_fstypename="apfs"`, `f_mntfromname="/dev/disk3s5"` and `f_mntonname="/System/Volumes/Data"`. The `/Users` firmlink resolves into the Data volume.
  - The same call returned `f_bsize=4096`, `f_iosize=1048576`, and `MNT_LOCAL` set.
  - `statfs("/")` returned `/dev/disk3s1s1`, the sealed system snapshot.
  - `/dev` (devfs) returned 512/512.
- **Alternative route through `st_dev`** [obs]:
  - A file's `st_dev` identified the volume's BSD node: `stat -f "%Sd %d"` printed `disk3s5 16777234`, which is major 1, minor 18.
  - The IOMedia object for `disk3s5` carries `"BSD Major"=1` and `"BSD Minor"=18`.
- **Network and userspace filesystems:** `f_mntfromname` does not start with `/dev/`, and `MNT_LOCAL` is clear for network filesystems (Inference from the flag's definition). The man page does not enumerate `f_fstypename` values such as `smbfs`, `nfs` or `webdav`, so that list is **UNVERIFIED**.

### 3.2 From the BSD name to IOKit and the device characteristics

APIs (A19; the SDK `IOKitLib.h`):
- **`IOBSDNameMatching(mainPort, options, bsdName)`** "Create[s] a matching dictionary that specifies an IOService match based on BSD device name"; "No options are currently defined". The implementation just returns `{"BSD Name": name}` [src] (A19 `IOKitLib.c:1086–1095`).
- **`IOServiceGetMatchingService(mainPort, matching)`** "Look[s] up a registered IOService object that matches a matching dictionary". It consumes one reference to `matching`, and the caller must release the returned service.
- **`kIOMainPortDefault`** (macOS 12+): "When specifying a main port to IOKit functions, the NULL argument indicates "use the default". This is a synonym for NULL…". `kIOMasterPortDefault` is its deprecated name, deprecated since macOS 12.0 [doc] (A23). Passing 0 works on every version (Inference from "synonym for NULL").
- **`IORegistryEntrySearchCFProperty(entry, plane, key, allocator, options)`:** "This function will search for a property, starting first with specified registry entry's property table, then iterating recusively [sic] through either the parent registry entries or the child registry entries of this entry. Once the first occurrence is found, it will lookup and return the value of the property… kIORegistryIterateRecursively may be set to recurse automatically into the registry hierarchy. Without this option, this method degenerates into the standard IORegistryEntryCreateCFProperty() call. kIORegistryIterateParents may be set to iterate the parents of the entry, in place of the children." Use `kIOServicePlane`, with `kIORegistryIterateRecursively` = 0x1 and `kIORegistryIterateParents` = 0x2 [doc] (A19, A23).

**The kernel does the same walk.** `DKIOCISSOLIDSTATE` reads `"Device Characteristics"` → `"Medium Type"` with `getProperty(key, gIOServicePlane)` starting from the IOMedia, and returns true only when the value equals `"Solid State"` [src] (A17 `IOMediaBSDClient.cpp:2175–2196`). `IOBlockStorageDriver` sets its `_solidState` flag the same way [src] (A18 `IOBlockStorageDriver.cpp:1320–1330`).

**APFS volumes are IOMedia objects.** `ioreg -c IOMedia` lists `AppleAPFSVolume` and `AppleAPFSMedia` objects [obs]. Observed service-plane chain for the Data volume [obs]:

```
IOEmbeddedNVMeBlockDevice "NS_01@1"   <- carries "Device Characteristics", "Protocol Characteristics"
 └ IOBlockStorageDriver
    └ IOMedia "APPLE SSD AP8192Z Media"             (disk0)
       └ IOGUIDPartitionScheme
          └ IOMedia "Container@2"                   (disk0s2 = APFS Physical Store)
             └ AppleAPFSContainerScheme
                └ AppleAPFSMedia                    (disk3 = synthesized container disk)
                   └ AppleAPFSContainer
                      └ AppleAPFSVolume "Data@5"    (disk3s5)  <- f_mntfromname
```

`diskutil info /` reports "APFS Container: disk3" and "APFS Physical Store: disk0s2" [obs]. A parent search starting at `disk3s5` therefore reaches the NVMe block device.

**Containers with several physical stores (Fusion):** the search returns only the *first* match, and which member that would be is **UNVERIFIED**. To see every backing device, iterate all parents with `IORegistryEntryGetParentIterator` and collect each `IOBlockStorageDevice` ancestor (Inference).

### 3.3 Exact registry key strings

**Device Characteristics** (`kIOPropertyDeviceCharacteristicsKey` = `"Device Characteristics"`) [src] (A13):
- `"Vendor Name"`, `"Product Name"`, `"Product Revision Level"`, `"Serial Number"`.
- `"Medium Type"`, which is *"Requirement: Optional"*. Its values are `kIOPropertyMediumTypeRotationalKey` = `"Rotational"` and `kIOPropertyMediumTypeSolidStateKey` = `"Solid State"`.
- `"Rotation Rate"` (RPM; optional).
- `"Physical Block Size"`, `"Logical Block Size"` (required "for hard disk drives with logical block size other than 512 bytes or that does not match its physical block size"), and `"Bytes per Physical Sector"`.
- `"Target Disk Mode"`, `"Invalid Startup Disk"`, and the optical-media feature keys.

**Protocol Characteristics** (`"Protocol Characteristics"`; *Mandatory*) [src] (A14):
- `"Physical Interconnect"` (*Mandatory*). Its values:
  - `"ATA"`, `"SATA"`, `"SAS"`, `"ATAPI"`
  - `"USB"`, `"FireWire"`, `"Secure Digital"`
  - `"SCSI Parallel Interface"`, `"Fibre Channel Interface"`
  - `"Virtual Interface"`
  - `"PCI"`, `"PCI-Express"`
  - `"Apple Fabric"`
- `"Physical Interconnect Location"` (*Mandatory*). Its values are `"Internal"`, `"External"`, `"Internal/External"`, and:
  - `"File"`, whose header example pairs it with `"Virtual Interface"`;
  - `"RAM"`, also paired with `"Virtual Interface"` in the header example.

  Inference: `File` identifies a disk image and `RAM` a RAM disk.
- `"Multiple Initiators"` and the SCSI identifier keys.

**IOStorageFeatures** (`"IOStorageFeatures"`, a dictionary) [src] (A15):
- `"Barrier"`: "the ability of the storage stack to honor a write barrier, guaranteeing that on power loss, writes after the barrier will not be visible until all writes before the barrier are visible".
- `"Force Unit Access"`, `"Priority"`, `"Unmap"`.
- The header describes this dictionary as "typically defined in the device object below the block storage driver object".

**Transfer limits** (`iokit/IOKit/IOKitKeys.h`) [src] (A12): `"IOMaximumBlockCountRead"`/`"…Write"`, `"IOMaximumByteCountRead"`/`"…Write"`, `"IOMaximumSegmentCountRead"`/`"…Write"`, `"IOMaximumSegmentByteCountRead"`/`"…Write"`, `"IOMinimumSegmentAlignmentByteCount"`, and `"IOMaximumPriorityCount"`.

**IOMedia keys** [src] (A16): `"Content"`, `"Content Hint"`, `"Ejectable"`, `"Leaf"`, `"Open"`, `"Preferred Block Size"`, `"Removable"`, `"Size"`, `"UUID"`, `"Whole"`, `"Writable"`. Also `"BSD Name"`, `"BSD Major"`, `"BSD Minor"` and `"BSD Unit"` [obs].

**IOBlockStorageDevice:** `"WriteCacheState"` (`kIOBlockStorageDeviceWriteCacheStateKey`) with `getWriteCacheState` and `setWriteCacheState`, plus `"device-type"` [src] (A18 `IOBlockStorageDevice.h:45–90`). This property did not appear on the observed NVMe device.

### 3.4 Values observed on the research machine [obs]

| Registry location | Key | Value |
|---|---|---|
| NVMe block device | `Device Characteristics` | `{"Medium Type"="Solid State", "Product Name"="APPLE SSD AP8192Z", "Product Revision Level"="2973.100", "Vendor Name"="", "Serial Number"=…}` |
| NVMe block device | `Protocol Characteristics` | `{"Physical Interconnect"="Apple Fabric", "Physical Interconnect Location"="Internal"}` |
| NVMe block device (top level, *not* inside Device Characteristics) | `Physical Block Size`, `Logical Block Size` | 4096, 4096 |
| NVMe block device | `IOStorageFeatures` | `{"Unmap"=Yes, "Priority"=Yes, "Barrier"=Yes}`. There is no `"Force Unit Access"` key |
| NVMe block device | `IOMaximumByteCountRead/Write`, `IOMinimumSegmentAlignmentByteCount` | 1048576, 4096 |
| AppleAPFSVolume disk3s5 | `Preferred Block Size` | 4096 |
| `sysctl` | `hw.pagesize` / `vm.pagesize` | 16384 |
| `diskutil info /` | Protocol, Solid State, Device Block Size | "Apple Fabric", "Yes", "4096 Bytes" |

### 3.5 `sys/disk.h` ioctls and the privileges they need

Public ioctls in `bsd/sys/disk.h` [src] (A6):

| ioctl | Encoding | Notes |
|---|---|---|
| `DKIOCGETBLOCKSIZE` | `_IOR('d',24,uint32_t)` | |
| `DKIOCGETBLOCKCOUNT` | `_IOR('d',25,uint64_t)` | |
| `DKIOCGETPHYSICALBLOCKSIZE` | `_IOR('d',77,uint32_t)` | |
| `DKIOCGETMAXBLOCKCOUNTREAD`/`WRITE` | `'d',64/65,uint64_t` | |
| `DKIOCGETMAXBYTECOUNTREAD`/`WRITE` | `'d',70/71,uint64_t` | |
| `DKIOCGETMAXSEGMENTCOUNTREAD`/`WRITE` | `'d',66/67` | |
| `DKIOCGETMAXSEGMENTBYTECOUNTREAD`/`WRITE` | `'d',68/69` | |
| `DKIOCGETMINSEGMENTALIGNMENTBYTECOUNT` | `'d',74` | |
| `DKIOCGETMAXSEGMENTADDRESSABLEBITCOUNT` | `'d',75` | |
| `DKIOCGETFEATURES` | `_IOR('d',76,uint32_t)` | `DK_FEATURE_BARRIER` 0x2, `DK_FEATURE_PRIORITY` 0x4, `DK_FEATURE_UNMAP` 0x10 |
| `DKIOCGETCOMMANDPOOLSIZE` | `'d',78` | |
| `DKIOCSYNCHRONIZE` | `_IOW('d',22,dk_synchronize_t)` | Option `DK_SYNCHRONIZE_OPTION_BARRIER` 0x2 |
| `DKIOCGETLOCATION` | `'d',33` | `DK_LOCATION_INTERNAL` 0, `DK_LOCATION_EXTERNAL` 1 |
| `DKIOCUNMAP` | `'d',31` | |
| `DKIOCGETPROVISIONSTATUS` | `'d',79` | |

**Kernel-only** definitions (inside `#ifdef KERNEL`, so the public header does not give them to user space): `DK_FEATURE_FORCE_UNIT_ACCESS` 0x1, `DKIOCISSOLIDSTATE`, `DKIOCISVIRTUAL`, `DKIOCGETIOMINSATURATIONBYTECOUNT`, `DKIOCSETBLOCKSIZE` [src] (A6).

**Privileges:**
- These ioctls operate on `/dev/diskN` or `/dev/rdiskN`. IOMediaBSDClient creates both nodes with owner `UID_ROOT`, group `GID_OPERATOR` and mode 0640. The media's `owner-uid`, `owner-gid` and `owner-mode` properties can override this [src] (A17 `IOMediaBSDClient.cpp:3638–3666`).
- On the research machine the nodes are `brw-r----- root operator` [obs], and the (admin) user is not in the `operator` group [obs]. So the ioctl route needs root in practice (Inference).
- `DKIOCGETPHYSICALBLOCKSIZE` just returns the `"Physical Block Size"` registry property, found by the same parent search, or else the media's preferred block size [src] (A17 `:1486–1500`). The unprivileged registry read (§3.3) therefore gives the same answer.

### 3.6 `statfs` `f_bsize` versus `f_iosize`

- `f_bsize` is the "fundamental file system block size", and `f_iosize` is the "optimal transfer block size" [doc] (A3).
- APFS on the research machine reports 4096 and 1 MiB [obs].
- Inference: treat `f_iosize` as a hint for sequential chunk size, not as an alignment requirement.

### 3.7 `fcntl` commands

Numeric values from the SDK 26.4 `sys/fcntl.h` [doc] (A5, A23):

| Command | Value |
|---|---|
| `F_PREALLOCATE` | 42 |
| `F_SETSIZE` | 43 |
| `F_RDADVISE` | 44 |
| `F_RDAHEAD` | 45 |
| `F_NOCACHE` | 48 |
| `F_LOG2PHYS` | 49 |
| `F_FULLFSYNC` | 51 |
| `F_GLOBAL_NOCACHE` | 55 |
| `F_NODIRECT` | 62 |
| `F_LOG2PHYS_EXT` | 65 |
| `F_BARRIERFSYNC` | 85 |
| `F_PUNCHHOLE` | 99 |
| `F_TRANSFEREXTENTS` | 110 |
| `F_NOCACHE_EXT` | 112 (macOS 26 SDK; absent from the 15.4 SDK) |

`fstore_t` flags: `F_ALLOCATECONTIG` 0x2, `F_ALLOCATEALL` 0x4, `F_ALLOCATEPERSIST` 0x8. Position modes: `F_PEOFPOSMODE` 3, `F_VOLPOSMODE` 4.

**`F_FULLFSYNC`** [doc] (A1):

> "Does the same thing as fsync(2) then asks the drive to flush all buffered data to the permanent storage device (arg is ignored). As this drains the entire queue of the device and acts as a barrier, data that had been fsync'd on the same device before is guaranteed to be persisted when this call returns. This is currently implemented on HFS, MS-DOS (FAT), Universal Disk Format (UDF) and APFS file systems. The operation may take quite a while to complete. Certain FireWire drives have also been known to ignore the request to flush their buffered data."

- xnu passes the command to the filesystem as `VNOP_IOCTL(vp, F_FULLFSYNC, …)`. The source comment reads "fsync + flush the journal + DKIOCSYNCHRONIZE" [src] (A8 `kern_descrip.c:3960–3990`).
- HFS turns it into `hfs_fsync(…, HFS_FSYNC_FULL)` → `hfs_flush(HFS_FLUSH_FULL)`, which runs `journal_flush(…, JOURNAL_FLUSH_FULL)`. A non-journaled HFS volume does `hfs_metasync_all` + `DKIOCSYNCHRONIZE` [src] (A20).
- APFS's implementation is closed source. Only the documented behavior above can be relied on.

**`F_BARRIERFSYNC`** [doc] (A1):

> "Does the same thing as fsync(2) then issues a barrier command to the drive (arg is ignored). The barrier applies to I/O that have been flushed with fsync(2) on the same device before. These operations are guaranteed to be persisted before any other I/O that would follow the barrier, although no assumption should be made on what has been persisted or not when this call returns… This is typically useful to guarantee valid state on disk when ordering is a concern but durability is not… execute operations of phase one, then fsync(2) each FD and issue a single barrier. Finally execute operations of phase two. This is currently implemented on HFS and APFS. It requires hardware support, which Apple SSDs are guaranteed to provide."

- **Kernel fallback** [src] (A8 `:3970–3988`): if the filesystem returns `ENOTTY`, `ENOTSUP` or `EINVAL`, xnu re-issues the call as `F_FULLFSYNC` and remembers the fallback for that mount (`MNTK_SUPL_USE_FULLSYNC`). So calling `F_BARRIERFSYNC` is always safe; at worst it costs as much as `F_FULLFSYNC`.
- HFS implements it as a journal flush plus `DKIOCSYNCHRONIZE` with `DK_SYNCHRONIZE_OPTION_BARRIER`. When the device lacks the barrier feature, HFS falls back to a full flush [src] (A20 `hfs_vfsutils.c:3483–3540`).

**`F_NOCACHE`:** "Turns data caching off/on. A non-zero value in arg turns data caching off. A value of zero in arg turns data caching on." [doc] (A1)
- It sets `FNOCACHE` on the *open file description*, not on the vnode [src] (A8 `:3699–3718`). `F_GLOBAL_NOCACHE` (55) applies to the vnode.
- `F_NOCACHE_EXT` (112, SDK 26): "turn data caching off/on for this fd and relax size and alignment restrictions for write". The kernel accepts it only on regular files [src] (A8, A23).
- `F_NODIRECT` (62): "used in conjunction with F_NOCACHE to indicate that DIRECT, synchonous [sic] writes…". The header comment is truncated in the SDK [doc] (A23).

**Does page-aligned I/O bypass the cache?** In the generic VFS cluster layer, yes, under these conditions:
- Writes take the direct (uncached) path only when `(flags & (IO_NOCACHE | IO_NODIRECT)) == IO_NOCACHE` and the buffer is in user space [src] (A10 `vfs_cluster.c:3026`).
- The first vector must be at least `MIN_DIRECT_WRITE_SIZE` (16384). With `F_NOCACHE_EXT` or the equivalent opt-in (`IO_NOCACHE_SWRITE`), the minimum drops to the filesystem block size [src] (A10 `:282`, `:3026–3035`, `:6245–6286`; A9 `vfs_vnops.c:1339–1350`).
- The file offset must be aligned to `PAGE_MASK`, which is 16 KiB on Apple Silicon [obs `hw.pagesize`]. The buffer must be aligned to the mount's memory alignment mask and to the device block size. Otherwise: "one of the 2 important offsets is misaligned so fire an I/O through the cache for this entire vector" [src] (A10 `:3262–3280`).
- A remainder smaller than a page also goes through the cache (Inference from the loop condition `io_req_size >= PAGE_SIZE`).
- **Whether APFS sends writes through `cluster_write` at all is UNVERIFIED**, because APFS is closed source. The read path (`cluster_read_direct`) was not read in detail and is **UNVERIFIED**. Measure it.

**`F_PREALLOCATE`** [doc] (A1): "Preallocate file storage space. Note: upon success, the space that is allocated can be the size requested, larger than the size requested, or (if the F_ALLOCATEALL flag is not provided) smaller than the space requested."
- The argument is `fstore_t {u_int32_t fst_flags; int fst_posmode; off_t fst_offset; off_t fst_length; off_t fst_bytesalloc /* OUT */}`.
- Flags: `F_ALLOCATECONTIG` "Allocate contiguous space. (Note that the file system may ignore this request if fst_length is very large.)"; `F_ALLOCATEALL` "Allocate all requested space or no space at all"; `F_ALLOCATEPERSIST` "Allocate space that is not freed when close(2) is called. (Note that the file system may ignore this request.)"
- Position modes: `F_PEOFPOSMODE` "Allocate from the physical end of file. In this case, fst_length indicates the number of newly allocated bytes desired." `F_VOLPOSMODE` "Allocate from the volume offset."
- Errors:
  - `EBADF` if the fd lacks write permission.
  - `EFBIG`.
  - `EINVAL` if "F_PEOFPOSMODE is set and fst_offset is a non-zero value, or when F_VOLPOSMODE is set and fst_offset is a negative or zero value".
  - `ENOSPC`.
- The kernel implements it through `VNOP_ALLOCATE` [src] (A8 `:3371–3460`).
- Neither the man page nor the code path we read says whether the logical file size changes. Inference: it does not, because this is physical-EOF allocation and `F_SETSIZE`/`ftruncate` are separate operations. Set the size with `ftruncate` and verify.

**`F_PUNCHHOLE`** [doc] (A1): "Deallocate a region and replace it with a hole. Subsequent reads of the affected region will return bytes of zeros that are usually not backed by physical blocks. This will not change the actual file size. Holes must be aligned to file system block boundaries. This will fail on file systems that do not support this interface."
- The argument is `fpunchhole_t {u_int32_t fp_flags; u_int32_t reserved; off_t fp_offset; off_t fp_length;}`.
- Errors:
  - `EPERM` without write permission.
  - `EINVAL` for negative values, misalignment, or non-zero `fp_flags`/`reserved`.
  - `ENOSPC`: "a filesystem that supports cloned files may return this error if punching a hole requires the creation of a clone".

**`F_RDAHEAD`:** "Turn read ahead off/on. A zero value in arg disables read ahead. A non-zero value in arg turns read ahead on." [doc] (A1) It sets `FNORDAHEAD`, which becomes `IO_RAOFF` [src] (A8 `:3687`; A9).

**`F_LOG2PHYS` / `F_LOG2PHYS_EXT`:** "Get disk device information. Currently this only returns the disk device address that corresponds to the current file offset. Note that the system may return -1 as the disk device address if the file is not backed by physical blocks. This is subject to change." The `_EXT` variant takes a file offset and length as inputs and returns `l2p_contigbytes`. The fd must be open for reading [doc] (A1). This is the tool for §3.10.

### 3.8 `fsync`, `fdatasync`, `O_SYNC` and `O_DSYNC` on macOS

- **fsync(2)** [doc] (A2): "Note that while fsync() will flush all data from the host to the drive (i.e. the "permanent storage device"), the drive itself may not physically write the data to the platters for quite some time and it may be written in an out-of-order sequence. Specifically, if the drive loses power or the OS crashes, the application may find that only some or none of their data was written. The disk drive may also re-order the data so that later writes may be present, while earlier writes are not. This is not a theoretical edge case. This scenario is easily reproduced with real world workloads and drive power failures." It continues: "Applications, such as databases, that require a strict ordering of writes should use F_FULLFSYNC…"
- **xnu internals** [src] (A11 `vfs_syscalls.c:8943–8999`):
  - `fsync` → `fsync_common(…, MNT_WAIT)` → `VNOP_FSYNC`.
  - `fdatasync` is syscall 187 → `fsync_common(…, MNT_DWAIT)`.
- **Declaring `fdatasync`:** `libsystem_kernel` exports `_fdatasync` [obs `nm`], and the SDK defines `SYS_fdatasync 187` in `sys/syscall.h`. But no SDK header declares `fdatasync()` [obs grep of SDK 26.4], so declare it yourself.
- Neither `fsync` nor `fdatasync` asks the drive to flush (Inference from the man page: only `F_FULLFSYNC` does).
- **`O_DSYNC`** (0x00400000) is handled like `O_FSYNC`/`O_SYNC`: "XXX We treat O_DSYNC as O_FSYNC for now, since we can not delay the non-essential metadata without some additional VFS work", and both become `IO_SYNC` [src] (A9 `vfs_vnops.c:1360–1370`). What `IO_SYNC` does to the *drive* cache is up to each filesystem. For APFS it is **UNVERIFIED**, so assume there is no drive flush.

### 3.9 Directory entries and rename durability

- **rename(2)** [doc] (A4): "The rename() system call guarantees that an instance of new will always exist, even if the system should crash in the middle of the operation." This guarantees atomicity. It does not say the rename is on stable storage when the call returns. `RENAME_SWAP` and `RENAME_EXCL` (`renamex_np`/`renameatx_np`) depend on `VOL_CAP_INT_RENAME_SWAP` (0x00040000) and `VOL_CAP_INT_RENAME_EXCL` [doc] (A4, A23 `sys/attr.h`).
- **HFS:** `fsync` on a directory vnode goes straight to metadata sync ("HFS directories don't have any data blocks"). The `F_FULLFSYNC` handler accepts any vnode [src] (A20 `hfs_vnops.c:3120`; `hfs_readwrite.c:2619–2632`).
- **APFS:** whether `fsync` or `F_FULLFSYNC` on a *directory* fd makes a rename durable is **UNVERIFIED**. One possibility (**UNVERIFIED**; crash-test it): `F_FULLFSYNC` "drains the entire queue of the device", so `F_FULLFSYNC` on any fd of the same volume *after* the rename should persist the APFS transaction that contains the rename. That depends on APFS committing its checkpoint as part of the call.

### 3.10 APFS: copy-on-write or in-place overwrite for non-cloned files?

**Verdict: UNVERIFIED. Apple's primary sources conflict.**

- *APFS Guide, FAQ* (archived, updated 2018-06-04) [doc] (A22): "Apple File System uses copy-on-write to avoid in-place changes to file data, which ensures that file system updates are crash protected without the write-twice overhead of journaling."
- *APFS Guide, Features* [doc] (A22): "Apple File System uses a novel copy-on-write metadata scheme to ensure that updates to the file system are crash protected, without the write-twice overhead of journaling."
- *Apple File System Reference* (2020-06-22) [doc] (A21):
  - "Regardless of their storage, objects on disk are never modified in place, and modified copies of an object are always written to a new location on disk." "Objects" here means APFS objects with `obj_phys_t` headers, i.e. metadata.
  - The volume flag `APFS_FS_ALWAYS_CHECK_EXTENTREF`: "The volume's extent reference tree is always consulted when deciding whether to overwrite an extent." This implies that an unshared extent *can* be overwritten in place (Inference).
  - `INODE_SNAPSHOT_COW_EXEMPTION`: "This inode is exempt from copy-on-write behavior if the data is part of a snapshot". And `APFS_COW_EXEMPT_COUNT_NAME`: "The number of files on the volume that don't use copy on write."
- The implementation is closed source.

**How to settle it for mantle** (Inference; a method, not a finding):
1. Write and `F_FULLFSYNC` a block-aligned region.
2. Record `l2p_devoffset` with `F_LOG2PHYS_EXT`.
3. Overwrite the region with the same size and alignment, then `F_FULLFSYNC`.
4. Read `l2p_devoffset` again.
5. Repeat with a local snapshot present, and on a `clonefile(2)`d file.

---

## 4. Windows

### 4.1 From a path to its volume and disk(s)

1. **`GetVolumePathNameW(lpszFileName, lpszVolumePathName, cch)`** [doc] (W1): "Retrieves the volume mount point where the specified path is mounted." It "returns the root of the volume where the end point of the specified path is located."
   - Junction points are followed: "If the specified path traverses a junction point, GetVolumePathName returns the volume to which the junction point refers".
   - For a network share it "returns the shortest path for which GetDriveType returns DRIVE_REMOTE".
   - You must pass a Win32 path. For an NT namespace path "the function returns the drive letter of the boot volume". It also returns the boot volume for a relative path without a volume qualifier.
   - "SMB does not support volume management functions."
2. **`GetVolumeNameForVolumeMountPointW(mountPoint, name, cch)`** [doc] (W2): the input "must end with a trailing backslash". The output has the form `"\\?\Volume{GUID}\"`, and 50 characters is a sufficient buffer.
   - "If there is more than one volume GUID path for the volume, only the first one in the mount manager's cache is returned."
   - The support table marks ReFS as not supported and notes: "Mount points aren't supported by ReFS volumes." Treat failure on a ReFS path as a normal case (Inference).
3. **`CreateFileW` on the volume or disk** [doc] (W3, W4):
   - Open a volume as `"\\.\X:"` or `"\\?\Volume{GUID}"` **without** the trailing backslash: "CreateFile processes a volume GUID path with an appended backslash as the root directory of the volume."
   - Open a disk as `"\\.\PhysicalDriveN"`.
   - Requirements: "The caller must have administrative privileges… The dwCreationDisposition parameter must have the OPEN_EXISTING flag. When opening a volume or floppy disk, the dwShareMode parameter must have the FILE_SHARE_WRITE flag."
   - The same section continues: "Note The dwDesiredAccess parameter can be zero, allowing the application to query device attributes without accessing a device… It can also be used for reading statistics without requiring higher-level data read/write permission." See §4.3 for the privilege question.
   - "Volume handles can be opened as noncached at the discretion of the particular file system… You should assume that all Microsoft file systems open volume handles as noncached."
4. **`IOCTL_VOLUME_GET_VOLUME_DISK_EXTENTS`** on the volume handle [doc] (W5):
   - Output `VOLUME_DISK_EXTENTS {DWORD NumberOfDiskExtents; DISK_EXTENT Extents[]}`, where `DISK_EXTENT {DWORD DiskNumber; LARGE_INTEGER StartingOffset; LARGE_INTEGER ExtentLength}`.
   - "When the number of extents returned is greater than one (1), the error code ERROR_MORE_DATA is returned. You should call DeviceIoControl again, allocating enough buffer space based on the value of NumberOfDiskExtents".
   - `DiskNumber` "is the same number that is used to construct the name of the disk, for example, the X in "\\?\PhysicalDriveX"". CreateFile's documentation adds that "a volume can span multiple physical disks".
   - Code 0x00560000 [ms-meta].
5. **`IOCTL_STORAGE_GET_DEVICE_NUMBER`** [doc] (W6) returns `STORAGE_DEVICE_NUMBER {DEVICE_TYPE DeviceType; DWORD DeviceNumber; DWORD PartitionNumber}`, where `PartitionNumber` is "–1" if the device can't be partitioned. The values are "guaranteed to remain unchanged until the device is removed or the system is restarted". Code 0x002D1080 [ms-meta].
6. **Classify the volume** [doc] (W33, W34):
   - `GetDriveTypeW(root)` returns `DRIVE_UNKNOWN` 0, `DRIVE_NO_ROOT_DIR` 1, `DRIVE_REMOVABLE` 2, `DRIVE_FIXED` 3, `DRIVE_REMOTE` 4, `DRIVE_CDROM` 5 or `DRIVE_RAMDISK` 6.
   - `GetVolumeInformationByHandleW(hFile, …)` returns the filesystem name and flags. The flags include:
     - `FILE_SUPPORTS_SPARSE_FILES` 0x40;
     - `FILE_VOLUME_IS_COMPRESSED` 0x8000;
     - `FILE_READ_ONLY_VOLUME` 0x80000;
     - `FILE_SUPPORTS_BLOCK_REFCOUNTING` 0x08000000 ("The file system reallocates on writes to shared clusters. Indicates that FSCTL_DUPLICATE_EXTENTS_TO_FILE is a supported operation");
     - `FILE_DAX_VOLUME` 0x20000000 [ms-meta];
     - `FILE_SUPPORTS_INTEGRITY_STREAMS` 0x04000000 [ms-meta].

### 4.2 `IOCTL_STORAGE_QUERY_PROPERTY` and its descriptors

- **Code:** 0x002D1400 [ms-meta]. Decoded with the CTL_CODE layout (W15), that is device type 0x2D (storage), function 0x500, `METHOD_BUFFERED` and **`FILE_ANY_ACCESS`**.
- **Input:** `STORAGE_PROPERTY_QUERY {STORAGE_PROPERTY_ID PropertyId; STORAGE_QUERY_TYPE QueryType; BYTE AdditionalParameters[1]}` [doc] (W8).
  - `QueryType` `PropertyStandardQuery` (0) "Instructs the driver to return an appropriate descriptor".
  - `PropertyExistsQuery` (1) "Instructs the driver to report whether the descriptor is supported", and then "no structure is returned".
- **Sizing:** "An application can determine the required buffer size by issuing a IOCTL_STORAGE_QUERY_PROPERTY control code passing a STORAGE_DESCRIPTOR_HEADER structure for the output buffer, and then using the returned Size member" [doc] (W9).
- **Failures** (WDK): "STATUS_INVALID_DEVICE_REQUEST, STATUS_INVALID_PARAMETER, or STATUS_NOT_SUPPORTED" [doc] (W7).

**`STORAGE_PROPERTY_ID` values** [doc] (W8); numbers [ms-meta]; minimum versions from W8:

| Id | Value | Descriptor | Minimum version and notes |
|---|---|---|---|
| `StorageDeviceProperty` | 0 | `STORAGE_DEVICE_DESCRIPTOR` | XP |
| `StorageAdapterProperty` | 1 | `STORAGE_ADAPTER_DESCRIPTOR` | |
| `StorageDeviceIdProperty` | 2 | SCSI VPD device identifiers | |
| `StorageDeviceUniqueIdProperty` | 3 | "Intended for driver usage" | |
| `StorageDeviceWriteCacheProperty` | 4 | `STORAGE_WRITE_CACHE_PROPERTY` | Vista |
| `StorageMiniportProperty` | 5 | reserved | |
| `StorageAccessAlignmentProperty` | 6 | `STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR` | Vista |
| `StorageDeviceSeekPenaltyProperty` | 7 | `DEVICE_SEEK_PENALTY_DESCRIPTOR` | Windows 7 |
| `StorageDeviceTrimProperty` | 8 | `DEVICE_TRIM_DESCRIPTOR` | Windows 7 |
| `StorageDeviceLBProvisioningProperty` | 11 | `DEVICE_LB_PROVISIONING_DESCRIPTOR` | Windows 8 |
| `StorageDevicePowerProperty` | 12 | | Windows 8 |
| `StorageDeviceCopyOffloadProperty` | 13 | | Windows 8 |
| `StorageDeviceMediumProductType` | 15 | `STORAGE_MEDIUM_PRODUCT_TYPE_DESCRIPTOR` | |
| `StorageDeviceIoCapabilityProperty` | 48 | `DEVICE_IO_CAPABILITY_DESCRIPTOR` | |
| `StorageAdapterProtocolSpecificProperty` / `StorageDeviceProtocolSpecificProperty` | 49 / 50 | `STORAGE_PROTOCOL_DATA_DESCRIPTOR` | |
| `StorageDeviceTemperatureProperty` | 52 | | |
| `StorageDeviceNumaProperty` | 59 | | |
| `StorageDeviceZonedDeviceProperty` | 60 | **"Reserved for system use."** Not usable for SMR detection | |
| `StorageDeviceEnduranceProperty` | 62 | "supported only for Non-Volatile Memory Express (NVMe) devices that implement a certain NVMe feature" | |

**`STORAGE_DEVICE_DESCRIPTOR`** (XP+) [doc] (W9) has these fields: `Version`, `Size`, `DeviceType` and `DeviceTypeModifier` (SCSI), `RemovableMedia`, `CommandQueueing`, `VendorIdOffset`, `ProductIdOffset`, `ProductRevisionOffset`, `SerialNumberOffset`, `STORAGE_BUS_TYPE BusType`, `RawPropertiesLength`, `RawDeviceProperties[1]`.
- Each `*Offset` is "the byte offset from the beginning of the structure to a null-terminated ASCII string… If the device has no … this member is zero."
- `ProductRevisionOffset` gives the firmware revision string.

**`STORAGE_BUS_TYPE`.** Descriptions are from the WDK version [doc] (W9) and numbers are [ms-meta]:

| Value | Name | Description |
|---|---|---|
| 0 | `BusTypeUnknown` | |
| 1 | `BusTypeScsi` | Also used for many VM disks (Inference; **UNVERIFIED**) |
| 2 | `BusTypeAtapi` | |
| 3 | `BusTypeAta` | |
| 4 | `BusType1394` | |
| 5 | `BusTypeSsa` | |
| 6 | `BusTypeFibre` | |
| 7 | `BusTypeUsb` | |
| 8 | `BusTypeRAID` | "a bus for a redundant array of independent disks" |
| 9 | `BusTypeiScsi` | |
| 10 | `BusTypeSas` | |
| 11 | `BusTypeSata` | |
| 12 | `BusTypeSd` | |
| 13 | `BusTypeMmc` | |
| 14 | `BusTypeVirtual` | "a virtual storage bus" |
| 15 | `BusTypeFileBackedVirtual` | "a virtual file backed storage bus" |
| 16 | `BusTypeSpaces` | "a storage spaces bus" |
| 17 | `BusTypeNvme` | |
| 18 | `BusTypeSCM` | storage-class memory |
| 19 | `BusTypeUfs` | |
| 20 | `BusTypeNvmeof` | Numbered by its position in the enum on current Learn. `windows-sys` 0.61.2 has no `BusTypeNvmeof` and has `BusTypeMax = 20`, so handle unknown values of 20 and above gracefully |
| — | `BusTypeMax` | "Do not use this value because it changes as new bus types are added" |
| 0x7F | `BusTypeMaxReserved` | |

**Other descriptors:**
- **`DEVICE_SEEK_PENALTY_DESCRIPTOR`** (Windows 7+) is `{DWORD Version; DWORD Size; BOOLEAN IncursSeekPenalty}`, where `IncursSeekPenalty` "Specifies whether the device incurs a seek penalty" [doc] (W10).
- **`STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR`** (Vista+) [doc] (W11) has `BytesPerCacheLine`, `BytesOffsetForCacheAlignment`, `BytesPerLogicalSector` ("the number of bytes in a logical sector") and `BytesPerPhysicalSector`. It also has `BytesOffsetForSectorAlignment`, "The logical sector offset within the first physical sector where the first logical sector is placed, in bytes".
- **`DEVICE_TRIM_DESCRIPTOR`** (Windows 7+) is `{…; BOOLEAN TrimEnabled}` [doc] (W12).

**`STORAGE_WRITE_CACHE_PROPERTY`** (Vista+) [doc] (W13). Fields:
- `WriteCacheType`: `Unknown` 0, `None` 1, `WriteBack` 2, `WriteThrough` 3.
- `WriteCacheEnabled`: `Unknown` 0, `Disabled` 1, `Enabled` 2.
- `WriteCacheChangeable`: `Unknown` 0, `NotChangeable` 1, `Changeable` 2.
- `WriteThroughSupported`: `Unknown` 0, `NotSupported` 1, `Supported` 2.
- `FlushCacheSupported`: "whether the device allows host software to flush the device cache".
- `UserDefinedPowerProtection`.
- `NVCacheEnabled`: "whether the device has a battery backup for the write cache".

The WDK storage guide [doc] (W14) adds:
- On FUA: "Do not confuse a write-through cache with a write-through request. A write-through request can be used with any kind of cache… If the device supports write-through requests, the initiator can bypass the write cache by setting the force unit access (FUA) bit in the command descriptor block (CDB) of the write command."
- On coverage: "The write cache property mechanism is not supported for RAID devices (because there is no standard technique for querying these devices) or for flash memory devices."

### 4.3 Privileges: what the documents actually say (they conflict)

- CreateFile, *Physical Disks and Volumes*: "The caller must have administrative privileges." The same section also has the note on zero access quoted in §4.1 [doc] (W4).
- *Defining I/O Control Codes* [doc] (W15): "FILE_ANY_ACCESS: The I/O manager sends the IRP for any caller that has a handle to the file object that represents the target device object." And: "Some system-defined I/O control codes have an Access value of FILE_ANY_ACCESS, which allows the caller to send the particular IOCTL regardless of the access granted to the device." `IOCTL_STORAGE_QUERY_PROPERTY`, `IOCTL_VOLUME_GET_VOLUME_DISK_EXTENTS` and `IOCTL_STORAGE_GET_DEVICE_NUMBER` all encode `FILE_ANY_ACCESS` [ms-meta].
- *Calling DeviceIoControl* [doc] (W16): Microsoft's sample opens `"\\\\.\\PhysicalDrive0"` with `0, // no access to the drive` and sends `IOCTL_DISK_GET_DRIVE_GEOMETRY`.
- *Advanced format (4K) disk compatibility update* [doc] (W17). It says that using `IOCTL_STORAGE_QUERY_PROPERTY` to get the physical sector size:
  - "Requires elevated privilege; if your app is not running with privilege, you may need to write a Windows Service Application";
  - "Does not support SMB volumes";
  - "Cannot be issued to any file handle (the IOCTL must be issued to a Volume Handle)".
- **Conclusion: UNVERIFIED.** We do not know whether a non-elevated process on current Windows can open `\\.\PhysicalDriveN` or `\\?\Volume{…}` with `dwDesiredAccess = 0` and get these descriptors. Design for both outcomes: treat `ERROR_ACCESS_DENIED` as "unknown", not as a failure, and use the per-handle queries in §4.4.

### 4.4 Unprivileged per-handle queries (Windows 8+)

- **`GetFileInformationByHandleEx(h, FileStorageInfo /*16*/, …)`** returns `FILE_STORAGE_INFO`. The information class is documented as "Use for any handles" [doc] (W29). Fields [doc] (W29):
  - `LogicalBytesPerSector`: "Logical bytes per sector reported by physical storage. This is the smallest size for which uncached I/O is supported."
  - `PhysicalBytesPerSectorForAtomicity`: "Bytes per sector for atomic writes. Writes smaller than this may require a read before the entire block can be written atomically."
  - `PhysicalBytesPerSectorForPerformance`: "Bytes per sector for optimal performance for writes."
  - `FileSystemEffectivePhysicalBytesPerSectorForAtomicity`: "the size of the block used for atomicity by the file system".
  - `Flags`: `STORAGE_INFO_FLAGS_ALIGNED_DEVICE` 0x1 and `STORAGE_INFO_FLAGS_PARTITION_ALIGNED_ON_DEVICE` 0x2.
  - `ByteOffsetForSectorAlignment` and `ByteOffsetForPartitionAlignment`. Each can be `STORAGE_INFO_OFFSET_UNKNOWN` (0xffffffff).
  - Remark: "If a volume is built on top of storage devices with different properties (for example a mirrored, spanned, striped, or RAID configuration) the sizes returned are those of the largest size of any of the underlying storage devices."
- **`NtQueryVolumeInformationFile(h, …, FileFsSectorSizeInformation /*11*/)`** returns `FILE_FS_SECTOR_SIZE_INFORMATION`, with the same fields [doc] (W30).
  - "No specific access rights are required to query this information. Thus this information is available as long as the volume is accessed through an open handle to the volume itself, or to a file or directory on the volume."
  - Its flags include **`SSINFO_FLAGS_NO_SEEK_PENALTY`** ("The storage device has no seek penalty") and a TRIM flag. The doc table mislabels the TRIM row as a second `SSINFO_FLAGS_PARTITION_ALIGNED_ON_DEVICE`, but describes it as "The storage device supports the TRIM operation".
  - Values [ms-meta]: `SSINFO_FLAGS_ALIGNED_DEVICE` 0x1, `…PARTITION_ALIGNED_ON_DEVICE` 0x2, `…NO_SEEK_PENALTY` 0x4, `…TRIM_ENABLED` 0x8, `…BYTE_ADDRESSABLE` 0x10.
  - "If the system is unable to determine values for PhysicalBytesPerSectorForAtomicity and PhysicalBytesPerSectorForPerformance from the storage device, then they are set to the value of LogicalBytesPerSector."
  - Calling from user mode: "If the call to the NtQueryVolumeInformationFile function occurs in user mode, you should use the name "NtQueryVolumeInformationFile"" [doc] (W30). `windows-sys` binds it from `ntdll.dll` [ms-meta].
  - The Advanced Format guide says this API is "Available for network volumes", "Can be issued to any file handle" and "Available for unprivileged apps" [doc] (W17).
  - Whether `FILE_STORAGE_INFO.Flags` also passes the NO_SEEK_PENALTY and TRIM bits through is **UNVERIFIED**, because W29 lists only two flags. Call `NtQueryVolumeInformationFile` directly to get them.
- **`GetFileInformationByHandleEx(h, FileAlignmentInfo /*17*/, …)`** returns `FILE_ALIGNMENT_INFO.AlignmentRequirement`. The two documents disagree about what the value means:
  - The Win32 page says "Minimum alignment requirement, in bytes" (W29).
  - The WDK `FILE_ALIGNMENT_INFORMATION` page says the value "must be one of the FILE_XXX_ALIGNMENT values defined in Wdm.h" (W29). Those are masks: `FILE_512_BYTE_ALIGNMENT` is 511 and `FILE_BYTE_ALIGNMENT` is 0 [ms-meta].
  - So read a value `v` as a required alignment of `v+1` bytes, and align to at least the sector size and the page size anyway (Inference).
- **`GetDiskFreeSpaceW`** → `lpBytesPerSector`. This is the *logical* sector size. The Advanced Format guide lists `GetDiskFreeSpace`, `FileFsVolumeInformation` and `IOCTL_DISK_GET_DRIVE_GEOMETRY(_EX)` as "APIs that retrieve the logical sector size" [doc] (W17, W28).
- **Sanity-check reported physical sector sizes** before use. The Advanced Format guide gives these rules [doc] (W17):
  - "Make sure that the reported physical sector size is >= the reported logical sector size… power of two… If the physical sector size is a power-of-two value between 512-bytes and 4 KB, you should consider using a physical sector size rounded down to the reported logical sector size".
  - "If the physical sector size is a power-of-two value greater than 4 KB, you should evaluate your app's ability to handle this scenario".

### 4.5 `FILE_FLAG_NO_BUFFERING` alignment

From *File Buffering* [doc] (W18):
- "File access sizes, including the optional file offset in the OVERLAPPED structure, if specified, must be for a number of bytes that is an integer multiple of the volume sector size."
- "File access buffer addresses for read and write operations should be physical sector-aligned, which means aligned on addresses in memory that are integer multiples of the volume's physical sector size. Depending on the disk, this requirement may not be enforced."
- "Microsoft strongly recommends that developers align unbuffered I/O to the physical sector size as reported by the IOCTL_STORAGE_QUERY_PROPERTY control code".
- "VirtualAlloc allocates memory that is aligned on addresses that are integer multiples of the system's page size… in most situations, page-aligned memory will also be sector-aligned".

From `CreateFileW` [doc] (W4): `FILE_FLAG_NO_BUFFERING` (0x20000000) "does not affect hard disk caching or memory mapped files".

**Why the physical sector size matters for resilience** [doc] (W17):
- "Because most hard disk drives update in place, the physical sector… could have been corrupted with incomplete info due to a partial overwrite. Put another way, you can think of it as potentially having lost all 8 logical sectors…"
- "the act of another app causing a Read-Modify-Write cycle can potentially cause your data to be lost even if your app is not running!"
- The guide recommends padding each commit record to the physical sector size.

### 4.6 Write-through, flushing and FUA

**`CreateFileW`, *Caching Behavior*** [doc] (W4):
- "If FILE_FLAG_WRITE_THROUGH is used but FILE_FLAG_NO_BUFFERING is not also specified, so that system caching is in effect, then the data is written to the system cache but is flushed to disk without delay."
- "If FILE_FLAG_WRITE_THROUGH and FILE_FLAG_NO_BUFFERING are both specified, so that system caching is not in effect, then the data is immediately flushed to disk without going through the Windows system cache. The operating system also requests a write-through of the hard disk's local hardware cache to persistent media. Note Not all hard disk hardware supports this write-through capability."
- "A write-through request via FILE_FLAG_WRITE_THROUGH also causes NTFS to flush any metadata changes, such as a time stamp update or a rename operation, that result from processing the request. For this reason, the FILE_FLAG_WRITE_THROUGH flag is often used with the FILE_FLAG_NO_BUFFERING flag as a replacement for calling the FlushFileBuffers function after each write".
- "When FILE_FLAG_NO_BUFFERING is combined with FILE_FLAG_OVERLAPPED… the file metadata may still be cached (for example, when creating an empty file). To ensure that the metadata is flushed to disk, use the FlushFileBuffers function."

**File Caching** [doc] (W19): "File system metadata is always cached. Therefore, to store any metadata changes to disk, the file must either be flushed or be opened with FILE_FLAG_WRITE_THROUGH."

**Is this FUA?**
- The storage stack's documented write-through mechanism is the FUA bit (§4.2, W14), and CreateFile says the OS "requests a write-through of the hard disk's local hardware cache".
- That `FILE_FLAG_WRITE_THROUGH` reaches the device *as FUA* on every storage path is Inference (**UNVERIFIED** end to end).
- The SQL Server storage guide states the contract SQL Server relies on: "The FILE_FLAG_WRITE_THROUGH option ensures that when a write operation returns successful completion, the data is correctly stored in stable storage." It also warns that drive caches "can't guarantee writes across a power cycle or similar failure point" [doc] (W37).

**`FlushFileBuffers(hFile)`** [doc] (W20):
- "The file handle must have the GENERIC_WRITE access right."
- "The FlushFileBuffers function writes all the buffered information for a specified file to the device or pipe."
- "…can be inefficient when used after every write… the application should use unbuffered I/O instead of frequently calling FlushFileBuffers."
- "To flush all open files on a volume, call FlushFileBuffers with a handle to the volume. The caller must have administrative privileges."

**`IRP_MJ_FLUSH_BUFFERS`**:
- Kernel documentation [doc] (W22): "It can be sent, for example, when a user-mode application has called a Win32 function such as FlushFileBuffers." For drivers: "The driver transfers any data currently cached in the device or held in the driver's internal buffers before completing the flush request."
- File-system documentation (W22): "The file system driver should flush to disk any important data or metadata associated with the file object".
- Inference from this chain: `FlushFileBuffers` flushes the *device* cache. This is consistent with the definition of `NtFlushBuffersFileEx` flag 0 below.

**`NtFlushBuffersFileEx(h, Flags, NULL, 0, &iosb)`** (Windows 8+; `ntifs.h`; from user mode through `ntdll`) [doc] (W21):

| Flags | Value [ms-meta] | Semantics (verbatim) | Filesystems |
|---|---|---|---|
| `0` (normal) | 0 | "File data and metadata in the file cache will be written, and the underlying storage is synchronized to flush its cache." | NTFS, ReFS, FAT, exFAT |
| `FLUSH_FLAGS_FILE_DATA_ONLY` | 0x1 | "File data in the file cache will be written. No metadata is written and the underlying storage is not synchronized to flush its cache. This flag is not valid with volume handles." | NTFS, FAT, exFAT |
| `FLUSH_FLAGS_NO_SYNC` | 0x2 | "File data and metadata in the file cache will be written. The underlying storage is not synchronized to flush its cache. This flag is not valid with volume handles." | NTFS, FAT, exFAT |
| `FLUSH_FLAGS_FILE_DATA_SYNC_ONLY` | 0x4 | "Data from the given file will be written from the Windows in-memory cache. Only metadata that is necessary for data retrieval will be flushed (timestamp updating will be skipped as much as possible). The underlying storage is synchronized to flush its cache. This flag is not valid with volume or directory handles." | NTFS |

- `FLUSH_FLAGS_FILE_DATA_SYNC_ONLY` is the `fdatasync` equivalent. The Windows version that introduced it is **UNVERIFIED**, so detect it at runtime by the error the call returns.
- `STATUS_ACCESS_DENIED` means "The file does has [sic] neither write or append access."

### 4.7 Allocation, end of file, valid data length (VDL) and zero-fill

**The three sizes of a stream** (SetFileValidData and SetEndOfFile remarks) [doc] (W23, W27): "File size: the size of the data in a file, to the byte. Allocation size: the size of the space that is allocated for a file on a disk, which is always an even multiple of the cluster size. Valid data length: the length of the data in a file that is actually written, to the byte. This value is always less than or equal to the file size."

**Zero-fill semantics:**
- fsutil documentation [doc] (W25): "In NTFS, there are two important concepts of file length: the end-of-file (EOF) marker and the Valid Data Length (VDL)… Any reads between VDL and EOF automatically return 0 to preserve the C2 object reuse requirement."
- KB 156932 [doc] (W26):
  - "On Windows, any write operation to a file that extends its length will be synchronous."
  - "Applications can make the previously mentioned write operation asynchronous by changing the Valid Data Length of the file by using the SetFileValidData function".
  - "Because the NTFS file system doesn't zero-fill the data up to the valid data length (VDL) that is defined by SetFileValidData, this function has security implications… SetFileValidData requires that the caller has the new SeManageVolumePrivilege enabled (by default, this is assigned only to administrators)."
- In the kernel, `CcZeroData` zeroes ranges "beyond the file's valid data length" [doc] (W24).

**`SetFileValidData(hFile, ValidDataLength)`** [doc] (W23):
- "The file must have been opened with the GENERIC_WRITE access right, and the SE_MANAGE_VOLUME_NAME privilege enabled… The file cannot be a network file, or be compressed, sparse, or transacted."
- `ValidDataLength` "must be a positive value that is greater than the current valid data length, but less than the current file size".
- Why use it: "…allows you to avoid filling data with zeros when writing nonsequentially to a file… existing data on disk from previously existing files can inadvertently become available to unintended readers."
- When it pays off: "if the extended portion of the file is large and will be written to randomly, such as in a database type of application, the time it takes to extend and write to the file will be faster than using SetEndOfFile and writing randomly. In most other situations, there is usually no performance gain…"
- The privilege is `SE_MANAGE_VOLUME_NAME` = `"SeManageVolumePrivilege"` ("Perform volume maintenance tasks") [doc] (W39). The kernel-side structure notes that "nonadministrators and remote users must have SeManageVolumePrivilege" [doc] (W24).

**Preallocating without that privilege** [doc] (W27):
- `SetFileInformationByHandle(h, FileAllocationInfo /*5*/, &FILE_ALLOCATION_INFO{AllocationSize})`. "The end-of-file (EOF) position for a file must always be less than or equal to the file allocation size. If the allocation size is set to a value that is less than EOF, the EOF position is automatically adjusted to match the file allocation size."
- `FileEndOfFileInfo` (6) sets EOF.
- `SetEndOfFile`: "If the file is extended, the contents of the file between the old end of the file and the new end of the file are not defined". The fsutil statement above gives the stronger, NTFS-specific guarantee that such reads return zeros.
- Inference: to get fast asynchronous random writes into a preallocated file without the privilege, write zeros sequentially *once* at allocation time. That advances the VDL, so later writes land inside the VDL and neither extend the file nor trigger zero-fill.

### 4.8 Making a create or rename durable

- **`MoveFileExW(existing, new, MOVEFILE_REPLACE_EXISTING /*0x1*/ | MOVEFILE_WRITE_THROUGH /*0x8*/)`** [doc] (W31): "MOVEFILE_WRITE_THROUGH: The function does not return until the file is actually moved on the disk. Setting this value guarantees that a move performed as a copy and delete operation is flushed to disk before the function returns. The flush occurs at the end of the copy operation. This value has no effect if MOVEFILE_DELAY_UNTIL_REBOOT is set." The explicit guarantee sentence only covers the copy-and-delete case. Whether a same-volume rename is also forced to stable media is **UNVERIFIED**, though the first sentence suggests it.
- **`SetFileInformationByHandle(h, FileRenameInfoEx /*22*/, FILE_RENAME_INFO{Flags, …})`** [doc] (W32):
  - `FILE_RENAME_FLAG_REPLACE_IF_EXISTS` is 0x1 and `FILE_RENAME_FLAG_POSIX_SEMANTICS` is 0x2 [ms-meta].
  - POSIX semantics: "If FILE_RENAME_REPLACE_IF_EXISTS is also specified, allow replacing a file even if there are existing handles to it. Existing handles to the replaced file continue to be valid… Any subsequent opens of the target name will open the renamed file".
  - The WDK typedef adds the `Flags` union under `_WIN32_WINNT >= _WIN32_WINNT_WIN10_RS1`, which suggests Windows 10 1607 (Inference).
  - A rename requires `DELETE` access.
- **The WRITE_THROUGH handle route.** Per CreateFile, `FILE_FLAG_WRITE_THROUGH` "causes NTFS to flush any metadata changes, such as … a rename operation, that result from processing the request" (W4). So opening the source file with `FILE_FLAG_WRITE_THROUGH` and renaming it through that handle is the documented way to make NTFS flush the rename (Inference about applying it this way).
- **Flushing a directory handle.** *Obtaining a Handle to a Directory* lists the functions that accept a directory handle, and `FlushFileBuffers` is not among them (W35). The `NtFlushBuffersFileEx` flag table says `FLUSH_FLAGS_FILE_DATA_SYNC_ONLY` is "not valid with volume or directory handles", which implies directory handles are accepted with the other flags (Inference). Whether flushing a directory handle persists a rename on NTFS or ReFS is **UNVERIFIED**.

### 4.9 IoRing (Windows 11)

- `CreateIoRing(IORING_VERSION, IORING_CREATE_FLAGS, sqSize, cqSize, HIORING*)` requires Windows build 22000 or later [doc] (W36).
- The `IORING_OP_CODE` enum lists `NOP`, `READ`, `REGISTER_FILES`, `REGISTER_BUFFERS`, `CANCEL`, `WRITE`, `FLUSH`, `READ_SCATTER` and `WRITE_GATHER`. `IsIoRingOpSupported(ring, op)` checks support at runtime [doc] (W36).
- Which Windows build first supports each opcode is **UNVERIFIED**. Always probe.

---

## 5. Cross-OS mapping

### 5.1 Detection primitives

| Question | Linux | macOS | Windows |
|---|---|---|---|
| Filesystem type | `fstatfs.f_type` + mountinfo field 9 | `statfs.f_fstypename` | `GetVolumeInformationByHandleW` (FS name) |
| Local or network | `f_type` (NFS/SMB/9P/Ceph/FUSE) | `!(f_flags & MNT_LOCAL)` | `GetDriveTypeW == DRIVE_REMOTE` |
| RAM-backed | `TMPFS_MAGIC`/`RAMFS_MAGIC`; zram/brd | Location `"RAM"` | `DRIVE_RAMDISK` |
| Backing device | `st_dev` → `/sys/dev/block/M:m`; `slaves/`; `loop/backing_file` | `f_mntfromname` → `IOBSDNameMatching` → parent search | `GetVolumePathNameW` → Volume GUID → `IOCTL_VOLUME_GET_VOLUME_DISK_EXTENTS` |
| SSD or HDD (hint) | `queue/rotational` (driver policy) | `"Medium Type"` (optional) | `IncursSeekPenalty`, or `SSINFO_FLAGS_NO_SEEK_PENALTY` (unprivileged) |
| Bus or interconnect | subsystem walk (nvme/scsi/usb/virtio/vmbus/xen/mmc); NVMe `transport` | `"Physical Interconnect"` + `"Location"` | `STORAGE_DEVICE_DESCRIPTOR.BusType` |
| Model and firmware | NVMe `model`/`firmware_rev`; SCSI `vendor`/`model`/`rev` | `"Product Name"`/`"Product Revision Level"` | `ProductIdOffset`/`ProductRevisionOffset` |
| Logical block size | `queue/logical_block_size` | `"Logical Block Size"` / `DKIOCGETBLOCKSIZE` (root) | `FILE_STORAGE_INFO.LogicalBytesPerSector` |
| Physical block size | `queue/physical_block_size` | `"Physical Block Size"` | `PhysicalBytesPerSectorForAtomicity` |
| Direct-I/O alignment | `statx(STATX_DIOALIGN)` | no API. The generic cluster layer needs page alignment and ≥16 KiB (§3.7) | sector-multiple sizes and offsets; physical-sector-aligned buffers |
| Optimal I/O size | `queue/optimal_io_size` (often 0) | `f_iosize`, `IOMaximumByteCountWrite` | `PhysicalBytesPerSectorForPerformance` |
| Volatile write cache | `queue/write_cache` | `"WriteCacheState"` (rarely present) | `STORAGE_WRITE_CACHE_PROPERTY` (not reported for RAID or flash) |
| FUA supported | `queue/fua` | `IOStorageFeatures."Force Unit Access"` | `WriteThroughSupported` |
| Zoned / SMR | `queue/zoned`, `scsi_disk/*/zoned_cap` | none found (**UNVERIFIED**) | `StorageDeviceZonedDeviceProperty` is "Reserved for system use" |
| TRIM | `queue/discard_max_hw_bytes` > 0 | `IOStorageFeatures."Unmap"`, `DK_FEATURE_UNMAP` | `DEVICE_TRIM_DESCRIPTOR`, `SSINFO_FLAGS_TRIM_ENABLED` |
| Virtual or image | `/sys/devices/virtual`, `virtio`/`xen`/`vmbus` ancestors, loop | `"Virtual Interface"`, Location `"File"` | `BusTypeVirtual`/`FileBackedVirtual`/`Spaces` |

### 5.2 Durability and I/O primitives

| Need | Linux | macOS | Windows |
|---|---|---|---|
| Data + metadata + device-cache flush | `fsync` | `F_FULLFSYNC` | `FlushFileBuffers` / `NtFlushBuffersFileEx(0)` |
| Data-only flush including the device cache | `fdatasync` | `fdatasync` then `F_FULLFSYNC` (fdatasync alone does **not** flush the drive) | `NtFlushBuffersFileEx(FLUSH_FLAGS_FILE_DATA_SYNC_ONLY)` (NTFS) |
| Ordering barrier without durability | none: use a flush | `F_BARRIERFSYNC` | none: use a flush |
| Durable per-write | `O_DSYNC` / `RWF_DSYNC` (FUA fast path on overwrites) | `O_DSYNC` becomes `IO_SYNC` (drive flush **UNVERIFIED**) | `FILE_FLAG_WRITE_THROUGH` (+ `NO_BUFFERING`) |
| Directory entry durable | `fsync(dirfd)` | **UNVERIFIED** (see §3.9) | `MOVEFILE_WRITE_THROUGH`; a WRITE_THROUGH handle for the rename |
| Bypass the page cache | `O_DIRECT` | `F_NOCACHE` (conditional, §3.7) | `FILE_FLAG_NO_BUFFERING` |
| Reserve space | `fallocate(0 or KEEP_SIZE)` (unwritten extents) | `F_PREALLOCATE` | `FileAllocationInfo` |
| Preallocate with written extents | `FALLOC_FL_WRITE_ZEROES` (6.17+), or write zeros | write zeros (**UNVERIFIED** whether it helps on APFS) | write zeros to advance the VDL, or `SetFileValidData` (privileged) |
| Punch a hole | `FALLOC_FL_PUNCH_HOLE\|KEEP_SIZE` | `F_PUNCHHOLE` | `FSCTL_SET_ZERO_DATA` on sparse files (not researched in depth) |
| Torn-write protection | `RWF_ATOMIC` + `STATX_WRITE_ATOMIC` | none found | `PhysicalBytesPerSectorForAtomicity` only |
| Async batched I/O | io_uring (5.1+) | none. Use threads (POSIX AIO not researched) | IoRing (22000+), or overlapped I/O + IOCP |

---

## 6. Implications for mantle

### 6.1 The detection pipeline

Run it when a data directory is opened. Cache the result keyed by filesystem ID and device ID, and re-probe after a remount. The output is a `StorageProfile`. Every field records its *provenance*: `Reported(api)`, `Measured(test)` or `Assumed(default)`.

**Linux**
1. Open the directory and call `fstatfs` to get `f_type`. Create a small probe file and `statx` it with `STATX_MNT_ID | STATX_DIOALIGN | STATX_DIO_READ_ALIGN | STATX_WRITE_ATOMIC`. Keep only the bits the kernel returns in `stx_mask`.
2. Find the mountinfo line whose field 1 equals `stx_mnt_id`. From it take the filesystem type (field 9), the mount source (field 10) and the super options (field 11).
3. Decide early from the filesystem type:
   - tmpfs or ramfs: **RAM**. Refuse durable mode unless explicitly overridden.
   - overlay with `volatile`: refuse.
   - NFS, SMB, 9P, Ceph or FUSE: **network or userspace**. Warn, fall back to buffered I/O with `fsync`, and never trust `O_DIRECT` alignment rules (§2.8).
4. If `major(st_dev) != 0`, open `/sys/dev/block/M:m` and resolve it. If a `partition` file is present, move to the parent directory. Then recurse through `slaves/*` and `loop/backing_file`, with a depth limit of about 8. Collect the leaf devices.
5. If `major(st_dev) == 0`:
   - btrfs: use `BTRFS_IOC_FS_INFO`/`DEV_INFO`, which work unprivileged.
   - overlay: use `upperdir=` from the super options. The format of those options is **UNVERIFIED**.
   - anything else: mark the device as unknown.
6. For each leaf:
   - Read the `queue/*` attributes in §2.4.
   - Read identity: NVMe `model`/`firmware_rev`/`transport`, or SCSI `vendor`/`model`/`rev` plus `zoned_cap`.
   - Classify it by the `/sys/devices/virtual` prefix and by the `subsystem` names of its ancestors.
7. Combine the leaves:
   - media is "rotational" if any leaf is rotational (matching the kernel's OR);
   - the physical block is the maximum;
   - `write_cache` is true if any leaf has one;
   - FUA counts only if the stacked device's own `queue/fua` is 1.

**macOS**
1. `fstatfs`: if `MNT_LOCAL` is clear, or `f_mntfromname` does not start with `/dev/`, classify as network or unknown.
2. `IOBSDNameMatching(0, 0, name)` → `IOServiceGetMatchingService(0, dict)`.
3. Use `IORegistryEntrySearchCFProperty(…, kIOServicePlane, key, …, recursive|parents)` to read each of these keys: `"Device Characteristics"`, `"Protocol Characteristics"`, `"Physical Block Size"`, `"Logical Block Size"`, `"IOStorageFeatures"`, `"IOMaximumByteCountWrite"`, `"IOMinimumSegmentAlignmentByteCount"`.
4. Classify the interconnect Location: `"File"` means a disk image; `"RAM"` a RAM disk; `"Virtual Interface"` a virtual device; `"External"` means external or removable.
5. If you need every backing store (Fusion), walk all parents with `IORegistryEntryGetParentIterator` instead of relying on the first match.
6. If a key is missing, record "unknown". Never fall back to `DKIOC*`, which needs root.

**Windows**
1. `GetFullPathNameW` → `GetVolumePathNameW` → `GetDriveTypeW`.
2. Open the data directory with `FILE_FLAG_BACKUP_SEMANTICS` and call:
   - `GetVolumeInformationByHandleW` for the FS name and flags;
   - `GetFileInformationByHandleEx(FileStorageInfo)` and `(FileAlignmentInfo)`;
   - `NtQueryVolumeInformationFile(FileFsSectorSizeInformation)` for NO_SEEK_PENALTY, TRIM and BYTE_ADDRESSABLE.

   All of these are **unprivileged**.
3. Then try the better data: `GetVolumeNameForVolumeMountPointW` → `CreateFileW("\\?\Volume{…}", 0, FILE_SHARE_READ|FILE_SHARE_WRITE, OPEN_EXISTING)` → `IOCTL_VOLUME_GET_VOLUME_DISK_EXTENTS`. For each disk number, open `\\.\PhysicalDriveN` with 0 access and query the Device, SeekPenalty, Trim, AccessAlignment and WriteCache properties.
4. Treat `ERROR_ACCESS_DENIED`, `ERROR_INVALID_FUNCTION` and `ERROR_NOT_SUPPORTED` as "unknown" and keep the step-2 values.
5. Classify by `BusType`:
   - `Nvme`, `Sata`, `Sas`, `Ata`: local physical devices.
   - `Scsi`: local physical *or* VM.
   - `Usb`, `Sd`, `Mmc`, `1394`: removable-class devices.
   - `Virtual`, `FileBackedVirtual`: virtual disks or VHDs.
   - `Spaces`: pooled storage. Its members are not visible through this path.
   - `iScsi`, `Fibre`, `Nvmeof`: network block devices.
   - `RAID`: an opaque hardware RAID.

### 6.2 What to trust, what to treat as a hint, what to measure

| Must obey (violating it produces errors) | Trust (reported facts) | Hint (verify by measurement) |
|---|---|---|
| `stx_dio_*_align`, `logical_block_size`, `NO_BUFFERING` sector multiples, `zone_write_granularity` and sequential zones (host-managed), `RWF_ATOMIC` size and alignment rules | filesystem type; local vs network; RAM-backed; loop or disk image; overlay `volatile`; the kernel's `write_cache`/`fua` (it decides whether the kernel flushes); `zoned` host-managed/host-aware; `independent_access_ranges` | `rotational`, `IncursSeekPenalty`, `Medium Type` (driver policy, bridges, VMs); bus type under a hypervisor; `physical_block_size` (bridges misreport it; apply the Advanced Format sanity rules); `optimal_io_size`; drive-managed SMR (usually unreported); whether a virtual disk *really* has a volatile cache; whether FUA or write-through is honored (only power-cut testing can prove it) |

### 6.3 Calibration probes

All probes run on a scratch file inside the data directory and have a bounded time budget. The thresholds are engineering choices; no source defines them.

1. **Direct-I/O probe.** Open with `O_DIRECT`, `F_NOCACHE` or `NO_BUFFERING` and do one aligned read and one aligned write. Fall back to buffered I/O on `EINVAL`.
2. **Latency class.** Measure QD1 random 4 KiB reads that bypass the page cache on a pre-written file. A median in the low hundreds of µs looks like flash; several ms looks like a spinning disk.
3. **Queue-depth scaling.** Compare IOPS at QD1, QD8 and QD32. NVMe scales with depth; HDDs and USB bridges barely do (see `03-io-and-persistence.md` §2–4 for the literature).
4. **Commit cost.** Compare the latency of each of these:
   - `pwrite` + `fdatasync`;
   - `O_DIRECT|O_DSYNC` overwrite (FUA path);
   - `F_FULLFSYNC` versus `F_BARRIERFSYNC`;
   - `FlushFileBuffers` versus a `WRITE_THROUGH` write.

   The results size the group-commit batch.
5. **SMR check**, only for rotational devices that report nothing: run sustained random writes over a large span and look for a throughput cliff. It is expensive, so run it only in an explicit `calibrate` mode.
6. **Copy-on-write check.** Compare physical offsets before and after an overwrite plus sync: FIEMAP on Linux, `F_LOG2PHYS_EXT` on macOS (§3.10).

### 6.4 Durable write protocols

**Linux**
- **WAL or log segments:**
  - Preallocate with `fallocate(FALLOC_FL_WRITE_ZEROES)`. On `EOPNOTSUPP`/`EINVAL`, or on kernels before 6.17, write zeros once with large `O_DIRECT` writes and `fdatasync`.
  - Then commit with `pwritev2(…, RWF_DSYNC)` on an `O_DIRECT` fd, or open the file `O_DSYNC`, overwriting inside EOF.
  - **Never use `O_SYNC`/`RWF_SYNC` for this**, because it disables the FUA path (§2.10).
  - If `queue/fua=0` and `write_cache` is write back, the kernel adds the flush itself, which is still correct. Group commits to amortize it.
- **New immutable chunk files:** `O_TMPFILE` (or a temporary name) → write → `fdatasync`/`fsync` → `linkat`/`rename` → `fsync(dirfd)`.
- **After an fsync or fdatasync error:** stop trusting that file. Re-replicate the data or rewrite it from memory. **Do not** retry and treat the success as durability (§2.9).
- **io_uring, when available:**
  - Link write → fsync chains with `IOSQE_IO_LINK`.
  - Use registered buffers (budget `RLIMIT_MEMLOCK`).
  - Use IOPOLL only when `queue/io_poll=1` and the fd is `O_DIRECT`.
  - SQPOLL needs no privileges only on 5.13+.
  - On `ENOSYS`/`EPERM`, fall back to a thread pool doing `pread`/`pwrite`.
- **Torn-write protection:** where `STATX_WRITE_ATOMIC` reports units, use `RWF_ATOMIC` with `O_DIRECT` and `O_DSYNC`. Otherwise rely on per-record checksums.

**macOS**
- **Commit:** `pwrite` batch → `fcntl(fd, F_FULLFSYNC)` once per group. The man page guarantees that data `fsync`ed earlier on the same device is persisted when `F_FULLFSYNC` returns, so with several files: `fsync` each one, then issue one `F_FULLFSYNC`.
- **Ordering only** (for example, "data before the manifest", with durability supplied later): use `F_BARRIERFSYNC`. It is safe everywhere because xnu promotes it to `F_FULLFSYNC` when a filesystem does not support it.
- **Cache bypass for large streams:** `F_NOCACHE` with 16 KiB-aligned offsets and buffers and I/O of at least 16 KiB. Measure the effect on APFS (**UNVERIFIED**).
- **Preallocation:** `F_PREALLOCATE {F_ALLOCATECONTIG|F_ALLOCATEALL, F_PEOFPOSMODE, fst_offset=0}`, then `ftruncate` to the logical size, then verify the size. Retry without `F_ALLOCATECONTIG` if it fails with `ENOSPC`.
- **Rename:** `rename` → `F_FULLFSYNC` on the file, and try it on the directory fd as well. Treat directory-entry durability as **UNVERIFIED** until mantle's crash tests confirm it on APFS.

**Windows**
- **WAL:**
  - Open with `FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH | FILE_FLAG_OVERLAPPED` on a preallocated file whose VDL has already been advanced.
  - Use `VirtualAlloc` buffers and make writes multiples of `max(LogicalBytesPerSector, sanity-checked PhysicalBytesPerSectorForAtomicity)`.
  - If `WriteThroughSupported != Supported`, or the query was unavailable, also call `FlushFileBuffers` at each group commit.
- **Data files:** buffered or unbuffered writes followed by `NtFlushBuffersFileEx(FLUSH_FLAGS_FILE_DATA_SYNC_ONLY)` on NTFS, or `FlushFileBuffers` elsewhere or when that call fails.
- **Preallocation:** `FileAllocationInfo` + `FileEndOfFileInfo`. Then either `SetFileValidData`, only if `SeManageVolumePrivilege` can be enabled *and* the file's ACL is restricted, or a one-time sequential zero write. Either way, later writes stay inside the VDL, which avoids the synchronous extend-and-zero-fill path (§4.7).
- **Rename or replace:** `MoveFileExW(…, MOVEFILE_REPLACE_EXISTING|MOVEFILE_WRITE_THROUGH)`, or `FileRenameInfoEx` with `REPLACE_IF_EXISTS|POSIX_SEMANTICS` on a handle opened with `DELETE | FILE_FLAG_WRITE_THROUGH`. Crash-test it.

### 6.5 Defaults when detection fails

- **Alignment:** 4096 bytes on Linux and Windows, 16 KiB on Apple Silicon for the `F_NOCACHE` direct path. If direct I/O fails, use buffered I/O.
- **Durability model:** assume a volatile write cache is present and FUA is not honored, so issue an explicit flush at every group commit (`fdatasync`, `F_FULLFSYNC` or `FlushFileBuffers`).
- **Media:** unknown. Start with moderate concurrency, then adapt after the §6.3 probes.
- **Network filesystems:** warn, and refuse them for the WAL by default. NFS `O_DIRECT` semantics differ, and "Some servers may also be configured to lie to clients about the I/O having reached stable storage" (M2).

### 6.6 Environment gotchas

- **Containers:**
  - io_uring may be blocked by the sysctl, by seccomp or by an LSM (§2.13).
  - `/sys` is readable even when it is mounted read-only (Inference).
  - Device nodes may be missing, so use sysfs.
  - The data directory may be on overlayfs, where `st_dev` is not uniform (L39). Mount a real volume for the data.
- **VMs:**
  - virtio-blk always reports rotational and never FUA.
  - The write cache depends on the hypervisor's feature bits (§2.5).
  - Cloud "NVMe" or "SCSI" devices can be network-backed. The model strings may reveal this, but there is no primary-source list of them (**UNVERIFIED**), so measure latency.
- **WSL2:** Windows drives appear as 9p (`V9FS_MAGIC`) according to general knowledge, but this is **UNVERIFIED** from primary sources. Treat them as a network filesystem.

---

## 7. UNVERIFIED items and open questions (consolidated)

1. Whether a **non-elevated** Windows process can open `\\.\PhysicalDriveN` or `\\?\Volume{…}` with 0 access and get `IOCTL_STORAGE_QUERY_PROPERTY` descriptors. The Learn documents contradict each other (§4.3).
2. Whether `FILE_STORAGE_INFO.Flags` includes the NO_SEEK_PENALTY and TRIM bits that `FILE_FS_SECTOR_SIZE_INFORMATION` has.
3. Whether `FILE_FLAG_WRITE_THROUGH` reaches every Windows storage path as FUA, and whether a same-volume `MOVEFILE_WRITE_THROUGH` rename is flushed to stable media.
4. The Windows version that introduced `FLUSH_FLAGS_FILE_DATA_SYNC_ONLY`, and whether flushing a *directory* handle persists renames on NTFS or ReFS.
5. Which build first supports each Windows IoRing opcode.
6. **APFS:** whether non-cloned file data is overwritten in place or copied on write (§3.10).
7. **APFS:** whether `F_NOCACHE` takes the cluster-layer direct path; how `IO_SYNC`/`O_DSYNC` affects the drive cache; whether directory `fsync`/`F_FULLFSYNC` persists renames.
8. On macOS, `f_fstypename` values for network and FSKit filesystems. Also which physical store a single parent search returns for a Fusion container.
9. Linux:
   - stability of `scsi_disk/*/zoned_cap` (no ABI document);
   - how often drive-managed SMR drives actually report `zoned=2`;
   - stability of `/sys/fs/btrfs/<fsid>/devices/`;
   - the format of overlay super options in mountinfo;
   - the errno returned when an LSM or seccomp policy denies `io_uring_setup`.
10. Linux: whether ext4's default is `dioread_nolock`. The current ext4 document still says the default is `dioread_lock`.
11. The `f_type` reported by in-kernel `ntfs3`. `ZFS_SUPER_MAGIC` comes from OpenZFS source, which is outside the owner's list of allowed sources.
12. Mapping DMI vendor strings and device model strings to hypervisors and cloud block services.

---

## 8. Source index

### Linux kernel (tag `v7.3-rc5`, commit `72d3fcf802c45d00b300f25b848a93c3a2bd7c7e`)

- **L1** `Documentation/ABI/stable/sysfs-block`. https://github.com/torvalds/linux/blob/v7.3-rc5/Documentation/ABI/stable/sysfs-block (rendered: https://docs.kernel.org/admin-guide/abi-stable.html)
- **L2** `Documentation/block/queue-sysfs.rst` (last present in v5.16). https://www.kernel.org/doc/html/v5.15/block/queue-sysfs.html. Removal commit: https://github.com/torvalds/linux/commit/208e4f9c0028
- **L3** `Documentation/ABI/testing/sysfs-dev`. https://github.com/torvalds/linux/blob/v7.3-rc5/Documentation/ABI/testing/sysfs-dev
- **L4** `Documentation/admin-guide/sysfs-rules.rst`. https://docs.kernel.org/admin-guide/sysfs-rules.html
- **L5** `Documentation/block/writeback_cache_control.rst`. https://docs.kernel.org/block/writeback_cache_control.html
- **L6** `block/holder.c`. https://github.com/torvalds/linux/blob/v7.3-rc5/block/holder.c#L34-L50
- **L7** `block/partitions/core.c`. https://github.com/torvalds/linux/blob/v7.3-rc5/block/partitions/core.c#L208-L215
- **L8** `block/genhd.c`. https://github.com/torvalds/linux/blob/v7.3-rc5/block/genhd.c#L519-L526 (and #L1034-L1041, #L1166-L1177)
- **L9** `Documentation/ABI/testing/sysfs-block-loop`. https://github.com/torvalds/linux/blob/v7.3-rc5/Documentation/ABI/testing/sysfs-block-loop
- **L10** `Documentation/ABI/testing/sysfs-block-dm`. https://github.com/torvalds/linux/blob/v7.3-rc5/Documentation/ABI/testing/sysfs-block-dm
- **L11** `Documentation/admin-guide/md.rst`. https://docs.kernel.org/admin-guide/md.html
- **L12** `Documentation/admin-guide/devices.txt`. https://docs.kernel.org/admin-guide/devices.html
- **L13** `Documentation/ABI/stable/sysfs-nvme`. https://github.com/torvalds/linux/blob/v7.3-rc5/Documentation/ABI/stable/sysfs-nvme
- **L14** `drivers/nvme/host/sysfs.c`. https://github.com/torvalds/linux/blob/v7.3-rc5/drivers/nvme/host/sysfs.c#L461-L473
  - `drivers/nvme/host/core.c` (#L1708, #L2476-L2482, #L4282-L4311): https://github.com/torvalds/linux/blob/v7.3-rc5/drivers/nvme/host/core.c
  - `drivers/nvme/host/multipath.c#L804`: https://github.com/torvalds/linux/blob/v7.3-rc5/drivers/nvme/host/multipath.c#L804
- **L15** `drivers/scsi/scsi_sysfs.c`. https://github.com/torvalds/linux/blob/v7.3-rc5/drivers/scsi/scsi_sysfs.c#L650-L655
- **L16** `drivers/scsi/sd.c`. https://github.com/torvalds/linux/blob/v7.3-rc5/drivers/scsi/sd.c (#L205-L215, #L711-L724, #L759-L778, #L3484-L3515, #L3818-L3824)
- **L17** `drivers/ata/libata-scsi.c` (#L2104, #L2230, #L2389-L2405): https://github.com/torvalds/linux/blob/v7.3-rc5/drivers/ata/libata-scsi.c
  - `include/linux/ata.h#L991-L994`: https://github.com/torvalds/linux/blob/v7.3-rc5/include/linux/ata.h#L991-L994
- **L18** `drivers/block/virtio_blk.c`. https://github.com/torvalds/linux/blob/v7.3-rc5/drivers/block/virtio_blk.c (#L1070-L1087, #L1442, #L1703)
- **L19** `drivers/block/loop.c`. https://github.com/torvalds/linux/blob/v7.3-rc5/drivers/block/loop.c#L992-L996
- **L20** `drivers/block/nbd.c`. https://github.com/torvalds/linux/blob/v7.3-rc5/drivers/block/nbd.c (#L246-L263, #L335-L352)
- **L21** `drivers/block/xen-blkfront.c`. https://github.com/torvalds/linux/blob/v7.3-rc5/drivers/block/xen-blkfront.c#L962-L966
- **L22** Stacking code:
  - `block/blk-settings.c` (#L376, #L786, #L852-L860): https://github.com/torvalds/linux/blob/v7.3-rc5/block/blk-settings.c
  - `drivers/md/dm-table.c#L2058`: https://github.com/torvalds/linux/blob/v7.3-rc5/drivers/md/dm-table.c#L2058
  - `drivers/md/md.c#L2633`: https://github.com/torvalds/linux/blob/v7.3-rc5/drivers/md/md.c#L2633
  - `drivers/md/dm.c` (#L761, #L2643): https://github.com/torvalds/linux/blob/v7.3-rc5/drivers/md/dm.c
- **L23** `block/blk-sysfs.c`. https://github.com/torvalds/linux/blob/v7.3-rc5/block/blk-sysfs.c (#L557-L583, #L585-L676)
- **L24** Commit `bd4a633b6f7c` "block: move the nonrot flag to queue_limits" (in v6.11). https://github.com/torvalds/linux/commit/bd4a633b6f7c
- **L25** `include/linux/blkdev.h` (#L321, #L363-L366). https://github.com/torvalds/linux/blob/v7.3-rc5/include/linux/blkdev.h
- **L26** `drivers/base/core.c#L3325`. https://github.com/torvalds/linux/blob/v7.3-rc5/drivers/base/core.c#L3325
- **L27** `Documentation/ABI/testing/sysfs-devices-removable`. https://github.com/torvalds/linux/blob/v7.3-rc5/Documentation/ABI/testing/sysfs-devices-removable
- **L28** Device-specific documents and sources:
  - `Documentation/ABI/testing/sysfs-bus-rbd`: https://github.com/torvalds/linux/blob/v7.3-rc5/Documentation/ABI/testing/sysfs-bus-rbd
  - zram: https://docs.kernel.org/admin-guide/blockdev/zram.html
  - MMC device attributes: https://docs.kernel.org/driver-api/mmc/mmc-dev-attrs.html
  - ublk: https://docs.kernel.org/block/ublk.html
  - `drivers/nvdimm/pmem.c#L505-L517`: https://github.com/torvalds/linux/blob/v7.3-rc5/drivers/nvdimm/pmem.c#L505-L517
- **L29** `include/uapi/linux/fs.h#L273-L308`. https://github.com/torvalds/linux/blob/v7.3-rc5/include/uapi/linux/fs.h#L273-L308
- **L30** `include/uapi/linux/stat.h#L203-L221`. https://github.com/torvalds/linux/blob/v7.3-rc5/include/uapi/linux/stat.h#L203-L221
- **L31** `include/uapi/linux/falloc.h`. https://github.com/torvalds/linux/blob/v7.3-rc5/include/uapi/linux/falloc.h
- **L32** `FALLOC_FL_WRITE_ZEROES`:
  - Commit `7bd43cc79cab` (first in v6.17): https://github.com/torvalds/linux/commit/7bd43cc79cab
  - XFS support: https://github.com/torvalds/linux/blob/v7.3-rc5/fs/xfs/xfs_file.c#L1538-L1597
- **L33** `include/uapi/linux/magic.h`. https://github.com/torvalds/linux/blob/v7.3-rc5/include/uapi/linux/magic.h
- **L34** `fs/iomap/direct-io.c`. https://github.com/torvalds/linux/blob/v7.3-rc5/fs/iomap/direct-io.c (#L440-L510, #L745-L775, #L845-L860)
- **L35** iomap documentation: https://docs.kernel.org/filesystems/iomap/design.html and https://docs.kernel.org/filesystems/iomap/operations.html
- **L36** ext4: https://docs.kernel.org/filesystems/ext4/ifork.html and https://docs.kernel.org/admin-guide/ext4.html
- **L37** `Documentation/filesystems/vfs.rst`, "Handling errors during writeback". https://docs.kernel.org/filesystems/vfs.html
- **L38** `Documentation/core-api/errseq.rst`. https://docs.kernel.org/core-api/errseq.html
- **L39** `Documentation/filesystems/overlayfs.rst`. https://docs.kernel.org/filesystems/overlayfs.html
- **L40** `Documentation/filesystems/fiemap.rst`. https://docs.kernel.org/filesystems/fiemap.html
- **L41** `Documentation/admin-guide/sysctl/kernel.rst` (io_uring_disabled, io_uring_group). https://docs.kernel.org/admin-guide/sysctl/kernel.html
- **L42** io_uring availability:
  - `init/Kconfig#L1960`: https://github.com/torvalds/linux/blob/v7.3-rc5/init/Kconfig#L1960
  - `kernel/sys_ni.c#L51-L53`: https://github.com/torvalds/linux/blob/v7.3-rc5/kernel/sys_ni.c#L51-L53
  - `io_uring/io_uring.c#L3114`: https://github.com/torvalds/linux/blob/v7.3-rc5/io_uring/io_uring.c#L3114
- **L43** tmpfs O_DIRECT, commit `e88e0d366f9c` (merged for v6.6). https://github.com/torvalds/linux/commit/e88e0d366f9cfbb810b0c8509dc5d130d5a53e02
- **L44** `fs/nfs/inode.c` (STATX_DIOALIGN present at v6.18, absent at v6.17). https://github.com/torvalds/linux/blob/v6.18/fs/nfs/inode.c
- **L45** SMB client `f_type`:
  - https://github.com/torvalds/linux/blob/v7.3-rc5/fs/smb/client/smb2ops.c#L3135
  - https://github.com/torvalds/linux/blob/v7.3-rc5/fs/smb/client/smb1ops.c#L1179
- **L46** btrfs:
  - `fs/btrfs/ioctl.c`: https://github.com/torvalds/linux/blob/v7.3-rc5/fs/btrfs/ioctl.c
  - `include/uapi/linux/btrfs.h#L1198-L1200`: https://github.com/torvalds/linux/blob/v7.3-rc5/include/uapi/linux/btrfs.h#L1198-L1200
  - `fs/btrfs/sysfs.c#L2266`: https://github.com/torvalds/linux/blob/v7.3-rc5/fs/btrfs/sysfs.c#L2266
- **L47** `drivers/firmware/dmi-id.c`. https://github.com/torvalds/linux/blob/v7.3-rc5/drivers/firmware/dmi-id.c#L41-L58
- **L48** `Documentation/admin-guide/xfs.rst` (deprecated and removed options table). https://docs.kernel.org/admin-guide/xfs.html
- **L49** iomap direct-I/O users:
  - https://github.com/torvalds/linux/blob/v7.3-rc5/fs/ext4/file.c
  - https://github.com/torvalds/linux/blob/v7.3-rc5/fs/xfs/xfs_file.c
  - https://github.com/torvalds/linux/blob/v7.3-rc5/fs/btrfs/direct-io.c

### man pages (man7.org; Linux man-pages 6.19; liburing pages as mirrored 2026-08-04)

- **M1** statx(2). https://man7.org/linux/man-pages/man2/statx.2.html
- **M2** open(2). https://man7.org/linux/man-pages/man2/open.2.html
- **M3** fsync(2). https://man7.org/linux/man-pages/man2/fsync.2.html
- **M4** sync_file_range(2). https://man7.org/linux/man-pages/man2/sync_file_range.2.html
- **M5** fallocate(2). https://man7.org/linux/man-pages/man2/fallocate.2.html
- **M6** posix_fallocate(3). https://man7.org/linux/man-pages/man3/posix_fallocate.3.html
- **M7** statfs(2). https://man7.org/linux/man-pages/man2/statfs.2.html
- **M8** rename(2). https://man7.org/linux/man-pages/man2/rename.2.html
- **M9** readv(2) (`preadv2`/`pwritev2`, `RWF_*`). https://man7.org/linux/man-pages/man2/readv.2.html
- **M10** syncfs(2). https://man7.org/linux/man-pages/man2/syncfs.2.html
- **M11** proc_pid_mountinfo(5). https://man7.org/linux/man-pages/man5/proc_pid_mountinfo.5.html
- **M12** loop(4). https://man7.org/linux/man-pages/man4/loop.4.html
- **M13** io_uring_setup(2). https://man7.org/linux/man-pages/man2/io_uring_setup.2.html
- **M14** io_uring_enter(2). https://man7.org/linux/man-pages/man2/io_uring_enter.2.html
- **M15** io_uring_register(2). https://man7.org/linux/man-pages/man2/io_uring_register.2.html
- **M16** io_uring(7). https://man7.org/linux/man-pages/man7/io_uring.7.html
- **M17** makedev(3). https://man7.org/linux/man-pages/man3/makedev.3.html

### Apple

- **A1** xnu `bsd/man/man2/fcntl.2`. https://github.com/apple-oss-distributions/xnu/blob/xnu-12377.121.6/bsd/man/man2/fcntl.2
- **A2** xnu `bsd/man/man2/fsync.2`. https://github.com/apple-oss-distributions/xnu/blob/xnu-12377.121.6/bsd/man/man2/fsync.2
- **A3** xnu `bsd/man/man2/statfs.2`. https://github.com/apple-oss-distributions/xnu/blob/xnu-12377.121.6/bsd/man/man2/statfs.2
- **A4** xnu `bsd/man/man2/rename.2`. https://github.com/apple-oss-distributions/xnu/blob/xnu-12377.121.6/bsd/man/man2/rename.2
- **A5** xnu `bsd/sys/fcntl.h`. https://github.com/apple-oss-distributions/xnu/blob/xnu-12377.121.6/bsd/sys/fcntl.h
- **A6** xnu `bsd/sys/disk.h`. https://github.com/apple-oss-distributions/xnu/blob/xnu-12377.121.6/bsd/sys/disk.h
- **A7** xnu `bsd/sys/mount.h`. https://github.com/apple-oss-distributions/xnu/blob/xnu-12377.121.6/bsd/sys/mount.h
- **A8** xnu `bsd/kern/kern_descrip.c`. https://github.com/apple-oss-distributions/xnu/blob/xnu-12377.121.6/bsd/kern/kern_descrip.c (#L3371 `F_PREALLOCATE`, #L3463 `F_PUNCHHOLE`, #L3687 `F_RDAHEAD`, #L3699 `F_NOCACHE`, #L3960-L3990 `F_FULLFSYNC`/`F_BARRIERFSYNC`)
- **A9** xnu `bsd/vfs/vfs_vnops.c`. https://github.com/apple-oss-distributions/xnu/blob/xnu-12377.121.6/bsd/vfs/vfs_vnops.c (#L1339 IO_NOCACHE; #L1360-L1370 O_DSYNC → IO_SYNC)
- **A10** xnu `bsd/vfs/vfs_cluster.c`. https://github.com/apple-oss-distributions/xnu/blob/xnu-12377.121.6/bsd/vfs/vfs_cluster.c (#L282, #L3026, #L3262-L3280, #L6245)
- **A11** xnu `bsd/vfs/vfs_syscalls.c`. https://github.com/apple-oss-distributions/xnu/blob/xnu-12377.121.6/bsd/vfs/vfs_syscalls.c (#L8943-L8999)
- **A12** xnu `iokit/IOKit/IOKitKeys.h`. https://github.com/apple-oss-distributions/xnu/blob/xnu-12377.121.6/iokit/IOKit/IOKitKeys.h
- **A13** `IOStorageDeviceCharacteristics.h`. https://github.com/apple-oss-distributions/IOStorageFamily/blob/IOStorageFamily-337.100.1/IOStorageDeviceCharacteristics.h
- **A14** `IOStorageProtocolCharacteristics.h`. https://github.com/apple-oss-distributions/IOStorageFamily/blob/IOStorageFamily-337.100.1/IOStorageProtocolCharacteristics.h
- **A15** `IOStorage.h`. https://github.com/apple-oss-distributions/IOStorageFamily/blob/IOStorageFamily-337.100.1/IOStorage.h
- **A16** `IOMedia.h`. https://github.com/apple-oss-distributions/IOStorageFamily/blob/IOStorageFamily-337.100.1/IOMedia.h
- **A17** `IOMediaBSDClient.cpp`. https://github.com/apple-oss-distributions/IOStorageFamily/blob/IOStorageFamily-337.100.1/IOMediaBSDClient.cpp (#L1486, #L2175, #L3639)
- **A18** Block storage driver and device:
  - `IOBlockStorageDriver.cpp`: https://github.com/apple-oss-distributions/IOStorageFamily/blob/IOStorageFamily-337.100.1/IOBlockStorageDriver.cpp
  - `IOBlockStorageDevice.h`: https://github.com/apple-oss-distributions/IOStorageFamily/blob/IOStorageFamily-337.100.1/IOBlockStorageDevice.h
- **A19** IOKitUser:
  - `IOKitLib.h`: https://github.com/apple-oss-distributions/IOKitUser/blob/IOKitUser-100231.100.18.0.1/IOKitLib.h
  - `IOKitLib.c`: https://github.com/apple-oss-distributions/IOKitUser/blob/IOKitUser-100231.100.18.0.1/IOKitLib.c
- **A20** hfs:
  - https://github.com/apple-oss-distributions/hfs/blob/hfs-715.100.10/core/hfs_readwrite.c#L2619-L2650
  - https://github.com/apple-oss-distributions/hfs/blob/hfs-715.100.10/core/hfs_vnops.c#L3105-L3245
  - https://github.com/apple-oss-distributions/hfs/blob/hfs-715.100.10/core/hfs_vfsutils.c#L3483-L3540
- **A21** *Apple File System Reference* (2020-06-22). https://developer.apple.com/support/downloads/Apple-File-System-Reference.pdf
- **A22** *Apple File System Guide* (archived):
  - FAQ: https://developer.apple.com/library/archive/documentation/FileManagement/Conceptual/APFS_Guide/FAQ/FAQ.html
  - Features: https://developer.apple.com/library/archive/documentation/FileManagement/Conceptual/APFS_Guide/Features/Features.html
- **A23** macOS SDK 26.4 headers (Command Line Tools): `usr/include/sys/fcntl.h`, `usr/include/sys/attr.h`, `usr/include/sys/syscall.h`, `System/Library/Frameworks/IOKit.framework/Headers/IOKitLib.h`. These are local files; the Apple open-source equivalents are A5 and A19.

### Microsoft (learn.microsoft.com, retrieved 2026-09-28)

- **W1** GetVolumePathNameW. https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getvolumepathnamew
- **W2** GetVolumeNameForVolumeMountPointW. https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getvolumenameforvolumemountpointw
- **W3** Naming a Volume. https://learn.microsoft.com/en-us/windows/win32/fileio/naming-a-volume
- **W4** CreateFileW. https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilew
- **W5** IOCTL_VOLUME_GET_VOLUME_DISK_EXTENTS and its structures:
  - https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ni-winioctl-ioctl_volume_get_volume_disk_extents
  - https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ns-winioctl-volume_disk_extents
  - https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ns-winioctl-disk_extent
- **W6** IOCTL_STORAGE_GET_DEVICE_NUMBER:
  - https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ni-winioctl-ioctl_storage_get_device_number
  - https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ns-winioctl-storage_device_number
- **W7** IOCTL_STORAGE_QUERY_PROPERTY:
  - Win32: https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ni-winioctl-ioctl_storage_query_property
  - WDK: https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntddstor/ni-ntddstor-ioctl_storage_query_property
- **W8** Query input types:
  - STORAGE_PROPERTY_QUERY: https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ns-winioctl-storage_property_query
  - STORAGE_QUERY_TYPE: https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ne-winioctl-storage_query_type
  - STORAGE_PROPERTY_ID: https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ne-winioctl-storage_property_id
- **W9** Device descriptor and bus type:
  - STORAGE_DEVICE_DESCRIPTOR: https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ns-winioctl-storage_device_descriptor
  - STORAGE_BUS_TYPE (Win32): https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ne-winioctl-storage_bus_type
  - STORAGE_BUS_TYPE (WDK): https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntddstor/ne-ntddstor-storage_bus_type
- **W10** DEVICE_SEEK_PENALTY_DESCRIPTOR. https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ns-winioctl-device_seek_penalty_descriptor
- **W11** STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR. https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ns-winioctl-storage_access_alignment_descriptor
- **W12** DEVICE_TRIM_DESCRIPTOR. https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ns-winioctl-device_trim_descriptor
- **W13** STORAGE_WRITE_CACHE_PROPERTY and its enums:
  - https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ns-winioctl-storage_write_cache_property
  - https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ne-winioctl-write_cache_type
  - https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ne-winioctl-write_cache_enable
  - https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ne-winioctl-write_cache_change
  - https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ne-winioctl-write_through
- **W14** Querying for the Write Cache Property (WDK). https://learn.microsoft.com/en-us/windows-hardware/drivers/storage/querying-for-the-write-cache-property
- **W15** Defining I/O Control Codes. https://learn.microsoft.com/en-us/windows-hardware/drivers/kernel/defining-i-o-control-codes
- **W16** Calling DeviceIoControl. https://learn.microsoft.com/en-us/windows/win32/devio/calling-deviceiocontrol
- **W17** Advanced format (4K) disk compatibility update. https://learn.microsoft.com/en-us/windows/win32/w8cookbook/advanced-format--4k--disk-compatibility-update
- **W18** File Buffering. https://learn.microsoft.com/en-us/windows/win32/fileio/file-buffering
- **W19** File Caching. https://learn.microsoft.com/en-us/windows/win32/fileio/file-caching
- **W20** FlushFileBuffers. https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers
- **W21** NtFlushBuffersFileEx. https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntifs/nf-ntifs-ntflushbuffersfileex
- **W22** IRP_MJ_FLUSH_BUFFERS:
  - Kernel: https://learn.microsoft.com/en-us/windows-hardware/drivers/kernel/irp-mj-flush-buffers
  - File systems: https://learn.microsoft.com/en-us/windows-hardware/drivers/ifs/irp-mj-flush-buffers
  - Storage class driver dispatch routines: https://learn.microsoft.com/en-us/windows-hardware/drivers/storage/storage-class-driver-s-dispatch-routines
- **W23** SetFileValidData. https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-setfilevaliddata
- **W24** Valid data length in the kernel:
  - FILE_VALID_DATA_LENGTH_INFORMATION: https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntddk/ns-ntddk-_file_valid_data_length_information
  - CcZeroData: https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntifs/nf-ntifs-cczerodata
- **W25** fsutil file. https://learn.microsoft.com/en-us/windows-server/administration/windows-commands/fsutil-file
- **W26** Asynchronous disk I/O appears as synchronous (KB 156932). https://learn.microsoft.com/en-us/troubleshoot/windows/win32/asynchronous-disk-io-synchronous
- **W27** Allocation and end of file:
  - SetFileInformationByHandle: https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-setfileinformationbyhandle
  - FILE_ALLOCATION_INFO: https://learn.microsoft.com/en-us/windows/win32/api/winbase/ns-winbase-file_allocation_info
  - FILE_END_OF_FILE_INFO: https://learn.microsoft.com/en-us/windows/win32/api/winbase/ns-winbase-file_end_of_file_info
  - SetEndOfFile: https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-setendoffile
- **W28** GetDiskFreeSpaceW. https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getdiskfreespacew
- **W29** Per-handle file information:
  - FILE_STORAGE_INFO: https://learn.microsoft.com/en-us/windows/win32/api/winbase/ns-winbase-file_storage_info
  - GetFileInformationByHandleEx: https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-getfileinformationbyhandleex
  - FILE_INFO_BY_HANDLE_CLASS: https://learn.microsoft.com/en-us/windows/win32/api/minwinbase/ne-minwinbase-file_info_by_handle_class
  - FILE_ALIGNMENT_INFO: https://learn.microsoft.com/en-us/windows/win32/api/winbase/ns-winbase-file_alignment_info
  - FILE_ALIGNMENT_INFORMATION (WDK): https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntddk/ns-ntddk-_file_alignment_information
- **W30** Volume sector-size information:
  - FILE_FS_SECTOR_SIZE_INFORMATION: https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntddk/ns-ntddk-_file_fs_sector_size_information
  - NtQueryVolumeInformationFile: https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntifs/nf-ntifs-ntqueryvolumeinformationfile
  - FS_INFORMATION_CLASS: https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/wdm/ne-wdm-_fsinfoclass
- **W31** MoveFileExW. https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-movefileexw
- **W32** Rename information:
  - FILE_RENAME_INFO: https://learn.microsoft.com/en-us/windows/win32/api/winbase/ns-winbase-file_rename_info
  - FILE_RENAME_INFORMATION (WDK): https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntifs/ns-ntifs-_file_rename_information
- **W33** GetDriveTypeW. https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getdrivetypew
- **W34** GetVolumeInformationByHandleW. https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getvolumeinformationbyhandlew
- **W35** Obtaining a Handle to a Directory. https://learn.microsoft.com/en-us/windows/win32/fileio/obtaining-a-handle-to-a-directory
- **W36** IoRing:
  - CreateIoRing: https://learn.microsoft.com/en-us/windows/win32/api/ioringapi/nf-ioringapi-createioring
  - IORING_OP_CODE: https://learn.microsoft.com/en-us/windows/win32/api/ntioring_x/ne-ntioring_x-ioring_op_code
  - IsIoRingOpSupported: https://learn.microsoft.com/en-us/windows/win32/api/ioringapi/nf-ioringapi-isioringopsupported
- **W37** SQL Server I/O fundamentals. https://learn.microsoft.com/en-us/sql/relational-databases/sql-server-storage-guide
- **W38** Restricted direct disk and volume access. https://learn.microsoft.com/en-us/previous-versions/windows/hardware/design/dn653576(v=vs.85)
- **W39** Privilege constants. https://learn.microsoft.com/en-us/windows/win32/secauthz/privilege-constants
- **[ms-meta]** `windows-sys` 0.61.2, generated from Windows SDK metadata by Microsoft's `windows-rs` project. https://github.com/microsoft/windows-rs and https://crates.io/crates/windows-sys/0.61.2. We read these files:
  - `src/Windows/Win32/System/Ioctl/mod.rs`
  - `src/Windows/Win32/Storage/FileSystem/mod.rs`
  - `src/Windows/Win32/System/SystemServices/mod.rs`
  - `src/Windows/Win32/System/WindowsProgramming/mod.rs`
  - `src/Windows/Wdk/Storage/FileSystem/mod.rs`

### Other

- **O1** OpenZFS `include/sys/fs/zfs.h#L1561` (`ZFS_SUPER_MAGIC`). This is outside the owner's list of allowed sources and is cited only because no kernel.org source defines the value. https://github.com/openzfs/zfs/blob/master/include/sys/fs/zfs.h
