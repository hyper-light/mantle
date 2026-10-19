//! The fixed facts of the machine, queried from the OS rather than measured: page sizes, cache
//! line, cores with their classes and NUMA nodes and L2 sizes, memory totals, address space, and
//! the power state. Each fact names its source per platform; a fact the OS will not give is
//! recorded as unknown with a note, never guessed silently (D-11).
//!
//! Sources:
//! - macOS: `sysctlbyname` (`hw.pagesize`, `hw.cachelinesize`, `hw.nperflevels`,
//!   `hw.perflevelN.*`, `hw.memsize`, `machdep.cpu.brand_string`, `kern.osversion`),
//!   `host_statistics64` for free memory, IOKit's power-source snapshot for the power state
//!   [B: Apple sysctl(3), IOPSCopyPowerSourcesInfo].
//! - Linux: `sysconf`, `sysinfo(2)`, `uname(2)`, and the kernel's pseudo-files under `/sys` and
//!   `/proc` (read as queries; never written) [B: man sysconf(3), man 5 proc, Documentation/ABI/].
//! - Windows: `GetSystemInfo`, `GetLargePageMinimum`, `GlobalMemoryStatusEx`,
//!   `GetLogicalProcessorInformation`, `GetSystemPowerStatus` [B: Microsoft Learn].
#![allow(unsafe_code)]

/// Which class a core belongs to, when the OS says.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoreClass {
    /// The tier above performance where the OS has one (Apple's "Super" level).
    Super,
    /// A performance core.
    Performance,
    /// An efficiency core.
    Efficiency,
    /// The OS did not say; the profile treats it as performance and notes the gap.
    Unknown,
}

/// One logical core.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CoreFacts {
    /// The OS's id for the core (the id affinity calls take).
    pub id: u32,
    /// Its class.
    pub class: CoreClass,
    /// The OS's performance level, 0 the fastest (Apple's perflevel index; capacity rank on Linux).
    pub level: u32,
    /// Its NUMA node (0 on machines with one).
    pub numa: u32,
    /// The L2 cache it sees, in bytes (0 when unknown).
    pub l2_bytes: u64,
}

/// Page facts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PageFacts {
    /// The base page size in bytes.
    pub base: u64,
    /// Huge page sizes the OS offers explicitly, ascending (empty when none).
    pub huge: Vec<u64>,
    /// Whether transparent huge pages can be requested (Linux `madvise`; false elsewhere).
    pub transparent_huge: bool,
    /// The mapping granularity (equals the base page except on Windows, where it is coarser).
    pub allocation_granularity: u64,
}

/// Memory facts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MemoryFacts {
    /// Physical memory in bytes.
    pub total: u64,
    /// Memory available to a new allocation without reclaim, in bytes, as the OS estimates it,
    /// on Linux within the room the process's cgroups leave it ([`cgroup_room`]).
    pub available: u64,
    /// The width of a user address, in bits.
    pub address_bits: u32,
    /// The tightest bound an OS, job or cgroup sets on this process's memory, if any (§4.2
    /// "effective capacity ... constrained by OS/job/cgroup/lock limits"): on Linux the smallest
    /// `memory.max` or `memory.high` (cgroup v2: past the one the kernel kills, past the other it
    /// throttles) or `memory.limit_in_bytes` (v1) up the process's cgroup path, and on
    /// every Unix a finite `RLIMIT_AS` or `RLIMIT_DATA`; Windows reports none yet (the job object's
    /// limit is owed). `None` when nothing binds; a bound above `total` is kept and clamped by
    /// [`effective_capacity`]. Absent from a profile written before the field existed.
    pub limit: Option<u64>,
}

/// The effective capacity a host's admission is over (§4.2, D-12): its physical memory, clamped to
/// the tightest OS/job/cgroup bound when one is set below it. Raw free memory never enters — it is
/// a fluctuating snapshot (docs/bugs/2026-09-10-provisioning-budget-from-mlock-limit.md), whereas
/// total and a configured bound are stable.
pub fn effective_capacity(total: u64, limit: Option<u64>) -> u64 {
    limit.map_or(total, |bound| bound.min(total))
}

/// A cgroup memory limit file's text as a bound: a decimal byte count, or none for `max` (v2's
/// "unlimited"), an empty file, or anything else — a value that cannot be read is no bound, never
/// a guessed one. Hostile text (a sign, a suffix, a number past `u64`) is refused the same way.
pub fn parse_cgroup_limit(text: &str) -> Option<u64> {
    let text = text.trim();
    if text.is_empty() || text == "max" || !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    text.parse::<u64>().ok()
}

/// The tightest of several optional bounds.
fn tightest(bounds: impl IntoIterator<Item = Option<u64>>) -> Option<u64> {
    bounds.into_iter().flatten().min()
}

/// A file's text, trimmed; none when it cannot be read. The cgroup files it reads are queries of
/// the kernel, never disk reads.
#[cfg_attr(not(any(target_os = "linux", test)), allow(dead_code))]
fn read_text(path: &str) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|s| s.trim().to_owned())
}

/// The memory controller's cgroup directories a `/proc/self/cgroup` text places a process under, in
/// the cgroup file system mounted at `mount` (`/sys/fs/cgroup`, v1's memory hierarchy at its
/// `memory`), its own first and its namespace's root last, each marked when it is v1's: a line is
/// `id:controllers:path`, v2's with no controllers (cgroup(7)), and a parent's limit binds its
/// children, so every ancestor is listed. Lines naming neither are passed over.
#[cfg_attr(not(any(target_os = "linux", test)), allow(dead_code))]
fn memory_dirs_of(cgroup: &str, mount: &str) -> Vec<(String, bool)> {
    let mut dirs = Vec::new();
    for line in cgroup.lines() {
        // Format: three colon-separated fields (the path may itself hold no colon; the split is
        // bounded to three so a hostile path cannot).
        let mut fields = line.splitn(3, ':');
        let (Some(_), Some(controllers), Some(path)) =
            (fields.next(), fields.next(), fields.next())
        else {
            continue;
        };
        let (root, v1) = if controllers.is_empty() {
            (mount.to_owned(), false)
        } else if controllers.split(',').any(|c| c == "memory") {
            (format!("{mount}/memory"), true)
        } else {
            continue;
        };
        // Each step cuts the path at its last separator, so the walk ends at the root.
        let mut dir = path.trim_end_matches('/').to_owned();
        loop {
            dirs.push((format!("{root}{dir}"), v1));
            match dir.rfind('/') {
                Some(cut) => dir.truncate(cut),
                None => break,
            }
        }
    }
    dirs
}

/// The most a cgroup directory lets its processes hold before the kernel acts on them: v2's
/// `memory.max`, past which it reclaims and then kills, and `memory.high`, past which it throttles
/// them under heavy reclaim, the lower (cgroup-v2.rst; the ceiling the kernel measures a cgroup's
/// headroom to, mm/memcontrol.c `mem_cgroup_wb_stats`); v1's `memory.limit_in_bytes`. None when no
/// file there reads as a bound.
#[cfg_attr(not(any(target_os = "linux", test)), allow(dead_code))]
fn memory_ceiling(dir: &str, v1: bool) -> Option<u64> {
    let bound =
        |file: &str| read_text(&format!("{dir}/{file}")).and_then(|t| parse_cgroup_limit(&t));
    if v1 {
        bound("memory.limit_in_bytes")
    } else {
        tightest([bound("memory.max"), bound("memory.high")])
    }
}

/// The tightest memory limit a `/proc/self/cgroup` text's cgroups set, in the cgroup file system
/// mounted at `mount`: the least ceiling ([`memory_ceiling`]) of the process's cgroup and every
/// ancestor. None when none sets one or none can be read.
#[cfg_attr(not(any(target_os = "linux", test)), allow(dead_code))]
fn least_ceiling(cgroup: &str, mount: &str) -> Option<u64> {
    memory_dirs_of(cgroup, mount)
        .iter()
        .filter_map(|(dir, v1)| memory_ceiling(dir, *v1))
        .min()
}

/// The least of `machine`, the OS's estimate of the memory the machine has for a new allocation,
/// and the room each cgroup with a ceiling over the process leaves it ([`cgroup_room`]), from the
/// cgroup's usage (`memory.current`, v1's `memory.usage_in_bytes`) and its `memory.stat`: a
/// machine with memory free may have none the process's cgroups let it take. A cgroup's room is at
/// least its headroom, the ceiling less the usage, so a cgroup whose headroom is no less than the
/// least found binds nothing and its `memory.stat` is not read; one whose `memory.stat` cannot be
/// read is its headroom, all the room known; one whose usage cannot be read lowers nothing.
#[cfg_attr(not(any(target_os = "linux", test)), allow(dead_code))]
fn within_cgroups(machine: Option<u64>, cgroup: &str, mount: &str) -> Option<u64> {
    let mut least = machine;
    for (dir, v1) in memory_dirs_of(cgroup, mount) {
        let Some(ceiling) = memory_ceiling(&dir, v1) else {
            continue;
        };
        let usage = if v1 {
            "memory.usage_in_bytes"
        } else {
            "memory.current"
        };
        let Some(usage) = read_text(&format!("{dir}/{usage}")).and_then(|t| parse_cgroup_limit(&t))
        else {
            continue;
        };
        // No headroom past the ceiling, as the kernel's own `min` gives none.
        let headroom = ceiling.saturating_sub(usage);
        if least.is_some_and(|l| headroom >= l) {
            continue;
        }
        let room = read_text(&format!("{dir}/memory.stat"))
            .and_then(|stat| cgroup_room(ceiling, usage, &stat, v1))
            .unwrap_or(headroom);
        least = tightest([least, Some(room)]);
    }
    least
}

/// The memory a cgroup leaves a new allocation, from its ceiling (the lower of `memory.max` and
/// `memory.high`, v1's `memory.limit_in_bytes`), its usage (`memory.current`, v1's
/// `memory.usage_in_bytes`) and its `memory.stat` text: the headroom under the ceiling, none past
/// it, as the kernel measures a cgroup's headroom for its own dirty-memory bound (mm/memcontrol.c
/// `mem_cgroup_wb_stats`), and the cgroup's file pages that are clean, which reclaim takes back
/// before a charge to the cgroup fails, where dirty pages and pages under writeback wait to be
/// written first (mm/page-writeback.c `mdtc_calc_avail` and `domain_dirty_avail`: the file pages
/// less the dirty and writeback ones). `v1` reads v1's hierarchical counts (`total_active_file` and
/// the rest; cgroup-v1/memory.rst §5.2). None when the text lacks a count or holds one that is not
/// a byte count: a reading that cannot be made is none, never a guessed one.
pub fn cgroup_room(ceiling: u64, usage: u64, stat: &str, v1: bool) -> Option<u64> {
    let names = if v1 {
        [
            "total_active_file",
            "total_inactive_file",
            "total_dirty",
            "total_writeback",
        ]
    } else {
        [
            "active_file",
            "inactive_file",
            "file_dirty",
            "file_writeback",
        ]
    };
    let [active, inactive, dirty, writeback] = names.map(|name| {
        stat.lines()
            .find_map(|line| {
                let mut fields = line.split_whitespace();
                (fields.next() == Some(name))
                    .then(|| fields.next())
                    .flatten()
            })
            .and_then(parse_cgroup_limit)
    });
    let file = active?.checked_add(inactive?)?;
    let unclean = dirty?.checked_add(writeback?)?;
    // Saturating as the kernel's own `min`s are: no headroom past the ceiling, and no clean pages
    // when the dirty ones, counted at another instant, are more.
    let headroom = ceiling.saturating_sub(usage);
    let clean = file.saturating_sub(unclean);
    headroom.checked_add(clean)
}

/// The CPU time a cgroup grants the process: `quota_us` microseconds of CPU in every `period_us`
/// microseconds (cgroup v2 `cpu.max`; v1 `cpu.cfs_quota_us` over `cpu.cfs_period_us`) [B: kernel
/// Documentation/admin-guide/cgroup-v2.rst "cpu.max"; Documentation/scheduler/sched-bwc.rst].
///
/// It is a share of time on the cores the process may run on, not a claim on any one of them (§4.3): a
/// container given two CPUs of an eighteen-CPU pool may run on all eighteen and shares every one with
/// the pool's other tenants. So it bounds how many shards can run at once without being throttled, and it
/// says whether the process owns its cores at all ([`CpuBudget::covers`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CpuBudget {
    /// Microseconds of CPU time the process's threads may use together in each period.
    pub quota_us: u64,
    /// The accounting period, microseconds.
    pub period_us: u64,
}

impl CpuBudget {
    /// Whether the budget buys `cores` CPUs at once (`quota ≥ period × cores`): whether every core of a set
    /// that size can run a thread of this process for the whole period without the quota stopping it. A
    /// product past `u128` covers nothing.
    pub fn covers(&self, cores: usize) -> bool {
        let Ok(cores) = u128::try_from(cores) else {
            return false;
        };
        u128::from(self.period_us)
            .checked_mul(cores)
            .is_some_and(|needed| u128::from(self.quota_us) >= needed)
    }

    /// The whole CPUs the budget buys at once, `⌊quota / period⌋`: the threads that can run together through
    /// a whole period. A zero period (never written by a kernel; hostile) grants no bound.
    pub fn whole_cpus(&self) -> u64 {
        self.quota_us
            .checked_div(self.period_us)
            .unwrap_or(u64::MAX)
    }

    /// The tighter of two budgets: the smaller share of time, compared by cross-multiplying so no rounding
    /// decides it.
    // Only the Linux cgroup walk and the tests call it; the other platforms keep it compiled.
    #[cfg_attr(not(any(target_os = "linux", test)), allow(dead_code))]
    fn tighter(self, other: CpuBudget) -> CpuBudget {
        let mine = u128::from(self.quota_us).saturating_mul(u128::from(other.period_us));
        let theirs = u128::from(other.quota_us).saturating_mul(u128::from(self.period_us));
        if theirs < mine { other } else { self }
    }
}

/// A cgroup v2 `cpu.max` file's text as a budget: `QUOTA PERIOD`, two decimal microsecond counts. `max`
/// (no quota), a missing period, a zero, and hostile text (signs, suffixes, a number past `u64`, a third
/// field) are no budget — never a guessed one.
pub fn parse_cpu_max(text: &str) -> Option<CpuBudget> {
    let mut fields = text.split_whitespace();
    let (Some(quota), Some(period), None) = (fields.next(), fields.next(), fields.next()) else {
        return None;
    };
    cpu_budget_of(quota, period)
}

/// A cgroup v1 pair of `cpu.cfs_quota_us` and `cpu.cfs_period_us` texts as a budget; a quota of `-1` (no
/// quota) or any text `parse_cpu_max` refuses is no budget.
pub fn parse_cfs_budget(quota: &str, period: &str) -> Option<CpuBudget> {
    cpu_budget_of(quota.trim(), period.trim())
}

/// A budget from two decimal microsecond counts, both non-zero.
fn cpu_budget_of(quota: &str, period: &str) -> Option<CpuBudget> {
    let count = |text: &str| {
        text.bytes()
            .all(|b| b.is_ascii_digit())
            .then(|| text.parse::<u64>().ok())
            .flatten()
            .filter(|value| *value > 0)
    };
    Some(CpuBudget {
        quota_us: count(quota)?,
        period_us: count(period)?,
    })
}

/// The tightest of several optional budgets (a parent cgroup's quota binds its children).
#[cfg_attr(not(any(target_os = "linux", test)), allow(dead_code))]
fn tightest_budget(budgets: impl IntoIterator<Item = Option<CpuBudget>>) -> Option<CpuBudget> {
    budgets.into_iter().flatten().reduce(CpuBudget::tighter)
}

/// A finite `RLIMIT_AS` or `RLIMIT_DATA` on this process (the smaller), as a memory bound; none
/// when both are unlimited or the query is refused.
#[cfg(unix)]
fn rlimit_bound() -> Option<u64> {
    use rustix::process::{Resource, getrlimit};
    tightest([
        getrlimit(Resource::As).current,
        getrlimit(Resource::Data).current,
    ])
}

/// Windows has no rlimit; the job object's memory limit is the owed counterpart.
#[cfg(not(unix))]
fn rlimit_bound() -> Option<u64> {
    None
}

/// The power state that gates re-measurement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PowerState {
    /// Mains or equivalent.
    Mains,
    /// Running on a battery.
    Battery,
    /// The OS did not say (a desktop without a power supply entry, or a refused query).
    Unknown,
}

/// The hardware and OS identity the cache is keyed by.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity {
    /// The CPU model string as the OS reports it.
    pub cpu: String,
    /// The OS name and build.
    pub os: String,
    /// The target architecture.
    pub arch: String,
    /// Number of logical cores.
    pub cores: u32,
    /// Physical memory in bytes.
    pub memory: u64,
    /// The base page size.
    pub page: u64,
}

impl Identity {
    /// A stable single-line rendering, hashed for the cache key and shown in `ProfileStale`.
    pub fn line(&self) -> String {
        format!(
            "{} | {} | {} | {} cores | {} bytes | {} page",
            self.cpu, self.os, self.arch, self.cores, self.memory, self.page
        )
    }
}

/// Every fixed fact together.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Facts {
    /// The identity.
    pub identity: Identity,
    /// Pages.
    pub page: PageFacts,
    /// The cache line in bytes.
    pub cache_line: u64,
    /// The cores this process may run on, by the OS's ids (its affinity mask where the OS keeps one:
    /// `sched_getaffinity` on Linux, the process affinity mask on Windows; every core on macOS, which keeps
    /// none), each with its class. Not `0..available_parallelism()`: that count is lowered by a CPU quota and
    /// names no core (docs/bugs/2026-09-29-every-daemon-under-a-cpu-quota-pinned-its-shard-to-cpu-1.md).
    pub cores: Vec<CoreFacts>,
    /// The CPU time a cgroup grants the process, when one bounds it ([`CpuBudget`]); `None` when nothing
    /// does (no quota, macOS, and Windows, whose job-object CPU rate limit is owed like its memory limit).
    pub cpu_budget: Option<CpuBudget>,
    /// Memory.
    pub memory: MemoryFacts,
    /// The power state at the time of the query.
    pub power: PowerState,
    /// Facts the OS would not give, each with what was recorded instead.
    pub notes: Vec<String>,
}

/// The Windows process affinity mask, shared with the probes that give a pinned thread its mask back.
#[cfg(windows)]
pub(crate) use platform::process_affinity_mask;

impl Facts {
    /// Memory available to a new allocation right now, as the OS estimates it, on Linux within the
    /// room the process's cgroups leave it (a few pseudo-file reads; the pressure source of dynamic
    /// quotas asks this before each growth, §4.2).
    pub fn memory_available_now() -> Option<u64> {
        platform::available_bytes()
    }

    /// This process's resident memory right now (one cheap query): what the daemon itself holds in RAM, its content,
    /// metadata and heap together, so a memory-pressure sampler can tell the host's shortfall that the daemon caused
    /// from pressure that comes from elsewhere (§4.2). `None` where the OS gives no reading.
    pub fn resident_now() -> Option<u64> {
        platform::resident_bytes()
    }

    /// Every fact, queried now (the boot profile's input).
    pub fn query() -> Facts {
        let mut notes = Vec::new();
        let page = platform::page(&mut notes);
        let cache_line = platform::cache_line(&mut notes);
        let cores = platform::cores(&mut notes);
        let cpu_budget = platform::cpu_budget(&mut notes);
        let mut memory = platform::memory(&mut notes);
        memory.limit = tightest([platform::cgroup_bound(&mut notes), rlimit_bound()]);
        let power = platform::power(&mut notes);
        let (cpu, os) = platform::identity(&mut notes);
        let identity = Identity {
            cpu,
            os,
            arch: std::env::consts::ARCH.to_owned(),
            cores: u32::try_from(cores.len()).unwrap_or(u32::MAX),
            memory: memory.total,
            page: page.base,
        };
        Facts {
            identity,
            page,
            cache_line,
            cores,
            cpu_budget,
            memory,
            power,
            notes,
        }
    }

    /// How many threads of this process can run at once: its cores, capped by the whole CPUs its budget buys
    /// ([`CpuBudget::whole_cpus`]) — a quota below the cpuset runs fewer threads than the cpuset has cores.
    pub fn cpus_at_once(&self) -> usize {
        let buys = self.cpu_budget.map_or(usize::MAX, |budget| {
            usize::try_from(budget.whole_cpus()).unwrap_or(usize::MAX)
        });
        self.cores.len().min(buys)
    }

    /// The largest L2 any core reports (0 when unknown).
    pub fn largest_l2(&self) -> u64 {
        self.cores.iter().map(|c| c.l2_bytes).max().unwrap_or(0)
    }
}

/// What the OS reports as locked (wired) for this process, for cross-checking a locking
/// sequence against the OS (AC-0.5): Linux `VmLck` from `/proc/self/status`, macOS `ri_wired_size`
/// from `proc_pid_rusage`; `None` where the OS has no such report.
pub fn locked_bytes() -> Option<u64> {
    platform::locked_bytes()
}

/// Shape: the largest cache line on any target we build for (Apple silicon's 128 bytes), used
/// only when the OS refuses to say; crossbeam's `CachePadded` uses the same fallback reasoning.
pub const CACHE_LINE_FALLBACK: u64 = 128;

/// The number of logical cores the OS lets this process use: macOS's core list when its performance
/// levels are refused (macOS keeps no affinity mask, so every logical CPU number is one the process may
/// run on). Never a source of core ids elsewhere: a CPU quota lowers the count and it names no core.
#[cfg(target_os = "macos")]
fn parallelism() -> u32 {
    std::thread::available_parallelism()
        .map(|n| u32::try_from(n.get()).unwrap_or(u32::MAX))
        .unwrap_or(1)
}

/// Parses a decimal integer from text that may carry a unit suffix (`K`, `M`, `kB`).
// Only the Linux sysfs queries and the tests call it; the other platforms keep it compiled.
#[cfg_attr(not(any(target_os = "linux", test)), allow(dead_code))]
fn parse_size(text: &str) -> Option<u64> {
    let text = text.trim();
    let digits: String = text.chars().take_while(char::is_ascii_digit).collect();
    let value: u64 = digits.parse().ok()?;
    let suffix = text.get(digits.len()..)?.trim().to_ascii_lowercase();
    /// Format: binary prefixes as the kernel's pseudo-files print them.
    const KIB: u64 = 1024;
    let multiplier = match suffix.as_str() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => KIB,
        "m" | "mb" | "mib" => KIB * KIB,
        "g" | "gb" | "gib" => KIB * KIB * KIB,
        _ => return None,
    };
    value.checked_mul(multiplier)
}

#[cfg(target_os = "macos")]
mod platform {
    use super::{
        CACHE_LINE_FALLBACK, CoreClass, CoreFacts, MemoryFacts, PageFacts, PowerState, parallelism,
    };
    use std::ffi::{CStr, c_void};

    pub(crate) fn sysctl_u64(name: &CStr) -> Option<u64> {
        let mut value: u64 = 0;
        let mut len = std::mem::size_of::<u64>();
        // SAFETY: `name` is NUL-terminated; `value` is a writable u64 and `len` its size; sysctl
        // writes at most `len` bytes and updates `len` to the number written.
        let rc = unsafe {
            libc::sysctlbyname(
                name.as_ptr(),
                (&raw mut value).cast::<c_void>(),
                &raw mut len,
                std::ptr::null_mut(),
                0,
            )
        };
        if rc != 0 {
            return None;
        }
        // A four-byte answer sits in the low bytes on this little-endian target.
        Some(if len == std::mem::size_of::<u32>() {
            value & u64::from(u32::MAX)
        } else {
            value
        })
    }

    fn sysctl_string(name: &CStr) -> Option<String> {
        let mut len: usize = 0;
        // SAFETY: a null buffer with a zero length asks sysctl for the size only.
        let rc = unsafe {
            libc::sysctlbyname(
                name.as_ptr(),
                std::ptr::null_mut(),
                &raw mut len,
                std::ptr::null_mut(),
                0,
            )
        };
        if rc != 0 || len == 0 {
            return None;
        }
        let mut buf = vec![0u8; len];
        // SAFETY: `buf` has exactly `len` writable bytes.
        let rc = unsafe {
            libc::sysctlbyname(
                name.as_ptr(),
                buf.as_mut_ptr().cast::<c_void>(),
                &raw mut len,
                std::ptr::null_mut(),
                0,
            )
        };
        if rc != 0 {
            return None;
        }
        buf.truncate(len);
        let end = buf.iter().position(|b| *b == 0).unwrap_or(buf.len());
        Some(String::from_utf8_lossy(buf.get(..end)?).into_owned())
    }

    pub(super) fn page(notes: &mut Vec<String>) -> PageFacts {
        let base = sysctl_u64(c"hw.pagesize").unwrap_or_else(|| {
            notes.push("hw.pagesize refused; recorded the C library's page size".to_owned());
            // SAFETY: sysconf has no preconditions.
            u64::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) }).unwrap_or(0)
        });
        // macOS offers no superpages to user programs (Appendix C), so `huge` stays empty.
        PageFacts {
            base,
            huge: Vec::new(),
            transparent_huge: false,
            allocation_granularity: base,
        }
    }

    pub(super) fn cache_line(notes: &mut Vec<String>) -> u64 {
        sysctl_u64(c"hw.cachelinesize")
            .filter(|v| *v > 0)
            .unwrap_or_else(|| {
                notes.push(format!(
                    "hw.cachelinesize refused; recorded the {CACHE_LINE_FALLBACK}-byte fallback"
                ));
                CACHE_LINE_FALLBACK
            })
    }

    pub(super) fn cores(notes: &mut Vec<String>) -> Vec<CoreFacts> {
        let levels = sysctl_u64(c"hw.nperflevels").unwrap_or(0);
        let mut cores = Vec::new();
        let mut next_id: u32 = 0;
        for level in 0..levels {
            let count = sysctl_u64(&level_key(level, "logicalcpu")).unwrap_or(0);
            let name = sysctl_string(&level_key(level, "name")).unwrap_or_default();
            let l2 = sysctl_u64(&level_key(level, "l2cachesize")).unwrap_or(0);
            let class = match name.as_str() {
                "Super" => CoreClass::Super,
                "Performance" => CoreClass::Performance,
                "Efficiency" => CoreClass::Efficiency,
                _ => CoreClass::Unknown,
            };
            if class == CoreClass::Unknown {
                notes.push(format!(
                    "hw.perflevel{level}.name is {name:?}; recorded unknown class"
                ));
            }
            let level = u32::try_from(level).unwrap_or(u32::MAX);
            for _ in 0..count {
                cores.push(CoreFacts {
                    id: next_id,
                    class,
                    level,
                    numa: 0,
                    l2_bytes: l2,
                });
                next_id = next_id.saturating_add(1);
            }
        }
        if cores.is_empty() {
            notes.push("hw.nperflevels refused; recorded every core as unknown class".to_owned());
            cores = (0..parallelism())
                .map(|id| CoreFacts {
                    id,
                    class: CoreClass::Unknown,
                    level: 0,
                    numa: 0,
                    l2_bytes: 0,
                })
                .collect();
        }
        cores
    }

    fn level_key(level: u64, leaf: &str) -> std::ffi::CString {
        std::ffi::CString::new(format!("hw.perflevel{level}.{leaf}")).unwrap_or_default()
    }

    pub(super) fn memory(notes: &mut Vec<String>) -> MemoryFacts {
        let total = sysctl_u64(c"hw.memsize").unwrap_or_else(|| {
            notes.push("hw.memsize refused; recorded 0".to_owned());
            0
        });
        let available = available_bytes().unwrap_or_else(|| {
            notes.push("host_statistics64 refused; recorded total as available".to_owned());
            total
        });
        MemoryFacts {
            total,
            available,
            address_bits: usize::BITS,
            limit: None,
        }
    }

    /// macOS has no cgroups; the process's memory bound is its rlimits alone.
    pub(super) fn cgroup_bound(_notes: &mut Vec<String>) -> Option<u64> {
        None
    }

    /// macOS has no CPU quota a process can be placed under (no cgroups; `taskpolicy` changes priority,
    /// not time): nothing bounds the process's CPU time.
    pub(super) fn cpu_budget(_notes: &mut Vec<String>) -> Option<super::CpuBudget> {
        None
    }

    // libc marks mach_host_self deprecated in favour of the mach2 crate; one declaration here
    // keeps the dependency set small (the symbol is in libSystem on every macOS we build for).
    unsafe extern "C" {
        fn mach_host_self() -> libc::mach_port_t;
    }

    pub(super) fn resident_bytes() -> Option<u64> {
        rusage_v0().map(|info| info.resident_size)
    }

    pub(super) fn available_bytes() -> Option<u64> {
        let page = sysctl_u64(c"hw.pagesize")?;
        // SAFETY: an all-zero libc::vm_statistics64 is a valid, if empty, value for the call below to fill.
        let mut stats: libc::vm_statistics64 = unsafe { std::mem::zeroed() };
        let mut count = libc::HOST_VM_INFO64_COUNT;
        // SAFETY: `stats` is a writable vm_statistics64 and `count` says how many integers it
        // holds; the host port is the caller's own.
        let rc = unsafe {
            libc::host_statistics64(
                mach_host_self(),
                libc::HOST_VM_INFO64,
                (&raw mut stats).cast::<libc::integer_t>(),
                &raw mut count,
            )
        };
        if rc != libc::KERN_SUCCESS {
            return None;
        }
        // Free, inactive, speculative and purgeable pages are what a new allocation can take
        // without swapping (Apple's own "memory available" arithmetic).
        let pages = u64::from(stats.free_count)
            .saturating_add(u64::from(stats.inactive_count))
            .saturating_add(u64::from(stats.speculative_count))
            .saturating_add(u64::from(stats.purgeable_count));
        Some(pages.saturating_mul(page))
    }

    #[link(name = "IOKit", kind = "framework")]
    unsafe extern "C" {
        fn IOPSCopyPowerSourcesInfo() -> *const c_void;
        fn IOPSGetProvidingPowerSourceType(snapshot: *const c_void) -> *const c_void;
    }

    #[link(name = "CoreFoundation", kind = "framework")]
    unsafe extern "C" {
        fn CFStringGetCString(
            string: *const c_void,
            buffer: *mut libc::c_char,
            size: isize,
            encoding: u32,
        ) -> u8;
        fn CFRelease(cf: *const c_void);
    }

    /// Format: kCFStringEncodingUTF8.
    const CF_UTF8: u32 = 0x0800_0100;

    pub(super) fn power(notes: &mut Vec<String>) -> PowerState {
        // SAFETY: the snapshot is a CF object we own and release below; the providing-type string is
        // owned by the snapshot (a "Get" call) and is read before the release.
        let state = unsafe {
            let snapshot = IOPSCopyPowerSourcesInfo();
            if snapshot.is_null() {
                None
            } else {
                let kind = IOPSGetProvidingPowerSourceType(snapshot);
                // Shape: room for the longest power-source name IOKit reports ("Battery Power").
                let mut buf = [0 as libc::c_char; 64];
                let ok = !kind.is_null()
                    && CFStringGetCString(
                        kind,
                        buf.as_mut_ptr(),
                        isize::try_from(buf.len()).unwrap_or(0),
                        CF_UTF8,
                    ) != 0;
                let text = if ok {
                    CStr::from_ptr(buf.as_ptr()).to_string_lossy().into_owned()
                } else {
                    String::new()
                };
                CFRelease(snapshot);
                Some(text)
            }
        };
        match state.as_deref() {
            Some("AC Power") => PowerState::Mains,
            Some("Battery Power") | Some("UPS Power") => PowerState::Battery,
            other => {
                notes.push(format!(
                    "power source query gave {other:?}; recorded unknown"
                ));
                PowerState::Unknown
            }
        }
    }

    #[repr(C)]
    struct RusageInfoV0 {
        // Format: the layout of rusage_info_v0 (sys/resource.h): a 16-byte uuid, then ten u64s.
        uuid: [u8; 16],
        user_time: u64,
        system_time: u64,
        pkg_idle_wkups: u64,
        interrupt_wkups: u64,
        pageins: u64,
        wired_size: u64,
        resident_size: u64,
        phys_footprint: u64,
        proc_start_abstime: u64,
        proc_exit_abstime: u64,
    }

    unsafe extern "C" {
        fn proc_pid_rusage(
            pid: libc::c_int,
            flavor: libc::c_int,
            buffer: *mut RusageInfoV0,
        ) -> libc::c_int;
    }

    /// This process's `rusage_info_v0`, or `None` when the OS refuses it.
    fn rusage_v0() -> Option<RusageInfoV0> {
        /// Format: RUSAGE_INFO_V0 (sys/resource.h).
        const RUSAGE_INFO_V0: libc::c_int = 0;
        // SAFETY: an all-zero rusage_info_v0 is a valid buffer for the call to fill.
        let mut info: RusageInfoV0 = unsafe { std::mem::zeroed() };
        // SAFETY: our own pid, the V0 flavor, and a writable buffer of the V0 layout.
        let rc = unsafe { proc_pid_rusage(libc::getpid(), RUSAGE_INFO_V0, &raw mut info) };
        (rc == 0).then_some(info)
    }

    pub(super) fn locked_bytes() -> Option<u64> {
        rusage_v0().map(|info| info.wired_size)
    }

    pub(super) fn identity(notes: &mut Vec<String>) -> (String, String) {
        let cpu = sysctl_string(c"machdep.cpu.brand_string").unwrap_or_else(|| {
            notes.push("machdep.cpu.brand_string refused; recorded hw.model".to_owned());
            sysctl_string(c"hw.model").unwrap_or_else(|| "unknown cpu".to_owned())
        });
        let build = sysctl_string(c"kern.osversion").unwrap_or_else(|| "unknown build".to_owned());
        let release = sysctl_string(c"kern.osproductversion").unwrap_or_else(|| "macOS".to_owned());
        (cpu, format!("macOS {release} ({build})"))
    }
}

#[cfg(target_os = "linux")]
mod platform {
    use super::{
        CACHE_LINE_FALLBACK, CoreClass, CoreFacts, CpuBudget, MemoryFacts, PageFacts, PowerState,
        parse_size,
    };
    use std::path::Path;

    /// Reads a pseudo-file as text (a query of the kernel, never a disk read; R1's allowed site).
    fn read(path: &str) -> Option<String> {
        super::read_text(path)
    }

    fn list(path: &str) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(path)
            .map(|d| {
                d.filter_map(Result::ok)
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default();
        names.sort();
        names
    }

    pub(super) fn page(_notes: &mut Vec<String>) -> PageFacts {
        let base = u64::try_from(rustix::param::page_size()).unwrap_or(0);
        let mut huge: Vec<u64> = list("/sys/kernel/mm/hugepages")
            .iter()
            .filter_map(|name| name.strip_prefix("hugepages-").and_then(parse_size))
            .collect();
        huge.sort_unstable();
        let transparent_huge = read("/sys/kernel/mm/transparent_hugepage/enabled")
            .is_some_and(|s| s.contains("[always]") || s.contains("[madvise]"));
        PageFacts {
            base,
            huge,
            transparent_huge,
            allocation_granularity: base,
        }
    }

    pub(super) fn cache_line(notes: &mut Vec<String>) -> u64 {
        read("/sys/devices/system/cpu/cpu0/cache/index0/coherency_line_size")
      .and_then(|s| s.parse::<u64>().ok())
      .filter(|v| *v > 0)
      .unwrap_or_else(|| {
        notes.push(format!("no cache line size from sysconf or sysfs; recorded the {CACHE_LINE_FALLBACK}-byte fallback"));
        CACHE_LINE_FALLBACK
      })
    }

    pub(super) fn cores(notes: &mut Vec<String>) -> Vec<CoreFacts> {
        let allowed = allowed_cores(notes);
        let atom = cpu_list("/sys/devices/cpu_atom/cpus");
        let core = cpu_list("/sys/devices/cpu_core/cpus");
        let mut unknown = 0u32;
        let cores = allowed
            .into_iter()
            .map(|id| {
                let base = format!("/sys/devices/system/cpu/cpu{id}");
                let class = class_of(id, &base, &atom, &core);
                if class == CoreClass::Unknown {
                    unknown = unknown.saturating_add(1);
                }
                CoreFacts {
                    id,
                    class,
                    level: u32::from(class == CoreClass::Efficiency),
                    numa: numa_of(&base),
                    l2_bytes: l2_of(&base),
                }
            })
            .collect();
        if unknown > 0 {
            notes.push(format!(
                "{unknown} core(s) have no class in sysfs; recorded unknown"
            ));
        }
        cores
    }

    /// The CPUs the calling thread may run on, by the kernel's ids: its affinity mask, which a cpuset
    /// narrows and a CPU quota does not [B: sched_getaffinity(2); cgroup-v2.rst "cpuset"]. The facts are
    /// queried before any probe pins the thread, so this is the process's own set. A kernel with more
    /// possible CPUs than the mask holds (`CpuSet::MAX_CPU`, 1,024) refuses the query; that host is outside
    /// the design's envelope (§2: 32–192 cores), and its facts then name no core, so nothing is pinned and
    /// one shard runs — said in a note, never a guessed id.
    fn allowed_cores(notes: &mut Vec<String>) -> Vec<u32> {
        use rustix::thread::{CpuSet, sched_getaffinity};
        match sched_getaffinity(None) {
            Ok(set) => (0..CpuSet::MAX_CPU)
                .filter(|&index| set.is_set(index))
                .filter_map(|index| u32::try_from(index).ok())
                .collect(),
            Err(e) => {
                notes.push(format!(
                    "sched_getaffinity refused ({e}); recorded no core, so no shard is pinned"
                ));
                Vec::new()
            }
        }
    }

    /// The tightest cgroup CPU budget over this process ([`CpuBudget`]): the process's cgroup path from
    /// `/proc/self/cgroup`, then `cpu.max` (v2) or `cpu.cfs_quota_us` with `cpu.cfs_period_us` (v1) at that
    /// cgroup and every ancestor up to the root — a parent's quota binds its children — the smallest share
    /// found. None when no controller bounds the process, or the files cannot be read (a query refused is no
    /// budget, never a guessed one). The walk mirrors [`cgroup_bound`].
    pub(super) fn cpu_budget(notes: &mut Vec<String>) -> Option<CpuBudget> {
        let cgroup = read("/proc/self/cgroup")?;
        let mut budgets = Vec::new();
        for line in cgroup.lines() {
            // Format: `id:controllers:path`, split at most three ways (a hostile path cannot add fields).
            let mut fields = line.splitn(3, ':');
            let (Some(_), Some(controllers), Some(path)) =
                (fields.next(), fields.next(), fields.next())
            else {
                continue;
            };
            let v1 = !controllers.is_empty();
            if v1 && !controllers.split(',').any(|c| c == "cpu") {
                continue;
            }
            let mut dir = path.trim_end_matches('/').to_owned();
            loop {
                budgets.push(budget_at(&dir, v1));
                match dir.rfind('/') {
                    Some(0) | None => break,
                    Some(cut) => dir.truncate(cut),
                }
            }
            budgets.push(budget_at("", v1));
        }
        let budget = super::tightest_budget(budgets);
        if let Some(budget) = budget {
            notes.push(format!(
                "cgroup CPU budget {} µs per {} µs",
                budget.quota_us, budget.period_us
            ));
        }
        budget
    }

    /// The budget one cgroup directory states: v2 `cpu.max`, or v1's quota over its period.
    fn budget_at(dir: &str, v1: bool) -> Option<CpuBudget> {
        if v1 {
            let root = "/sys/fs/cgroup/cpu";
            super::parse_cfs_budget(
                &read(&format!("{root}{dir}/cpu.cfs_quota_us"))?,
                &read(&format!("{root}{dir}/cpu.cfs_period_us"))?,
            )
        } else {
            super::parse_cpu_max(&read(&format!("/sys/fs/cgroup{dir}/cpu.max"))?)
        }
    }

    fn class_of(id: u32, base: &str, atom: &[u32], core: &[u32]) -> CoreClass {
        if let Some(capacity) =
            read(&format!("{base}/cpu_capacity")).and_then(|s| s.parse::<u64>().ok())
        {
            /// Format: the kernel normalizes the largest core's capacity to 1024.
            const FULL_CAPACITY: u64 = 1024;
            return if capacity >= FULL_CAPACITY {
                CoreClass::Performance
            } else {
                CoreClass::Efficiency
            };
        }
        if atom.contains(&id) {
            return CoreClass::Efficiency;
        }
        if core.contains(&id) {
            return CoreClass::Performance;
        }
        if atom.is_empty() && core.is_empty() && Path::new("/sys/devices/system/cpu/cpu0").exists()
        {
            // A homogeneous machine: every core is the same class, which is performance.
            return CoreClass::Performance;
        }
        CoreClass::Unknown
    }

    /// Parses a kernel cpu list such as `0-3,8,10-11`.
    fn cpu_list(path: &str) -> Vec<u32> {
        let Some(text) = read(path) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for part in text.split(',') {
            let part = part.trim();
            if let Some((a, b)) = part.split_once('-') {
                if let (Ok(a), Ok(b)) = (a.parse::<u32>(), b.parse::<u32>()) {
                    out.extend(a..=b);
                }
            } else if let Ok(v) = part.parse::<u32>() {
                out.push(v);
            }
        }
        out
    }

    fn numa_of(base: &str) -> u32 {
        list(base)
            .iter()
            .filter_map(|n| n.strip_prefix("node").and_then(|d| d.parse::<u32>().ok()))
            .next()
            .unwrap_or(0)
    }

    fn l2_of(base: &str) -> u64 {
        for index in list(&format!("{base}/cache")) {
            let dir = format!("{base}/cache/{index}");
            if read(&format!("{dir}/level")).as_deref() == Some("2") {
                return read(&format!("{dir}/size"))
                    .and_then(|s| parse_size(&s))
                    .unwrap_or(0);
            }
        }
        0
    }

    pub(super) fn memory(_notes: &mut Vec<String>) -> MemoryFacts {
        let info = rustix::system::sysinfo();
        let unit = u64::from(info.mem_unit).max(1);
        // c_ulong is u32 on i686 and u64 elsewhere; the conversion is for the former.
        #[allow(clippy::useless_conversion)]
        let (total, free) = (
            u64::from(info.totalram).saturating_mul(unit),
            u64::from(info.freeram).saturating_mul(unit),
        );
        let available = available_bytes().unwrap_or(free);
        MemoryFacts {
            total,
            available,
            address_bits: usize::BITS,
            limit: None,
        }
    }

    /// Where the cgroup file system is mounted (cgroup(7); v1's memory hierarchy at its `memory`).
    const CGROUP_MOUNT: &str = "/sys/fs/cgroup";

    /// The tightest cgroup memory limit over this process (§4.2 "OS/job/cgroup limits"): the least
    /// ceiling of its cgroup and every ancestor up to the root ([`super::least_ceiling`] of
    /// `/proc/self/cgroup`) — a parent's limit binds its children. None when no controller limits
    /// the process, or the files cannot be read (a query refused is no bound, never a guessed one).
    pub(super) fn cgroup_bound(notes: &mut Vec<String>) -> Option<u64> {
        let bound =
            read("/proc/self/cgroup").and_then(|text| super::least_ceiling(&text, CGROUP_MOUNT));
        if let Some(bytes) = bound {
            notes.push(format!("cgroup memory limit {bytes} bytes"));
        }
        bound
    }

    /// This process's resident memory now: `/proc/self/statm`'s second field, in pages (proc(5)).
    pub(super) fn resident_bytes() -> Option<u64> {
        // `/proc/self/statm`: size, then resident, in pages (proc(5)).
        let pages: u64 = read("/proc/self/statm")?
            .split_whitespace()
            .nth(1)?
            .parse()
            .ok()?;
        pages.checked_mul(u64::try_from(rustix::param::page_size()).ok()?)
    }

    /// Memory available to a new allocation now: the kernel's estimate for the machine
    /// (`MemAvailable` of `/proc/meminfo`, which counts the page cache reclaim can take back,
    /// proc(5)), within the room the cgroups over this process leave it
    /// ([`super::within_cgroups`] of `/proc/self/cgroup`).
    pub(super) fn available_bytes() -> Option<u64> {
        let machine = read("/proc/meminfo").and_then(|text| {
            text.lines()
                .find_map(|l| l.strip_prefix("MemAvailable:"))
                .and_then(parse_size)
        });
        match read("/proc/self/cgroup") {
            Some(cgroup) => super::within_cgroups(machine, &cgroup, CGROUP_MOUNT),
            None => machine,
        }
    }

    pub(super) fn power(notes: &mut Vec<String>) -> PowerState {
        let supplies = list("/sys/class/power_supply");
        let mut mains_online = false;
        let mut battery = false;
        for name in &supplies {
            let base = format!("/sys/class/power_supply/{name}");
            match read(&format!("{base}/type")).as_deref() {
                Some("Mains") | Some("USB") => {
                    if read(&format!("{base}/online")).as_deref() == Some("1") {
                        mains_online = true;
                    }
                }
                Some("Battery") => battery = true,
                _ => {}
            }
        }
        match (mains_online, battery) {
            (true, _) => PowerState::Mains,
            (false, true) => PowerState::Battery,
            (false, false) => {
                notes.push("no power supply entry answered; recorded unknown".to_owned());
                PowerState::Unknown
            }
        }
    }

    pub(super) fn locked_bytes() -> Option<u64> {
        let status = read("/proc/self/status")?;
        let line = status.lines().find(|l| l.starts_with("VmLck:"))?;
        let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
        /// Format: VmLck is printed in kibibytes.
        const KIB: u64 = 1024;
        Some(kib.saturating_mul(KIB))
    }

    pub(super) fn identity(notes: &mut Vec<String>) -> (String, String) {
        let cpu = read("/proc/cpuinfo")
            .and_then(|text| {
                text.lines()
                    .find(|l| {
                        l.starts_with("model name")
                            || l.starts_with("Model")
                            || l.starts_with("cpu model")
                    })
                    .and_then(|l| l.split_once(':').map(|(_, v)| v.trim().to_owned()))
            })
            .unwrap_or_else(|| {
                notes.push("/proc/cpuinfo has no model line; recorded unknown cpu".to_owned());
                "unknown cpu".to_owned()
            });
        let uts = rustix::system::uname();
        let os = format!(
            "{} {}",
            uts.sysname().to_string_lossy(),
            uts.release().to_string_lossy()
        );
        (cpu, os)
    }
}

#[cfg(windows)]
mod platform {
    /// The live "available" probe arrives with the Windows bridge (Phase 4); the facts' value
    /// stands until then.
    pub(super) fn available_bytes() -> Option<u64> {
        None
    }

    /// No reading, as for available memory: the pressure sampler holds nothing back here.
    pub(super) fn resident_bytes() -> Option<u64> {
        None
    }

    use super::{CACHE_LINE_FALLBACK, CoreClass, CoreFacts, MemoryFacts, PageFacts, PowerState};
    use windows_sys::Win32::System::Memory::GetLargePageMinimum;
    use windows_sys::Win32::System::Power::GetSystemPowerStatus;
    use windows_sys::Win32::System::Power::SYSTEM_POWER_STATUS;
    use windows_sys::Win32::System::SystemInformation::{
        GetLogicalProcessorInformation, GetSystemInfo, GetVersion, GlobalMemoryStatusEx,
        MEMORYSTATUSEX, RelationCache, RelationNumaNode, SYSTEM_INFO,
        SYSTEM_LOGICAL_PROCESSOR_INFORMATION,
    };
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, GetProcessAffinityMask};

    fn system_info() -> SYSTEM_INFO {
        // SAFETY: an all-zero SYSTEM_INFO is a valid, if empty, value for the call below to fill.
        let mut info: SYSTEM_INFO = unsafe { std::mem::zeroed() };
        // SAFETY: `info` is a writable SYSTEM_INFO; GetSystemInfo cannot fail.
        unsafe { GetSystemInfo(&raw mut info) };
        info
    }

    pub(super) fn page(_notes: &mut Vec<String>) -> PageFacts {
        let info = system_info();
        // SAFETY: no preconditions; 0 means large pages are unavailable to this process.
        let large = unsafe { GetLargePageMinimum() };
        let huge = if large > 0 {
            vec![u64::try_from(large).unwrap_or(0)]
        } else {
            Vec::new()
        };
        PageFacts {
            base: u64::from(info.dwPageSize),
            huge,
            transparent_huge: false,
            allocation_granularity: u64::from(info.dwAllocationGranularity),
        }
    }

    fn processor_information() -> Vec<SYSTEM_LOGICAL_PROCESSOR_INFORMATION> {
        let mut len: u32 = 0;
        // SAFETY: a null buffer asks for the required length.
        unsafe { GetLogicalProcessorInformation(std::ptr::null_mut(), &raw mut len) };
        let entry = u32::try_from(std::mem::size_of::<SYSTEM_LOGICAL_PROCESSOR_INFORMATION>())
            .unwrap_or(u32::MAX);
        if len == 0 || entry == 0 {
            return Vec::new();
        }
        let count = usize::try_from(len.checked_div(entry).unwrap_or(0)).unwrap_or(0);
        let mut out: Vec<SYSTEM_LOGICAL_PROCESSOR_INFORMATION> = Vec::with_capacity(count);
        // SAFETY: the buffer holds `count` entries of `len` bytes total; on success the call filled
        // `len` bytes, so `count` entries are initialized.
        let ok = unsafe { GetLogicalProcessorInformation(out.as_mut_ptr(), &raw mut len) };
        if ok == 0 {
            return Vec::new();
        }
        // SAFETY: see above; `len / entry` entries were written.
        unsafe {
            out.set_len(
                usize::try_from(len.checked_div(entry).unwrap_or(0))
                    .unwrap_or(0)
                    .min(count),
            )
        };
        out
    }

    pub(super) fn cache_line(notes: &mut Vec<String>) -> u64 {
        for entry in processor_information() {
            if entry.Relationship == RelationCache {
                // SAFETY: the relationship says the union holds the cache descriptor.
                let cache = unsafe { entry.Anonymous.Cache };
                if cache.Level == 1 && cache.LineSize > 0 {
                    return u64::from(cache.LineSize);
                }
            }
        }
        notes.push(format!("GetLogicalProcessorInformation gave no L1 line size; recorded the {CACHE_LINE_FALLBACK}-byte fallback"));
        CACHE_LINE_FALLBACK
    }

    pub(super) fn cores(notes: &mut Vec<String>) -> Vec<CoreFacts> {
        let info = processor_information();
        let mut l2: u64 = 0;
        for entry in &info {
            if entry.Relationship == RelationCache {
                // SAFETY: the relationship says the union holds the cache descriptor.
                let cache = unsafe { entry.Anonymous.Cache };
                if cache.Level == 2 {
                    l2 = l2.max(u64::from(cache.Size));
                }
            }
        }
        let numa_of = |id: u32| -> u32 {
            for entry in &info {
                if entry.Relationship == RelationNumaNode
                    && entry
                        .ProcessorMask
                        .checked_shr(id)
                        .is_some_and(|bits| bits & 1 == 1)
                {
                    // SAFETY: the relationship says the union holds the NUMA descriptor.
                    return unsafe { entry.Anonymous.NumaNode.NodeNumber };
                }
            }
            0
        };
        // The cores this process may run on are its affinity mask's bits, not `0..available_parallelism()`
        // (docs/bugs/2026-09-29-every-daemon-under-a-cpu-quota-pinned-its-shard-to-cpu-1.md).
        let Some(mask) = process_affinity_mask() else {
            notes.push(
                "GetProcessAffinityMask refused; recorded no core, so no shard is pinned"
                    .to_owned(),
            );
            return Vec::new();
        };
        notes.push(
            "core classes are not queried on Windows in this phase; recorded unknown".to_owned(),
        );
        (0..usize::BITS)
            .filter(|&id| mask.checked_shr(id).is_some_and(|bits| bits & 1 == 1))
            .map(|id| CoreFacts {
                id,
                class: CoreClass::Unknown,
                level: 0,
                numa: numa_of(id),
                l2_bytes: l2,
            })
            .collect()
    }

    /// The process's affinity mask: the logical processors of its group it may run on, one bit each (up to
    /// 64), or `None` when the query is refused or names none [B: GetProcessAffinityMask, Microsoft Learn].
    /// The core facts' source, and the mask the core matrix and the wake probe give the calling thread back
    /// after pinning it.
    pub(crate) fn process_affinity_mask() -> Option<usize> {
        let mut process: usize = 0;
        let mut system: usize = 0;
        // SAFETY: the current process pseudo-handle and two writable usize outputs.
        let ok = unsafe {
            GetProcessAffinityMask(GetCurrentProcess(), &raw mut process, &raw mut system)
        };
        (ok != 0 && process != 0).then_some(process)
    }

    /// Windows has no cgroups; a job object's CPU rate limit is the counterpart, owed with its memory limit.
    pub(super) fn cpu_budget(_notes: &mut Vec<String>) -> Option<super::CpuBudget> {
        None
    }

    pub(super) fn memory(notes: &mut Vec<String>) -> MemoryFacts {
        // SAFETY: an all-zero MEMORYSTATUSEX is a valid, if empty, value for the call below to fill.
        let mut status: MEMORYSTATUSEX = unsafe { std::mem::zeroed() };
        status.dwLength = u32::try_from(std::mem::size_of::<MEMORYSTATUSEX>()).unwrap_or(0);
        // SAFETY: `status` is a writable MEMORYSTATUSEX with its length set.
        let ok = unsafe { GlobalMemoryStatusEx(&raw mut status) };
        if ok == 0 {
            notes.push("GlobalMemoryStatusEx refused; recorded 0".to_owned());
            return MemoryFacts {
                total: 0,
                available: 0,
                address_bits: usize::BITS,
                limit: None,
            };
        }
        MemoryFacts {
            total: status.ullTotalPhys,
            available: status.ullAvailPhys,
            address_bits: usize::BITS,
            limit: None,
        }
    }

    /// Windows has no cgroups; the job object's memory limit is the owed bound.
    pub(super) fn cgroup_bound(_notes: &mut Vec<String>) -> Option<u64> {
        None
    }

    pub(super) fn power(notes: &mut Vec<String>) -> PowerState {
        // SAFETY: an all-zero SYSTEM_POWER_STATUS is a valid, if empty, value for the call below to fill.
        let mut status: SYSTEM_POWER_STATUS = unsafe { std::mem::zeroed() };
        // SAFETY: `status` is a writable SYSTEM_POWER_STATUS.
        let ok = unsafe { GetSystemPowerStatus(&raw mut status) };
        if ok == 0 {
            notes.push("GetSystemPowerStatus refused; recorded unknown".to_owned());
            return PowerState::Unknown;
        }
        match status.ACLineStatus {
            1 => PowerState::Mains,
            0 => PowerState::Battery,
            _ => PowerState::Unknown,
        }
    }

    pub(super) fn locked_bytes() -> Option<u64> {
        None
    }

    pub(super) fn identity(_notes: &mut Vec<String>) -> (String, String) {
        let info = system_info();
        // SAFETY: no preconditions.
        let version = unsafe { GetVersion() };
        let cpu = format!(
            "windows processor architecture {} level {} revision {}",
            // SAFETY: the union's named struct is the documented layout on every current Windows.
            unsafe { info.Anonymous.Anonymous.wProcessorArchitecture },
            info.wProcessorLevel,
            info.wProcessorRevision
        );
        (cpu, format!("windows {version:#x}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A cgroup limit file's text becomes a bound only when it is a plain byte count; `max`, an empty
    /// file and hostile text (signs, suffixes, spaces inside, a number past `u64`) are no bound at
    /// all — never a guessed one (§4.2: a limit that cannot be read must not become a smaller or
    /// larger reservation).
    #[test]
    fn a_cgroup_limit_is_a_bound_only_when_it_is_a_byte_count() {
        assert_eq!(parse_cgroup_limit("2147483648\n"), Some(2_147_483_648));
        assert_eq!(parse_cgroup_limit("  4096  "), Some(4096));
        for hostile in [
            "max",
            "",
            "\n",
            "-1",
            "+4096",
            "4096k",
            "40 96",
            "18446744073709551616",
            "0x1000",
            "max\n4096",
        ] {
            assert_eq!(parse_cgroup_limit(hostile), None, "{hostile:?}");
        }
    }

    /// A cgroup CPU budget is read only from the kernel's exact forms: v2 `cpu.max` as `QUOTA PERIOD`, v1 as
    /// a quota and a period; `max`, v1's `-1`, a missing or third field, a zero, and hostile text (signs,
    /// suffixes, hex, a number past `u64`) are no budget at all — never a guessed one (§4.3: a budget read
    /// wrong would fix shards on a shared pool or run more than the quota buys).
    #[test]
    fn a_cpu_budget_is_read_only_from_the_kernels_exact_forms() {
        let two = Some(CpuBudget {
            quota_us: 200_000,
            period_us: 100_000,
        });
        assert_eq!(parse_cpu_max("200000 100000\n"), two);
        assert_eq!(parse_cfs_budget("200000\n", " 100000 "), two);
        for hostile in [
            "max 100000",
            "",
            "200000",
            "200000 100000 7",
            "0 100000",
            "200000 0",
            "-200000 100000",
            "+200000 100000",
            "2e5 100000",
            "0x30d40 100000",
            "18446744073709551616 100000",
        ] {
            assert_eq!(parse_cpu_max(hostile), None, "{hostile:?}");
        }
        assert_eq!(parse_cfs_budget("-1", "100000"), None);
        assert_eq!(parse_cfs_budget("200000", "max"), None);
    }

    /// A budget covers a set of cores when it buys them all at once, rounds its whole CPUs down, and the
    /// tightest of several is the smallest share, decided without rounding (a parent's 1.5 CPUs binds a
    /// child's 2); arithmetic past `u128` covers nothing, and a hostile zero period is no bound.
    #[test]
    fn a_budget_covers_what_it_buys_and_the_tightest_share_binds() {
        let budget = |quota_us, period_us| CpuBudget {
            quota_us,
            period_us,
        };
        assert!(budget(200_000, 100_000).covers(2));
        assert!(!budget(200_000, 100_000).covers(3));
        assert!(!budget(u64::MAX, u64::MAX).covers(usize::MAX));
        assert_eq!(budget(250_000, 100_000).whole_cpus(), 2);
        assert_eq!(budget(50_000, 100_000).whole_cpus(), 0);
        assert_eq!(budget(1, 0).whole_cpus(), u64::MAX);
        assert_eq!(
            tightest_budget([
                Some(budget(200_000, 100_000)),
                None,
                Some(budget(150_000, 100_000)),
                Some(budget(400_000, 200_000)),
            ]),
            Some(budget(150_000, 100_000))
        );
        assert_eq!(tightest_budget([None, None]), None);
    }

    /// The facts count the threads that run at once as the cores capped by the budget's whole CPUs.
    #[test]
    fn the_cpus_at_once_are_the_cores_capped_by_the_budget() {
        let mut facts = Facts::query();
        let cores = facts.cores.len();
        facts.cpu_budget = None;
        assert_eq!(facts.cpus_at_once(), cores);
        facts.cpu_budget = Some(CpuBudget {
            quota_us: 100_000,
            period_us: 100_000,
        });
        assert_eq!(facts.cpus_at_once(), cores.min(1));
    }

    /// The effective capacity is the physical memory clamped to a bound set below it; a bound above
    /// it, or none, leaves the physical memory (§4.2 "effective capacity").
    #[test]
    fn the_effective_capacity_is_total_clamped_to_a_bound_below_it() {
        assert_eq!(effective_capacity(1 << 37, Some(1 << 31)), 1 << 31);
        assert_eq!(effective_capacity(1 << 37, Some(1 << 40)), 1 << 37);
        assert_eq!(effective_capacity(1 << 37, None), 1 << 37);
        assert_eq!(effective_capacity(1 << 37, Some(0)), 0);
    }

    /// A cgroup's room is its headroom under the ceiling, none past it, and its file pages less
    /// the dirty and writeback ones, none when those are more; v1 reads its hierarchical totals.
    /// A text lacking a count, or with a count that is not a byte count, or a sum past `u64`, is
    /// no reading: v2's text read as v1's is none, since its counts carry other names.
    #[test]
    fn a_cgroup_s_room_is_its_headroom_and_its_clean_file_pages() {
        const MIB: u64 = 1 << 20;
        const GIB: u64 = 1 << 30;
        // The counts of a v2 `memory.stat`, in its order, among the names that share their words.
        let v2 = |active: u64, inactive: u64, dirty: u64, writeback: u64| {
            format!(
                "anon {}\nfile {}\nfile_mapped 4096\nfile_dirty {dirty}\nfile_writeback {writeback}\n\
                 inactive_anon 0\nactive_anon {}\ninactive_file {inactive}\nactive_file {active}\n\
                 unevictable 0\n",
                22 * GIB,
                active + inactive,
                22 * GIB,
            )
        };
        let text = v2(300 * MIB, 500 * MIB, 100 * MIB, 50 * MIB);
        assert_eq!(
            cgroup_room(24 * GIB, 23 * GIB, &text, false),
            Some(GIB + 650 * MIB)
        );
        // Past the ceiling only the clean file pages are room; with more dirty than file, none.
        assert_eq!(
            cgroup_room(24 * GIB, 25 * GIB, &text, false),
            Some(650 * MIB)
        );
        let dirtier = v2(MIB, MIB, 2 * MIB, MIB);
        assert_eq!(cgroup_room(24 * GIB, 24 * GIB, &dirtier, false), Some(0));
        let v1 = "cache 8192\ndirty 4096\ntotal_dirty 4096\ntotal_writeback 0\n\
                  total_inactive_file 8192\ntotal_active_file 4096\n";
        assert_eq!(
            cgroup_room(1 << 20, 1 << 19, v1, true),
            Some((1 << 19) + 8192)
        );
        assert_eq!(cgroup_room(24 * GIB, 23 * GIB, &text, true), None);
        for (from, to) in [
            ("file_writeback 52428800\n", ""),
            ("active_file 314572800", "active_file 300M"),
            ("active_file 314572800", "active_file -1"),
            (
                "inactive_file 524288000",
                "inactive_file 18446744073709551615",
            ),
        ] {
            let hostile = text.replace(from, to);
            assert_ne!(hostile, text, "{from:?}");
            assert_eq!(
                cgroup_room(24 * GIB, 23 * GIB, &hostile, false),
                None,
                "{to:?}"
            );
        }
        assert_eq!(
            cgroup_room(u64::MAX, 0, &v2(1, 0, 0, 0), false),
            None,
            "headroom and clean pages past u64"
        );
    }

    /// The memory directories are the process's cgroup and each ancestor to its namespace's root,
    /// in v2's hierarchy or v1's memory controller's, and no other controller's.
    #[test]
    fn the_memory_dirs_are_the_cgroup_and_each_ancestor() {
        let dirs = |text: &str| -> Vec<(String, bool)> { memory_dirs_of(text, "/sys/fs/cgroup") };
        let owned = |list: &[(&str, bool)]| -> Vec<(String, bool)> {
            list.iter().map(|(d, v1)| ((*d).to_owned(), *v1)).collect()
        };
        assert_eq!(
            dirs("0::/user.slice/run-1.scope\n"),
            owned(&[
                ("/sys/fs/cgroup/user.slice/run-1.scope", false),
                ("/sys/fs/cgroup/user.slice", false),
                ("/sys/fs/cgroup", false),
            ])
        );
        assert_eq!(dirs("0::/\n"), owned(&[("/sys/fs/cgroup", false)]));
        assert_eq!(
            dirs("5:cpu,cpuacct:/a\n4:memory:/a/b/\n"),
            owned(&[
                ("/sys/fs/cgroup/memory/a/b", true),
                ("/sys/fs/cgroup/memory/a", true),
                ("/sys/fs/cgroup/memory", true),
            ])
        );
        assert!(dirs("").is_empty());
        assert!(dirs("garbage\n3:pids:/x\n").is_empty());
    }

    /// A cgroup file system written to a directory of this test's own: each cgroup a directory of
    /// the files given, under the mount; removed when dropped.
    struct FakeCgroups {
        mount: std::path::PathBuf,
    }

    impl FakeCgroups {
        fn new(name: &str) -> Self {
            let mount = std::env::temp_dir()
                .join(format!("hyper-rt-cgroups-{}-{name}", std::process::id()));
            let _ = std::fs::remove_dir_all(&mount);
            std::fs::create_dir_all(&mount).unwrap();
            FakeCgroups { mount }
        }

        fn mount(&self) -> &str {
            self.mount.to_str().unwrap()
        }

        fn write(&self, dir: &str, files: &[(&str, &str)]) {
            let dir = self.mount.join(dir.trim_start_matches('/'));
            std::fs::create_dir_all(&dir).unwrap();
            for (name, text) in files {
                std::fs::write(dir.join(name), text).unwrap();
            }
        }

        fn remove(&self, dir: &str, file: &str) {
            std::fs::remove_file(self.mount.join(dir.trim_start_matches('/')).join(file)).unwrap();
        }
    }

    impl Drop for FakeCgroups {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.mount);
        }
    }

    /// The memory a process may take is the least of the machine's and the room of each cgroup over
    /// it with a ceiling, `memory.high` as much as `memory.max`: a process in a 24 GiB cgroup on a
    /// machine with 82 GB free has the cgroup's room, which `MemAvailable` alone (the reading before
    /// this) did not give. A cgroup whose `memory.stat` cannot be read is its headroom; one whose
    /// usage cannot be read lowers nothing; with no ceiling anywhere the machine's reading stands.
    /// v1's memory hierarchy is read by its own names. The tightest ceiling is the facts' limit.
    #[test]
    fn the_memory_available_is_within_each_bounding_cgroup_s_room() {
        const MIB: u64 = 1 << 20;
        const GIB: u64 = 1 << 30;
        let fake = FakeCgroups::new("room");
        let mount = fake.mount();
        let stat = |active: u64, inactive: u64, dirty: u64, writeback: u64| {
            format!(
                "anon 0\nfile {}\nfile_dirty {dirty}\nfile_writeback {writeback}\n\
                 inactive_file {inactive}\nactive_file {active}\n",
                active + inactive
            )
        };
        let unbounded = [("memory.max", "max\n"), ("memory.high", "max\n")];
        let cgroup = "0::/a/b\n";
        fake.write("/", &[("memory.stat", &stat(0, 0, 0, 0))]);
        fake.write("/a", &unbounded);
        fake.write("/a/b", &unbounded);
        let machine = Some(82_000_000_000);
        assert_eq!(within_cgroups(machine, cgroup, mount), machine);
        assert_eq!(least_ceiling(cgroup, mount), None);

        let max = (24 * GIB).to_string();
        let used = (23 * GIB).to_string();
        let a_stat = stat(300 * MIB, 500 * MIB, 100 * MIB, 50 * MIB);
        fake.write(
            "/a",
            &[
                ("memory.max", &max),
                ("memory.current", &used),
                ("memory.stat", &a_stat),
            ],
        );
        let room = GIB + 650 * MIB;
        assert_eq!(within_cgroups(machine, cgroup, mount), Some(room));
        assert_eq!(within_cgroups(None, cgroup, mount), Some(room));
        assert_eq!(within_cgroups(Some(GIB), cgroup, mount), Some(GIB));
        assert_eq!(least_ceiling(cgroup, mount), Some(24 * GIB));

        // A leaf throttled at 1 GiB, 900 MiB used, none of it file pages: its room binds.
        let high = GIB.to_string();
        let leaf_used = (900 * MIB).to_string();
        fake.write(
            "/a/b",
            &[
                ("memory.high", &high),
                ("memory.current", &leaf_used),
                ("memory.stat", &stat(0, 0, 0, 0)),
            ],
        );
        assert_eq!(within_cgroups(machine, cgroup, mount), Some(124 * MIB));
        assert_eq!(least_ceiling(cgroup, mount), Some(GIB));

        // Unreadable counts: the leaf's `memory.stat` gone leaves its headroom; its usage gone
        // leaves the parent's room.
        fake.remove("/a/b", "memory.stat");
        assert_eq!(within_cgroups(machine, cgroup, mount), Some(124 * MIB));
        fake.remove("/a/b", "memory.current");
        assert_eq!(within_cgroups(machine, cgroup, mount), Some(room));

        let v1 = "4:memory:/x\n";
        fake.write(
            "/memory/x",
            &[
                ("memory.limit_in_bytes", &(2 * GIB).to_string()),
                ("memory.usage_in_bytes", &GIB.to_string()),
                (
                    "memory.stat",
                    "total_inactive_file 8192\ntotal_active_file 4096\ntotal_dirty 4096\n\
                     total_writeback 0\n",
                ),
            ],
        );
        assert_eq!(within_cgroups(machine, v1, mount), Some(GIB + 8192));
        assert_eq!(least_ceiling(v1, mount), Some(2 * GIB));
    }

    /// The tightest of several optional bounds is the smallest present one; none present is none.
    #[test]
    fn the_tightest_bound_is_the_smallest_present() {
        assert_eq!(tightest([None, Some(7), Some(3), None]), Some(3));
        assert_eq!(tightest([None, None]), None);
    }

    #[test]
    fn the_facts_are_queried_without_refusal_on_this_machine() {
        let facts = Facts::query();
        assert!(facts.page.base >= 4096, "{facts:?}");
        assert!(facts.page.base.is_power_of_two());
        assert!(
            facts.cache_line >= 32 && facts.cache_line.is_power_of_two(),
            "{}",
            facts.cache_line
        );
        assert!(!facts.cores.is_empty());
        assert!(facts.memory.total > 0);
        assert!(facts.memory.available <= facts.memory.total || facts.memory.total == 0);
        assert!(!facts.identity.cpu.is_empty());
        let line = facts.identity.line();
        assert!(line.contains("cores"), "{line}");
    }

    #[test]
    fn sizes_with_unit_suffixes_parse() {
        assert_eq!(parse_size("2048kB"), Some(2048 * 1024));
        assert_eq!(parse_size("1024K"), Some(1024 * 1024));
        assert_eq!(parse_size("16 MB"), Some(16 * 1024 * 1024));
        assert_eq!(parse_size("12345"), Some(12345));
        assert_eq!(parse_size("x"), None);
        assert_eq!(parse_size("3 parsecs"), None);
    }
}
