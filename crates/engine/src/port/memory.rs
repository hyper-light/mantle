//! The machine's memory as a budget that follows it reads it (docs/research/65): what the machine
//! could give this process now without writing anything out first.
//!
//! On Linux the kernel's estimate of the memory a new allocation can take (`MemAvailable`,
//! proc(5)) counts the whole file LRU, dirty and writeback pages with the clean (mm/show_mem.c
//! `si_mem_available`), though reclaim cannot take those back before they are written
//! (mm/vmscan.c `shrink_folio_list`); the kernel's own room for a cgroup's dirty memory counts only
//! clean file pages (mm/page-writeback.c `mdtc_calc_avail`). And when the kernel moves a process's
//! pages out to swap (on a disk, or compressed in memory by zram), the memory it frees reads as
//! available: a process that sized itself by the estimate would grow by what the kernel took from
//! it. So the room is the estimate less the dirty and writeback pages and less this process's own
//! pages in swap (`VmSwap`, proc(5)). Elsewhere it is hyper-rt's reading of the OS's own estimate.

/// The memory the machine could give this process now without writing anything out first; none
/// without a reading.
pub fn room_now() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        room_of(&meminfo, &status)
    }
    #[cfg(not(target_os = "linux"))]
    {
        hyper_rt::machine::facts::Facts::memory_available_now()
    }
}

/// The room in `meminfo` (`/proc/meminfo`'s text) for the process whose `/proc/self/status` text
/// is `status`: `MemAvailable` less `Dirty`, `Writeback` and the process's `VmSwap`, none past
/// nothing. None without `MemAvailable`; a count missing or unreadable is none of that kind.
#[cfg(any(target_os = "linux", test))]
fn room_of(meminfo: &str, status: &str) -> Option<u64> {
    let available = kib(meminfo, "MemAvailable:")?;
    let held_back = [
        kib(meminfo, "Dirty:"),
        kib(meminfo, "Writeback:"),
        kib(status, "VmSwap:"),
    ]
    .into_iter()
    .flatten()
    .fold(0u64, u64::saturating_add);
    Some(available.saturating_sub(held_back))
}

/// The bytes a `name kB` line of a proc file states, none when it is missing or unreadable.
#[cfg(any(target_os = "linux", test))]
fn kib(text: &str, name: &str) -> Option<u64> {
    text.lines()
        .find_map(|line| line.strip_prefix(name))
        .and_then(|rest| rest.trim().strip_suffix("kB"))
        .and_then(|count| count.trim().parse::<u64>().ok())
        .and_then(|count| count.checked_mul(1024))
}

#[cfg(test)]
mod tests {
    use super::*;

    const MEMINFO: &str = "MemTotal:       98690668 kB\nMemFree:        74950608 kB\nMemAvailable:   80540128 kB\nCached:          6094500 kB\nDirty:            315480 kB\nWriteback:          1024 kB\nAnonPages:      15177928 kB\n";
    const STATUS: &str = "Name:\tranges\nVmRSS:\t62100876 kB\nRssAnon:\t62096492 kB\nVmSwap:\t41459496 kB\nThreads:\t40\n";

    #[test]
    fn the_room_is_the_estimate_less_dirty_writeback_and_the_process_s_swap() {
        let room = room_of(MEMINFO, STATUS).unwrap();
        assert_eq!(room, (80_540_128 - 315_480 - 1_024 - 41_459_496) * 1024);
    }

    #[test]
    fn a_count_missing_is_none_of_its_kind_and_no_estimate_is_no_reading() {
        assert_eq!(
            room_of(MEMINFO, "Name:\tranges\n").unwrap(),
            (80_540_128 - 315_480 - 1_024) * 1024
        );
        assert_eq!(room_of("MemTotal: 1 kB\n", STATUS), None);
        assert_eq!(room_of("MemAvailable: 12x kB\n", STATUS), None);
    }

    #[test]
    fn a_process_swapped_past_the_estimate_has_no_room() {
        let status = "VmSwap:\t90000000 kB\n";
        assert_eq!(room_of(MEMINFO, status), Some(0));
    }
}
