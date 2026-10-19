//! The time the process's tasks have waited for memory: Linux's pressure stall information (PSI,
//! kernel `Documentation/accounting/psi.rst`), the cumulative microseconds in which at least one
//! task of a group stalled on memory (the "some" line's `total=`). Read from the process's own
//! cgroup's `memory.pressure` (cgroup v2: the `0::PATH` line of `/proc/self/cgroup`), so other
//! groups' stalls do not count; else from the machine's `/proc/pressure/memory`. None where the
//! kernel keeps no count (before Linux 4.20, PSI off) and on other systems (docs/research/64 §16).

/// The microseconds the process's tasks have stalled on memory since the count began, or none.
#[cfg(target_os = "linux")]
pub fn memory_stalled_us() -> Option<u64> {
    static PATH: std::sync::OnceLock<Option<std::path::PathBuf>> = std::sync::OnceLock::new();
    let path = PATH.get_or_init(|| {
        let own = std::fs::read_to_string("/proc/self/cgroup")
            .ok()
            .and_then(|text| {
                text.lines().find_map(|l| l.strip_prefix("0::")).map(|rel| {
                    std::path::Path::new("/sys/fs/cgroup")
                        .join(rel.trim_start_matches('/'))
                        .join("memory.pressure")
                })
            })
            .filter(|p| some_total(p).is_some());
        own.or_else(|| {
            Some(std::path::PathBuf::from("/proc/pressure/memory"))
                .filter(|p| some_total(p).is_some())
        })
    });
    path.as_deref().and_then(some_total)
}

/// No count off Linux.
#[cfg(not(target_os = "linux"))]
pub fn memory_stalled_us() -> Option<u64> {
    None
}

/// The `total=` of a pressure file's "some" line.
#[cfg(target_os = "linux")]
fn some_total(path: &std::path::Path) -> Option<u64> {
    let text = std::fs::read_to_string(path).ok()?;
    parse_some_total(&text)
}

/// The `total=` of the "some" line of a pressure file's text: `some avg10=0.00 avg60=0.00
/// avg300=0.00 total=12345` (psi.rst's format).
#[cfg(any(target_os = "linux", test))]
fn parse_some_total(text: &str) -> Option<u64> {
    text.lines()
        .find_map(|l| l.strip_prefix("some "))?
        .split_whitespace()
        .find_map(|field| field.strip_prefix("total="))?
        .parse()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_some_line_s_total_is_read_and_the_full_line_s_is_not() {
        let text = "some avg10=0.00 avg60=0.90 avg300=2.78 total=26045739\n\
                    full avg10=0.00 avg60=0.83 avg300=2.67 total=25196350\n";
        assert_eq!(parse_some_total(text), Some(26_045_739));
        assert_eq!(parse_some_total("full avg10=0.00 total=5\n"), None);
        assert_eq!(parse_some_total("some avg10=0.00\n"), None);
        assert_eq!(parse_some_total(""), None);
    }

    /// Where the kernel keeps a count, the process reads one, and it never runs backward.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_linux_count_where_the_kernel_keeps_one_never_runs_backward() {
        let first = memory_stalled_us();
        if std::fs::read_to_string("/proc/pressure/memory").is_ok() {
            assert!(
                first.is_some(),
                "the kernel keeps a count the process did not read"
            );
        }
        if let (Some(a), Some(b)) = (first, memory_stalled_us()) {
            assert!(b >= a, "{b} after {a}");
        }
    }
}
