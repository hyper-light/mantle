//! Human-readable quantities: SI units for capacities and rates (as drive makers and `df -H`
//! state them) and adaptive time units.

// Quantities are shown to three significant figures; u64 -> f64 loses nothing visible.

pub fn capacity(bytes: u64) -> String {
    scaled(
        bytes as f64,
        1000.0,
        &["B", "kB", "MB", "GB", "TB", "PB", "EB"],
    )
}

pub fn rate(bytes_per_sec: f64) -> String {
    format!(
        "{}/s",
        scaled(bytes_per_sec, 1000.0, &["B", "kB", "MB", "GB", "TB"])
    )
}

pub fn nanos(ns: u64) -> String {
    let ns = ns as f64;
    if ns < 1_000.0 {
        format!("{ns:.0} ns")
    } else if ns < 1_000_000.0 {
        format!("{} µs", three(ns / 1_000.0))
    } else if ns < 1_000_000_000.0 {
        format!("{} ms", three(ns / 1_000_000.0))
    } else {
        format!("{} s", three(ns / 1_000_000_000.0))
    }
}

fn scaled(mut value: f64, step: f64, units: &[&str]) -> String {
    let mut unit = units.first().copied().unwrap_or("");
    for next in units.iter().skip(1) {
        if value < step {
            break;
        }
        value /= step;
        unit = next;
    }
    let number = if unit == units.first().copied().unwrap_or("") {
        format!("{value:.0}")
    } else {
        three(value)
    };
    if unit.is_empty() {
        number
    } else if unit.chars().all(char::is_alphabetic) && unit.len() == 1 && unit != "B" {
        // Count suffixes (K, M, G) attach to the number.
        format!("{number}{unit}")
    } else {
        format!("{number} {unit}")
    }
}

/// Three significant figures without trailing noise: 1.23, 12.3, 123.
fn three(value: f64) -> String {
    if value >= 100.0 {
        format!("{value:.0}")
    } else if value >= 10.0 {
        format!("{value:.1}")
    } else {
        format!("{value:.2}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn units() {
        assert_eq!(capacity(46_114_729_984), "46.1 GB");
        assert_eq!(capacity(7_998_499_225_600), "8.00 TB");
        assert_eq!(capacity(512), "512 B");
        assert_eq!(rate(10_600_000_000.0), "10.6 GB/s");
        assert_eq!(nanos(73_727), "73.7 µs");
        assert_eq!(nanos(4_718_591), "4.72 ms");
        assert_eq!(nanos(900), "900 ns");
    }
}
