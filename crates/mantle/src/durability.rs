//! `mantle durability`: how likely a block is to be lost in a year under each scheme mantle
//! can store it in, for the failure and repair rates given, and the scheme that meets the
//! durability target at least cost (docs/design/durability.md).

use std::io::Write;

use mantle_ec::durability::{self, Burst, Choice, DomainLoss, Rates, Scheme, YEAR};

/// What was asked, in the units operators state them in.
#[derive(Debug, Clone)]
pub struct Options {
    /// Failure domains a stripe spreads over, one chunk to each.
    pub domains: usize,
    /// Each chunk's device's annual failure rate, as a fraction.
    pub annual_failure: f64,
    /// Hours from a chunk's loss to its rebuild.
    pub repair_hours: f64,
    /// Events per year and the fraction of nodes each destroys.
    pub bursts: Vec<(f64, f64)>,
    /// Zones the stripe spreads over, and each zone's losses per year.
    pub zones: Option<(usize, f64)>,
    /// The highest annual loss probability a block may have.
    pub target: f64,
}

pub fn durability(out: &mut impl Write, options: &Options) -> Result<(), String> {
    if !(options.repair_hours.is_finite() && options.repair_hours > 0.0) {
        return Err(format!(
            "--repair-hours {} is not a positive duration",
            options.repair_hours
        ));
    }
    let rates = Rates {
        chunk: options.annual_failure / YEAR,
        domains: options.zones.map(|(domains, per_year)| DomainLoss {
            domains,
            rate: per_year / YEAR,
        }),
        bursts: options
            .bursts
            .iter()
            .map(|&(per_year, fraction)| Burst {
                rate: per_year / YEAR,
                fraction,
            })
            .collect(),
        repair: 1.0 / options.repair_hours,
    };
    writeln!(
        out,
        "  {:<9} {:>6} {:>9} {:>22} {:>18}",
        "scheme", "chunks", "overhead", "mean years to loss", "loss in a year"
    )
    .map_err(|e| e.to_string())?;
    let candidates = durability::candidates();
    for scheme in &candidates {
        let fits = scheme.width() <= options.domains;
        let mean = durability::mean_time_to_loss(*scheme, &rates).map_err(|e| e.to_string())?;
        let annual = durability::loss_within(*scheme, &rates, YEAR).map_err(|e| e.to_string())?;
        writeln!(
            out,
            "  {:<9} {:>6} {:>9.2} {:>22} {:>18}{}",
            name(scheme),
            scheme.width(),
            overhead(scheme),
            format!("{:.3e}", mean / YEAR),
            format!("{:.3e}", annual),
            if fits {
                ""
            } else {
                "  (wider than the domains)"
            }
        )
        .map_err(|e| e.to_string())?;
    }
    let choice = durability::choose(&candidates, options.domains, &rates, options.target)
        .map_err(|e| e.to_string())?;
    match choice {
        Some(Choice::Meets {
            scheme,
            annual_loss,
        }) => writeln!(
            out,
            "{} meets the target of {:.0e} a year at the least overhead: {:.3e}",
            name(&scheme),
            options.target,
            annual_loss
        ),
        Some(Choice::Short {
            scheme,
            annual_loss,
        }) => writeln!(
            out,
            "no scheme over {} domains meets the target of {:.0e} a year; {} comes closest at \
             {:.3e}",
            options.domains,
            options.target,
            name(&scheme),
            annual_loss
        ),
        None => writeln!(out, "no scheme fits {} domains", options.domains),
    }
    .map_err(|e| e.to_string())
}

fn name(scheme: &Scheme) -> String {
    match scheme {
        Scheme::Copies(n) => format!("R{n}"),
        Scheme::Rs(code) => format!("RS({},{})", code.data(), code.parity()),
    }
}

fn overhead(scheme: &Scheme) -> f64 {
    let (width, needed) = (scheme.width(), scheme.needed());
    let as_f64 = |n: usize| u32::try_from(n).map_or(f64::from(u32::MAX), f64::from);
    as_f64(width) / as_f64(needed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_scheme_is_listed_and_one_is_chosen() {
        let mut out = Vec::new();
        durability(
            &mut out,
            &Options {
                domains: 12,
                annual_failure: 0.04,
                repair_hours: 1.0,
                bursts: Vec::new(),
                zones: None,
                target: 1e-11,
            },
        )
        .unwrap();
        let text = String::from_utf8(out).unwrap();
        for scheme in ["R1", "R3", "RS(4,2)", "RS(6,3)", "RS(9,6)"] {
            assert!(text.contains(scheme), "{scheme} missing:\n{text}");
        }
        assert!(text.contains("(wider than the domains)"), "{text}");
        assert!(text.contains("meets the target"), "{text}");
    }
}
