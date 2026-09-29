//! How likely a block is to be lost under a scheme, and the scheme a block should use
//! (docs/design/durability.md; docs/research/15).
//!
//! A stripe is a Markov chain over how many of its chunks are lost, in the manner of Ford et
//! al. (OSDI 2010, §7): chunks are lost one at a time as their devices fail, several at once
//! when a failure domain is lost or an event strikes across the cluster, and repaired one at a
//! time, and the stripe is lost once fewer chunks remain than it needs. Ford's chain counts
//! chunks unavailable for fifteen minutes or more; this one counts chunks lost for good, which
//! is what durability is (docs/research/15 §1.1).
//!
//! The mean time to loss is the chain's mean time to absorption. Loss is rare against repair,
//! so the chain's rates span ten orders of magnitude and Gaussian elimination loses most of
//! its digits to cancellation (docs/research/15 §4.1). The chain is solved instead by
//! eliminating states one at a time from its jump chain, Kohlas's method in Hunter's form
//! (Special Matrices 2016, Theorem 2): every step adds, multiplies and divides non-negative
//! numbers, so no digits are lost to subtraction.

use crate::Code;

/// Hours in a year of 365.25 days.
pub const YEAR: f64 = 8766.0;

/// How a block is stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheme {
    /// Whole copies, any one of which serves.
    Copies(usize),
    /// A Reed–Solomon code, any `data` of whose chunks rebuild the rest.
    Rs(Code),
}

impl Scheme {
    /// Chunks the scheme writes, one to a failure domain.
    pub fn width(&self) -> usize {
        match self {
            Self::Copies(n) => *n,
            Self::Rs(code) => code.width(),
        }
    }

    /// Chunks that must survive for the block to be read.
    pub fn needed(&self) -> usize {
        match self {
            Self::Copies(_) => 1,
            Self::Rs(code) => code.data(),
        }
    }

    /// Whether `self` stores fewer bytes per byte of block than `other`: the ratio of width to
    /// chunks needed, compared exactly.
    pub fn cheaper_than(&self, other: &Self) -> bool {
        let ours = self.width().checked_mul(other.needed());
        let theirs = other.width().checked_mul(self.needed());
        matches!((ours, theirs), (Some(a), Some(b)) if a < b)
    }

    /// Whether `self` stores exactly as many bytes per byte of block as `other`.
    fn costs_as_much_as(&self, other: &Self) -> bool {
        let ours = self.width().checked_mul(other.needed());
        let theirs = other.width().checked_mul(self.needed());
        matches!((ours, theirs), (Some(a), Some(b)) if a == b)
    }
}

/// The schemes a block may use: one to three whole copies, three being the replication of a
/// block still being written (docs/research/04 §R1.1), and every code of [`crate::CODES`].
pub fn candidates() -> Vec<Scheme> {
    let copies = (1..=3).map(Scheme::Copies);
    let codes = crate::CODES
        .into_iter()
        .filter_map(|(data, parity)| Code::new(data, parity).ok().map(Scheme::Rs));
    copies.chain(codes).collect()
}

/// The rates, per hour, at which a stripe's chunks are lost and repaired.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Rates {
    /// Each chunk's loss on its own: its device's failure rate.
    pub chunk: f64,
    /// Losses of whole failure domains.
    pub domains: Option<DomainLoss>,
    /// Events that strike chunks across domains at once.
    pub bursts: Vec<Burst>,
    /// Repair of one lost chunk, from the loss to the rebuilt chunk, one chunk at a time:
    /// Ford et al.'s serial repair, which they choose "to gain more conservative estimates"
    /// (§7.1).
    pub repair: f64,
}

/// Losses of whole failure domains: `domains` of them, the stripe's chunks spread over them as
/// evenly as they go, each domain lost at `rate` per hour.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DomainLoss {
    pub domains: usize,
    pub rate: f64,
}

/// Events at `rate` per hour, each destroying every chunk independently with probability
/// `fraction`: a random share of the cluster's nodes that does not come back.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Burst {
    pub rate: f64,
    pub fraction: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
pub enum DurabilityError {
    #[error("a scheme that needs {needed} of {width} chunks")]
    Scheme { width: usize, needed: usize },
    #[error("a rate or probability that is negative or not finite")]
    Rate,
}

/// The mean time, in hours, until a whole stripe of `scheme` loses more chunks than it can
/// rebuild from; infinite when nothing can lose it.
pub fn mean_time_to_loss(scheme: Scheme, rates: &Rates) -> Result<f64, DurabilityError> {
    let q = generator(scheme, rates)?;
    Ok(absorption_time(q))
}

/// The probability that a whole stripe is lost within `hours`, from its mean time to loss.
/// Repairs end in hours and losses come years apart, so the stripe returns to whole many times
/// before it is lost, and the time to loss is close to exponential with that mean
/// (docs/research/15 §4.4; checked against the exact transient in the tests).
pub fn loss_within(mean_time_to_loss: f64, hours: f64) -> f64 {
    -(-hours / mean_time_to_loss).exp_m1()
}

/// What [`choose`] finds.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Choice {
    /// The scheme of least overhead whose annual loss probability is within the target.
    Meets { scheme: Scheme, annual_loss: f64 },
    /// No scheme that fits meets the target; this one comes closest.
    Short { scheme: Scheme, annual_loss: f64 },
}

/// The scheme a block should use: among `candidates` no wider than `domains`, the one of least
/// overhead whose annual loss probability is at most `target`, the narrower of two that cost
/// the same, since a repair reads fewer chunks. `None` when no candidate fits the domains.
pub fn choose(
    candidates: &[Scheme],
    domains: usize,
    rates: &Rates,
    target: f64,
) -> Result<Option<Choice>, DurabilityError> {
    let mut meets: Option<(Scheme, f64)> = None;
    let mut closest: Option<(Scheme, f64)> = None;
    for &scheme in candidates.iter().filter(|s| s.width() <= domains) {
        let loss = loss_within(mean_time_to_loss(scheme, rates)?, YEAR);
        if closest.is_none_or(|(_, best)| loss < best) {
            closest = Some((scheme, loss));
        }
        if loss <= target
            && meets.is_none_or(|(best, _)| {
                scheme.cheaper_than(&best)
                    || (scheme.costs_as_much_as(&best) && scheme.width() < best.width())
            })
        {
            meets = Some((scheme, loss));
        }
    }
    Ok(match (meets, closest) {
        (Some((scheme, annual_loss)), _) => Some(Choice::Meets {
            scheme,
            annual_loss,
        }),
        (None, Some((scheme, annual_loss))) => Some(Choice::Short {
            scheme,
            annual_loss,
        }),
        (None, None) => None,
    })
}

/// The chain's rates: `q[t][u]` from `t` chunks lost to `u`, for `t` from 0 to the most the
/// stripe survives, with loss the last state.
fn generator(scheme: Scheme, rates: &Rates) -> Result<Vec<Vec<f64>>, DurabilityError> {
    let (width, needed) = (scheme.width(), scheme.needed());
    let spare = width
        .checked_sub(needed)
        .filter(|_| needed > 0)
        .ok_or(DurabilityError::Scheme { width, needed })?;
    let valid = |x: f64| x.is_finite() && x >= 0.0;
    let bursts_valid = rates
        .bursts
        .iter()
        .all(|b| valid(b.rate) && valid(b.fraction) && b.fraction <= 1.0);
    let domains_valid = rates.domains.is_none_or(|d| valid(d.rate) && d.domains > 0);
    if !(valid(rates.chunk) && valid(rates.repair) && bursts_valid && domains_valid) {
        return Err(DurabilityError::Rate);
    }
    // States 0..=spare lost, then loss.
    let states = spare.saturating_add(2);
    let loss = spare.saturating_add(1);
    let mut q = vec![vec![0.0; states]; states];
    let mut add = |from: usize, lost: usize, rate: f64| {
        let to = from.saturating_add(lost).min(loss);
        if lost > 0
            && let Some(cell) = q.get_mut(from).and_then(|row| row.get_mut(to))
        {
            *cell += rate;
        }
    };
    for t in 0..=spare {
        let available = width.saturating_sub(t);
        add(t, 1, count(available) * rates.chunk);
        if let Some(d) = rates.domains {
            // The chunks still there, spread evenly: `extra` domains hold one more.
            let each = available.checked_div(d.domains).unwrap_or(0);
            let extra = available.checked_rem(d.domains).unwrap_or(0);
            add(t, each.saturating_add(1), count(extra) * d.rate);
            add(t, each, count(d.domains.saturating_sub(extra)) * d.rate);
        }
        for burst in &rates.bursts {
            for (hit, p) in binomial(available, burst.fraction).into_iter().enumerate() {
                add(t, hit, burst.rate * p);
            }
        }
    }
    for t in 1..=spare {
        if let Some(cell) = q
            .get_mut(t)
            .and_then(|row| row.get_mut(t.saturating_sub(1)))
        {
            *cell += rates.repair;
        }
    }
    Ok(q)
}

/// P(h of `n` are struck) for h = 0..=n, each struck independently with probability `f`.
fn binomial(n: usize, f: f64) -> Vec<f64> {
    let mut out = Vec::with_capacity(n.saturating_add(1));
    let mut choose = 1.0;
    for h in 0..=n {
        let rest = i32::try_from(n.saturating_sub(h)).unwrap_or(i32::MAX);
        let struck = i32::try_from(h).unwrap_or(i32::MAX);
        out.push(choose * f.powi(struck) * (1.0 - f).powi(rest));
        // C(n, h+1) = C(n, h)·(n−h)/(h+1).
        choose = choose * count(n.saturating_sub(h)) / count(h.saturating_add(1));
    }
    out
}

fn count(n: usize) -> f64 {
    u32::try_from(n).map_or(f64::from(u32::MAX), f64::from)
}

/// The mean time from state 0 to the last state, which absorbs, of the chain whose rates are
/// `q`. The chain's jump chain holds each state's exit probabilities and mean holding time;
/// eliminating a state folds its paths and time into the states that lead to it (Hunter,
/// Special Matrices 2016, Theorem 2, (31)–(32)), and the sum of a state's exits other than to
/// itself stands for one minus its self-loop, so nothing is subtracted. When state 0 and loss
/// alone remain, the mean time is state 0's holding time over its chance of leaving for loss
/// (Hunter (49)).
fn absorption_time(q: Vec<Vec<f64>>) -> f64 {
    let states = q.len();
    let Some(loss) = states.checked_sub(1).filter(|&l| l > 0) else {
        return f64::INFINITY;
    };
    let mut p = q;
    let mut hold = vec![0.0; loss];
    for (t, row) in p.iter_mut().enumerate().take(loss) {
        let total: f64 = row
            .iter()
            .enumerate()
            .filter(|&(u, _)| u != t)
            .map(|(_, r)| r)
            .sum();
        if total <= 0.0 {
            // A state nothing leaves: loss is never reached from it.
            return f64::INFINITY;
        }
        for (u, r) in row.iter_mut().enumerate() {
            *r = if u == t { 0.0 } else { *r / total };
        }
        if let Some(h) = hold.get_mut(t) {
            *h = 1.0 / total;
        }
    }
    // Eliminate the transient states from the last to the second.
    for m in (1..loss).rev() {
        let Some(row_m) = p.get(m).cloned() else {
            return f64::INFINITY;
        };
        let hold_m = hold.get(m).copied().unwrap_or(0.0);
        let leave: f64 = row_m
            .iter()
            .enumerate()
            .filter(|&(j, _)| j < m || j == loss)
            .map(|(_, r)| r)
            .sum();
        if leave <= 0.0 {
            return f64::INFINITY;
        }
        for i in 0..m {
            let Some(to_m) = p.get(i).and_then(|row| row.get(m)).copied() else {
                continue;
            };
            if to_m <= 0.0 {
                continue;
            }
            if let Some(row_i) = p.get_mut(i) {
                for (j, r) in row_i.iter_mut().enumerate() {
                    if j < m || j == loss {
                        *r += to_m * row_m.get(j).copied().unwrap_or(0.0) / leave;
                    }
                }
                if let Some(r) = row_i.get_mut(m) {
                    *r = 0.0;
                }
            }
            if let Some(h) = hold.get_mut(i) {
                *h += to_m * hold_m / leave;
            }
        }
    }
    let to_loss = p
        .first()
        .and_then(|row| row.get(loss))
        .copied()
        .unwrap_or(0.0);
    let hold_0 = hold.first().copied().unwrap_or(0.0);
    if to_loss > 0.0 {
        hold_0 / to_loss
    } else {
        f64::INFINITY
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rs(data: usize, parity: usize) -> Scheme {
        Scheme::Rs(Code::new(data, parity).unwrap())
    }

    fn independent(chunk: f64, repair: f64) -> Rates {
        Rates {
            chunk,
            repair,
            ..Rates::default()
        }
    }

    /// Ford et al. §8.2's closed form for chunks failing independently at `λ` each and
    /// repaired one at a time at `ρ`, a stripe of `s` chunks needing `r`:
    /// (1/λ) Σ_{k=0}^{s−r} Σ_{i=0}^{k} (ρ/λ)^i / (s−k+i)_{(i+1)}, with (a)_{(b)} the falling
    /// factorial. Every term is positive, so it is an accurate reference.
    fn ford(s: usize, r: usize, lambda: f64, rho: f64) -> f64 {
        let falling = |a: usize, b: usize| (0..b).map(|j| (a - j) as f64).product::<f64>();
        let mut sum = 0.0;
        for k in 0..=(s - r) {
            for i in 0..=k {
                sum += (rho / lambda).powi(i32::try_from(i).unwrap()) / falling(s - k + i, i + 1);
            }
        }
        sum / lambda
    }

    fn close(a: f64, b: f64, tolerance: f64) -> bool {
        ((a - b) / b).abs() <= tolerance
    }

    /// The reduction reproduces the closed form to rounding, across widths and across ratios of
    /// repair to failure from one to a trillion, where elimination with subtraction loses
    /// nearly every digit (docs/research/15 §4.1).
    #[test]
    fn independent_failures_match_fords_closed_form() {
        let schemes = [
            Scheme::Copies(1),
            Scheme::Copies(2),
            Scheme::Copies(3),
            Scheme::Copies(5),
            rs(2, 1),
            rs(4, 2),
            rs(6, 3),
            rs(8, 4),
            rs(10, 4),
            rs(9, 6),
        ];
        for scheme in schemes {
            for ratio in [1.0, 1e2, 1e4, 1e8, 1e12] {
                let (lambda, rho) = (1e-6, 1e-6 * ratio);
                let got = mean_time_to_loss(scheme, &independent(lambda, rho)).unwrap();
                let want = ford(scheme.width(), scheme.needed(), lambda, rho);
                assert!(
                    close(got, want, 1e-12),
                    "{scheme:?} at ρ/λ = {ratio}: {got} against {want}"
                );
            }
        }
    }

    /// Ford §8.2's special cases, and his scaling: with repair far faster than failure, cutting
    /// repair time by µ multiplies the mean time to loss by µ^(s−r), µ² for three copies and
    /// µ⁴ for RS(9,4)'s shape.
    #[test]
    fn two_and_three_copies_and_the_scaling_of_repair() {
        let (l, p) = (1e-5, 0.5);
        let two = mean_time_to_loss(Scheme::Copies(2), &independent(l, p)).unwrap();
        assert!(close(two, (3.0 * l + p) / (2.0 * l * l), 1e-13));
        let three = mean_time_to_loss(Scheme::Copies(3), &independent(l, p)).unwrap();
        let want = (11.0 * l * l + 4.0 * l * p + p * p) / (6.0 * l * l * l);
        assert!(close(three, want, 1e-13));
        for (scheme, exponent) in [(Scheme::Copies(3), 2), (rs(9, 4), 4)] {
            let base = mean_time_to_loss(scheme, &independent(1e-8, 1.0)).unwrap();
            let faster = mean_time_to_loss(scheme, &independent(1e-8, 10.0)).unwrap();
            assert!(
                close(faster / base, 10f64.powi(exponent), 1e-5),
                "{scheme:?}"
            );
        }
    }

    /// With repair far faster than every loss, a burst finds the stripe whole, and the mean
    /// time to loss is one over the rate of bursts that strike more chunks than the stripe
    /// spares.
    #[test]
    fn bursts_lose_what_they_strike_beyond_the_spare_chunks() {
        let (rate, fraction) = (1e-4, 0.01);
        for scheme in [Scheme::Copies(3), rs(6, 3), rs(8, 4), rs(9, 6)] {
            let rates = Rates {
                bursts: vec![Burst { rate, fraction }],
                repair: 1e6,
                ..Rates::default()
            };
            let n = scheme.width();
            let spare = n - scheme.needed();
            let fatal: f64 = binomial(n, fraction).iter().skip(spare + 1).sum();
            let got = mean_time_to_loss(scheme, &rates).unwrap();
            assert!(close(got, 1.0 / (rate * fatal), 1e-4), "{scheme:?}");
        }
        // A burst that destroys everything loses the stripe at its own rate.
        let all = Rates {
            bursts: vec![Burst {
                rate,
                fraction: 1.0,
            }],
            repair: 1.0,
            ..Rates::default()
        };
        let got = mean_time_to_loss(rs(6, 3), &all).unwrap();
        assert!(close(got, 1.0 / rate, 1e-12));
    }

    /// A domain that holds one chunk of the stripe is one more way to lose a single chunk; a
    /// domain holding three of RS(6,3)'s nine is survived, and one holding four is not.
    #[test]
    fn domain_losses_strike_the_chunks_a_domain_holds() {
        let (lambda, delta, rho) = (1e-5, 3e-6, 0.5);
        let spread = Rates {
            chunk: lambda,
            domains: Some(DomainLoss {
                domains: 12,
                rate: delta,
            }),
            repair: rho,
            ..Rates::default()
        };
        let folded = independent(lambda + delta, rho);
        for scheme in [Scheme::Copies(3), rs(8, 4)] {
            let a = mean_time_to_loss(scheme, &spread).unwrap();
            let b = mean_time_to_loss(scheme, &folded).unwrap();
            assert!(close(a, b, 1e-12), "{scheme:?}");
        }
        // Over two zones every zone lost takes four or five of RS(6,3)'s nine chunks, more than
        // its three spare. Over three a zone takes three, which the stripe survives unless a
        // second zone goes before repair: some thousands of times longer.
        let rate = 1e-6;
        let zones = |domains| Rates {
            domains: Some(DomainLoss { domains, rate }),
            repair: 1.0,
            ..Rates::default()
        };
        let two = mean_time_to_loss(rs(6, 3), &zones(2)).unwrap();
        assert!(close(two, 1.0 / (2.0 * rate), 1e-12), "{two}");
        let three = mean_time_to_loss(rs(6, 3), &zones(3)).unwrap();
        assert!(three > 1e4 / rate, "{three}");
    }

    /// The exponential law from the mean against the exact transient, computed by
    /// uniformization (all terms non-negative): three copies over a year, at rates where the
    /// stripe is lost within the year with probability near one in a thousand and repair
    /// takes an hour, so the stripe settles within hours of the year's start.
    #[test]
    fn loss_within_a_year_matches_the_exact_transient() {
        let rates = independent(2.5e-3, 1.0);
        let scheme = Scheme::Copies(3);
        let q = generator(scheme, &rates).unwrap();
        let n = q.len();
        let exit: Vec<f64> = (0..n)
            .map(|i| (0..n).filter(|&j| j != i).map(|j| q[i][j]).sum())
            .collect();
        let lambda = exit.iter().cloned().fold(0.0, f64::max);
        // P = I + Q/Λ; the loss state keeps what reaches it.
        let step = |v: &Vec<f64>| -> Vec<f64> {
            let mut out = vec![0.0; n];
            for i in 0..n {
                let stay = if i == n - 1 {
                    1.0
                } else {
                    1.0 - exit[i] / lambda
                };
                out[i] += v[i] * stay;
                if i != n - 1 {
                    for j in 0..n {
                        if j != i {
                            out[j] += v[i] * q[i][j] / lambda;
                        }
                    }
                }
            }
            out
        };
        let t = YEAR;
        // Past twice the Poisson mean and a margin, the weights left are negligible.
        let limit = lambda * t * 2.0 + 200.0;
        let mut v = vec![0.0; n];
        v[0] = 1.0;
        let mut log_weight = -lambda * t; // ln Poisson(0; Λt)
        let mut exact = 0.0;
        let mut k = 0usize;
        while (k as f64) < limit {
            exact += log_weight.exp() * v[n - 1];
            v = step(&v);
            k += 1;
            log_weight += (lambda * t).ln() - (k as f64).ln();
        }
        let mean = mean_time_to_loss(scheme, &rates).unwrap();
        let approx = loss_within(mean, t);
        assert!(exact > 1e-4 && exact < 1e-2, "{exact}");
        assert!(close(approx, exact, 1e-3), "{approx} against {exact}");
    }

    /// The choice: the cheapest scheme within the target, the narrower of equal costs, never
    /// wider than the domains, and the closest when none meets the target.
    #[test]
    fn the_cheapest_scheme_within_the_target_is_chosen() {
        let candidates = [
            Scheme::Copies(1),
            Scheme::Copies(2),
            Scheme::Copies(3),
            rs(4, 2),
            rs(6, 3),
            rs(8, 4),
            rs(9, 6),
        ];
        // Chunks lost at 4% a year, rebuilt within an hour: independent loss alone.
        let rates = independent(0.04 / YEAR, 1.0);
        let pick = |domains, target| choose(&candidates, domains, &rates, target).unwrap();
        // RS(4,2), RS(6,3) and RS(8,4) all cost 1.5; the narrowest wins.
        assert!(matches!(
            pick(12, 1e-9),
            Some(Choice::Meets { scheme, .. }) if scheme == rs(4, 2)
        ));
        // A tighter target needs more parity.
        assert!(matches!(
            pick(12, 1e-13),
            Some(Choice::Meets { scheme, .. }) if scheme == rs(6, 3)
        ));
        // Three domains leave only copies.
        assert!(matches!(
            pick(3, 1e-9),
            Some(Choice::Meets { scheme, .. }) if scheme == Scheme::Copies(3)
        ));
        // One domain cannot meet eleven nines; the best it has is reported.
        assert!(matches!(
            pick(1, 1e-11),
            Some(Choice::Short { scheme, .. }) if scheme == Scheme::Copies(1)
        ));
        assert_eq!(pick(0, 1e-11), None);
    }

    #[test]
    fn every_stored_code_is_a_candidate() {
        let all = candidates();
        assert_eq!(all.len(), 3 + crate::CODES.len());
        assert!(all.contains(&Scheme::Copies(3)) && all.contains(&rs(9, 6)));
    }

    #[test]
    fn nonsense_is_refused() {
        let bad = [
            independent(-1.0, 1.0),
            independent(f64::NAN, 1.0),
            Rates {
                bursts: vec![Burst {
                    rate: 1.0,
                    fraction: 1.5,
                }],
                ..Rates::default()
            },
            Rates {
                domains: Some(DomainLoss {
                    domains: 0,
                    rate: 1.0,
                }),
                ..Rates::default()
            },
        ];
        for rates in bad {
            assert_eq!(
                mean_time_to_loss(Scheme::Copies(3), &rates),
                Err(DurabilityError::Rate)
            );
        }
        assert!(matches!(
            mean_time_to_loss(Scheme::Copies(0), &independent(1.0, 1.0)),
            Err(DurabilityError::Scheme { .. })
        ));
        // Nothing fails: never lost.
        assert_eq!(
            mean_time_to_loss(Scheme::Copies(3), &independent(0.0, 1.0)),
            Ok(f64::INFINITY)
        );
    }
}
