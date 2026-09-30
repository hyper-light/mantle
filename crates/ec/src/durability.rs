//! How likely a block is to be lost under a scheme, and the scheme a block should use
//! (docs/design/durability.md; docs/research/15).
//!
//! A stripe is a Markov chain over where its surviving chunks are, in the manner of Ford et
//! al. (OSDI 2010, §7): chunks are lost one at a time as their devices fail, a whole failure
//! domain's at once when the domain is lost, several across domains when an event strikes
//! the cluster, and repaired one at a time, and the stripe is lost once fewer chunks remain
//! than it needs. Ford's chain counts chunks unavailable for fifteen minutes or more; this one
//! counts chunks lost for good, which is what durability is (docs/research/15 §1.1). A state
//! is how many domains hold each count of the stripe's chunks: a chunk lost stays lost from
//! its own domain, so the next domain lost takes what that domain still holds, and repair
//! rebuilds each chunk in a domain holding the fewest (audit B09). Domains are alike, so which
//! domains hold what does not matter, only how many hold each count.
//!
//! The mean time to loss is the chain's mean time to absorption. Loss is rare against repair,
//! so the chain's rates span ten orders of magnitude and Gaussian elimination loses most of
//! its digits to cancellation (docs/research/15 §4.1). The chain is solved instead by
//! eliminating states one at a time from its jump chain, Kohlas's method in Hunter's form
//! (Special Matrices 2016, Theorem 2): every step adds, multiplies and divides non-negative
//! numbers, so no digits are lost to subtraction.
//!
//! The probability of loss within a time is not an estimate but an enclosure: every number
//! that feeds it is carried as an interval rounded outward, so the upper end is at least the
//! chain's exact probability and the lower end at most it (docs/research/15 §4.6). A reported
//! loss probability is the upper end, never optimistic about the chain it solves.

use crate::Code;

/// Hours in a year of 365.25 days.
pub const YEAR: f64 = 8766.0;

/// Field inputs for a deployment that has not yet measured its own, each the conservative end
/// of the primary-source data in docs/research/15 §8, per year (docs/design/durability.md §5).
pub mod field {
    /// A disk's permanent failures a year: 6.30%, the highest per-model annualized failure
    /// rate in Backblaze's 2025 fleet, 4.6 times the fleet's 1.36%, since a stripe's chunks
    /// can share a model and a batch (docs/research/15 §8.1).
    pub const DISK_FAILURES: f64 = 0.063;
    /// A flash device's permanent failures a year: 2.7%, the worst four-year replacement
    /// fraction of Schroeder et al. (FAST 2016, Table 5), 10.31%, as a constant hazard
    /// (docs/research/15 §8.2).
    pub const FLASH_FAILURES: f64 = 0.027;
    /// Power-on restarts that lose nodes, a year: one. Cidon et al. (ATC 2013) state "once or
    /// twice per year", a frequency their cited source does not confirm (docs/research/15
    /// §8.4): the least-qualified input here.
    pub const POWER_LOSSES: f64 = 1.0;
    /// The share of nodes such a restart destroys: 1%, the upper end of HDFS's "one-half to one
    /// percent of the nodes will not survive a full power-on restart" (Shvachko et al., MSST
    /// 2010; docs/research/15 §8.4).
    pub const POWER_LOSS_FRACTION: f64 = 0.01;
}

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

/// The copies a block may have: one to three, three being the replication of a block still
/// being written (docs/research/04 §R1.1).
const MOST_COPIES: usize = 3;

/// The schemes a block may use: one to three whole copies, and every code of
/// [`crate::CODES`].
pub fn candidates() -> Vec<Scheme> {
    let copies = (1..=MOST_COPIES).map(Scheme::Copies);
    let codes = crate::CODES
        .into_iter()
        .filter_map(|(data, parity)| Code::new(data, parity).ok().map(Scheme::Rs));
    copies.chain(codes).collect()
}

/// The widest stripe the model takes: the widest scheme mantle stores. The chain's states
/// grow with the width, and the transient's matrices with the square of the states, so this
/// bounds both (CLAUDE.md §2).
fn widest() -> usize {
    crate::CODES
        .into_iter()
        .filter_map(|(data, parity)| data.checked_add(parity))
        .fold(MOST_COPIES, usize::max)
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

/// Losses of whole failure domains: `domains` of them, the stripe's chunks placed over them
/// as evenly as they go, each domain lost at `rate` per hour. A domain lost takes the chunks
/// it holds then; it is replaced, and repair places chunks in it again.
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

/// The probability that a stripe is lost within a time, enclosed: the chain's exact
/// probability is at least `lower` and at most `upper`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LossBounds {
    pub lower: f64,
    pub upper: f64,
}

/// The mean time, in hours, until a whole stripe of `scheme` loses more chunks than it can
/// rebuild from; infinite when nothing can lose it.
pub fn mean_time_to_loss(scheme: Scheme, rates: &Rates) -> Result<f64, DurabilityError> {
    let q = estimates(&generator(scheme, rates)?);
    Ok(absorption_time(q))
}

/// A guaranteed upper bound on the probability that a whole stripe of `scheme` is lost within
/// `hours`: the upper end of [`loss_bounds`].
pub fn loss_within(scheme: Scheme, rates: &Rates, hours: f64) -> Result<f64, DurabilityError> {
    Ok(loss_bounds(scheme, rates, hours)?.upper)
}

/// The probability that a whole stripe of `scheme` is lost within `hours`, enclosed.
///
/// It is the whole state's entry for loss in the transient e^(Qt), computed by scaling and
/// squaring the uniformized series e^(Qτ) = e^(−Λτ) Σₙ (Λτ)ⁿ/n! Pⁿ, P = I + Q/Λ, for τ = t/2ˢ
/// with Λτ ≤ ½ (docs/research/15 §4.6). Every term is non-negative, so no digit is lost to
/// subtraction; every operation is rounded outward; the series' tail is added to the upper
/// end, bounded by its first omitted weight since each Pⁿ is stochastic; and e^(−Λτ) is
/// enclosed as one over the weights' sum without calling a library exponential, whose error
/// the standard library does not state. The enclosure therefore holds whatever the rates are,
/// where the exponential law 1 − e^(−t/M) this replaced held only where loss is rare against
/// repair, and then to an order of approximation, not a bound (docs/research/15 §4.5).
pub fn loss_bounds(
    scheme: Scheme,
    rates: &Rates,
    hours: f64,
) -> Result<LossBounds, DurabilityError> {
    if !(hours.is_finite() && hours >= 0.0) {
        return Err(DurabilityError::Rate);
    }
    let q = generator(scheme, rates)?;
    transient_loss(&q, hours).ok_or(DurabilityError::Rate)
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
/// The probability compared is the upper end of its enclosure, so a scheme is never chosen
/// on rounding in its favour.
pub fn choose(
    candidates: &[Scheme],
    domains: usize,
    rates: &Rates,
    target: f64,
) -> Result<Option<Choice>, DurabilityError> {
    let mut meets: Option<(Scheme, f64)> = None;
    let mut closest: Option<(Scheme, f64)> = None;
    for &scheme in candidates.iter().filter(|s| s.width() <= domains) {
        let loss = loss_within(scheme, rates, YEAR)?;
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

/// A non-negative real known to lie in `[lo, hi]`, with `est` its value in ordinary
/// round-to-nearest arithmetic. Each operation rounds `lo` down and `hi` up by one step past
/// its round-to-nearest result, which lies within half a step of the exact one, so the
/// interval always holds the exact value (IEEE 754 §4.3.1; docs/research/15 §4.6).
#[derive(Debug, Clone, Copy, PartialEq)]
struct Enclosed {
    lo: f64,
    est: f64,
    hi: f64,
}

impl Enclosed {
    const ZERO: Self = Self::exact(0.0);

    const fn exact(x: f64) -> Self {
        Self {
            lo: x,
            est: x,
            hi: x,
        }
    }

    fn add(self, other: Self) -> Self {
        Self {
            lo: down(self.lo + other.lo),
            est: self.est + other.est,
            hi: up_sum(self.hi, other.hi),
        }
    }

    fn mul(self, other: Self) -> Self {
        Self {
            lo: down(self.lo * other.lo),
            est: self.est * other.est,
            hi: up_product(self.hi, other.hi),
        }
    }

    /// `self / other`; a divisor whose lower end is zero leaves the upper end unbounded.
    fn div(self, other: Self) -> Self {
        let hi = if self.hi == 0.0 {
            0.0
        } else if other.lo > 0.0 {
            (self.hi / other.lo).next_up()
        } else {
            f64::INFINITY
        };
        Self {
            lo: if other.hi > 0.0 {
                down(self.lo / other.hi)
            } else {
                0.0
            },
            est: self.est / other.est,
            hi,
        }
    }

    /// 1 − `self`, for `self` a probability. The difference is exact for a subtrahend of at
    /// least ½ (Sterbenz), so only a smaller one is rounded outward: a probability of exactly
    /// one leaves exactly zero, not a spurious least positive number.
    fn complement(self) -> Self {
        Self {
            lo: if self.hi >= 0.5 {
                (1.0 - self.hi).max(0.0)
            } else {
                down(1.0 - self.hi)
            },
            est: 1.0 - self.est,
            hi: if self.lo >= 0.5 {
                1.0 - self.lo
            } else {
                (1.0 - self.lo).next_up().min(1.0)
            },
        }
    }
}

/// One step below `x`, and not below zero: at most the exact value `x` rounds, for a
/// non-negative exact value.
fn down(x: f64) -> f64 {
    x.next_down().max(0.0)
}

/// At least `a + b` for non-negative `a` and `b`: exact when both are zero.
fn up_sum(a: f64, b: f64) -> f64 {
    let s = a + b;
    if s == 0.0 { 0.0 } else { s.next_up() }
}

/// At least `a · b` for non-negative `a` and `b`: exact when either is zero, and a positive
/// product that underflows to zero is bounded by the least positive number.
fn up_product(a: f64, b: f64) -> f64 {
    if a == 0.0 || b == 0.0 {
        0.0
    } else {
        (a * b).next_up()
    }
}

/// How many failure domains hold each count of a stripe's chunks: `holding[k]` hold `k`.
type Occupancy = Vec<usize>;

/// The chunks a state holds; `None` past `usize`, which a stripe no wider than
/// [`widest`] never reaches.
fn held(state: &Occupancy) -> Option<usize> {
    state
        .iter()
        .enumerate()
        .try_fold(0usize, |sum, (k, &h)| sum.checked_add(k.checked_mul(h)?))
}

/// The chain's rates, `q[s][u]` from state `s` to `u`, each enclosed: the states the stripe
/// can be in while it survives, reached from the chunks placed as evenly as the domains
/// allow, which is state 0, and loss, the last state.
fn generator(scheme: Scheme, rates: &Rates) -> Result<Vec<Vec<Enclosed>>, DurabilityError> {
    let (width, needed) = (scheme.width(), scheme.needed());
    if width < needed || needed == 0 || width > widest() {
        return Err(DurabilityError::Scheme { width, needed });
    }
    let valid = |x: f64| x.is_finite() && x >= 0.0;
    let bursts_valid = rates
        .bursts
        .iter()
        .all(|b| valid(b.rate) && valid(b.fraction) && b.fraction <= 1.0);
    let domains_valid = rates.domains.is_none_or(|d| valid(d.rate) && d.domains > 0);
    if !(valid(rates.chunk) && valid(rates.repair) && bursts_valid && domains_valid) {
        return Err(DurabilityError::Rate);
    }
    // Without domain losses, a chunk's domain is only where repair puts it: each its own.
    let domains = rates.domains.map_or(width, |d| d.domains);
    let scheme_error = DurabilityError::Scheme { width, needed };
    // Domains are at least one, checked above, so neither division fails.
    let each = width.checked_div(domains).ok_or(scheme_error)?;
    let full = width.checked_rem(domains).ok_or(scheme_error)?;
    let most = if full > 0 {
        each.checked_add(1).ok_or(scheme_error)?
    } else {
        each
    };
    let mut whole: Occupancy = vec![0; most.checked_add(1).ok_or(scheme_error)?];
    if full > 0 {
        if let Some(h) = whole.get_mut(most) {
            *h = full;
        }
        if let Some(h) = whole.get_mut(each) {
            *h = domains.checked_sub(full).ok_or(scheme_error)?;
        }
    } else if let Some(h) = whole.get_mut(each) {
        *h = domains;
    }
    // The states reachable from whole, found breadth first, with each one's moves.
    let mut index: std::collections::HashMap<Occupancy, usize> = std::collections::HashMap::new();
    let mut states: Vec<Occupancy> = vec![whole.clone()];
    index.insert(whole, 0);
    let mut moves: Vec<Vec<(Option<usize>, Enclosed)>> = Vec::new();
    let mut at = 0usize;
    while let Some(state) = states.get(at).cloned() {
        let mut out = Vec::new();
        for (next, rate) in transitions(&state, width, rates).ok_or(scheme_error)? {
            if rate.hi <= 0.0 {
                continue;
            }
            if held(&next).ok_or(scheme_error)? < needed {
                out.push((None, rate));
                continue;
            }
            let to = match index.get(&next) {
                Some(&to) => to,
                None => {
                    let to = states.len();
                    index.insert(next.clone(), to);
                    states.push(next);
                    to
                }
            };
            out.push((Some(to), rate));
        }
        moves.push(out);
        at = at.checked_add(1).ok_or(scheme_error)?;
    }
    let loss = states.len();
    let size = loss.checked_add(1).ok_or(scheme_error)?;
    let mut q = vec![vec![Enclosed::ZERO; size]; size];
    for (from, out) in moves.into_iter().enumerate() {
        for (to, rate) in out {
            let to = to.unwrap_or(loss);
            if to != from
                && let Some(cell) = q.get_mut(from).and_then(|row| row.get_mut(to))
            {
                *cell = cell.add(rate);
            }
        }
    }
    Ok(q)
}

/// The round-to-nearest value of each enclosed rate.
fn estimates(q: &[Vec<Enclosed>]) -> Vec<Vec<f64>> {
    q.iter()
        .map(|row| row.iter().map(|r| r.est).collect())
        .collect()
}

/// Where `state` goes next, and at what rate: a chunk failing, a domain lost, a burst
/// striking, or a chunk rebuilt in a domain holding the fewest, one at a time. `None` if the
/// state's chunks overflow a count, which a stripe no wider than [`widest`] never does.
fn transitions(
    state: &Occupancy,
    width: usize,
    rates: &Rates,
) -> Option<Vec<(Occupancy, Enclosed)>> {
    let mut out = Vec::new();
    let moved = |from: usize, to: usize| -> Option<Occupancy> {
        let mut next = state.clone();
        let h = next.get_mut(from)?;
        *h = h.checked_sub(1)?;
        let h = next.get_mut(to)?;
        *h = h.checked_add(1)?;
        Some(next)
    };
    for (k, &domains) in state.iter().enumerate().skip(1) {
        if domains == 0 {
            continue;
        }
        let holding = Enclosed::exact(count(domains));
        out.push((
            moved(k, k.checked_sub(1)?)?,
            holding
                .mul(Enclosed::exact(count(k)))
                .mul(Enclosed::exact(rates.chunk)),
        ));
        if let Some(d) = rates.domains {
            out.push((moved(k, 0)?, holding.mul(Enclosed::exact(d.rate))));
        }
    }
    for burst in &rates.bursts {
        for (next, p) in struck(state, burst.fraction) {
            if &next != state {
                out.push((next, Enclosed::exact(burst.rate).mul(p)));
            }
        }
    }
    if held(state)? < width
        && let Some(fewest) = state.iter().position(|&h| h > 0)
    {
        out.push((
            moved(fewest, fewest.checked_add(1)?)?,
            Enclosed::exact(rates.repair),
        ));
    }
    Some(out)
}

/// What a burst that destroys each chunk with probability `f` leaves of `state`, with the
/// chance of each: a domain holding `k` chunks keeps `k − j` of them with the binomial chance
/// of `j` struck, each domain on its own.
fn struck(state: &Occupancy, f: f64) -> Vec<(Occupancy, Enclosed)> {
    let mut partial: std::collections::HashMap<Occupancy, Enclosed> =
        std::collections::HashMap::new();
    partial.insert(vec![0; state.len()], Enclosed::exact(1.0));
    for (k, &domains) in state.iter().enumerate() {
        if domains == 0 {
            continue;
        }
        // The domains holding `k`, each losing `j` with probability p[j]: how many lose each.
        let p = binomial(k, f);
        let mut next: std::collections::HashMap<Occupancy, Enclosed> =
            std::collections::HashMap::new();
        for (split, chance) in splits(domains, &p) {
            for (base, q) in &partial {
                let mut after = base.clone();
                for (j, &n) in split.iter().enumerate() {
                    if let Some(h) = k.checked_sub(j).and_then(|kept| after.get_mut(kept))
                        && let Some(sum) = h.checked_add(n)
                    {
                        *h = sum;
                    }
                }
                let entry = next.entry(after).or_insert(Enclosed::ZERO);
                *entry = entry.add(q.mul(chance));
            }
        }
        partial = next;
    }
    partial.into_iter().collect()
}

/// Every way `n` domains fall into outcomes of probabilities `p`, each domain on its own, with
/// the multinomial chance of each: `split[j]` domains have outcome `j`.
fn splits(n: usize, p: &[Enclosed]) -> Vec<(Vec<usize>, Enclosed)> {
    let mut out = Vec::new();
    let mut split = vec![0usize; p.len()];
    fill(n, p, 0, &mut split, Enclosed::exact(1.0), &mut out);
    out
}

/// Assigns the domains left, `left` of them, to outcomes from `j` on, each assignment's chance
/// the multinomial term built so far.
fn fill(
    left: usize,
    p: &[Enclosed],
    j: usize,
    split: &mut Vec<usize>,
    chance: Enclosed,
    out: &mut Vec<(Vec<usize>, Enclosed)>,
) {
    let Some(&pj) = p.get(j) else {
        return;
    };
    let Some(after) = j.checked_add(1) else {
        return;
    };
    if after == p.len() {
        // The last outcome takes every domain left: C(left, left)·pj^left.
        if let Some(s) = split.get_mut(j) {
            *s = left;
        }
        out.push((split.clone(), chance.mul(power(pj, left))));
        if let Some(s) = split.get_mut(j) {
            *s = 0;
        }
        return;
    }
    // C(left, m)·pj^m for m of the domains left taking outcome j.
    let mut choose = Enclosed::exact(1.0);
    for m in 0..=left {
        if let Some(s) = split.get_mut(j) {
            *s = m;
        }
        let (Some(rest), Some(next)) = (left.checked_sub(m), m.checked_add(1)) else {
            break;
        };
        fill(
            rest,
            p,
            after,
            split,
            chance.mul(choose).mul(power(pj, m)),
            out,
        );
        choose = choose
            .mul(Enclosed::exact(count(rest)))
            .div(Enclosed::exact(count(next)));
    }
    if let Some(s) = split.get_mut(j) {
        *s = 0;
    }
}

/// `x` to the power `n` by repeated multiplication, each rounding enclosed; `n` is at most a
/// stripe's width.
fn power(x: Enclosed, n: usize) -> Enclosed {
    (0..n).fold(Enclosed::exact(1.0), |acc, _| acc.mul(x))
}

/// P(h of `n` are struck) for h = 0..=n, each struck independently with probability `f`.
fn binomial(n: usize, f: f64) -> Vec<Enclosed> {
    let f = Enclosed::exact(f);
    let spared = f.complement();
    let mut out = Vec::new();
    let mut choose = Enclosed::exact(1.0);
    // h struck and `rest` spared, h counting up as `rest` counts down.
    for (h, rest) in (0..=n).zip((0..=n).rev()) {
        out.push(choose.mul(power(f, h)).mul(power(spared, rest)));
        // C(n, h+1) = C(n, h)·(n−h)/(h+1); h + 1 is exact, far below 2⁵³.
        choose = choose
            .mul(Enclosed::exact(count(rest)))
            .div(Enclosed::exact(count(h) + 1.0));
    }
    out
}

/// `n` as a float, exactly: a stripe's counts are far below 2³².
fn count(n: usize) -> f64 {
    u32::try_from(n).map_or(f64::from(u32::MAX), f64::from)
}

/// The mean time from state 0 to the last state, which absorbs, of the chain whose rates are
/// `q`: state 0's holding time, folded with the time spent in the states eliminated, over its
/// chance of leaving for loss rather than returning (Hunter (49)).
fn absorption_time(q: Vec<Vec<f64>>) -> f64 {
    match first_passage(q) {
        Some(p) if p.doomed > 0.0 => p.hold / p.doomed,
        _ => f64::INFINITY,
    }
}

/// State 0 of a chain reduced to state 0 and loss.
#[derive(Debug, Clone, Copy)]
struct Passage {
    /// State 0's mean holding time with the time spent in the eliminated states folded in:
    /// the mean time from entering state 0 to entering it again or reaching loss.
    hold: f64,
    /// The chance that a stay in state 0 ends in loss before the chain returns to state 0.
    doomed: f64,
}

/// Reduces the chain of `q` to state 0 and loss. The chain's jump chain holds each state's
/// exit probabilities and mean holding time; eliminating a state folds its paths and time into
/// the states that lead to it (Hunter, Special Matrices 2016, Theorem 2, (31)–(32)), and the
/// sum of a state's exits other than to itself stands for one minus its self-loop, so nothing
/// is subtracted. `None` when a state has no way out, so loss is never reached from it.
fn first_passage(q: Vec<Vec<f64>>) -> Option<Passage> {
    let states = q.len();
    let loss = states.checked_sub(1).filter(|&l| l > 0)?;
    let mut p = q;
    let mut hold = vec![0.0; loss];
    for (t, row) in p.iter_mut().enumerate().take(loss) {
        let total = row
            .iter()
            .enumerate()
            .filter(|&(u, _)| u != t)
            .fold(0.0, |sum, (_, r)| sum + r);
        if total <= 0.0 {
            return None;
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
        let row_m = p.get(m).cloned()?;
        let hold_m = hold.get(m).copied().unwrap_or(0.0);
        let leave = row_m
            .iter()
            .enumerate()
            .filter(|&(j, _)| j < m || j == loss)
            .fold(0.0, |sum, (_, r)| sum + r);
        if leave <= 0.0 {
            return None;
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
    Some(Passage {
        hold: hold.first().copied().unwrap_or(0.0),
        doomed: p
            .first()
            .and_then(|row| row.get(loss))
            .copied()
            .unwrap_or(0.0),
    })
}

/// Squarings are bounded by the exponent range: a finite double is below 2¹⁰²⁴, and each
/// squaring halves the step until Λτ ≤ ½.
const MAX_SQUARINGS: u32 = 1100;

/// Terms of the series for e^(Qτ) at Λτ ≤ ½: the tail after K terms is at most
/// 2·(½)^(K+1)/(K+1)!, which at K = 170 is below 10⁻³⁰⁰ times 10⁻⁵⁰ and so below every
/// positive double; 170! is the largest factorial a double holds.
const MAX_TERMS: u32 = 170;

/// Entrywise bounds of a square matrix of non-negative entries: `lo ≤ exact ≤ hi`.
#[derive(Debug, Clone)]
struct Band {
    lo: Vec<Vec<f64>>,
    hi: Vec<Vec<f64>>,
}

impl Band {
    fn identity(n: usize) -> Self {
        let unit: Vec<Vec<f64>> = (0..n)
            .map(|i| (0..n).map(|j| if i == j { 1.0 } else { 0.0 }).collect())
            .collect();
        Self {
            lo: unit.clone(),
            hi: unit,
        }
    }

    /// The product `self · other`, each entry's bounds rounded outward term by term.
    fn times(&self, other: &Self) -> Self {
        Self {
            lo: product(&self.lo, &other.lo, down),
            hi: product(&self.hi, &other.hi, f64::next_up),
        }
    }

    /// Row `loss` of a chain's transient is exactly the unit row: loss absorbs.
    fn absorb(&mut self, loss: usize) {
        for m in [&mut self.lo, &mut self.hi] {
            if let Some(row) = m.get_mut(loss) {
                for (j, x) in row.iter_mut().enumerate() {
                    *x = if j == loss { 1.0 } else { 0.0 };
                }
            }
        }
    }

    /// No entry of a stochastic matrix exceeds one.
    fn cap(&mut self) {
        for x in self.hi.iter_mut().flatten() {
            *x = x.min(1.0);
        }
    }
}

/// `a · b` for non-negative matrices, every positive product and partial sum passed through
/// `round`, which moves it one step outward.
fn product(a: &[Vec<f64>], b: &[Vec<f64>], round: fn(f64) -> f64) -> Vec<Vec<f64>> {
    a.iter()
        .map(|row| {
            let mut out = vec![0.0; row.len()];
            for (&x, b_row) in row.iter().zip(b) {
                if x == 0.0 {
                    continue;
                }
                for (c, &y) in out.iter_mut().zip(b_row) {
                    if y != 0.0 {
                        *c = round(*c + round(x * y));
                    }
                }
            }
            out
        })
        .collect()
}

/// Adds the non-negative matrix `b` into `a`, each positive sum passed through `round`.
fn accumulate(a: &mut [Vec<f64>], b: &[Vec<f64>], round: fn(f64) -> f64) {
    for (a_row, b_row) in a.iter_mut().zip(b) {
        for (x, &y) in a_row.iter_mut().zip(b_row) {
            if y != 0.0 {
                *x = round(*x + y);
            }
        }
    }
}

/// The enclosure of the whole state's entry for loss, the last state, in e^(Q·hours) for the
/// generator `q`. `None` when Λ·hours overflows a double.
fn transient_loss(q: &[Vec<Enclosed>], hours: f64) -> Option<LossBounds> {
    let n = q.len();
    let loss = n.checked_sub(1)?;
    let exits: Vec<Enclosed> = q
        .iter()
        .enumerate()
        .map(|(i, row)| {
            row.iter()
                .enumerate()
                .filter(|&(j, _)| j != i)
                .fold(Enclosed::ZERO, |sum, (_, &r)| sum.add(r))
        })
        .collect();
    // Λ is any rate at least every state's exit rate; the largest upper end is one.
    let lambda = exits.iter().fold(0.0, |most, e| f64::max(most, e.hi));
    if lambda <= 0.0 || hours == 0.0 {
        return Some(LossBounds {
            lower: 0.0,
            upper: 0.0,
        });
    }
    let lambda = Enclosed::exact(lambda);
    // P = I + Q/Λ: each off-diagonal rate over Λ, and the diagonal one less each exit over Λ.
    let mut step = Band::identity(n);
    for (i, row) in q.iter().enumerate().take(loss) {
        for (j, &rate) in row.iter().enumerate() {
            let entry = if i == j {
                exits.get(i).copied()?.div(lambda).complement()
            } else {
                rate.div(lambda)
            };
            if let (Some(lo), Some(hi)) = (
                step.lo.get_mut(i).and_then(|r| r.get_mut(j)),
                step.hi.get_mut(i).and_then(|r| r.get_mut(j)),
            ) {
                *lo = entry.lo;
                *hi = entry.hi;
            }
        }
    }
    // x = Λ·hours/2ˢ ≤ ½. Halving a normal double is exact, and x stays above ¼.
    let y = Enclosed::exact(hours).mul(lambda);
    if !y.hi.is_finite() {
        return None;
    }
    let (mut x_lo, mut x_hi, mut squarings) = (y.lo, y.hi, 0u32);
    while x_hi > 0.5 {
        if squarings >= MAX_SQUARINGS {
            return None;
        }
        x_lo *= 0.5;
        x_hi *= 0.5;
        squarings = squarings.checked_add(1)?;
    }
    // The series Σ xⁿ/n! Pⁿ, with its term Pⁿ·xⁿ/n! bounded below at x_lo and above at x_hi,
    // and the sum of its weights xⁿ/n!, which is e^x to within the tail.
    let mut term = Band::identity(n);
    let mut sum = Band::identity(n);
    let mut weight = (1.0, 1.0);
    let mut weights = (1.0, 1.0);
    // The tail's effect on the answer is at most its bound times the states times the
    // squarings' doubling; the series stops once that is below every positive normal double,
    // or at MAX_TERMS, past which the tail's bound cannot fall.
    let doubling = 2f64.powi(i32::try_from(squarings).ok()?);
    let negligible = f64::MIN_POSITIVE / (doubling * count(n));
    let mut tail = f64::INFINITY;
    for k in 1..=MAX_TERMS {
        let kf = f64::from(k);
        let (f_lo, f_hi) = (down(x_lo / kf), (x_hi / kf).next_up());
        term = term.times(&step);
        for x in term.lo.iter_mut().flatten() {
            *x = down(*x * f_lo);
        }
        for x in term.hi.iter_mut().flatten() {
            *x = up_product(*x, f_hi);
        }
        accumulate(&mut sum.lo, &term.lo, down);
        accumulate(&mut sum.hi, &term.hi, f64::next_up);
        weight = (down(weight.0 * f_lo), up_product(weight.1, f_hi));
        weights = (down(weights.0 + weight.0), up_sum(weights.1, weight.1));
        // Σ_{m>k} x^m/m! ≤ x^k/k! · x/(k+1) · 1/(1 − x/(k+2)) ≤ 2·x^k/k!·x/(k+1), x ≤ ½.
        tail = (2.0 * (weight.1 * x_hi).next_up() / (kf + 1.0)).next_up();
        if tail <= negligible {
            break;
        }
    }
    // e^(−x) lies between 1/(Σ weights at x_hi + tail) and 1/(Σ weights at x_lo).
    let scale_lo = down(1.0 / (weights.1 + tail).next_up());
    let scale_hi = (1.0 / weights.0).next_up();
    let mut e = sum;
    for x in e.lo.iter_mut().flatten() {
        *x = down(*x * scale_lo);
    }
    for row in e.hi.iter_mut().take(loss) {
        for x in row.iter_mut() {
            // Every omitted Pⁿ is stochastic, so each omitted entry is at most the tail.
            *x = (up_sum(*x, tail) * scale_hi).next_up();
        }
    }
    e.absorb(loss);
    e.cap();
    for _ in 0..squarings {
        e = e.times(&e);
        e.absorb(loss);
        e.cap();
    }
    let entry = |m: &Vec<Vec<f64>>| m.first().and_then(|row| row.get(loss)).copied();
    Some(LossBounds {
        lower: entry(&e.lo)?,
        upper: entry(&e.hi)?.min(1.0),
    })
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
            let fatal: f64 = binomial(n, fraction)
                .iter()
                .skip(spare + 1)
                .map(|p| p.est)
                .sum();
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

    /// Where loss is rare against repair, the time to loss is close to exponential (Keilson;
    /// docs/research/15 §4.5): three copies over a year, lost within it with probability near
    /// one in a thousand, repair within an hour. The law 1 − e^(−t/M) falls inside the
    /// enclosure to the thousandth Keilson's ratio gives, and the enclosure is inside the
    /// renewal bound t·q₀·p, which holds for every chain (docs/research/15 §4.6).
    #[test]
    fn where_loss_is_rare_the_exponential_law_is_within_the_enclosure() {
        let rates = independent(2.5e-3, 1.0);
        let scheme = Scheme::Copies(3);
        let bounds = loss_bounds(scheme, &rates, YEAR).unwrap();
        let mean = mean_time_to_loss(scheme, &rates).unwrap();
        let law = -(-YEAR / mean).exp_m1();
        assert!(bounds.lower > 1e-4 && bounds.upper < 1e-2, "{bounds:?}");
        assert!(bounds.upper / bounds.lower - 1.0 < 1e-8, "{bounds:?}");
        assert!(close(law, bounds.upper, 1e-3), "{law} against {bounds:?}");
        assert!(
            bounds.upper <= renewal_bound(scheme, &rates, YEAR),
            "{bounds:?}"
        );
    }

    /// The renewal bound: a stripe is lost only on a stay in the whole state that ends in loss
    /// before the stripe is whole again, stays begin at most q₀·t times in expectation within
    /// t, and each ends so with probability p, so P(T ≤ t) ≤ q₀·p·t (docs/research/15 §4.6).
    fn renewal_bound(scheme: Scheme, rates: &Rates, hours: f64) -> f64 {
        let q = estimates(&generator(scheme, rates).unwrap());
        let exit: f64 = q[0].iter().skip(1).sum();
        exit * first_passage(q).unwrap().doomed * hours
    }

    /// No stripe is lost more surely than the renewal bound says, whatever the repair.
    #[test]
    fn the_enclosure_is_within_the_renewal_bound() {
        for (scheme, repair) in [
            (Scheme::Copies(2), 1.0),
            (Scheme::Copies(3), 0.01),
            (rs(6, 3), 1.0),
            (rs(9, 6), 10.0),
            (rs(4, 2), 1e-3),
        ] {
            let rates = independent(0.04 / YEAR, repair);
            let bounds = loss_bounds(scheme, &rates, YEAR).unwrap();
            let renewal = renewal_bound(scheme, &rates, YEAR);
            assert!(
                bounds.lower <= renewal * (1.0 + 1e-12),
                "{scheme:?} at {repair}: {bounds:?} against {renewal}"
            );
        }
    }

    /// Without repair, the chunks of a stripe fail independently, each within t with
    /// probability p = 1 − e^(−λt), and the stripe is lost when more than its spare chunks
    /// have: the binomial tail, a sum of non-negative terms. The enclosure holds it, and is
    /// narrow, from a billionth of a mean lifetime to ten of them.
    #[test]
    fn without_repair_the_enclosure_holds_the_binomial_tail() {
        for scheme in [Scheme::Copies(1), Scheme::Copies(3), rs(6, 3), rs(9, 6)] {
            let (n, spare) = (scheme.width(), scheme.width() - scheme.needed());
            for x in [1e-9f64, 1e-4, 0.1, 1.0, 10.0] {
                let (p, q) = (-(-x).exp_m1(), (-x).exp());
                let exact: f64 = (spare + 1..=n)
                    .map(|k| {
                        let c: f64 = (0..k).map(|j| (n - j) as f64 / (j + 1) as f64).product();
                        c * p.powi(i32::try_from(k).unwrap())
                            * q.powi(i32::try_from(n - k).unwrap())
                    })
                    .sum();
                let got = loss_bounds(scheme, &independent(x, 0.0), 1.0).unwrap();
                assert!(
                    encloses(got, exact),
                    "{scheme:?} at λt = {x}: {got:?} against {exact}"
                );
            }
        }
    }

    /// Two copies failing at λ each and repaired at ρ: the whole state leaves at 2λ, the
    /// degraded one returns at ρ or is lost at λ. The survival function is
    /// (r₂e^(r₁t) − r₁e^(r₂t))/(r₂ − r₁) for r₁, r₂ the eigenvalues of the chain's transient
    /// part, the roots of r² + (3λ + ρ)r + 2λ² = 0. Rates are chosen so that one minus it keeps
    /// its digits.
    #[test]
    fn with_repair_the_enclosure_holds_the_two_state_closed_form() {
        for (lambda, rho, t) in [(0.1, 1.0, 5.0), (0.01, 2.0, 100.0), (1.0, 0.5, 1.0)] {
            let b: f64 = 3.0 * lambda + rho;
            let root = (b * b - 8.0 * lambda * lambda).sqrt();
            let (r1, r2) = ((-b + root) / 2.0, (-b - root) / 2.0);
            let survival = (r2 * (r1 * t).exp() - r1 * (r2 * t).exp()) / (r2 - r1);
            let exact = 1.0 - survival;
            let got = loss_bounds(Scheme::Copies(2), &independent(lambda, rho), t).unwrap();
            assert!(
                got.lower <= exact * (1.0 + 1e-12) && got.upper >= exact * (1.0 - 1e-12),
                "λ {lambda}, ρ {rho}, t {t}: {got:?} against {exact}"
            );
            assert!(got.upper / got.lower - 1.0 < 1e-9, "{got:?}");
        }
    }

    /// Whether `bounds` hold `exact`, a closed form evaluated in floating point to within a few
    /// roundings, and are within a billionth of each other.
    fn encloses(bounds: LossBounds, exact: f64) -> bool {
        let slack = 1e-13;
        bounds.lower <= exact * (1.0 + slack)
            && bounds.upper >= exact * (1.0 - slack)
            && bounds.upper <= bounds.lower * (1.0 + 1e-9)
    }

    /// The enclosure's width, from repair far slower than the year to far faster: each of the
    /// log₂(2Λt) squarings doubles the bounds' relative width and adds a few roundings, so the
    /// width grows in proportion to Λt, the repairs a year.
    #[test]
    fn the_enclosure_is_narrow_across_repair_rates() {
        for repair in [0.0, 1e-4, 1.0, 1e3, 1e6] {
            let rates = Rates {
                chunk: 0.04 / YEAR,
                bursts: vec![Burst {
                    rate: 1.0 / YEAR,
                    fraction: 0.01,
                }],
                repair,
                ..Rates::default()
            };
            for scheme in [Scheme::Copies(3), rs(6, 3), rs(10, 4)] {
                let got = loss_bounds(scheme, &rates, YEAR).unwrap();
                // Measured: 2×10⁻¹³ without repair, 1.6×10⁻⁹ at one hour, 1.5×10⁻⁶ at 3.6 s
                // and 8.4×10⁻⁴ at 3.6 ms, about Λt·10⁻¹³; this allows ten times that.
                let allowed = 1e-12 * (1.0 + repair * YEAR);
                assert!(
                    got.lower > 0.0 && got.upper / got.lower - 1.0 < allowed,
                    "{scheme:?} at {repair}: {got:?}"
                );
            }
        }
    }

    /// The audit's placement (B09): RS(6,3) over three zones, three chunks in each, zones lost
    /// at λ each, nothing else lost and nothing repaired. The first zone lost leaves two zones
    /// holding chunks, and the next of those two lost loses the stripe, so the time to loss is
    /// an exponential of rate 3λ and then one of 2λ: its mean is 1/(3λ) + 1/(2λ) = 5/(6λ), and
    /// its chance within t is 1 − 3e^(−2λt) + 2e^(−3λt). Spreading the six survivors evenly
    /// again, as the chain once did, gave 2/(3λ); and the exponential law with the exact mean
    /// is far off the exact chance, since nothing returns the stripe to whole.
    #[test]
    fn a_fixed_placements_domain_losses_are_modeled_exactly() {
        let lambda = 0.25;
        let rates = Rates {
            domains: Some(DomainLoss {
                domains: 3,
                rate: lambda,
            }),
            ..Rates::default()
        };
        let mean = mean_time_to_loss(rs(6, 3), &rates).unwrap();
        assert!(close(mean, 5.0 / (6.0 * lambda), 1e-12), "{mean}");
        for t in [1e-6, 0.01, 1.0, 10.0] {
            // 1 − 3a² + 2a³ = (1 − a)²(1 + 2a) for a = e^(−λt), with 1 − a kept exact.
            let (a, spent) = ((-lambda * t).exp(), -(-lambda * t).exp_m1());
            let exact = spent * spent * (1.0 + 2.0 * a);
            let got = loss_bounds(rs(6, 3), &rates, t).unwrap();
            assert!(encloses(got, exact), "t {t}: {got:?} against {exact}");
        }
        let law = -(-0.01 / mean).exp_m1();
        let got = loss_within(rs(6, 3), &rates, 0.01).unwrap();
        assert!(law > 10.0 * got, "{law} against {got}");
    }

    /// The chain over how many domains hold each count, against a chain over what each
    /// labelled domain holds, built by the same rules one domain at a time: a chunk failing,
    /// a domain lost, a burst striking each chunk on its own, and repair rebuilding a chunk
    /// in one of the domains holding the fewest, each of those equally. Grouping alike domains
    /// changes no mean time to loss.
    #[test]
    fn grouping_alike_domains_changes_no_mean_time_to_loss() {
        let rates = |domains| Rates {
            chunk: 1e-3,
            domains: Some(DomainLoss {
                domains,
                rate: 2e-3,
            }),
            bursts: vec![Burst {
                rate: 1e-3,
                fraction: 0.3,
            }],
            repair: 0.5,
        };
        for (scheme, domains) in [
            (rs(6, 3), 2),
            (rs(6, 3), 3),
            (rs(6, 3), 4),
            (Scheme::Copies(3), 2),
            (rs(4, 2), 4),
            (rs(2, 1), 2),
            (rs(3, 2), 7),
        ] {
            let r = rates(domains);
            let grouped = mean_time_to_loss(scheme, &r).unwrap();
            let labelled = absorption_time(labelled_generator(scheme, &r));
            assert!(
                close(grouped, labelled, 1e-10),
                "{scheme:?} over {domains}: {grouped} against {labelled}"
            );
        }
    }

    /// The chain over labelled domains: each state what every domain holds.
    fn labelled_generator(scheme: Scheme, rates: &Rates) -> Vec<Vec<f64>> {
        let (width, needed) = (scheme.width(), scheme.needed());
        let d = rates.domains.unwrap();
        let (n, lost) = (d.domains, d.rate);
        let mut whole = vec![width / n; n];
        for c in whole.iter_mut().take(width % n) {
            *c += 1;
        }
        let mut index = std::collections::HashMap::new();
        let mut states = vec![whole.clone()];
        index.insert(whole, 0usize);
        let mut moves: Vec<Vec<(Option<usize>, f64)>> = Vec::new();
        let mut at = 0;
        while at < states.len() {
            let s = states[at].clone();
            let mut next: Vec<(Vec<usize>, f64)> = Vec::new();
            for i in 0..n {
                if s[i] > 0 {
                    let mut t = s.clone();
                    t[i] -= 1;
                    next.push((t, s[i] as f64 * rates.chunk));
                    let mut t = s.clone();
                    t[i] = 0;
                    next.push((t, lost));
                }
            }
            for b in &rates.bursts {
                // Every combination of how many each domain loses.
                let mut outcomes = vec![(s.clone(), 1.0)];
                for i in 0..n {
                    let p = binomial(s[i], b.fraction);
                    let mut grown = Vec::new();
                    for (o, q) in &outcomes {
                        for (j, pj) in p.iter().enumerate() {
                            let mut t = o.clone();
                            t[i] -= j;
                            grown.push((t, q * pj.est));
                        }
                    }
                    outcomes = grown;
                }
                for (t, q) in outcomes {
                    if t != s {
                        next.push((t, b.rate * q));
                    }
                }
            }
            if s.iter().sum::<usize>() < width {
                let fewest = *s.iter().min().unwrap();
                let ties: Vec<usize> = (0..n).filter(|&i| s[i] == fewest).collect();
                for &i in &ties {
                    let mut t = s.clone();
                    t[i] += 1;
                    next.push((t, rates.repair / ties.len() as f64));
                }
            }
            let mut out = Vec::new();
            for (t, rate) in next {
                if t.iter().sum::<usize>() < needed {
                    out.push((None, rate));
                    continue;
                }
                let to = *index.entry(t.clone()).or_insert_with(|| {
                    states.push(t);
                    states.len() - 1
                });
                out.push((Some(to), rate));
            }
            moves.push(out);
            at += 1;
        }
        let loss = states.len();
        let mut q = vec![vec![0.0; loss + 1]; loss + 1];
        for (from, out) in moves.into_iter().enumerate() {
            for (to, rate) in out {
                let to = to.unwrap_or(loss);
                if to != from {
                    q[from][to] += rate;
                }
            }
        }
        q
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

    /// docs/design/durability.md §6, from the field inputs and one-hour repair: independent
    /// disk failures alone leave RS(6,3) within eleven nines over 12 domains, the yearly power
    /// loss leaves nothing within them, and over 15 domains RS(9,6) meets 10⁻⁹.
    #[test]
    fn the_design_tables_choices_hold() {
        let disks = independent(field::DISK_FAILURES / YEAR, 1.0);
        let outage = Rates {
            bursts: vec![Burst {
                rate: field::POWER_LOSSES / YEAR,
                fraction: field::POWER_LOSS_FRACTION,
            }],
            ..disks.clone()
        };
        let all = candidates();
        let pick = |rates: &Rates, domains, target| choose(&all, domains, rates, target).unwrap();
        assert!(matches!(
            pick(&disks, 12, 1e-11),
            Some(Choice::Meets { scheme, annual_loss }) if scheme == rs(6, 3) && annual_loss < 1e-13
        ));
        assert!(matches!(
            pick(&outage, 12, 1e-11),
            Some(Choice::Short { scheme, annual_loss })
                if scheme == rs(8, 4) && close(annual_loss, 7.5e-8, 0.01)
        ));
        assert!(matches!(
            pick(&outage, 15, 1e-9),
            Some(Choice::Meets { scheme, annual_loss })
                if scheme == rs(9, 6) && close(annual_loss, 6.1e-11, 0.01)
        ));
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
        // Wider than every scheme mantle stores.
        assert!(matches!(
            loss_within(Scheme::Copies(16), &independent(1.0, 1.0), 1.0),
            Err(DurabilityError::Scheme { .. })
        ));
        assert_eq!(
            loss_within(Scheme::Copies(3), &independent(1.0, 1.0), f64::NAN),
            Err(DurabilityError::Rate)
        );
        // Nothing fails: never lost.
        assert_eq!(
            mean_time_to_loss(Scheme::Copies(3), &independent(0.0, 1.0)),
            Ok(f64::INFINITY)
        );
    }
}
