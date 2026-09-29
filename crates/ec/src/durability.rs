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

/// The relative precision of a loss probability: the report prints two significant figures,
/// and a thousandth keeps the third right.
const PRECISION: f64 = 1e-3;

/// The mean time, in hours, until a whole stripe of `scheme` loses more chunks than it can
/// rebuild from; infinite when nothing can lose it.
pub fn mean_time_to_loss(scheme: Scheme, rates: &Rates) -> Result<f64, DurabilityError> {
    let q = generator(scheme, rates)?;
    Ok(absorption_time(q))
}

/// The probability that a whole stripe of `scheme` is lost within `hours`.
///
/// Where repair returns a degraded stripe to whole many times before it is lost, the time to
/// loss is close to exponential, which a rarely reached set's first passage is, to within the
/// ratio of an excursion's length to the mean passage time (Keilson, *Markov Chain Models:
/// Rarity and Exponentiality*, 1979; docs/research/15 §4.5): the law is taken when that ratio,
/// the spare chunks' repair time over the mean time to loss, is within [`PRECISION`]. Where
/// it is not, as without repair, the chance is computed exactly by uniformization, whose terms
/// are all non-negative (docs/research/15 §4.4). A mean time to loss within `PRECISION` of
/// `hours` makes loss all but certain: past it the stripe survives with probability at most
/// their ratio, by Markov's inequality.
pub fn loss_within(scheme: Scheme, rates: &Rates, hours: f64) -> Result<f64, DurabilityError> {
    if !(hours.is_finite() && hours >= 0.0) {
        return Err(DurabilityError::Rate);
    }
    let q = generator(scheme, rates)?;
    let mean = absorption_time(q.clone());
    if !mean.is_finite() {
        return Ok(0.0);
    }
    if mean <= PRECISION * hours {
        return Ok(1.0);
    }
    // A stripe with no spare chunk is lost at its first loss, an exponential time exactly.
    let spare = count(scheme.width().saturating_sub(scheme.needed()));
    let excursion = if spare == 0.0 {
        0.0
    } else {
        spare / rates.repair
    };
    if excursion <= PRECISION * mean {
        return Ok(-(-hours / mean).exp_m1());
    }
    Ok(uniformized_loss(&q, hours))
}

/// The probability that the chain of `q`, started whole, reaches loss, its last state, within
/// `hours`: Σₙ Poisson(n; Λt)·P(loss within n steps of the chain I + Q/Λ), for Λ the largest
/// rate out of any state. Every term is non-negative, so no digit is lost to subtraction. The
/// sum stops once what the rest of the Poisson weights could add is within [`PRECISION`] of
/// it, or below any number the arithmetic holds; the weights past twice their mean fall at
/// least geometrically, so it stops.
fn uniformized_loss(q: &[Vec<f64>], hours: f64) -> f64 {
    let n = q.len();
    let Some(loss) = n.checked_sub(1) else {
        return 0.0;
    };
    let exit: Vec<f64> = q
        .iter()
        .enumerate()
        .map(|(i, row)| {
            row.iter()
                .enumerate()
                .filter(|&(j, _)| j != i)
                .map(|(_, r)| r)
                .sum()
        })
        .collect();
    let lambda = exit.iter().copied().fold(0.0, f64::max);
    if lambda <= 0.0 {
        return 0.0;
    }
    let mean = lambda * hours;
    let mut v = vec![0.0; n];
    if let Some(first) = v.first_mut() {
        *first = 1.0;
    }
    let mut log_weight = -mean;
    let mut sum = 0.0;
    let mut step = 0u64;
    loop {
        sum += log_weight.exp() * v.get(loss).copied().unwrap_or(0.0);
        step = step.saturating_add(1);
        let k = step as f64;
        log_weight += mean.ln() - k.ln();
        // Past twice the mean each weight is at most half the one before, so the rest sum to
        // at most twice the next.
        if k > 2.0 * mean {
            let rest = 2.0 * log_weight.exp();
            if rest <= PRECISION * sum || rest < f64::MIN_POSITIVE {
                return sum;
            }
        }
        let mut next = vec![0.0; n];
        for (i, &p) in v.iter().enumerate() {
            if p == 0.0 {
                continue;
            }
            if i == loss {
                if let Some(x) = next.get_mut(i) {
                    *x += p;
                }
                continue;
            }
            let row = q.get(i).map(Vec::as_slice).unwrap_or(&[]);
            let out = exit.get(i).copied().unwrap_or(0.0);
            if let Some(x) = next.get_mut(i) {
                *x += p * ((lambda - out) / lambda).max(0.0);
            }
            for (j, &r) in row.iter().enumerate() {
                if j != i
                    && r > 0.0
                    && let Some(x) = next.get_mut(j)
                {
                    *x += p * r / lambda;
                }
            }
        }
        v = next;
    }
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

/// How many failure domains hold each count of a stripe's chunks: `holding[k]` hold `k`.
type Occupancy = Vec<usize>;

/// The chain's rates, `q[s][u]` from state `s` to `u`: the states the stripe can be in while
/// it survives, reached from the chunks placed as evenly as the domains allow, which is state
/// 0, and loss, the last state.
fn generator(scheme: Scheme, rates: &Rates) -> Result<Vec<Vec<f64>>, DurabilityError> {
    let (width, needed) = (scheme.width(), scheme.needed());
    if width < needed || needed == 0 {
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
    let most = width.div_ceil(domains);
    let mut whole: Occupancy = vec![0; most.saturating_add(1)];
    let full = width.checked_rem(domains).unwrap_or(0);
    let each = width.checked_div(domains).unwrap_or(0);
    if full > 0 {
        if let Some(h) = whole.get_mut(most) {
            *h = full;
        }
        if let Some(h) = whole.get_mut(each) {
            *h = domains.saturating_sub(full);
        }
    } else if let Some(h) = whole.get_mut(each) {
        *h = domains;
    }
    // The states reachable from whole, found breadth first, with each one's moves.
    let mut index: std::collections::HashMap<Occupancy, usize> = std::collections::HashMap::new();
    let mut states: Vec<Occupancy> = vec![whole.clone()];
    index.insert(whole, 0);
    let mut moves: Vec<Vec<(Option<usize>, f64)>> = Vec::new();
    let mut at = 0;
    while let Some(state) = states.get(at).cloned() {
        let mut out = Vec::new();
        for (next, rate) in transitions(&state, width, rates) {
            if rate <= 0.0 {
                continue;
            }
            let held: usize = next
                .iter()
                .enumerate()
                .map(|(k, &h)| k.saturating_mul(h))
                .sum();
            if held < needed {
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
        at = at.saturating_add(1);
    }
    let loss = states.len();
    let mut q = vec![vec![0.0; loss.saturating_add(1)]; loss.saturating_add(1)];
    for (from, out) in moves.into_iter().enumerate() {
        for (to, rate) in out {
            let to = to.unwrap_or(loss);
            if to != from
                && let Some(cell) = q.get_mut(from).and_then(|row| row.get_mut(to))
            {
                *cell += rate;
            }
        }
    }
    Ok(q)
}

/// Where `state` goes next, and at what rate: a chunk failing, a domain lost, a burst
/// striking, or a chunk rebuilt in a domain holding the fewest, one at a time.
fn transitions(state: &Occupancy, width: usize, rates: &Rates) -> Vec<(Occupancy, f64)> {
    let mut out = Vec::new();
    let moved = |from: usize, to: usize| {
        let mut next = state.clone();
        if let Some(h) = next.get_mut(from) {
            *h = h.saturating_sub(1);
        }
        if let Some(h) = next.get_mut(to) {
            *h = h.saturating_add(1);
        }
        next
    };
    for (k, &domains) in state.iter().enumerate().skip(1) {
        if domains == 0 {
            continue;
        }
        let holding = count(domains);
        out.push((
            moved(k, k.saturating_sub(1)),
            holding * count(k) * rates.chunk,
        ));
        if let Some(d) = rates.domains {
            out.push((moved(k, 0), holding * d.rate));
        }
    }
    for burst in &rates.bursts {
        for (next, p) in struck(state, burst.fraction) {
            if &next != state {
                out.push((next, burst.rate * p));
            }
        }
    }
    let held: usize = state
        .iter()
        .enumerate()
        .map(|(k, &h)| k.saturating_mul(h))
        .sum();
    if held < width
        && let Some(fewest) = state.iter().position(|&h| h > 0)
    {
        out.push((moved(fewest, fewest.saturating_add(1)), rates.repair));
    }
    out
}

/// What a burst that destroys each chunk with probability `f` leaves of `state`, with the
/// chance of each: a domain holding `k` chunks keeps `k − j` of them with the binomial chance
/// of `j` struck, each domain on its own.
fn struck(state: &Occupancy, f: f64) -> Vec<(Occupancy, f64)> {
    let mut partial: std::collections::HashMap<Occupancy, f64> = std::collections::HashMap::new();
    partial.insert(vec![0; state.len()], 1.0);
    for (k, &domains) in state.iter().enumerate() {
        if domains == 0 {
            continue;
        }
        // The domains holding `k`, each losing `j` with probability p[j]: how many lose each.
        let p = binomial(k, f);
        let mut next: std::collections::HashMap<Occupancy, f64> = std::collections::HashMap::new();
        for (split, chance) in splits(domains, &p) {
            for (base, q) in &partial {
                let mut after = base.clone();
                for (j, &n) in split.iter().enumerate() {
                    if let Some(h) = after.get_mut(k.saturating_sub(j)) {
                        *h = h.saturating_add(n);
                    }
                }
                *next.entry(after).or_insert(0.0) += q * chance;
            }
        }
        partial = next;
    }
    partial.into_iter().collect()
}

/// Every way `n` domains fall into outcomes of probabilities `p`, each domain on its own, with
/// the multinomial chance of each: `split[j]` domains have outcome `j`.
fn splits(n: usize, p: &[f64]) -> Vec<(Vec<usize>, f64)> {
    let mut out = Vec::new();
    let mut split = vec![0usize; p.len()];
    fill(n, p, 0, &mut split, 1.0, &mut out);
    out
}

/// Assigns the domains left, `left` of them, to outcomes from `j` on, each assignment's chance
/// the multinomial term built so far.
fn fill(
    left: usize,
    p: &[f64],
    j: usize,
    split: &mut Vec<usize>,
    chance: f64,
    out: &mut Vec<(Vec<usize>, f64)>,
) {
    let Some(&pj) = p.get(j) else {
        return;
    };
    if j.saturating_add(1) == p.len() {
        // The last outcome takes every domain left: C(left, left)·pj^left.
        if let Some(s) = split.get_mut(j) {
            *s = left;
        }
        let exponent = i32::try_from(left).unwrap_or(i32::MAX);
        out.push((split.clone(), chance * pj.powi(exponent)));
        if let Some(s) = split.get_mut(j) {
            *s = 0;
        }
        return;
    }
    // C(left, m)·pj^m for m of the domains left taking outcome j.
    let mut choose = 1.0;
    for m in 0..=left {
        let exponent = i32::try_from(m).unwrap_or(i32::MAX);
        if let Some(s) = split.get_mut(j) {
            *s = m;
        }
        fill(
            left.saturating_sub(m),
            p,
            j.saturating_add(1),
            split,
            chance * choose * pj.powi(exponent),
            out,
        );
        choose = choose * count(left.saturating_sub(m)) / count(m.saturating_add(1));
    }
    if let Some(s) = split.get_mut(j) {
        *s = 0;
    }
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

    /// Where loss is rare against repair, the exponential law from the mean agrees with the
    /// exact transient, by uniformization, to the precision kept: three copies over a year, at
    /// rates where the stripe is lost within the year with probability near one in a thousand
    /// and repair takes an hour.
    #[test]
    fn where_loss_is_rare_the_exponential_law_is_the_exact_transient() {
        let rates = independent(2.5e-3, 1.0);
        let scheme = Scheme::Copies(3);
        let exact = uniformized_loss(&generator(scheme, &rates).unwrap(), YEAR);
        let law = loss_within(scheme, &rates, YEAR).unwrap();
        let mean = mean_time_to_loss(scheme, &rates).unwrap();
        assert!(
            close(law, -(-YEAR / mean).exp_m1(), 1e-15),
            "the law was taken"
        );
        assert!(exact > 1e-4 && exact < 1e-2, "{exact}");
        assert!(close(law, exact, PRECISION), "{law} against {exact}");
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
        for t in [0.01, 1.0, 10.0] {
            let x = lambda * t;
            let exact = 1.0 - 3.0 * (-2.0 * x).exp() + 2.0 * (-3.0 * x).exp();
            let got = loss_within(rs(6, 3), &rates, t).unwrap();
            assert!(close(got, exact, PRECISION), "t {t}: {got} against {exact}");
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
                            grown.push((t, q * pj));
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
