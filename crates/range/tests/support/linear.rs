//! A linearizability checker (docs/research/06 §A6.8): Wing and Gong's search over the orders
//! a history allows, with Lowe's memo of every configuration already tried (the linearized
//! set and the model's state), one partition at a time, which is Horn and Kroening's WGL.
//! Herlihy and Wing's locality makes a history linearizable exactly when each key's is.

use std::collections::HashSet;
use std::hash::Hash;

/// A sequential specification: an immutable state and what each operation does to it.
pub trait Model {
    type State: Clone + Eq + Hash;
    type Input;
    type Output;
    fn init(&self) -> Self::State;
    /// The state after `input` answered with `output`, if that answer is legal in `state`;
    /// `output` is `None` for an operation whose answer nobody saw.
    fn step(
        &self,
        state: &Self::State,
        input: &Self::Input,
        output: Option<&Self::Output>,
    ) -> Option<Self::State>;
}

/// One operation as a client saw it.
#[derive(Debug, Clone)]
pub struct Operation<I, O> {
    pub call: u64,
    /// `None` for an operation that never returned: it may have taken effect at any point
    /// after its call, or never (Herlihy and Wing's extension of a history).
    pub ret: Option<u64>,
    pub input: I,
    pub output: Option<O>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Linearizable,
    /// No order fits; `longest` operations were the most ever linearized at once.
    NotLinearizable {
        longest: usize,
    },
    /// The search passed its budget of steps.
    Unknown,
}

/// Whether `ops`, one partition's history, is linearizable with respect to `model`, taking
/// at most `budget` steps.
pub fn check<M: Model>(model: &M, ops: &[Operation<M::Input, M::Output>], budget: u64) -> Verdict {
    let n = ops.len();
    // Events sorted by time, calls before returns at a tie, since intervals are closed.
    let mut events: Vec<(u64, bool, usize)> = Vec::with_capacity(2 * n);
    for (i, op) in ops.iter().enumerate() {
        events.push((op.call, false, i));
        events.push((op.ret.unwrap_or(u64::MAX), true, i));
    }
    events.sort_unstable();
    let len = events.len();
    // A doubly linked list over the events, with `len` as the head and the end.
    let head = len;
    let mut next: Vec<usize> = (1..=len).chain(std::iter::once(0)).collect();
    let mut prev: Vec<usize> = std::iter::once(head).chain(0..len).collect();
    next[head] = if len == 0 { head } else { 0 };
    if len > 0 {
        next[len - 1] = head;
        prev[head] = len - 1;
    }
    let mut call_at = vec![0; n];
    let mut ret_at = vec![0; n];
    for (pos, &(_, is_ret, op)) in events.iter().enumerate() {
        if is_ret {
            ret_at[op] = pos;
        } else {
            call_at[op] = pos;
        }
    }
    let unlink = |next: &mut Vec<usize>, prev: &mut Vec<usize>, pos: usize| {
        let (p, q) = (prev[pos], next[pos]);
        next[p] = q;
        prev[q] = p;
    };
    let relink = |next: &mut Vec<usize>, prev: &mut Vec<usize>, pos: usize| {
        let (p, q) = (prev[pos], next[pos]);
        next[p] = pos;
        prev[q] = pos;
    };

    let words = n.div_ceil(64);
    let mut linearized = vec![0u64; words];
    let mut cache: HashSet<(Vec<u64>, M::State)> = HashSet::new();
    let mut stack: Vec<(usize, M::State)> = Vec::new();
    let mut state = model.init();
    let mut entry = next[head];
    let mut longest = 0;
    let mut steps = 0u64;
    while next[head] != head {
        steps += 1;
        if steps > budget {
            return Verdict::Unknown;
        }
        let (_, is_ret, op) = events[entry];
        if !is_ret {
            if let Some(after) = model.step(&state, &ops[op].input, ops[op].output.as_ref()) {
                let mut set = linearized.clone();
                set[op / 64] |= 1 << (op % 64);
                if cache.insert((set.clone(), after.clone())) {
                    stack.push((op, std::mem::replace(&mut state, after)));
                    linearized = set;
                    longest = longest.max(stack.len());
                    // Lift the operation: its call and its return leave the list.
                    unlink(&mut next, &mut prev, call_at[op]);
                    unlink(&mut next, &mut prev, ret_at[op]);
                    entry = next[head];
                    continue;
                }
            }
            entry = next[entry];
        } else {
            // A return reached: some operation called before it must be undone.
            let Some((undone, before)) = stack.pop() else {
                return Verdict::NotLinearizable { longest };
            };
            state = before;
            linearized[undone / 64] &= !(1 << (undone % 64));
            relink(&mut next, &mut prev, ret_at[undone]);
            relink(&mut next, &mut prev, call_at[undone]);
            entry = next[call_at[undone]];
        }
    }
    Verdict::Linearizable
}

/// A register whose writes are unique, as an object key's current version is: a put makes
/// its value current, and a get sees the current value.
pub struct Register;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Input {
    Put(String),
    Get,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Output {
    Put,
    Got(Option<String>),
}

impl Model for Register {
    type State = Option<String>;
    type Input = Input;
    type Output = Output;

    fn init(&self) -> Self::State {
        None
    }

    fn step(
        &self,
        state: &Self::State,
        input: &Input,
        output: Option<&Output>,
    ) -> Option<Self::State> {
        match (input, output) {
            (Input::Put(value), _) => Some(Some(value.clone())),
            (Input::Get, Some(Output::Got(seen))) if seen == state => Some(state.clone()),
            (Input::Get, None) => Some(state.clone()),
            (Input::Get, _) => None,
        }
    }
}
