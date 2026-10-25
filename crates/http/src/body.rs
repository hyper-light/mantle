//! A request's body as it streams (RFC 9112 §6.3, §7.1): Content-Length's octets counted off, or
//! the chunked coding decoded, its body's octets given back as spans of the caller's input so
//! nothing is copied. A chunk's size is parsed with its overflow checked; its extensions are
//! passed unread, their total held to [`MAX_HEAD`] per request, the same bound as the rest of the
//! message (RFC 9112 §7.1.1); the trailer section is held to the same bound and kept apart from
//! the head (§7.1.2).

use std::ops::Range;

use crate::head::field;
use crate::{Framing, MAX_HEAD, Refusal, field_octet};

/// What one call to [`Body::decode`] made of its input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    /// The input's octets taken: the next call is given the input past them.
    pub consumed: usize,
    /// The body's octets among those taken, a span of the input; empty when the octets taken
    /// were the coding's own.
    pub data: Range<usize>,
    /// The body ended with the octets taken: what follows in the input is the next request's.
    pub end: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Content-Length's octets left.
    Length(u64),
    /// A chunk's size line, its size so far and whether a digit has come.
    Size {
        size: u64,
        digit: bool,
    },
    /// Past a chunk's size, its extensions up to the line's end.
    Extensions(u64),
    /// A CR ended a size line: its LF next.
    SizeLf(u64),
    /// A chunk's data, its octets left.
    Data(u64),
    /// A chunk's data ended: the CRLF or LF after it next.
    DataEnd,
    /// The CR after a chunk's data: its LF next.
    DataLf,
    /// The trailer section: where in the trailers its current line begins, and whether the last
    /// octet was a CR.
    Trailer {
        line: usize,
        cr: bool,
    },
    Done,
}

/// A request's body being read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Body {
    state: State,
    /// The octets of chunk extensions taken.
    extensions: usize,
    /// The trailer section's field lines, each ended by an LF.
    trailers: Vec<u8>,
}

impl Body {
    /// The body a head's framing gives.
    pub fn new(framing: Framing) -> Self {
        let state = match framing {
            Framing::None | Framing::Length(0) => State::Done,
            Framing::Length(n) => State::Length(n),
            Framing::Chunked => State::Size {
                size: 0,
                digit: false,
            },
        };
        Self {
            state,
            extensions: 0,
            trailers: Vec::new(),
        }
    }

    /// Whether the body has ended.
    pub fn done(&self) -> bool {
        self.state == State::Done
    }

    /// The trailer section's fields, each its name and its value bare of whitespace, once a
    /// chunked body has ended.
    pub fn trailers(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.trailers
            .split(|b| *b == b'\n')
            .filter(|line| !line.is_empty())
            .filter_map(|line| {
                let (colon, first, last) = field(line).ok()?;
                Some((line.get(..colon)?, line.get(first..last)?))
            })
    }

    /// Takes as much of `input` as the body's next step does: the coding's own octets up to the
    /// next of the body's, and then as many of the body's as `input` holds, or up to the body's
    /// end.
    pub fn decode(&mut self, input: &[u8]) -> Result<Step, Refusal> {
        let mut at = 0usize;
        loop {
            let taken = |n: usize| {
                at.checked_add(n)
                    .ok_or(Refusal::Chunked("an input past usize"))
            };
            match self.state {
                State::Done => {
                    return Ok(Step {
                        consumed: at,
                        data: at..at,
                        end: true,
                    });
                }
                State::Length(left) | State::Data(left) => {
                    let ready = input.len().saturating_sub(at);
                    let n = usize::try_from(left).map_or(ready, |l| l.min(ready));
                    let rest = left
                        .checked_sub(u64::try_from(n).unwrap_or(u64::MAX))
                        .ok_or(Refusal::Chunked("a chunk past its size"))?;
                    let length = matches!(self.state, State::Length(_));
                    self.state = match (length, rest) {
                        (true, 0) => State::Done,
                        (true, rest) => State::Length(rest),
                        (false, 0) => State::DataEnd,
                        (false, rest) => State::Data(rest),
                    };
                    let end = taken(n)?;
                    return Ok(Step {
                        consumed: end,
                        data: at..end,
                        end: self.state == State::Done,
                    });
                }
                _ => {
                    let Some(&b) = input.get(at) else {
                        return Ok(Step {
                            consumed: at,
                            data: at..at,
                            end: false,
                        });
                    };
                    at = taken(1)?;
                    self.take(b)?;
                }
            }
        }
    }

    /// One octet of the chunked coding's own: a size line, the line end after a chunk's data, or
    /// the trailer section.
    fn take(&mut self, b: u8) -> Result<(), Refusal> {
        self.state = match self.state {
            State::Size { size, digit } => match hex(b) {
                Some(d) => State::Size {
                    size: size
                        .checked_mul(16)
                        .and_then(|s| s.checked_add(u64::from(d)))
                        .ok_or(Refusal::Chunked("a chunk's size past 64 bits"))?,
                    digit: true,
                },
                None if !digit => return Err(Refusal::Chunked("a chunk size without a digit")),
                None => match b {
                    b'\r' => State::SizeLf(size),
                    b'\n' => sized(size),
                    b';' | b' ' | b'\t' => {
                        self.extension()?;
                        State::Extensions(size)
                    }
                    _ => return Err(Refusal::Chunked("a chunk size line")),
                },
            },
            State::Extensions(size) => match b {
                b'\r' => State::SizeLf(size),
                b'\n' => sized(size),
                _ if field_octet(b) => {
                    self.extension()?;
                    State::Extensions(size)
                }
                _ => return Err(Refusal::Chunked("a control octet in a chunk extension")),
            },
            State::SizeLf(size) if b == b'\n' => sized(size),
            State::SizeLf(_) => return Err(Refusal::Chunked("a CR not before LF")),
            State::DataEnd => match b {
                b'\r' => State::DataLf,
                b'\n' => State::Size {
                    size: 0,
                    digit: false,
                },
                _ => return Err(Refusal::Chunked("chunk data past its size")),
            },
            State::DataLf if b == b'\n' => State::Size {
                size: 0,
                digit: false,
            },
            State::DataLf => return Err(Refusal::Chunked("a CR not before LF")),
            State::Trailer { line, cr } => return self.trailer(b, line, cr),
            State::Length(_) | State::Data(_) | State::Done => return Ok(()),
        };
        Ok(())
    }

    /// Counts an octet of chunk extensions against the request's bound.
    fn extension(&mut self) -> Result<(), Refusal> {
        self.extensions = self.extensions.saturating_add(1);
        if self.extensions > MAX_HEAD {
            return Err(Refusal::Chunked("chunk extensions past the head's bound"));
        }
        Ok(())
    }

    /// One octet of the trailer section: a field line's, or the empty line that ends the body.
    fn trailer(&mut self, b: u8, line: usize, cr: bool) -> Result<(), Refusal> {
        if cr && b != b'\n' {
            return Err(Refusal::Chunked("a CR not before LF"));
        }
        match b {
            b'\r' => {
                self.state = State::Trailer { line, cr: true };
            }
            b'\n' => {
                let current = self.trailers.get(line..).unwrap_or_default();
                if current.is_empty() {
                    self.state = State::Done;
                } else {
                    field(current).map_err(Refusal::Chunked)?;
                    self.push(b'\n')?;
                    self.state = State::Trailer {
                        line: self.trailers.len(),
                        cr: false,
                    };
                }
            }
            _ => {
                self.push(b)?;
                self.state = State::Trailer { line, cr: false };
            }
        }
        Ok(())
    }

    fn push(&mut self, b: u8) -> Result<(), Refusal> {
        if self.trailers.len() >= MAX_HEAD {
            return Err(Refusal::Chunked("a trailer section past the head's bound"));
        }
        self.trailers.push(b);
        Ok(())
    }
}

/// What a size line's end leads to: a chunk's data, or after the last chunk, the trailer section.
fn sized(size: u64) -> State {
    if size == 0 {
        State::Trailer { line: 0, cr: false }
    } else {
        State::Data(size)
    }
}

fn hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b.wrapping_sub(b'0')),
        b'a'..=b'f' => Some(b.wrapping_sub(b'a').wrapping_add(10)),
        b'A'..=b'F' => Some(b.wrapping_sub(b'A').wrapping_add(10)),
        _ => None,
    }
}
