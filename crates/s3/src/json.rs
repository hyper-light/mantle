//! JSON as RFC 8259 defines it (docs/research/17 §1), for the documents S3 takes as JSON: a
//! reader that accepts the texts the grammar describes, in UTF-8, and refuses what I-JSON
//! (RFC 7493 §2) forbids a receiver to trust: an object naming one member twice, which IAM's
//! policy grammar forbids too, and a string holding an unpaired surrogate. Numbers are kept as
//! written, so no range or precision is lost before a caller reads one as it needs. Nesting is
//! bounded, so a deep text is refused; the caller bounds the text's size.
//!
//! A value is flat (CLAUDE.md §10): its tokens in the order the text writes them, each array's
//! and object's opening token counting the tokens before its close. The reader makes them in
//! one pass over the text, the writer writes them first to last, a lookup steps over a member's
//! whole value at once, and comparing, copying and dropping a value is a loop over one `Vec`.
//! None of them walks down the nesting with a container's work waiting on what it holds.

/// The deepest nesting read: one level past the deepest document read. A bucket policy nests
/// six levels, a statement's condition values inside its operator inside its `Condition`
/// inside the statement inside `Statement` inside the policy (17 §3.1), and a POST policy
/// three, a condition's operands inside the condition inside `conditions` (19 §4.2). The
/// seventh level lets a policy's checks find an object or array where a value belongs and
/// refuse it with S3's message for it; a deeper one is refused as JSON too deep, as RFC 8259 §9
/// lets a reader limit "the maximum depth of nesting". The reader's record of the containers
/// open around it holds this many and no more.
pub const MAX_DEPTH: usize = 7;

/// A JSON value: its tokens in the order its text writes them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Value {
    tokens: Vec<Token>,
}

/// A value inside a document, as a reader takes it: a scalar, or an array or object whose items
/// or members it reads in turn. It borrows the document's tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum View<'a> {
    Null,
    Bool(bool),
    /// A number as written, which the grammar checked.
    Number(&'a str),
    String(&'a str),
    Array(Items<'a>),
    /// Members in the order written, each name once.
    Object(Members<'a>),
}

/// An array's items, in order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Items<'a> {
    /// The tokens between the array's `[` and its `]`.
    tokens: &'a [Token],
}

/// An object's members, in the order written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Members<'a> {
    /// The tokens between the object's `{` and its `}`: each member's name, then its value's.
    tokens: &'a [Token],
}

/// One token of a value.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Null,
    Bool(bool),
    /// A number as written.
    Number(String),
    String(String),
    /// An object member's name; its value's tokens follow it.
    Name(String),
    /// `[` or `{`, and the count of tokens between it and its close, so a reader steps over
    /// the whole container at once. The count is relative, so a value's tokens are the same
    /// wherever it sits.
    Open(Kind, usize),
    /// `]` or `}`.
    Close(Kind),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Array,
    Object,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum JsonError {
    #[error("the text is not UTF-8")]
    Utf8,
    #[error("the text is not JSON: {0}")]
    Syntax(&'static str),
    #[error("an object names a member twice")]
    Duplicate,
    #[error("the text nests deeper than {MAX_DEPTH} levels")]
    Depth,
    #[error("a string holds an unpaired UTF-16 surrogate")]
    Surrogate,
}

/// Reads one JSON text.
pub fn parse(text: &[u8]) -> Result<Value, JsonError> {
    let text = std::str::from_utf8(text).map_err(|_| JsonError::Utf8)?;
    let mut reader = Reader {
        text,
        at: 0,
        tokens: Vec::new(),
        open: Open {
            containers: [(Kind::Array, 0); MAX_DEPTH],
            depth: 0,
        },
    };
    reader.document()?;
    Ok(Value {
        tokens: reader.tokens,
    })
}

struct Reader<'a> {
    text: &'a str,
    /// The byte offset read to, always at a character boundary.
    at: usize,
    /// The tokens read, in the text's order.
    tokens: Vec<Token>,
    open: Open,
}

/// The containers open around the reader, outermost first: each one's kind, and the index of
/// its opening token, which counts the tokens inside once it closes. A text nests at most
/// [`MAX_DEPTH`] levels, so the record holds that many and no more.
struct Open {
    containers: [(Kind, usize); MAX_DEPTH],
    /// How many are open.
    depth: usize,
}

impl Open {
    /// Opens a container one level deeper, within the bound.
    fn push(&mut self, kind: Kind, at: usize) -> Result<(), JsonError> {
        let slot = self
            .containers
            .get_mut(self.depth)
            .ok_or(JsonError::Depth)?;
        *slot = (kind, at);
        self.depth = self.depth.checked_add(1).ok_or(JsonError::Depth)?;
        Ok(())
    }

    /// The innermost open container.
    fn innermost(&self) -> Option<(Kind, usize)> {
        self.containers.get(self.depth.checked_sub(1)?).copied()
    }

    /// Closes the innermost open container.
    fn pop(&mut self) {
        if let Some(depth) = self.depth.checked_sub(1) {
            self.depth = depth;
        }
    }
}

impl Reader<'_> {
    fn peek(&self) -> Option<u8> {
        self.text.as_bytes().get(self.at).copied()
    }

    fn advance(&mut self, bytes: usize) -> Result<(), JsonError> {
        self.at = self
            .at
            .checked_add(bytes)
            .filter(|at| *at <= self.text.len())
            .ok_or(JsonError::Syntax("an unfinished text"))?;
        Ok(())
    }

    /// `ws = *( %x20 / %x09 / %x0A / %x0D )` (RFC 8259 §2).
    fn space(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.at = self.at.saturating_add(1);
        }
    }

    /// `JSON-text = ws value ws` (RFC 8259 §2), read in one pass: a value begins, and once one
    /// ends, the closes after it close their containers, then the text ends or a comma begins
    /// the next value. Every turn of either loop reads at least one byte, so both end within
    /// the text.
    fn document(&mut self) -> Result<(), JsonError> {
        loop {
            self.space();
            if self.value()? {
                continue;
            }
            loop {
                self.space();
                let Some((kind, _)) = self.open.innermost() else {
                    if self.at != self.text.len() {
                        return Err(JsonError::Syntax("content after the value"));
                    }
                    return Ok(());
                };
                match (kind, self.peek()) {
                    (_, Some(b',')) => {
                        self.advance(1)?;
                        self.space();
                        if kind == Kind::Object {
                            self.name()?;
                        }
                        break;
                    }
                    (Kind::Array, Some(b']')) | (Kind::Object, Some(b'}')) => {
                        self.advance(1)?;
                        self.close()?;
                    }
                    (Kind::Array, _) => return Err(JsonError::Syntax("an array not closed")),
                    (Kind::Object, _) => return Err(JsonError::Syntax("an object not closed")),
                }
            }
        }
    }

    /// Reads a value's first token: a whole scalar, or a container's opening. `true` when a
    /// container opened and the first value inside it follows.
    fn value(&mut self) -> Result<bool, JsonError> {
        let token = match self.peek() {
            Some(b'{') => return self.open(Kind::Object),
            Some(b'[') => return self.open(Kind::Array),
            Some(b'"') => Token::String(self.string()?),
            Some(b'-' | b'0'..=b'9') => self.number()?,
            Some(b't') => self.literal("true", Token::Bool(true))?,
            Some(b'f') => self.literal("false", Token::Bool(false))?,
            Some(b'n') => self.literal("null", Token::Null)?,
            Some(_) => return Err(JsonError::Syntax("a character no value begins with")),
            None => return Err(JsonError::Syntax("no value")),
        };
        self.tokens.push(token);
        Ok(false)
    }

    /// Opens a container one level deeper, within the bound: an empty one closes at once, and
    /// an object's first member's name is read. `true` when the first value inside follows.
    fn open(&mut self, kind: Kind) -> Result<bool, JsonError> {
        self.open.push(kind, self.tokens.len())?;
        self.advance(1)?;
        self.tokens.push(Token::Open(kind, 0));
        self.space();
        let close = match kind {
            Kind::Array => b']',
            Kind::Object => b'}',
        };
        if self.peek() == Some(close) {
            self.advance(1)?;
            self.close()?;
            return Ok(false);
        }
        if kind == Kind::Object {
            self.name()?;
        }
        Ok(true)
    }

    /// A member's name and the colon after it; its value follows.
    fn name(&mut self) -> Result<(), JsonError> {
        if self.peek() != Some(b'"') {
            return Err(JsonError::Syntax("a member without a string name"));
        }
        let name = self.string()?;
        self.tokens.push(Token::Name(name));
        self.space();
        if self.peek() != Some(b':') {
            return Err(JsonError::Syntax("a member without a colon"));
        }
        self.advance(1)
    }

    /// Closes the innermost container, its close read: an object's names are checked distinct,
    /// and its opening token counts the tokens inside.
    fn close(&mut self) -> Result<(), JsonError> {
        let nothing_open = JsonError::Syntax("a close with nothing open");
        let (kind, at) = self.open.innermost().ok_or(nothing_open)?;
        self.open.pop();
        let Some((Token::Open(..), inside)) =
            self.tokens.get(at..).and_then(<[Token]>::split_first)
        else {
            return Err(nothing_open);
        };
        let inner = inside.len();
        if kind == Kind::Object {
            // Names compare after escapes are replaced (RFC 8259 §8.3; RFC 7493 §2.3).
            let mut names: Vec<&str> = Members { tokens: inside }
                .iter()
                .map(|(name, _)| name)
                .collect();
            names.sort_unstable();
            if names.array_windows::<2>().any(|[a, b]| a == b) {
                return Err(JsonError::Duplicate);
            }
        }
        if let Some(Token::Open(_, count)) = self.tokens.get_mut(at) {
            *count = inner;
        }
        self.tokens.push(Token::Close(kind));
        Ok(())
    }

    fn literal(&mut self, name: &str, token: Token) -> Result<Token, JsonError> {
        if !self
            .text
            .get(self.at..)
            .is_some_and(|rest| rest.starts_with(name))
        {
            return Err(JsonError::Syntax("a misspelled literal"));
        }
        self.advance(name.len())?;
        Ok(token)
    }

    /// `number = [ minus ] int [ frac ] [ exp ]` (RFC 8259 §6), kept as written.
    fn number(&mut self) -> Result<Token, JsonError> {
        let start = self.at;
        if self.peek() == Some(b'-') {
            self.advance(1)?;
        }
        match self.peek() {
            Some(b'0') => self.advance(1)?,
            Some(b'1'..=b'9') => self.digits(),
            _ => return Err(JsonError::Syntax("a number without an integer part")),
        }
        if self.peek() == Some(b'.') {
            self.advance(1)?;
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return Err(JsonError::Syntax("a fraction without digits"));
            }
            self.digits();
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.advance(1)?;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.advance(1)?;
            }
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return Err(JsonError::Syntax("an exponent without digits"));
            }
            self.digits();
        }
        let written = self
            .text
            .get(start..self.at)
            .ok_or(JsonError::Syntax("a number"))?;
        Ok(Token::Number(written.to_owned()))
    }

    fn digits(&mut self) {
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.at = self.at.saturating_add(1);
        }
    }

    /// A string: its escapes replaced, a surrogate pair joined, and an unescaped control
    /// character refused (RFC 8259 §7).
    fn string(&mut self) -> Result<String, JsonError> {
        self.advance(1)?;
        let mut out = String::new();
        loop {
            let rest = self
                .text
                .get(self.at..)
                .ok_or(JsonError::Syntax("an unfinished string"))?;
            let run = rest
                .find(|c: char| c == '"' || c == '\\' || c < ' ')
                .ok_or(JsonError::Syntax("an unfinished string"))?;
            out.push_str(rest.get(..run).ok_or(JsonError::Syntax("a string"))?);
            self.advance(run)?;
            match self.peek() {
                Some(b'"') => {
                    self.advance(1)?;
                    return Ok(out);
                }
                Some(b'\\') => {
                    self.advance(1)?;
                    let c = self.escape()?;
                    out.push(c);
                }
                _ => return Err(JsonError::Syntax("a control character in a string")),
            }
        }
    }

    /// The character an escape after `\` stands for.
    fn escape(&mut self) -> Result<char, JsonError> {
        let byte = self
            .peek()
            .ok_or(JsonError::Syntax("an unfinished escape"))?;
        self.advance(1)?;
        Ok(match byte {
            b'"' => '"',
            b'\\' => '\\',
            b'/' => '/',
            b'b' => '\u{8}',
            b'f' => '\u{c}',
            b'n' => '\n',
            b'r' => '\r',
            b't' => '\t',
            b'u' => return self.unicode(),
            _ => return Err(JsonError::Syntax("an escape JSON does not define")),
        })
    }

    /// `\uXXXX`, or the two that encode a surrogate pair.
    fn unicode(&mut self) -> Result<char, JsonError> {
        let unit = self.hex4()?;
        if (0xDC00..=0xDFFF).contains(&unit) {
            return Err(JsonError::Surrogate);
        }
        if !(0xD800..=0xDBFF).contains(&unit) {
            return char::from_u32(unit).ok_or(JsonError::Surrogate);
        }
        if !self
            .text
            .get(self.at..)
            .is_some_and(|rest| rest.starts_with("\\u"))
        {
            return Err(JsonError::Surrogate);
        }
        self.advance(2)?;
        let low = self.hex4()?;
        if !(0xDC00..=0xDFFF).contains(&low) {
            return Err(JsonError::Surrogate);
        }
        let high = unit.checked_sub(0xD800).ok_or(JsonError::Surrogate)?;
        let low = low.checked_sub(0xDC00).ok_or(JsonError::Surrogate)?;
        let code = high
            .checked_shl(10)
            .and_then(|high| high.checked_add(low))
            .and_then(|code| code.checked_add(0x1_0000))
            .ok_or(JsonError::Surrogate)?;
        char::from_u32(code).ok_or(JsonError::Surrogate)
    }

    fn hex4(&mut self) -> Result<u32, JsonError> {
        let digits = self
            .text
            .get(self.at..)
            .and_then(|rest| rest.get(..4))
            .ok_or(JsonError::Syntax("an unfinished \\u escape"))?;
        if !digits.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(JsonError::Syntax("a \\u escape without four hex digits"));
        }
        let unit = crate::sigv4::hex_value(digits.as_bytes())
            .and_then(|unit| u32::try_from(unit).ok())
            .ok_or(JsonError::Syntax("a \\u escape without four hex digits"))?;
        self.advance(4)?;
        Ok(unit)
    }
}

impl Value {
    /// The string `text`.
    pub fn string(text: String) -> Self {
        Self {
            tokens: vec![Token::String(text)],
        }
    }

    /// The value, as a reader takes it.
    pub fn view(&self) -> View<'_> {
        // The reader and `string` each make one value's tokens, so the first begins one.
        split(&self.tokens).map_or(View::Null, |(view, _)| view)
    }

    /// The member named `name`, if this is an object that has one.
    pub fn member(&self, name: &str) -> Option<View<'_>> {
        self.view().member(name)
    }

    /// The value written compactly, as [`View::compact`] writes it.
    pub fn compact(&self) -> String {
        self.view().compact()
    }
}

impl<'a> View<'a> {
    /// The member named `name`, if this is an object that has one.
    pub fn member(self, name: &str) -> Option<View<'a>> {
        match self {
            Self::Object(members) => members.get(name),
            _ => None,
        }
    }

    /// The value written compactly: no white space between tokens, and in strings only what
    /// RFC 8259 §7 requires escaped, escaped. Its length is counted first, so the text is
    /// allocated once, at that length; a reservation the allocator refuses leaves it to grow as
    /// it is written, where `String::with_capacity` would panic on a length past `isize::MAX`.
    pub fn compact(self) -> String {
        let mut len = Some(0_usize);
        self.write(&mut |piece: &str| len = len.and_then(|len| len.checked_add(piece.len())));
        let mut out = String::new();
        if let Some(len) = len {
            out.try_reserve_exact(len).unwrap_or_default();
        }
        self.write(&mut |piece: &str| out.push_str(piece));
        out
    }

    /// Writes the value's text to `out`, piece by piece.
    fn write(self, out: &mut impl FnMut(&str)) {
        match self {
            Self::Null => out("null"),
            Self::Bool(true) => out("true"),
            Self::Bool(false) => out("false"),
            Self::Number(written) => out(written),
            Self::String(text) => write_string(text, out),
            Self::Array(items) => {
                out("[");
                write_tokens(items.tokens, out);
                out("]");
            }
            Self::Object(members) => {
                out("{");
                write_tokens(members.tokens, out);
                out("}");
            }
        }
    }
}

impl<'a> Items<'a> {
    pub fn is_empty(self) -> bool {
        self.tokens.is_empty()
    }

    /// The items in order, each a step: a container's tokens are stepped over at once.
    pub fn iter(self) -> impl Iterator<Item = View<'a>> {
        let mut rest = self.tokens;
        std::iter::from_fn(move || {
            let (item, after) = split(rest)?;
            rest = after;
            Some(item)
        })
    }
}

impl<'a> Members<'a> {
    pub fn is_empty(self) -> bool {
        self.tokens.is_empty()
    }

    /// The members in order, names and values, each a step: a value's tokens are stepped over
    /// at once.
    pub fn iter(self) -> impl Iterator<Item = (&'a str, View<'a>)> {
        let mut rest = self.tokens;
        std::iter::from_fn(move || {
            let (Token::Name(name), after) = rest.split_first()? else {
                return None;
            };
            let (value, after) = split(after)?;
            rest = after;
            Some((name.as_str(), value))
        })
    }

    /// The member named `name`.
    pub fn get(self, name: &str) -> Option<View<'a>> {
        self.iter()
            .find(|(member, _)| *member == name)
            .map(|(_, value)| value)
    }
}

/// The value whose first token begins `tokens`, and the tokens after its last: a container's
/// opening token counts the tokens inside it, so one step passes the whole container. `None`
/// where no value begins.
fn split(tokens: &[Token]) -> Option<(View<'_>, &[Token])> {
    let (first, rest) = tokens.split_first()?;
    let view = match first {
        Token::Null => View::Null,
        Token::Bool(b) => View::Bool(*b),
        Token::Number(written) => View::Number(written),
        Token::String(text) => View::String(text),
        Token::Open(kind, count) => {
            let (inside, after) = rest.split_at_checked(*count)?;
            let (Token::Close(_), after) = after.split_first()? else {
                return None;
            };
            let view = match kind {
                Kind::Array => View::Array(Items { tokens: inside }),
                Kind::Object => View::Object(Members { tokens: inside }),
            };
            return Some((view, after));
        }
        Token::Name(_) | Token::Close(_) => return None,
    };
    Some((view, rest))
}

/// Writes a run of tokens first to last: a comma between two values that follow each other in
/// a container, which the token before each says, and a colon after each name.
fn write_tokens(tokens: &[Token], out: &mut impl FnMut(&str)) {
    let mut after_value = false;
    for token in tokens {
        if after_value && !matches!(token, Token::Close(_)) {
            out(",");
        }
        after_value = !matches!(token, Token::Name(_) | Token::Open(..));
        match token {
            Token::Null => out("null"),
            Token::Bool(true) => out("true"),
            Token::Bool(false) => out("false"),
            Token::Number(written) => out(written),
            Token::String(text) => write_string(text, out),
            Token::Name(name) => {
                write_string(name, out);
                out(":");
            }
            Token::Open(Kind::Array, _) => out("["),
            Token::Open(Kind::Object, _) => out("{"),
            Token::Close(Kind::Array) => out("]"),
            Token::Close(Kind::Object) => out("}"),
        }
    }
}

/// A string, quoted, with what RFC 8259 §7 requires escaped and nothing else: `"`, `\`, and a
/// control character as `\u00XX`.
fn write_string(text: &str, out: &mut impl FnMut(&str)) {
    const HEX: &str = "0123456789abcdef";
    out("\"");
    let mut rest = text;
    while let Some(at) = rest.find(|c: char| c == '"' || c == '\\' || c < ' ') {
        let Some((run, escaped)) = rest.split_at_checked(at) else {
            break;
        };
        out(run);
        let mut chars = escaped.chars();
        match chars.next() {
            Some('"') => out("\\\""),
            Some('\\') => out("\\\\"),
            Some(c) => {
                out("\\u00");
                let code = u32::from(c);
                for nibble in [code >> 4, code & 15] {
                    let digit = usize::try_from(nibble).ok().and_then(|n| HEX.get(n..=n));
                    out(digit.unwrap_or("0"));
                }
            }
            None => break,
        }
        rest = chars.as_str();
    }
    out(rest);
    out("\"");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(text: &str) -> Result<Value, JsonError> {
        parse(text.as_bytes())
    }

    fn string(text: &str) -> Value {
        Value::string(text.into())
    }

    /// Values made from their parts, as a test states what a text reads as: a container's
    /// tokens are its parts' tokens, between its opening and its close.
    impl Value {
        fn null() -> Self {
            Self {
                tokens: vec![Token::Null],
            }
        }

        fn bool(b: bool) -> Self {
            Self {
                tokens: vec![Token::Bool(b)],
            }
        }

        fn number(written: String) -> Self {
            Self {
                tokens: vec![Token::Number(written)],
            }
        }

        fn array(items: Vec<Value>) -> Self {
            Self::container(Kind::Array, items.into_iter().map(|item| (None, item)))
        }

        fn object(members: Vec<(String, Value)>) -> Self {
            Self::container(
                Kind::Object,
                members.into_iter().map(|(name, value)| (Some(name), value)),
            )
        }

        fn container(kind: Kind, parts: impl Iterator<Item = (Option<String>, Value)>) -> Self {
            let mut tokens = vec![Token::Open(kind, 0)];
            for (name, value) in parts {
                tokens.extend(name.map(Token::Name));
                tokens.extend(value.tokens);
            }
            tokens[0] = Token::Open(kind, tokens.len() - 1);
            tokens.push(Token::Close(kind));
            Self { tokens }
        }
    }

    #[test]
    fn values_read_as_written() {
        assert_eq!(read(" null "), Ok(Value::null()));
        assert_eq!(read("true"), Ok(Value::bool(true)));
        assert_eq!(read("-0.5e+10"), Ok(Value::number("-0.5e+10".into())));
        assert_eq!(read("1E400"), Ok(Value::number("1E400".into())));
        assert_eq!(
            read(r#"{"a":[1,"b",{}],"c":null}"#),
            Ok(Value::object(vec![
                (
                    "a".into(),
                    Value::array(vec![
                        Value::number("1".into()),
                        string("b"),
                        Value::object(Vec::new())
                    ])
                ),
                ("c".into(), Value::null()),
            ]))
        );
        assert_eq!(
            read(r#""\"\\\/\b\f\n\r\t\u00e9\uD834\uDD1E""#),
            Ok(string("\"\\/\u{8}\u{c}\n\r\t\u{e9}\u{1D11E}"))
        );
        assert_eq!(
            read("\"\u{1D11E}\u{FFFF}\""),
            Ok(string("\u{1D11E}\u{FFFF}"))
        );
    }

    /// I-JSON's refusals (RFC 7493 §2.1, §2.3): a name given twice, compared after its escapes
    /// are replaced, and an unpaired surrogate.
    #[test]
    fn what_a_receiver_cannot_trust_is_refused() {
        assert_eq!(read(r#"{"a":1,"a":2}"#), Err(JsonError::Duplicate));
        assert_eq!(
            read(r#"{"a\\b":1,"a\u005Cb":2}"#),
            Err(JsonError::Duplicate)
        );
        assert!(read(r#"{"a":1,"A":2}"#).is_ok());
        for lone in [
            r#""\uDEAD""#,
            r#""\uD800""#,
            r#""\uD800\u0041""#,
            r#""\uD800x""#,
        ] {
            assert_eq!(read(lone), Err(JsonError::Surrogate), "{lone}");
        }
    }

    #[test]
    fn what_the_grammar_excludes_is_refused() {
        for text in [
            "",
            " ",
            "{",
            "[1,]",
            "[01]",
            "[1.]",
            "[.5]",
            "[1e]",
            "[-]",
            "[+1]",
            "[NaN]",
            "{\"a\" 1}",
            "{a:1}",
            "{\"a\":1,}",
            "[\"\t\"]",
            "[\"\\x\"]",
            "[\"\\u12\"]",
            "tru",
            "nul",
            "[] []",
            "\u{FEFF}{}",
            "{'a':1}",
            "[1 2]",
        ] {
            assert!(read(text).is_err(), "{text:?}");
        }
        assert_eq!(parse(b"[\"\xff\"]"), Err(JsonError::Utf8));
    }

    #[test]
    fn nesting_is_bounded() {
        let deepest = "[".repeat(MAX_DEPTH) + &"]".repeat(MAX_DEPTH);
        assert!(read(&deepest).is_ok());
        let deeper = "[".repeat(MAX_DEPTH + 1) + &"]".repeat(MAX_DEPTH + 1);
        assert_eq!(read(&deeper), Err(JsonError::Depth));
        assert_eq!(read(&"[".repeat(100_000)), Err(JsonError::Depth));
    }

    /// A text nesting `depth` levels around `1`, each level opened by the next of `openers` in
    /// turn, with white space around every structural character when `spaced`.
    fn nested(depth: usize, openers: &[&str], spaced: bool) -> String {
        let (mut text, mut closes) = (String::new(), String::new());
        for level in 0..depth {
            let opener = openers[level % openers.len()];
            text += opener;
            closes.insert(0, if opener == "[" { ']' } else { '}' });
        }
        let text = text + "1" + &closes;
        if !spaced {
            return text;
        }
        ["[", "]", "{", "}", ":"]
            .iter()
            .fold(text, |text, c| text.replace(c, &format!(" {c} ")))
    }

    /// A document as deep as the bound reads, in arrays, objects and both, and its compact form
    /// is the text without its white space, which reads back. One level deeper is refused as
    /// too deep, before anything after the opening past the bound is read.
    #[test]
    fn a_document_at_the_bound_reads_and_writes() {
        for openers in [
            &["["][..],
            &["{\"a\":"],
            &["[", "{\"a\":"],
            &["{\"a\":", "["],
        ] {
            let compact = nested(MAX_DEPTH, openers, false);
            let value = read(&nested(MAX_DEPTH, openers, true)).unwrap();
            assert_eq!(value.compact(), compact);
            assert_eq!(read(&compact), Ok(value.clone()));
            // Down to the innermost value, a step a level.
            let mut inner = value.view();
            for _ in 0..MAX_DEPTH {
                inner = match inner {
                    View::Array(items) => items.iter().next().unwrap(),
                    View::Object(members) => members.get("a").unwrap(),
                    other => panic!("{other:?}"),
                };
            }
            assert_eq!(inner, View::Number("1"));
            for spaced in [false, true] {
                let deeper = nested(MAX_DEPTH + 1, openers, spaced);
                assert_eq!(read(&deeper), Err(JsonError::Depth), "{deeper}");
            }
        }
        assert_eq!(
            read(&("[".repeat(MAX_DEPTH) + "x")),
            Err(JsonError::Syntax("a character no value begins with"))
        );
        assert_eq!(
            read(&("[".repeat(MAX_DEPTH + 1) + "x")),
            Err(JsonError::Depth)
        );
    }

    /// A lookup steps over each member's whole value: a member after a deep one, or after an
    /// object holding a member of its name, is found as written; a name no member has, or a
    /// lookup in a value that is no object, finds nothing.
    #[test]
    fn lookups_step_over_whole_values() {
        let value =
            read(r#"{"a":[[[[[{"d":0}]]]]],"b":{"d":1},"d":2,"e":[3,{"d":4},[],5]}"#).unwrap();
        assert_eq!(value.member("d"), Some(View::Number("2")));
        assert_eq!(
            value.member("b").unwrap().member("d"),
            Some(View::Number("1"))
        );
        assert_eq!(value.member("c"), None);
        let Some(View::Array(items)) = value.member("e") else {
            panic!("{value:?}");
        };
        let items: Vec<View<'_>> = items.iter().collect();
        assert_eq!(items.len(), 4);
        assert_eq!(items[0], View::Number("3"));
        assert_eq!(items[1].member("d"), Some(View::Number("4")));
        assert!(matches!(items[2], View::Array(empty) if empty.is_empty()));
        assert_eq!(items[3], View::Number("5"));
        assert_eq!(items[0].member("d"), None);
        assert_eq!(read("[{\"d\":1}]").unwrap().member("d"), None);
        let View::Object(members) = value.view() else {
            panic!("{value:?}");
        };
        let names: Vec<&str> = members.iter().map(|(name, _)| name).collect();
        assert_eq!(names, ["a", "b", "d", "e"]);
    }

    /// Values compare by what they hold, wherever they sit in a document, since a container
    /// counts the tokens inside it from its own opening.
    #[test]
    fn values_compare_wherever_they_sit() {
        let value = read(r#"[[1,{"a":[2]}],[1,{"a":[2]}],[1,{"a":[3]}]]"#).unwrap();
        let View::Array(items) = value.view() else {
            panic!("{value:?}");
        };
        let items: Vec<View<'_>> = items.iter().collect();
        assert_eq!(items[0], items[1]);
        assert_ne!(items[1], items[2]);
        assert_eq!(items[1].compact(), r#"[1,{"a":[2]}]"#);
        let alone = read(r#"[1,{"a":[2]}]"#).unwrap();
        assert_eq!(alone.view(), items[0]);
        assert_ne!(read("[1,2]"), read("[[1,2]]"));
        assert_ne!(read(r#"{"a":1}"#), read(r#"{"b":1}"#));
    }

    /// The compact form reads back as the value, and holds no white space between tokens.
    #[test]
    fn the_compact_form_reads_back() {
        let text = " { \"Version\" : \"2012-10-17\" , \"Statement\" : [ { \"Effect\" : \"Allow\" , \
                    \"N\" : 1.5e3 , \"Q\" : \"a\\\"b\\u0001\" } ] } ";
        let value = read(text).unwrap();
        let compact = value.compact();
        assert_eq!(
            compact,
            "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"N\":1.5e3,\
             \"Q\":\"a\\\"b\\u0001\"}]}"
        );
        assert_eq!(read(&compact), Ok(value));
    }

    fn value_strategy() -> impl proptest::strategy::Strategy<Value = Value> {
        use proptest::prelude::*;
        let text = prop::collection::vec(
            prop_oneof![
                Just('"'),
                Just('\\'),
                Just('\u{1}'),
                Just('\n'),
                Just('\u{1D11E}'),
                any::<char>()
            ],
            0..8,
        )
        .prop_map(|chars| chars.into_iter().collect::<String>());
        let number = prop_oneof![
            Just("0".to_string()),
            Just("-1.5e-300".to_string()),
            Just("123456789012345678901234567890".to_string()),
            any::<i64>().prop_map(|n| n.to_string()),
        ];
        let leaf = prop_oneof![
            Just(Value::null()),
            any::<bool>().prop_map(Value::bool),
            number.prop_map(Value::number),
            text.clone().prop_map(Value::string),
        ];
        leaf.prop_recursive(6, 64, 6, move |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..6).prop_map(Value::array),
                prop::collection::btree_map(text.clone(), inner, 0..6)
                    .prop_map(|members| Value::object(members.into_iter().collect())),
            ]
        })
    }

    proptest::proptest! {
        /// Any value reads back from its compact form exactly.
        #[test]
        fn compact_forms_read_back(value in value_strategy()) {
            proptest::prop_assert_eq!(parse(value.compact().as_bytes()), Ok(value));
        }

        /// Any bytes are answered, a value or an error, without a panic.
        #[test]
        fn any_bytes_are_answered(bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..256)) {
            let _ = parse(&bytes);
        }

        /// Any bytes built from JSON's own tokens are answered too, however they nest.
        #[test]
        fn any_tokens_are_answered(tokens in proptest::collection::vec(
            proptest::sample::select(vec!["{", "}", "[", "]", ",", ":", "\"a\"", "1", "-", "e", ".",
                "true", "null", " ", "\\u", "D800", "\"\\u"]), 0..64)) {
            let _ = parse(tokens.concat().as_bytes());
        }
    }
}
