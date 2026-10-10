//! JSON as RFC 8259 defines it (docs/research/17 §1), for the documents S3 takes as JSON: a
//! reader that accepts the texts the grammar describes, in UTF-8, and refuses what I-JSON
//! (RFC 7493 §2) forbids a receiver to trust: an object naming one member twice, which IAM's
//! policy grammar forbids too, and a string holding an unpaired surrogate. Numbers are kept as
//! written, so no range or precision is lost before a caller reads one as it needs. Nesting is
//! bounded, so a deep text is refused rather than followed down the stack; the caller bounds
//! the text's size.

/// The deepest nesting read: one level past the deepest document read. A bucket policy nests
/// six levels, a statement's condition values inside its operator inside its `Condition`
/// inside the statement inside `Statement` inside the policy (17 §3.1), and a POST policy
/// three, a condition's operands inside the condition inside `conditions` (19 §4.2). The
/// seventh level lets a policy's checks find an object or array where a value belongs and
/// refuse it with S3's message for it; a deeper one is refused as JSON too deep.
pub const MAX_DEPTH: usize = 7;

/// A JSON value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Null,
    Bool(bool),
    /// A number as written, which the grammar checked.
    Number(String),
    String(String),
    Array(Vec<Value>),
    /// Members in the order written, each name once.
    Object(Vec<(String, Value)>),
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
    let mut reader = Reader { text, at: 0 };
    reader.space();
    let value = reader.value(0)?;
    reader.space();
    if reader.at != text.len() {
        return Err(JsonError::Syntax("content after the value"));
    }
    Ok(value)
}

struct Reader<'a> {
    text: &'a str,
    /// The byte offset read to, always at a character boundary.
    at: usize,
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

    fn value(&mut self, depth: usize) -> Result<Value, JsonError> {
        match self.peek() {
            Some(b'{') => self.object(depth),
            Some(b'[') => self.array(depth),
            Some(b'"') => Ok(Value::String(self.string()?)),
            Some(b'-' | b'0'..=b'9') => self.number(),
            Some(b't') => self.literal("true", Value::Bool(true)),
            Some(b'f') => self.literal("false", Value::Bool(false)),
            Some(b'n') => self.literal("null", Value::Null),
            Some(_) => Err(JsonError::Syntax("a character no value begins with")),
            None => Err(JsonError::Syntax("no value")),
        }
    }

    fn literal(&mut self, name: &str, value: Value) -> Result<Value, JsonError> {
        if !self
            .text
            .get(self.at..)
            .is_some_and(|rest| rest.starts_with(name))
        {
            return Err(JsonError::Syntax("a misspelled literal"));
        }
        self.advance(name.len())?;
        Ok(value)
    }

    /// Enters a nested value: one level deeper, within the bound.
    fn deeper(depth: usize) -> Result<usize, JsonError> {
        depth
            .checked_add(1)
            .filter(|depth| *depth <= MAX_DEPTH)
            .ok_or(JsonError::Depth)
    }

    fn object(&mut self, depth: usize) -> Result<Value, JsonError> {
        let depth = Self::deeper(depth)?;
        self.advance(1)?;
        self.space();
        let mut members: Vec<(String, Value)> = Vec::new();
        if self.peek() == Some(b'}') {
            self.advance(1)?;
            return Ok(Value::Object(members));
        }
        loop {
            if self.peek() != Some(b'"') {
                return Err(JsonError::Syntax("a member without a string name"));
            }
            let name = self.string()?;
            self.space();
            if self.peek() != Some(b':') {
                return Err(JsonError::Syntax("a member without a colon"));
            }
            self.advance(1)?;
            self.space();
            let value = self.value(depth)?;
            members.push((name, value));
            self.space();
            match self.peek() {
                Some(b',') => {
                    self.advance(1)?;
                    self.space();
                }
                Some(b'}') => {
                    self.advance(1)?;
                    break;
                }
                _ => return Err(JsonError::Syntax("an object not closed")),
            }
        }
        // Names compare after escapes are replaced (RFC 8259 §8.3; RFC 7493 §2.3).
        let mut names: Vec<&str> = members.iter().map(|(name, _)| name.as_str()).collect();
        names.sort_unstable();
        if names.array_windows::<2>().any(|[a, b]| a == b) {
            return Err(JsonError::Duplicate);
        }
        Ok(Value::Object(members))
    }

    fn array(&mut self, depth: usize) -> Result<Value, JsonError> {
        let depth = Self::deeper(depth)?;
        self.advance(1)?;
        self.space();
        let mut items = Vec::new();
        if self.peek() == Some(b']') {
            self.advance(1)?;
            return Ok(Value::Array(items));
        }
        loop {
            items.push(self.value(depth)?);
            self.space();
            match self.peek() {
                Some(b',') => {
                    self.advance(1)?;
                    self.space();
                }
                Some(b']') => {
                    self.advance(1)?;
                    return Ok(Value::Array(items));
                }
                _ => return Err(JsonError::Syntax("an array not closed")),
            }
        }
    }

    /// `number = [ minus ] int [ frac ] [ exp ]` (RFC 8259 §6), kept as written.
    fn number(&mut self) -> Result<Value, JsonError> {
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
        Ok(Value::Number(written.to_owned()))
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
    /// The member named `name`, if this is an object that has one.
    pub fn member(&self, name: &str) -> Option<&Value> {
        match self {
            Self::Object(members) => members
                .iter()
                .find(|(member, _)| member == name)
                .map(|(_, value)| value),
            _ => None,
        }
    }

    /// The value written compactly: no white space between tokens, and in strings only what
    /// RFC 8259 §7 requires escaped, escaped.
    pub fn compact(&self) -> String {
        let mut out = String::new();
        self.write(&mut out);
        out
    }

    fn write(&self, out: &mut String) {
        match self {
            Self::Null => out.push_str("null"),
            Self::Bool(true) => out.push_str("true"),
            Self::Bool(false) => out.push_str("false"),
            Self::Number(written) => out.push_str(written),
            Self::String(text) => write_string(out, text),
            Self::Array(items) => {
                out.push('[');
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    item.write(out);
                }
                out.push(']');
            }
            Self::Object(members) => {
                out.push('{');
                for (i, (name, value)) in members.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    write_string(out, name);
                    out.push(':');
                    value.write(out);
                }
                out.push('}');
            }
        }
    }
}

fn write_string(out: &mut String, text: &str) {
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if c < ' ' => {
                const HEX: &[u8; 16] = b"0123456789abcdef";
                let code = u32::from(c);
                out.push_str("\\u00");
                for nibble in [code >> 4, code & 15] {
                    let digit = usize::try_from(nibble).ok().and_then(|n| HEX.get(n));
                    out.push(char::from(digit.copied().unwrap_or(b'0')));
                }
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(text: &str) -> Result<Value, JsonError> {
        parse(text.as_bytes())
    }

    fn string(text: &str) -> Value {
        Value::String(text.into())
    }

    #[test]
    fn values_read_as_written() {
        assert_eq!(read(" null "), Ok(Value::Null));
        assert_eq!(read("true"), Ok(Value::Bool(true)));
        assert_eq!(read("-0.5e+10"), Ok(Value::Number("-0.5e+10".into())));
        assert_eq!(read("1E400"), Ok(Value::Number("1E400".into())));
        assert_eq!(
            read(r#"{"a":[1,"b",{}],"c":null}"#),
            Ok(Value::Object(vec![
                (
                    "a".into(),
                    Value::Array(vec![
                        Value::Number("1".into()),
                        string("b"),
                        Value::Object(Vec::new())
                    ])
                ),
                ("c".into(), Value::Null),
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
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            number.prop_map(Value::Number),
            text.clone().prop_map(Value::String),
        ];
        leaf.prop_recursive(6, 64, 6, move |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..6).prop_map(Value::Array),
                prop::collection::btree_map(text.clone(), inner, 0..6)
                    .prop_map(|members| Value::Object(members.into_iter().collect())),
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
