//! XML bodies (docs/design/s3-protocol.md §2; docs/research/13): a reader for the documents S3
//! requests carry and a writer for the documents its responses carry.
//!
//! The reader checks what XML 1.0 and Namespaces in XML 1.0 require of a document without a
//! document type declaration, and refuses a declaration, so no entity is ever declared or
//! expanded (RFC 7303 §10; 13 §2). It pulls elements as a schema reader asks for them and
//! builds no tree: it descends only into elements the schema names, and every step consumes
//! input, so its work is linear in a body whose size the caller has bounded. One deliberate
//! difference from XML 1.0: a character reference may name any character but NUL, as XML 1.1
//! allows, because S3 lists a key holding a control character that way and a key listed must
//! be one a client can send back (13 §7). Bodies are UTF-8 (docs/design/s3-protocol.md §2).

use std::borrow::Cow;
use std::collections::BTreeSet;

/// S3's namespace. Request documents may carry it or none (13 §6.1).
pub const NAMESPACE: &str = "http://s3.amazonaws.com/doc/2006-03-01/";
const XML_NAMESPACE: &str = "http://www.w3.org/XML/1998/namespace";
const XMLNS_NAMESPACE: &str = "http://www.w3.org/2000/xmlns/";
/// XML Schema's instance namespace, whose `type` attribute names an ACL grantee's kind (13 §6.8).
const XSI_NAMESPACE: &str = "http://www.w3.org/2001/XMLSchema-instance";

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum XmlError {
    #[error("the body is larger than the {limit} bytes this request takes")]
    TooLarge { limit: usize },
    #[error("the body is not well-formed XML: {0}")]
    NotWellFormed(&'static str),
    #[error("the body is not the document this request takes: {0}")]
    Schema(&'static str),
}

impl XmlError {
    /// The S3 error code and status: `MalformedXML`, "not well formed or did not validate
    /// against our published schema", and `MaxMessageLengthExceeded`, "Your request was too
    /// large" (05 §11.2).
    pub fn code(&self) -> (&'static str, u16) {
        match self {
            Self::TooLarge { .. } => ("MaxMessageLengthExceeded", 400),
            Self::NotWellFormed(_) | Self::Schema(_) => ("MalformedXML", 400),
        }
    }
}

/// Reads one request document.
///
/// [`open`](Self::open) reads through the root's start tag. The schema reader then consumes
/// every element it is given: with [`child`](Self::child) until `None` for an element that
/// holds elements, or with [`text`](Self::text) for one that holds text. It ends with
/// [`finish`](Self::finish).
pub struct Reader<'a> {
    rest: &'a str,
    /// Qualified names of the open elements, outermost first.
    open: Vec<&'a str>,
    /// Namespace bindings in scope, innermost last.
    bindings: Vec<Binding<'a>>,
    /// The element last given was an empty-element tag: its content is empty and it is closed.
    closed: bool,
    /// The element that may carry `xsi:type`, as an ACL's `Grantee` does.
    typed: Option<&'static str>,
    /// The local part of the `xsi:type` of the element last given, if it carries one.
    xsi_type: Option<String>,
}

struct Binding<'a> {
    /// The depth of the element that declared it; the root is 1.
    depth: usize,
    /// `""` for the default namespace.
    prefix: &'a str,
    namespace: Cow<'a, str>,
}

impl<'a> Reader<'a> {
    /// Opens a body of at most `limit` bytes whose root element is `root`.
    pub fn open(body: &'a [u8], limit: usize, root: &str) -> Result<Self, XmlError> {
        // The limit counts the document without the white space between its elements, as
        // the gateway keeps it while reading (`Compact`), so a body reads alike as sent or as
        // kept.
        if body.len() > limit {
            let mut counted = Compact::counting(limit);
            counted.push(body)?;
            counted.finish()?;
        }
        let text = std::str::from_utf8(body).map_err(|_| XmlError::NotWellFormed("not UTF-8"))?;
        // Every character of a document matches Char (XML 1.0 §2.2); markup is built from them.
        if !text.chars().all(is_char) {
            return Err(XmlError::NotWellFormed("a character XML does not allow"));
        }
        let mut reader = Self {
            rest: text.strip_prefix('\u{FEFF}').unwrap_or(text),
            open: Vec::new(),
            bindings: Vec::new(),
            closed: false,
            typed: None,
            xsi_type: None,
        };
        reader.declaration()?;
        reader.misc()?;
        if reader.rest.starts_with("<!DOCTYPE") {
            return Err(XmlError::NotWellFormed(
                "a document type declaration, which mantle refuses",
            ));
        }
        if !reader.rest.starts_with('<') {
            return Err(XmlError::NotWellFormed("no root element"));
        }
        if reader.start_tag()? != root {
            return Err(XmlError::Schema("not the root element this request takes"));
        }
        Ok(reader)
    }

    /// Admits `xsi:type` on the elements named `element` after the root, as ACL documents
    /// carry it on a grantee (13 §6.8); every other element refuses it with the rest of the
    /// attributes S3 does not use.
    pub fn admit_types(mut self, element: &'static str) -> Self {
        self.typed = Some(element);
        self
    }

    /// The local part of the `xsi:type` the element last given carries, in S3's namespace or
    /// none, as its elements are.
    pub fn xsi_type(&self) -> Option<&str> {
        self.xsi_type.as_deref()
    }

    /// The local name of the next child of the element last given, or `None` once that
    /// element's end tag is read. Only white space, comments and processing instructions may
    /// sit between its children.
    pub fn child(&mut self) -> Result<Option<&'a str>, XmlError> {
        if std::mem::take(&mut self.closed) {
            return Ok(None);
        }
        self.misc()?;
        if self.rest.starts_with("</") {
            self.end_tag()?;
            return Ok(None);
        }
        if self.rest.is_empty() {
            return Err(XmlError::NotWellFormed("an element left open"));
        }
        if !self.rest.starts_with('<') || self.rest.starts_with("<![CDATA[") {
            return Err(XmlError::Schema("text where S3 expects elements"));
        }
        self.start_tag().map(Some)
    }

    /// The text of the element last given, with references replaced and line ends
    /// normalized (XML 1.0 §2.11, §4.6), and its end tag. Comments and processing instructions
    /// inside it are dropped; an element inside it is refused.
    pub fn text(&mut self) -> Result<Cow<'a, str>, XmlError> {
        if std::mem::take(&mut self.closed) {
            return Ok(Cow::Borrowed(""));
        }
        let mut text = None;
        loop {
            let end = self.rest.find(['<', '&', '\r']).unwrap_or(self.rest.len());
            let (run, rest) = split(self.rest, end)?;
            // CharData never holds `]]>` (XML 1.0 [14]).
            if run.contains("]]>") {
                return Err(XmlError::NotWellFormed("]]> in text"));
            }
            self.rest = rest;
            append(&mut text, run);
            if self.rest.starts_with("</") {
                self.end_tag()?;
                return Ok(text.unwrap_or(Cow::Borrowed("")));
            } else if self.rest.starts_with("<!--") {
                self.comment()?;
            } else if self.rest.starts_with("<?") {
                self.instruction()?;
            } else if let Some(rest) = self.rest.strip_prefix("<![CDATA[") {
                let end = rest
                    .find("]]>")
                    .ok_or(XmlError::NotWellFormed("an unclosed CDATA section"))?;
                let (data, rest) = split(rest, end)?;
                self.rest = strip(rest, "]]>")?;
                for c in normalized_lines(data) {
                    push(&mut text, c);
                }
            } else if self.rest.starts_with('<') {
                return Err(XmlError::Schema("an element where S3 expects text"));
            } else if self.rest.starts_with('&') {
                let c = self.reference()?;
                push(&mut text, c);
            } else if let Some(rest) = self.rest.strip_prefix('\r') {
                self.rest = rest.strip_prefix('\n').unwrap_or(rest);
                push(&mut text, '\n');
            } else {
                return Err(XmlError::NotWellFormed("an element left open"));
            }
        }
    }

    /// Ends the document: after the root's end tag come only white space, comments and
    /// processing instructions (XML 1.0 [1], [27]).
    pub fn finish(mut self) -> Result<(), XmlError> {
        if !self.open.is_empty() || self.closed {
            return Err(XmlError::Schema("the document holds more than S3 reads"));
        }
        self.misc()?;
        if self.rest.is_empty() {
            Ok(())
        } else {
            Err(XmlError::NotWellFormed("content after the root element"))
        }
    }

    /// The XML declaration, if any: version 1.x, the encoding UTF-8 if one is named, and
    /// standalone yes or no (XML 1.0 [23]–[26], [32], [80]–[81]).
    fn declaration(&mut self) -> Result<(), XmlError> {
        const MALFORMED: XmlError = XmlError::NotWellFormed("a malformed XML declaration");
        let Some(rest) = self.rest.strip_prefix("<?xml") else {
            return Ok(());
        };
        if !rest.starts_with(is_space) {
            // `<?xml-stylesheet ...?>` and the like are processing instructions.
            return Ok(());
        }
        self.rest = rest;
        let version = self.pseudo_attribute("version")?.ok_or(MALFORMED)?;
        let digits = version.strip_prefix("1.").ok_or(MALFORMED)?;
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return Err(MALFORMED);
        }
        if let Some(encoding) = self.pseudo_attribute("encoding")?
            && !encoding.eq_ignore_ascii_case("UTF-8")
        {
            return Err(XmlError::NotWellFormed("an encoding other than UTF-8"));
        }
        if let Some(standalone) = self.pseudo_attribute("standalone")?
            && standalone != "yes"
            && standalone != "no"
        {
            return Err(MALFORMED);
        }
        self.skip_space();
        self.rest = self.rest.strip_prefix("?>").ok_or(MALFORMED)?;
        Ok(())
    }

    /// ` name="value"` in the XML declaration; `None`, consuming nothing, when the next
    /// pseudo-attribute is not `name`.
    fn pseudo_attribute(&mut self, name: &str) -> Result<Option<&'a str>, XmlError> {
        const MALFORMED: XmlError = XmlError::NotWellFormed("a malformed XML declaration");
        let spaced = self.rest.trim_start_matches(is_space);
        if spaced.len() == self.rest.len() {
            return Ok(None);
        }
        let Some(rest) = spaced.strip_prefix(name) else {
            return Ok(None);
        };
        let rest = rest.trim_start_matches(is_space);
        let rest = rest.strip_prefix('=').ok_or(MALFORMED)?;
        let rest = rest.trim_start_matches(is_space);
        let quote = match rest.as_bytes().first() {
            Some(b'"') => "\"",
            Some(b'\'') => "'",
            _ => return Err(MALFORMED),
        };
        let rest = strip(rest, quote)?;
        let end = rest.find(quote).ok_or(MALFORMED)?;
        let (value, rest) = split(rest, end)?;
        self.rest = strip(rest, quote)?;
        Ok(Some(value))
    }

    /// White space, comments and processing instructions (XML 1.0 [27]).
    fn misc(&mut self) -> Result<(), XmlError> {
        loop {
            self.skip_space();
            if self.rest.starts_with("<!--") {
                self.comment()?;
            } else if self.rest.starts_with("<?") {
                self.instruction()?;
            } else {
                return Ok(());
            }
        }
    }

    /// A comment (XML 1.0 [15]), in which `--` appears only as part of the closing `-->`.
    fn comment(&mut self) -> Result<(), XmlError> {
        let rest = strip(self.rest, "<!--")?;
        let end = rest
            .find("--")
            .ok_or(XmlError::NotWellFormed("an unclosed comment"))?;
        let (_, rest) = split(rest, end)?;
        self.rest = rest
            .strip_prefix("-->")
            .ok_or(XmlError::NotWellFormed("-- inside a comment"))?;
        Ok(())
    }

    /// A processing instruction (XML 1.0 [16]–[17]), whose target is not `xml` in any case
    /// and holds no colon (Namespaces in XML 1.0 §7).
    fn instruction(&mut self) -> Result<(), XmlError> {
        self.rest = strip(self.rest, "<?")?;
        let target = self.name()?;
        if target.eq_ignore_ascii_case("xml") || target.contains(':') {
            return Err(XmlError::NotWellFormed(
                "a processing instruction with a reserved target",
            ));
        }
        if !self.rest.starts_with("?>") && !self.rest.starts_with(is_space) {
            return Err(XmlError::NotWellFormed(
                "a processing instruction's target runs into its content",
            ));
        }
        let end = self.rest.find("?>").ok_or(XmlError::NotWellFormed(
            "an unclosed processing instruction",
        ))?;
        let (_, rest) = split(self.rest, end)?;
        self.rest = strip(rest, "?>")?;
        Ok(())
    }

    /// A start tag or empty-element tag (XML 1.0 [40], [44]) of an element in S3's namespace
    /// or none, bringing its namespace declarations into scope; returns its local name.
    fn start_tag(&mut self) -> Result<&'a str, XmlError> {
        self.rest = strip(self.rest, "<")?;
        let qualified = self.name()?;
        // The names seen, sorted, so that finding one given twice costs a search, not a pass:
        // the body's size limit bounds how many a tag holds.
        let mut attributes: Vec<(&'a str, Cow<'a, str>)> = Vec::new();
        let mut names = BTreeSet::new();
        let empty = loop {
            let spaced = self.skip_space();
            if let Some(rest) = self.rest.strip_prefix("/>") {
                self.rest = rest;
                break true;
            }
            if let Some(rest) = self.rest.strip_prefix('>') {
                self.rest = rest;
                break false;
            }
            if !spaced {
                return Err(XmlError::NotWellFormed(
                    "attributes not separated by white space",
                ));
            }
            let name = self.name()?;
            self.skip_space();
            self.rest = strip(self.rest, "=")?;
            self.skip_space();
            let value = self.attribute_value()?;
            if !names.insert(name) {
                return Err(XmlError::NotWellFormed("an attribute given twice"));
            }
            attributes.push((name, value));
        };
        let depth = self
            .open
            .len()
            .checked_add(1)
            .ok_or(XmlError::Schema("elements nested too deeply"))?;
        // Declarations first: an attribute's prefix may be declared on its own tag.
        let mut typed = Vec::new();
        for (name, value) in attributes {
            if name == "xmlns" || name.starts_with("xmlns:") {
                self.declare(depth, name, value)?;
            } else {
                typed.push((name, value));
            }
        }
        self.xsi_type = None;
        for (name, value) in typed {
            let (prefix, local) = split_qualified(name)?;
            // An unprefixed attribute is in no namespace (Namespaces in XML 1.0 §6.2).
            if self.typed.is_none()
                || prefix.is_empty()
                || local != "type"
                || self.namespace(prefix)? != Some(XSI_NAMESPACE)
            {
                return Err(XmlError::Schema("an attribute S3's documents do not carry"));
            }
            // Two attributes with one expanded name (Namespaces in XML 1.0 §6.3).
            if self.xsi_type.is_some() {
                return Err(XmlError::NotWellFormed("an attribute given twice"));
            }
            self.xsi_type = Some(self.type_name(&value)?.to_owned());
        }
        let (prefix, local) = split_qualified(qualified)?;
        if self.namespace(prefix)?.is_some_and(|n| n != NAMESPACE) {
            return Err(XmlError::Schema("an element outside S3's namespace"));
        }
        if self.xsi_type.is_some() && self.typed != Some(local) {
            return Err(XmlError::Schema("an attribute S3's documents do not carry"));
        }
        if empty {
            self.unbind(depth);
            self.closed = true;
        } else {
            self.open.push(qualified);
        }
        Ok(local)
    }

    /// The local part of an `xsi:type` value, a QName: collapsed as XML Schema collapses one
    /// (Part 2 §3.2.18), its prefix resolved like an element's, to S3's namespace or none.
    fn type_name<'v>(&self, value: &'v str) -> Result<&'v str, XmlError> {
        let (prefix, local) = split_qualified(value.trim_matches(is_space))?;
        if !local.starts_with(is_name_start) || !local.chars().all(is_name_char) {
            return Err(XmlError::Schema("an xsi:type that is not a name"));
        }
        if self.namespace(prefix)?.is_some_and(|n| n != NAMESPACE) {
            return Err(XmlError::Schema("an xsi:type outside S3's namespace"));
        }
        Ok(local)
    }

    /// A namespace declaration (Namespaces in XML 1.0 §3 and its constraints). S3's request
    /// documents carry no other attribute but `xsi:type`.
    fn declare(
        &mut self,
        depth: usize,
        name: &'a str,
        namespace: Cow<'a, str>,
    ) -> Result<(), XmlError> {
        const RESERVED: XmlError = XmlError::NotWellFormed("a reserved namespace misdeclared");
        let prefix = if name == "xmlns" {
            ""
        } else if let Some(prefix) = name.strip_prefix("xmlns:") {
            // PrefixedAttName: the prefix is an NCName; and no prefix is undeclared.
            if !prefix.starts_with(is_name_start) || prefix.contains(':') || namespace.is_empty() {
                return Err(XmlError::NotWellFormed("a malformed namespace declaration"));
            }
            prefix
        } else {
            return Err(XmlError::Schema("an attribute S3's documents do not carry"));
        };
        // `xmlns` is never declared and nothing is bound to its namespace; `xml` is bound only
        // to its own, which nothing else is bound to.
        if prefix == "xmlns"
            || namespace == XMLNS_NAMESPACE
            || (prefix == "xml") != (namespace == XML_NAMESPACE)
        {
            return Err(RESERVED);
        }
        self.bindings.push(Binding {
            depth,
            prefix,
            namespace,
        });
        Ok(())
    }

    /// The namespace `prefix` names in scope; `None` for no namespace.
    fn namespace(&self, prefix: &str) -> Result<Option<&str>, XmlError> {
        if prefix == "xml" {
            return Ok(Some(XML_NAMESPACE));
        }
        if prefix == "xmlns" {
            return Err(XmlError::NotWellFormed("an element with the prefix xmlns"));
        }
        match self.bindings.iter().rev().find(|b| b.prefix == prefix) {
            // `xmlns=""` leaves the default namespace undeclared.
            Some(binding) if binding.namespace.is_empty() => Ok(None),
            Some(binding) => Ok(Some(binding.namespace.as_ref())),
            None if prefix.is_empty() => Ok(None),
            None => Err(XmlError::NotWellFormed("an undeclared namespace prefix")),
        }
    }

    /// Takes out of scope the bindings of elements deeper than `depth - 1`.
    fn unbind(&mut self, depth: usize) {
        while self.bindings.last().is_some_and(|b| b.depth >= depth) {
            self.bindings.pop();
        }
    }

    /// An end tag (XML 1.0 [42]) that matches the innermost open element.
    fn end_tag(&mut self) -> Result<(), XmlError> {
        self.rest = strip(self.rest, "</")?;
        let name = self.name()?;
        self.skip_space();
        self.rest = strip(self.rest, ">")?;
        let depth = self.open.len();
        if self.open.pop() != Some(name) {
            return Err(XmlError::NotWellFormed(
                "an end tag that does not match its start tag",
            ));
        }
        self.unbind(depth);
        Ok(())
    }

    /// A quoted attribute value, with references replaced and white space normalized (XML
    /// 1.0 [10], §3.3.3, after §2.11's line ends).
    fn attribute_value(&mut self) -> Result<Cow<'a, str>, XmlError> {
        let quote = match self.rest.as_bytes().first() {
            Some(b'"') => '"',
            Some(b'\'') => '\'',
            _ => return Err(XmlError::NotWellFormed("an unquoted attribute value")),
        };
        self.rest = self.rest.get(1..).unwrap_or("");
        let mut value = None;
        loop {
            let end = self
                .rest
                .find([quote, '<', '&', '\t', '\n', '\r'])
                .ok_or(XmlError::NotWellFormed("an unterminated attribute value"))?;
            let (run, rest) = split(self.rest, end)?;
            append(&mut value, run);
            self.rest = rest;
            let mut chars = rest.chars();
            match chars.next() {
                Some('<') => return Err(XmlError::NotWellFormed("< in an attribute value")),
                Some('&') => {
                    let c = self.reference()?;
                    push(&mut value, c);
                }
                Some('\r') => {
                    let after = chars.as_str();
                    self.rest = after.strip_prefix('\n').unwrap_or(after);
                    push(&mut value, ' ');
                }
                Some(c) if c == quote => {
                    self.rest = chars.as_str();
                    return Ok(value.unwrap_or(Cow::Borrowed("")));
                }
                _ => {
                    self.rest = chars.as_str();
                    push(&mut value, ' ');
                }
            }
        }
    }

    /// A character or entity reference (XML 1.0 [66]–[68]). With no document type declaration
    /// only the five predefined entities exist (XML 1.0 §4.1, WFC: Entity Declared; §4.6).
    fn reference(&mut self) -> Result<char, XmlError> {
        let rest = strip(self.rest, "&")?;
        let end = rest
            .find(';')
            .ok_or(XmlError::NotWellFormed("an unterminated reference"))?;
        let (name, rest) = split(rest, end)?;
        self.rest = strip(rest, ";")?;
        let c = match name {
            "lt" => '<',
            "gt" => '>',
            "amp" => '&',
            "apos" => '\'',
            "quot" => '"',
            _ => {
                let code = if let Some(hex) = name.strip_prefix("#x") {
                    code_point(hex, 16)
                } else if let Some(decimal) = name.strip_prefix('#') {
                    code_point(decimal, 10)
                } else {
                    return Err(XmlError::NotWellFormed(
                        "a reference to an undeclared entity",
                    ));
                };
                code.and_then(char::from_u32)
                    .filter(|c| is_referable(*c))
                    .ok_or(XmlError::NotWellFormed(
                        "a reference to a character XML does not allow",
                    ))?
            }
        };
        Ok(c)
    }

    /// A Name (XML 1.0 [5]).
    fn name(&mut self) -> Result<&'a str, XmlError> {
        let mut chars = self.rest.char_indices();
        if !chars.next().is_some_and(|(_, c)| is_name_start(c)) {
            return Err(XmlError::NotWellFormed("a name expected"));
        }
        let end = chars
            .find(|(_, c)| !is_name_char(*c))
            .map_or(self.rest.len(), |(at, _)| at);
        let (name, rest) = split(self.rest, end)?;
        self.rest = rest;
        Ok(name)
    }

    /// Skips white space (XML 1.0 [3]); whether there was any.
    fn skip_space(&mut self) -> bool {
        let rest = self.rest.trim_start_matches(is_space);
        let skipped = rest.len() != self.rest.len();
        self.rest = rest;
        skipped
    }
}

/// A qualified name's prefix (`""` for none) and local part (Namespaces in XML 1.0 [7]–[11]).
fn split_qualified(name: &str) -> Result<(&str, &str), XmlError> {
    match name.split_once(':') {
        None => Ok(("", name)),
        Some((prefix, local))
            if !prefix.is_empty() && !local.contains(':') && local.starts_with(is_name_start) =>
        {
            Ok((prefix, local))
        }
        Some(_) => Err(XmlError::NotWellFormed("a name with a misplaced colon")),
    }
}

/// `text` split at `at`, which a search of `text` found.
fn split(text: &str, at: usize) -> Result<(&str, &str), XmlError> {
    text.split_at_checked(at)
        .ok_or(XmlError::NotWellFormed("a split inside a character"))
}

/// `text` after `token`, which it starts with.
fn strip<'t>(text: &'t str, token: &str) -> Result<&'t str, XmlError> {
    text.strip_prefix(token)
        .ok_or(XmlError::NotWellFormed("unexpected markup"))
}

/// Adds a run of the body to text read so far, borrowing while the text is that one run.
fn append<'a>(text: &mut Option<Cow<'a, str>>, run: &'a str) {
    match text {
        None => *text = Some(Cow::Borrowed(run)),
        Some(text) => text.to_mut().push_str(run),
    }
}

fn push(text: &mut Option<Cow<'_, str>>, c: char) {
    text.get_or_insert(Cow::Borrowed("")).to_mut().push(c);
}

/// `data` with each CR LF pair and each lone CR read as LF (XML 1.0 §2.11).
fn normalized_lines(data: &str) -> impl Iterator<Item = char> + '_ {
    let mut chars = data.chars().peekable();
    std::iter::from_fn(move || {
        let c = chars.next()?;
        if c == '\r' {
            chars.next_if_eq(&'\n');
            Some('\n')
        } else {
            Some(c)
        }
    })
}

/// A character reference's digits, `None` unless all are digits of `radix` (XML 1.0 [66]).
fn code_point(digits: &str, radix: u32) -> Option<u32> {
    if digits.is_empty() || !digits.chars().all(|c| c.is_digit(radix)) {
        return None;
    }
    u32::from_str_radix(digits, radix).ok()
}

/// XML 1.0 [3] S.
fn is_space(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\r' | '\n')
}

/// XML 1.0 [2] Char.
fn is_char(c: char) -> bool {
    matches!(c, '\t' | '\n' | '\r' | '\u{20}'..='\u{D7FF}' | '\u{E000}'..='\u{FFFD}' | '\u{10000}'..='\u{10FFFF}')
}

/// XML 1.1 [2] Char: what a character reference may name.
fn is_referable(c: char) -> bool {
    matches!(c, '\u{1}'..='\u{D7FF}' | '\u{E000}'..='\u{FFFD}' | '\u{10000}'..='\u{10FFFF}')
}

/// XML 1.0 [4] NameStartChar.
fn is_name_start(c: char) -> bool {
    matches!(c,
        ':' | 'A'..='Z' | '_' | 'a'..='z' | '\u{C0}'..='\u{D6}' | '\u{D8}'..='\u{F6}'
        | '\u{F8}'..='\u{2FF}' | '\u{370}'..='\u{37D}' | '\u{37F}'..='\u{1FFF}'
        | '\u{200C}'..='\u{200D}' | '\u{2070}'..='\u{218F}' | '\u{2C00}'..='\u{2FEF}'
        | '\u{3001}'..='\u{D7FF}' | '\u{F900}'..='\u{FDCF}' | '\u{FDF0}'..='\u{FFFD}'
        | '\u{10000}'..='\u{EFFFF}')
}

/// XML 1.0 [4a] NameChar.
fn is_name_char(c: char) -> bool {
    is_name_start(c)
        || matches!(c, '-' | '.' | '0'..='9' | '\u{B7}' | '\u{300}'..='\u{36F}' | '\u{203F}'..='\u{2040}')
}

/// Writes one response document.
pub struct Writer {
    out: String,
}

impl Writer {
    /// A document whose root element is `root`, in S3's namespace when `namespaced`, holding
    /// what `body` writes.
    pub fn document(root: &str, namespaced: bool, body: impl FnOnce(&mut Self)) -> String {
        let mut writer = Self {
            out: String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"),
        };
        writer.out.push('<');
        writer.out.push_str(root);
        if namespaced {
            writer.out.push_str(" xmlns=\"");
            writer.out.push_str(NAMESPACE);
            writer.out.push('"');
        }
        writer.out.push('>');
        body(&mut writer);
        writer.end(root);
        writer.out
    }

    /// An element holding what `body` writes.
    pub fn element(&mut self, name: &str, body: impl FnOnce(&mut Self)) {
        self.start(name);
        body(self);
        self.end(name);
    }

    /// An element of the kind `xsi_type` names, holding what `body` writes, with `xsi`
    /// declared on it as AWS's samples declare it on an ACL grantee (13 §6.8).
    pub fn typed(&mut self, name: &str, xsi_type: &str, body: impl FnOnce(&mut Self)) {
        self.out.push('<');
        self.out.push_str(name);
        self.out.push_str(" xmlns:xsi=\"");
        self.out.push_str(XSI_NAMESPACE);
        self.out.push_str("\" xsi:type=\"");
        escape_attribute(&mut self.out, xsi_type);
        self.out.push_str("\">");
        body(self);
        self.end(name);
    }

    /// An element holding `text`.
    pub fn text(&mut self, name: &str, text: &str) {
        self.start(name);
        escape(&mut self.out, text);
        self.end(name);
    }

    /// Text in the element being written, as GetBucketLocation's root holds its region.
    pub fn content(&mut self, text: &str) {
        escape(&mut self.out, text);
    }

    fn start(&mut self, name: &str) {
        self.out.push('<');
        self.out.push_str(name);
        self.out.push('>');
    }

    fn end(&mut self, name: &str) {
        self.out.push_str("</");
        self.out.push_str(name);
        self.out.push('>');
    }
}

/// `text` as a double-quoted attribute value: `&`, `<` and `"` escaped as XML 1.0 [10]
/// requires, and white space as character references, which a reader would otherwise
/// normalize to spaces (§3.3.3).
fn escape_attribute(out: &mut String, text: &str) {
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '"' => out.push_str("&quot;"),
            '\t' => out.push_str("&#x9;"),
            '\n' => out.push_str("&#xa;"),
            '\r' => out.push_str("&#xd;"),
            c if is_char(c) => out.push(c),
            c => out.push_str(&format!("&#x{:x};", u32::from(c))),
        }
    }
}

/// `text` as element content: `&` and `<` escaped as XML 1.0 §2.4 requires, `>` so that `]]>`
/// cannot form, and as character references a carriage return, which a reader would turn into
/// a line feed (§2.11), and each character XML 1.0 cannot carry, as S3 writes them (13 §7).
fn escape(out: &mut String, text: &str) {
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '\r' => out.push_str("&#xd;"),
            c if is_char(c) => out.push(c),
            c => out.push_str(&format!("&#x{:x};", u32::from(c))),
        }
    }
}

/// A request document with the white space between its elements dropped as it is read, so a
/// size limit bounds what is kept rather than the white space around it
/// (docs/design/s3-protocol.md §2).
///
/// S3's request documents give each element either elements or text, never both (13 §6), so
/// a run of white space after an end tag, an empty-element tag or the XML declaration, or
/// before a start tag, lies between elements and carries nothing, and is dropped; a run inside
/// an element that holds text, a key of one space, is kept. Inside a tag, a run of white space
/// outside an attribute's value is kept as one space. Comments, processing instructions and
/// CDATA sections are kept as they are. A run that cannot yet be placed, which only the byte
/// after it decides, is counted, and its bytes kept only while they fit the limit, so white
/// space costs the reader time but no memory past the limit.
pub struct Compact {
    kept: Vec<u8>,
    /// Bytes kept, and whether they are stored or only counted.
    len: usize,
    store: bool,
    limit: usize,
    state: Markup,
    /// A run of white space not yet placed: its bytes, while they fit, and its length.
    run: Vec<u8>,
    run_len: usize,
    /// White space here lies between elements.
    between: bool,
    /// No markup has been read yet.
    start: bool,
}

#[derive(Debug, Clone, Copy)]
enum Markup {
    Text,
    /// Just read `<`: the byte after it says what it opens.
    Open,
    /// Just read `<!`.
    Bang,
    /// A start or end tag, up to its `>`: the quote of the value being read, whether the last
    /// byte kept was a space, whether it is an end tag, and its last byte that is not space.
    Tag {
        quote: Option<u8>,
        space: bool,
        end: bool,
        last: u8,
    },
    /// Kept as it is until `end`, of which `matched` bytes have been read; then white space
    /// lies between elements if `between`.
    Until {
        end: &'static [u8],
        matched: usize,
        between: bool,
    },
}

impl Compact {
    /// A reader that keeps at most `limit` bytes of the document.
    pub fn new(limit: usize) -> Self {
        Self {
            kept: Vec::new(),
            len: 0,
            store: true,
            limit,
            state: Markup::Text,
            run: Vec::new(),
            run_len: 0,
            between: false,
            start: true,
        }
    }

    /// A reader that only counts what it would keep, refusing past `limit`.
    pub fn counting(limit: usize) -> Self {
        Self {
            store: false,
            ..Self::new(limit)
        }
    }

    /// Reads the next bytes of the document.
    pub fn push(&mut self, bytes: &[u8]) -> Result<(), XmlError> {
        for &b in bytes {
            self.byte(b)?;
        }
        Ok(())
    }

    /// The document as kept.
    pub fn finish(mut self) -> Result<Vec<u8>, XmlError> {
        // A run the document ends in is kept, for the reader to judge.
        self.flush_run()?;
        Ok(self.kept)
    }

    fn keep(&mut self, bytes: &[u8]) -> Result<(), XmlError> {
        let len = self.len.saturating_add(bytes.len());
        if len > self.limit {
            return Err(XmlError::TooLarge { limit: self.limit });
        }
        self.len = len;
        if self.store {
            self.kept.extend_from_slice(bytes);
        }
        Ok(())
    }

    /// Keeps the run of white space before this byte: it is data. Nothing is kept past the
    /// run's start while it runs, so its bytes were all stored if it fits.
    fn flush_run(&mut self) -> Result<(), XmlError> {
        let len = self.len.saturating_add(self.run_len);
        if len > self.limit {
            return Err(XmlError::TooLarge { limit: self.limit });
        }
        self.len = len;
        let run = std::mem::take(&mut self.run);
        self.run_len = 0;
        if self.store {
            self.kept.extend_from_slice(&run);
        }
        Ok(())
    }

    fn drop_run(&mut self) {
        self.run.clear();
        self.run_len = 0;
    }

    fn byte(&mut self, b: u8) -> Result<(), XmlError> {
        let space = matches!(b, b' ' | b'\t' | b'\r' | b'\n');
        match self.state {
            Markup::Text => {
                if b == b'<' {
                    self.state = Markup::Open;
                } else if space {
                    if !self.between {
                        self.run_len = self.run_len.saturating_add(1);
                        // Its bytes are held only while they could still fit.
                        if self.store && self.len.saturating_add(self.run_len) <= self.limit {
                            self.run.push(b);
                        }
                    }
                } else {
                    self.flush_run()?;
                    self.keep(&[b])?;
                    self.between = false;
                }
            }
            Markup::Open => {
                let first = self.start;
                self.start = false;
                match b {
                    b'/' => {
                        self.flush_run()?;
                        self.keep(b"</")?;
                        self.state = Markup::Tag {
                            quote: None,
                            space: false,
                            end: true,
                            last: b'/',
                        };
                    }
                    b'?' => {
                        self.flush_run()?;
                        self.keep(b"<?")?;
                        // After the XML declaration, white space precedes the root.
                        self.state = Markup::Until {
                            end: b"?>",
                            matched: 0,
                            between: first,
                        };
                    }
                    b'!' => {
                        self.flush_run()?;
                        self.keep(b"<!")?;
                        self.state = Markup::Bang;
                    }
                    _ => {
                        // A start tag: the run before it lies between elements.
                        self.drop_run();
                        self.keep(&[b'<', b])?;
                        self.state = Markup::Tag {
                            quote: None,
                            space: false,
                            end: false,
                            last: b,
                        };
                    }
                }
            }
            Markup::Bang => {
                self.keep(&[b])?;
                let end: &'static [u8] = match b {
                    b'-' => b"-->",
                    b'[' => b"]]>",
                    _ => b">",
                };
                // "<!-" has read the comment's first dash of its opening.
                self.state = Markup::Until {
                    end,
                    matched: 0,
                    between: false,
                };
                if b == b'>' {
                    self.state = Markup::Text;
                }
            }
            Markup::Tag {
                quote,
                space: was_space,
                end,
                last,
            } => {
                if let Some(q) = quote {
                    self.keep(&[b])?;
                    self.state = Markup::Tag {
                        quote: if b == q { None } else { Some(q) },
                        space: false,
                        end,
                        last: b,
                    };
                } else if space {
                    if !was_space {
                        self.keep(b" ")?;
                    }
                    self.state = Markup::Tag {
                        quote: None,
                        space: true,
                        end,
                        last,
                    };
                } else if b == b'>' {
                    self.keep(b">")?;
                    // After an end tag or an empty-element tag, white space lies between
                    // elements; after a start tag it may be an element's text.
                    self.between = end || last == b'/';
                    self.state = Markup::Text;
                } else {
                    self.keep(&[b])?;
                    let quote = if b == b'"' || b == b'\'' {
                        Some(b)
                    } else {
                        None
                    };
                    self.state = Markup::Tag {
                        quote,
                        space: false,
                        end,
                        last: b,
                    };
                }
            }
            Markup::Until {
                end,
                matched,
                between,
            } => {
                self.keep(&[b])?;
                let expected = end.get(matched).copied();
                let matched = if expected == Some(b) {
                    matched.saturating_add(1)
                } else if matched == 2 && end.first() == end.get(1) && end.first() == Some(&b) {
                    // "--->" and "]]]>": the last two of the repeated byte still count.
                    2
                } else if end.first() == Some(&b) {
                    1
                } else {
                    0
                };
                if matched == end.len() {
                    self.between = between;
                    self.state = Markup::Text;
                } else {
                    self.state = Markup::Until {
                        end,
                        matched,
                        between,
                    };
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A document's elements: a leaf holds text, a branch holds elements.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Node {
        Leaf(String, String),
        Branch(String, Vec<Node>),
    }

    fn read_node(reader: &mut Reader<'_>, name: &str, shape: &Node) -> Result<Node, XmlError> {
        match shape {
            Node::Leaf(..) => Ok(Node::Leaf(name.into(), reader.text()?.into_owned())),
            Node::Branch(_, children) => {
                let mut read = Vec::new();
                let mut shapes = children.iter();
                while let Some(child) = reader.child()? {
                    let shape = shapes.next().ok_or(XmlError::Schema("extra child"))?;
                    let (Node::Leaf(want, _) | Node::Branch(want, _)) = shape;
                    if child != want {
                        return Err(XmlError::Schema("unexpected child"));
                    }
                    read.push(read_node(reader, child, shape)?);
                }
                Ok(Node::Branch(name.into(), read))
            }
        }
    }

    fn read(body: &str, shape: &Node) -> Result<Node, XmlError> {
        let root = match shape {
            Node::Leaf(name, _) | Node::Branch(name, _) => name.as_str(),
        };
        let mut reader = Reader::open(body.as_bytes(), usize::MAX, root)?;
        let node = read_node(&mut reader, root, shape)?;
        reader.finish()?;
        Ok(node)
    }

    fn leaf(name: &str, text: &str) -> Node {
        Node::Leaf(name.into(), text.into())
    }

    fn branch(name: &str, children: Vec<Node>) -> Node {
        Node::Branch(name.into(), children)
    }

    fn delete(key: &str) -> Node {
        branch("Delete", vec![branch("Object", vec![leaf("Key", key)])])
    }

    #[test]
    fn documents_read_as_xml_1_0_says() {
        let want = delete("a&b<c>\"d'\n\re\u{1}");
        let body = "\u{FEFF}<?xml version='1.0' encoding=\"utf-8\" standalone='yes' ?>\r\n\
            <!-- a comment --><?pi data?>\n\
            <Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Object >\
            <Key>a&amp;b&lt;<![CDATA[c>]]>&quot;d&apos;\r\n&#13;e&#x1;</Key>\n  </Object\n>\
            </Delete >\n<!-- trailing -->\n";
        assert_eq!(read(body, &want), Ok(want.clone()));
        // No declaration, no namespace, a prefixed S3 namespace, and an empty element.
        assert_eq!(
            read(
                "<Delete><Object><Key>k</Key></Object></Delete>",
                &delete("k")
            ),
            Ok(delete("k"))
        );
        let prefixed = "<s3:Delete xmlns:s3='http://s3.amazonaws.com/doc/2006-03-01/'>\
            <s3:Object><s3:Key>k</s3:Key></s3:Object></s3:Delete>";
        assert_eq!(read(prefixed, &delete("k")), Ok(delete("k")));
        assert_eq!(
            read("<Delete><Object><Key/></Object></Delete>", &delete("")),
            Ok(delete(""))
        );
        let empty_root = branch("VersioningConfiguration", vec![]);
        assert_eq!(
            read("<VersioningConfiguration/>", &empty_root),
            Ok(empty_root)
        );
    }

    #[test]
    fn text_borrows_the_body_when_it_can() {
        let body = b"<Key>plain</Key>";
        let mut reader = Reader::open(body, usize::MAX, "Key").unwrap();
        assert!(matches!(reader.text(), Ok(Cow::Borrowed("plain"))));
        reader.finish().unwrap();
    }

    #[test]
    fn what_xml_1_0_forbids_is_refused() {
        let shape = delete("k");
        for body in [
            "",
            "   ",
            "<Delete><Object><Key>k</Key></Object></Delete><Delete/>",
            "<Delete><Object><Key>k</Object></Key></Delete>",
            "<Delete><Object><Key>k</Key></Object>",
            "<Delete><Object><Key>]]></Key></Object></Delete>",
            "<Delete><Object><Key>&bogus;</Key></Object></Delete>",
            "<Delete><Object><Key>&#0;</Key></Object></Delete>",
            "<Delete><Object><Key>&#xD800;</Key></Object></Delete>",
            "<Delete><Object><Key>&#xFFFE;</Key></Object></Delete>",
            "<Delete><Object><Key>&#+65;</Key></Object></Delete>",
            "<Delete><Object><Key>&#x;</Key></Object></Delete>",
            "<Delete><Object><Key>&#99999999999;</Key></Object></Delete>",
            "<Delete><Object><Key>&amp</Key></Object></Delete>",
            "<Delete><Object><Key>\u{1}</Key></Object></Delete>",
            "<Delete><Object><Key>\u{FFFF}</Key></Object></Delete>",
            "<Delete><!-- a -- b --><Object><Key>k</Key></Object></Delete>",
            "<Delete><!-- a ---><Object><Key>k</Key></Object></Delete>",
            "<Delete><?xml nope?><Object><Key>k</Key></Object></Delete>",
            "<Delete><?a:b c?><Object><Key>k</Key></Object></Delete>",
            "<?xml version='2.0'?><Delete/>",
            "<?xml version='1.0' encoding='ISO-8859-1'?><Delete/>",
            "<?xml version='1.0' standalone='maybe'?><Delete/>",
            " <?xml version='1.0'?><Delete/>",
            "<!DOCTYPE Delete [<!ENTITY a 'b'>]><Delete/>",
            "<Delete a='1' a='2'/>",
            "<Delete xmlns:x='u' xmlns:x='v'/>",
            "<Delete xmlns='a<b'/>",
            "<Delete xmlns='x'xmlns:y='z'/>",
            "<Delete xmlns:p=''/>",
            "<Delete xmlns:xmlns='u'/>",
            "<Delete xmlns:xml='u'/>",
            "<Delete xmlns:p='http://www.w3.org/XML/1998/namespace'/>",
            "<p:Delete/>",
            "<xmlns:Delete/>",
            "<:Delete/>",
            "<Delete:/>",
            "<a:b:Delete/>",
            "<1Delete/>",
            "<Delete><Object><Key>k</Key></Object></Delete>trailing",
            "<Delete><Object><Key>k</Key></Object></Delete",
            "<Delete><Object><Key>k</Key></Object></Delete>\u{0}",
        ] {
            assert!(read(body, &shape).is_err(), "accepted {body:?}");
        }
        assert_eq!(
            Reader::open(b"\xFF<Delete/>", usize::MAX, "Delete").err(),
            Some(XmlError::NotWellFormed("not UTF-8"))
        );
    }

    #[test]
    fn what_s3_documents_do_not_hold_is_refused() {
        let shape = delete("k");
        for body in [
            "<Other/>",
            "<Delete>text<Object><Key>k</Key></Object></Delete>",
            "<Delete><![CDATA[x]]><Object><Key>k</Key></Object></Delete>",
            "<Delete><Object><Key>k<b/></Key></Object></Delete>",
            "<Delete xmlns='urn:other'><Object><Key>k</Key></Object></Delete>",
            "<Delete id='1'><Object><Key>k</Key></Object></Delete>",
        ] {
            assert!(
                matches!(read(body, &shape), Err(XmlError::Schema(_))),
                "{body:?}: {:?}",
                read(body, &shape)
            );
        }
        // Declarations are read however many a tag carries, and one given twice among them is
        // found.
        let many: String = (0..1000).map(|i| format!(" xmlns:p{i}='u'")).collect();
        let tag = format!("<Delete{many}><Object><Key>k</Key></Object></Delete>");
        assert!(read(&tag, &shape).is_ok());
        let twice = format!("<Delete{many} xmlns:p500='v'><Object><Key>k</Key></Object></Delete>");
        assert_eq!(
            read(&twice, &shape).map(|_| ()),
            Err(XmlError::NotWellFormed("an attribute given twice"))
        );
    }

    /// `xsi:type` on an ACL grantee (13 §6.8), in the forms Namespaces in XML allows, and only
    /// where a document admits it.
    #[test]
    fn xsi_type_is_read_where_admitted() {
        let grantee = |attributes: &str| {
            format!(
                "<AccessControlPolicy xmlns='http://s3.amazonaws.com/doc/2006-03-01/'>\
                 <Grantee {attributes}><ID>i</ID></Grantee></AccessControlPolicy>"
            )
        };
        let read_type = |body: &str| -> Result<Option<String>, XmlError> {
            let mut r = Reader::open(body.as_bytes(), 1 << 12, "AccessControlPolicy")?
                .admit_types("Grantee");
            assert_eq!(r.child()?, Some("Grantee"));
            let kind = r.xsi_type().map(str::to_owned);
            assert_eq!(r.child()?, Some("ID"));
            assert_eq!(r.xsi_type(), None, "a child carries its parent's type");
            r.text()?;
            assert_eq!(r.child()?, None);
            assert_eq!(r.child()?, None);
            r.finish()?;
            Ok(kind)
        };
        let xsi = "xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'";
        // AWS's sample form, another prefix, and a value prefixed with S3's namespace.
        assert_eq!(
            read_type(&grantee(&format!("{xsi} xsi:type='CanonicalUser'"))),
            Ok(Some("CanonicalUser".into()))
        );
        assert_eq!(
            read_type(&grantee(
                "xmlns:i='http://www.w3.org/2001/XMLSchema-instance' i:type=' Group '"
            )),
            Ok(Some("Group".into()))
        );
        assert_eq!(
            read_type(&grantee(&format!(
                "{xsi} xmlns:s3='http://s3.amazonaws.com/doc/2006-03-01/' xsi:type='s3:Group'"
            ))),
            Ok(Some("Group".into()))
        );
        assert_eq!(read_type(&grantee("")), Ok(None));
        // Refused: another attribute, an unprefixed `type`, a value in another namespace or
        // not a name, one type given twice under two prefixes, and an undeclared prefix.
        for refused in [
            format!("{xsi} xsi:nil='true'"),
            "type='CanonicalUser'".to_string(),
            format!("{xsi} xmlns:o='urn:other' xsi:type='o:CanonicalUser'"),
            format!("{xsi} xsi:type='Canonical User'"),
            format!(
                "{xsi} xmlns:i='http://www.w3.org/2001/XMLSchema-instance' \
                 xsi:type='Group' i:type='Group'"
            ),
            "xsi:type='CanonicalUser'".to_string(),
        ] {
            assert!(read_type(&grantee(&refused)).is_err(), "{refused}");
        }
        // Nor is a type read on an element other than the one admitted.
        let on_id = format!(
            "<AccessControlPolicy><Grantee><ID {xsi} xsi:type='CanonicalUser'>i</ID>\
             </Grantee></AccessControlPolicy>"
        );
        assert!(read_type(&on_id).is_err());
        // A document that does not admit types refuses one.
        let body = grantee(&format!("{xsi} xsi:type='CanonicalUser'"));
        let mut r = Reader::open(body.as_bytes(), 1 << 12, "AccessControlPolicy").unwrap();
        assert!(r.child().is_err());
    }

    /// What the writer writes as a typed element, the reader reads back.
    #[test]
    fn a_typed_element_reads_back() {
        let doc = Writer::document("AccessControlPolicy", true, |w| {
            w.typed("Grantee", "CanonicalUser", |w| w.text("ID", "a\"b"))
        });
        assert!(doc.contains(
            "<Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \
             xsi:type=\"CanonicalUser\"><ID>a\"b</ID></Grantee>"
        ));
        let mut r = Reader::open(doc.as_bytes(), 1 << 12, "AccessControlPolicy")
            .unwrap()
            .admit_types("Grantee");
        assert_eq!(r.child(), Ok(Some("Grantee")));
        assert_eq!(r.xsi_type(), Some("CanonicalUser"));
        assert_eq!(r.child(), Ok(Some("ID")));
        assert_eq!(r.text().unwrap(), "a\"b");
    }

    #[test]
    fn a_body_over_its_limit_is_refused_before_it_is_read() {
        assert_eq!(
            Reader::open(b"<Delete/>", 8, "Delete").err(),
            Some(XmlError::TooLarge { limit: 8 })
        );
        assert_eq!(
            XmlError::TooLarge { limit: 8 }.code(),
            ("MaxMessageLengthExceeded", 400)
        );
        assert_eq!(XmlError::Schema("x").code(), ("MalformedXML", 400));
    }

    #[test]
    fn attribute_values_are_normalized() {
        // A namespace declared through references and white space still names S3's.
        let body = "<Delete xmlns='http://s3.amazonaws.com/doc/2006&#x2D;03-01/'>\
            <Object><Key>k</Key></Object></Delete>";
        assert_eq!(read(body, &delete("k")), Ok(delete("k")));
        let body = "<Delete xmlns='http://s3.amazonaws.com/doc/2006-03-01/\r\n'/>";
        assert!(matches!(
            read(body, &branch("Delete", vec![])),
            Err(XmlError::Schema(_))
        ));
    }

    #[test]
    fn the_writer_escapes_what_a_reader_would_change() {
        let doc = Writer::document("Error", false, |w| {
            w.text("Code", "NoSuchKey");
            w.element("Detail", |w| w.text("Key", "a&b<c>]]>\r\n\t\u{1}\u{FFFE}é"));
        });
        assert_eq!(
            doc,
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>NoSuchKey</Code>\
             <Detail><Key>a&amp;b&lt;c&gt;]]&gt;&#xd;\n\t&#x1;&#xfffe;é</Key></Detail></Error>"
        );
        let doc = Writer::document("Delete", true, |w| {
            w.element("Object", |w| w.text("Key", "k\r\u{1}é&"))
        });
        assert!(doc.contains("<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"));
        // What the writer writes, the reader reads back exactly.
        assert_eq!(read(&doc, &delete("k\r\u{1}é&")), Ok(delete("k\r\u{1}é&")));
    }

    /// Serialization choices drawn from a seed, so each generated document is one of the many
    /// ways XML can write the same elements.
    struct Choices(u64);

    impl Choices {
        fn pick(&mut self, n: usize) -> usize {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            usize::try_from(self.0 % n as u64).unwrap()
        }
    }

    fn serialize(root: &Node, seed: u64) -> String {
        let mut c = Choices(seed | 1);
        let mut out = String::new();
        if c.pick(3) == 0 {
            out.push('\u{FEFF}');
        }
        if c.pick(2) == 0 {
            let q = if c.pick(2) == 0 { '"' } else { '\'' };
            out.push_str(&format!("<?xml version={q}1.0{q}"));
            if c.pick(2) == 0 {
                let name = ["UTF-8", "utf-8", "Utf-8"][c.pick(3)];
                out.push_str(&format!(" encoding = {q}{name}{q}"));
            }
            if c.pick(2) == 0 {
                out.push_str(&format!(" standalone={q}{}{q}", ["yes", "no"][c.pick(2)]));
            }
            out.push_str([" ?>", "?>"][c.pick(2)]);
        }
        misc(&mut out, &mut c);
        let (prefix, declaration) = match c.pick(4) {
            0 => ("", String::new()),
            1 => ("", format!(" xmlns=\"{NAMESPACE}\"")),
            2 => ("s3:", format!(" xmlns:s3='{NAMESPACE}'")),
            _ => (
                "",
                " xmlns='http://s3.amazonaws.com/doc/2006&#x2D;03&#45;01/'".to_string(),
            ),
        };
        element(&mut out, root, &mut c, prefix, &declaration);
        misc(&mut out, &mut c);
        out
    }

    fn misc(out: &mut String, c: &mut Choices) {
        for _ in 0..c.pick(3) {
            out.push_str(
                [
                    " ",
                    "\n",
                    "\r\n",
                    "\t",
                    "<!-- c -->",
                    "<!---->",
                    "<!-- - -->",
                    "<?pi?>",
                    "<?pi a?b ?>",
                ][c.pick(9)],
            );
        }
    }

    fn element(out: &mut String, node: &Node, c: &mut Choices, prefix: &str, declaration: &str) {
        let (Node::Leaf(name, _) | Node::Branch(name, _)) = node;
        let empty = match node {
            Node::Leaf(_, text) => text.is_empty(),
            Node::Branch(_, children) => children.is_empty(),
        };
        out.push_str(&format!("<{prefix}{name}{declaration}"));
        out.push_str(["", " ", "\n"][c.pick(3)]);
        if empty && c.pick(2) == 0 {
            out.push_str("/>");
            return;
        }
        out.push('>');
        match node {
            Node::Leaf(_, text) => {
                for ch in text.chars() {
                    if c.pick(8) == 0 {
                        out.push_str(["<!--x-->", "<?p?>"][c.pick(2)]);
                    }
                    write_char(out, ch, c);
                }
            }
            Node::Branch(_, children) => {
                for child in children {
                    misc(out, c);
                    element(out, child, c, prefix, "");
                }
                misc(out, c);
            }
        }
        out.push_str(&format!(
            "</{prefix}{name}{}>",
            ["", " ", "\r\n"][c.pick(3)]
        ));
    }

    fn write_char(out: &mut String, ch: char, c: &mut Choices) {
        let code = u32::from(ch);
        match c.pick(5) {
            0 => out.push_str(&format!("&#{code};")),
            1 => out.push_str(&format!("&#x{}{code:X};", "0".repeat(c.pick(3)))),
            2 if ch != '\r' => out.push_str(&format!("<![CDATA[{ch}]]>")),
            _ => match ch {
                '&' => out.push_str("&amp;"),
                '<' => out.push_str("&lt;"),
                '\r' => out.push_str("&#xD;"),
                '>' if out.ends_with("]]") => out.push_str("&gt;"),
                '"' if c.pick(2) == 0 => out.push_str("&quot;"),
                '\'' if c.pick(2) == 0 => out.push_str("&apos;"),
                _ => out.push(ch),
            },
        }
    }

    /// roxmltree's reading of the same document, as the oracle.
    fn oracle(body: &str, shape: &Node) -> Option<Node> {
        fn extract(node: roxmltree::Node<'_, '_>, shape: &Node) -> Node {
            let name = node.tag_name().name().to_string();
            match shape {
                Node::Leaf(..) => Node::Leaf(
                    name,
                    node.children()
                        .filter(|n| n.is_text())
                        .filter_map(|n| n.text())
                        .collect(),
                ),
                Node::Branch(_, shapes) => Node::Branch(
                    name,
                    node.children()
                        .filter(|n| n.is_element())
                        .zip(shapes)
                        .map(|(n, s)| extract(n, s))
                        .collect(),
                ),
            }
        }
        let options = roxmltree::ParsingOptions {
            allow_dtd: false,
            ..roxmltree::ParsingOptions::default()
        };
        let doc = roxmltree::Document::parse_with_options(body, options).ok()?;
        Some(extract(doc.root_element(), shape))
    }

    /// Whether a character reference in `body` names a character XML 1.0 forbids and mantle
    /// accepts (the XML 1.1 difference in the module comment).
    fn refers_to_restricted(body: &str) -> bool {
        body.match_indices("&#").any(|(at, _)| {
            let digits: String = body
                .get(at + 2..)
                .unwrap()
                .chars()
                .take_while(|c| *c != ';')
                .collect();
            let code = match digits.strip_prefix('x') {
                Some(hex) => u32::from_str_radix(hex, 16),
                None => digits.parse(),
            };
            code.ok()
                .and_then(char::from_u32)
                .is_some_and(|c| is_referable(c) && !is_char(c))
        })
    }

    fn name_strategy() -> impl Strategy<Value = String> {
        prop::sample::select(vec![
            "Key", "Object", "Part", "ETag", "É", "a-b.c_d", "x\u{B7}y", "中",
        ])
        .prop_map(String::from)
    }

    fn text_strategy() -> impl Strategy<Value = String> {
        let special =
            prop::sample::select(vec!['&', '<', '>', ']', '\r', '\n', '\t', '"', '\'', ' ']);
        let any_char = any::<char>().prop_filter("an XML 1.0 character", |c| is_char(*c));
        prop::collection::vec(prop_oneof![special, any_char], 0..10)
            .prop_map(|chars| chars.into_iter().collect())
    }

    fn node_strategy() -> impl Strategy<Value = Node> {
        let leaf = (name_strategy(), text_strategy()).prop_map(|(n, t)| Node::Leaf(n, t));
        leaf.prop_recursive(3, 24, 4, |inner| {
            (name_strategy(), prop::collection::vec(inner, 0..4))
                .prop_map(|(n, children)| Node::Branch(n, children))
        })
    }

    use proptest::prelude::*;

    proptest! {
        /// Every way of writing the same elements reads as roxmltree reads it.
        #[test]
        fn documents_read_as_roxmltree_reads_them(shape in node_strategy(), seed in any::<u64>()) {
            let body = serialize(&shape, seed);
            prop_assert_eq!(read(&body, &shape), Ok(shape.clone()), "{:?}", body);
            prop_assert_eq!(oracle(&body, &shape), Some(shape), "{:?}", body);
        }

        /// What roxmltree refuses, mantle refuses.
        #[test]
        fn mutations_roxmltree_refuses_are_refused(
            shape in node_strategy(),
            seed in any::<u64>(),
            at in any::<prop::sample::Index>(),
            len in 0usize..4,
            insert in prop::sample::select(vec!["", "<", ">", "&", ";", "]]>", "--", "/", "'", "\"", "=", ":", "?", "!", " ", "#", "x", "\u{1}"]),
        ) {
            let body = serialize(&shape, seed);
            let chars: Vec<char> = body.chars().collect();
            let start = at.index(chars.len() + 1);
            let end = (start + len).min(chars.len());
            let mutated: String = chars[..start]
                .iter()
                .chain(insert.chars().collect::<Vec<_>>().iter())
                .chain(chars[end..].iter())
                .collect();
            if oracle(&mutated, &shape).is_none() && !refers_to_restricted(&mutated) {
                prop_assert!(read(&mutated, &shape).is_err(), "accepted {:?}", mutated);
            }
        }

        /// What the writer writes, the reader reads back exactly, whatever the text holds.
        #[test]
        fn written_text_reads_back(text in any::<String>().prop_filter("no NUL", |t| !t.contains('\0'))) {
            let body = Writer::document("Key", true, |_| {});
            prop_assert!(body.ends_with("</Key>"));
            let doc = Writer::document("Delete", false, |w| w.element("Object", |w| w.text("Key", &text)));
            prop_assert_eq!(read(&doc, &delete(&text)), Ok(delete(&text)));
        }
    }
}
