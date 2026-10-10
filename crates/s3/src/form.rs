//! `multipart/form-data` bodies (RFC 7578; RFC 2046 §5.1) as S3's POST Object reads them
//! (docs/research/19 §2, §5): the fields before the file are kept, the file's content streams
//! out as it arrives, and whatever follows the file is ignored, as S3 ignores it.
//!
//! The decoder holds at most the fields before the file, which S3 bounds (`MAX_PRE_DATA`), and
//! of the file only the few bytes that may begin the delimiter ending it, so a file of any size
//! passes through in the memory of the pieces it arrives in.

use std::panic::{AssertUnwindSafe, catch_unwind};

use memchr::memmem::Finder;

/// "The form data and boundaries (excluding the contents of the file) cannot exceed 20KB"
/// (19 §2.1). AWS does not say whether a KB is 1,000 bytes or 1,024 (19 §11); the larger
/// reading refuses no form S3 accepts.
pub const MAX_PRE_DATA: usize = 20 * 1024;

/// A boundary "must be no longer than 70 characters, not counting the two leading hyphens"
/// (RFC 2046 §5.1.1).
pub const MAX_BOUNDARY: usize = 70;

/// "The variable `${filename}` is automatically replaced with the name of the file provided by
/// the user and is recognized by all form fields" (19 §2.3).
const FILENAME: &str = "${filename}";

/// The body is read as if a CRLF came before it, so its first delimiter is found as every other
/// is: "the initial CRLF is considered to be attached to the boundary delimiter line" (RFC 2046
/// §5.1.1), and a body may begin with its first delimiter.
const CRLF: &[u8] = b"\r\n";

/// What the body holds before the file's content, with the CRLF assumed before it.
const LIMIT: usize = CRLF.len() + MAX_PRE_DATA;

/// Linear whitespace, which may stand between the tokens of a header field.
const WS: [char; 2] = [' ', '\t'];

/// Why a form body is refused, each with S3's message (19 §2.6, §8).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FormError {
    /// The request is not `multipart/form-data`: 412 with S3's `Condition` (19 §8.1).
    #[error("At least one of the pre-conditions you specified did not hold")]
    NotForm,
    /// The body breaks RFC 7578 or RFC 2046, for the reason given.
    #[error("The body of your POST request is not well-formed multipart/form-data.")]
    Malformed(&'static str),
    /// S3's message as it answers, "preceeding" and all (19 §13).
    #[error("Your POST request fields preceeding the upload file was too large.")]
    PreDataTooLong,
    /// The body closed with no part named `file`.
    #[error("POST requires exactly one file upload per request.")]
    NoFile,
    /// The substring search unwound.
    #[error("We encountered an internal error. Please try again.")]
    Internal,
}

impl FormError {
    /// The S3 error code and status, as S3 answered each (19 §9, §13), where its error table
    /// names others: `PreconditionFailed` for `RequestIsNotMultiPartContent`,
    /// `MaxPostPreDataLengthExceeded` without the table's `Error`, and `InvalidArgument` for a
    /// form with no file, never `IncorrectNumberOfFilesInPostRequest`.
    pub fn code(&self) -> (&'static str, u16) {
        match self {
            Self::NotForm => ("PreconditionFailed", 412),
            Self::Malformed(_) => ("MalformedPOSTRequest", 400),
            Self::PreDataTooLong => ("MaxPostPreDataLengthExceeded", 400),
            Self::NoFile => ("InvalidArgument", 400),
            Self::Internal => ("InternalError", 500),
        }
    }

    /// The elements S3's error carries after its message (19 §8.1, §13).
    pub fn details(&self) -> &'static [(&'static str, &'static str)] {
        match self {
            Self::NotForm => &[(
                "Condition",
                "Bucket POST must be of the enclosure-type multipart/form-data",
            )],
            Self::PreDataTooLong => &[("MaxPostPreDataLengthBytes", "20480")],
            Self::NoFile => &[("ArgumentName", "file"), ("ArgumentValue", "0")],
            _ => &[],
        }
    }
}

/// The fields a form sent before its file.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Form {
    /// Names as sent. A name sent twice holds its values joined by a comma: "If you have
    /// multiple fields with the same name, the values must be separated by commas" (19 §2.4).
    fields: Vec<(String, String)>,
    /// The file part's `filename`, as sent, if it had one.
    filename: Option<String>,
}

impl Form {
    /// The field `name`, in any case: "The post parameters are case insensitive" (19 §2.4).
    pub fn get(&self, name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(field, _)| field.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    /// Every field, in the order first sent.
    pub fn fields(&self) -> &[(String, String)] {
        &self.fields
    }

    /// The name the file's part gave it, as sent.
    pub fn filename(&self) -> Option<&str> {
        self.filename.as_deref()
    }

    fn add(&mut self, name: String, value: &str) {
        match self
            .fields
            .iter_mut()
            .find(|(field, _)| field.eq_ignore_ascii_case(&name))
        {
            Some((_, held)) => {
                held.push(',');
                held.push_str(value);
            }
            None => self.fields.push((name, value.to_owned())),
        }
    }

    /// Replaces `${filename}` in every field with the file's name after its last `/` or `\`:
    /// "only the text following the last slash (/) or backslash (\) is used" (19 §2.3). A file
    /// part without a `filename` leaves the variable as written, as S3 was recorded doing, where
    /// AWS's text says it becomes empty (19 §8.1, §10 item 4). S3 checks a policy against the
    /// fields expanded (19 §4.4), so the expanded fields are held to the bound on the fields as
    /// sent, before any is built.
    fn expand(&mut self) -> Result<(), FormError> {
        let Some(filename) = &self.filename else {
            return Ok(());
        };
        let name = filename.rsplit(['/', '\\']).next().unwrap_or(filename);
        let mut size: usize = 0;
        for (field, value) in &self.fields {
            let uses = value.matches(FILENAME).count();
            size = uses
                .checked_mul(FILENAME.len())
                .and_then(|variables| value.len().checked_sub(variables))
                .and_then(|kept| kept.checked_add(uses.checked_mul(name.len())?))
                .and_then(|expanded| expanded.checked_add(field.len()))
                .and_then(|field| size.checked_add(field))
                .ok_or(FormError::PreDataTooLong)?;
        }
        if size > MAX_PRE_DATA {
            return Err(FormError::PreDataTooLong);
        }
        for (_, value) in &mut self.fields {
            if value.contains(FILENAME) {
                *value = value.replace(FILENAME, name);
            }
        }
        Ok(())
    }
}

/// Decodes one form body, fed as it arrives.
pub struct Decoder {
    /// CRLF, two hyphens and the boundary: the delimiter that begins every part and ends the
    /// file (RFC 2046 §5.1.1).
    delimiter: Vec<u8>,
    finder: Finder<'static>,
    /// The empty line that ends a part's header fields.
    blank: Finder<'static>,
    form: Form,
    phase: Phase,
}

enum Phase {
    /// The form before its file.
    Fields(Fields),
    /// The file, and what follows it.
    File(File),
}

/// The form before its file, as far as it has arrived.
struct Fields {
    /// What has arrived, after the assumed CRLF.
    buf: Vec<u8>,
    /// How far `buf` has been read.
    at: usize,
    /// Where the search for what ends the current step resumes: no match begins before it.
    from: usize,
    step: Step,
}

enum Step {
    /// Before the first delimiter: the preamble, which "implementations must ignore" (RFC 2046
    /// §5.1.1).
    Preamble,
    /// The rest of a delimiter's line.
    Tail(Tail),
    /// A part's header fields, up to the empty line that ends them.
    Headers,
    /// A field's value, which began at `start`.
    Value { name: String, start: usize },
}

/// The file's content and the rest of the body.
struct File {
    state: State,
    /// The bytes of the file passed out.
    length: u64,
}

enum State {
    /// The file's content. `held` is the end of what arrived that may begin the delimiter.
    Content { held: Vec<u8> },
    /// The rest of the line whose delimiter ended the file.
    Tail(Tail),
    /// Past the file: "Any fields below it are ignored" (19 §2.4).
    Rest,
}

/// Where a delimiter's line is after its boundary: `--` closes the body, and transport padding
/// then a CRLF begin the next part (RFC 2046 §5.1.1).
#[derive(Debug, Clone, Copy)]
enum Tail {
    Start,
    Dash,
    Padding,
    Cr,
}

/// A delimiter's line after one more byte.
enum Line {
    Open(Tail),
    /// `--`: the close delimiter, after which only the epilogue, ignored, follows.
    Close,
    /// A CRLF: a part follows.
    Part,
}

impl Tail {
    /// "The boundary may be followed by zero or more characters of linear whitespace. It is
    /// then terminated by either another CRLF ... or by two CRLFs" (RFC 2046 §5.1.1). Any other
    /// byte means the boundary appeared inside a part, which "the boundary delimiter MUST NOT";
    /// taking it for a delimiter would store the file cut short, so the body is refused.
    fn next(self, byte: u8) -> Result<Line, FormError> {
        match (self, byte) {
            (Self::Start, b'-') => Ok(Line::Open(Self::Dash)),
            (Self::Dash, b'-') => Ok(Line::Close),
            (Self::Start | Self::Padding, b' ' | b'\t') => Ok(Line::Open(Self::Padding)),
            (Self::Start | Self::Padding, b'\r') => Ok(Line::Open(Self::Cr)),
            (Self::Cr, b'\n') => Ok(Line::Part),
            _ => Err(FormError::Malformed(
                "a delimiter's line holds more than its boundary",
            )),
        }
    }
}

impl Decoder {
    /// A decoder for a body whose `Content-Type` is `content_type`: `multipart/form-data` with
    /// a boundary.
    pub fn new(content_type: Option<&str>) -> Result<Self, FormError> {
        let boundary = boundary(content_type)?;
        let mut delimiter = b"\r\n--".to_vec();
        delimiter.extend_from_slice(boundary.as_bytes());
        let finder = Finder::new(&delimiter).into_owned();
        let mut buf = Vec::new();
        buf.extend_from_slice(CRLF);
        Ok(Self {
            delimiter,
            finder,
            blank: Finder::new(b"\r\n\r\n").into_owned(),
            form: Form::default(),
            phase: Phase::Fields(Fields {
                buf,
                at: 0,
                from: 0,
                step: Step::Preamble,
            }),
        })
    }

    /// Consumes `input`, appending the file's bytes it carries to `out`. Once the file has
    /// begun, `form` gives the fields before it; a caller checks them before it keeps anything
    /// `out` holds.
    pub fn feed(&mut self, input: &[u8], out: &mut Vec<u8>) -> Result<(), FormError> {
        let Phase::Fields(fields) = &mut self.phase else {
            return self.file(input, out);
        };
        let room = LIMIT.saturating_sub(fields.buf.len()).min(input.len());
        let (head, rest) = input.split_at_checked(room).ok_or(FormError::Internal)?;
        fields.buf.extend_from_slice(head);
        let Some(start) = read(
            fields,
            &self.delimiter,
            &self.finder,
            &self.blank,
            &mut self.form,
        )?
        else {
            // The file's content begins past the bound, since the fields before it fill it.
            if fields.buf.len() >= LIMIT {
                return Err(FormError::PreDataTooLong);
            }
            return Ok(());
        };
        let buf = std::mem::take(&mut fields.buf);
        self.form.expand()?;
        self.phase = Phase::File(File {
            state: State::Content { held: Vec::new() },
            length: 0,
        });
        self.file(buf.get(start..).unwrap_or_default(), out)?;
        self.file(rest, out)
    }

    /// The fields before the file, once the file has begun.
    pub fn form(&self) -> Option<&Form> {
        match self.phase {
            Phase::Fields(_) => None,
            Phase::File(_) => Some(&self.form),
        }
    }

    /// Ends the body: the file ended with a delimiter. Returns the form and the file's length.
    pub fn finish(self) -> Result<(Form, u64), FormError> {
        match self.phase {
            Phase::Fields(Fields {
                step: Step::Preamble,
                ..
            }) => Err(FormError::Malformed("the body holds no delimiter")),
            Phase::Fields(_) => Err(FormError::Malformed("the body ends before its file")),
            Phase::File(File {
                state: State::Content { .. },
                ..
            }) => Err(FormError::Malformed("the body ends inside the file")),
            Phase::File(file) => Ok((self.form, file.length)),
        }
    }

    fn file(&mut self, mut input: &[u8], out: &mut Vec<u8>) -> Result<(), FormError> {
        let Phase::File(file) = &mut self.phase else {
            return Err(FormError::Internal);
        };
        let delimiter = self.delimiter.as_slice();
        loop {
            match &mut file.state {
                State::Rest => return Ok(()),
                State::Tail(tail) => {
                    let Some((&byte, rest)) = input.split_first() else {
                        return Ok(());
                    };
                    input = rest;
                    match tail.next(byte)? {
                        Line::Open(next) => *tail = next,
                        Line::Close | Line::Part => file.state = State::Rest,
                    }
                }
                State::Content { held } => {
                    if input.is_empty() {
                        return Ok(());
                    }
                    if !held.is_empty() {
                        // Whether the delimiter the last piece ended with goes on in this one.
                        let expected = delimiter.get(held.len()..).unwrap_or_default();
                        let compared = expected.len().min(input.len());
                        let (head, rest) = input
                            .split_at_checked(compared)
                            .ok_or(FormError::Internal)?;
                        if expected.starts_with(head) {
                            if compared == expected.len() {
                                held.clear();
                                file.state = State::Tail(Tail::Start);
                                input = rest;
                                continue;
                            }
                            held.extend_from_slice(head);
                            return Ok(());
                        }
                        emit(out, &mut file.length, held)?;
                        held.clear();
                    }
                    match find(&self.finder, input)? {
                        Some(at) => {
                            let (content, rest) =
                                input.split_at_checked(at).ok_or(FormError::Internal)?;
                            emit(out, &mut file.length, content)?;
                            input = rest.get(delimiter.len()..).unwrap_or_default();
                            file.state = State::Tail(Tail::Start);
                        }
                        None => {
                            // Only the input's last CR can begin a delimiter it does not hold
                            // whole: a delimiter's one CR is its first byte, since a boundary
                            // holds none.
                            let window = input
                                .len()
                                .saturating_sub(delimiter.len().saturating_sub(1));
                            let begins = input
                                .get(window..)
                                .and_then(|end| end.iter().rposition(|&b| b == b'\r'))
                                .and_then(|at| window.checked_add(at))
                                .unwrap_or(input.len());
                            let (content, end) =
                                input.split_at_checked(begins).ok_or(FormError::Internal)?;
                            if delimiter.starts_with(end) {
                                emit(out, &mut file.length, content)?;
                                held.extend_from_slice(end);
                            } else {
                                emit(out, &mut file.length, input)?;
                            }
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}

/// Passes `data` of the file out.
fn emit(out: &mut Vec<u8>, length: &mut u64, data: &[u8]) -> Result<(), FormError> {
    *length = u64::try_from(data.len())
        .ok()
        .and_then(|len| length.checked_add(len))
        .ok_or(FormError::Malformed("the file is longer than 2^64 bytes"))?;
    out.extend_from_slice(data);
    Ok(())
}

/// Reads the form before its file as far as it has arrived: `Some` with where in `fields.buf`
/// the file's content begins, `None` while more of the body is needed.
fn read(
    fields: &mut Fields,
    delimiter: &[u8],
    finder: &Finder<'_>,
    blank: &Finder<'_>,
    form: &mut Form,
) -> Result<Option<usize>, FormError> {
    loop {
        match &mut fields.step {
            Step::Preamble => {
                let Some(found) = search(finder, &fields.buf, &mut fields.from)? else {
                    return Ok(None);
                };
                fields.at = after(found, delimiter.len())?;
                fields.from = fields.at;
                fields.step = Step::Tail(Tail::Start);
            }
            Step::Tail(tail) => {
                let mut state = *tail;
                loop {
                    let Some(&byte) = fields.buf.get(fields.at) else {
                        *tail = state;
                        return Ok(None);
                    };
                    fields.at = after(fields.at, 1)?;
                    match state.next(byte)? {
                        Line::Open(next) => state = next,
                        Line::Close => return Err(FormError::NoFile),
                        Line::Part => break,
                    }
                }
                // The search for the empty line starts at the CRLF just read, so a part with
                // no header fields is found as one.
                fields.from = fields
                    .at
                    .checked_sub(CRLF.len())
                    .ok_or(FormError::Internal)?;
                fields.step = Step::Headers;
            }
            Step::Headers => {
                let Some(found) = search(blank, &fields.buf, &mut fields.from)? else {
                    return Ok(None);
                };
                let end = after(found, CRLF.len())?;
                let headers = fields
                    .buf
                    .get(fields.at..end)
                    .ok_or(FormError::Malformed("a part has no header fields"))?;
                if headers.is_empty() {
                    return Err(FormError::Malformed("a part has no header fields"));
                }
                let (name, filename, form_data) = part(headers)?;
                let start = after(end, CRLF.len())?;
                // "The file or text content must be the last field in the form" (19 §2.2): the
                // `form-data` part named `file`, in any case, whether or not it names a file,
                // since clients name one on every part (19 §5.5). S3 read a part of another
                // disposition, or of another name, as a field (19 §13).
                if form_data && name.eq_ignore_ascii_case("file") {
                    form.filename = filename;
                    return Ok(Some(start));
                }
                fields.at = start;
                fields.from = start;
                fields.step = Step::Value { name, start };
            }
            Step::Value { name, start } => {
                let Some(found) = search(finder, &fields.buf, &mut fields.from)? else {
                    return Ok(None);
                };
                let value = fields.buf.get(*start..found).unwrap_or_default();
                let value = std::str::from_utf8(value)
                    .map_err(|_| FormError::Malformed("a field's value is not UTF-8"))?;
                form.add(std::mem::take(name), value);
                fields.at = after(found, delimiter.len())?;
                fields.from = fields.at;
                fields.step = Step::Tail(Tail::Start);
            }
        }
    }
}

fn after(at: usize, length: usize) -> Result<usize, FormError> {
    at.checked_add(length).ok_or(FormError::Internal)
}

/// Where `finder`'s needle is in `buf`, looking from `from`. When it is absent, `from` moves to
/// the first place it could still begin once more of the body arrives, so no byte is searched
/// twice but the few that may begin it.
fn search(finder: &Finder<'_>, buf: &[u8], from: &mut usize) -> Result<Option<usize>, FormError> {
    let haystack = buf.get(*from..).unwrap_or_default();
    match find(finder, haystack)? {
        Some(at) => Ok(Some(after(*from, at)?)),
        None => {
            let needle = finder.needle().len();
            *from = (*from).max(buf.len().saturating_sub(needle.saturating_sub(1)));
            Ok(None)
        }
    }
}

/// memchr's substring search, SIMD over the Two-Way algorithm, linear in the haystack
/// (Crochemore and Perrin, "Two-way string-matching", J. ACM 38(3), 1991), behind an unwind
/// boundary as every dependency is called (CLAUDE.md §1).
fn find(finder: &Finder<'_>, haystack: &[u8]) -> Result<Option<usize>, FormError> {
    catch_unwind(AssertUnwindSafe(|| finder.find(haystack))).map_err(|_| FormError::Internal)
}

/// The boundary of a request whose `Content-Type` is `multipart/form-data` (RFC 7578 §4.1,
/// §8). The type's name is case-insensitive (RFC 9110 §8.3.1), as are its parameters' names
/// (RFC 2045 §5.1).
fn boundary(content_type: Option<&str>) -> Result<String, FormError> {
    let content_type = content_type.ok_or(FormError::NotForm)?;
    let end = content_type.find(';').unwrap_or(content_type.len());
    let (media, _) = content_type
        .split_at_checked(end)
        .ok_or(FormError::NotForm)?;
    if !media
        .trim_matches(WS)
        .eq_ignore_ascii_case("multipart/form-data")
    {
        return Err(FormError::NotForm);
    }
    let (_, parameters) = parameterized(content_type)?;
    let mut boundary = None;
    for (name, value) in parameters {
        if name.eq_ignore_ascii_case("boundary") && boundary.replace(value).is_some() {
            return Err(FormError::Malformed(
                "the Content-Type names two boundaries",
            ));
        }
    }
    let boundary = boundary.ok_or(FormError::Malformed("the Content-Type names no boundary"))?;
    // `boundary := 0*69<bchars> bcharsnospace` (RFC 2046 §5.1.1).
    let valid = (1..=MAX_BOUNDARY).contains(&boundary.len())
        && boundary
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"'()+_,-./:=? ".contains(&b))
        && !boundary.ends_with(' ');
    if !valid {
        return Err(FormError::Malformed(
            "the boundary breaks RFC 2046's grammar",
        ));
    }
    Ok(boundary)
}

/// A part's name, its file's name, and whether its disposition is `form-data`, from its header
/// fields (RFC 7578 §4.2): each part "MUST contain a Content-Disposition header field ... where
/// the disposition type is "form-data"" with a "name" parameter. S3 read a part of another
/// disposition as a field (19 §13), and so does mantle. Any other header field "MUST be
/// ignored" (§4.8), a part's `Content-Type` among them: an object's type is its `Content-Type`
/// field's, which a policy can cover, and never a part's. A field folded onto more lines is
/// unfolded (RFC 5322 §2.2.3, which RFC 2046's part headers follow).
fn part(headers: &[u8]) -> Result<(String, Option<String>, bool), FormError> {
    let text = std::str::from_utf8(headers)
        .map_err(|_| FormError::Malformed("a part's header fields are not UTF-8"))?;
    let text = text.strip_suffix("\r\n").unwrap_or(text);
    let mut unfolded: Vec<String> = Vec::new();
    for line in text.split("\r\n") {
        if line.bytes().any(|b| b.is_ascii_control() && b != b'\t') {
            return Err(FormError::Malformed(
                "a part's header field holds a control character",
            ));
        }
        if line.starts_with(WS) {
            unfolded
                .last_mut()
                .ok_or(FormError::Malformed("a part's header fields begin folded"))?
                .push_str(line);
        } else {
            unfolded.push(line.to_owned());
        }
    }
    let mut disposition = None;
    for line in &unfolded {
        let (name, value) = line
            .split_once(':')
            .ok_or(FormError::Malformed("a part's header field has no colon"))?;
        if name.is_empty() || !name.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(FormError::Malformed("a part's header field has no name"));
        }
        if name.eq_ignore_ascii_case("content-disposition") && disposition.replace(value).is_some()
        {
            return Err(FormError::Malformed(
                "a part has two Content-Disposition fields",
            ));
        }
    }
    let disposition =
        disposition.ok_or(FormError::Malformed("a part has no Content-Disposition"))?;
    let (kind, parameters) = parameterized(disposition)?;
    let (mut name, mut filename) = (None, None);
    for (parameter, value) in parameters {
        // `filename*` "MUST NOT be used" (RFC 7578 §4.2), and is ignored with every other.
        let slot = if parameter.eq_ignore_ascii_case("name") {
            &mut name
        } else if parameter.eq_ignore_ascii_case("filename") {
            &mut filename
        } else {
            continue;
        };
        if slot.replace(value).is_some() {
            return Err(FormError::Malformed(
                "a part's disposition repeats a parameter",
            ));
        }
    }
    let name = name.ok_or(FormError::Malformed("a part has no name"))?;
    Ok((name, filename, kind.eq_ignore_ascii_case("form-data")))
}

/// A header field's parameters, names as sent and values unquoted.
type Parameters = Vec<(String, String)>;

/// A header field's leading token and its parameters, `*( OWS ";" OWS [ parameter ] )` (RFC
/// 9110 §5.6.6). Whitespace may also stand around `=`, as between any two tokens of the
/// structured fields RFC 2045 §5.1 and RFC 2183 §2 define.
fn parameterized(text: &str) -> Result<(&str, Parameters), FormError> {
    const MALFORMED: FormError = FormError::Malformed("a header field's parameters are malformed");
    let end = text.find(';').unwrap_or(text.len());
    let (head, mut rest) = text.split_at_checked(end).ok_or(MALFORMED)?;
    let mut parameters = Vec::new();
    loop {
        rest = rest.trim_start_matches(WS);
        let Some(after) = rest.strip_prefix(';') else {
            if rest.is_empty() {
                break;
            }
            return Err(MALFORMED);
        };
        rest = after.trim_start_matches(WS);
        if rest.is_empty() || rest.starts_with(';') {
            continue;
        }
        let (name, after) = token(rest).ok_or(MALFORMED)?;
        let after = after
            .trim_start_matches(WS)
            .strip_prefix('=')
            .ok_or(MALFORMED)?
            .trim_start_matches(WS);
        let (value, after) = match after.strip_prefix('"') {
            Some(quoted) => unquote(quoted)?,
            None => token(after)
                .map(|(value, after)| (value.to_owned(), after))
                .ok_or(MALFORMED)?,
        };
        parameters.push((name.to_owned(), value));
        rest = after;
    }
    Ok((head.trim_matches(WS), parameters))
}

/// The token `text` begins with, and what follows it: one or more `tchar` (RFC 9110 §5.6.2).
fn token(text: &str) -> Option<(&str, &str)> {
    let tchar = |c: char| c.is_ascii_alphanumeric() || "!#$%&'*+-.^_`|~".contains(c);
    let end = text.find(|c: char| !tchar(c)).unwrap_or(text.len());
    let (token, rest) = text.split_at_checked(end)?;
    if token.is_empty() {
        return None;
    }
    Some((token, rest))
}

/// A quoted string's value, from after its opening quote, and what follows its closing one. A
/// backslash quotes the `"` or `\` after it, the only characters a sender quotes (RFC 9110
/// §5.6.4); before any other it stands for itself, so a Windows path in a `filename`, which S3
/// reads as one ("`C:\Program Files\directory1\file.txt` is interpreted as `file.txt`", 19
/// §2.3), keeps its separators.
fn unquote(text: &str) -> Result<(String, &str), FormError> {
    let mut value = String::new();
    let mut chars = text.char_indices();
    while let Some((at, c)) = chars.next() {
        match c {
            '"' => {
                let rest = text.get(after(at, 1)?..).unwrap_or_default();
                return Ok((value, rest));
            }
            '\\' => match chars.clone().next() {
                Some((_, quoted @ ('"' | '\\'))) => {
                    chars.next();
                    value.push(quoted);
                }
                _ => value.push('\\'),
            },
            c if c.is_ascii_control() && c != '\t' => {
                return Err(FormError::Malformed(
                    "a quoted string holds a control character",
                ));
            }
            c => value.push(c),
        }
    }
    Err(FormError::Malformed("a quoted string is not closed"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const TYPE: &str = "multipart/form-data; boundary=9431149156168";

    /// A body with `fields` as parts, then the file, then `after` as parts past it.
    fn body(
        boundary: &str,
        fields: &[(&str, &str)],
        file: &[u8],
        after: &[(&str, &str)],
    ) -> Vec<u8> {
        let mut out = Vec::new();
        let part = |out: &mut Vec<u8>, name: &str, value: &[u8]| {
            out.extend_from_slice(
                format!("--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n")
                    .as_bytes(),
            );
            out.extend_from_slice(value);
            out.extend_from_slice(b"\r\n");
        };
        for (name, value) in fields {
            part(&mut out, name, value.as_bytes());
        }
        out.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"MyFilename.jpg\"\r\nContent-Type: image/jpeg\r\n\r\n"
            )
            .as_bytes(),
        );
        out.extend_from_slice(file);
        out.extend_from_slice(b"\r\n");
        for (name, value) in after {
            part(&mut out, name, value.as_bytes());
        }
        out.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
        out
    }

    fn decode(content_type: &str, body: &[u8], piece: usize) -> Result<(Form, Vec<u8>), FormError> {
        let mut decoder = Decoder::new(Some(content_type))?;
        let mut out = Vec::new();
        for chunk in body.chunks(piece.max(1)) {
            decoder.feed(chunk, &mut out)?;
        }
        let (form, length) = decoder.finish()?;
        assert_eq!(length, out.len() as u64);
        Ok((form, out))
    }

    /// RESTObjectPOST's syntax block: fields, the file, and `submit` after it, ignored (19
    /// §2.1), decoded the same fed whole or a byte at a time.
    #[test]
    fn the_documented_form_decodes_in_any_pieces() {
        let fields = [
            ("key", "user/user1/${filename}"),
            (
                "tagging",
                "<Tagging><TagSet><Tag><Key>Tag Name</Key><Value>Tag Value</Value></Tag></TagSet></Tagging>",
            ),
            ("success_action_redirect", "success_redirect"),
            ("Content-Type", "image/jpeg"),
            ("x-amz-meta-uuid", "14365123651274"),
            ("x-amz-meta-tag", "Some,Tag,For,Picture"),
            ("Policy", "policy"),
        ];
        let file = b"...file content...\r\n--9431149156\r\n";
        let body = body(
            "9431149156168",
            &fields,
            file,
            &[("submit", "Upload to Amazon S3")],
        );
        for piece in [1, 2, 3, 7, 16, 64, body.len()] {
            let (form, out) = decode(TYPE, &body, piece).unwrap();
            assert_eq!(out, file, "piece {piece}");
            assert_eq!(form.get("KEY"), Some("user/user1/MyFilename.jpg"));
            assert_eq!(form.get("content-type"), Some("image/jpeg"));
            assert_eq!(form.get("submit"), None);
            assert_eq!(form.filename(), Some("MyFilename.jpg"));
            assert_eq!(form.fields().len(), fields.len());
        }
    }

    /// Fields are named in any case; a name sent twice holds its values joined by commas (19
    /// §2.4); the file is the part named `file` in any case.
    #[test]
    fn names_are_case_insensitive_and_repeats_join() {
        let body =
            b"--b\r\nContent-Disposition: form-data; name=\"x-amz-meta-tag\"\r\n\r\nNinja\r\n\
--b\r\ncontent-disposition: FORM-DATA; NAME=\"X-Amz-Meta-Tag\"\r\n\r\nStallman\r\n\
--b\r\nContent-Disposition: form-data; name=\"FILE\"\r\n\r\ndata\r\n--b--";
        let (form, out) = decode("Multipart/Form-Data; Boundary=b", body, 5).unwrap();
        assert_eq!(form.get("x-amz-meta-tag"), Some("Ninja,Stallman"));
        assert_eq!(form.fields().len(), 1);
        assert_eq!(out, b"data");
        assert_eq!(form.filename(), None);
    }

    /// `requests`, which s3-tests posts with, names a file on every part (19 §5.5): only the
    /// part named `file` is the file.
    #[test]
    fn a_filename_on_a_field_does_not_make_it_the_file() {
        let body = b"--b\r\nContent-Disposition: form-data; name=\"key\"; filename=\"key\"\r\n\r\nfoo.txt\r\n\
--b\r\nContent-Disposition: form-data; name=\"file\"; filename=\"file\"\r\n\r\nbar\r\n--b--\r\n";
        let (form, out) = decode("multipart/form-data; boundary=b", body, 1).unwrap();
        assert_eq!(form.get("key"), Some("foo.txt"));
        assert_eq!(out, b"bar");
    }

    /// `${filename}` becomes the file's name after its last `/` or `\` (19 §2.3), empty for a
    /// browser's empty file input, and stays as written when the part names no file (19 §8.1).
    #[test]
    fn filename_expands_as_s3_does() {
        let form = |disposition: &str| {
            let body = format!(
                "--b\r\nContent-Disposition: form-data; name=\"key\"\r\n\r\nuser/${{filename}}/${{filename}}\r\n\
--b\r\nContent-Disposition: {disposition}\r\n\r\n\r\n--b--"
            );
            decode("multipart/form-data; boundary=b", body.as_bytes(), 3)
                .unwrap()
                .0
                .get("key")
                .map(str::to_owned)
        };
        assert_eq!(
            form(r#"form-data; name="file"; filename="C:\Program Files\directory1\file.txt""#),
            Some("user/file.txt/file.txt".into())
        );
        assert_eq!(
            form(r#"form-data; name="file"; filename="a/b\\c.txt""#),
            Some("user/c.txt/c.txt".into())
        );
        assert_eq!(
            form(r#"form-data; name="file"; filename="""#),
            Some("user//".into())
        );
        assert_eq!(
            form(r#"form-data; name="file""#),
            Some("user/${filename}/${filename}".into())
        );
        assert_eq!(
            form(r#"form-data; name="file"; filename=report.pdf"#),
            Some("user/report.pdf/report.pdf".into())
        );
    }

    /// A preamble, transport padding and an epilogue are allowed and ignored (RFC 2046
    /// §5.1.1); a folded header field is unfolded; the file part's other fields are ignored.
    #[test]
    fn the_grammar_s_optional_parts_are_read() {
        let body = b"This is the preamble.\r\n--b \t\r\nContent-Disposition: form-data;\r\n name=key\r\n\r\nk\r\n\
--b\r\nContent-Type: text/plain\r\nContent-Disposition: form-data; name=file; filename=\"a.txt\"\r\nX-Other: 1\r\n\r\n\
body\r\n--b--  \r\nThis is the epilogue.";
        let (form, out) = decode(
            "multipart/form-data; boundary=\"b\"; charset=utf-8",
            body,
            4,
        )
        .unwrap();
        assert_eq!(form.get("key"), Some("k"));
        assert_eq!(out, b"body");
    }

    #[test]
    fn a_request_that_is_not_a_form_is_refused_with_s3_s_condition() {
        for content_type in [
            None,
            Some("text/html"),
            Some("multipart/mixed; boundary=b"),
            Some("application/x-www-form-urlencoded"),
        ] {
            let refused = Decoder::new(content_type).err();
            assert_eq!(refused, Some(FormError::NotForm), "{content_type:?}");
        }
        assert_eq!(FormError::NotForm.code(), ("PreconditionFailed", 412));
        assert_eq!(
            FormError::NotForm.details(),
            &[(
                "Condition",
                "Bucket POST must be of the enclosure-type multipart/form-data"
            )]
        );
    }

    #[test]
    fn a_boundary_follows_rfc_2046() {
        let long = "b".repeat(MAX_BOUNDARY);
        assert!(Decoder::new(Some(&format!("multipart/form-data; boundary={long}"))).is_ok());
        for bad in [
            "multipart/form-data",
            "multipart/form-data; charset=utf-8",
            "multipart/form-data; boundary=",
            "multipart/form-data; boundary=\"\"",
            "multipart/form-data; boundary=\"a b \"",
            "multipart/form-data; boundary=\"a;b\"",
            "multipart/form-data; boundary=a; boundary=b",
            "multipart/form-data; boundary=\"unclosed",
            "multipart/form-data; boundary",
        ] {
            assert!(
                matches!(Decoder::new(Some(bad)), Err(FormError::Malformed(_))),
                "{bad}"
            );
        }
        let too_long = format!("multipart/form-data; boundary={long}b");
        assert!(matches!(
            Decoder::new(Some(&too_long)),
            Err(FormError::Malformed(_))
        ));
    }

    #[test]
    fn malformed_bodies_are_refused() {
        let t = "multipart/form-data; boundary=b";
        let refused = |body: &[u8]| decode(t, body, 2).err();
        let malformed = |body: &[u8]| matches!(refused(body), Some(FormError::Malformed(_)));
        // No delimiter, a part with no headers, no disposition, no name.
        assert!(malformed(b"just text"));
        assert!(malformed(b"--b\r\n\r\nvalue\r\n--b--"));
        assert!(malformed(
            b"--b\r\nContent-Type: text/plain\r\n\r\nv\r\n--b--"
        ));

        assert!(malformed(
            b"--b\r\nContent-Disposition: form-data\r\n\r\nv\r\n--b--"
        ));
        assert!(malformed(
            b"--b\r\nContent-Disposition: form-data; name=\"a\"; name=\"b\"\r\n\r\nv\r\n--b--"
        ));
        // A bare LF in a header, a control character, a header without a colon.
        assert!(malformed(
            b"--b\r\nContent-Disposition: form-data;\n name=\"a\"\r\n\r\nv\r\n--b--"
        ));
        assert!(malformed(
            b"--b\r\nContent-Disposition: form-data; name=\"a\x01\"\r\n\r\nv\r\n--b--"
        ));
        assert!(malformed(
            b"--b\r\nContent-Disposition form-data\r\n\r\nv\r\n--b--"
        ));
        // A value that is not UTF-8.
        assert!(malformed(
            b"--b\r\nContent-Disposition: form-data; name=\"a\"\r\n\r\n\xff\r\n--b--"
        ));
        // The body ends before its file, or inside it.
        assert!(malformed(
            b"--b\r\nContent-Disposition: form-data; name=\"a\"\r\n\r\nv"
        ));
        assert!(malformed(
            b"--b\r\nContent-Disposition: form-data; name=\"file\"\r\n\r\ndata\r\n--"
        ));
        // The boundary inside the file: refused, never a file cut short.
        assert!(malformed(
            b"--b\r\nContent-Disposition: form-data; name=\"file\"\r\n\r\nda\r\n--bta\r\n--b--"
        ));
        // The body closes with no file, or with a `file` part of another disposition, which S3
        // read as a field (19 §13).
        assert_eq!(
            refused(b"--b\r\nContent-Disposition: form-data; name=\"key\"\r\n\r\nk\r\n--b--"),
            Some(FormError::NoFile)
        );
        assert_eq!(
            refused(b"--b\r\nContent-Disposition: file; name=\"file\"; filename=\"f\"\r\n\r\nk\r\n--b--"),
            Some(FormError::NoFile)
        );
        assert_eq!(FormError::NoFile.code(), ("InvalidArgument", 400));
        assert_eq!(
            FormError::NoFile.details(),
            &[("ArgumentName", "file"), ("ArgumentValue", "0")]
        );
    }

    /// The file ends at its delimiter; a delimiter with nothing after it ends the file as well
    /// as the close delimiter does.
    #[test]
    fn the_file_ends_at_its_delimiter() {
        let t = "multipart/form-data; boundary=b";
        let head = b"--b\r\nContent-Disposition: form-data; name=\"file\"\r\n\r\n".to_vec();
        for end in [
            &b"\r\n--b"[..],
            b"\r\n--b-",
            b"\r\n--b--",
            b"\r\n--b\r\nanything",
        ] {
            let mut body = head.clone();
            body.extend_from_slice(b"data");
            body.extend_from_slice(end);
            for piece in 1..=body.len() {
                let (_, out) = decode(t, &body, piece).unwrap();
                assert_eq!(out, b"data", "{end:?} in pieces of {piece}");
            }
        }
    }

    /// "The form data and boundaries (excluding the contents of the file) cannot exceed 20KB"
    /// (19 §2.1): a form whose file begins at the bound decodes; one byte more is refused.
    #[test]
    fn the_fields_before_the_file_are_bounded() {
        let t = "multipart/form-data; boundary=b";
        let shape = |value: usize| {
            let mut body =
                b"--b\r\nContent-Disposition: form-data; name=\"x-amz-meta-a\"\r\n\r\n".to_vec();
            body.extend(std::iter::repeat_n(b'v', value));
            body.extend_from_slice(
                b"\r\n--b\r\nContent-Disposition: form-data; name=\"file\"\r\n\r\n",
            );
            body
        };
        let overhead = shape(0).len();
        let mut fits = shape(MAX_PRE_DATA - overhead);
        assert_eq!(fits.len(), MAX_PRE_DATA);
        fits.extend_from_slice(b"file\r\n--b--");
        for piece in [1, 1000, fits.len()] {
            let (form, out) = decode(t, &fits, piece).unwrap();
            assert_eq!(out, b"file");
            assert_eq!(
                form.get("x-amz-meta-a").unwrap().len(),
                MAX_PRE_DATA - overhead
            );
        }
        let mut over = shape(MAX_PRE_DATA - overhead + 1);
        over.extend_from_slice(b"file\r\n--b--");
        for piece in [1, 1000, over.len()] {
            assert_eq!(
                decode(t, &over, piece).err(),
                Some(FormError::PreDataTooLong)
            );
        }
        assert_eq!(
            FormError::PreDataTooLong.code(),
            ("MaxPostPreDataLengthExceeded", 400)
        );
        assert_eq!(
            FormError::PreDataTooLong.details(),
            &[("MaxPostPreDataLengthBytes", "20480")]
        );
        // A preamble counts toward the bound too.
        let preamble = vec![b'p'; MAX_PRE_DATA + 1];
        assert_eq!(
            decode(t, &preamble, 4096).err(),
            Some(FormError::PreDataTooLong)
        );
        // So does what `${filename}` expands to.
        let mut body = b"--b\r\nContent-Disposition: form-data; name=\"key\"\r\n\r\n".to_vec();
        body.extend_from_slice(FILENAME.repeat(100).as_bytes());
        body.extend_from_slice(
            b"\r\n--b\r\nContent-Disposition: form-data; name=\"file\"; filename=\"",
        );
        body.extend(std::iter::repeat_n(b'n', 300));
        body.extend_from_slice(b"\"\r\n\r\ndata\r\n--b--");
        assert_eq!(decode(t, &body, 64).err(), Some(FormError::PreDataTooLong));
    }

    #[test]
    fn quoted_strings_follow_rfc_9110_and_keep_windows_paths() {
        assert_eq!(
            parameterized(r#"form-data; name="a\"b"; filename="C:\dir\x\\y.txt""#).unwrap(),
            (
                "form-data",
                vec![
                    ("name".into(), "a\"b".into()),
                    ("filename".into(), r"C:\dir\x\y.txt".into())
                ]
            )
        );
        assert_eq!(
            parameterized("form-data ; name = key ;; filename=\"é.txt\";").unwrap(),
            (
                "form-data",
                vec![
                    ("name".into(), "key".into()),
                    ("filename".into(), "é.txt".into())
                ]
            )
        );
        for bad in [
            "form-data; name",
            "form-data; name=",
            "form-data; name=\"a",
            "form-data; =a",
            "form-data; name=a b",
        ] {
            assert!(parameterized(bad).is_err(), "{bad}");
        }
    }

    fn arbitrary_value() -> impl Strategy<Value = String> {
        proptest::collection::vec(
            prop_oneof![
                Just("\r\n".to_owned()),
                Just("\r\n--".to_owned()),
                Just("\r\n--bound".to_owned()),
                Just("-".to_owned()),
                Just("\r".to_owned()),
                "[a-z é]{1,5}",
            ],
            0..8,
        )
        .prop_map(|parts| parts.concat())
    }

    proptest! {
        /// Any form, with values and a file holding every prefix of the delimiter but never the
        /// delimiter itself, decodes to what was sent however the body is cut.
        #[test]
        fn any_form_decodes_to_what_was_sent(
            values in proptest::collection::vec(arbitrary_value(), 0..6),
            file in proptest::collection::vec(prop_oneof![
                Just(b"\r\n--boundar".to_vec()),
                Just(b"\r".to_vec()),
                Just(b"\r\n".to_vec()),
                proptest::collection::vec(any::<u8>(), 0..50),
            ], 0..12),
            piece in 1usize..300,
        ) {
            let file: Vec<u8> = file.concat();
            let delimiter = b"\r\n--boundary";
            prop_assume!(!file.windows(delimiter.len()).any(|w| w == delimiter));
            prop_assume!(!file.starts_with(&delimiter[2..]));
            prop_assume!(values.iter().all(|v| !v.contains("\r\n--boundary") && !v.starts_with("--boundary")));
            let names: Vec<String> = (0..values.len()).map(|i| format!("x-amz-meta-{i}")).collect();
            let fields: Vec<(&str, &str)> = names.iter().map(String::as_str).zip(values.iter().map(String::as_str)).collect();
            let body = body("boundary", &fields, &file, &[("after", "ignored")]);
            let (form, out) = decode("multipart/form-data; boundary=boundary", &body, piece).unwrap();
            prop_assert_eq!(out, file);
            prop_assert_eq!(form.fields().len(), values.len());
            for (name, value) in &fields {
                prop_assert_eq!(form.get(name), Some(*value));
            }
        }
    }
}
