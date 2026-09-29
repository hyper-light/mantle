//! Listing a bucket (docs/research/05 §6, §4.7; docs/design/s3-protocol.md §3): its keys
//! (ListObjects, ListObjectsV2), and the versions (ListObjectVersions) or multipart uploads
//! (ListMultipartUploads) under its keys. A page takes a prefix, rolls keys up into common
//! prefixes at a delimiter, returns at most `max-keys` items, and says where the next page
//! starts.
//!
//! Keys are in UTF-8 byte order (05 §6.1). A page counts each common prefix as one item, "All
//! of the keys that roll up into a common prefix count as a single return", and passes the rest
//! of a common prefix with one seek to the first key beyond it, so a prefix holding millions of
//! keys costs one step.
//!
//! An index may pass keys that list nothing: a key whose current version is a delete marker
//! (05 §6.4), a key holding only uploads, or, for uploads, a key holding only versions. A page
//! passes at most [`MAX_KEYS`] of them, so it never reads more than twice the keys the largest
//! page lists, and then ends early: "The response might contain fewer keys" (05 §6.2). The next
//! page starts just after the last key passed, and can still list the common prefix that key is
//! in.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

/// `max-keys`: "By default, the action returns up to 1,000 key names. The response might
/// contain fewer keys but will never contain more" (05 §6.2). Also the most keys that list
/// nothing one page passes.
pub const MAX_KEYS: usize = 1000;

/// What a seek found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step<T> {
    Found(T),
    /// Nothing left before the end lists.
    End,
    /// The budget of keys that list nothing ran out; this is the last key passed.
    Paused(String),
}

/// Where a page of keys starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Start<'a> {
    /// After a key or common prefix a client names (`marker`, `start-after`) or a page listed.
    /// A common prefix the key falls in "is filtered out from results if it is not
    /// lexicographically greater than" it (05 §6.2), so the page starts beyond that prefix.
    After(&'a str),
    /// Just after a key a scan passed without listing it: the common prefix it falls in may
    /// not have been listed, so the page can still list it.
    Past(&'a str),
}

/// Where the next page of keys starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resume {
    After(String),
    Past(String),
}

impl Resume {
    pub fn start(&self) -> Start<'_> {
        match self {
            Self::After(key) => Start::After(key),
            Self::Past(key) => Start::Past(key),
        }
    }

    /// The key or common prefix it names.
    pub fn key(&self) -> &str {
        match self {
            Self::After(key) | Self::Past(key) => key,
        }
    }
}

/// How a client asks for the next page of keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Paging {
    /// By a key it sends back as `marker` (ListObjects).
    Marker,
    /// By a token mantle issues (ListObjectsV2).
    Token,
}

/// An ordered index of a bucket's keys, for ListObjects: it leaves out a key whose current
/// version is a delete marker (05 §6.4).
pub trait Keys {
    type Value;
    type Error;
    /// The first key at or after `from` and before `to` that lists, with its value, passing
    /// at most `*budget` keys that do not and counting each one off.
    fn seek(
        &mut self,
        from: &[u8],
        to: &[u8],
        budget: &mut usize,
    ) -> Result<Step<(String, Self::Value)>, Self::Error>;
}

/// What one page of keys asks for.
#[derive(Debug, Clone, Copy)]
pub struct Request<'a> {
    pub prefix: &'a str,
    /// No delimiter when `None` or empty.
    pub delimiter: Option<&'a str>,
    /// `marker` or `start-after`, or a continuation; `None` for the first page.
    pub start: Option<Start<'a>>,
    pub max_keys: usize,
    pub paging: Paging,
}

/// One page of keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page<V> {
    pub contents: Vec<(String, V)>,
    pub common_prefixes: Vec<String>,
    pub truncated: bool,
    /// Where the next page starts, on a truncated page.
    pub next: Option<Resume>,
}

impl<V> Page<V> {
    /// `KeyCount`: keys and common prefixes, which ceph s3-tests counts together (05 §6.2).
    pub fn key_count(&self) -> usize {
        self.contents
            .len()
            .saturating_add(self.common_prefixes.len())
    }

    /// `NextContinuationToken`, on a truncated page.
    pub fn continuation(&self) -> Option<String> {
        self.next.as_ref().map(continuation_token)
    }

    /// ListObjects' `NextMarker`. S3 returns it "only if you have the delimiter request
    /// parameter specified" (05 §6.3); mantle also returns it on a page that ended at a key it
    /// passed, since a client without it resumes after the last key listed, before the keys
    /// the page passed, and a page that listed none would end the listing.
    pub fn next_marker(&self, delimited: bool) -> Option<&str> {
        match self.next.as_ref()? {
            Resume::After(key) if delimited => Some(key),
            Resume::After(_) => None,
            Resume::Past(key) => Some(key),
        }
    }
}

/// One page of `keys`.
pub fn page<K: Keys>(request: &Request<'_>, keys: &mut K) -> Result<Page<K::Value>, K::Error> {
    let mut page = Page {
        contents: Vec::new(),
        common_prefixes: Vec::new(),
        truncated: false,
        next: None,
    };
    let max = request.max_keys.min(MAX_KEYS);
    if max == 0 {
        // ceph s3-tests: MaxKeys=0 lists nothing and is not truncated (05 §6.2).
        return Ok(page);
    }
    let prefix = request.prefix;
    let delimiter = request.delimiter.filter(|d| !d.is_empty());
    let end = beyond(prefix);
    let mut from = start(prefix, delimiter, request.start);
    let mut budget = MAX_KEYS;
    let mut last = None;
    loop {
        match keys.seek(&from, &end, &mut budget)? {
            Step::End => break,
            Step::Paused(passed) => {
                page.truncated = true;
                last = Some(match rolled_up(&passed, prefix, delimiter) {
                    // A marker inside a common prefix filters that prefix out (05 §6.3), so a
                    // client that pages by marker could never be shown it: it is listed now,
                    // since the keys it holds lie ahead. A full page leaves it for the next,
                    // which starts after the last item listed.
                    Some(common) if request.paging == Paging::Marker => {
                        if page.key_count() < max {
                            page.common_prefixes.push(common.to_owned());
                            Resume::After(common.to_owned())
                        } else {
                            last.unwrap_or(Resume::Past(passed))
                        }
                    }
                    _ => Resume::Past(passed),
                });
                break;
            }
            Step::Found((key, value)) => {
                if page.key_count() >= max {
                    page.truncated = true;
                    break;
                }
                if let Some(common) = rolled_up(&key, prefix, delimiter) {
                    from = beyond(common);
                    last = Some(Resume::After(common.to_owned()));
                    page.common_prefixes.push(common.to_owned());
                } else {
                    from = just_after(&key);
                    last = Some(Resume::After(key.clone()));
                    page.contents.push((key, value));
                }
            }
        }
    }
    if page.truncated {
        page.next = last;
    }
    Ok(page)
}

/// Where a page begins in key space.
fn start(prefix: &str, delimiter: Option<&str>, start: Option<Start<'_>>) -> Vec<u8> {
    match start {
        Some(Start::After(after)) if after >= prefix => match rolled_up(after, prefix, delimiter) {
            Some(common) => beyond(common),
            None => just_after(after),
        },
        Some(Start::Past(past)) if past >= prefix => just_after(past),
        _ => prefix.as_bytes().to_vec(),
    }
}

/// An entry of a listing with several entries per key: a version, named by its key and
/// version ID, or a multipart upload, named by its key and upload ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listed<V> {
    pub key: String,
    pub id: String,
    pub value: V,
}

/// Where a seek for entries starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryFrom {
    /// The first entry of the first key at or after these bytes.
    Key(Vec<u8>),
    /// The entry after the one `key` and `id` name.
    After { key: String, id: String },
}

/// An ordered index of a bucket's versions (ListObjectVersions) or uploads
/// (ListMultipartUploads): keys in byte order, and within a key, versions newest first
/// (05 §6.4) or uploads in the order they were created (05 §4.7).
pub trait Entries {
    type Value;
    type Error;
    /// The first entry at or after `from` whose key is before `to`, passing at most
    /// `*budget` keys that hold none and counting each one off.
    fn seek(
        &mut self,
        from: &EntryFrom,
        to: &[u8],
        budget: &mut usize,
    ) -> Result<Step<Listed<Self::Value>>, Self::Error>;
}

/// The ID marker of a page that ended at a key it passed: with that key as the key marker,
/// the next page starts just after it and can still list the common prefix it is in. No
/// version or upload ID is this string (docs/design/s3-protocol.md §3).
pub const PASSED: &str = "passed";

/// What one page of entries asks for.
#[derive(Debug, Clone, Copy)]
pub struct EntryRequest<'a> {
    pub prefix: &'a str,
    /// No delimiter when `None` or empty.
    pub delimiter: Option<&'a str>,
    /// `key-marker`.
    pub key_marker: Option<&'a str>,
    /// `version-id-marker` or `upload-id-marker`, which applies only with a key marker.
    pub id_marker: Option<&'a str>,
    /// `max-keys` or `max-uploads`.
    pub max: usize,
}

/// One page of entries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryPage<V> {
    pub entries: Vec<Listed<V>>,
    pub common_prefixes: Vec<String>,
    pub truncated: bool,
    /// Where the next page starts, as a key marker and an ID marker, on a truncated page.
    pub next: Option<(String, String)>,
}

impl<V> EntryPage<V> {
    /// Entries and common prefixes, which `max-keys` counts together (05 §6.4).
    pub fn count(&self) -> usize {
        self.entries
            .len()
            .saturating_add(self.common_prefixes.len())
    }

    /// ListMultipartUploads' `NextKeyMarker` and `NextUploadIdMarker`, which all three of AWS's
    /// samples write, truncated or not: where the next page starts, or on a page that is not
    /// truncated, the last upload listed, and empty when it lists none (13 §9.3).
    pub fn upload_markers(&self) -> (String, String) {
        match (&self.next, self.entries.last()) {
            (Some(next), _) => next.clone(),
            (None, Some(last)) => (last.key.clone(), last.id.clone()),
            (None, None) => (String::new(), String::new()),
        }
    }
}

/// One page of `index`.
pub fn entries<I: Entries>(
    request: &EntryRequest<'_>,
    index: &mut I,
) -> Result<EntryPage<I::Value>, I::Error> {
    let mut page = EntryPage {
        entries: Vec::new(),
        common_prefixes: Vec::new(),
        truncated: false,
        next: None,
    };
    let max = request.max.min(MAX_KEYS);
    if max == 0 {
        return Ok(page);
    }
    let prefix = request.prefix;
    let delimiter = request.delimiter.filter(|d| !d.is_empty());
    let end = beyond(prefix);
    let mut from = entry_start(request, delimiter);
    let mut budget = MAX_KEYS;
    let mut last = None;
    loop {
        match index.seek(&from, &end, &mut budget)? {
            Step::End => break,
            Step::Paused(passed) => {
                page.truncated = true;
                last = Some((passed, PASSED.to_owned()));
                break;
            }
            Step::Found(entry) => {
                if page.count() >= max {
                    page.truncated = true;
                    break;
                }
                if let Some(common) = rolled_up(&entry.key, prefix, delimiter) {
                    from = EntryFrom::Key(beyond(common));
                    last = Some((common.to_owned(), String::new()));
                    page.common_prefixes.push(common.to_owned());
                } else {
                    from = EntryFrom::After {
                        key: entry.key.clone(),
                        id: entry.id.clone(),
                    };
                    last = Some((entry.key.clone(), entry.id.clone()));
                    page.entries.push(entry);
                }
            }
        }
    }
    if page.truncated {
        page.next = last;
    }
    Ok(page)
}

/// Where a page of entries begins. A key marker alone starts after all of its key's
/// entries, "only the keys lexicographically greater than the specified key-marker"; with an
/// ID marker, after that entry of the key (05 §4.7, §6.4). A key marker in a common prefix
/// starts beyond the prefix, as for keys, unless the ID marker is [`PASSED`].
fn entry_start(request: &EntryRequest<'_>, delimiter: Option<&str>) -> EntryFrom {
    let prefix = request.prefix;
    let Some(key) = request.key_marker.filter(|k| *k >= prefix) else {
        return EntryFrom::Key(prefix.as_bytes().to_vec());
    };
    if request.id_marker == Some(PASSED) {
        return EntryFrom::Key(just_after(key));
    }
    if let Some(common) = rolled_up(key, prefix, delimiter) {
        return EntryFrom::Key(beyond(common));
    }
    match request.id_marker.filter(|id| !id.is_empty()) {
        Some(id) => EntryFrom::After {
            key: key.to_owned(),
            id: id.to_owned(),
        },
        None => EntryFrom::Key(just_after(key)),
    }
}

/// The common prefix `key` rolls up into: through the first delimiter after `prefix`.
fn rolled_up<'k>(key: &'k str, prefix: &str, delimiter: Option<&str>) -> Option<&'k str> {
    let d = delimiter?;
    let at = key.strip_prefix(prefix)?.find(d)?;
    key.get(..prefix.len().checked_add(at)?.checked_add(d.len())?)
}

/// The smallest byte string after `key`: `key` followed by a zero byte.
pub fn just_after(key: &str) -> Vec<u8> {
    let mut next = key.as_bytes().to_vec();
    next.push(0);
    next
}

/// No UTF-8 string contains the byte 0xFF, so `prefix` followed by it sorts after every key
/// that starts with `prefix` and before every other key after them.
pub fn beyond(prefix: &str) -> Vec<u8> {
    let mut next = prefix.as_bytes().to_vec();
    next.push(0xFF);
    next
}

/// A continuation token: opaque to clients, "not a real key" (05 §6.2). It names where the
/// next page starts, and whether that is after an item listed or past a key the scan passed.
/// A token a client alters only moves that start, as `start-after` could.
pub fn continuation_token(next: &Resume) -> String {
    let (tag, key) = match next {
        Resume::After(key) => (b'a', key),
        Resume::Past(key) => (b'p', key),
    };
    let mut bytes = Vec::with_capacity(key.len().saturating_add(1));
    bytes.push(tag);
    bytes.extend_from_slice(key.as_bytes());
    URL_SAFE_NO_PAD.encode(bytes)
}

/// Where a continuation token starts the page; `None` if it is not one of mantle's.
pub fn continuation_start(token: &str) -> Option<Resume> {
    let bytes = URL_SAFE_NO_PAD.decode(token.trim()).ok()?;
    let (&tag, key) = bytes.split_first()?;
    let key = String::from_utf8(key.to_vec()).ok()?;
    match tag {
        b'a' => Some(Resume::After(key)),
        b'p' => Some(Resume::Past(key)),
        _ => None,
    }
}

/// A key, prefix or marker as `encoding-type=url` returns it: percent-encoded except for
/// unreserved characters and `/`, as ceph s3-tests expects (`foo+1/` → `foo%2B1/`,
/// `quux ab/` → `quux%20ab/`) and AWS's `test_file(3).png` → `test_file%283%29.png` shows.
pub fn url_encode(text: &str) -> String {
    crate::sigv4::uri_encode(text.as_bytes(), false)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("max-keys must be a non-negative integer")]
pub struct InvalidMaxKeys;

impl InvalidMaxKeys {
    /// `400 InvalidArgument`, as s3-tests expects for `max-keys=blah` (05 §6.3).
    pub fn code(&self) -> (&'static str, u16) {
        ("InvalidArgument", 400)
    }
}

/// `max-keys` as sent; absent is the default.
pub fn max_keys(value: Option<&str>) -> Result<usize, InvalidMaxKeys> {
    match value {
        None => Ok(MAX_KEYS),
        Some(v) => v.trim().parse().map_err(|_| InvalidMaxKeys),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::convert::Infallible;

    /// Keys, each listing or not (a delete marker's key lists nothing), with a count of the
    /// index's work.
    struct Index {
        keys: BTreeMap<Vec<u8>, bool>,
        seeks: usize,
        passed: usize,
    }

    impl Keys for Index {
        type Value = ();
        type Error = Infallible;
        fn seek(
            &mut self,
            from: &[u8],
            to: &[u8],
            budget: &mut usize,
        ) -> Result<Step<(String, ())>, Infallible> {
            self.seeks += 1;
            if from >= to {
                return Ok(Step::End);
            }
            for (k, &lists) in self.keys.range(from.to_vec()..to.to_vec()) {
                let key = String::from_utf8(k.clone()).unwrap();
                if lists {
                    return Ok(Step::Found((key, ())));
                }
                self.passed += 1;
                *budget -= 1;
                if *budget == 0 {
                    return Ok(Step::Paused(key));
                }
            }
            Ok(Step::End)
        }
    }

    fn index<S: AsRef<str>>(keys: &[S]) -> Index {
        Index {
            keys: keys
                .iter()
                .map(|k| (k.as_ref().as_bytes().to_vec(), true))
                .collect(),
            seeks: 0,
            passed: 0,
        }
    }

    fn req<'a>(
        prefix: &'a str,
        delimiter: Option<&'a str>,
        after: Option<&'a str>,
        max: usize,
    ) -> Request<'a> {
        Request {
            prefix,
            delimiter,
            start: after.map(Start::After),
            max_keys: max,
            paging: Paging::Token,
        }
    }

    fn list(ix: &mut Index, r: Request<'_>) -> Page<()> {
        page(&r, ix).unwrap()
    }

    fn keys(p: &Page<()>) -> Vec<&str> {
        p.contents.iter().map(|(k, _)| k.as_str()).collect()
    }

    /// Lists every page, following continuation tokens as a V2 client does: the keys and
    /// common prefixes in order, and how many pages it took.
    fn all_pages(
        ix: &mut Index,
        prefix: &str,
        delimiter: Option<&str>,
        max: usize,
    ) -> (Vec<String>, usize) {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        let mut pages = 0;
        loop {
            pages += 1;
            let resume = token.as_deref().map(|t| continuation_start(t).unwrap());
            let r = Request {
                start: resume.as_ref().map(Resume::start),
                ..req(prefix, delimiter, None, max)
            };
            let p = list(ix, r);
            out.extend(p.contents.iter().map(|(k, _)| k.clone()));
            out.extend(p.common_prefixes.iter().cloned());
            match p.continuation() {
                Some(t) => token = Some(t),
                None => return (out, pages),
            }
        }
    }

    #[test]
    fn keys_come_back_in_utf8_byte_order() {
        let mut ix = index(&["éclair/", "apple/", "Apple/", "中 文/"]);
        let p = list(&mut ix, req("", None, None, 1000));
        assert_eq!(keys(&p), ["Apple/", "apple/", "éclair/", "中 文/"]);
    }

    /// ceph s3-tests' delimiter paging: MaxKeys=1 over asdf, boo/bar, boo/baz/xyzzy, cquux/…
    /// gives pages [asdf], [boo/], [cquux/] (05 §6.2), each resuming with one seek.
    #[test]
    fn delimiter_pages_skip_what_a_common_prefix_rolled_up() {
        let mut ix = index(&[
            "asdf",
            "boo/bar",
            "boo/baz/xyzzy",
            "cquux/thud",
            "cquux/bla",
        ]);
        let mut resume: Option<Resume> = None;
        let mut pages = Vec::new();
        loop {
            ix.seeks = 0;
            let r = Request {
                start: resume.as_ref().map(Resume::start),
                ..req("", Some("/"), None, 1)
            };
            let p = list(&mut ix, r);
            assert!(ix.seeks <= 2, "{} seeks", ix.seeks);
            pages.push((
                keys(&p).iter().map(|k| k.to_string()).collect::<Vec<_>>(),
                p.common_prefixes.clone(),
            ));
            if !p.truncated {
                break;
            }
            resume = continuation_start(&p.continuation().unwrap());
        }
        assert_eq!(pages.len(), 3);
        assert_eq!(pages[0], (vec!["asdf".to_string()], vec![]));
        assert_eq!(pages[1], (vec![], vec!["boo/".to_string()]));
        assert_eq!(pages[2], (vec![], vec!["cquux/".to_string()]));
    }

    /// 999 keys under `0/` roll into one common prefix that costs one seek, so the default
    /// page holds it and the four keys after it (05 §6.2).
    #[test]
    fn a_large_common_prefix_is_one_item_and_one_seek() {
        let mut all: Vec<String> = (0..999).map(|i| format!("0/{i:04}")).collect();
        all.extend(["1999", "1999#", "1999+", "2000"].map(String::from));
        let mut ix = index(&all);
        let p = list(&mut ix, req("", Some("/"), None, 1000));
        assert_eq!(p.common_prefixes, ["0/"]);
        assert_eq!(keys(&p), ["1999", "1999#", "1999+", "2000"]);
        assert!(!p.truncated);
        assert_eq!(p.key_count(), 5);
        assert_eq!(ix.seeks, 6);
    }

    /// s3-tests' encoding case: keys `foo+1/bar`, `foo/bar/xyzzy`, `quux ab/thud`, `asdf+b`
    /// list as `asdf%2Bb` and prefixes `foo%2B1/`, `foo/`, `quux%20ab/` (05 §6.2).
    #[test]
    fn url_encoding_matches_s3_tests() {
        let mut ix = index(&["foo+1/bar", "foo/bar/xyzzy", "quux ab/thud", "asdf+b"]);
        let p = list(&mut ix, req("", Some("/"), None, 1000));
        let encoded: Vec<String> = keys(&p).into_iter().map(url_encode).collect();
        assert_eq!(encoded, ["asdf%2Bb"]);
        let encoded: Vec<String> = p.common_prefixes.iter().map(|c| url_encode(c)).collect();
        assert_eq!(encoded, ["foo%2B1/", "foo/", "quux%20ab/"]);
        assert_eq!(url_encode("test_file(3).png"), "test_file%283%29.png");
    }

    #[test]
    fn prefixes_delimiters_and_start_after() {
        let mut ix = index(&["asdf/", "asdf/b", "asdfa", "b a/c", "b+1", "boo"]);
        // A key equal to the prefix and ending in the delimiter is a key (05 §6.2).
        let p = list(&mut ix, req("asdf/", Some("/"), None, 1000));
        assert_eq!(keys(&p), ["asdf/", "asdf/b"]);
        // Other delimiters work, and the prefix bounds the scan.
        let p = list(&mut ix, req("b", Some(" "), None, 1000));
        assert_eq!(p.common_prefixes, ["b "]);
        assert_eq!(keys(&p), ["b+1", "boo"]);
        // start-after need not exist; a common prefix not above it is left out.
        let p = list(&mut ix, req("", Some("/"), Some("asdf/a"), 1000));
        assert_eq!(p.common_prefixes, ["b a/"]);
        assert_eq!(keys(&p), ["asdfa", "b+1", "boo"]);
        // start-after before the prefix starts at the prefix; after it, lists nothing.
        let p = list(&mut ix, req("b", None, Some("a"), 1000));
        assert_eq!(keys(&p), ["b a/c", "b+1", "boo"]);
        let p = list(&mut ix, req("b", None, Some("c"), 1000));
        assert!(p.contents.is_empty() && !p.truncated);
        // MaxKeys=0 lists nothing and is not truncated.
        let p = list(&mut ix, req("", None, None, 0));
        assert!(p.contents.is_empty() && !p.truncated);
        // An empty delimiter is no delimiter.
        let p = list(&mut ix, req("", Some(""), None, 1000));
        assert_eq!(p.contents.len(), 6);
    }

    #[test]
    fn pages_hold_at_most_a_thousand_and_resume_where_they_stopped() {
        let all: Vec<String> = (0..2500).map(|i| format!("k{i:05}")).collect();
        let mut ix = index(&all);
        let (seen, pages) = all_pages(&mut ix, "", None, usize::MAX);
        assert_eq!(seen, all);
        assert_eq!(pages, 3);
    }

    /// Keys that list nothing are passed at most MAX_KEYS a page, and the page after a pause
    /// starts past them: listing makes progress however long the run, and every key that
    /// lists is listed once.
    #[test]
    fn a_long_run_of_keys_that_list_nothing_pauses_and_resumes() {
        let mut ix = index::<&str>(&[]);
        for i in 0..2_600 {
            ix.keys.insert(format!("d{i:05}").into_bytes(), false);
        }
        for k in ["a", "e", "d01500x"] {
            ix.keys.insert(k.as_bytes().to_vec(), true);
        }
        let p = list(&mut ix, req("", None, None, 1000));
        // "a" lists; the next 1,000 keys pass the budget.
        assert_eq!(keys(&p), ["a"]);
        assert!(p.truncated);
        assert_eq!(p.next, Some(Resume::Past("d00999".into())));
        let (seen, pages) = all_pages(&mut ix, "", None, 1000);
        assert_eq!(seen, ["a", "d01500x", "e"]);
        assert_eq!(pages, 3);
        // No page passed more than MAX_KEYS keys.
        ix.passed = 0;
        let _ = all_pages(&mut ix, "", None, 1000);
        assert!(ix.passed <= 3 * MAX_KEYS);
    }

    /// With a delimiter, a token resumes inside a common prefix it has not listed and lists
    /// it once a key under it lists.
    #[test]
    fn a_token_resumes_inside_a_common_prefix_it_has_not_listed() {
        let mut ix = index::<&str>(&[]);
        for i in 0..1_500 {
            ix.keys.insert(format!("p/{i:05}").into_bytes(), false);
        }
        ix.keys.insert(b"p/live".to_vec(), true);
        ix.keys.insert(b"q".to_vec(), true);
        let p = list(&mut ix, req("", Some("/"), None, 1000));
        assert!(p.truncated && p.common_prefixes.is_empty() && p.contents.is_empty());
        assert_eq!(p.next, Some(Resume::Past("p/00999".into())));
        // The second page lists "q" and the prefix "p/"; each page gives keys, then prefixes.
        let (seen, _) = all_pages(&mut ix, "", Some("/"), 1000);
        assert_eq!(seen, ["q", "p/"]);
    }

    /// A marker cannot resume inside a common prefix without dropping it (05 §6.3), so a V1
    /// page that pauses inside one lists the prefix and resumes beyond it; without a
    /// delimiter it names the key it passed as NextMarker.
    #[test]
    fn a_marker_page_lists_the_common_prefix_it_pauses_in() {
        let mut ix = index::<&str>(&[]);
        for i in 0..1_500 {
            ix.keys.insert(format!("p/{i:05}").into_bytes(), false);
        }
        ix.keys.insert(b"p/live".to_vec(), true);
        ix.keys.insert(b"q".to_vec(), true);
        let v1 = |start| Request {
            paging: Paging::Marker,
            start,
            ..req("", Some("/"), None, 1000)
        };
        let p = list(&mut ix, v1(None));
        assert_eq!(p.common_prefixes, ["p/"]);
        assert_eq!(p.next_marker(true), Some("p/"));
        let p = list(&mut ix, v1(Some(Start::After("p/"))));
        assert_eq!(keys(&p), ["q"]);
        assert!(!p.truncated && p.next_marker(true).is_none());
        // Without a delimiter the page names the last key it passed.
        let p = list(
            &mut ix,
            Request {
                delimiter: None,
                ..v1(None)
            },
        );
        assert!(p.contents.is_empty() && p.truncated);
        assert_eq!(p.next_marker(false), Some("p/00999"));
        // A page ended by max-keys names NextMarker only with a delimiter (05 §6.3).
        let mut ix = index(&["a", "b"]);
        let p = list(
            &mut ix,
            Request {
                max_keys: 1,
                delimiter: None,
                ..v1(None)
            },
        );
        assert_eq!(p.next_marker(false), None);
        assert_eq!(p.next_marker(true), Some("a"));
    }

    #[test]
    fn index_errors_reach_the_caller() {
        struct Broken;
        impl Keys for Broken {
            type Value = ();
            type Error = &'static str;
            fn seek(
                &mut self,
                _: &[u8],
                _: &[u8],
                _: &mut usize,
            ) -> Result<Step<(String, ())>, &'static str> {
                Err("unavailable")
            }
        }
        assert_eq!(
            page(&req("", None, None, 10), &mut Broken),
            Err("unavailable")
        );
    }

    #[test]
    fn tokens_and_max_keys_parse() {
        for resume in [Resume::After("a/é b".into()), Resume::Past("x".into())] {
            assert_eq!(
                continuation_start(&continuation_token(&resume)),
                Some(resume)
            );
        }
        assert_eq!(continuation_start("not base64!"), None);
        assert_eq!(continuation_start(&URL_SAFE_NO_PAD.encode(b"zkey")), None);
        assert_eq!(continuation_start(""), None);
        assert_eq!(max_keys(Some("blah")), Err(InvalidMaxKeys));
        assert_eq!(max_keys(Some("-1")), Err(InvalidMaxKeys));
        assert_eq!(max_keys(Some("5")), Ok(5));
        assert_eq!(max_keys(None), Ok(1000));
    }

    /// Versions or uploads: per key, entries in index order, each listing or not per key.
    struct Store {
        keys: BTreeMap<String, Vec<String>>,
        seeks: usize,
    }

    impl Entries for Store {
        type Value = ();
        type Error = Infallible;
        fn seek(
            &mut self,
            from: &EntryFrom,
            to: &[u8],
            budget: &mut usize,
        ) -> Result<Step<Listed<()>>, Infallible> {
            self.seeks += 1;
            let (start, after): (Vec<u8>, Option<(&str, &str)>) = match from {
                EntryFrom::Key(k) => (k.clone(), None),
                EntryFrom::After { key, id } => (key.as_bytes().to_vec(), Some((key, id))),
            };
            for (key, ids) in &self.keys {
                if key.as_bytes() < start.as_slice() || key.as_bytes() >= to {
                    continue;
                }
                let next = match after {
                    Some((k, id)) if k == key => {
                        ids.iter().position(|i| i == id).map_or(0, |at| at + 1)
                    }
                    _ => 0,
                };
                match ids.get(next) {
                    Some(id) => {
                        return Ok(Step::Found(Listed {
                            key: key.clone(),
                            id: id.clone(),
                            value: (),
                        }));
                    }
                    None if ids.is_empty() => {
                        *budget -= 1;
                        if *budget == 0 {
                            return Ok(Step::Paused(key.clone()));
                        }
                    }
                    None => {}
                }
            }
            Ok(Step::End)
        }
    }

    fn store(entries: &[(&str, &[&str])]) -> Store {
        Store {
            keys: entries
                .iter()
                .map(|(k, ids)| (k.to_string(), ids.iter().map(|i| i.to_string()).collect()))
                .collect(),
            seeks: 0,
        }
    }

    fn entry_req<'a>(
        prefix: &'a str,
        delimiter: Option<&'a str>,
        key_marker: Option<&'a str>,
        id_marker: Option<&'a str>,
        max: usize,
    ) -> EntryRequest<'a> {
        EntryRequest {
            prefix,
            delimiter,
            key_marker,
            id_marker,
            max,
        }
    }

    fn listed(p: &EntryPage<()>) -> Vec<(String, String)> {
        p.entries
            .iter()
            .map(|e| (e.key.clone(), e.id.clone()))
            .collect()
    }

    /// Every page, following NextKeyMarker and NextVersionIdMarker as botocore's paginator does.
    fn every_entry(
        s: &mut Store,
        prefix: &str,
        delimiter: Option<&str>,
        max: usize,
    ) -> Vec<(String, String)> {
        let mut out = Vec::new();
        let mut markers: Option<(String, String)> = None;
        loop {
            let (key, id) = match &markers {
                Some((k, i)) => (Some(k.as_str()), Some(i.as_str())),
                None => (None, None),
            };
            let p = entries(&entry_req(prefix, delimiter, key, id, max), s).unwrap();
            out.extend(listed(&p));
            out.extend(p.common_prefixes.iter().map(|c| (c.clone(), String::new())));
            match p.next {
                Some(next) => markers = Some(next),
                None => return out,
            }
        }
    }

    /// Versions page by key and version ID, a key's versions resume inside it, and the
    /// markers resume as S3 describes: a key marker alone after the whole key, with an ID
    /// after that entry (05 §4.7, §6.4).
    #[test]
    fn entries_page_by_key_and_id() {
        let mut s = store(&[
            ("a", &["3", "2", "1"]),
            ("b", &["9"]),
            ("c/x", &["5"]),
            ("c/y", &["6"]),
        ]);
        let p = entries(&entry_req("", None, None, None, 2), &mut s).unwrap();
        assert_eq!(
            listed(&p),
            [("a".into(), "3".into()), ("a".into(), "2".into())]
        );
        assert_eq!(p.next, Some(("a".into(), "2".into())));
        let p = entries(&entry_req("", None, Some("a"), Some("2"), 2), &mut s).unwrap();
        assert_eq!(
            listed(&p),
            [("a".into(), "1".into()), ("b".into(), "9".into())]
        );
        // A key marker alone skips all of its key's entries.
        let p = entries(&entry_req("", None, Some("a"), None, 10), &mut s).unwrap();
        assert_eq!(p.entries[0].key, "b");
        // With a delimiter a key's entries roll up; a marker inside the prefix skips it.
        let p = entries(&entry_req("", Some("/"), None, None, 10), &mut s).unwrap();
        assert_eq!(p.common_prefixes, ["c/"]);
        assert_eq!(p.count(), 5);
        let p = entries(
            &entry_req("", Some("/"), Some("c/x"), Some("5"), 10),
            &mut s,
        )
        .unwrap();
        assert!(p.entries.is_empty() && p.common_prefixes.is_empty());
        // Following the markers lists everything once.
        let all = every_entry(&mut s, "", None, 1);
        assert_eq!(all.len(), 6);
        let all = every_entry(&mut s, "", Some("/"), 1);
        assert_eq!(all.last(), Some(&("c/".to_string(), String::new())));
        assert_eq!(all.len(), 5);
    }

    /// Keys that hold no entries pause a page, and the reserved marker resumes just past the
    /// last one, listing the common prefix it is in.
    #[test]
    fn entries_pause_and_resume_with_the_reserved_marker() {
        let mut keys: Vec<(String, Vec<String>)> = (0..1_500)
            .map(|i| (format!("p/{i:05}"), Vec::new()))
            .collect();
        keys.push(("p/z".into(), vec!["7".into()]));
        keys.push(("q".into(), vec!["8".into()]));
        let mut s = Store {
            keys: keys.into_iter().collect(),
            seeks: 0,
        };
        let p = entries(&entry_req("", Some("/"), None, None, 1000), &mut s).unwrap();
        assert!(p.truncated && p.entries.is_empty() && p.common_prefixes.is_empty());
        assert_eq!(p.next, Some(("p/00999".into(), PASSED.into())));
        let all = every_entry(&mut s, "", Some("/"), 1000);
        assert_eq!(
            all,
            [
                ("q".to_string(), "8".to_string()),
                ("p/".to_string(), String::new())
            ]
        );
    }

    /// AWS's ListMultipartUploads samples: a truncated page names where the next starts; one
    /// that is not names its last upload, and a page of common prefixes alone names nothing.
    #[test]
    fn upload_markers_follow_aws_samples() {
        let mut s = store(&[("my-divisor", &["x"]), ("my-movie.m2ts", &["v", "y"])]);
        let p = entries(&entry_req("", None, None, None, 2), &mut s).unwrap();
        assert_eq!(p.upload_markers(), ("my-movie.m2ts".into(), "v".into()));
        let mut s = store(&[
            ("photos/2006/a.jpg", &["1"]),
            ("sample.jpg", &["u"]),
            ("videos/2006/b.wmv", &["2"]),
        ]);
        let p = entries(&entry_req("", Some("/"), None, None, 1000), &mut s).unwrap();
        assert_eq!(p.common_prefixes, ["photos/", "videos/"]);
        assert_eq!(p.upload_markers(), ("sample.jpg".into(), "u".into()));
        let p = entries(&entry_req("photos/", Some("/"), None, None, 1000), &mut s).unwrap();
        assert_eq!(p.upload_markers(), (String::new(), String::new()));
    }

    #[test]
    fn entry_markers_outside_the_prefix_and_max_zero() {
        let mut s = store(&[("a", &["1"]), ("b", &["2"])]);
        // A key marker before the prefix starts at the prefix, after it lists nothing.
        let p = entries(&entry_req("b", None, Some("a"), Some("1"), 10), &mut s).unwrap();
        assert_eq!(listed(&p), [("b".into(), "2".into())]);
        let p = entries(&entry_req("a", None, Some("z"), None, 10), &mut s).unwrap();
        assert!(p.entries.is_empty() && !p.truncated);
        let p = entries(&entry_req("", None, None, None, 0), &mut s).unwrap();
        assert!(p.entries.is_empty() && !p.truncated && p.next.is_none());
        // An ID marker without a key marker is ignored (05 §4.7).
        let p = entries(&entry_req("", None, None, Some("1"), 10), &mut s).unwrap();
        assert_eq!(p.entries.len(), 2);
    }
}
