//! Listing a bucket's keys (docs/research/05 §6): a prefix, roll-ups of keys into common
//! prefixes at a delimiter, a limit on what one page returns, and where the next page starts.
//!
//! Keys are in UTF-8 byte order (05 §6.1). A page counts each common prefix as one item, "All
//! of the keys that roll up into a common prefix count as a single return", and passes the rest
//! of a common prefix with one seek to the first key beyond it, so a prefix holding millions of
//! keys costs one step and a page at most `max-keys + 1` seeks. The next page starts after the
//! last item returned, key or common prefix.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

/// An ordered index to list from: the keys a listing shows, which leaves out a key whose
/// current version is a delete marker (05 §6.4).
pub trait Keys {
    type Value;
    type Error;
    /// The first key at or after `from` in byte order, with its value.
    fn seek(&mut self, from: &[u8]) -> Result<Option<(String, Self::Value)>, Self::Error>;
}

/// What one page asks for.
#[derive(Debug, Clone, Copy)]
pub struct Request<'a> {
    pub prefix: &'a str,
    /// No delimiter when `None` or empty.
    pub delimiter: Option<&'a str>,
    /// Start strictly after this key: `start-after`, `marker`, or a continuation.
    pub after: Option<&'a str>,
    pub max_keys: usize,
}

/// One page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page<V> {
    pub contents: Vec<(String, V)>,
    pub common_prefixes: Vec<String>,
    pub truncated: bool,
    /// The last key or common prefix returned: V1's `NextMarker`, and where the next page
    /// starts.
    pub last: Option<String>,
}

impl<V> Page<V> {
    /// `KeyCount`: keys and common prefixes, which ceph s3-tests counts together (05 §6.2).
    pub fn key_count(&self) -> usize {
        self.contents
            .len()
            .saturating_add(self.common_prefixes.len())
    }

    /// `NextContinuationToken`, when truncated.
    pub fn continuation(&self) -> Option<String> {
        self.truncated
            .then(|| self.last.as_deref().map(continuation_token))
            .flatten()
    }
}

/// `max-keys`: "By default, the action returns up to 1,000 key names. The response might
/// contain fewer keys but will never contain more" (05 §6.2).
pub const MAX_KEYS: usize = 1000;

/// One page of `keys`.
pub fn page<K: Keys>(request: &Request<'_>, keys: &mut K) -> Result<Page<K::Value>, K::Error> {
    let mut page = Page {
        contents: Vec::new(),
        common_prefixes: Vec::new(),
        truncated: false,
        last: None,
    };
    let max = request.max_keys.min(MAX_KEYS);
    if max == 0 {
        // ceph s3-tests: MaxKeys=0 lists nothing and is not truncated (05 §6.2).
        return Ok(page);
    }
    let prefix = request.prefix;
    let delimiter = request.delimiter.filter(|d| !d.is_empty());
    // A common prefix "is filtered out from results if it is not lexicographically greater
    // than the `StartAfter` value" (05 §6.2). The one `after` rolls up into is a prefix of
    // `after`, so not greater, and every key under it rolls up into it: the page starts
    // beyond it. Every other common prefix the page meets is greater than `after`.
    let mut from = match request.after {
        Some(after) if after >= prefix => match rolled_up(after, prefix, delimiter) {
            Some(common) => beyond(common),
            None => just_after(after),
        },
        _ => prefix.as_bytes().to_vec(),
    };
    while let Some((key, value)) = keys.seek(&from)? {
        if !key.starts_with(prefix) {
            break;
        }
        if page.key_count() >= max {
            page.truncated = true;
            break;
        }
        if let Some(common) = rolled_up(&key, prefix, delimiter) {
            from = beyond(common);
            page.last = Some(common.to_owned());
            page.common_prefixes.push(common.to_owned());
        } else {
            from = just_after(&key);
            page.last = Some(key.clone());
            page.contents.push((key, value));
        }
    }
    Ok(page)
}

/// The common prefix `key` rolls up into: through the first delimiter after `prefix`.
fn rolled_up<'k>(key: &'k str, prefix: &str, delimiter: Option<&str>) -> Option<&'k str> {
    let d = delimiter?;
    let at = key.strip_prefix(prefix)?.find(d)?;
    key.get(..prefix.len().checked_add(at)?.checked_add(d.len())?)
}

/// The smallest byte string after `key`: `key` followed by a zero byte.
fn just_after(key: &str) -> Vec<u8> {
    let mut next = key.as_bytes().to_vec();
    next.push(0);
    next
}

/// No UTF-8 string contains the byte 0xFF, so `prefix` followed by it sorts after every key
/// that starts with `prefix` and before every other key after them.
fn beyond(prefix: &str) -> Vec<u8> {
    let mut next = prefix.as_bytes().to_vec();
    next.push(0xFF);
    next
}

/// A continuation token: opaque to clients, "not a real key" (05 §6.2). It names where the
/// next page starts; a token a client alters only moves that start, as `start-after` could.
pub fn continuation_token(last: &str) -> String {
    URL_SAFE_NO_PAD.encode(last.as_bytes())
}

/// The key a continuation token starts after; `None` if it is not one of mantle's.
pub fn continuation_start(token: &str) -> Option<String> {
    String::from_utf8(URL_SAFE_NO_PAD.decode(token.trim()).ok()?).ok()
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
    use std::collections::BTreeSet;
    use std::convert::Infallible;

    struct Index {
        keys: BTreeSet<Vec<u8>>,
        seeks: usize,
    }

    impl Keys for Index {
        type Value = ();
        type Error = Infallible;
        fn seek(&mut self, from: &[u8]) -> Result<Option<(String, ())>, Infallible> {
            self.seeks += 1;
            Ok(self
                .keys
                .range(from.to_vec()..)
                .next()
                .map(|k| (String::from_utf8(k.clone()).unwrap(), ())))
        }
    }

    fn index<S: AsRef<str>>(keys: &[S]) -> Index {
        Index {
            keys: keys
                .iter()
                .map(|k| k.as_ref().as_bytes().to_vec())
                .collect(),
            seeks: 0,
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
            after,
            max_keys: max,
        }
    }

    fn list(ix: &mut Index, r: Request<'_>) -> Page<()> {
        page(&r, ix).unwrap()
    }

    fn keys(p: &Page<()>) -> Vec<&str> {
        p.contents.iter().map(|(k, _)| k.as_str()).collect()
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
        let mut after: Option<String> = None;
        let mut pages = Vec::new();
        loop {
            ix.seeks = 0;
            let p = list(&mut ix, req("", Some("/"), after.as_deref(), 1));
            assert!(ix.seeks <= 2, "{} seeks", ix.seeks);
            pages.push((
                keys(&p).iter().map(|k| k.to_string()).collect::<Vec<_>>(),
                p.common_prefixes.clone(),
            ));
            if !p.truncated {
                break;
            }
            after = continuation_start(&p.continuation().unwrap());
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
        let mut seen = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let after = token.as_deref().and_then(continuation_start);
            let p = list(&mut ix, req("", None, after.as_deref(), usize::MAX));
            assert!(p.key_count() <= MAX_KEYS);
            seen.extend(p.contents.into_iter().map(|(k, _)| k));
            match p.truncated {
                true => token = Some(continuation_token(p.last.as_deref().unwrap())),
                false => break,
            }
        }
        assert_eq!(seen, all);
    }

    #[test]
    fn index_errors_reach_the_caller() {
        struct Broken;
        impl Keys for Broken {
            type Value = ();
            type Error = &'static str;
            fn seek(&mut self, _: &[u8]) -> Result<Option<(String, ())>, &'static str> {
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
        assert_eq!(
            continuation_start(&continuation_token("a/é b")),
            Some("a/é b".into())
        );
        assert_eq!(continuation_start("not base64!"), None);
        assert_eq!(max_keys(Some("blah")), Err(InvalidMaxKeys));
        assert_eq!(max_keys(Some("-1")), Err(InvalidMaxKeys));
        assert_eq!(max_keys(Some("5")), Ok(5));
        assert_eq!(max_keys(None), Ok(1000));
    }
}
