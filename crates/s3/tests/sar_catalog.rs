//! The action and condition-key catalog (`mantle_s3::policy::catalog`) against the Service
//! Authorization Reference's S3 document it was written from, `tests/data/sar-s3.json`
//! (docs/research/17 §5), read with mantle's own JSON reader: regenerating the catalog with
//! `scripts/s3-actions.py` is the only way to change it.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros
)]

use std::path::Path;

use mantle_s3::json::{self, Value, View};
use mantle_s3::policy::{KeyType, Kind, catalog};

fn text(value: View<'_>) -> &str {
    match value {
        View::String(text) => text,
        other => panic!("not a string: {other:?}"),
    }
}

fn list(value: Option<View<'_>>) -> Vec<View<'_>> {
    match value {
        Some(View::Array(items)) => items.iter().collect(),
        None => Vec::new(),
        other => panic!("not a list: {other:?}"),
    }
}

fn document() -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sar-s3.json");
    json::parse(&std::fs::read(path).unwrap()).unwrap()
}

#[test]
fn the_catalog_is_the_references() {
    let document = document();
    assert_eq!(text(document.member("Version").unwrap()), "v1.4");
    let mut expected: Vec<(String, Kind, Vec<String>)> = Vec::new();
    // The actions on no resource at all, which only an identity policy names (research/91 §3).
    let mut accounts: Vec<(String, Kind, Vec<String>)> = Vec::new();
    for action in list(document.member("Actions")) {
        let mut keys: Vec<String> = list(action.member("ActionConditionKeys"))
            .iter()
            .map(|key| text(*key).to_owned())
            .filter(|key| key.starts_with("s3:"))
            .collect();
        keys.sort();
        let name = text(action.member("Name").unwrap()).to_owned();
        let resources = list(action.member("Resources"));
        if resources.is_empty() {
            accounts.push((name, Kind::Account, keys));
            continue;
        }
        let kinds: Vec<Kind> = resources
            .iter()
            .filter_map(|resource| match text(resource.member("Name").unwrap()) {
                "bucket" => Some(Kind::Bucket),
                "object" => Some(Kind::Object),
                _ => None,
            })
            .collect();
        let [kind] = kinds.as_slice() else {
            assert!(kinds.is_empty(), "{action:?}");
            continue;
        };
        expected.push((name, *kind, keys));
    }
    expected.sort_by_key(|(name, _, _)| name.to_ascii_lowercase());
    accounts.sort_by_key(|(name, _, _)| name.to_ascii_lowercase());
    let listed = |actions: &[mantle_s3::policy::Action]| -> Vec<(String, Kind, Vec<String>)> {
        actions
            .iter()
            .map(|action| {
                (
                    action.name.to_owned(),
                    action.kind,
                    action.keys.iter().map(|key| (*key).to_owned()).collect(),
                )
            })
            .collect()
    };
    assert_eq!(listed(&catalog::ACTIONS), expected);
    assert_eq!(listed(&catalog::ACCOUNT_ACTIONS), accounts);

    let mut keys: Vec<(String, KeyType)> = list(document.member("ConditionKeys"))
        .iter()
        .filter_map(|key| {
            let name = text(key.member("Name").unwrap());
            let kind = match text(list(key.member("Types"))[0]) {
                "ARN" => KeyType::Arn,
                "ArrayOfString" => KeyType::ArrayOfString,
                "Bool" => KeyType::Bool,
                "Date" => KeyType::Date,
                "Numeric" => KeyType::Numeric,
                "String" => KeyType::String,
                other => panic!("a type the catalog has no name for: {other}"),
            };
            name.starts_with("s3:").then(|| (name.to_owned(), kind))
        })
        .collect();
    keys.sort_by(|a, b| a.0.cmp(&b.0));
    let actual: Vec<(String, KeyType)> = catalog::KEYS
        .iter()
        .map(|(name, kind)| ((*name).to_owned(), *kind))
        .collect();
    assert_eq!(actual, keys);
}
