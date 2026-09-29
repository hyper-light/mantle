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

use mantle_s3::json::{self, Value};
use mantle_s3::policy::{KeyType, Kind, catalog};

fn text(value: &Value) -> &str {
    match value {
        Value::String(text) => text,
        other => panic!("not a string: {other:?}"),
    }
}

fn list(value: Option<&Value>) -> &[Value] {
    match value {
        Some(Value::Array(items)) => items,
        None => &[],
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
    for action in list(document.member("Actions")) {
        let kinds: Vec<Kind> = list(action.member("Resources"))
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
        let mut keys: Vec<String> = list(action.member("ActionConditionKeys"))
            .iter()
            .map(|key| text(key).to_owned())
            .filter(|key| key.starts_with("s3:"))
            .collect();
        keys.sort();
        expected.push((text(action.member("Name").unwrap()).to_owned(), *kind, keys));
    }
    expected.sort_by_key(|(name, _, _)| name.to_ascii_lowercase());
    let actual: Vec<(String, Kind, Vec<String>)> = catalog::ACTIONS
        .iter()
        .map(|action| {
            (
                action.name.to_owned(),
                action.kind,
                action.keys.iter().map(|key| (*key).to_owned()).collect(),
            )
        })
        .collect();
    assert_eq!(actual, expected);

    let mut keys: Vec<(String, KeyType)> = list(document.member("ConditionKeys"))
        .iter()
        .filter_map(|key| {
            let name = text(key.member("Name").unwrap());
            let kind = match text(&list(key.member("Types"))[0]) {
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
