#!/usr/bin/env python3
"""Writes crates/s3/src/policy/catalog.rs from the Service Authorization Reference's S3
document, crates/s3/tests/data/sar-s3.json (docs/research/17 §5): the actions a bucket
policy can name, with the kind of resource each acts on and the S3 condition keys it
carries, and S3's condition keys with their types. tests/sar_catalog.rs checks the file
against the document."""

import json
import pathlib
import subprocess

ROOT = pathlib.Path(__file__).resolve().parent.parent

# The reference's key types, named as Rust names variants.
TYPES = {
    "ARN": "Arn",
    "ArrayOfString": "ArrayOfString",
    "Bool": "Bool",
    "Date": "Date",
    "Numeric": "Numeric",
    "String": "String",
}
SOURCE = ROOT / "crates/s3/tests/data/sar-s3.json"
TARGET = ROOT / "crates/s3/src/policy/catalog.rs"


def main():
    document = json.loads(SOURCE.read_text(encoding="utf-8"))
    actions = []
    for action in document["Actions"]:
        kinds = {r["Name"] for r in action.get("Resources", [])} & {"bucket", "object"}
        if not kinds:
            continue
        (kind,) = kinds
        keys = sorted(k for k in action.get("ActionConditionKeys", []) if k.startswith("s3:"))
        actions.append((action["Name"], kind, keys))
    actions.sort(key=lambda a: a[0].lower())
    keys = sorted(
        (k["Name"], k["Types"][0]) for k in document["ConditionKeys"] if k["Name"].startswith("s3:")
    )
    out = [
        "//! S3's actions and condition keys, as the Service Authorization Reference's S3",
        f"//! document, version {document['Version']}, lists them (docs/research/17 §5). Written by",
        "//! `scripts/s3-actions.py` from `tests/data/sar-s3.json`, which `tests/sar_catalog.rs`",
        "//! checks it against.",
        "",
        "use super::{Action, KeyType, Kind};",
        "",
        "/// The actions that act on a bucket or an object, which a bucket policy can name, in",
        "/// the order of their names without regard to case.",
        f"pub const ACTIONS: [Action; {len(actions)}] = [",
    ]
    for name, kind, action_keys in actions:
        listed = ", ".join(f'"{k}"' for k in action_keys)
        out.append(f"    Action {{")
        out.append(f'        name: "{name}",')
        out.append(f"        kind: Kind::{kind.capitalize()},")
        out.append(f"        keys: &[{listed}],")
        out.append(f"    }},")
    out.append("];")
    out.append("")
    out.append("/// S3's condition keys and their types; a name ending `/<key>` or `/${TagKey}` names a")
    out.append("/// family, one key for each tag key.")
    out.append(f"pub const KEYS: [(&str, KeyType); {len(keys)}] = [")
    for name, kind in keys:
        out.append(f'    ("{name}", KeyType::{TYPES[kind]}),')
    out.append("];")
    TARGET.parent.mkdir(parents=True, exist_ok=True)
    TARGET.write_text("\n".join(out) + "\n", encoding="utf-8")
    subprocess.run(["rustfmt", "--edition", "2024", str(TARGET)], check=True)


if __name__ == "__main__":
    main()
