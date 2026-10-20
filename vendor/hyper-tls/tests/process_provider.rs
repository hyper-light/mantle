//! The process-default provider is the crate's one provider, aws-lc-rs. The default test runner
//! builds each test file into its own executable, so this test sees a fresh process.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::todo,
    clippy::unimplemented,
    clippy::indexing_slicing,
    clippy::string_slice,
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::disallowed_macros,
    clippy::disallowed_types,
    clippy::disallowed_methods,
    clippy::cognitive_complexity,
    clippy::unwrap_in_result,
    clippy::panic_in_result_fn,
    clippy::missing_panics_doc,
    clippy::dbg_macro,
    unreachable_pub
)]

use hyper_tls::crypto::CryptoProvider;
use hyper_tls::ClientConfig;

mod common;
use crate::common::*;

#[test]
fn test_aws_lc_rs_used_as_implicit_provider() {
    assert!(CryptoProvider::get_default().is_none());

    // implicitly installs aws-lc-rs provider
    finish_client_config(KeyType::Rsa2048, ClientConfig::builder().unwrap());

    let default = CryptoProvider::get_default().expect("provider missing");
    let debug = format!("{default:?}");
    assert!(debug.contains("secure_random: AwsLcRs"));

    let builder = ClientConfig::builder().unwrap();
    assert_eq!(format!("{:?}", builder.crypto_provider()), debug);
}
