#![allow(dead_code)]
#![allow(clippy::disallowed_types, clippy::duplicate_mod)]

use hyper_tls::client::{ClientConfig, ServerCertVerifierBuilder, WebPkiServerVerifier};
use hyper_tls::crypto::CryptoProvider;
use hyper_tls::server::{ClientCertVerifierBuilder, ServerConfig, WebPkiClientVerifier};
use hyper_tls::RootCertStore;
pub use rustls_test::*;

pub fn server_config_builder(
    provider: &CryptoProvider,
) -> hyper_tls::ConfigBuilder<ServerConfig, hyper_tls::WantsVerifier> {
    // ensure `ServerConfig::builder()` is covered, even though it is
    // equivalent to `builder_with_provider(provider::provider().into())`.
    if exactly_one_provider() {
        hyper_tls::ServerConfig::builder().unwrap()
    } else {
        hyper_tls::ServerConfig::builder_with_provider(static_provider(provider.clone()))
            .with_safe_default_protocol_versions()
            .unwrap()
    }
}

pub fn server_config_builder_with_versions(
    versions: &[&'static hyper_tls::SupportedProtocolVersion],
    provider: &CryptoProvider,
) -> hyper_tls::ConfigBuilder<ServerConfig, hyper_tls::WantsVerifier> {
    if exactly_one_provider() {
        hyper_tls::ServerConfig::builder_with_protocol_versions(versions).unwrap()
    } else {
        hyper_tls::ServerConfig::builder_with_provider(static_provider(provider.clone()))
            .with_protocol_versions(versions)
            .unwrap()
    }
}

pub fn client_config_builder(
    provider: &CryptoProvider,
) -> hyper_tls::ConfigBuilder<ClientConfig, hyper_tls::WantsVerifier> {
    // ensure `ClientConfig::builder()` is covered, even though it is
    // equivalent to `builder_with_provider(provider::provider().into())`.
    if exactly_one_provider() {
        hyper_tls::ClientConfig::builder().unwrap()
    } else {
        hyper_tls::ClientConfig::builder_with_provider(static_provider(provider.clone()))
            .with_safe_default_protocol_versions()
            .unwrap()
    }
}

pub fn client_config_builder_with_versions(
    versions: &[&'static hyper_tls::SupportedProtocolVersion],
    provider: &CryptoProvider,
) -> hyper_tls::ConfigBuilder<ClientConfig, hyper_tls::WantsVerifier> {
    if exactly_one_provider() {
        hyper_tls::ClientConfig::builder_with_protocol_versions(versions).unwrap()
    } else {
        hyper_tls::ClientConfig::builder_with_provider(static_provider(provider.clone()))
            .with_protocol_versions(versions)
            .unwrap()
    }
}

pub fn webpki_client_verifier_builder(
    roots: RootCertStore,
    provider: &CryptoProvider,
) -> ClientCertVerifierBuilder {
    if exactly_one_provider() {
        WebPkiClientVerifier::builder(roots)
    } else {
        WebPkiClientVerifier::builder_with_provider(roots, provider)
    }
}

pub fn webpki_server_verifier_builder(
    roots: RootCertStore,
    provider: &CryptoProvider,
) -> ServerCertVerifierBuilder {
    if exactly_one_provider() {
        WebPkiServerVerifier::builder(roots)
    } else {
        WebPkiServerVerifier::builder_with_provider(roots, provider)
    }
}

fn exactly_one_provider() -> bool {
    true
}
