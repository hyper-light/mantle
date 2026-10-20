//! Tests for configuring and using a [`ServerCertVerifier`] for a client.

#![allow(clippy::disallowed_types, clippy::duplicate_mod)]

use super::*;

mod common;

use common::{
    client_config_builder, do_handshake, do_handshake_until_both_error, do_handshake_until_error,
    make_client_config_with_versions, make_pair_for_configs, make_server_config,
    server_config_builder, ErrorFromPeer, KeyType, MockServerVerifier, Shared,
};
use hyper_tls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use hyper_tls::client::WebPkiServerVerifier;
use hyper_tls::server::{ClientHello, ResolvesServerCert};
use hyper_tls::sign::CertifiedKey;
use hyper_tls::{
    AlertDescription, CertificateError, DigitallySignedStruct, DistinguishedName, Error,
    InvalidMessage, RootCertStore,
};
use pki_types::{CertificateDer, ServerName};
use x509_parser::prelude::FromDer;
use x509_parser::x509::X509Name;

#[test]
fn client_can_override_certificate_verification() {
    let provider = provider::default_provider();
    for kt in KeyType::all_for_provider(&provider).iter() {
        let verifier = MockServerVerifier::accepts_anything();

        let server_config = Shared::new(make_server_config(*kt, &provider));

        for version in hyper_tls::ALL_VERSIONS {
            let mut client_config = make_client_config_with_versions(*kt, &[version], &provider);
            client_config
                .dangerous()
                .set_certificate_verifier(Box::new(verifier.clone()));

            let (mut client, mut server) =
                make_pair_for_configs(client_config, server_config.clone());
            do_handshake(&mut client, &mut server);
        }
    }
}

#[test]
fn client_can_override_certificate_verification_and_reject_certificate() {
    let provider = provider::default_provider();
    for kt in KeyType::all_for_provider(&provider).iter() {
        let verifier = MockServerVerifier::rejects_certificate(Error::InvalidMessage(
            InvalidMessage::HandshakePayloadTooLarge,
        ));

        let server_config = Shared::new(make_server_config(*kt, &provider));

        for version in hyper_tls::ALL_VERSIONS {
            let mut client_config = make_client_config_with_versions(*kt, &[version], &provider);
            client_config
                .dangerous()
                .set_certificate_verifier(Box::new(verifier.clone()));

            let (mut client, mut server) =
                make_pair_for_configs(client_config, server_config.clone());
            let errs = do_handshake_until_both_error(&mut client, &mut server);
            assert_eq!(
                errs,
                Err(vec![
                    ErrorFromPeer::Client(Error::InvalidMessage(
                        InvalidMessage::HandshakePayloadTooLarge,
                    )),
                    ErrorFromPeer::Server(Error::AlertReceived(AlertDescription::HandshakeFailure)),
                ]),
            );
        }
    }
}

#[test]
fn client_can_override_certificate_verification_and_reject_tls12_signatures() {
    let provider = provider::default_provider();
    for kt in KeyType::all_for_provider(&provider).iter() {
        let mut client_config =
            make_client_config_with_versions(*kt, &[&hyper_tls::version::TLS12], &provider);
        let verifier = MockServerVerifier::rejects_tls12_signatures(Error::InvalidMessage(
            InvalidMessage::HandshakePayloadTooLarge,
        ));

        client_config
            .dangerous()
            .set_certificate_verifier(Box::new(verifier));

        let server_config = Shared::new(make_server_config(*kt, &provider));

        let (mut client, mut server) = make_pair_for_configs(client_config, server_config.clone());
        let errs = do_handshake_until_both_error(&mut client, &mut server);
        assert_eq!(
            errs,
            Err(vec![
                ErrorFromPeer::Client(Error::InvalidMessage(
                    InvalidMessage::HandshakePayloadTooLarge,
                )),
                ErrorFromPeer::Server(Error::AlertReceived(AlertDescription::HandshakeFailure)),
            ]),
        );
    }
}

#[test]
fn client_can_override_certificate_verification_and_reject_tls13_signatures() {
    let provider = provider::default_provider();
    for kt in KeyType::all_for_provider(&provider).iter() {
        let mut client_config = make_client_config_with_versions(
            *kt,
            &[&hyper_tls::version::TLS13],
            &provider::default_provider(),
        );
        let verifier = MockServerVerifier::rejects_tls13_signatures(Error::InvalidMessage(
            InvalidMessage::HandshakePayloadTooLarge,
        ));

        client_config
            .dangerous()
            .set_certificate_verifier(Box::new(verifier));

        let server_config = Shared::new(make_server_config(*kt, &provider));

        let (mut client, mut server) = make_pair_for_configs(client_config, server_config.clone());
        let errs = do_handshake_until_both_error(&mut client, &mut server);
        assert_eq!(
            errs,
            Err(vec![
                ErrorFromPeer::Client(Error::InvalidMessage(
                    InvalidMessage::HandshakePayloadTooLarge,
                )),
                ErrorFromPeer::Server(Error::AlertReceived(AlertDescription::HandshakeFailure)),
            ]),
        );
    }
}

#[test]
fn client_can_override_certificate_verification_and_offer_no_signature_schemes() {
    let provider = provider::default_provider();
    for kt in KeyType::all_for_provider(&provider).iter() {
        let verifier = MockServerVerifier::offers_no_signature_schemes();

        let server_config = Shared::new(make_server_config(*kt, &provider));

        for version in hyper_tls::ALL_VERSIONS {
            let mut client_config = make_client_config_with_versions(*kt, &[version], &provider);
            client_config
                .dangerous()
                .set_certificate_verifier(Box::new(verifier.clone()));

            let (mut client, mut server) =
                make_pair_for_configs(client_config, server_config.clone());
            let errs = do_handshake_until_both_error(&mut client, &mut server);
            assert_eq!(
                errs,
                Err(vec![
                    ErrorFromPeer::Server(Error::InvalidMessage(
                        hyper_tls::InvalidMessage::NoSignatureSchemes
                    )),
                    ErrorFromPeer::Client(Error::AlertReceived(AlertDescription::DecodeError)),
                ])
            );
        }
    }
}

#[test]
fn client_can_request_certain_trusted_cas() {
    let provider = provider::default_provider();
    // These keys have CAs with different names, which our test needs.
    // They also share the same sigalgs, so the server won't pick one over the other based on sigalgs.
    let key_types = [KeyType::Rsa2048, KeyType::Rsa3072, KeyType::Rsa4096];
    let cert_resolver = ResolvesCertChainByCaName(
        key_types
            .iter()
            .map(|kt| {
                (
                    kt.ca_distinguished_name().to_vec().into(),
                    kt.certified_key_with_cert_chain(&provider).unwrap(),
                )
            })
            .collect(),
    );

    let server_config = Shared::new(
        server_config_builder(&provider)
            .with_no_client_auth()
            .with_cert_resolver(Box::new(cert_resolver)),
    );

    let mut cas_unaware_error_count = 0;

    for key_type in key_types {
        let mut root_store = RootCertStore::empty();
        root_store.add(key_type.ca_cert()).unwrap();
        // Each configuration owns its verifier, so the same verifier is built for both.
        let server_verifier_builder =
            WebPkiServerVerifier::builder_with_provider(root_store, &provider);
        let server_verifier = server_verifier_builder.clone().build().unwrap();

        let cas_sending_server_verifier = Box::new(ServerCertVerifierWithCasExt {
            verifier: server_verifier_builder.build().unwrap(),
            ca_names: vec![DistinguishedName::from(
                key_type.ca_distinguished_name().to_vec(),
            )],
        });

        let cas_sending_client_config = client_config_builder(&provider)
            .dangerous()
            .with_custom_certificate_verifier(cas_sending_server_verifier)
            .with_no_client_auth();

        let (mut client, mut server) =
            make_pair_for_configs(cas_sending_client_config, server_config.clone());
        do_handshake(&mut client, &mut server);

        let cas_unaware_client_config = client_config_builder(&provider)
            .dangerous()
            .with_custom_certificate_verifier(server_verifier)
            .with_no_client_auth();

        let (mut client, mut server) =
            make_pair_for_configs(cas_unaware_client_config, server_config.clone());

        cas_unaware_error_count += do_handshake_until_error(&mut client, &mut server)
            .inspect_err(|e| {
                assert!(matches!(
                    e,
                    ErrorFromPeer::Client(Error::InvalidCertificate(
                        CertificateError::UnknownIssuer
                    ))
                ))
            })
            .is_err() as usize;

        println!("key type {key_type:?} success!");
    }

    // For cas_unaware clients, all of them should fail except one that happens to
    // have the cert the server sends
    assert_eq!(cas_unaware_error_count, key_types.len() - 1);
}

#[derive(Debug)]
pub struct ResolvesCertChainByCaName(Vec<(DistinguishedName, CertifiedKey)>);

impl ResolvesServerCert for ResolvesCertChainByCaName {
    fn resolve(&self, client_hello: ClientHello<'_>) -> Option<&CertifiedKey> {
        let Some(cas_extension) = client_hello.certificate_authorities() else {
            println!(
                "ResolvesCertChainByCaName: no CAs extension in ClientHello, returning default cert"
            );
            return Some(&self.0[0].1);
        };
        for (name, certified_key) in self.0.iter() {
            let name = X509Name::from_der(name.as_ref()).unwrap().1;
            if cas_extension.iter().any(|ca_name| {
                X509Name::from_der(ca_name.as_ref()).is_ok_and(|(_, ca_name)| ca_name == name)
            }) {
                println!("ResolvesCertChainByCaName: found matching CA name: {name}");
                return Some(certified_key);
            }
        }
        println!("ResolvesCertChainByCaName: no matching CA name found, returning default Cert");
        Some(&self.0[0].1)
    }
}

#[derive(Debug)]
struct ServerCertVerifierWithCasExt {
    verifier: Box<dyn ServerCertVerifier>,
    ca_names: Vec<DistinguishedName>,
}

impl ServerCertVerifier for ServerCertVerifierWithCasExt {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: pki_types::UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        self.verifier
            .verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.verifier.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.verifier.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<hyper_tls::SignatureScheme> {
        self.verifier.supported_verify_schemes()
    }

    fn requires_raw_public_keys(&self) -> bool {
        self.verifier.requires_raw_public_keys()
    }

    fn root_hint_subjects(&self) -> Option<&[DistinguishedName]> {
        println!("ServerCertVerifierWithCasExt::root_hint_subjects() called!");
        Some(&self.ca_names)
    }
}
