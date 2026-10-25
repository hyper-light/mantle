# 71. The S3 endpoint's key, profile and certificate chain

Status: research note, 2026-10-25. For `mantle init`'s first access key, the S3 endpoint's
certificates and the operator's profile, and for the endpoint's TLS (node.md §1.5, §4.1). Sources
[1] to [19] were read from documents and source code, each quoted where it was read; no handshake
was run against a mantle endpoint. Links with line anchors are pinned to the commit read that day.

## The question

Mantle's cell authority and node certificates are Ed25519 (RFC 8410), used for mutual TLS 1.3
between mantle's own nodes. The S3 listener serves HTTP/1.1 over TLS to stock S3 clients with a
chain mantle issues itself, the operator giving clients the authority as a PEM `ca_bundle`, and
those clients sign with an access key mantle issues too. Three things follow. Which signature
algorithm the endpoint's leaf and its issuing authority use: Ed25519, ECDSA P-256
(`ecdsa_secp256r1_sha256`) or RSA. What the endpoint's certificate must carry for the verifiers
stock clients run. And how stock clients take a key, an endpoint and its authority: the files and
settings they read, and the forms a key and its account take.

The two halves of the chain fail differently. An Ed25519 leaf needs the client's TLS stack to
accept `ed25519` for the server's handshake signature (TLS 1.3 CertificateVerify, TLS 1.2
ServerKeyExchange). An Ed25519 authority over a P-256 leaf needs only the client's certificate
verifier to check an Ed25519 signature, because the issuer's key makes the leaf's signature; only
a trust anchor's own self-signature is exempt [1].

## Sources

[1] RFC 8446, https://www.rfc-editor.org/rfc/rfc8446. §9.1: "A TLS-compliant application MUST support digital
signatures with rsa_pkcs1_sha256 (for certificates), rsa_pss_rsae_sha256 (for CertificateVerify and certificates), and
ecdsa_secp256r1_sha256." §4.2.3: "If no "signature_algorithms_cert" extension is present, then the
"signature_algorithms" extension also applies to signatures appearing in certificates." §4.4.2.2: "Certificates that
are self-signed or certificates that are expected to be trust anchors are not validated as part of the chain and
therefore MAY be signed with any algorithm."

[2] AWS CLI v2, https://awscli.amazonaws.com/v2/documentation/api/latest/topic/s3-config.html (source
https://github.com/aws/aws-cli/blob/da4fc51eb10ebd1e2a21ec9877335a9de1fdb7b6/awscli/topics/s3-config.rst#L326-L371):
`preferred_transfer_client` "**Default** - ``auto``"; "Currently, it resolves to ``crt`` when all of the following
criteria is met:", among them "The ``s3`` command used is not an S3 to S3 copy transfer." and "There are no other
running processes of the AWS CLI using the CRT S3 transfer client." CHANGELOG.rst (same commit), 2.38.0, tagged
2026-10-13 (#L124-L135): "feature:crt: Enable the CRT S3 transfer client by default on all hosts."; 2.38.5, tagged
2026-10-22 (#L16): "Update bundled OpenSSL version to ``3.5.9`` for all installers." The CRT client always gets a CA
file: awscli/customizations/s3/factory.py#L530-L536 "verify = self._session.get_config_variable('ca_bundle')", then
"verify = DEFAULT_CA_BUNDLE"; awscli/s3transfer/crt.py#L210-L211
"tls_ctx_options.override_default_trust_store_from_path(" "ca_filepath=verify".

[3] AWS Common Runtime TLS, aws-c-io (https://github.com/awslabs/aws-c-io/blob/35b0fd2ba8823c59cfca966f78f180afdefc9c0f).
README#L218-L221: "Linux | Signal-to-noise (s2n)", "Apple Devices | Security Framework/ Secure Transport", "Windows |
Secure Channel". README#L803: "If unset or empty, Apple Secure Transport is used by default (which provides
FIPS-validated TLS but lacks TLS 1.3 support)." source/darwin/secure_transport_tls_channel_handler.c#L53: "/* Even
though TLS 1.3 can be configured in SecureTransport, it never actually worked. */"; a CA file goes to
"SecTrustSetAnchorCertificates(trust, secure_transport_handler->ca_certs)" (#L483). source/windows/
secure_channel_tls_handler.c#L2462-L2464: "if (options->verify_peer && aws_tls_options_buf_is_set(&options->ca_file))
{" … "dw_flags |= SCH_CRED_MANUAL_CRED_VALIDATION;", then "engine_config.hExclusiveRoot = sc_handler->custom_ca_store;"
(#L311) and "CertGetCertificateChain(" (#L342). source/s2n/s2n_tls_channel_handler.c#L1625-L1627 default policy:
"security_policy = "AWS-CRT-SDK-TLSv1.0-2025-PQ";". aws-crt-python README
(https://github.com/awslabs/aws-crt-python/blob/a964bef3b361abf14eea49028e54ccbb2d60a7e5/README.md): "| TLS versions |
Up to TLS 1.2 | Up to TLS 1.3 |" (#L68, Secure Transport, the default, against s2n-tls); "On Windows, the OS's default
TLS library (Schannel) is used." (#L137); "Code to build libcrypto comes from AWS-LC." (#L145). Mountpoint
(https://github.com/awslabs/mountpoint-s3/blob/9e23c24e/README.md#L96): "The AWS Common Runtime for interacting with S3".

[4] s2n-tls (https://github.com/aws/s2n-tls/blob/7ab0ce61d9c06d1cedb2c1ca246bd6ae85b8a3ec). crypto/s2n_pkey.h#L26-L33,
the key types: "S2N_PKEY_TYPE_RSA = 0," "S2N_PKEY_TYPE_ECDSA," "S2N_PKEY_TYPE_RSA_PSS," "S2N_PKEY_TYPE_MLDSA,".
crypto/s2n_pkey.c#L254-L270: "case EVP_PKEY_RSA:" "case EVP_PKEY_RSA_PSS:" "case EVP_PKEY_EC:" (ML-DSA under a build
flag) "default:" "RESULT_BAIL(S2N_ERR_DECODE_CERTIFICATE);", applied to the server's leaf at
tls/s2n_x509_validator.c#L1004 "RESULT_GUARD(s2n_pkey_from_x509(leaf_cert, &public_key, pkey_type));". The scheme
declarations (tls/s2n_signature_scheme.h#L49-L82) are RSA PKCS1, ECDSA, RSA-PSS and ML-DSA, none Ed25519;
`TLS_SIGNATURE_SCHEME_ED25519` is defined as `0x0807` (tls/s2n_tls_parameters.h#L147) and used by no scheme. The
AWS-CRT-SDK policies use ".signature_preferences = &s2n_signature_preferences_20200207," (tls/s2n_security_policies.c
#L944-L950; one uses the 20240501 list), which includes "&s2n_ecdsa_sha256," (tls/s2n_signature_scheme.c#L381), and
none sets certificate signature or key preferences. tls/s2n_x509_validator.c#L453-L455: "We only restrict the
signature algorithm on the certificates in the peer's certificate chain if the certificate_signature_preferences field
is set in the security policy." Usage guide chart (docs/usage-guide/topics/ch06-security-policies.md#L88): "| version |
RSA PKCS1 | ECDSA | SHA-1 Legacy | RSA PSS |".

[5] AWS-LC, the CRT's libcrypto
(https://github.com/aws/aws-lc/blob/cd481c207e38f06aeb5766f4a1cdfce10a4c0af8/crypto/x509/algorithm.c#L114-L119): "if
(sigalg_nid == NID_ED25519 || sigalg_nid == NID_MLDSA44 ||" … "return EVP_DigestVerifyInit(ctx, NULL, NULL, NULL,
pkey);".

[6] botocore (https://github.com/boto/botocore/blob/fecd1e19cf8e1c04b66480397455e1458867bf6d/botocore/httpsession.py#L113-L130):
"We vendor this function to ensure that the SSL contexts we construct always use the std lib SSLContext instead of
pyopenssl." It sets protocol, ciphers and options, no signature algorithms (read). s3cmd
(https://github.com/s3tools/s3cmd/blob/cee84f9c539a7bbf5ee73c7bf29a47632119c0c6/S3/ConnMan.py#L49): "context =
ssl.create_default_context(cafile=cafile)".

[7] OpenSSL. NEWS, "Major changes between OpenSSL 1.1.0i and OpenSSL 1.1.1 [11 Sep 2018]"
(https://github.com/openssl/openssl/blob/b372b1f76450acdfed1e2301a39810146e28b02c/NEWS#L156-L184): "EdDSA (both Ed25519
and Ed448) including X509 and TLS support". Default signature list in 3.0
(https://github.com/openssl/openssl/blob/e9e85dd09d0eabc9e5437007c6bd033d4ef8b6b1/ssl/t1_lib.c#L952-L957):
"TLSEXT_SIGALG_ecdsa_secp256r1_sha256," … "TLSEXT_SIGALG_ed25519,". 1.0.2
(https://github.com/openssl/openssl/blob/12ad22dd16ffe47f8cde3cddb84a160e8cdb3e30/doc/apps/ciphers.pod#L578):
"TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256   ECDHE-ECDSA-AES128-GCM-SHA256".

[8] Python builds. PEP 644 (https://peps.python.org/pep-0644/, Python-Version 3.10): "This PEP proposes for CPython’s
standard library to support only OpenSSL 1.1.1 LTS or newer." and "Python 3.10 will no longer support TLS/SSL and fast
hashing on platforms with OpenSSL 1.0.2 or LibreSSL." python.org installers, Mac/BuildScript/build-installer.py#L249:
"name="OpenSSL 3.5.9"," on cpython branches 3.13 (42155a0f) and 3.14 (d612e0d6), "name="OpenSSL 3.0.16"," on 3.12
(b6664462); PCbuild/get_externals.bat: "openssl-bin-3.5.9" (3.13 #L80, 3.14 #L82), "openssl-bin-3.0.16.2" (3.12 #L80).
Amazon Linux (https://docs.aws.amazon.com/linux/al2023/ug/openssl3.html): "By default, AL2 comes with OpenSSL 1.0.2."

[9] LibreSSL. ChangeLog, release 3.7.0
(https://github.com/libressl/portable/blob/46e634475c4b6e0f0ebd343226bfdcc673da7a0c/ChangeLog#L1017): "Added Ed25519
support both as a primitive and via OpenSSL's EVP interfaces." The TLS signature table
(https://github.com/libressl/openbsd/blob/ad3418a862e15ac14ce89ecb3e1989ab9e8739aa/src/lib/libssl/ssl_sigalgs.c#L30-L140)
has RSA PKCS1, ECDSA (".value = SIGALG_ECDSA_SECP256R1_SHA256," #L64), RSA-PSS and SHA-1 entries, no Ed25519 (read).
Local observation on this macOS 26 machine: Xcode's `/usr/bin/python3` reports `3.9.6 | LibreSSL 2.8.3`; `/usr/bin/openssl`
(LibreSSL 3.3.6) decoding an Ed25519 certificate prints "Unable to load Public Key" and "unsupported algorithm".

[10] Microsoft Learn. CNG Algorithm Identifiers (https://learn.microsoft.com/en-us/windows/win32/seccng/cng-algorithm-identifiers,
updated 2026-05-30): the signature algorithms listed are DSA, ECDSA ("Generic prime elliptic curve digital signature
algorithm"; ECDSA_P256 "The 256-bit prime elliptic curve digital signature algorithm (FIPS 186-2)."), RSA, ML-DSA
("Support for this algorithm begins with Windows 11, version 24H2."), a prerelease Composite-ML-DSA, and LMS, XMSS,
SLH-DSA ("not currently implemented by the default CNG provider"); no EdDSA identifier exists. CNG Named Elliptic Curves
(https://learn.microsoft.com/en-us/windows/win32/seccng/cng-named-elliptic-curves): curve25519 has "Object identifier |
None" and an empty "TLS capable". Schannel CRYPTO_SETTINGS
(https://learn.microsoft.com/en-us/windows/win32/api/schannel/ns-schannel-crypto_settings): "TlsParametersCngAlgUsageCertSig
| Signature and/or hash used to sign certificate. (e.g. RSA, DSA, ECDSA, SHA1, SHA256)"; strCngAlgId is "The CNG
algorithm identifier." TLS Cipher Suites in Windows 11
(https://learn.microsoft.com/en-us/windows/win32/secauthn/tls-cipher-suites-in-windows-11), enabled by default:
"TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384 | Yes | TLS 1.2", "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256 | Yes | TLS 1.2".
Protocols in TLS/SSL (https://learn.microsoft.com/en-us/windows/win32/secauthn/protocols-in-tls-ssl--schannel-ssp-):
"TLS 1.3 is supported starting in Windows 11 and Windows Server 2022."

[11] Field reports from Windows (third parties, not Microsoft). https://github.com/tailscale/tailscale/issues/21679
(opened 2026-10-06, corrected 2026-10-22; fix PR #21680 open): "Schannel fails the handshake when the chain contains
an Ed25519 certificate and the client uses manual validation. This happens over TLS 1.3 and 1.2, even when the client
accepts any certificate."; ".NET `SslStream`, accept-all callback, TLS 1.3 and 1.2 | The token supplied to the function
is invalid (.NET 10 and .NET Framework 4.8)"; "Reproduced on Windows Server 2022 (20348), Windows Server 2025 (26100),
Windows 11 24H2 (26100) and Windows 11 25H2 (26200)." A maintainer could not reproduce the first, wrong report; under
automatic validation an extra Ed25519 certificate outside the path passed. Also
https://github.com/rustls/rustls-platform-verifier/issues/195 (2025-09-29, open), an Ed25519
certificate on Windows: "Verify result: Err(General("Invalid algorithm specified. (os error -2146893816)"))", "The
error seems to occur during chain construction (`CertGetCertificateChain`)".

[12] Apple documentation. https://support.apple.com/en-us/103769 ("Requirements for trusted certificates in iOS 13 and
macOS 10.15"): "TLS server certificates and issuing CAs using RSA keys must use key sizes greater than or equal to 2048
bits."; "TLS server certificates and issuing CAs must use a hash algorithm from the SHA-2 family in the signature
algorithm."; "TLS server certificates must present the DNS name of the server in the Subject Alternative Name extension
of the certificate."; for certificates issued after July 1, 2019, "TLS server certificates must contain an
ExtendedKeyUsage (EKU) extension containing the id-kp-serverAuth OID." and "TLS server certificates must have a validity
period of 825 days or fewer". Apple Platform Security (https://support.apple.com/guide/security/tls-security-sec100a75d12/web):
"TLS clients using the SecureTransport APIs can’t use TLS 1.3."; "certificates need to be valid and signed using SHA-256
or stronger with a minimum 2048-bit RSA key or 256-bit elliptic curve key." Preventing Insecure Network Connections
(https://developer.apple.com/documentation/security/preventing-insecure-network-connections): "The server certificate
must be signed with either a Rivest-Shamir-Adleman (RSA) key of at least 2048 bits, or an Elliptic-Curve Cryptography
(ECC) key of at least 256 bits." None of the three names Ed25519 or EdDSA.

[13] Apple public API, macOS 26.5 SDK (Xcode), Security.framework headers
(https://developer.apple.com/documentation/security/seckeyalgorithm): SecKey.h defines RSA and ECDSA signature
algorithms only, e.g. `kSecKeyAlgorithmECDSASignatureMessageX962SHA256`; SecItem.h's asymmetric key types are
`kSecAttrKeyTypeRSA`, `kSecAttrKeyTypeDSA`, `kSecAttrKeyTypeECDSA`, `kSecAttrKeyTypeEC`, `kSecAttrKeyTypeECSECPrimeRandom`;
no public Security header contains "Ed25519", "EdDSA" or "Curve25519"; SecProtocolOptions.h ("Protocol options are used
to configure security protocols in the network stack.") has no signature-algorithm setter.

[14] Apple open source Security (https://github.com/apple-oss-distributions/Security). Commit db15acbe
("Security-61901.0.87.0.1") is the Security of macOS 26.0 (submodule at distribution-macOS tag macos-260).
OSX/sec/Security/SecSignatureVerificationSupport.c#L133-L134: "case kSecEd25519AlgorithmID:" "return
kSecKeyAlgorithmEdDSASignatureMessageCurve25519SHA512;". The case is present in Security-61040.1.3 (macOS 14.0, tag
macos-140) and absent from Security-60420.140.26 (macOS 13.5, tag macos-135) and every earlier tag checked.
tests/TrustTests/EvaluationTests/KeySizeTests.m#L205: "// If we support EdDSA, we should be able to find the root and
verify the signature on the leaf" (its chain is RFC 8410's example, an Ed25519 root over an X25519 leaf; decoded
locally). protocol/SecProtocolPriv.h#L707-L721, a private header: "sec_protocol_options_set_eddsa_enabled" … "Enable
EDDSA support (for TLS 1.3)." protocol/SecProtocolTest.m#L1063-L1064: "sec_protocol_options_set_eddsa_enabled(optionsA,
true);" then "XCTAssertFalse(sec_protocol_options_are_equal(optionsA, optionsB));" with optionsB untouched, so the
default is off (inferred).

[15] Apple coreTLS, the engine under Secure Transport, last published as coreTLS-167 in the macOS 10.15 distributions
and absent from every later one checked (https://github.com/apple-oss-distributions/coreTLS/blob/0e31699de32c98122ded49d0fd2f2cd37b704324/lib/tls_types.h#L103-L108):
"/* TLS 1.2 Signature Algorithms extension values for signature field. */" "tls_signature_algorithm_Anonymous = 0,"
"tls_signature_algorithm_RSA = 1," "tls_signature_algorithm_DSA = 2," "tls_signature_algorithm_ECDSA = 3".

[16] Java. JDK-8166596 "TLS support for the EdDSA signature algorithm" (https://bugs.openjdk.org/browse/JDK-8166596),
Fix Version 16, no backport among its issue links. Release note JDK-8257710: "The SunJSSE provider now supports the use
of the EdDSA signature algorithm. Specifically SunJSSE may use certificates containing EdDSA keys for server side and
client side authentication and may use certificates signed with the EdDSA algorithm." AWS SDK for Java 2.x
(https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/setup.html): "An installation of Java 8 or later." JDK 8's
SunJSSE suites (https://docs.oracle.com/javase/8/docs/technotes/guides/security/SunProviders.html) include
"TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256".

[17] Go. https://go.dev/doc/go1.13: "Ed25519 certificates are now supported in TLS versions 1.2 and 1.3."
https://github.com/golang/go/blob/0d37007fda071631fbcb520b99c85a8e4c93108a/src/crypto/tls/defaults.go#L49-L56 lists
"ECDSAWithP256AndSHA256," and "Ed25519,". src/crypto/x509/verify.go#L523-L524: "If opts.Roots is nil, the platform
verifier might be used, and verification details might differ from what is described below."; #L566-L578, on "windows",
"darwin" and "ios": "// If the platform verifier succeeded, or there are no additional // roots, return the platform
verifier result. Otherwise, continue // with the Go verifier." rclone `--ca-cert`
(https://github.com/rclone/rclone/blob/81e7372f6fbedfa80592e9a564593d63a8dc32f3/fs/fshttp/http.go#L261-L273):
"caCertPool := x509.NewCertPool()" … "t.TLSClientConfig.RootCAs = caCertPool". mc, via minio/pkg
(https://github.com/minio/pkg/blob/1cf596aed7f8d0e6df1288637f5ba6c26cdbcc57/certs/ca-certs.go#L41-L74): "rootCAs, _ :=
loadSystemRoots()" (off Windows "caPool, err := x509.SystemCertPool()", certs/cert_pool_nix.go#L64), then
"rootCAs.AppendCertsFromPEM(bytes)" for each file in its CAs directory.

[18] rustls 0.23 with aws-lc-rs
(https://github.com/rustls/rustls/blob/f04804fa13341a9b1dc02b5d1dd8a746f2c4b7fb/rustls/src/crypto/aws_lc_rs/mod.rs#L159-L210):
"webpki_algs::ECDSA_P256_SHA256," … "webpki_algs::ED25519," and "(SignatureScheme::ED25519, &[webpki_algs::ED25519]),".

[19] .NET (https://learn.microsoft.com/en-us/dotnet/standard/security/cross-platform-cryptography): "Cryptographic
operations in .NET are done by operating system (OS) libraries."; its asymmetric sections are RSA, ECDSA, ECDH, DSA.

[20] AWS SDKs and Tools Reference Guide, "Using shared config and credentials files to globally
configure AWS SDKs and tools" (https://docs.aws.amazon.com/sdkref/latest/guide/file-format.html):
"Both the shared `config` and `credentials` files are plaintext files that contain only ASCII
characters (UTF-8 encoded). They take the form of what are generally referred to as INI files."
"All entries in a section take the general form of `setting-name=value`." "Lines can be commented
out by starting the line with a hashtag character (`#`)." "`[default]` is the only profile that does
not require the `profile` section identifier." "The rules for the `credentials` file are generally
identical to those for the `config` file, except that profile sections don't begin with the word
`profile`." "Only the following settings that are considered "secrets" or sensitive can be stored
in the `credentials` file: `aws_access_key_id`,`aws_secret_access_key`, and `aws_session_token`."

[21] AWS SDKs and Tools Reference Guide, "Service-specific endpoints"
(https://docs.aws.amazon.com/sdkref/latest/guide/feature-ss-endpoints.html): "`endpoint_url` -
shared AWS `config` file setting"; "When specified directly within a profile or as an environment
variable, this setting specifies the endpoint that is used for all service requests."; "Valid
values: A URL including the scheme and host for the endpoint." Its support table: "AWS CLI v2 |
Yes", "SDK for Python (Boto3) | Yes", and Yes for Go V2, Java 2.x, JavaScript 3.x, .NET 3.x and 4.x,
Rust, Swift, PHP 3.x, Ruby 3.x, C++ and Kotlin; No for Go 1.x (V1), Java 1.x and JavaScript 2.x.

[22] AWS CLI User Guide, "Configuration and credential file settings in the AWS CLI"
(https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html): "`ca_bundle` Specifies
a CA certificate bundle (a file with the `.pem` extension) that is used to verify SSL
certificates." "Can be overridden by the `AWS_CA_BUNDLE` environment variable or the `--ca-bundle`
command line option." "You can specify a non-default location for the files by setting the
`AWS_CONFIG_FILE` and `AWS_SHARED_CREDENTIALS_FILE` environment variables to another local path."

[23] AWS CLI Command Reference, "AWS CLI S3 Configuration"
(https://docs.aws.amazon.com/cli/latest/topic/s3-config.html): "There's two styles of constructing an
S3 endpoint. The first is with the bucket included as part of the hostname. This corresponds to the
addressing style of `virtual`. The second is with the bucket included as part of the path of the
URI, corresponding to the addressing style of `path`." "Note that all the S3 configuration values are
indented and nested under the top level `s3` key."

[24] IAM API Reference, "AccessKey" (https://docs.aws.amazon.com/IAM/latest/APIReference/API_AccessKey.html):
"AccessKeyId", "Length Constraints: Minimum length of 16. Maximum length of 128.", "Pattern:
`[\w]+`"; "SecretAccessKey", "The secret key used to sign requests.", "Type: String".

[25] Amazon S3 API Reference, "CreateBucket"
(https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html): "If you don't specify a
Region, the bucket is created in the US East (N. Virginia) Region (us-east-1) by default."

[26] RFC 6761 §6.3 (https://www.rfc-editor.org/rfc/rfc6761): "Users are free to use localhost names
as they would any other domain names. Users may assume that IPv4 and IPv6 address queries for
localhost names will always resolve to the respective IP loopback address."

[27] RFC 9114 §3.1 (https://www.rfc-editor.org/rfc/rfc9114): "A client MAY attempt access to a
resource with an "https" URI by resolving the host identifier to an IP address, establishing a QUIC
connection to that address on the indicated port".

[28] Python, `ssl` (https://docs.python.org/3/library/ssl.html), on `create_default_context`:
"Changed in version 3.13: The context now uses VERIFY_X509_PARTIAL_CHAIN and VERIFY_X509_STRICT in
its default verify flags.", which "make the underlying OpenSSL implementation behave more like a
conforming implementation of RFC 5280".

[29] RFC 5280 (https://www.rfc-editor.org/rfc/rfc5280). §4.2.1.1: "The keyIdentifier field of the
authorityKeyIdentifier extension MUST be included in all certificates generated by conforming CAs
to facilitate certification path construction." and "where a CA distributes its public key in the
form of a "self-signed" certificate, the authority key identifier MAY be omitted." §4.2.1.2: "this
extension MUST appear in all conforming CA certificates" and "this extension SHOULD be included in
all end entity certificates."

[30] RFC 7093 §2 (https://www.rfc-editor.org/rfc/rfc7093), its first method: "The keyIdentifier is
composed of the leftmost 160-bits of the SHA-256 hash of the value of the BIT STRING
subjectPublicKey", the BIT STRING's tag, length and unused-bits count left out.

[31] RFC 7468 (https://www.rfc-editor.org/rfc/rfc7468). §2: "Generators MUST wrap the base64-encoded
lines so that each line consists of exactly 64 characters except for the final line". §5.1: a
certificate's label is "-----BEGIN CERTIFICATE-----".

## What the sources settle

### The chain

Which stack each client runs. Since AWS CLI 2.38.0 `aws s3 cp/sync/mv/rm` use the CRT transfer client on every host,
falling back to the classic botocore client for S3-to-S3 copies, while another CLI process holds the CRT client, and for
some settings [2]. The CRT client always loads a CA file [2] into aws-c-io: s2n-tls with AWS-LC on Linux, Secure
Transport (TLS 1.2 at most) on macOS, Schannel with manual validation against an exclusive root store on Windows [3].
All other `aws` commands, and the classic path, run botocore on Python's ssl over the bundled OpenSSL 3.5.9 [2, 6].
boto3 and s3cmd use their Python's OpenSSL: 1.1.1 or later for CPython 3.10+, 3.5.9 or 3.0.16 in python.org builds [8],
with two common exceptions, Amazon Linux 2's OpenSSL 1.0.2 [8] and the Xcode Python's LibreSSL 2.8.3 [9]. .NET uses
the OS stack, Schannel on Windows [19, 11]; Java uses JSSE unless it adds the CRT [16]; rclone and mc use Go's TLS
and, given a CA file, Go's verifier, mc on macOS after trying the platform verifier [17]; Mountpoint uses the CRT [3].

An Ed25519 leaf fails on s2n-tls, which has no Ed25519 key type (the leaf is rejected with `S2N_ERR_DECODE_CERTIFICATE`)
and no Ed25519 scheme [4]; on Secure Transport, whose last published engine knows RSA, DSA and ECDSA only [15] (that
the current, unpublished engine is the same is inferred); on Schannel, where CNG has no EdDSA [10] and clients fail on
any Ed25519 certificate in the chain [11]; in Apple's Network framework, where EdDSA sits behind a private switch that
is off by default [13, 14] (inferred); in LibreSSL's TLS layer [9]; in OpenSSL before 1.1.1 [7]; in JSSE before
JDK 16 [16]. It works in OpenSSL 1.1.1 and later [7], Go 1.13 and later [17], rustls with webpki [18] and JDK 16 and
later [16]. An Ed25519 leaf would break the AWS CLI's default `aws s3` transfers on Linux, macOS and Windows, every
.NET client on Windows, Mountpoint, Java 8 and 11, and boto3 on Amazon Linux 2 or the Xcode Python.

Keeping the Ed25519 cell authority as issuer over a P-256 leaf puts an Ed25519 signature on the leaf, which the client
must verify [1]. Windows fails: Schannel under manual validation rejects a chain holding an Ed25519 certificate [11],
and the CLI's CRT validates manually because it always passes a CA file [2, 3]; `CertGetCertificateChain` reports
"Invalid algorithm specified" [11], so Go with only OS roots on Windows should fail as well (inferred). Apple's trust
engine verifies Ed25519 certificate signatures from macOS 14, not on macOS 13 or earlier [14]. On Linux the CRT
probably passes: under the CRT policies s2n leaves chain signatures to libcrypto, and AWS-LC verifies Ed25519 [4, 5]
(inferred, untested). Java before 16, OpenSSL 1.0.2 and LibreSSL 2.8.3 fail [7, 9, 16].

ECDSA P-256 verifies on every stack examined. TLS 1.3 makes it mandatory [1]; the CRT's s2n policies carry
`ecdsa_secp256r1_sha256` [4]; Schannel enables the ECDHE-ECDSA suites by default (Windows 11 list) and CNG has ECDSA_P256 [10]; Secure
Transport's engine knows ECDSA [15] and Apple accepts a 256-bit elliptic-curve key [12]; OpenSSL 1.0.2 through 3.x [7],
LibreSSL [9], JDK 8 and later [16], Go [17] and rustls [18] all carry it. RSA is as universal [1, 12] but reaches no
client that P-256 misses, so it adds no compatibility; which of the two costs less on mantle's hardware is a
measurement, not a compatibility question.

The evidence supports one answer: the S3 endpoint's chain is ECDSA P-256 throughout. A P-256 issuing authority,
separate from the Ed25519 cell authority, signs a P-256 leaf with ecdsa-with-SHA256, and the server signs handshakes
with `ecdsa_secp256r1_sha256`. Node-to-node mTLS keeps Ed25519, which rustls verifies [18]. The same sources put two
more conditions on the endpoint. It must offer TLS 1.2 beside 1.3: Secure Transport, the CLI's transfer path on macOS,
cannot use TLS 1.3 [3, 12], and Schannel on Windows 10 and Server 2019 has none [10]; with a P-256 leaf that means the ECDHE-ECDSA
AES-GCM suites Schannel enables by default [10]. And for clients that use Apple's trust engine, the leaf needs its DNS
names in the Subject Alternative Name, an EKU with id-kp-serverAuth, SHA-2 signatures and at most 825 days of
validity [12].

Against this: Go, rustls, OpenSSL 1.1.1+ and JDK 16+ handle Ed25519 completely [7, 16, 17, 18], so most Python and Go
clients would accept an all-Ed25519 chain; the failures sit on platform and CRT stacks, which since CLI 2.38.0 carry the
CLI's default transfers [2]. macOS 14+ [14] and probably s2n [4, 5] would verify an Ed25519 authority; Windows would not.

### What the endpoint's certificate carries

Its DNS names, and its addresses, in its Subject Alternative Name [12]; an EKU with
id-kp-serverAuth [12]; SHA-2 signatures [12]; at most 825 days of validity [12], so an endpoint's
certificate is issued again before then. A verifier holding to RFC 5280, Python's default from 3.13
[28], asks the key identifiers RFC 5280 requires: the authority's own (a CA's Subject Key
Identifier), and in the leaf its issuer's (Authority Key Identifier) [29]; the leaf's own is a
SHOULD [29]. Computed by RFC 7093's first method [30], they need no SHA-1. On a loopback address the
name `localhost` reaches it [26]; an address every interface answers on is no one address, so the
certificate names the DNS names the operator states for it. The authority goes to clients as PEM in
lines of 64 characters [31].

### The key and the profile

Stock clients take a key and an endpoint from AWS's shared files [20]: the key in the credentials
file (`aws_access_key_id`, `aws_secret_access_key`) and the endpoint in the config's
`endpoint_url` [21], which the AWS CLI v2 and boto3 both read [21], with the authority as the
config's `ca_bundle` [22]. Pointing `AWS_CONFIG_FILE` and `AWS_SHARED_CREDENTIALS_FILE` at a cell's
files takes them without touching the operator's own [22]. An endpoint named by an address or by
`localhost` has no DNS name a bucket can be put before, so its clients put the bucket in the path,
`addressing_style = path` under the profile's `s3` [23]. SigV4 signs for a region, and S3's own
default is us-east-1 [25]. An access key's id is 16 to 128 word characters [24]: twenty, as AWS's own
example ids are, from RFC 4648's base32 alphabet, uniform from a random octet's low five bits and
with no `/`, which the signature's credential scope separates by (research/05 §1). A secret is a
string [24]: forty characters of base64 from 30 random octets, 240 bits, as long as AWS's examples.

### Where the endpoint listens

An https URI's port is the one a client dials over TCP and, by RFC 9114, over QUIC [27]: the S3
endpoint listens over TCP on the port the node's QUIC listens on unless its operator states another.

## Open

No client was run against a mantle endpoint. An end-to-end matrix would turn inferred cells into measured ones: the
clients above (CLI crt and classic, boto3 on three OpenSSL/LibreSSL builds, Java 8 to 21, .NET, Go, Mountpoint) against
P-256/P-256, Ed25519 over P-256, Ed25519/Ed25519 and RSA chains, on Linux, macOS 13/14/26 and Windows 10/11/Server 2022+.

Inferred, not confirmed: s2n with AWS-LC accepting an Ed25519 authority; Secure Transport's engine after macOS 10.15
(unpublished); the Network framework's EdDSA default; whether Schannel ever offers `ed25519` in its ClientHello
(undocumented); Go's platform verifier on Windows. Microsoft states nowhere that Ed25519 is unsupported; the Windows
conclusion rests on CNG's missing EdDSA identifier and two third-party reports [10, 11].

Not examined: FIPS-mode clients (the OpenSSL FIPS provider, RHEL's FIPS crypto policy, AWS-LC FIPS), and the handshake
cost of P-256 against RSA on the running hardware, which the constants rule requires to be measured before any claim.

An endpoint's certificate is issued for 825 days [12]; issuing it again before then, from the
endpoint's authority the operator keeps, is still to build. Keys per principal, and kiln's Veil
credentials, are owner decisions a later step takes; the cell's first key stands until then.
