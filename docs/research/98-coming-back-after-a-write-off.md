# 98. Coming back after a write-off: re-admitting a node online

Status: research note, 2026-11-18. For docs/design/membership.md §5.1: how a machine whose member
the cell wrote off (its data rebuilt on other nodes) comes back as a new member on its own, as the
owner decided ("Automatically": it wipes its disks and rejoins). Sources [1] to [11] were read on
the web on 2026-11-18 and are quoted where read; [12] is the Raft dissertation as research/06
§A1.7 quotes it.

## The questions

1. Who may issue a node an identity online, while the cell's authority keeps its key offline
   (membership.md §2)?
2. What can a returning machine show, and what does each scheme prove about the machine and
   trust beyond it? What keeps a stolen machine or disk out?
3. What must a machine do with its disks before it serves again, once its data was rebuilt
   elsewhere?

## Sources

[1] Kubernetes, "TLS bootstrapping",
https://kubernetes.io/docs/reference/access-authn-authz/kubelet-tls-bootstrapping/. Bootstrap:
the kubelet "now has limited credentials to create and retrieve a certificate signing request
(CSR)"; "CSR is approved in one of two ways": "If configured, kube-controller-manager
automatically approves the CSR" and "If configured, an outside process, possibly a person,
approves the CSR using the Kubernetes API or via `kubectl`". Approval by policy: "The
`csrapproving` controller ... uses the `SubjectAccessReview` API to determine if a given user is
authorized to request a CSR, then approves based on the authorization outcome." "It only ignores
unauthorized requests." Renewal: "`selfnodeclient`: If a node is renewing its certificate, then it
already has a certificate (by definition), which it uses continuously to authenticate as part of
the group `system:nodes`." The kubelet can "rotate its client certificates by creating new CSRs as
its existing credentials expire." The signer is online in the control plane:
`--cluster-signing-cert-file="/etc/path/to/kubernetes/ca/ca.crt"
--cluster-signing-key-file="/etc/path/to/kubernetes/ca/ca.key"`. On the token's scope: "you could
disable a particular bootstrap group's access when you are done provisioning the nodes."

[2] Kubernetes, "Authenticating with Bootstrap Tokens",
https://kubernetes.io/docs/reference/access-authn-authz/bootstrap-tokens/: "Bootstrap tokens are a
simple bearer token that is meant to be used when creating new clusters or joining new nodes to an
existing cluster." "Expired tokens are rejected when used for authentication".

[3] SPIFFE, "SPIRE Concepts", https://spiffe.io/docs/latest/spire-about/spire-concepts/: "Node
attestor plugins which, together with agent node attestors, verify the identity of the node the
agent is running on." Its means: "an identity document delivered to the node via a cloud platform
(such as an AWS Instance Identity Document)", "verifying a private key stored on a Hardware Security
Module or Trusted Platform Module attached to the node", "a manual verification provided through a
join token when the agent is installed", "using an existing X.509 certificate". "A join token is a
pre-shared key between a SPIRE Server and Agent." "To help protect against misuse, join tokens
expire immediately after use." "By default the SPIRE Server acts as its own certificate authority."

[4] SPIRE's server documentation, https://github.com/spiffe/spire/tree/main/doc (main, read
2026-11-18). `plugin_server_nodeattestor_x509pop.md`: "The `x509pop` plugin attests nodes that have
been provisioned with an x509 identity through an out-of-band mechanism. It verifies that the
certificate is rooted to a trusted set of CAs and issues a signature-based proof-of-possession
challenge to the agent plugin to verify that the node is in possession of the private key." "The
SPIFFE ID produced by the plugin is based on the certificate fingerprint".
`plugin_server_nodeattestor_jointoken.md`: "The `join_token` plugin attests a node based on a
pre-shared, one-time-use join token." `plugin_server_upstreamauthority_disk.md`: "The `disk` plugin
loads CA credentials from disk, using them to generate intermediate signing certificates for the
server's signing authority." `spire_server.md`: `spire-server agent ban`, "Ban attested node given
its spiffeID. A banned attested node is not able to re-attest."; `spire-server agent evict`,
"De-attesting an already attested node given its spiffeID."; and "Banned nodes are not pruned."

[5] RFC 8555, Automatic Certificate Management Environment (ACME),
https://www.rfc-editor.org/rfc/rfc8555.txt. §7.5: "This process assures the server of two things:
1. That the client controls the private key of the account key pair, and 2. That the client
controls the identifier in question." §6.5: "ACME POST requests have a mandatory anti-replay
mechanism. This mechanism is based on the server maintaining a list of nonces that it has issued,
and requiring any signed request from the client to carry such a nonce." §10.2: "The integrity of
the authorization process thus depends on the identifier validation challenges to ensure that the
challenge can only be completed by someone who both (1) holds the private key of the account key
pair and (2) controls the identifier in question."

[6] RFC 8446, TLS 1.3, https://www.rfc-editor.org/rfc/rfc8446.txt, §4.4.3: "This message is used
to provide explicit proof that an endpoint possesses the private key corresponding to its
certificate. The CertificateVerify message also provides integrity for the handshake up to this
point." "Clients MUST send this message whenever authenticating via a certificate".

[7] RFC 5280, https://www.rfc-editor.org/rfc/rfc5280.txt, §4.2.1.9: "The pathLenConstraint field is
meaningful only if the cA boolean is asserted and the key usage extension, if present, asserts the
keyCertSign bit". "A pathLenConstraint of zero indicates that no non-self-issued intermediate CA
certificates may follow in a valid certification path."

[8] CA/Browser Forum, Baseline Requirements for the Issuance and Management of Publicly-Trusted TLS
Server Certificates, version 2.3.1 (4 October 2026),
https://github.com/cabforum/servercert/blob/main/docs/BR.md, §6.1.7: "Private Keys corresponding
to Root Certificates MUST NOT be used to sign Certificates except in the following cases:" among
them "Certificates for Subordinate CAs and Cross-Certified Subordinate CA Certificates".

[9] etcd v3.5, "Runtime reconfiguration",
https://etcd.io/docs/v3.5/op-guide/runtime-configuration/: "To replace the machine, follow the
instructions for removing the member from the cluster, and then add a new member in its place."
"If etcd starts using the data directory of a removed member, etcd automatically exits if it
connects to any active member in the cluster:" printing "etcd: this member has been permanently
removed from the cluster. Exiting."

[10] Cockroach Labs, "Node Shutdown", https://www.cockroachlabs.com/docs/stable/node-shutdown
(served from docs.cockroachlabs.com): "Decommission a node to permanently remove it from the
cluster". "This node is now prevented from communicating with the rest of the cluster". "If a node
has completed decommissioning, you must start a new node. A fully decommissioned node is
permanently decommissioned, and cannot be recommissioned." Of a node stopped too long: "If the node
then stays offline for the duration set by server.time_until_store_dead (5 minutes by default), the
cluster considers the node “dead” and starts to rebalance its range replicas onto other nodes."

[11] Ceph, "Adding/Removing OSDs",
https://docs.ceph.com/en/latest/rados/operations/add-or-rm-osds/, Replacing an OSD: `ceph osd
destroy` "removes the OSD’s CephX keys, its config-key data, and its lockbox keys. On an encrypted
OSD, the config-key data holds the encryption key, so the data on the OSD can never be read again."
"If the disk that you plan to use is not new (for example, if it held an OSD before), zap it."
Removing one: the purge "removes the OSD from the CRUSH map, removes the OSD’s authentication key,
and removes the OSD from the OSD map." And "ceph-volume lvm encryption",
https://docs.ceph.com/en/latest/ceph-volume/lvm/encryption/: "When setting up the OSD, a secret key
is created. That secret key is passed to the monitor"; `dmcrypt_key`, "The secret (or private) key
to unlock encrypted devices".

[12] D. Ongaro, "Consensus: Bridging Theory and Practice", PhD dissertation, Stanford, 2014, §3.8,
as research/06 quotes it: "If a server loses any of its persistent state, it cannot safely rejoin
the cluster with its prior identity. Such a server can usually be added back into the cluster with
a new identity by invoking a cluster membership change."

## 1. What each scheme proves, and what it trusts

| Scheme | What it proves about the asker | What it trusts beyond that | Again after removal |
|---|---|---|---|
| Kubernetes bootstrap token [1][2] | it holds a bearer secret | whoever handed the token out, and the token's expiry | a new token, or the operator approves |
| Kubernetes renewal (`selfnodeclient`) [1] | it holds the key of a certificate the cluster still honors | the certificate's first issuance | no: renewal needs a live certificate |
| SPIRE join token [3][4] | it holds a one-time secret | the token's distribution; spent at first use | no: "expire immediately after use" |
| SPIRE `x509pop` [4] | it holds the key of a certificate rooted in a trusted CA, named by its fingerprint | the out-of-band provisioning of that certificate | yes after `evict`, never after `ban` |
| SPIRE TPM or cloud document [3] | a key that never left a chip, or a platform's statement about the instance | the chip's or the platform's maker | yes, while the platform vouches |
| ACME [5] | it holds the account key and controls the identifier now | the validation channel | each order validates again |
| TLS 1.3 client certificate [6] | it holds the certificate's key, over this handshake's transcript | the certificate's issuer | as the verifier admits |

None of the schemes that run on any machine tells a machine from a copy of its disk: a bearer
token, a join token and a certificate's key are files, and whoever holds the files holds the proof.
Only a key in hardware [3] ties the proof to one machine, and that proves the chip, not whose hands
hold it. What every scheme adds against theft is a way to *withdraw* standing: Kubernetes' token
expiry and group disabling [1][2], SPIRE's ban [4], CockroachDB's permanent decommission [10].

The schemes that admit a node again on its own are those whose proof is a credential the node
already held: Kubernetes renews with the certificate it has [1], and SPIRE re-attests an evicted
`x509pop` node by proof of possession of its provisioned certificate [4]. Proof of possession is
fresh in both: a signature over a challenge the server chose [4], or over the TLS 1.3 transcript
[6], so a recorded request does not replay, as ACME's nonces stop replays [5].

## 2. Issuing online with the root offline

Kubernetes keeps its cluster CA's key online in the control plane, which signs each approved CSR
[1]. SPIRE's server is its own CA, or holds an intermediate an upstream authority mints for it [4],
so the root's key stays off the online server. The public web's rule is the same: a root's key
signs subordinate CAs, not leaves [8]. RFC 5280 bounds what a subordinate may sign: a
pathLenConstraint of zero lets it sign end-entity certificates and no further CA [7].

An online signer's key, wherever it sits, is a credential like any other: taken, it signs. In
Kubernetes and SPIRE a signature is the whole admission. A system whose admission is also a
committed record of its own (mantle's root range spends grants and holds each member's fingerprint,
membership.md §2) can make a signature necessary but not sufficient: the verifier admits a
subordinate's certificate only at a fingerprint the record holds.

## 3. A machine whose data was rebuilt

Every system read forbids coming back as the removed member. etcd's removed member exits on
contact with the cluster, and its machine returns as a new member added in its place [9];
CockroachDB's decommissioned node "cannot be recommissioned" and the operator starts "a new node"
[10]; Ceph's destroyed OSD loses its keys, and a disk that held an OSD is zapped before it is used
again [11]. Raft's reason is the log's: a server's persistent state belongs to its identity, and a
server without it returns only under a new one [12]; equally, state written under an old identity
is never presented under a new one.

Ceph's destroy is a cryptographic erase where the OSD is encrypted: its key is the OSD's own, held
by the monitors, and destroying it leaves "the data on the OSD can never be read again" [11]. That
works because no other OSD's data is under that key. A store whose data keys are not a device's own
cannot erase one device by forgetting a key.

## 4. What mantle takes

- **Withdrawal is a statement, absence is not.** SPIRE separates eviction, after which a node may
  attest again, from a ban, after which it never may [4]. membership.md §5 writes a member off for
  its absence, a judgment about time, not about whose hands hold the machine; a ban is the
  statement about hands.
- **The proof is the credential the node held.** As `x509pop` and Kubernetes' renewal admit a
  node by a certificate it already holds [1][4], the written-off certificate, its key proven over
  the TLS 1.3 handshake [6], is what asks.
- **A subordinate issuer, the root offline.** The authority signs one CA certificate with a
  pathLenConstraint of zero [7][8], as SPIRE's upstream authority mints its server's [4]; the
  cell signs under it, and admits what it signed only at fingerprints it committed (§2).
- **Wipe by deletion.** mantle seals each file under its own data key, wrapped under the cell's
  root key (encryption.md §2): no key is a volume's own, so Ceph's erase by forgetting a key [11]
  has no counterpart, and the rebuilt copies elsewhere are sealed under the same file keys. The
  returning machine deletes its old state [9][10][11][12].

Not taken: bearer and join tokens [1][2][3], a secret that is a file beside the disk unless a
supervisor keeps it apart, which makes coming back the supervisor's act and not the cell's, and
whose expiry is a time to choose; and hardware attestation [3], the one proof that tells a machine
from its disk, which is each platform's own (a TPM, a vendor's enclave) and which mantle's portable
path cannot assume present (CLAUDE.md §7).
