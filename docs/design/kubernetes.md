# A cell on Kubernetes

Status: built 2026-11-03 for plan step 11 of the data layer (a container image and a Helm chart for
mantle, proven in a kind cluster in CI). The facts it rests on are research/78's.

A cell of three founders runs as three pods of one StatefulSet, each `mantle serve` on a Kubernetes
node and two claims of its own. The same binary is the node, its install step, its readiness check,
the cell's clients and the transfer harness; nothing here is a mode of the node, which runs as it
runs on a laptop.

## 1. The image

`Dockerfile` builds `mantle` alone with the workspace's release profile on the pinned toolchain and
copies it onto `gcr.io/distroless/cc-debian13:nonroot`: no shell, no package manager, the non-root
user 65532, the binary at `/mantle` as the entrypoint. The runtime base is the builder's Debian
release, whose glibc the binary links. `mantle serve` is PID 1 and spawns nothing, so no init shim
is needed; SIGTERM begins its orderly stop (node.md §1.7).

## 2. Names

A founder is dialed at a Service of its own, `<fullname>-N`, which selects its pod alone by the
`statefulset.kubernetes.io/pod-name` label the StatefulSet gives each pod and carries its QUIC and
plane ports (UDP) and its S3 listener (TCP). A pod's address goes with the pod, and a node resolves
its peers' names once, at its start (`process::resolve`), so a peer deleted and made again would
come back where its peers never look; a Service's cluster IP is assigned once and kept through
updates, so the name resolves to the same address across every deletion of the pod behind it
(research/78). Each founder binds every address on its ports and is reached at its Service's name,
which `mantle init` states as its reach (`reach=HOST`, node.md §1.5): its peers' `peer` lines and
its clients' `node` lines name that host on its ports. The per-pod Services publish not-ready
addresses, since a founder is ready only once the cell it forms through those very names enlisted
it, and the endpoints controllers then count every endpoint ready (research/78 [11]). The
StatefulSet's governing Service is headless and publishes not-ready addresses too. The cell's S3
endpoint, `<fullname>-s3`, puts every ready founder's listener behind one address. Off loopback the
S3 listener speaks TLS with the endpoint's certificate for the names `mantle init` states
(`name=HOST`, node.md §1.5), so each founder names its own Service and the cell's S3 Service, and a
stock client takes the endpoint's authority (`OUT/s3/authority.pem`) as its chain.

Open: a peer's QUIC and plane reach a founder through its Service's cluster IP, a translation
kube-proxy keeps in the flow's conntrack entry, and each peer dials from one UDP port within about 3
s, which renews the entry past the pod it reached. A founder its peers dial (the highest id dials no
one) was so out of reach for 3 min 7 s after its pod was replaced in the KIND cell, until the
entries went, one deleted by hand and the other most likely at a kube-proxy sync (research/78, "A
Service's cluster IP carries a peer's QUIC only until its pod is replaced"). The fix proposed there,
founders reached at their pods' addresses by the headless Service's per-pod names, each resolved
again when a link to it ends, is not built.

## 3. Identity and keys

The chart never generates a key: an identity changed by a `helm upgrade` would change the node's
id and every peer's pin. The operator runs `mantle init` once for the founders, each stated with
its reach, its failure domain and its volume at the claim the chart mounts (`volume=/volume:SIZE`),
and, where a reader of the cell's metadata is wanted, `mantle init gateway` pins a gateway's
certificate in each founder before any starts. Each founder's directory then reaches its pod as one
Secret, `<fullname>-N-founder`.

A Secret's keys name flat files (alphanumerics, `-`, `_` and `.`), while a founder's directory
holds directories and files named by its node ids, and a projected file takes the volume's mode,
0644 unless stated, while `mantle serve` reads a key only from a file its owner alone may read
(research/78 [5, 10]). So `mantle pack DIR FILE` carries the directory as one file, each file's
path, whether it is a key (a file its owner alone may read where it was packed) and its bytes, with
a CRC-32C over all of it and at most a Secret's 1 MiB (`crates/mantle/src/pack.rs`); the Secret's
one key, `founder`, holds it. The pod's init container runs `mantle install /founder
/var/lib/mantle`: the pack's checksum checked, every path one within the directory, each file
written whole and durable, each key owner-only as the pod's user, and the configuration last, so a
directory holding a configuration holds every file, an install a crash cut short is made whole by
the next, and a node directory holding a configuration already is kept as it is with the node's
state beside it. The pod's own pack alone is mounted, by `subPathExpr` on its name from a projected
volume of every founder's Secret, as slates' chart mounts its keys; the kubelet still materializes
every founder's Secret on the pod's node, which a per-pod identity issued in the cluster would
remove.

## 4. Storage

Each founder holds two claims from `volumeClaimTemplates`: `node` at `/var/lib/mantle`, its node
directory (its configuration and keys, log, run record, readiness lock, calibration and ranges'
stores), and `volume` at `/volume`, its chunk volume, two devices the node identifies and measures
apart (node.md §1.4). The volume's bytes are its operator's statement at `mantle init`, which its
claim holds at least: kind's local-path provisioner does not hold a claim to its size (research/78
Open), so the statement, not the claim, bounds the volume.

## 5. Readiness, resources and stopping

A pod is ready once the node's own word says so: `mantle status DIR` exits 0 while a living process
of the node holds its readiness lock (`live`, node.md §1.6), which the node takes exclusively as
the cell's root range enlists it in its run, before it says `enlisted`, and holds until it ends;
the check asks for it shared without waiting, and exits 1 when it is granted. A process's locks
end with it however it ends, so a node killed, its successor still recovering its stores, is not
ready, where a record compared with the run record said a killed founder ready 57 ms after its
`SIGKILL` (research/78, "Ready while an enlisted process lives"). A bound port says nothing of
whether a node serves, since its listener binds before it enlists. Readiness decides whether the
S3 Service sends the pod requests (research/78 [4]).

Requests equal limits, so the pod is Guaranteed and its cgroup's `memory.max` is the memory the
node derives its budgets within (node.md §1.4). The chart's 2 GiB holds a founder's floor with room
past it: a founder of the KIND cell's statements (a 1 GiB volume, three founders) served under a
2 GiB ceiling on one CPU derived a floor of 882,877,159 bytes, its objects' 83,513,198, volumes'
147,045,216, engines' 822,800, ranges' 268,862,970, log's 225,853,540, transport's 70,796,203 and
threads' 85,983,232, the rest its indexes' and engines' shares (measured 2026-11-03 on adacachy,
`taskset -c 0`, `memory=2G`; the ceiling stands in for the cgroup's limit, which the allowance
takes alike). The pods start `Parallel`: a cell needs a majority
of its founders to elect, and `OrderedReady` would wait for each to be ready before starting the
next, which a founder alone never is. A required anti-affinity on `kubernetes.io/hostname` puts
each founder on a Kubernetes node of its own, its failure domain. SIGTERM begins the orderly stop,
bounded by the node's `stop_ms` (10 s unstated, `derive::DEFAULT_STOP`), and the kubelet kills at
the pod's grace: the chart refuses a grace not above its `stopSeconds`.

## 6. The chart's gates and the KIND cell

`deploy/helm/mantle` passes `helm lint` and renders byte for byte its golden
(`deploy/helm/mantle/ci/golden.yaml`) under the pinned helm (`scripts/check-chart.sh`).

`scripts/kind-cell.sh`, the CI job `kind`, proves it: the image built, a kind cluster of a control
plane and three workers (`deploy/kind/cluster.yaml`), a cell of three made by the image's own
`mantle init` with each founder reached at its Service's name and the harness's gateway pinned,
each founder's pack and the cell's set as Secrets, the chart installed and waited for until every
founder is ready, one founder to a kind node checked, and the transfer harness's CI scale run from
a Job in the cluster (`deploy/kind/harness.yaml`) through both doors. The Job's image
(`deploy/kind/harness.Dockerfile`) is the mantle binary beside the stock S3 clients the harness's
HTTP door runs and kubectl at the cluster's version.

The harness runs on a cell running elsewhere with `--cell` (`crates/mantle/src/transfer/
outside.rs`): the cell's directory, its gateway pinned, each founder reached at the name its cell
file states, resolved once as the founders resolve their peers, and each case's objects in a bucket
of its own. It starts, kills and counts the chunks of none of the cell's nodes: every acknowledged
object is read back through each node in turn, its placement and its controls judged as on a cell
it made. With `--disrupt`, a command is started halfway through the first small set's native puts,
here `disrupt mantle-1 mantle` (`deploy/kind/disrupt.sh`): founder pod mantle-1 deleted, and its
successor of the same name waited for until it is made and ready, each wait on its fact. The puts
go on beside it, and its end, which must be success, is waited for before the set's S3 puts and
every read. A cell short a node refuses a block it cannot place on distinct nodes `SlowDown`
(gateway `PutError::Unplaced`), and while its nodes are gone in turn the native client may find no
node to take a put in its attempts (`ClientError::Untaken`) or lose a put's answer
(`ClientError::Lost`): each such put is counted and acknowledged by nothing, any other failure
ends the run, and every put the cell acknowledged must read back exactly through every node after
the disruption. The Job
exits 0 only with every check exact; it first did in run 41738909698 (2026-11-03), its rows in
STATUS.md.
