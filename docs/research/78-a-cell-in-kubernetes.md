# 78. A cell in Kubernetes: names, keys, readiness and termination

Status: research note, 2026-11-03. For plan step 11 of the data layer (a container image and a
Helm chart for mantle, proven in a kind cluster) and docs/design/kubernetes.md. Sources [1] to
[6], [10] and [11] were read from the Kubernetes documentation on 2026-11-03, each quoted where it
was read; [7] to [9] and [12] are mantle's own code, read at `9166554e`.

## The question

A cell of three founders runs as three pods. Four things a pod changes about a node's life
need settling before the chart is written: how a founder is named so its peers and clients reach
it across a pod's deletion, how its keys reach its directory owner-only, when it is ready, and
how it is stopped.

## Sources

[1] Kubernetes, "StatefulSets",
https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/. "`Parallel` pod management
tells the StatefulSet controller to launch or terminate all Pods in parallel, and to not wait for
Pods to become Running and Ready or completely terminated prior to launching or terminating
another Pod." "`OrderedReady` pod management is the default for StatefulSets." "Each Pod in a
StatefulSet derives its hostname from the name of the StatefulSet and the ordinal of the Pod. The
pattern for the constructed hostname is `$(statefulset name)-$(ordinal)`." "When the StatefulSet
controller creates a Pod, it adds a label, `statefulset.kubernetes.io/pod-name`, that is set to
the name of the Pod. This label allows you to attach a Service to a specific Pod in the
StatefulSet."

[2] Kubernetes, "Service", https://kubernetes.io/docs/concepts/services-networking/service/.
"Pods are ephemeral resources (you should not expect that an individual Pod is reliable and
durable)." "Each Pod gets its own IP address." "Kubernetes assigns this Service an IP address (the
_cluster IP_), that is used by the virtual IP address mechanism." Headless Services "report the
endpoint IP addresses of the individual pods via internal DNS records". "The default protocol for
Services is TCP; you can also use any other supported protocol."

[3] Kubernetes API reference, "Service", ServiceSpec `clusterIP`,
https://kubernetes.io/docs/reference/kubernetes-api/service-resources/service-v1/. "clusterIP is the
IP address of the service and is usually assigned randomly." "This field may not be changed
through updates unless the type field is also being changed to ExternalName".

[4] Kubernetes, "Pod Lifecycle", https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/.
The kubelet stops a pod's containers "by first sending a TERM (aka. SIGTERM) signal, with a grace
period timeout, to the main process in each container". "The default
`terminationGracePeriodSeconds` setting is 30 seconds." "When the grace period expires, if there
is still any container running in the Pod, the kubelet triggers forcible shutdown. The container
runtime sends `SIGKILL` to any processes still running in any container in the Pod." "Readiness
probes determine when a container is ready to accept traffic." "If the readiness probe returns a
failed state, the EndpointSlice controller removes the Pod's IP address from the EndpointSlices of
all Services that match the Pod."

[5] Kubernetes API reference, volumes, `KeyToPath.mode`,
https://kubernetes.io/docs/reference/kubernetes-api/config-and-storage-resources/volume/. "mode
is Optional: mode bits used to set permissions on this file." "If not specified, the volume
defaultMode will be used. This might be in conflict with other options that affect the file
mode, like fsGroup, and the result can be other mode bits set." A projected `defaultMode`
"Defaults to 0644".

[6] Kubernetes, "Configure a Security Context for a Pod or Container",
https://kubernetes.io/docs/tasks/configure-pod-container/security-context/. With `fsGroup` set
"all processes of the container are also part of the supplementary group ID" and "The owner for
volume `/data/demo` and any files created in that volume will be Group ID" that group.

[7] mantle, `crates/node/src/process.rs` `resolve`: a peer line's names are resolved once,
"before the node's runtime starts", and an unresolvable one fails the start
(`ProcessError::Resolve`). The module's doc: "Standard input closing ends `mantle node`, its
harness's child, at once ...; `mantle serve` runs on, as under a service manager, whose input is
empty." A stop is asked by SIGTERM or a `stop` line and bounded by the node's `stop_ms`.

[8] mantle, `crates/mantle/src/init.rs`: a founder's `quic` and `plane` are one `SocketAddr`
each, the address it binds and, where it is dialable (a port and a specified address), the
address its peers' `peer` lines and its clients' `node` lines name; a founder stated at
`0.0.0.0` gets peer lines with no address, for a harness to supply on standard input.

[9] mantle, `crates/mantle/src/files.rs` and `secret::read`: a key is written owner-only and
read only from a file no one but its owner may read; a start refuses a key others may read.

[10] Kubernetes, "Secrets", https://kubernetes.io/docs/concepts/configuration/secret/.
"Individual Secrets are limited to 1MiB in size. This is to discourage creation of very large
Secrets that could exhaust the API server and kubelet memory." "The keys of `data` and
`stringData` must consist of alphanumeric characters, `-`, `_` or `.`." "A container using a
Secret as a subPath volume mount does not receive automated Secret updates."

[11] Kubernetes API reference, "Service", ServiceSpec `publishNotReadyAddresses`,
https://kubernetes.io/docs/reference/kubernetes-api/service-resources/service-v1/.
"publishNotReadyAddresses indicates that any agent which deals with endpoints for this Service
should disregard any indications of ready/not-ready." "The Kubernetes controllers that generate
Endpoints and EndpointSlice resources for Services interpret this to mean that all endpoints are
considered "ready" even if the Pods themselves are not."

[12] mantle, `crates/gateway/src/put.rs`: a block whose chunks no volume the placement offered
took is refused `PutError::Unplaced`, answered `503 SlowDown`; a block's chunks go to distinct
volumes, and a cell of three domains keeps a block's three copies on three nodes.

## What the sources settle

**A founder is named by a Service of its own.** A pod's address is its own and goes with it [2],
and a node resolves its peers' names once, at its start [7]: a peer deleted and made again comes
back at another address its peers never look up. A Service selecting one pod by its
`statefulset.kubernetes.io/pod-name` label [1] has a cluster IP assigned when the Service is made
and not changed through updates [3], so a name for it resolves to the same address across every
deletion of the pod behind it, before that pod first runs and after. QUIC and the plane are UDP,
which a Service carries [2]. Each founder therefore binds `0.0.0.0` and is dialed at its own
Service's name, which `mantle init` states as the founder's reach (`reach=HOST:PORT`), the address
its peers' `peer` lines and its clients' `node` lines name where it differs from the one it binds
[8].

**Founders start together.** A cell needs a majority to elect, and `OrderedReady` waits for each
pod to be Ready before the next [1], which a founder alone never is: the StatefulSet's pods start
`Parallel` [1].

**Keys are installed owner-only.** A Secret's projected file takes `defaultMode`, 0644 unless
stated, and `fsGroup` "can" set "other mode bits" [5, 6], while a node reads a key only from a
file no one but its owner may read [9]. The founder's directory therefore comes from its Secret
through an install step that copies it into the node's volume as the pod's user, keys owner-only,
once, before its first start; later starts keep what the volume holds.

**A founder is one Secret's value.** A Secret's keys name flat files of a few characters [10],
while a founder's directory holds directories and files its node ids name, so the directory is
carried as one file, `mantle pack`'s, its files' paths and key marks inside it, checksummed, and
at most a Secret's 1 MiB [10]; each pod mounts its own by `subPathExpr`, which a later change of
the Secret does not reach [10], as the install is once.

**Per-pod Services publish not-ready addresses.** A founder becomes ready only once the cell it
forms with its peers enlisted it, and its peers reach it at its Service; endpoints of a Service
that publishes not-ready addresses are counted ready [11], so each founder's Service carries its
traffic before it is ready. The cell's S3 Service does not, so only enlisted founders take
requests [4].

**A disruption's refusals.** With one of three founders gone, a block cannot be placed on three
distinct nodes, and the gateway refuses it `SlowDown` [12]: a harness that deletes a founder's pod
mid-transfer counts such puts unacknowledged, and judges the cell by every acknowledged object
reading back exactly through every node once the pod's successor is ready.

**Ready once enlisted.** A pod's readiness decides whether the cell's S3 Service sends it requests
[4]. A node binds its listener before it has enlisted [7], so a bound port says nothing of whether
it serves; the node's own word does: it is ready once this run has enlisted.

**Stopped within the grace.** SIGTERM begins the node's orderly stop, bounded by `stop_ms` [7],
and the pod is killed at its grace [4]: the grace is stated above `stop_ms`.

## Open

kind's local-path storage does not hold a claim to its stated size (the provisioner's own
README), so a founder's volume bytes are stated in the chart's values rather than read from its
claim; a provisioner that does enforce sizes is not measured here.
