#!/usr/bin/env bash
# The KIND cell (plan step 11; docs/design/kubernetes.md): the mantle image built, a kind cluster of
# a control plane and three workers made, a cell of three made by `mantle init` with each founder
# reached at its own Service's name, the harness's gateway pinned, each founder's pack and the
# cell's set as Secrets, the chart installed and waited for until every founder is ready (enlisted
# in its run), one founder to a kind node checked, and the harness's CI scale run through both doors
# from a Job in the cluster, founder pod mantle-1 deleted halfway through its small set's native
# puts. Exits as the harness Job did: 0 only with every check exact. The cluster is deleted at the
# end, and the work directory with it.
#
# Needs docker, kind, kubectl and helm on the PATH (the CI job pins them).
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
cluster="${KIND_CLUSTER:-mantle-kind}"
namespace=mantle
release=mantle
founders=3
work="$(mktemp -d)"
# What the cell's pods were when a step failed: their state, events and every container's log, the
# one before a restart too, said before the cluster goes.
diagnose() {
  set +e
  kubectl -n "$namespace" get pods,jobs,pvc -o wide
  kubectl -n "$namespace" describe pods
  for pod in $(kubectl -n "$namespace" get pods -o name); do
    echo "== $pod"
    kubectl -n "$namespace" logs "$pod" --all-containers=true --tail=400
    kubectl -n "$namespace" logs "$pod" --all-containers=true --previous --tail=400
  done
}
finish() {
  status=$?
  if [ "$status" -ne 0 ]; then
    diagnose
  fi
  kind delete cluster --name "$cluster" || status=1
  rm -rf -- "$work"
  exit "$status"
}
trap finish EXIT

docker build -t mantle:kind .
kind create cluster --name "$cluster" --config deploy/kind/cluster.yaml --wait 10m
server="$(kubectl version -o json | python3 -c 'import json, sys; print(json.load(sys.stdin)["serverVersion"]["gitVersion"])')"
docker build -f deploy/kind/harness.Dockerfile --build-arg KUBECTL="$server" -t mantle-harness:kind .
kind load docker-image --name "$cluster" mantle:kind mantle-harness:kind
kubectl create namespace "$namespace"

# The cell, made here by the image's own binary as this user: each founder binds every address on
# its ports and is reached at its Service's name, its volume at the claim the chart mounts.
mantle() {
  docker run --rm --user "$(id -u):$(id -g)" --volume "$work:/work" mantle:kind "$@"
}
args=(init /work/cell --max-nodes "$founders" --max-volumes "$founders")
for n in $(seq 0 $((founders - 1))); do
  args+=(--node "quic=0.0.0.0:7000,plane=0.0.0.0:7001,s3=0.0.0.0:7480,reach=$release-$n.$namespace.svc.cluster.local,name=$release-$n.$namespace.svc.cluster.local,name=$release-s3.$namespace.svc.cluster.local,domain=node-$n,volume=/volume:1G")
done
mantle "${args[@]}" | tee "$work/init.txt"
mantle init gateway /work/cell/gateway --cell /work/cell
# Founder N is the Nth `node` line init said, as it was the Nth --node.
n=0
while read -r _ _ dir _; do
  dir="${dir%:}"
  mantle pack "$dir" "/work/founder-$n"
  kubectl -n "$namespace" create secret generic "$release-$n-founder" --from-file=founder="$work/founder-$n"
  n=$((n + 1))
done < <(grep '^node ' "$work/init.txt")
test "$n" -eq "$founders"
mantle pack /work/cell /work/cell.pack
kubectl -n "$namespace" create secret generic mantle-harness-cell --from-file=cell="$work/cell.pack"

helm install "$release" deploy/helm/mantle --namespace "$namespace" \
  --set fullnameOverride="$release" --set image.repository=mantle --set image.tag=kind \
  --set storage.volumeSize=2Gi --wait --timeout 20m
kubectl -n "$namespace" get pods -o wide
placed="$(kubectl -n "$namespace" get pods -l app.kubernetes.io/instance="$release" -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | sort -u | wc -l)"
if [ "$placed" -ne "$founders" ]; then
  echo "kind-cell: $founders founders on $placed kind nodes, not one each" >&2
  exit 1
fi

kubectl apply -f deploy/kind/harness.yaml
# The Job's verdict: complete or failed, whichever it comes to first, each waited for as a fact of
# its own; its log said either way. kubectl takes no wait without a positive bound: each is the CI
# job's own timeout, 2 hours, which bounds the run.
kubectl -n "$namespace" wait --for=condition=Complete job/mantle-harness --timeout=2h >/dev/null &
complete=$!
kubectl -n "$namespace" wait --for=condition=Failed job/mantle-harness --timeout=2h >/dev/null &
failed=$!
ended=""
wait -n -p ended "$complete" "$failed" || true
for pid in "$complete" "$failed"; do
  if [ "$pid" != "$ended" ]; then
    kill "$pid"
    # Its own end, the signal's: it was waiting on what will not come now.
    wait "$pid" || true
  fi
done
kubectl -n "$namespace" logs job/mantle-harness --all-containers=true
complete="$(kubectl -n "$namespace" get job mantle-harness -o jsonpath='{.status.succeeded}')"
if [ "$complete" != "1" ]; then
  echo "kind-cell: the harness Job did not complete" >&2
  exit 1
fi
