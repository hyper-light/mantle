#!/bin/sh
# The KIND cell's disruption (scripts/kind-cell.sh): deletes founder pod $1 in namespace $2 and waits
# until the StatefulSet's successor of the same name is ready, each wait on its fact: the old pod
# gone, the new one made, the new one ready (`mantle status`: enlisted in its run). The harness runs
# it halfway through its first small set's native puts (`mantle bench transfer --disrupt`).
# kubectl takes no wait without a positive bound ("Timeout for context shall not be negative",
# kubectl 1.37): each is bounded by the CI job's own timeout, 2 hours, which bounds the run.
set -eu
pod="$1"
namespace="$2"
bound=2h
kubectl -n "$namespace" delete pod "$pod" --wait=true --timeout="$bound"
kubectl -n "$namespace" wait --for=create "pod/$pod" --timeout="$bound"
kubectl -n "$namespace" wait --for=condition=Ready "pod/$pod" --timeout="$bound"
