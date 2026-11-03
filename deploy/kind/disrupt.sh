#!/bin/sh
# The KIND cell's disruption (scripts/kind-cell.sh): deletes founder pod $1 in namespace $2 and waits
# until the StatefulSet's successor of the same name is ready, each wait on its fact: the old pod
# gone, the new one made, the new one ready (`mantle status`: enlisted in its run). The harness runs
# it halfway through its first small set's native puts (`mantle bench transfer --disrupt`). The
# waits' bound is the harness Job's own deadline.
set -eu
pod="$1"
namespace="$2"
kubectl -n "$namespace" delete pod "$pod" --wait=true --timeout=-1s
kubectl -n "$namespace" wait --for=create "pod/$pod" --timeout=-1s
kubectl -n "$namespace" wait --for=condition=Ready "pod/$pod" --timeout=-1s
