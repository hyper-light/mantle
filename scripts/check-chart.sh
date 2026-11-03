#!/usr/bin/env bash
# The chart's gates (docs/design/kubernetes.md): `helm lint`, and `helm template` matching the golden
# render byte for byte, under the pinned helm the CI job installs. With --update, writes the render
# as the new golden instead.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
chart=deploy/helm/mantle
values="$chart/ci/values-golden.yaml"
golden="$chart/ci/golden.yaml"
helm version --short
helm lint "$chart" --values "$values"
render() {
  helm template mantle "$chart" --namespace mantle --values "$values"
}
if [ "${1:-}" = "--update" ]; then
  render > "$golden"
else
  render | diff -u "$golden" -
fi
