#!/usr/bin/env bash
set -euo pipefail

bundle="$1"
entries="$(unzip -Z1 "$bundle")"
artifacts="$(grep -E '\.(jar|pom)$' <<< "$entries" || true)"

if [[ -z "$artifacts" ]]; then
  echo "Bundle $bundle holds no jars or poms" >&2
  exit 1
fi

if grep -q 'flink-projections-examples' <<< "$entries"; then
  echo "Bundle $bundle includes flink-projections-examples, which must not be published" >&2
  exit 1
fi

unsigned="$(while read -r artifact; do grep -qxF "$artifact.asc" <<< "$entries" || echo "$artifact"; done <<< "$artifacts")"
if [[ -n "$unsigned" ]]; then
  echo "Bundle $bundle has artifacts without a signature:" >&2
  echo "$unsigned" >&2
  exit 1
fi

echo "$artifacts"
