#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: check-bundle.sh <central-bundle.zip> <version>" >&2
  exit 2
fi

bundle="$1"
version="$2"
group_path='io/github/mannkostir'
library_modules=(flink-projections flink-projections-kafka flink-projections-elasticsearch)

expected_artifacts() {
  echo "$group_path/flink-projections-parent/$version/flink-projections-parent-$version.pom"
  for module in "${library_modules[@]}"; do
    local prefix="$group_path/$module/$version/$module-$version"
    echo "$prefix.pom"
    echo "$prefix.jar"
    echo "$prefix-sources.jar"
    echo "$prefix-javadoc.jar"
  done
}

entries="$(unzip -Z1 "$bundle")"
artifacts="$(grep -E '\.(jar|pom)$' <<< "$entries" || true)"

if [[ -z "$artifacts" ]]; then
  echo "Bundle $bundle holds no jars or poms" >&2
  exit 1
fi

missing="$(expected_artifacts | while read -r expected; do grep -qxF "$expected" <<< "$entries" || echo "$expected"; done)"
if [[ -n "$missing" ]]; then
  echo "Bundle $bundle is missing expected artifacts:" >&2
  echo "$missing" >&2
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
