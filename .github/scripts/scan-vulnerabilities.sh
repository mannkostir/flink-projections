#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: scan-vulnerabilities.sh <bom.json>" >&2
  exit 2
fi

sbom="$1"
version='2.6.0'
config="$(cd "$(dirname "$0")/../.." && pwd)/osv-scanner.toml"

if [[ ! -f "$sbom" ]]; then
  echo "SBOM $sbom not found; generate it with: mvn org.cyclonedx:cyclonedx-maven-plugin:makeAggregateBom" >&2
  exit 2
fi

platform="$(uname -s)_$(uname -m)"
case "$platform" in
  Linux_x86_64)
    asset='osv-scanner_linux_amd64'
    checksum='ca69b3d3cd08f889a49dc0a383122f71cc528b83803671df5fd874d97485b108'
    ;;
  Linux_aarch64)
    asset='osv-scanner_linux_arm64'
    checksum='2c71403eb443d05891c4f268c3ad771cf4f16e5443463fd7851ef8f454d3c7e4'
    ;;
  Darwin_x86_64)
    asset='osv-scanner_darwin_amd64'
    checksum='60c5296637e977b28eeda5c7f13573e447659a632922737f94d11fa7e30ad6ca'
    ;;
  Darwin_arm64)
    asset='osv-scanner_darwin_arm64'
    checksum='98c460dcd37de25819babd757d04542045b6243113e209edcd4d89fedb0256b4'
    ;;
  *)
    echo "No pinned osv-scanner build for $platform" >&2
    exit 2
    ;;
esac

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT
scanner="$workdir/osv-scanner"

curl -fsSL --retry 3 -o "$scanner" "https://github.com/google/osv-scanner/releases/download/v$version/$asset"

actual="$(shasum -a 256 "$scanner" | cut -d ' ' -f 1)"
if [[ "$actual" != "$checksum" ]]; then
  echo "Checksum mismatch for $asset $version: expected $checksum, got $actual" >&2
  exit 1
fi
chmod +x "$scanner"

OSV_SCANNER_LOCAL_DB_CACHE_DIRECTORY="$workdir/db" "$scanner" scan source \
  --offline \
  --download-offline-databases \
  --config "$config" \
  --lockfile "$sbom"
