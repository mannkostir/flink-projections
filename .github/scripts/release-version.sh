#!/usr/bin/env bash
set -euo pipefail

ref_type="$1"
ref_name="$2"
dry_run="$3"
release_tag='^v[0-9]+\.[0-9]+\.[0-9]+$'

if [[ "$ref_type" == "tag" ]]; then
  if [[ ! "$ref_name" =~ $release_tag ]]; then
    echo "Tag '$ref_name' is not a release tag: use vMAJOR.MINOR.PATCH, for example v0.1.0" >&2
    exit 1
  fi
  echo "version=${ref_name#v}"
  echo "publish=true"
  exit 0
fi

if [[ "$dry_run" == "true" ]]; then
  echo "version=0.0.0-dryrun"
  echo "publish=false"
  exit 0
fi

echo "A release publishes only from a vMAJOR.MINOR.PATCH tag: run this workflow by hand only with dry-run ticked" >&2
exit 1
