#!/usr/bin/env bash
set -euo pipefail

destination=${1:?Usage: bash scripts/download-openapi-nexus.sh DESTINATION}
version=0.2.3
asset=openapi-nexus-x86_64-unknown-linux-musl

mkdir -p -- "$destination"
curl -fsSL "https://github.com/rust-codegen-group/openapi-nexus/releases/download/$version/$asset.tar.xz" \
  | tar -xJ --strip-components=1 -C "$destination"
"$destination/openapi-nexus" --version
