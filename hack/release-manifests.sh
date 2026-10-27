#!/bin/sh
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
set -eu

if [ "$#" -ne 3 ]; then
  printf 'Usage: %s TAG CONTROLLER_IMAGE ARTIFACTS_DIRECTORY\n' "$0" >&2
  exit 1
fi
tag=$1
image=$2
artifacts=$3
case "$tag" in ''|*/*|.|..) printf 'TAG must be an explicit release tag without path separators\n' >&2; exit 1;; esac
if [ -z "$image" ]; then
  printf 'CONTROLLER_IMAGE must be supplied (tagged local render or actual published digest)\n' >&2
  exit 1
fi
kustomize=${KUSTOMIZE:-kustomize}
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT HUP INT TERM

# Copy only public component sources, never the repository's configuration,
# credentials, test output, runtime snapshots or an ambient kubeconfig.
cp -R "$root/config" "$scratch/config"
(
  cd "$scratch/config/manager"
  "$kustomize" edit set image "controller=$image"
)
output="$artifacts/bootstrap-talos/$tag"
mkdir -p "$output"
"$kustomize" build "$scratch/config/default" > "$output/bootstrap-components.yaml"
cp "$root/config/metadata/metadata.yaml" "$output/metadata.yaml"
(
  cd "$output"
  sha256sum bootstrap-components.yaml metadata.yaml > SHA256SUMS
  sha512sum bootstrap-components.yaml metadata.yaml > SHA512SUMS
)
