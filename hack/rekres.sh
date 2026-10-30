#!/bin/sh
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
set -eu

revision=${KRES_REVISION:?the Makefile must supply the pinned Kres source revision}
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT HUP INT TERM

# Only public generator inputs enter the disposable repository. In particular,
# no repository credentials, runtime snapshots or ambient kubeconfig are copied.
cp "$root/.kres.yaml" "$scratch/.kres.yaml"
cp "$root/go.mod" "$scratch/go.mod"
mkdir -p "$scratch/.github/workflows"
cp "$root/.github/workflows/ci.yaml" "$scratch/.github/workflows/ci.yaml"
git -C "$scratch" init --quiet --initial-branch=main
git -C "$scratch" remote add origin https://github.com/Polychoronix/cluster-api-bootstrap-provider-talos.git
git -C "$scratch" config branch.main.remote origin
git -C "$scratch" config branch.main.merge refs/heads/main
(
  cd "$scratch"
  env -u GITHUB_TOKEN GOMAXPROCS=2 GOFLAGS=-p=1 \
    go run -ldflags "-X github.com/siderolabs/kres/internal/version.Tag=$revision" \
    "github.com/siderolabs/kres/cmd/kres@$revision" gen
)

# Kres also emits Conform and auxiliary workflows. Those are not this target's
# outputs: an origin owner is not a valid personal-account GPG trust policy.
if ! cmp -s "$scratch/.github/workflows/ci.yaml" "$root/.github/workflows/ci.yaml"; then
  cp "$scratch/.github/workflows/ci.yaml" "$root/.github/workflows/ci.yaml"
fi
