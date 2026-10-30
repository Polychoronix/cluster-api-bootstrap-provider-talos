# Cluster API Bootstrap Provider Talos (CABPT)

<!-- textlint-disable -->
> [!CAUTION]
> Sidero Labs is no longer actively developing Cluster API Bootstrap Provider Talos.
>
> For an alternative, please see [Omni](https://github.com/siderolabs/omni.git).
>
> Unless you have an existing support contract covering Cluster API Bootstrap Provider Talos, all support will be provided by the community (including questions in our Slack workspace).
<!-- textlint-enable -->

## Intro

The Cluster API Bootstrap Provider Talos (CABPT) is a project by [Sidero Labs](https://siderolabs.com/) that provides a [Cluster API](https://github.com/kubernetes-sigs/cluster-api) (CAPI) bootstrap provider for use in deploying Talos-based Kubernetes nodes across any environment.
Given some basic info, this provider will generate bootstrap configurations for a given machine and reconcile the necessary custom resources for CAPI to pick up the generated data.

## Corequisites

There are a few corequisites and assumptions that go into using this project:

- [Cluster API](https://github.com/kubernetes-sigs/cluster-api)
- [Talos](https://talos.dev/)


## Personal fork release tooling

This fork's source is
[`Polychoronix/cluster-api-bootstrap-provider-talos`](https://github.com/Polychoronix/cluster-api-bootstrap-provider-talos).
Local image defaults use the personal account's lowercase registry namespace,
`ghcr.io/polychoronix/cluster-api-talos-controller`. `SOURCE_URL`, the full Git
`REVISION` and `TAG` supply OCI source/revision/version labels; the license remains
MPL-2.0. A source checkout or locally rendered tag is not a published release.

`make rekres` uses native Go (at least 1.26.2) and the exact Kres source revision
[`15ff2fda3c9184427ef4f8ba0e2b94389b0f8dc7`](https://github.com/siderolabs/kres/tree/15ff2fda3c9184427ef4f8ba0e2b94389b0f8dc7).
It runs `gen` in a disposable repository with `.kres.yaml`'s
`compileGHWorkflowsOnly: true`, explicitly unsets `GITHUB_TOKEN`, and publishes
only the generated `ci.yaml`. It does not overwrite Makefile, Dockerfile, Conform,
Renovate or auxiliary workflows, and does not configure GitHub remotely. Its
generated CI has no tag trigger or publisher. The inherited `generic` runner
group is inaccessible to this personal fork and must not be treated as usable CI.

The maintained `source.yaml` is nonpublishing validation on public Ubuntu runners:
it checks pinned API/CRD generation freshness, source tests, an actual Kustomize
release render and the controller build. It does not run `make test` or the
ambient-cluster integration suite. The historical `.conform.yaml` is deliberately
preserved, not adopted as a personal-account signing policy: Conform's
[`gitHubOrganization` resolver](https://github.com/siderolabs/conform/blob/b951e4415e9cde1cc0cda7ad3eb467e195d8eb77/internal/policy/commit/check_gpg_identity.go#L54-L79)
lists organization members, not a personal user's keys. Neither GPG-required nor
DCO enforcement is weakened here; enabling fork conformance requires a separately
reviewed trust-policy decision. Public source CI does not invoke that policy.

The sole tag publisher is the maintained `release.yaml`. It uses the actual Git
tag and full source commit, lowercases `github.repository_owner` for GHCR,
classifies hyphenated versions as prereleases, and renders deployment images from
the actual pushed buildx digest, never a guessed digest or community image. The
workflow requires real publication credentials; local `make container` defaults
to `PUSH=false`.

The tag publisher installs checksum-verified Kustomize 4.1.0, matching source
validation and the Docker release stage, and passes its isolated runner-temporary
binary path to `make release-manifests` rather than using ambient Kustomize.

For an explicitly supplied local-render tag and installed Kustomize, run:

```sh
: "${TAG:?Set the reviewed release or local-render tag}"
make release-manifests TAG="$TAG" \
  CONTROLLER_IMAGE="ghcr.io/polychoronix/cluster-api-talos-controller:$TAG" \
  KUSTOMIZE=kustomize
RELEASE_TEST_KUSTOMIZE=kustomize go test -count=1 ./internal/release
```

The single renderer, `hack/release-manifests.sh`, is shared by native Make and the
Docker release stage. It copies only public `config/` inputs and outputs
`_out/bootstrap-talos/$TAG/{bootstrap-components.yaml,metadata.yaml,SHA256SUMS,SHA512SUMS}`.
Both checksum files cover the rendered components and metadata. The pinned Linux
amd64 Kustomize 4.1.0 binary's
[published SHA256](https://github.com/kubernetes-sigs/kustomize/releases/download/kustomize/v4.1.0/checksums.txt)
is `77263c66afd04106348bebd019096933e26c0054d5b0ade8265f3426cf440ae6`;
its historical module cannot be installed with `go install package@version`
because it contains exclude directives. Native tooling and the Docker release
stage verify the published binary instead.

Use an actual published digest when rendering deployable release components.
Local rendering does not prove image publication, public visibility, anonymous
pulls or live deployment. The paired CACPPT dependency and release version must
match the approved CABPT fork release exactly before integrating either provider.

## Installing

Install this CABPT fork only with the matched
[`Polychoronix/cluster-api-control-plane-provider-talos`](https://github.com/Polychoronix/cluster-api-control-plane-provider-talos)
fork and its exact CABPT dependency. **The current integration branch is
source-only: no matched personal-fork provider release is published yet.** A
public source commit, local component render or successful native build is not
an available image or provider release. Do not use the community provider catalog
or a `latest` release as an accidental substitute.

After separately approving and publishing both matched releases, verify their
component/metadata checksums, actual image digests and required image visibility
or pull access. Select that exact approved version and the personal-fork sources
explicitly. The examples below use `v0.9.0-craci.1` **only as a candidate/example**;
they describe the standard GitHub release-asset layout, not currently published
assets. Replace the example version everywhere with the actual approved
published version before running an installation.

For `clusterctl`, configure its supported provider-source overrides in
`~/.cluster-api/clusterctl.yaml` (preserve any unrelated entries):

```yaml
providers:
  - name: talos
    type: BootstrapProvider
    url: https://github.com/Polychoronix/cluster-api-bootstrap-provider-talos/releases/download/v0.9.0-craci.1/bootstrap-components.yaml
  - name: talos
    type: ControlPlaneProvider
    url: https://github.com/Polychoronix/cluster-api-control-plane-provider-talos/releases/download/v0.9.0-craci.1/control-plane-components.yaml
```

Then use the same published version explicitly for both providers, with an
independently reviewed infrastructure provider/version:

```sh
: "${TALOS_PROVIDER_VERSION:?Set the approved matched published version}"
: "${INFRASTRUCTURE_PROVIDER:?Set the reviewed infrastructure provider:version}"
clusterctl init \
  --core cluster-api:v1.14.2 \
  --bootstrap "talos:$TALOS_PROVIDER_VERSION" \
  --control-plane "talos:$TALOS_PROVIDER_VERSION" \
  --infrastructure "$INFRASTRUCTURE_PROVIDER"
```

For installations managed by Cluster API Operator, configure the existing
`BootstrapProvider` and `ControlPlaneProvider` resources instead of running a
second installer. Use its supported
[`spec.version` and `spec.fetchConfig.url` configuration](https://cluster-api-operator.sigs.k8s.io/topics/configuration/provider-spec-configuration)
with the same approved published version. Operator fetch URLs are repository
**release endpoints**, not the direct component-file URLs used above:

| Operator resource | `spec.version` | `spec.fetchConfig.url` |
|---|---|---|
| `BootstrapProvider/talos` | Actual approved matched version | `https://github.com/Polychoronix/cluster-api-bootstrap-provider-talos/releases` |
| `ControlPlaneProvider/talos` | Same approved matched version | `https://github.com/Polychoronix/cluster-api-control-plane-provider-talos/releases` |

Keep the reviewed provider namespaces, configuration and adoption/rollout gates.
These standard source overrides do not authorize replacing a live provider,
adopting Machines or disrupting nodes; those remain separately reviewed steps.

## Compatibility

### Current modified fork

The current fork targets CAPI **1.14.2 / v1beta2**. Local verification covers
Talos **1.14** complete-config rendering, imported mTLS transport, management
observation and journaled update/recovery contracts with isolated API/runtime
tests. This is not a claim of published images, live rollout validation or full
legacy Talos runtime support. The OS-install observation contract is currently
1.14; older capability support remains unresolved until equivalent observations
are implemented and tested. See the actual
[in-place change/runtime capability matrix](docs/in-place-updates.md#what-can-be-updated-in-place)
and [existing-node management contract](docs/in-place-updates.md#declarative-existing-node-management).

### Historical community release series

The preserved tables below describe the historical community releases, not the
modified personal fork's verified runtime/adoption capabilities. A historical
checkmark must not be used to infer lifecycle support for an older Talos version
after this cutover.

Historical Cluster API compatibility:

|                | v1alpha3 (v0.3) | v1alpha4 (v0.4) | v1beta1 (v1.x) | v1beta2 (v1.12+) |
| -------------- | --------------- | --------------- | -------------- | ---------------- |
| CABPT (v0.5.x) |                 |                 | ✓              |                  |
| CABPT (v0.6.x) |                 |                 | ✓              |                  |
| CABPT (v0.7.x) |                 |                 |                | ✓                |
| CABPT (v0.8.x) |                 |                 |                | ✓                |
| CABPT (v0.9.x) |                 |                 |                | ✓                |

Historical Kubernetes installation/configuration support:

|                | v1.19 | v1.20 | v1.21 | v1.22 | v1.23 | v1.24 | v1.25 | v1.26 | v1.27 | v1.28 | v1.29 | v1.30 | v1.31 | v1.32 | v1.33 | v1.34 | v1.35 | v1.36 | v1.37 |
| -------------- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- |
| CABPT (v0.5.x) | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     |       |       |       |       |       |       |       |       |       |       |       |
| CABPT (v0.6.x) |       |       |       |       |       |       |       | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     |       |
| CABPT (v0.7.x) |       |       |       |       |       |       |       |       |       |       | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     |       |
| CABPT (v0.8.x) |       |       |       |       |       |       |       |       |       |       | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     |       |
| CABPT (v0.9.x) |       |       |       |       |       |       |       |       |       |       | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     |

Historical Talos configuration compatibility:

|                  | v1.0  | v1.1  | v1.2  | v1.3  | v1.4  | v1.5  | v1.6  | v1.7  | v1.8  | v1.9  | v1.10 | v1.11 | v1.12 | v1.13 | v1.14 |
| ---------------- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- |
| CABPT (v0.5.x)   | ✓     | ✓     | ✓     | ✓     |       |       |       |       |       |       |       |       |       |       |       |
| CABPT (v0.6.x)   |       |       | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     |       |
| CABPT (v0.7.x)   |       |       |       |       |       |       |       |       |       | ✓     | ✓     | ✓     | ✓     | ✓     |       |
| CABPT (v0.8.x)   |       |       |       |       |       |       |       |       |       |       | ✓     | ✓     | ✓     | ✓     |       |
| CABPT (v0.9.x)   |       |       |       |       |       |       |       |       |       |       | ✓     | ✓     | ✓     | ✓     | ✓     |

The `talosVersion` field requests the machine-configuration generation contract
(see below). A generated or parseable configuration is not proof of observation,
upgrade, downgrade or recovery support for that OS version; use the current fork
capability matrix above.

## Usage

CABPT is not used directly, but rather via CACPPT (`TalosControlPlane`) for control plane nodes or via `MachineDeployment` (`MachinePool`) for worker nodes.
In either case, CABPT settings are passed via `TalosConfigTemplate` resource:

```yaml
apiVersion: controlplane.cluster.x-k8s.io/v1alpha3
kind: TalosControlPlane
metadata:
  name: mycluster-cp
spec:
  controlPlaneConfig:
    controlplane:
      generateType: controlplane
      talosVersion: v1.11
  ...
```

```yaml
apiVersion: bootstrap.cluster.x-k8s.io/v1alpha3
kind: TalosConfigTemplate
metadata:
  name: mycluster-workers
spec:
  template:
    spec:
      generateType: worker
      talosVersion: v1.9
```

Fields available in the `TalosConfigTemplate` (and `TalosConfig`) resources:

- `generateType`: Talos machine configuration type to generate (`controlplane`, `init` (deprecated), `worker`) or `none` for user-supplied configuration (see below)
- `talosVersion`: version of Talos to generate machine configuration for (e.g. `v1.0`, patch version might be omitted).
   CABPT defaults to the latest supported Talos version, but it can generate configuration compatible with previous versions of Talos.
   It is recommended to always set this field explicitly to avoid issues when CABPT is upgraded to the version which supports new Talos version.
- `configPatches` (optional): set of machine configuration patches to apply to the generated configuration.
- `data` (only for `generateType: none`): user-supplied machine configuration.
- `hostname` (optional): configure hostname in the generate machine configuration:
  - `source` (`MachineName`): set the hostname in the generated machine configuration to the `Machine` name (not supported with `MachinePool` deployments)
- `imageFactory` (optional): the [Talos Image Factory](https://factory.talos.dev) schematic the machine installs and upgrades with.
  CABPT registers the schematic, pins the Talos version (a bare minor in `talosVersion` resolves to the newest released patch once and is recorded in `status.imageFactory`), and renders `<factory>/metal-installer/<schematic>:<version>` as the installer image: an `UnattendedInstallConfig` document from Talos 1.14 (disk selector `system_disk`, override it with a strategic patch), `machine.install.image` before.
  The controller talks to the Factory at `--image-factory-url` (default `https://factory.talos.dev`). Fields:
  - `extensions`: official system extension names, e.g. `siderolabs/nvme-cli`; each must exist for the resolved version.
  - `extraKernelArgs`: kernel arguments baked into the Factory images.
  - `overlay`: `{name, image}` for single-board computers.
  - `bootloader`: `auto`, `dual-boot`, `grub` or `sd-boot`.

### Generated Machine Configuration

When `generateType` is set to the machine type of the Talos nodes (`controlplane` for control plane nodes and `worker` for worker nodes), CABPT generates a set of cluster-wide
secrets which are used to provision machine configuration for each node.
Machine configuration generated is compatible with the Talos version set in the `talosVersion` field.

```yaml
spec:
  generateType: controlplane
  talosVersion: v1.5
```

### User-supplied Machine Configuration

In this mode CABPT passes through machine configuration set in `data` field as bootstrap data to the `Machine`.
Machine configuration can be generated with `talosctl gen config`.

```yaml
spec:
  generateType: none
  data: |
    version: v1alpha1
    machine:
      type: controlplane
    ...
    ...
    ...
```

### Secret-backed configuration inputs

Use `configSecretRef: {name: baseline, key: config}` for a complete Talos baseline,
including every document. References are required, namespace-local Secret keys;
no arbitrary namespace, filesystem path, or templated Secret name is accepted.
The baseline bypasses generation and retains imported identity, networking and
storage. It is mutually exclusive with public `data`. Use `generateType: none`,
`worker` or `controlplane`; an explicit role must match the baseline.

`strategicPatchSecretRefs` supplies an ordered list of private strategic patches.
`machineConfig` supports a per-Machine `configSecretRef`, `configPatches`,
`strategicPatches`, `strategicPatchSecretRefs` and `configRevision` without
replacing shared patch policy. A Machine baseline replaces only the base.
Patch precedence is shared public JSON, resolved installer image, shared public
strategic/generated hostname, shared Secret patches, Machine public JSON and
strategic patches, then Machine Secret patches. Talos strategic merging preserves
unrelated documents/list entries. JSON6902 remains limited to single-document
configuration, as required by Talos machinery; multi-document inputs fail safely.

Keep private configuration in Secrets, never public `data`/patches or revisions.
Changing an owner/template's non-secret `configRevision` triggers requested
reconciliation after a same-name Secret rotation. Shared and Machine revisions
are independent; public patch/version changes do not need an extra revision.
Secret changes alone do not apply live drift. Generated Secret data contains
private `inputFreshness` metadata binding desired objects and Secret UID/version
identities; runtime updates use the live TalosConfig output rather than an
adopted Machine's original installation seed. No resolved private digest is
published on Machine/TalosConfig annotations or conditions. Existing MachinePool
convergence annotations retain only the public spec/reference/revision token.
MachineDeployment updates rotate the immutable `TalosConfigTemplate`: create a
new named template containing the new public desired revision, then update the
MachineDeployment bootstrap template reference. Do not edit an existing template
in place. Both old and new MachineSets must have bootstrap templates for in-place
planning; establish that association before initial owner adoption.

On imported baselines, a resolved installer image changes only the image and
retains the complete reviewed install/provisioning/disk-selection policy. Missing
installer policy requires reviewed input rather than a generated disk default.
The final role must match the original baseline/generated role after all patches.
Baseline-backed Kubernetes transitions require a reviewed baseline/patch whose
kubelet and enabled control-plane component image version tags match the
requested Machine version; mismatches fail before apply. Custom registry ports
and version-tag-plus-digest images work; digest-only/opaque version tags cannot
prove a target. Baseline import does not silently rewrite actual versions.

### Explicit Talos management endpoints

Set `talosEndpoint` on the per-Machine TalosConfig with `address: host:port`,
`serverName: native-talos-hostname` and optional `node` for single-node proxy
routing. Dial transport and verified TLS identity are independent; Kubernetes
InternalIP/ExternalIP selection is not changed. `pkg/talosclient.New` uses the
standard `<cluster>-talosconfig` Secret's `talosconfig` key with strict Talos mTLS;
call routed RPCs with `pkg/talosclient.WithNode`. One client serves one endpoint.
Clients use no ambient talosconfig, insecure maintenance mode or filesystem
credentials. Ordinary provisioning retains its address fallback.

Existing imported management identity Secrets remain read-only, including
endpoints and ownership. Newly provider-created identity Secrets carry a
creation-only provenance marker; matching Cluster owner/CA/bundle without that
marker never authorizes takeover. Baseline CA extraction uses the exact imported
issuing certificate/key; workers without an issuing key require imported mTLS
credentials matching their accepted CA. Invalid/mismatching credentials fail
without replacement. Requested updates use private durable operation journals and
observed completion receipts; see [in-place updates](docs/in-place-updates.md).
Fresh initial CAPI bootstrap lineage supports full Secret baselines, while
existing seed/pivot/adoption requires separately reviewed observation/activation.
Legacy OS compatibility and real Service/hardware rollout remain explicit gates.

Local regression checks use real Talos-generated multi-document fixtures and
in-memory mTLS gRPC RPCs. The optional disposable controller/admission/conversion
smoke runs with an explicitly supplied `KUBEBUILDER_ASSETS` directory:
`go test ./controllers -run '^TestConfigInputControllerDisposableEnvtest$'`.
It starts its own API server/etcd and never uses an existing kubeconfig/cluster.


### Configuration Patches

Machine configuration can be customized by applying configuration patches.
Any field of the [Talos machine configuration](https://www.talos.dev/docs/latest/reference/configuration/)
can be overridden on a per-machine basis using this method.

There are two [patch formats](https://www.talos.dev/latest/talos-guides/configuration/patching/) supported by CABPT:

- the [JSON 6902](http://jsonpatch.com/) format that you may be used to in tools like `kustomize`;
- strategic merge patches which look like incomplete machine configuration documents.

See Talos Linux documentation for more information on patching.

> [!IMPORTANT]
> JSON patches are not compatible with multi-document Talos Linux machine configuration.
> JSON patches are not compatible with Talos Linux >= 1.12.

JSON 6902 patch:

```yaml
spec:
  generateType: controlplane
  talosVersion: v1.6
  configPatches:
    - op: replace
      path: /machine/install
      value:
        disk: /dev/sda
    - op: add
      path: /cluster/network/cni
      value:
        name: custom
        urls:
          - https://docs.projectcalico.org/v3.18/manifests/calico.yaml
```

Strategic merge patch:

```yaml
spec:
  generateType: controlplane
  talosVersion: v1.6
  strategicPatches:
    - |
      machine:
        install:
          disk: /dev/sda
    - |
      cluster:
        network:
          cni:
            name: custom
            urls:
              - https://docs.projectcalico.org/v3.18/manifests/calico.yaml
```

### Retrieving `talosconfig`

Client-side `talosconfig` is required to access the cluster using Talos API.
CABPT generates `talosconfig` for generated machine configuration and stores it as `<cluster>-talosconfig` secret in cluster's namespace.

`talosconfig` can be retrieved with:

```shell
kubectl get secret --namespace <cluster-namespace> <cluster-name>-talosconfig -o jsonpath='{.data.talosconfig}' | base64 -d > cluster-talosconfig
talosctl config merge cluster-talosconfig
talosctl -n <IP> version
```

CABPT updates endpoints only for positively provider-created generated identity
Secrets. Existing imported credentials/endpoints are validated and never rewritten.

### Operation

CABPT reconciles `TalosConfig` resources.
Once `TalosConfig` and its associated `Machine` are ready, CABPT generates machine configuration and stores it in the `<machine>-bootstrap-data` Secret.
Kubernetes cluster CA is stored in the `<cluster>-ca` Secret.
Cluster-wide shared secrets are stored in the `<cluster>-talos` Secret.
Client-side Talos API configuration is stored in the `<cluster>-talosconfig` Secret.

As part of its operation, CABPT sets a number of Conditions on the `TalosConfig/Status` resource:

- `DataSecretAvailable`: CABPT generated machine configuration in `<machine>-bootstrap-data` Secret, CABPT unblocks infrastructure provider to boot the `Machine`.
- `ClientConfigAvailableCondition`: CABPT generated Talos client configuration in `<cluster>-talosconfig` Secret.

```shell
$ kubectl describe talosconfig talosconfig-cp-0
Status:
  Conditions:
    Last Transition Time:  2021-10-25T20:54:09Z
    Status:                True
    Type:                  Ready
    Last Transition Time:  2021-10-25T20:54:09Z
    Status:                True
    Type:                  ClientConfigAvailable
    Last Transition Time:  2021-10-25T20:54:09Z
    Status:                True
    Type:                  DataSecretAvailable
  Data Secret Name:        cp-0-bootstrap-data
```

If CABPT fails to perform its operations, it stores error message for the respective Condition.

```shell
Status:
  Conditions:
    Last Transition Time:  2021-10-25T20:54:09Z
    Message:               configuration input shared public: JSON patch failed
    Reason:                DataSecretGenerationFailed
    Severity:              Error
    Status:                False
```

These statuses are also presented in the `clusterctl describe cluster --show-conditions all` output.

### Limitations

Once CABPT has rendered a `Machine`'s configuration into its `<machine>-bootstrap-data` Secret, editing the `TalosConfig` spec does not re-render it.
Nothing re-reads that Secret after the machine has booted, so the change takes effect only for machines created afterwards, or through the [in-place update flow](docs/in-place-updates.md).
To roll it out to running machines, use an in-place update or replace the machines.

A `MachinePool` is different, because a pool keeps a single `<pool>-bootstrap-data` Secret that its infrastructure provider re-reads every time it creates an instance.
There a spec change **is** acted on: CABPT re-renders the Secret in place, keeping its name, and applies the result to the pool's running members over the Talos API, one node at a time.
This is CABPT's own behaviour rather than the Cluster API in-place update flow, which has no MachinePool path at all; see [in-place updates](docs/in-place-updates.md) for what it does and does not cover, and `--enable-machine-pool-in-place-updates` to turn it off.
It is on by default, so **upgrading to a build that has it applies the pool's current spec to every running member without prompting** — including a spec edit that was inert while the Secret was frozen, which may reboot each node in turn; read [Adopting it on an existing pool](docs/in-place-updates.md#adopting-it-on-an-existing-pool) before you upgrade.

## Building

With Docker available, `make release TAG="$TAG"` builds local release artifacts
for the explicitly supplied tag; publication remains disabled unless approved
and requested with `PUSH=true`. The components are at
`_out/bootstrap-talos/$TAG/bootstrap-components.yaml`, beside metadata and both
checksum files. Native rendering and public release tooling are described above.
After separately approving deployment, that exact file can be installed with
`kubectl apply -f "_out/bootstrap-talos/$TAG/bootstrap-components.yaml"`.

## Support

Join our [Slack](https://slack.dev.talos-systems.io)!
