# UnattendedInstall rendering, lifecycle upgrades and a v1beta2-only status in CABPT

Date: 2026-09-16. Status: approved design, awaiting implementation plan.

Follows the Talos 1.14 and CAPI 1.14 bumps on the `talos-1.14` branch. Companion
documents: the CACPPT cleanup in `cluster-api-control-plane-provider-talos`
(`docs/design/2026-09-16-capi-v1beta2-cleanup.md`) and the talos2disk change in
`sidero-community/actions`
(`docs/superpowers/specs/2026-09-16-talos2disk-unattended-install-design.md`).

## Goal

Three things, reviewed together because they touch the same code:

1. Render the installer image CABPT resolves from `spec.imageFactory` as an
   `UnattendedInstallConfig` document wherever the target Talos reads one, instead
   of the deprecated `machine.install.image` field.
2. Drive in-place Talos upgrades through the lifecycle service Talos 1.14
   deprecates the machine service upgrade in favour of.
3. Stop carrying the Cluster API v1beta1-contract compatibility block
   (`status.deprecated.v1beta1`) and the other deprecated CAPI and
   controller-runtime APIs.

## Decisions

| Question | Decision |
|---|---|
| Talos versions that still get `machine.install.image` | Contracts below 1.14 keep the legacy strategic patch. The document does not exist there. Dropping the patch entirely would drop Talos 1.10 to 1.13 from the support table, which is out of scope. |
| Disk selector in the rendered document | `system_disk`. Validation requires a selector; CABPT does not know the install disk; `system_disk` is a no-op on an installed node and the correct target for an in-place upgrade. Users override it with a strategic patch on the document. |
| Where the document is applied | As CABPT's pre-patch, before user strategic patches, so a user patch still wins on both image and selector. Same ordering as the legacy patch today. |
| Nodes without the lifecycle service (Talos before 1.13.0-alpha.2) | Keep the machine service upgrade as a fallback, chosen from the version the node reports. It stays the only remaining use of a deprecated Talos client call. |
| How the hook follows a lifecycle upgrade | An in-memory per-machine tracker in the hook handler drives pull, install stream and reboot in the background; hook calls report progress and never block on the stream. |
| `status.deprecated.v1beta1` | Removed from the v1beta1 API now, ahead of CAPI's own removal. Nothing in the fleet reads it; CAPI core v1.12+ reads only v1beta2 fields. |
| v1alpha3 conditions | v1alpha3 keeps its schema but stops importing CAPI's deprecated v1beta1 package: local copies of the condition types, derived from the v1beta2 conditions on conversion. |

## 1. Installer image rendering

`installImagePatch` becomes contract-aware. It takes the version contract the
configuration was generated for and the resolved image:

- Contract with `UnattendedInstallConfig()` true (1.14 and later, and an unset
  `talosVersion`, which means the current Talos): a multi-document patch

  ```yaml
  apiVersion: v1alpha1
  kind: UnattendedInstallConfig
  installer:
    image: <resolved image>
  provisioning:
    diskSelector:
      match: system_disk
  ```

  The generator never emits this document itself because CABPT passes no install
  disk, so the patch creates it rather than merging into one. `wipe` and `reboot`
  stay unset (Talos defaults).
- Older contracts: the existing `machine.install.image` strategic patch.

The gate is machinery's own `VersionContract.UnattendedInstallConfig()`, not a
semver comparison, so it tracks whatever Talos considers the document's floor.
`genConfigs` already parses the contract; it is threaded to the patch builder.
The user-supplied path (`generateType: none`) is unchanged: CABPT does not add an
install image to configurations it did not generate.

In-place updates read the desired image with the same precedence Talos uses:
`UnattendedInstallConfig().InstallerImage()` when a document is present, else
`Machine().Install().Image()`. `InPlaceConfigHash` is unchanged; the installer
image was already a discrete input.

Documentation: the `spec.imageFactory` section of the README, the in-place
updates guide and the 2026-09-12 Image Factory design note now describe the
document form, with the legacy field called out as the pre-1.14 rendering.

## 2. Lifecycle upgrades

Talos 1.14's `talosctl upgrade` does three things the legacy machine service
did on the node: pull the installer image into the system containerd, stream the
install until it reports an exit code, then reboot. The lifecycle service is
served from Talos 1.13.0-alpha.2 on.

### Node client

`NodeClient.Upgrade(ctx, image)` becomes a blocking call that performs the
whole sequence and returns when the reboot has been requested:

1. `ImageClient.Pull` with `common.ContainerdInstance{Driver: CONTAINERD}` and
   the image reference, drained to EOF.
2. `LifecycleClient.Upgrade` with the same containerd instance and
   `InstallArtifactsSource{ImageName: image}`, consumed until a progress message
   carries an exit code. Non-zero is an error carrying the last progress message.
3. `Reboot` with Talos' default mode. Talos cordons and drains the node itself as
   part of its reboot sequence, as it did inside the legacy upgrade.

`NodeClient.LegacyUpgrade(ctx, image)` keeps today's fire-and-forget machine
service call for nodes that predate the lifecycle service.

### Tracker

The hook is called repeatedly and must answer quickly, so the blocking upgrade
runs off the request path. The handler keeps `upgrades map[machine key]*upgrade`
under a mutex, where an entry records the image, whether the sequence finished,
and its error if any.

`DoUpdateMachine`, after applying the configuration and finding
`needsUpgrade(desiredImage, runningVersion)`:

- Running version below 1.13.0-alpha.2: call `LegacyUpgrade` and return
  "retry in 30 s", exactly as today.
- No entry: open a node client of its own (the request-scoped one is closed when
  the call returns), start a goroutine bounded by a handler-level context and an
  upgrade timeout (30 minutes), record the entry, return "retry in 30 s".
- Entry running: return "retry in 30 s" with the image in the message.
- Entry failed: respond with a retryable failure carrying the error, drop the
  entry. The next call starts over; pull and install are idempotent.
- Entry finished: the node is rebooting. The existing code path handles it: an
  unreachable API or a still-old version returns "retry in 30 s"; the new
  version means `needsUpgrade` is false, the entry is dropped and the update
  completes. A finished entry whose node still reports the old version after the
  upgrade timeout is dropped so the sequence restarts.

A controller restart loses the map. The next call sees the old version and
starts a new sequence, which is safe for the same reason a failed entry is.

### What does not change

The desired image still comes from the rendered configuration, `needsUpgrade`
still compares the image tag to the running version, and MachinePool members
are still never upgraded by CABPT.

## 3. v1beta2-only status

### v1beta1 API

`TalosConfigStatus.Deprecated`, `TalosConfigDeprecatedStatus` and
`TalosConfigV1Beta1DeprecatedStatus` are removed, with the v1beta1-style
condition and reason constants and every `v1beta1conditions.Mark*` call in the
controller. `status.conditions` (`metav1.Condition`) and `status.initialization`
are the only status the controller maintains. CRDs are regenerated.

### v1alpha3 API

v1alpha3 keeps its schema, including `status.conditions` in the old shape, but
defines the `Condition`, `Conditions`, `ConditionType` and `ConditionSeverity`
types locally instead of importing CAPI's deprecated v1beta1 package. Conversion:

- hub to v1alpha3: each `metav1.Condition` becomes a v1alpha3 condition with the
  same type, status, reason, message and transition time and no severity;
  `ready` is `initialization.dataSecretCreated`; `failureReason` and
  `failureMessage` are empty, as CABPT never set them.
- v1alpha3 to hub: v1alpha3 conditions are not turned into v1beta2 conditions
  (unchanged); the hub status is restored from the conversion annotation.

Round-trip tests are adjusted for the fields that are now derived.

### Scheme registration

Both API packages replace controller-runtime's deprecated `scheme.Builder` with
`runtime.NewSchemeBuilder`, keeping the exported `AddToScheme` and the
`localSchemeBuilder` the generated conversions register with.

## Consequences

- talos2disk must read the installer image from the document for 1.14-shaped
  configurations. It does not write into the configuration, so the document does
  not conflict with anything it does. See the companion spec.
- Configurations for Talos 1.10 to 1.13 render exactly as before.
- The v1beta1 CRD status loses the `deprecated` sub-block. CAPI core, clusterctl
  and the other providers in the fleet read only v1beta2 fields.
- The one remaining deprecated Talos call, the legacy upgrade, is reachable only
  for nodes older than 1.13.

## Testing

- Rendering: a 1.14 contract produces the document with image and selector and
  no `machine.install`; a 1.13 contract produces the legacy field; a user
  strategic patch on the document overrides the selector; `generateType: none`
  data is untouched.
- In-place reader: the installer image is read from either form.
- Lifecycle: fakes for the image, lifecycle and machine gRPC clients verify the
  pull, stream and reboot sequence and that a non-zero exit code fails with the
  last message; handler tests with a fake node client cover starting, waiting,
  failing and finishing an upgrade, and the legacy path for a 1.12 node.
- Status: conversion tests for the derived v1alpha3 fields; the controller
  suite asserts only on v1beta2 conditions.

## Out of scope

- An API field for the install disk selector. A strategic patch covers it.
- `wipe` and `reboot` options on the rendered document.
- Dropping Talos versions before 1.14 or 1.13.
- Draining nodes from the hook; Talos drains on reboot.
