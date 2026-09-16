# UnattendedInstall rendering, lifecycle upgrades and v1beta2-only status: implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Render the installer image as an `UnattendedInstallConfig` document on Talos 1.14 contracts, drive in-place upgrades through the Talos lifecycle service with a per-machine tracker, and remove the CAPI v1beta1 compatibility block and other deprecated CAPI and controller-runtime APIs from CABPT.

**Architecture:** The installer image patch becomes contract-aware and the in-place reader accepts both forms. A new `lifecycleUpgrade` sequence (pull, stream, reboot) runs in the background under an `upgradeTracker` owned by the hook handler, with the machine service upgrade kept for pre-1.13 nodes. The v1beta1 API drops `status.deprecated`; v1alpha3 gets local condition types derived from v1beta2 conditions on conversion.

**Tech Stack:** Go 1.26.5, Talos machinery v1.14.0, Cluster API v1.14.2 (`sigs.k8s.io/cluster-api/api` module), controller-runtime v0.24.1, controller-gen v0.21.0, conversion-gen v0.36.0, testify.

**Spec:** `docs/design/2026-09-16-unattended-install-lifecycle-upgrades.md`

## Global Constraints

- Branch `talos-1.14`; every new `.go` file starts with the MPL header used by every other file in the repo.
- Commit messages: conventional type, imperative lower-case header under 89 characters, a body, no sign-off trailer (matches the branch's history).
- Contracts below 1.14 keep the legacy `machine.install.image` patch; nodes below Talos 1.13.0-alpha.2 keep the machine service upgrade.
- Rendered document selector is exactly `system_disk`; the document is applied before user strategic patches.
- Run tests with `GOTOOLCHAIN=auto go test ./api/... ./internal/... ./controllers/... -count=1` (the integration package needs a cluster and is excluded).
- Regenerate with: `go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.21.0 object:headerFile=./hack/boilerplate.go.txt paths="./..."`, `go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.21.0 crd:crdVersions=v1 paths="./api/..." output:crd:dir=config/crd/bases output:webhook:dir=config/webhook webhook`, and `go run k8s.io/code-generator/cmd/conversion-gen@v0.36.0 --output-file=zz_generated.conversion.go --go-header-file=./hack/boilerplate.go.txt ./api/v1alpha3`.

---

## File structure

| File | Responsibility |
|---|---|
| `controllers/installer_image.go` | Build the installer image patch in the form the contract reads (modify) |
| `controllers/talosconfig_controller.go` | `versionContractFor`, thread the contract into the patch, drop v1beta1 conditions (modify) |
| `controllers/generation_test.go` | Rendering tests (modify) |
| `controllers/installer_image_test.go` | Unit tests for the patch builder (create) |
| `internal/inplace/updatemachine.go` | Document-first install image read; tracker-driven upgrade step (modify) |
| `internal/inplace/lifecycle.go` | `lifecycleUpgrade`: pull, install stream, reboot over the gRPC clients (create) |
| `internal/inplace/lifecycle_test.go` | Fake gRPC clients and sequence tests (create) |
| `internal/inplace/upgrades.go` | `upgradeTracker` (create) |
| `internal/inplace/upgrades_test.go` | Tracker tests (create) |
| `internal/inplace/talos.go` | `NodeClient.Upgrade` blocking lifecycle sequence, `LegacyUpgrade` (modify) |
| `internal/inplace/server.go` | Handler owns a tracker (modify) |
| `internal/inplace/updatemachine_test.go` | Fake node with both upgrade paths; handler tests (modify) |
| `api/v1alpha3/condition_types.go` | Local `Condition` types replacing the CAPI v1beta1 import (create) |
| `api/v1alpha3/conditions.go`, `api/v1alpha3/v1beta2_conditions.go`, `api/v1alpha3/talosconfig_types.go` | Use the local types and CAPI v1beta2 constants (modify) |
| `api/v1alpha3/conversion.go`, `api/v1alpha3/conversion_test.go` | Condition element conversions; derived legacy fields (modify) |
| `api/v1beta1/talosconfig_types.go` | Remove `Deprecated` (modify); `api/v1beta1/v1beta1_conditions.go` (delete) |
| `api/v1alpha3/groupversion_info.go`, `api/v1beta1/groupversion_info.go` | `runtime.NewSchemeBuilder` (modify) |
| `config/crd/bases/*.yaml`, `api/**/zz_generated.*.go` | Regenerated |
| `README.md`, `docs/in-place-updates.md`, `docs/design/2026-09-12-image-factory-schematic.md`, `hack/release.toml` | Docs (modify) |

---

### Task 1: Contract-aware installer image patch

**Files:**
- Modify: `controllers/installer_image.go`
- Modify: `controllers/talosconfig_controller.go` (the `installImagePatch(installerImage)` call in `reconcileGenerate`, and the contract block in `genConfigs`)
- Create: `controllers/installer_image_test.go`
- Modify: `controllers/generation_test.go`

**Interfaces:**
- Produces: `func installImagePatch(contract *config.VersionContract, image string) string` and `func versionContractFor(talosVersion string) (*config.VersionContract, error)` in package `controllers`.

- [ ] **Step 1: Write the failing reconcile-level tests**

Append to `controllers/generation_test.go` (it already imports `configloader`, `assert`, `require`, `bootstrapv1beta1`):

```go
// TestReconcileRendersTheInstallerImageAsADocument: on a 1.14 contract the resolved image lands
// in an UnattendedInstallConfig document with the system_disk selector, and no legacy block.
func TestReconcileRendersTheInstallerImageAsADocument(t *testing.T) {
	t.Parallel()

	f := newImageFactoryFixture(t, specWith("v1.14", "siderolabs/nvme-cli"), newFakeFactory())

	require.NoError(t, f.reconcile(t))

	provider, err := configloader.NewFromBytes([]byte(f.bootstrapData(t)))
	require.NoError(t, err)

	image := f.config(t).Status.ImageFactory.InstallerImage
	require.NotEmpty(t, image)
	require.NotNil(t, provider.UnattendedInstallConfig(), "a 1.14 contract must carry the document")
	assert.Equal(t, image, provider.UnattendedInstallConfig().InstallerImage())
	assert.Equal(t, "system_disk", provider.UnattendedInstallConfig().VolumeSelector().String())
	assert.Empty(t, provider.Machine().Install().Image(), "no legacy field alongside the document")
}

// TestReconcileLetsAStrategicPatchOverrideTheDiskSelector: CABPT's document is a default, so a
// user's patch on the same document wins.
func TestReconcileLetsAStrategicPatchOverrideTheDiskSelector(t *testing.T) {
	t.Parallel()

	spec := specWith("v1.14", "siderolabs/nvme-cli")
	spec.StrategicPatches = []string{"apiVersion: v1alpha1\nkind: UnattendedInstallConfig\nprovisioning:\n  diskSelector:\n    match: disk.dev_path == \"/dev/nvme0n1\"\n"}

	f := newImageFactoryFixture(t, spec, newFakeFactory())

	require.NoError(t, f.reconcile(t))

	provider, err := configloader.NewFromBytes([]byte(f.bootstrapData(t)))
	require.NoError(t, err)
	require.NotNil(t, provider.UnattendedInstallConfig())
	assert.Equal(t, `disk.dev_path == "/dev/nvme0n1"`, provider.UnattendedInstallConfig().VolumeSelector().String())
	assert.Equal(t, f.config(t).Status.ImageFactory.InstallerImage, provider.UnattendedInstallConfig().InstallerImage(), "the image survives the user's patch")
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `GOTOOLCHAIN=auto go test ./controllers/ -run 'TestReconcileRendersTheInstallerImageAsADocument|TestReconcileLetsAStrategicPatchOverrideTheDiskSelector' -count=1`
Expected: both FAIL at `require.NotNil(... UnattendedInstallConfig())`: the legacy patch renders no document.

- [ ] **Step 3: Implement the builder and the contract helper**

Replace the body of `controllers/installer_image.go` below the package clause with:

```go
import (
	"fmt"

	"github.com/siderolabs/talos/pkg/machinery/config"
)

// unattendedInstallPatch is the document form of the installer image. The generator emits no
// install document when CABPT passes no install disk, so this patch creates it. Validation
// requires a disk selector: system_disk names the disk a running node booted from, which is a
// no-op on an installed node and the right target for an in-place upgrade. Users override it
// with a strategic patch of their own on the document.
const unattendedInstallPatch = `apiVersion: v1alpha1
kind: UnattendedInstallConfig
installer:
  image: %s
provisioning:
  diskSelector:
    match: system_disk
`

// legacyInstallPatch is the machine.install.image form for Talos versions that predate the
// UnattendedInstallConfig document.
const legacyInstallPatch = "machine:\n  install:\n    image: %s\n"

// installImagePatch renders the installer image resolveInstallerImage derived from
// spec.imageFactory as a strategic merge patch in the form the target Talos reads: the
// UnattendedInstallConfig document for contracts that know it (1.14 and an unset talosVersion),
// the legacy machine.install.image field before that.
//
// It is applied ahead of any user-supplied strategic patches so that an explicit patch in the
// TalosConfig still wins. The resolved image is a good default, not an override of intent.
func installImagePatch(contract *config.VersionContract, image string) string {
	if contract.UnattendedInstallConfig() {
		return fmt.Sprintf(unattendedInstallPatch, image)
	}

	return fmt.Sprintf(legacyInstallPatch, image)
}
```

In `controllers/talosconfig_controller.go` add next to `genTalosConfigFile`:

```go
// versionContractFor parses spec.talosVersion into the contract configuration is generated for.
// Unset means the current Talos, which machinery represents as a nil contract.
func versionContractFor(talosVersion string) (*config.VersionContract, error) {
	if talosVersion == "" {
		return defaultVersionContract, nil
	}

	contract, err := config.ParseContractFromVersion(talosVersion)
	if err != nil {
		return nil, fmt.Errorf("invalid talos-version: %w", err)
	}

	return contract, nil
}
```

In `genConfigs` replace

```go
	versionContract := defaultVersionContract

	if scope.Config.Spec.TalosVersion != "" {
		var err error
		versionContract, err = config.ParseContractFromVersion(scope.Config.Spec.TalosVersion)
		if err != nil {
			return retBundle, patches, fmt.Errorf("invalid talos-version: %w", err)
		}
	}
```

with

```go
	versionContract, err := versionContractFor(scope.Config.Spec.TalosVersion)
	if err != nil {
		return retBundle, patches, err
	}
```

In `reconcileGenerate` replace

```go
	var resolvedPatches []string
	if installerImage != "" {
		resolvedPatches = append(resolvedPatches, installImagePatch(installerImage))
	}
```

with

```go
	var resolvedPatches []string
	if installerImage != "" {
		contract, err := versionContractFor(config.Spec.TalosVersion)
		if err != nil {
			return err
		}

		resolvedPatches = append(resolvedPatches, installImagePatch(contract, installerImage))
	}
```

- [ ] **Step 4: Run to verify they pass**

Run: `GOTOOLCHAIN=auto go test ./controllers/ -run 'TestReconcileRendersTheInstallerImageAsADocument|TestReconcileLetsAStrategicPatchOverrideTheDiskSelector|TestReconcileRendersTheResolvedInstallerImage' -count=1 -v`
Expected: PASS for all three (the pre-existing one still finds `image: factory...` inside the document).

- [ ] **Step 5: Pin the contract gate with unit tests**

The fixture can only drive a contract the fake Factory serves, so the unset contract and the pre-1.14 form are pinned directly. Create `controllers/installer_image_test.go`:

```go
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"testing"

	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/configloader"
	"github.com/siderolabs/talos/pkg/machinery/config/configpatcher"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testInstaller = "factory.talos.dev/metal-installer/abc:v1.14.0"

// minimalWorker is enough v1alpha1 for the strategic patcher to merge into.
const minimalWorker = "version: v1alpha1\nmachine:\n  type: worker\n  token: a.b\ncluster:\n  controlPlane:\n    endpoint: https://1.2.3.4:6443\n"

func applyInstallImagePatch(t *testing.T, contract *config.VersionContract) config.Provider {
	t.Helper()

	patch, err := configpatcher.LoadPatch([]byte(installImagePatch(contract, testInstaller)))
	require.NoError(t, err)

	out, err := configpatcher.Apply(configpatcher.WithBytes([]byte(minimalWorker)), []configpatcher.Patch{patch})
	require.NoError(t, err)

	data, err := out.Bytes()
	require.NoError(t, err)

	provider, err := configloader.NewFromBytes(data)
	require.NoError(t, err)

	return provider
}

func TestInstallImagePatchRendersTheDocumentFrom114(t *testing.T) {
	t.Parallel()

	for name, contract := range map[string]*config.VersionContract{
		"v1.14":   config.TalosVersion1_14,
		"current": config.TalosVersionCurrent,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			provider := applyInstallImagePatch(t, contract)

			require.NotNil(t, provider.UnattendedInstallConfig())
			assert.Equal(t, testInstaller, provider.UnattendedInstallConfig().InstallerImage())
			assert.Equal(t, "system_disk", provider.UnattendedInstallConfig().VolumeSelector().String())
			assert.Empty(t, provider.Machine().Install().Image(), "no legacy field alongside the document")
		})
	}
}

func TestInstallImagePatchRendersTheLegacyFieldBefore114(t *testing.T) {
	t.Parallel()

	provider := applyInstallImagePatch(t, config.TalosVersion1_13)

	assert.Nil(t, provider.UnattendedInstallConfig())
	assert.Equal(t, testInstaller, provider.Machine().Install().Image())
}

func TestVersionContractFor(t *testing.T) {
	t.Parallel()

	contract, err := versionContractFor("")
	require.NoError(t, err)
	assert.Nil(t, contract, "unset means the current Talos")

	contract, err = versionContractFor("v1.13")
	require.NoError(t, err)
	assert.Equal(t, config.TalosVersion1_13, contract)

	_, err = versionContractFor("nonsense")
	assert.Error(t, err)
}
```

- [ ] **Step 6: Prove the gate tests can fail, then run them**

Temporarily change `if contract.UnattendedInstallConfig() {` to `if true {` in `installImagePatch`, run `GOTOOLCHAIN=auto go test ./controllers/ -run TestInstallImagePatchRendersTheLegacyFieldBefore114 -count=1` and confirm it FAILS; restore the line, then run `GOTOOLCHAIN=auto go test ./controllers/ -run 'TestInstallImagePatch|TestVersionContractFor' -count=1 -v` and confirm PASS.

- [ ] **Step 7: Run the whole controllers package**

Run: `GOTOOLCHAIN=auto go test ./controllers/ -count=1`
Expected: `ok`.

- [ ] **Step 8: Commit**

```bash
git add controllers/installer_image.go controllers/installer_image_test.go controllers/talosconfig_controller.go controllers/generation_test.go
git commit -m "feat: render the installer image as an UnattendedInstallConfig document

Contracts that know the document (1.14 and an unset talosVersion) get an
UnattendedInstallConfig carrying the resolved image and a system_disk
selector; older contracts keep machine.install.image. The document is
still applied ahead of user strategic patches, so a patch on it wins."
```

---

### Task 2: In-place reader accepts the document

**Files:**
- Modify: `internal/inplace/updatemachine.go` (`installImage`)
- Modify: `internal/inplace/updatemachine_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/inplace/updatemachine_test.go`:

```go
// machineConfigWithDocument renders a configuration whose installer image lives only in an
// UnattendedInstallConfig document, the 1.14 shape.
func machineConfigWithDocument(image string) string {
	return fmt.Sprintf(`version: v1alpha1
debug: false
machine:
  type: worker
  token: aaaaaa.bbbbbbbbbbbbbbbb
  ca:
    crt: ""
    key: ""
cluster:
  id: dGVzdA==
  secret: dGVzdA==
  controlPlane:
    endpoint: https://192.168.1.1:6443
  token: aaaaaa.bbbbbbbbbbbbbbbb
  ca:
    crt: ""
    key: ""
---
apiVersion: v1alpha1
kind: UnattendedInstallConfig
installer:
  image: %s
provisioning:
  diskSelector:
    match: system_disk
`, image)
}

func TestInstallImageReadsTheUnattendedInstallDocument(t *testing.T) {
	t.Parallel()

	image, err := installImage([]byte(machineConfigWithDocument("factory.talos.dev/metal-installer/abc:v1.14.0")))
	require.NoError(t, err)
	assert.Equal(t, "factory.talos.dev/metal-installer/abc:v1.14.0", image)
}

func TestInstallImageReadsTheLegacyField(t *testing.T) {
	t.Parallel()

	image, err := installImage([]byte(machineConfigWithImage("ghcr.io/siderolabs/installer:v1.13.0")))
	require.NoError(t, err)
	assert.Equal(t, "ghcr.io/siderolabs/installer:v1.13.0", image)
}
```

- [ ] **Step 2: Run to verify the document case fails**

Run: `GOTOOLCHAIN=auto go test ./internal/inplace/ -run 'TestInstallImageReads' -count=1 -v`
Expected: the document test FAILS with `expected: "factory..." actual: ""`; the legacy test passes.

- [ ] **Step 3: Implement**

Replace `installImage` in `internal/inplace/updatemachine.go`:

```go
// installImage reads the installer image out of a rendered Talos machine configuration, with
// the precedence Talos applies: an UnattendedInstallConfig document first, the legacy
// machine.install.image field otherwise.
func installImage(data []byte) (string, error) {
	provider, err := configloader.NewFromBytes(data)
	if err != nil {
		return "", fmt.Errorf("failed to parse machine configuration: %w", err)
	}

	if doc := provider.UnattendedInstallConfig(); doc != nil {
		return doc.InstallerImage(), nil
	}

	if provider.Machine() == nil || provider.Machine().Install() == nil {
		return "", nil
	}

	return provider.Machine().Install().Image(), nil
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `GOTOOLCHAIN=auto go test ./internal/inplace/ -count=1`
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
git add internal/inplace/updatemachine.go internal/inplace/updatemachine_test.go
git commit -m "feat(inplace): read the desired installer image from the install document

Talos gives an UnattendedInstallConfig document precedence over
machine.install.image, and CABPT renders the document from 1.14 on."
```

---

### Task 3: Lifecycle upgrade sequence

**Files:**
- Create: `internal/inplace/lifecycle.go`
- Create: `internal/inplace/lifecycle_test.go`

**Interfaces:**
- Produces: `type upgradeClients struct{ images machineapi.ImageServiceClient; lifecycle machineapi.LifecycleServiceClient; machine machineapi.MachineServiceClient }` and `func lifecycleUpgrade(ctx context.Context, c upgradeClients, image string) error`.

- [ ] **Step 1: Write the failing tests**

Create `internal/inplace/lifecycle_test.go`:

```go
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package inplace

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/siderolabs/talos/pkg/machinery/api/common"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// fakeStream serves canned items then EOF (or err), standing in for a server-streaming client.
type fakeStream[T any] struct {
	grpc.ClientStream

	items []*T
	err   error
}

func (s *fakeStream[T]) Recv() (*T, error) {
	if len(s.items) == 0 {
		if s.err != nil {
			return nil, s.err
		}

		return nil, io.EOF
	}

	item := s.items[0]
	s.items = s.items[1:]

	return item, nil
}

type fakeImages struct {
	machineapi.ImageServiceClient

	pulled []string
	err    error
}

func (f *fakeImages) Pull(_ context.Context, in *machineapi.ImageServicePullRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[machineapi.ImageServicePullResponse], error) {
	f.pulled = append(f.pulled, in.ImageRef)

	if f.err != nil {
		return nil, f.err
	}

	return &fakeStream[machineapi.ImageServicePullResponse]{}, nil
}

type fakeLifecycle struct {
	machineapi.LifecycleServiceClient

	request  *machineapi.LifecycleServiceUpgradeRequest
	progress []*machineapi.LifecycleServiceUpgradeResponse
}

func (f *fakeLifecycle) Upgrade(_ context.Context, in *machineapi.LifecycleServiceUpgradeRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[machineapi.LifecycleServiceUpgradeResponse], error) {
	f.request = in

	return &fakeStream[machineapi.LifecycleServiceUpgradeResponse]{items: f.progress}, nil
}

type fakeMachine struct {
	machineapi.MachineServiceClient

	reboots []*machineapi.RebootRequest
}

func (f *fakeMachine) Reboot(_ context.Context, in *machineapi.RebootRequest, _ ...grpc.CallOption) (*machineapi.RebootResponse, error) {
	f.reboots = append(f.reboots, in)

	return &machineapi.RebootResponse{}, nil
}

func progressMessage(msg string) *machineapi.LifecycleServiceUpgradeResponse {
	return &machineapi.LifecycleServiceUpgradeResponse{Progress: &machineapi.LifecycleServiceInstallProgress{
		Response: &machineapi.LifecycleServiceInstallProgress_Message{Message: msg},
	}}
}

func progressExit(code int32) *machineapi.LifecycleServiceUpgradeResponse {
	return &machineapi.LifecycleServiceUpgradeResponse{Progress: &machineapi.LifecycleServiceInstallProgress{
		Response: &machineapi.LifecycleServiceInstallProgress_ExitCode{ExitCode: code},
	}}
}

const lifecycleImage = "factory.talos.dev/metal-installer/abc:v1.14.0"

func TestLifecycleUpgradePullsInstallsAndReboots(t *testing.T) {
	t.Parallel()

	images := &fakeImages{}
	lifecycle := &fakeLifecycle{progress: []*machineapi.LifecycleServiceUpgradeResponse{progressMessage("unpacking"), progressExit(0)}}
	machine := &fakeMachine{}

	require.NoError(t, lifecycleUpgrade(context.Background(), upgradeClients{images: images, lifecycle: lifecycle, machine: machine}, lifecycleImage))

	assert.Equal(t, []string{lifecycleImage}, images.pulled)
	require.NotNil(t, lifecycle.request)
	assert.Equal(t, lifecycleImage, lifecycle.request.Source.ImageName)
	assert.Equal(t, common.ContainerDriver_CONTAINERD, lifecycle.request.Containerd.Driver)
	assert.Equal(t, common.ContainerdNamespace_NS_SYSTEM, lifecycle.request.Containerd.Namespace)
	require.Len(t, machine.reboots, 1)
	assert.Equal(t, machineapi.RebootRequest_DEFAULT, machine.reboots[0].Mode)
}

func TestLifecycleUpgradeFailsOnANonZeroExitCode(t *testing.T) {
	t.Parallel()

	lifecycle := &fakeLifecycle{progress: []*machineapi.LifecycleServiceUpgradeResponse{progressMessage("verifying image"), progressExit(2)}}
	machine := &fakeMachine{}

	err := lifecycleUpgrade(context.Background(), upgradeClients{images: &fakeImages{}, lifecycle: lifecycle, machine: machine}, lifecycleImage)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "exited with code 2")
	assert.Contains(t, err.Error(), "verifying image", "the last progress message tells the operator what failed")
	assert.Empty(t, machine.reboots, "a failed install must not reboot")
}

func TestLifecycleUpgradeFailsWhenTheStreamEndsWithoutAnExitCode(t *testing.T) {
	t.Parallel()

	lifecycle := &fakeLifecycle{progress: []*machineapi.LifecycleServiceUpgradeResponse{progressMessage("unpacking")}}
	machine := &fakeMachine{}

	err := lifecycleUpgrade(context.Background(), upgradeClients{images: &fakeImages{}, lifecycle: lifecycle, machine: machine}, lifecycleImage)

	require.Error(t, err)
	assert.Empty(t, machine.reboots)
}

func TestLifecycleUpgradeStopsWhenThePullFails(t *testing.T) {
	t.Parallel()

	lifecycle := &fakeLifecycle{}

	err := lifecycleUpgrade(context.Background(), upgradeClients{images: &fakeImages{err: errors.New("registry unreachable")}, lifecycle: lifecycle, machine: &fakeMachine{}}, lifecycleImage)

	require.ErrorContains(t, err, "registry unreachable")
	assert.Nil(t, lifecycle.request, "no install without the image")
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `GOTOOLCHAIN=auto go test ./internal/inplace/ -run TestLifecycleUpgrade -count=1`
Expected: build failure, `undefined: upgradeClients` and `undefined: lifecycleUpgrade`.

- [ ] **Step 3: Implement the sequence**

Create `internal/inplace/lifecycle.go`:

```go
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package inplace

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/siderolabs/talos/pkg/machinery/api/common"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	"google.golang.org/grpc"
)

// upgradeClients is the slice of the Talos API a lifecycle upgrade drives.
type upgradeClients struct {
	images    machineapi.ImageServiceClient
	lifecycle machineapi.LifecycleServiceClient
	machine   machineapi.MachineServiceClient
}

// systemContainerd is where installer images live. talosctl upgrade targets the same instance.
var systemContainerd = &common.ContainerdInstance{
	Driver:    common.ContainerDriver_CONTAINERD,
	Namespace: common.ContainerdNamespace_NS_SYSTEM,
}

// lifecycleUpgrade upgrades a node the way talosctl 1.14 does: pull the installer image into the
// system containerd, stream the install until it reports an exit code, then reboot. Talos cordons
// and drains the node itself as part of its reboot sequence. It returns once the reboot has been
// requested, which can be many minutes after it was called.
func lifecycleUpgrade(ctx context.Context, c upgradeClients, image string) error {
	pull, err := c.images.Pull(ctx, &machineapi.ImageServicePullRequest{Containerd: systemContainerd, ImageRef: image})
	if err != nil {
		return fmt.Errorf("failed to start pulling %q: %w", image, err)
	}

	if err := drain(pull); err != nil {
		return fmt.Errorf("failed to pull %q: %w", image, err)
	}

	install, err := c.lifecycle.Upgrade(ctx, &machineapi.LifecycleServiceUpgradeRequest{
		Containerd: systemContainerd,
		Source:     &machineapi.InstallArtifactsSource{ImageName: image},
	})
	if err != nil {
		return fmt.Errorf("failed to start the upgrade to %q: %w", image, err)
	}

	if err := waitForInstall(install, image); err != nil {
		return err
	}

	if _, err := c.machine.Reboot(ctx, &machineapi.RebootRequest{Mode: machineapi.RebootRequest_DEFAULT}); err != nil {
		return fmt.Errorf("failed to reboot into %q: %w", image, err)
	}

	return nil
}

// waitForInstall consumes the install stream until it reports an exit code.
func waitForInstall(stream grpc.ServerStreamingClient[machineapi.LifecycleServiceUpgradeResponse], image string) error {
	var last string

	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("the upgrade to %q ended without an exit code; last message: %q", image, last)
		}

		if err != nil {
			return fmt.Errorf("the upgrade to %q failed: %w", image, err)
		}

		switch progress := resp.GetProgress().GetResponse().(type) {
		case *machineapi.LifecycleServiceInstallProgress_Message:
			last = progress.Message
		case *machineapi.LifecycleServiceInstallProgress_ExitCode:
			if progress.ExitCode != 0 {
				return fmt.Errorf("the upgrade to %q exited with code %d: %s", image, progress.ExitCode, last)
			}

			return nil
		}
	}
}

// drain reads a pull stream to its end; progress is not interesting, errors are.
func drain(stream grpc.ServerStreamingClient[machineapi.ImageServicePullResponse]) error {
	for {
		if _, err := stream.Recv(); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return err
		}
	}
}
```

- [ ] **Step 4: Run to verify they pass**

Run: `GOTOOLCHAIN=auto go test ./internal/inplace/ -run TestLifecycleUpgrade -count=1 -v`
Expected: PASS for all four.

- [ ] **Step 5: Commit**

```bash
git add internal/inplace/lifecycle.go internal/inplace/lifecycle_test.go
git commit -m "feat(inplace): drive a Talos upgrade through the lifecycle service

Pull the installer image into the system containerd, stream the install
until it reports an exit code and reboot, the sequence talosctl 1.14 runs
now that the machine service upgrade is deprecated."
```

---

### Task 4: Node client with both upgrade paths

**Files:**
- Modify: `internal/inplace/talos.go`
- Modify: `internal/inplace/updatemachine_test.go` (`fakeNode`)

**Interfaces:**
- Produces: `NodeClient.Upgrade(ctx, image string) error` now blocks for the lifecycle sequence; new `NodeClient.LegacyUpgrade(ctx, image string) error`.

- [ ] **Step 1: Write the failing wiring test**

Append to `internal/inplace/lifecycle_test.go`:

```go
func TestNodeClientUpgradeRunsTheLifecycleSequence(t *testing.T) {
	t.Parallel()

	machine := &fakeMachine{}
	node := &talosNodeClient{upgrade: upgradeClients{
		images:    &fakeImages{},
		lifecycle: &fakeLifecycle{progress: []*machineapi.LifecycleServiceUpgradeResponse{progressExit(0)}},
		machine:   machine,
	}}

	require.NoError(t, node.Upgrade(context.Background(), lifecycleImage))
	assert.Len(t, machine.reboots, 1)
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `GOTOOLCHAIN=auto go test ./internal/inplace/ -run TestNodeClientUpgradeRunsTheLifecycleSequence -count=1`
Expected: build failure, `unknown field upgrade in struct literal of type talosNodeClient`.

- [ ] **Step 3: Implement**

In `internal/inplace/talos.go` change the interface, struct, constructor and upgrade methods:

```go
	// Upgrade drives a lifecycle upgrade to image: pull, install and reboot. It blocks for as
	// long as the install takes and returns once the reboot has been requested. Talos cordons
	// and drains the node itself as part of the reboot.
	Upgrade(ctx context.Context, image string) error

	// LegacyUpgrade asks a node that predates the lifecycle service (Talos before 1.13) to
	// upgrade itself through the machine service, and returns as soon as it has been asked.
	LegacyUpgrade(ctx context.Context, image string) error
```

```go
// talosNodeClient is the gRPC-backed NodeClient.
type talosNodeClient struct {
	client  *talosclient.Client
	upgrade upgradeClients
}
```

In `NewNodeClient` return:

```go
		return &talosNodeClient{
			client:  tc,
			upgrade: upgradeClients{images: tc.ImageClient, lifecycle: tc.LifecycleClient, machine: tc.MachineClient},
		}, nil
```

Replace the `Upgrade` method with:

```go
func (t *talosNodeClient) Upgrade(ctx context.Context, image string) error {
	return lifecycleUpgrade(ctx, t.upgrade, image)
}

func (t *talosNodeClient) LegacyUpgrade(ctx context.Context, image string) error {
	// Nodes before Talos 1.13 serve only the machine service upgrade, which Talos 1.14
	// deprecates; it is kept for them alone. The node performs the whole sequence itself.
	_, err := t.client.Upgrade(ctx, image, false, false) //nolint:staticcheck // pre-1.13 nodes have nothing else
	if err != nil {
		return fmt.Errorf("failed to upgrade Talos to %q: %w", image, err)
	}

	return nil
}
```

Update `fakeNode` in `internal/inplace/updatemachine_test.go`:

```go
// fakeNode records what the update state machine did to a node.
type fakeNode struct {
	version string

	applied        [][]byte
	upgrades       []string
	legacyUpgrades []string

	versionErr error
	upgradeErr error
}

func (f *fakeNode) Upgrade(_ context.Context, image string) error {
	f.upgrades = append(f.upgrades, image)

	return f.upgradeErr
}

func (f *fakeNode) LegacyUpgrade(_ context.Context, image string) error {
	f.legacyUpgrades = append(f.legacyUpgrades, image)

	return nil
}
```

- [ ] **Step 4: Run the package**

Run: `GOTOOLCHAIN=auto go test ./internal/inplace/ -count=1`
Expected: `ok` (the handler still calls `Upgrade` for every node; Task 5 changes that).

- [ ] **Step 5: Commit**

```bash
git add internal/inplace/talos.go internal/inplace/updatemachine_test.go internal/inplace/lifecycle_test.go
git commit -m "feat(inplace): give the node client a lifecycle upgrade and a legacy fallback

Upgrade now blocks for the lifecycle sequence; LegacyUpgrade keeps the
machine service call for nodes before Talos 1.13, the one deprecated
Talos call left."
```

---

### Task 5: Upgrade tracker and the hook's upgrade step

**Files:**
- Create: `internal/inplace/upgrades.go`
- Create: `internal/inplace/upgrades_test.go`
- Modify: `internal/inplace/server.go` (`Handler`, `NewHandler`)
- Modify: `internal/inplace/updatemachine.go` (upgrade step)
- Modify: `internal/inplace/updatemachine_test.go`

**Interfaces:**
- Consumes: `NodeClient.Upgrade` / `LegacyUpgrade` from Task 4.
- Produces: `type upgradeTracker` with `newUpgradeTracker()`, `start(key types.NamespacedName, image string, run func() error)`, `get(key) (upgrade, bool)`, `forget(key)`; `Handler.upgrades *upgradeTracker`; `func lifecycleUpgradeSupported(runningVersion string) bool`.

- [ ] **Step 1: Write the failing tracker tests**

Create `internal/inplace/upgrades_test.go`:

```go
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package inplace

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
)

var trackerKey = types.NamespacedName{Namespace: "default", Name: "machine-1"}

func TestUpgradeTrackerRunsTheSequenceOnceAndRecordsTheOutcome(t *testing.T) {
	t.Parallel()

	tracker := newUpgradeTracker()
	done := make(chan struct{})

	tracker.start(trackerKey, "img", func() error {
		<-done

		return errors.New("install exited with code 1")
	})

	current, ok := tracker.get(trackerKey)
	require.True(t, ok)
	assert.Equal(t, "img", current.image)
	assert.False(t, current.done)
	assert.NoError(t, current.err)

	close(done)

	require.Eventually(t, func() bool {
		current, _ := tracker.get(trackerKey)

		return current.done
	}, time.Second, 10*time.Millisecond)

	current, _ = tracker.get(trackerKey)
	assert.EqualError(t, current.err, "install exited with code 1")

	tracker.forget(trackerKey)
	_, ok = tracker.get(trackerKey)
	assert.False(t, ok)
}

func TestLifecycleUpgradeSupported(t *testing.T) {
	t.Parallel()

	assert.True(t, lifecycleUpgradeSupported("v1.14.0"))
	assert.True(t, lifecycleUpgradeSupported("v1.13.0"))
	assert.True(t, lifecycleUpgradeSupported("v1.13.0-alpha.2"))
	assert.False(t, lifecycleUpgradeSupported("v1.13.0-alpha.1"))
	assert.False(t, lifecycleUpgradeSupported("v1.12.3"))
	assert.False(t, lifecycleUpgradeSupported(""))
	assert.False(t, lifecycleUpgradeSupported("garbage"))
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `GOTOOLCHAIN=auto go test ./internal/inplace/ -run 'TestUpgradeTracker|TestLifecycleUpgradeSupported' -count=1`
Expected: build failure, `undefined: newUpgradeTracker`, `undefined: lifecycleUpgradeSupported`.

- [ ] **Step 3: Implement the tracker**

Create `internal/inplace/upgrades.go`:

```go
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package inplace

import (
	"strings"
	"sync"
	"time"

	"github.com/Masterminds/semver/v3"
	"k8s.io/apimachinery/pkg/types"
)

// upgradeTimeout bounds one lifecycle upgrade sequence, and how long a finished sequence is
// remembered while the node comes back on the new version.
const upgradeTimeout = 30 * time.Minute

// lifecycleUpgradeMinimum is the first Talos that serves the lifecycle service.
var lifecycleUpgradeMinimum = semver.MustParse("1.13.0-alpha.2")

// lifecycleUpgradeSupported reports whether a node running the given version tag can be
// upgraded through the lifecycle service. Anything unparsable is treated as too old.
func lifecycleUpgradeSupported(runningVersion string) bool {
	version, err := semver.NewVersion(strings.TrimPrefix(runningVersion, "v"))
	if err != nil {
		return false
	}

	return !version.LessThan(lifecycleUpgradeMinimum)
}

// upgrade is one lifecycle upgrade sequence, in flight or finished, for a machine.
type upgrade struct {
	image   string
	started time.Time
	done    bool
	err     error
}

// upgradeTracker follows the lifecycle upgrades the hook has started, so that each hook call
// can report on a sequence instead of waiting for it. A controller restart loses it; the next
// hook call then sees the old version and starts the sequence over, which is safe because pull
// and install are idempotent.
type upgradeTracker struct {
	mu       sync.Mutex
	upgrades map[types.NamespacedName]*upgrade
	now      func() time.Time
}

func newUpgradeTracker() *upgradeTracker {
	return &upgradeTracker{upgrades: map[types.NamespacedName]*upgrade{}, now: time.Now}
}

// start records a sequence for key and runs it in the background; its outcome is kept for the
// next look. A sequence already recorded for key is left alone.
func (t *upgradeTracker) start(key types.NamespacedName, image string, run func() error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, exists := t.upgrades[key]; exists {
		return
	}

	entry := &upgrade{image: image, started: t.now()}
	t.upgrades[key] = entry

	go func() {
		err := run()

		t.mu.Lock()
		defer t.mu.Unlock()

		entry.done = true
		entry.err = err
	}()
}

// get returns a snapshot of the sequence recorded for key.
func (t *upgradeTracker) get(key types.NamespacedName) (upgrade, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	entry, ok := t.upgrades[key]
	if !ok {
		return upgrade{}, false
	}

	return *entry, true
}

// forget drops the sequence recorded for key.
func (t *upgradeTracker) forget(key types.NamespacedName) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.upgrades, key)
}
```

- [ ] **Step 4: Run the tracker tests**

Run: `GOTOOLCHAIN=auto go test ./internal/inplace/ -run 'TestUpgradeTracker|TestLifecycleUpgradeSupported' -count=1 -v`
Expected: PASS.

- [ ] **Step 5: Write the failing handler tests**

In `internal/inplace/updatemachine_test.go`, change `TestUpdateMachine_UpgradesWhenInstallerImageDiffers` to cover the legacy node, and add the lifecycle cases. The fixture's `handler` is built with `NewHandler`; the factory it is given must return the fixture's `fakeNode` for every call, including the tracker's background call (check `newUpdateFixture` and keep it so).

```go
func TestUpdateMachine_UsesTheMachineServiceUpgradeOnNodesBefore113(t *testing.T) {
	t.Parallel()

	f := newUpdateFixture(t, "v1.12.3", "ghcr.io/siderolabs/installer:v1.13.0", freshHash(t), "")

	resp := &runtimehooksv1.UpdateMachineResponse{}
	f.handler.DoUpdateMachine(context.Background(), f.request, resp)

	require.Equal(t, runtimehooksv1.ResponseStatusSuccess, resp.Status, resp.Message)
	assert.Len(t, f.node.applied, 1, "config is applied before the upgrade so the new image is recorded")
	assert.Equal(t, []string{"ghcr.io/siderolabs/installer:v1.13.0"}, f.node.legacyUpgrades)
	assert.Empty(t, f.node.upgrades, "a pre-1.13 node has no lifecycle service")
	assert.Positive(t, resp.RetryAfterSeconds, "must keep polling while the node reboots")
}

func TestUpdateMachine_StartsOneLifecycleUpgradeAndKeepsPolling(t *testing.T) {
	t.Parallel()

	f := newUpdateFixture(t, "v1.13.0", "factory.talos.dev/metal-installer/abc:v1.14.0", freshHash(t), "")

	resp := &runtimehooksv1.UpdateMachineResponse{}
	f.handler.DoUpdateMachine(context.Background(), f.request, resp)

	require.Equal(t, runtimehooksv1.ResponseStatusSuccess, resp.Status, resp.Message)
	assert.Positive(t, resp.RetryAfterSeconds)
	assert.Contains(t, resp.Message, "factory.talos.dev/metal-installer/abc:v1.14.0")

	require.Eventually(t, func() bool { return len(f.node.upgrades) == 1 }, time.Second, 10*time.Millisecond, "the sequence runs in the background")

	// The node still reports the old version: the sequence is finished, the reboot pending.
	resp = &runtimehooksv1.UpdateMachineResponse{}
	f.handler.DoUpdateMachine(context.Background(), f.request, resp)

	require.Equal(t, runtimehooksv1.ResponseStatusSuccess, resp.Status, resp.Message)
	assert.Positive(t, resp.RetryAfterSeconds)
	assert.Len(t, f.node.upgrades, 1, "a finished sequence is not started again while the node reboots")
	assert.Empty(t, f.node.legacyUpgrades)

	// Back on the new version: the update completes and the sequence is forgotten.
	f.node.version = "v1.14.0"
	resp = &runtimehooksv1.UpdateMachineResponse{}
	f.handler.DoUpdateMachine(context.Background(), f.request, resp)

	require.Equal(t, runtimehooksv1.ResponseStatusSuccess, resp.Status, resp.Message)
	assert.Zero(t, resp.RetryAfterSeconds)
	_, tracked := f.handler.upgrades.get(types.NamespacedName{Namespace: "default", Name: "machine-1"})
	assert.False(t, tracked)
}

func TestUpdateMachine_ReportsAFailedLifecycleUpgradeOnceThenRetries(t *testing.T) {
	t.Parallel()

	f := newUpdateFixture(t, "v1.13.0", "factory.talos.dev/metal-installer/abc:v1.14.0", freshHash(t), "")
	f.node.upgradeErr = errors.New("the upgrade exited with code 1: bad image")

	resp := &runtimehooksv1.UpdateMachineResponse{}
	f.handler.DoUpdateMachine(context.Background(), f.request, resp)
	require.Equal(t, runtimehooksv1.ResponseStatusSuccess, resp.Status, resp.Message)

	require.Eventually(t, func() bool {
		current, ok := f.handler.upgrades.get(types.NamespacedName{Namespace: "default", Name: "machine-1"})

		return ok && current.done
	}, time.Second, 10*time.Millisecond)

	resp = &runtimehooksv1.UpdateMachineResponse{}
	f.handler.DoUpdateMachine(context.Background(), f.request, resp)
	assert.Equal(t, runtimehooksv1.ResponseStatusFailure, resp.Status)
	assert.Contains(t, resp.Message, "failed to upgrade Talos")

	// The failure was surfaced once; the next call starts the sequence over.
	resp = &runtimehooksv1.UpdateMachineResponse{}
	f.handler.DoUpdateMachine(context.Background(), f.request, resp)
	assert.Equal(t, runtimehooksv1.ResponseStatusSuccess, resp.Status, resp.Message)
	require.Eventually(t, func() bool { return len(f.node.upgrades) == 2 }, time.Second, 10*time.Millisecond)
}
```

Add `"errors"` and `"time"` to that file's imports and delete the old `TestUpdateMachine_UpgradesWhenInstallerImageDiffers`.

- [ ] **Step 6: Run to verify they fail**

Run: `GOTOOLCHAIN=auto go test ./internal/inplace/ -run 'TestUpdateMachine_' -count=1`
Expected: build failure on `f.handler.upgrades` (no such field), then after adding the field the lifecycle tests fail because `Upgrade` is still called synchronously for every node.

- [ ] **Step 7: Implement the handler changes**

In `internal/inplace/server.go`:

```go
// Handler serves the Cluster API in-place update hooks for Talos machines.
type Handler struct {
	client        client.Client
	newNodeClient NodeClientFactory
	upgrades      *upgradeTracker
}

func NewHandler(c client.Client, factory NodeClientFactory) *Handler {
	return &Handler{
		client:        c,
		newNodeClient: factory,
		upgrades:      newUpgradeTracker(),
	}
}
```

In `internal/inplace/updatemachine.go`, replace the block starting at `if needsUpgrade(desiredImage, runningVersion) {` through the end of `DoUpdateMachine` with:

```go
	machineKey := types.NamespacedName{Namespace: machine.Namespace, Name: machine.Name}

	if needsUpgrade(desiredImage, runningVersion) {
		h.continueUpgrade(ctx, log, resp, node, upgradeTarget{
			machine:    machineKey,
			cluster:    clusterKey,
			endpoints:  endpoints,
			image:      desiredImage,
			running:    runningVersion,
		})

		return
	}

	// The node runs the desired version, so any sequence recorded for it is over.
	h.upgrades.forget(machineKey)

	log.Info("in-place update applied")

	resp.Status = runtimehooksv1.ResponseStatusSuccess
	resp.RetryAfterSeconds = 0
}

// upgradeTarget is what continueUpgrade needs to know about the machine being upgraded.
type upgradeTarget struct {
	machine   types.NamespacedName
	cluster   types.NamespacedName
	endpoints []string
	image     string
	running   string
}

// continueUpgrade moves a Talos upgrade along by one hook call.
//
// Nodes before Talos 1.13 upgrade themselves through the machine service and need nothing more
// than the request. Newer nodes are driven through the lifecycle service, whose sequence takes
// minutes; it runs in the background under the tracker, and each call reports on it: started,
// still running, failed (once, then the sequence is retried from scratch) or finished and
// waiting for the node to come back on the new version.
func (h *Handler) continueUpgrade(ctx context.Context, log logr.Logger, resp *runtimehooksv1.UpdateMachineResponse, node NodeClient, target upgradeTarget) {
	if !lifecycleUpgradeSupported(target.running) {
		log.Info("upgrading Talos in place through the machine service", "image", target.image, "running", target.running)

		if err := node.LegacyUpgrade(ctx, target.image); err != nil {
			respondRetryFailure(&resp.CommonRetryResponse, "failed to upgrade Talos", err, log)

			return
		}

		respondRetry(resp, fmt.Sprintf("upgrading Talos to %s", target.image))

		return
	}

	current, tracked := h.upgrades.get(target.machine)

	switch {
	case !tracked:
		log.Info("starting a lifecycle upgrade", "image", target.image, "running", target.running)

		h.upgrades.start(target.machine, target.image, func() error {
			upgradeCtx, cancel := context.WithTimeout(context.Background(), upgradeTimeout)
			defer cancel()

			// The request-scoped client is closed when this hook call returns; the sequence
			// outlives it and needs a connection of its own.
			background, err := h.newNodeClient(upgradeCtx, target.cluster, target.endpoints)
			if err != nil {
				return fmt.Errorf("failed to connect for the upgrade: %w", err)
			}

			defer background.Close() //nolint:errcheck // best effort

			return background.Upgrade(upgradeCtx, target.image)
		})

		respondRetry(resp, fmt.Sprintf("upgrading Talos to %s", target.image))
	case current.err != nil:
		h.upgrades.forget(target.machine)
		respondRetryFailure(&resp.CommonRetryResponse, "failed to upgrade Talos", current.err, log)
	case current.done && time.Since(current.started) > upgradeTimeout:
		// The install finished long ago and the node never came back on the new version.
		log.Info("the node did not come back on the new version, restarting the upgrade", "image", target.image)
		h.upgrades.forget(target.machine)
		respondRetry(resp, fmt.Sprintf("restarting the upgrade to %s", target.image))
	case current.done:
		respondRetry(resp, fmt.Sprintf("waiting for the node to reboot into %s", target.image))
	default:
		respondRetry(resp, fmt.Sprintf("upgrading Talos to %s", target.image))
	}
}

// respondRetry asks Cluster API to call again after the reboot wait.
func respondRetry(resp *runtimehooksv1.UpdateMachineResponse, message string) {
	resp.Status = runtimehooksv1.ResponseStatusSuccess
	resp.Message = message
	resp.RetryAfterSeconds = waitForRebootSeconds
}
```

Add `"time"` and `"github.com/go-logr/logr"` to the file's imports. The old `respondRetryFailure` call for `node.Upgrade` and the trailing block are gone.

- [ ] **Step 8: Run the package**

Run: `GOTOOLCHAIN=auto go test ./internal/inplace/ -count=1 -race`
Expected: `ok`, no data races.

- [ ] **Step 9: Commit**

```bash
git add internal/inplace/upgrades.go internal/inplace/upgrades_test.go internal/inplace/server.go internal/inplace/updatemachine.go internal/inplace/updatemachine_test.go
git commit -m "feat(inplace): track lifecycle upgrades across hook calls

The lifecycle sequence takes minutes and the hook must answer at once,
so the handler starts it in the background under a per-machine tracker
and reports on it: started, running, failed once then retried, or
finished and waiting for the node to come back. Nodes before Talos 1.13
keep the machine service upgrade."
```

---

### Task 6: v1alpha3 conditions without the CAPI v1beta1 package

**Files:**
- Create: `api/v1alpha3/condition_types.go`
- Modify: `api/v1alpha3/conditions.go`, `api/v1alpha3/v1beta2_conditions.go`, `api/v1alpha3/talosconfig_types.go`, `api/v1alpha3/conversion.go`, `api/v1alpha3/conversion_test.go`
- Regenerate: `api/v1alpha3/zz_generated.deepcopy.go`, `api/v1alpha3/zz_generated.conversion.go`

**Interfaces:**
- Produces: `v1alpha3.Condition`, `v1alpha3.Conditions`, `v1alpha3.ConditionType`, `v1alpha3.ConditionSeverity`; `Convert_v1alpha3_Condition_To_v1_Condition` and `Convert_v1_Condition_To_v1alpha3_Condition`.

- [ ] **Step 1: Write the failing conversion test**

Append to `api/v1alpha3/conversion_test.go`:

```go
// A hub object's v1beta2 conditions surface as v1alpha3 conditions of the old shape, without a
// severity, so old clients still see what the controller observed.
func TestTalosConfigConvertFromDerivesLegacyConditions(t *testing.T) {
	t.Parallel()

	transition := metav1.NewTime(time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC))
	hub := &bootstrapv1beta1.TalosConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Status: bootstrapv1beta1.TalosConfigStatus{
			Initialization: bootstrapv1beta1.TalosConfigInitializationStatus{DataSecretCreated: ptr.To(true)},
			Conditions: []metav1.Condition{{
				Type: "DataSecretAvailable", Status: metav1.ConditionTrue, Reason: "Available", Message: "rendered", LastTransitionTime: transition,
			}},
		},
	}

	spoke := &bootstrapv1alpha3.TalosConfig{}
	require.NoError(t, spoke.ConvertFrom(hub))

	assert.True(t, spoke.Status.Ready)
	require.Len(t, spoke.Status.Conditions, 1)
	assert.Equal(t, bootstrapv1alpha3.Condition{
		Type: "DataSecretAvailable", Status: corev1.ConditionTrue, Reason: "Available", Message: "rendered", LastTransitionTime: transition,
	}, spoke.Status.Conditions[0])
	assert.Empty(t, spoke.Status.FailureReason)
}
```

Add `"time"`, `corev1 "k8s.io/api/core/v1"` and `"k8s.io/utils/ptr"` to its imports.

- [ ] **Step 2: Run to verify it fails**

Run: `GOTOOLCHAIN=auto go test ./api/v1alpha3/ -run TestTalosConfigConvertFromDerivesLegacyConditions -count=1`
Expected: build failure, `undefined: bootstrapv1alpha3.Condition`.

- [ ] **Step 3: Add the local condition types**

Create `api/v1alpha3/condition_types.go`:

```go
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v1alpha3

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The condition shape of the v1alpha3 API. It is the Cluster API v1beta1 condition this version
// was built on, defined here so the served schema outlives that package.

// ConditionType is a valid value for Condition.Type.
type ConditionType string

// ConditionSeverity expresses the severity of a Condition Type failing.
type ConditionSeverity string

const (
	// ConditionSeverityError specifies that a condition with `Status=False` is an error.
	ConditionSeverityError ConditionSeverity = "Error"

	// ConditionSeverityWarning specifies that a condition with `Status=False` is a warning.
	ConditionSeverityWarning ConditionSeverity = "Warning"

	// ConditionSeverityInfo specifies that a condition with `Status=False` is informative.
	ConditionSeverityInfo ConditionSeverity = "Info"

	// ConditionSeverityNone is the default severity.
	ConditionSeverityNone ConditionSeverity = ""
)

// Condition defines an observation of a TalosConfig's operational state.
type Condition struct {
	// Type of condition in CamelCase.
	Type ConditionType `json:"type"`

	// Status of the condition, one of True, False, Unknown.
	Status corev1.ConditionStatus `json:"status"`

	// Severity provides an explicit classification of Reason code when Status is False.
	// +optional
	Severity ConditionSeverity `json:"severity,omitempty"`

	// LastTransitionTime is the last time the condition transitioned from one status to another.
	LastTransitionTime metav1.Time `json:"lastTransitionTime"`

	// Reason is a brief CamelCase word for the condition's last transition.
	// +optional
	Reason string `json:"reason,omitempty"`

	// Message is a human readable message indicating details about the transition.
	// +optional
	Message string `json:"message,omitempty"`
}

// Conditions is a list of Condition.
type Conditions []Condition
```

In `api/v1alpha3/conditions.go` remove the import and change both `capiv1.ConditionType` to `ConditionType`.

In `api/v1alpha3/v1beta2_conditions.go` replace the import with `clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"` and the constants:

```go
	TalosConfigReadyV1Beta2Condition = clusterv1.ReadyCondition
	TalosConfigReadyV1Beta2Reason = clusterv1.ReadyReason
	TalosConfigNotReadyV1Beta2Reason = clusterv1.NotReadyReason
	TalosConfigReadyUnknownV1Beta2Reason = clusterv1.ReadyUnknownReason
	ClientConfigAvailableV1Beta2Reason = clusterv1.AvailableReason
	ClientConfigAvailableInternalErrorV1Beta2Reason = clusterv1.InternalErrorReason
	DataSecretAvailableV1Beta2Reason = clusterv1.AvailableReason
	DataSecretNotAvailableV1Beta2Reason = clusterv1.NotAvailableReason
```

In `api/v1alpha3/talosconfig_types.go` change `Conditions capiv1.Conditions` to `Conditions Conditions` and drop the now-unused import.

In `api/v1alpha3/conversion.go` add the element conversions (keep the file's `metav1` and `apimachineryconversion` imports, add `corev1 "k8s.io/api/core/v1"`):

```go
// Convert_v1alpha3_Condition_To_v1_Condition is required by conversion-gen for the status
// conditions; the hand-written status conversions decide when it is used.
func Convert_v1alpha3_Condition_To_v1_Condition(in *Condition, out *metav1.Condition, _ apimachineryconversion.Scope) error {
	*out = metav1.Condition{
		Type:               string(in.Type),
		Status:             metav1.ConditionStatus(in.Status),
		LastTransitionTime: in.LastTransitionTime,
		Reason:             in.Reason,
		Message:            in.Message,
	}

	return nil
}

// Convert_v1_Condition_To_v1alpha3_Condition derives an old-shape condition; severity has no
// v1beta2 equivalent and stays empty.
func Convert_v1_Condition_To_v1alpha3_Condition(in *metav1.Condition, out *Condition, _ apimachineryconversion.Scope) error {
	*out = Condition{
		Type:               ConditionType(in.Type),
		Status:             corev1.ConditionStatus(in.Status),
		LastTransitionTime: in.LastTransitionTime,
		Reason:             in.Reason,
		Message:            in.Message,
	}

	return nil
}
```

In `Convert_v1beta1_TalosConfigStatus_To_v1alpha3_TalosConfigStatus` delete the `out.Conditions = nil` reset and the block reading `in.Deprecated`, so the generated element-wise conversion of `in.Conditions` stands and `out.Ready` still derives from `Initialization.DataSecretCreated`. In `Convert_v1alpha3_TalosConfigStatus_To_v1beta1_TalosConfigStatus` keep `out.Conditions = nil` followed by the `in.V1Beta2` read, and delete the block that fills `out.Deprecated` (Task 7 removes the type). Remove the `clusterv1beta1` import once nothing uses it.

- [ ] **Step 4: Regenerate and run**

Run the deepcopy and conversion-gen commands from Global Constraints, then:
`GOTOOLCHAIN=auto go build ./... && GOTOOLCHAIN=auto go test ./api/... -count=1`
Expected: build OK (if `Deprecated` still exists at this point the generated code compiles either way), the new test PASSES, the fuzz round-trip tests still pass.

- [ ] **Step 5: Commit**

```bash
git add api/v1alpha3
git commit -m "refactor(api): give v1alpha3 its own condition types

The served v1alpha3 schema keeps the old condition shape but no longer
imports Cluster API's deprecated v1beta1 package for it; old-shape
conditions are now derived from the v1beta2 conditions on conversion."
```

---

### Task 7: Remove status.deprecated.v1beta1 from the v1beta1 API

**Files:**
- Modify: `api/v1beta1/talosconfig_types.go`; delete `api/v1beta1/v1beta1_conditions.go`
- Modify: `controllers/talosconfig_controller.go` (lines listed below)
- Modify: `api/v1alpha3/conversion.go` (already trimmed in Task 6)
- Regenerate: deepcopy, conversions, CRDs

- [ ] **Step 1: Write the failing test**

Append to `controllers/generation_test.go`:

```go
// TestReconcileWritesOnlyV1Beta2Conditions: the deprecated v1beta1 block is gone and the
// summary condition is maintained in the v1beta2 shape alone.
func TestReconcileWritesOnlyV1Beta2Conditions(t *testing.T) {
	t.Parallel()

	f := newImageFactoryFixture(t, bootstrapv1beta1.TalosConfigSpec{GenerateType: "worker", TalosVersion: "v1.14"}, nil)

	require.NoError(t, f.reconcile(t))

	cfg := f.config(t)
	require.NotEmpty(t, cfg.Status.Conditions)

	raw, err := json.Marshal(cfg.Status)
	require.NoError(t, err)
	assert.NotContains(t, string(raw), `"deprecated"`)
}
```

Add `"encoding/json"` to the imports.

- [ ] **Step 2: Run to verify it fails**

Run: `GOTOOLCHAIN=auto go test ./controllers/ -run TestReconcileWritesOnlyV1Beta2Conditions -count=1`
Expected: FAIL on the `"deprecated"` assertion.

- [ ] **Step 3: Remove the block**

In `api/v1beta1/talosconfig_types.go` delete the `Deprecated` field, the `TalosConfigDeprecatedStatus` and `TalosConfigV1Beta1DeprecatedStatus` types, and the `GetV1Beta1Conditions`/`SetV1Beta1Conditions` methods; drop the `capiv1` import if nothing else uses it. Delete `api/v1beta1/v1beta1_conditions.go`.

In `controllers/talosconfig_controller.go`:
- delete the `v1beta1conditions.SetSummary(...)` call and the `patch.WithOwnedV1Beta1Conditions{...}` option in the deferred patch;
- delete every `v1beta1conditions.MarkTrue(...)` and `v1beta1conditions.MarkFalse(...)` call (at the sites currently around lines 282, 293, 300, 327, 350, 368, 381), leaving the neighbouring `conditions.Set(...)` calls;
- remove the `v1beta1conditions` import; keep `capiv1` (still used for Cluster, Machine and labels).

- [ ] **Step 4: Regenerate and run everything**

Run the three generation commands from Global Constraints, then:
`GOTOOLCHAIN=auto go build ./... && GOTOOLCHAIN=auto go vet ./... && GOTOOLCHAIN=auto go test ./api/... ./internal/... ./controllers/... -count=1`
Expected: build and vet OK, all tests pass, `git status` shows the two CRD files, both deepcopy files and the conversion file regenerated.

- [ ] **Step 5: Commit**

```bash
git add api config controllers
git commit -m "feat(api)!: drop the Cluster API v1beta1 compatibility block from TalosConfig status

status.deprecated.v1beta1 mirrored the old contract's conditions and
failure fields next to the v1beta2 ones. Nothing reads it, CAPI core
v1.12+ reads only the v1beta2 fields, and v1alpha3 now derives its
conditions from them, so it goes ahead of CAPI's own removal."
```

---

### Task 8: Scheme builders without controller-runtime's deprecated helper

**Files:**
- Modify: `api/v1beta1/groupversion_info.go`, `api/v1alpha3/groupversion_info.go`, and every `SchemeBuilder.Register(...)` init in `api/v1beta1/*_types.go` and `api/v1alpha3/*_types.go`
- Create: `api/v1beta1/groupversion_info_test.go`

- [ ] **Step 1: Write the failing test**

Create `api/v1beta1/groupversion_info_test.go`:

```go
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v1beta1_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"

	bootstrapv1beta1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
)

func TestAddToSchemeRegistersEveryKind(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, bootstrapv1beta1.AddToScheme(scheme))

	for _, kind := range []string{"TalosConfig", "TalosConfigList", "TalosConfigTemplate", "TalosConfigTemplateList"} {
		assert.True(t, scheme.Recognizes(bootstrapv1beta1.GroupVersion.WithKind(kind)), kind)
	}
}
```

- [ ] **Step 2: Run it**

Run: `GOTOOLCHAIN=auto go test ./api/v1beta1/ -run TestAddToSchemeRegistersEveryKind -count=1`
Expected: PASS already (this is a refactor; the test guards it).

- [ ] **Step 3: Replace the builder in both packages**

`api/v1beta1/groupversion_info.go` body:

```go
import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// GroupVersion is group version used to register these objects
	GroupVersion = schema.GroupVersion{Group: "bootstrap.cluster.x-k8s.io", Version: "v1beta1"}

	// schemeBuilder collects the registrations; types files add theirs from init.
	schemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme adds the types in this group-version to the given scheme.
	AddToScheme = schemeBuilder.AddToScheme

	// objectTypes are the kinds registered under GroupVersion.
	objectTypes []runtime.Object
)

// register queues objects for registration under GroupVersion.
func register(objects ...runtime.Object) {
	objectTypes = append(objectTypes, objects...)
}

func addKnownTypes(s *runtime.Scheme) error {
	s.AddKnownTypes(GroupVersion, objectTypes...)
	metav1.AddToGroupVersion(s, GroupVersion)

	return nil
}
```

`api/v1alpha3/groupversion_info.go`: the same with `Version: "v1alpha3"`, plus `localSchemeBuilder = &schemeBuilder` in the `var` block (the generated conversions call `localSchemeBuilder.Register`).

In every `*_types.go` `init()` replace `SchemeBuilder.Register(` with `register(`.

- [ ] **Step 4: Run the API and controller tests**

Run: `GOTOOLCHAIN=auto go build ./... && GOTOOLCHAIN=auto go test ./api/... ./controllers/... -count=1`
Expected: `ok`, including the conversion tests, which exercise the v1alpha3 registrations.

- [ ] **Step 5: Commit**

```bash
git add api
git commit -m "refactor(api): register schemes with runtime.NewSchemeBuilder

controller-runtime deprecates scheme.Builder for api packages."
```

---

### Task 9: Documentation and release notes

**Files:**
- Modify: `README.md` (the `spec.imageFactory` section), `docs/in-place-updates.md` (the change table rows for `spec.imageFactory` and "Talos OS version", the "Installer image resolution" section), `docs/design/2026-09-12-image-factory-schematic.md` (a status line at the top), `hack/release.toml`

- [ ] **Step 1: Update the in-place guide**

Replace the two table rows:

```markdown
| `TalosConfig.spec.imageFactory` | yes | config regenerated with the new installer image (an `UnattendedInstallConfig` document from Talos 1.14, `machine.install.image` before); the node is upgraded only if the image tag names a Talos version it is not already running |
| Talos OS version (installer image) | yes | Talos 1.13+: lifecycle service (pull, install, reboot), driven in the background and reported on each hook call; older nodes: the machine service `Upgrade`. Talos cordons and drains the node itself on reboot |
```

Replace the first paragraph of "Installer image resolution" with:

```markdown
The installer image is rendered by CABPT itself from `spec.imageFactory` on the TalosConfig:
the schematic is registered with the Talos Image Factory, the Talos version is pinned in
`status.imageFactory.talosVersion`, and the resulting
`<factory>/metal-installer/<schematic>:<version>` is applied as a strategic patch ahead of any
`strategicPatches`, so an explicit patch still wins. From Talos 1.14 (and for an unset
`talosVersion`) it is an `UnattendedInstallConfig` document whose disk selector is `system_disk`;
patch the document to choose another disk. Older contracts get `machine.install.image`.
```

In the "What the MachinePool path does not do" list replace `machine.install.image` with "installer image" in both bullets.

- [ ] **Step 2: Update the README and the design note**

In `README.md`, in the `spec.imageFactory` description replace `renders machine.install.image as <factory>/metal-installer/<schematic>:<version>` with `renders <factory>/metal-installer/<schematic>:<version> as the installer image: an UnattendedInstallConfig document from Talos 1.14 (disk selector system_disk, override it with a strategic patch), machine.install.image before`.

Add to `docs/design/2026-09-12-image-factory-schematic.md` after the "Date/Status" line:

```markdown
Superseded in part by `2026-09-16-unattended-install-lifecycle-upgrades.md`: from Talos 1.14 the
image is rendered as an `UnattendedInstallConfig` document rather than `machine.install.image`.
```

- [ ] **Step 3: Release notes**

In `hack/release.toml` append to the Talos note:

```
On Talos 1.14 contracts the resolved installer image is rendered as an
UnattendedInstallConfig document (disk selector `system_disk`; patch the document to
change it). In-place upgrades of Talos 1.13+ nodes go through the lifecycle service.
```

and add a note:

```
[notes.api]
    title = "API"
    description = """\
`status.deprecated.v1beta1` is removed from TalosConfig: only the v1beta2 conditions and
`status.initialization` remain. v1alpha3 clients still see old-shape conditions, derived
from the v1beta2 ones.
"""
```

- [ ] **Step 4: Commit**

```bash
git add README.md docs hack/release.toml
git commit -m "docs: describe the install document, lifecycle upgrades and the v1beta2-only status"
```

---

## Self-review

- Spec section 1 (rendering, precedence, in-place reader, docs): Tasks 1, 2, 9.
- Spec section 2 (node client, tracker, legacy fallback, timeout): Tasks 3, 4, 5.
- Spec section 3 (v1beta1 block, v1alpha3 local types, scheme builders): Tasks 6, 7, 8.
- Names used across tasks: `installImagePatch(contract, image)`, `versionContractFor`, `installImage`, `upgradeClients`, `lifecycleUpgrade`, `NodeClient.Upgrade`/`LegacyUpgrade`, `upgradeTracker.start/get/forget`, `lifecycleUpgradeSupported`, `Handler.upgrades`, `v1alpha3.Condition`; each is defined in the task that introduces it.
