// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package configinput_test

import (
	"context"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
	"github.com/siderolabs/talos/pkg/machinery/config/configloader"
	"github.com/siderolabs/talos/pkg/machinery/config/configpatcher"
	"github.com/siderolabs/talos/pkg/machinery/config/encoder"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestBaselineImageChangePreservesReviewedInstallDisk(t *testing.T) {
	ctx := ownerContext(t)
	cfg := inputConfig()
	patch, err := configpatcher.LoadPatch([]byte("apiVersion: v1alpha1\nkind: UnattendedInstallConfig\ninstaller:\n  image: registry.example/install:v1.14.0\nprovisioning:\n  diskSelector:\n    match: disk.dev_path == \"/dev/nvme0n1\"\n"))
	require.NoError(t, err)
	out, err := configpatcher.Apply(configpatcher.WithBytes(completeBaseline(t, ctx)), []configpatcher.Patch{patch})
	require.NoError(t, err)
	baseline, err := out.Config()
	require.NoError(t, err)
	data, err := baseline.EncodeBytes(encoder.WithComments(encoder.CommentsDisabled))
	require.NoError(t, err)
	ctx.InstallerImage = "registry.example/new-install:v1.14.1"
	rendered := renderInput(t, inputClient(t, inputSecret("base", data)), cfg, ctx)
	final, err := configloader.NewFromBytes(rendered.Data())
	require.NoError(t, err)
	require.Equal(t, ctx.InstallerImage, final.UnattendedInstallConfig().InstallerImage())
	require.Equal(t, baseline.UnattendedInstallConfig().VolumeSelector().String(), final.UnattendedInstallConfig().VolumeSelector().String())
	require.Contains(t, final.UnattendedInstallConfig().VolumeSelector().String(), "/dev/nvme0n1")
}

func TestFinalRoleCannotChangeThroughAnyPatch(t *testing.T) {
	for _, location := range []string{"shared public", "Machine public", "shared Secret", "Machine Secret"} {
		t.Run(location, func(t *testing.T) {
			ctx := ownerContext(t)
			cfg := inputConfig()
			cfg.Spec.GenerateType = "none"
			patch := "machine:\n  type: worker\n"
			reader := inputClient(t, inputSecret("base", completeBaseline(t, ctx)), inputSecret("role-patch", []byte(patch)))
			switch location {
			case "shared public":
				cfg.Spec.StrategicPatches = []string{patch}
			case "Machine public":
				cfg.Spec.MachineConfig = &bootstrapv1.MachineConfigOverlay{StrategicPatches: []string{patch}}
			case "shared Secret":
				cfg.Spec.StrategicPatchSecretRefs = []bootstrapv1.SecretKeyReference{{Name: "role-patch", Key: "config"}}
			case "Machine Secret":
				cfg.Spec.MachineConfig = &bootstrapv1.MachineConfigOverlay{StrategicPatchSecretRefs: []bootstrapv1.SecretKeyReference{{Name: "role-patch", Key: "config"}}}
			}
			inputs, err := configinput.Resolve(context.Background(), reader, cfg)
			require.NoError(t, err)
			_, err = configinput.Render(inputs, ctx)
			require.Error(t, err)
			require.True(t, strings.Contains(err.Error(), "role"))
			require.NotContains(t, err.Error(), privateSentinel)
		})
	}
}

func TestInvalidSecretInputNamesItsSafeReference(t *testing.T) {
	ctx := ownerContext(t)
	for _, location := range []string{"baseline", "shared", "Machine"} {
		t.Run(location, func(t *testing.T) {
			cfg := inputConfig()
			reader := inputClient(t, inputSecret("base", completeBaseline(t, ctx)), inputSecret("invalid-private", []byte("machine: ["+privateSentinel)))
			ref := bootstrapv1.SecretKeyReference{Name: "invalid-private", Key: "config"}
			switch location {
			case "baseline":
				cfg.Spec.ConfigSecretRef = &ref
			case "shared":
				cfg.Spec.StrategicPatchSecretRefs = []bootstrapv1.SecretKeyReference{ref}
			case "Machine":
				cfg.Spec.MachineConfig = &bootstrapv1.MachineConfigOverlay{StrategicPatchSecretRefs: []bootstrapv1.SecretKeyReference{ref}}
			}
			inputs, err := configinput.Resolve(context.Background(), reader, cfg)
			require.NoError(t, err)
			_, err = configinput.Render(inputs, ctx)
			require.Error(t, err)
			require.Contains(t, err.Error(), "tenant/invalid-private/config")
			require.NotContains(t, err.Error(), privateSentinel)
		})
	}
}
