// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package configinput_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
)

func componentPatches(role machine.Type, tag string) []string {
	patches := []string{"apiVersion: v1alpha1\nkind: KubeletConfig\nimage: registry.example.com:5443/kubelet:" + tag + "\n"}
	if role == machine.TypeControlPlane {
		for _, component := range []struct{ kind, image string }{{"KubeAPIServerConfig", "kube-apiserver"}, {"KubeControllerManagerConfig", "kube-controller-manager"}, {"KubeSchedulerConfig", "kube-scheduler"}} {
			patches = append(patches, "apiVersion: v1alpha1\nkind: "+component.kind+"\nimage: registry.example.com:5443/"+component.image+":"+tag+"\n")
		}
	}
	return patches
}

func TestReviewedPublicVersionPatchWorksWithoutRevisionBump(t *testing.T) {
	t.Parallel()
	for _, role := range []machine.Type{machine.TypeWorker, machine.TypeControlPlane} {
		ctx := ownerContext(t)
		cfg := inputConfig()
		cfg.Spec.GenerateType = role.String()
		cfg.Spec.ConfigRevision = "unchanged"
		cfg.Spec.StrategicPatches = componentPatches(role, "v1.37.1")
		baseline := generatedBaseline(t, ctx, config.TalosVersion1_14, role)
		ctx.Machine.Spec.Version = "v1.37.1"
		reader := inputClient(t, inputSecret("base", baseline))
		output := renderInput(t, reader, cfg, ctx)
		assert.Contains(t, string(output.Data()), "registry.example.com:5443/kubelet:v1.37.1")
		assert.Equal(t, "unchanged", cfg.Spec.ConfigRevision)
		cfg.Spec.StrategicPatches = nil
		resolved, err := configinput.Resolve(context.Background(), reader, cfg)
		require.NoError(t, err)
		output, err = configinput.Render(resolved, ctx)
		require.Error(t, err)
		assert.Nil(t, output)
		assert.Contains(t, err.Error(), "version")
		assert.NotContains(t, fmt.Sprintf("%v %+v %#v", err, err, err), privateSentinel)
	}
}

func TestComponentImageTagsAndDigests(t *testing.T) {
	t.Parallel()
	ctx := ownerContext(t)
	baseline := generatedBaseline(t, ctx, config.TalosVersion1_14, machine.TypeWorker)
	for _, test := range []struct {
		name, image string
		valid       bool
	}{
		{"custom registry port", "registry.example.com:5443/custom/kubelet:v1.37.0", true},
		{"tag and digest", "registry.example.com:5443/custom/kubelet:v1.37.0@sha256:" + strings.Repeat("a", 64), true},
		{"digest only", "registry.example.com:5443/custom/kubelet@sha256:" + strings.Repeat("a", 64), false},
		{"opaque tag", "registry.example.com:5443/custom/kubelet:" + privateSentinel, false},
		{"wrong version", "registry.example.com:5443/custom/kubelet:v1.36.0", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := inputConfig()
			cfg.Spec.GenerateType = "worker"
			cfg.Spec.StrategicPatches = []string{"apiVersion: v1alpha1\nkind: KubeletConfig\nimage: " + test.image + "\n"}
			resolved, err := configinput.Resolve(context.Background(), inputClient(t, inputSecret("base", baseline)), cfg)
			require.NoError(t, err)
			output, err := configinput.Render(resolved, ctx)
			if test.valid {
				require.NoError(t, err)
				assert.Contains(t, string(output.Data()), test.image)
			} else {
				require.Error(t, err)
				assert.Nil(t, output)
				assert.NotContains(t, fmt.Sprintf("%v %+v %#v", err, err, err), privateSentinel)
			}
		})
	}
}

func TestImportedRoleMismatchIsNotConverted(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		imported  machine.Type
		requested string
	}{{machine.TypeWorker, "controlplane"}, {machine.TypeControlPlane, "worker"}} {
		ctx := ownerContext(t)
		cfg := inputConfig()
		cfg.Spec.GenerateType = test.requested
		resolved, err := configinput.Resolve(context.Background(), inputClient(t, inputSecret("base", generatedBaseline(t, ctx, config.TalosVersion1_14, test.imported))), cfg)
		require.NoError(t, err)
		output, err := configinput.Render(resolved, ctx)
		require.Error(t, err)
		assert.Nil(t, output)
		assert.Contains(t, err.Error(), "role")
	}
}
