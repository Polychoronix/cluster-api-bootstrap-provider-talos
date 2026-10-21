// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package configinput_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/configloader"
	"github.com/siderolabs/talos/pkg/machinery/config/configpatcher"
	"github.com/siderolabs/talos/pkg/machinery/config/encoder"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
)

func hostnamePatch(name string) string {
	return "apiVersion: v1alpha1\nkind: HostnameConfig\nauto: off\nhostname: " + name + "\n"
}

func renderedHostname(t *testing.T, output *configinput.Rendered) string {
	t.Helper()
	provider, err := configloader.NewFromBytes(output.Data())
	require.NoError(t, err)
	require.NotNil(t, provider.NetworkHostnameConfig())
	return provider.NetworkHostnameConfig().Hostname()
}

func TestCompleteBaselinePreservesAllDocumentsAndIdentity(t *testing.T) {
	t.Parallel()
	ctx := ownerContext(t)
	baseline := completeBaseline(t, ctx)
	cfg := inputConfig()
	cfg.Spec.GenerateType = "controlplane"
	cfg.Spec.Hostname.Source = bootstrapv1.HostnameSourceMachineName
	reader := inputClient(t, inputSecret("base", baseline))
	// A baseline must not acquire identity or use generation defaults.
	ctx.SecretsBundle = nil
	ctx.Cluster.Spec.ClusterNetwork.Pods.CIDRBlocks = []string{"172.16.0.0/16"}
	ctx.Cluster.Spec.ClusterNetwork.ServiceDomain = "do-not-overwrite.internal"
	output := renderInput(t, reader, cfg, ctx)
	assert.ElementsMatch(t, documentValues(t, baseline), documentValues(t, output.Data()), "every baseline document including CA/key, network lists, registries and volumes must survive")
	assert.Equal(t, "imported-host", renderedHostname(t, output))
	assert.Contains(t, string(output.Data()), privateSentinel)
	assert.NotContains(t, string(output.Data()), "do-not-overwrite.internal")

	cfg.Spec.StrategicPatches = []string{hostnamePatch("policy-host")}
	output = renderInput(t, reader, cfg, ctx)
	expectedPatch, err := configpatcher.LoadPatch([]byte(hostnamePatch("policy-host")))
	require.NoError(t, err)
	expected, err := configpatcher.Apply(configpatcher.WithBytes(baseline), []configpatcher.Patch{expectedPatch})
	require.NoError(t, err)
	expectedProvider, err := expected.Config()
	require.NoError(t, err)
	expectedData, err := expectedProvider.EncodeBytes(encoder.WithComments(encoder.CommentsDisabled))
	require.NoError(t, err)
	assert.ElementsMatch(t, documentValues(t, expectedData), documentValues(t, output.Data()))
}

func TestSharedAndMachineOrderedPrecedence(t *testing.T) {
	t.Parallel()
	ctx := ownerContext(t)
	baseline := completeBaseline(t, ctx)
	cfg := inputConfig()
	cfg.Spec.ConfigSecretRef.Name = "missing-shared-base"
	cfg.Spec.StrategicPatches = []string{hostnamePatch("shared-public-first"), hostnamePatch("shared-public-last")}
	cfg.Spec.StrategicPatchSecretRefs = []bootstrapv1.SecretKeyReference{{Name: "shared-first", Key: "config"}, {Name: "shared-last", Key: "config"}}
	cfg.Spec.MachineConfig = &bootstrapv1.MachineConfigOverlay{
		ConfigSecretRef:          &bootstrapv1.SecretKeyReference{Name: "machine-base", Key: "config"},
		StrategicPatches:         []string{hostnamePatch("machine-public-first"), hostnamePatch("machine-public-last")},
		StrategicPatchSecretRefs: []bootstrapv1.SecretKeyReference{{Name: "machine-first", Key: "config"}, {Name: "machine-last", Key: "config"}},
	}
	reader := inputClient(t, inputSecret("machine-base", baseline),
		inputSecret("shared-first", []byte(hostnamePatch("shared-secret-first"))), inputSecret("shared-last", []byte(hostnamePatch("shared-secret-last"))),
		inputSecret("machine-first", []byte(hostnamePatch("machine-secret-first"))), inputSecret("machine-last", []byte(hostnamePatch("machine-secret-last"))))
	ctx.SecretsBundle = nil
	for _, expected := range []string{"machine-secret-last", "machine-secret-first", "machine-public-last", "shared-secret-last", "shared-public-last"} {
		assert.Equal(t, expected, renderedHostname(t, renderInput(t, reader, cfg, ctx)))
		switch expected {
		case "machine-secret-last":
			cfg.Spec.MachineConfig.StrategicPatchSecretRefs = cfg.Spec.MachineConfig.StrategicPatchSecretRefs[:1]
		case "machine-secret-first":
			cfg.Spec.MachineConfig.StrategicPatchSecretRefs = nil
		case "machine-public-last":
			cfg.Spec.MachineConfig.StrategicPatches = nil
		case "shared-secret-last":
			cfg.Spec.StrategicPatchSecretRefs = nil
		}
	}
}

func TestJSONPublicPrecedenceOnSupportedSingleDocumentBaseline(t *testing.T) {
	t.Parallel()
	ctx := ownerContext(t)
	baseline := generatedBaseline(t, ctx, config.TalosVersion1_11, machine.TypeWorker)
	cfg := inputConfig()
	cfg.Spec.TalosVersion = "v1.11"
	cfg.Spec.ConfigPatches = []bootstrapv1.ConfigPatches{{Op: "add", Path: "/machine/nodeLabels", Value: apiextensions.JSON{Raw: []byte(`{"shared":"keep","winner":"shared-json"}`)}}}
	cfg.Spec.StrategicPatches = []string{"machine:\n  nodeLabels:\n    winner: shared-public\n"}
	cfg.Spec.StrategicPatchSecretRefs = []bootstrapv1.SecretKeyReference{{Name: "shared", Key: "config"}}
	cfg.Spec.MachineConfig = &bootstrapv1.MachineConfigOverlay{
		ConfigPatches:            []bootstrapv1.ConfigPatches{{Op: "replace", Path: "/machine/nodeLabels/winner", Value: apiextensions.JSON{Raw: []byte(`"machine-json"`)}}},
		StrategicPatches:         []string{"machine:\n  nodeLabels:\n    machine: keep\n    winner: machine-public\n"},
		StrategicPatchSecretRefs: []bootstrapv1.SecretKeyReference{{Name: "machine", Key: "config"}},
	}
	reader := inputClient(t, inputSecret("base", baseline), inputSecret("shared", []byte("machine:\n  nodeLabels:\n    winner: shared-secret\n")), inputSecret("machine", []byte("machine:\n  nodeLabels:\n    winner: machine-secret\n")))
	output := renderInput(t, reader, cfg, ctx)
	assert.Contains(t, string(output.Data()), "winner: machine-secret")
	assert.Contains(t, string(output.Data()), "shared: keep")
	assert.Contains(t, string(output.Data()), "machine: keep")
	cfg.Spec.MachineConfig.StrategicPatchSecretRefs = nil
	assert.Contains(t, string(renderInput(t, reader, cfg, ctx).Data()), "winner: machine-public")
	cfg.Spec.MachineConfig.StrategicPatches = nil
	assert.Contains(t, string(renderInput(t, reader, cfg, ctx).Data()), "winner: machine-json")
}

func TestGeneratedPathMatchesTalosGenerator(t *testing.T) {
	t.Parallel()
	for _, role := range []machine.Type{machine.TypeWorker, machine.TypeControlPlane} {
		ctx := ownerContext(t)
		cfg := inputConfig()
		cfg.Spec.ConfigSecretRef = nil
		cfg.Spec.GenerateType = role.String()
		output := renderInput(t, inputClient(t), cfg, ctx)
		expected := generatedBaseline(t, ctx, config.TalosVersion1_14, role)
		assert.ElementsMatch(t, documentValues(t, expected), documentValues(t, output.Data()))
	}
}

func TestMultiDocumentJSONPatchFailsWithoutLeakingBaseline(t *testing.T) {
	t.Parallel()
	ctx := ownerContext(t)
	cfg := inputConfig()
	cfg.Spec.ConfigPatches = []bootstrapv1.ConfigPatches{{Op: "add", Path: "/machine/nodeLabels", Value: apiextensions.JSON{Raw: []byte(`{"label":"yes"}`)}}}
	resolved, err := configinput.Resolve(context.Background(), inputClient(t, inputSecret("base", completeBaseline(t, ctx))), cfg)
	require.NoError(t, err)
	output, err := configinput.Render(resolved, ctx)
	require.Error(t, err)
	assert.Nil(t, output)
	assert.Contains(t, err.Error(), "JSON")
	assert.NotContains(t, fmt.Sprintf("%v %+v %#v", err, err, err), privateSentinel)
}

func TestPrivateInputsHaveNoPublicFormattingOrJSONSurface(t *testing.T) {
	t.Parallel()
	ctx := ownerContext(t)
	cfg := inputConfig()
	resolved, err := configinput.Resolve(context.Background(), inputClient(t, inputSecret("base", completeBaseline(t, ctx))), cfg)
	require.NoError(t, err)
	output, err := configinput.Render(resolved, ctx)
	require.NoError(t, err)
	for _, object := range []interface{}{resolved, output, cfg} {
		assert.NotContains(t, fmt.Sprintf("%v %+v %#v", object, object, object), privateSentinel)
		data, err := json.Marshal(object)
		require.NoError(t, err)
		assert.NotContains(t, string(data), privateSentinel)
	}
}
