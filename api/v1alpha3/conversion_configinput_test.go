// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v1alpha3_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilconversion "sigs.k8s.io/cluster-api/util/conversion"

	alpha "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1alpha3"
	beta "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
)

func configInputSpec() beta.TalosConfigSpec {
	return beta.TalosConfigSpec{
		GenerateType:             "worker",
		ConfigSecretRef:          &beta.SecretKeyReference{Name: "shared-base", Key: "config.yaml"},
		ConfigRevision:           "shared-2",
		ConfigPatches:            []beta.ConfigPatches{{Op: "add", Path: "/machine/nodeLabels", Value: apiextensions.JSON{Raw: []byte(`{"shared":"yes"}`)}}},
		StrategicPatches:         []string{"shared-first", "shared-second"},
		StrategicPatchSecretRefs: []beta.SecretKeyReference{{Name: "shared-first", Key: "patch"}, {Name: "shared-second", Key: "patch"}},
		MachineConfig: &beta.MachineConfigOverlay{
			ConfigSecretRef:          &beta.SecretKeyReference{Name: "machine-base", Key: "config.yaml"},
			ConfigRevision:           "machine-3",
			ConfigPatches:            []beta.ConfigPatches{{Op: "add", Path: "/machine/nodeLabels", Value: apiextensions.JSON{Raw: []byte(`{"machine":"yes"}`)}}},
			StrategicPatches:         []string{"machine-first", "machine-second"},
			StrategicPatchSecretRefs: []beta.SecretKeyReference{{Name: "machine-first", Key: "patch"}, {Name: "machine-second", Key: "patch"}},
		},
		TalosEndpoint: &beta.TalosEndpoint{Address: "[2001:db8::1]:50000", ServerName: "talos.example.com", Node: "2001:db8::2"},
	}
}

// Shared fields must survive through the served schema, not just conversion-data.
// An old-version client must be able to change them without the hub stash undoing it.
func TestConfigInputServedRoundTrip(t *testing.T) {
	t.Parallel()
	t.Run("TalosConfig", func(t *testing.T) {
		hub := &beta.TalosConfig{ObjectMeta: metav1.ObjectMeta{Name: "node", Namespace: "tenant"}, Spec: configInputSpec()}
		hub.Spec.ImageFactory = imageFactorySpec()
		spoke := &alpha.TalosConfig{}
		require.NoError(t, spoke.ConvertFrom(hub))
		assert.Equal(t, "machine-base", spoke.Spec.MachineConfig.ConfigSecretRef.Name)
		spoke.Spec.ConfigRevision = "shared-4"
		spoke.Spec.MachineConfig.ConfigRevision = "machine-5"
		spoke.Spec.StrategicPatchSecretRefs[0].Key = "new-patch"
		spoke.Spec.TalosEndpoint.ServerName = "new.example.com"
		want := configInputSpec()
		want.ImageFactory = imageFactorySpec()
		want.ConfigRevision = "shared-4"
		want.MachineConfig.ConfigRevision = "machine-5"
		want.StrategicPatchSecretRefs[0].Key = "new-patch"
		want.TalosEndpoint.ServerName = "new.example.com"
		restored := &beta.TalosConfig{}
		require.NoError(t, spoke.ConvertTo(restored))
		assert.Equal(t, want, restored.Spec)
		assert.NotContains(t, restored.Annotations, utilconversion.DataAnnotation)

		delete(spoke.Annotations, utilconversion.DataAnnotation)
		withoutStash := &beta.TalosConfig{}
		require.NoError(t, spoke.ConvertTo(withoutStash))
		want.ImageFactory = nil
		assert.Equal(t, want, withoutStash.Spec)
	})
	t.Run("TalosConfigTemplate", func(t *testing.T) {
		hub := templateHub()
		hub.Spec.Template.Spec = configInputSpec()
		hub.Spec.Template.Spec.ImageFactory = imageFactorySpec()
		spoke := &alpha.TalosConfigTemplate{}
		require.NoError(t, spoke.ConvertFrom(hub))
		spoke.Spec.Template.Spec.MachineConfig.ConfigSecretRef.Key = "replacement.yaml"
		want := configInputSpec()
		want.ImageFactory = imageFactorySpec()
		want.MachineConfig.ConfigSecretRef.Key = "replacement.yaml"
		restored := &beta.TalosConfigTemplate{}
		require.NoError(t, spoke.ConvertTo(restored))
		assert.Equal(t, want, restored.Spec.Template.Spec)
		assert.Equal(t, hub.Spec.Template.ObjectMeta, restored.Spec.Template.ObjectMeta)
		delete(spoke.Annotations, utilconversion.DataAnnotation)
		withoutStash := &beta.TalosConfigTemplate{}
		require.NoError(t, spoke.ConvertTo(withoutStash))
		want.ImageFactory = nil
		assert.Equal(t, want, withoutStash.Spec.Template.Spec)
	})
}

func mutateBetaInputs(spec *beta.TalosConfigSpec) {
	spec.ConfigSecretRef.Name = "changed"
	spec.ConfigRevision = "changed"
	spec.ConfigPatches[0].Value.Raw[0] = ' '
	spec.StrategicPatches[0] = "changed"
	spec.StrategicPatchSecretRefs[0].Name = "changed"
	spec.MachineConfig.ConfigSecretRef.Key = "changed"
	spec.MachineConfig.ConfigRevision = "changed"
	spec.MachineConfig.ConfigPatches[0].Value.Raw[0] = ' '
	spec.MachineConfig.StrategicPatches[0] = "changed"
	spec.MachineConfig.StrategicPatchSecretRefs[0].Key = "changed"
	spec.TalosEndpoint.Address = "changed"
	spec.TalosEndpoint.ServerName = "changed"
	spec.TalosEndpoint.Node = "changed"
}

func mutateAlphaInputs(spec *alpha.TalosConfigSpec) {
	spec.ConfigSecretRef.Name = "changed"
	spec.ConfigRevision = "changed"
	spec.ConfigPatches[0].Value.Raw[0] = ' '
	spec.StrategicPatches[0] = "changed"
	spec.StrategicPatchSecretRefs[0].Name = "changed"
	spec.MachineConfig.ConfigSecretRef.Key = "changed"
	spec.MachineConfig.ConfigRevision = "changed"
	spec.MachineConfig.ConfigPatches[0].Value.Raw[0] = ' '
	spec.MachineConfig.StrategicPatches[0] = "changed"
	spec.MachineConfig.StrategicPatchSecretRefs[0].Key = "changed"
	spec.TalosEndpoint.Address = "changed"
	spec.TalosEndpoint.ServerName = "changed"
	spec.TalosEndpoint.Node = "changed"
}

func TestConfigInputDeepCopyIndependence(t *testing.T) {
	t.Parallel()
	t.Run("v1beta1", func(t *testing.T) {
		original := &beta.TalosConfig{Spec: configInputSpec()}
		mutateBetaInputs(&original.DeepCopy().Spec)
		assert.Equal(t, configInputSpec(), original.Spec)
		template := &beta.TalosConfigTemplate{Spec: beta.TalosConfigTemplateSpec{Template: beta.TalosConfigTemplateResource{Spec: configInputSpec()}}}
		mutateBetaInputs(&template.DeepCopy().Spec.Template.Spec)
		assert.Equal(t, configInputSpec(), template.Spec.Template.Spec)
		list := &beta.TalosConfigList{Items: []beta.TalosConfig{*original}}
		mutateBetaInputs(&list.DeepCopy().Items[0].Spec)
		assert.Equal(t, configInputSpec(), list.Items[0].Spec)
		templates := &beta.TalosConfigTemplateList{Items: []beta.TalosConfigTemplate{*template}}
		mutateBetaInputs(&templates.DeepCopy().Items[0].Spec.Template.Spec)
		assert.Equal(t, configInputSpec(), templates.Items[0].Spec.Template.Spec)
	})
	t.Run("v1alpha3", func(t *testing.T) {
		original := &alpha.TalosConfig{}
		require.NoError(t, original.ConvertFrom(&beta.TalosConfig{Spec: configInputSpec()}))
		want := original.Spec.DeepCopy()
		mutateAlphaInputs(&original.DeepCopy().Spec)
		assert.Equal(t, *want, original.Spec)
		template := &alpha.TalosConfigTemplate{Spec: alpha.TalosConfigTemplateSpec{Template: alpha.TalosConfigTemplateResource{Spec: *want}}}
		mutateAlphaInputs(&template.DeepCopy().Spec.Template.Spec)
		assert.Equal(t, *want, template.Spec.Template.Spec)
		list := &alpha.TalosConfigList{Items: []alpha.TalosConfig{*original}}
		mutateAlphaInputs(&list.DeepCopy().Items[0].Spec)
		assert.Equal(t, *want, list.Items[0].Spec)
		templates := &alpha.TalosConfigTemplateList{Items: []alpha.TalosConfigTemplate{*template}}
		mutateAlphaInputs(&templates.DeepCopy().Items[0].Spec.Template.Spec)
		assert.Equal(t, *want, templates.Items[0].Spec.Template.Spec)
	})
}
