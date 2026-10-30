// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v1beta1

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestValidateConfigInputReferences(t *testing.T) {
	t.Parallel()
	locations := []struct {
		name string
		set  func(*TalosConfigSpec, SecretKeyReference)
		path string
	}{
		{"shared baseline", func(s *TalosConfigSpec, r SecretKeyReference) { s.ConfigSecretRef = &r }, "spec.configSecretRef"},
		{"shared patch", func(s *TalosConfigSpec, r SecretKeyReference) {
			s.StrategicPatchSecretRefs = []SecretKeyReference{{Name: "valid", Key: "patch"}, r}
		}, "spec.strategicPatchSecretRefs[1]"},
		{"machine baseline", func(s *TalosConfigSpec, r SecretKeyReference) {
			s.MachineConfig = &MachineConfigOverlay{ConfigSecretRef: &r}
		}, "spec.machineConfig.configSecretRef"},
		{"machine patch", func(s *TalosConfigSpec, r SecretKeyReference) {
			s.MachineConfig = &MachineConfigOverlay{StrategicPatchSecretRefs: []SecretKeyReference{{Name: "valid", Key: "patch"}, r}}
		}, "spec.machineConfig.strategicPatchSecretRefs[1]"},
	}
	invalid := []struct {
		name  string
		ref   SecretKeyReference
		field string
	}{
		{"empty name", SecretKeyReference{Key: "config.yaml"}, "name"},
		{"uppercase name", SecretKeyReference{Name: "BadName", Key: "config.yaml"}, "name"},
		{"namespace path", SecretKeyReference{Name: "other-namespace/secret", Key: "config.yaml"}, "name"},
		{"filesystem path", SecretKeyReference{Name: "/etc/talos/config", Key: "config.yaml"}, "name"},
		{"template name", SecretKeyReference{Name: "{{ .Machine.Name }}", Key: "config.yaml"}, "name"},
		{"long name", SecretKeyReference{Name: strings.Repeat("a", 254), Key: "config.yaml"}, "name"},
		{"empty key", SecretKeyReference{Name: "valid"}, "key"},
		{"path key", SecretKeyReference{Name: "valid", Key: "config/private.yaml"}, "key"},
		{"whitespace key", SecretKeyReference{Name: "valid", Key: "private key"}, "key"},
		{"long key", SecretKeyReference{Name: "valid", Key: strings.Repeat("a", 254)}, "key"},
		{"reserved dot key", SecretKeyReference{Name: "valid", Key: "."}, "key"},
		{"reserved prefix key", SecretKeyReference{Name: "valid", Key: "..private"}, "key"},
	}
	for _, location := range locations {
		t.Run(location.name, func(t *testing.T) {
			valid := TalosConfigSpec{GenerateType: "worker"}
			location.set(&valid, SecretKeyReference{Name: "tenant.config-1", Key: "Config_v1.yaml"})
			require.Empty(t, ValidateConfigInputs(field.NewPath("spec"), valid))
			for _, test := range invalid {
				t.Run(test.name, func(t *testing.T) {
					spec := TalosConfigSpec{GenerateType: "worker"}
					location.set(&spec, test.ref)
					errs := ValidateConfigInputs(field.NewPath("spec"), spec)
					require.Len(t, errs, 1)
					assert.Equal(t, location.path+"."+test.field, errs[0].Field)
					assert.Equal(t, "", errs[0].BadValue, "error must not echo user input")
				})
			}
		})
	}
}

func TestValidateConfigInputBaselines(t *testing.T) {
	t.Parallel()
	ref := &SecretKeyReference{Name: "baseline", Key: "config"}
	for _, generateType := range []string{"none", "controlplane", "worker"} {
		for _, machine := range []bool{false, true} {
			spec := TalosConfigSpec{GenerateType: generateType, ConfigSecretRef: ref}
			if machine {
				spec.MachineConfig = &MachineConfigOverlay{ConfigSecretRef: &SecretKeyReference{Name: "machine-baseline", Key: "config"}}
			}
			require.Empty(t, ValidateConfigInputs(field.NewPath("spec"), spec))
			spec.Data = "private-baseline-sentinel"
			errs := ValidateConfigInputs(field.NewPath("spec"), spec)
			require.Len(t, errs, 1)
			assert.Equal(t, "spec.data", errs[0].Field)
			assert.NotContains(t, errs.ToAggregate().Error(), spec.Data)
		}
	}
	for _, generateType := range []string{"init", "", "other"} {
		for _, machine := range []bool{false, true} {
			spec := TalosConfigSpec{GenerateType: generateType}
			if machine {
				spec.MachineConfig = &MachineConfigOverlay{ConfigSecretRef: ref}
			} else {
				spec.ConfigSecretRef = ref
			}
			errs := ValidateConfigInputs(field.NewPath("spec"), spec)
			require.Len(t, errs, 1)
			assert.Equal(t, "spec.generateType", errs[0].Field)
		}
	}
	// Legacy templates and public data remain accepted; rendering checks completeness.
	require.Empty(t, ValidateConfigInputs(field.NewPath("spec"), TalosConfigSpec{GenerateType: "none"}))
	require.Empty(t, ValidateConfigInputs(field.NewPath("spec"), TalosConfigSpec{GenerateType: "none", Data: "public config"}))
}

func TestValidateTalosEndpoints(t *testing.T) {
	t.Parallel()
	for _, endpoint := range []TalosEndpoint{
		{Address: "talos.example.com:50000", ServerName: "talos.example.com"},
		{Address: "192.0.2.1:443", ServerName: "talos.example.com", Node: "192.0.2.2"},
		{Address: "[2001:db8::1]:50000", ServerName: "2001:db8::1", Node: "2001:db8::2"},
	} {
		require.Empty(t, ValidateConfigInputs(field.NewPath("spec"), TalosConfigSpec{TalosEndpoint: &endpoint}))
	}
	for _, test := range []struct {
		endpoint TalosEndpoint
		field    string
	}{
		{TalosEndpoint{Address: "host", ServerName: "host"}, "address"},
		{TalosEndpoint{Address: ":50000", ServerName: "host"}, "address"},
		{TalosEndpoint{Address: "host:0", ServerName: "host"}, "address"},
		{TalosEndpoint{Address: "host:65536", ServerName: "host"}, "address"},
		{TalosEndpoint{Address: "host:abc", ServerName: "host"}, "address"},
		{TalosEndpoint{Address: "https://host:50000", ServerName: "host"}, "address"},
		{TalosEndpoint{Address: "2001:db8::1:50000", ServerName: "host"}, "address"},
		{TalosEndpoint{Address: "host:50000"}, "serverName"},
		{TalosEndpoint{Address: "host:50000", ServerName: "host:50000"}, "serverName"},
		{TalosEndpoint{Address: "host:50000", ServerName: "/etc/private-sentinel"}, "serverName"},
		{TalosEndpoint{Address: "host:50000", ServerName: "host", Node: "node1,node2"}, "node"},
		{TalosEndpoint{Address: "host:50000", ServerName: "host", Node: "node1 node2"}, "node"},
	} {
		errs := ValidateConfigInputs(field.NewPath("spec"), TalosConfigSpec{TalosEndpoint: &test.endpoint})
		require.Len(t, errs, 1)
		assert.Equal(t, "spec.talosEndpoint."+test.field, errs[0].Field)
		assert.Equal(t, "", errs[0].BadValue)
	}
}

func TestConfigInputAdmissionValidation(t *testing.T) {
	t.Parallel()
	for _, spec := range []TalosConfigSpec{
		{GenerateType: "worker", ConfigSecretRef: &SecretKeyReference{Name: "other/secret", Key: "config"}},
		{GenerateType: "worker", MachineConfig: &MachineConfigOverlay{StrategicPatchSecretRefs: []SecretKeyReference{{Name: "secret", Key: "private/key"}}}},
		{GenerateType: "worker", Data: "private-baseline-sentinel", MachineConfig: &MachineConfigOverlay{ConfigSecretRef: &SecretKeyReference{Name: "secret", Key: "config"}}},
		{GenerateType: "init", ConfigSecretRef: &SecretKeyReference{Name: "secret", Key: "config"}},
		{GenerateType: "worker", TalosEndpoint: &TalosEndpoint{Address: "host", ServerName: "host"}},
	} {
		config := &TalosConfig{Spec: spec}
		_, err := config.ValidateCreate(context.Background(), config)
		require.Error(t, err)
		assert.NotContains(t, err.Error(), "private-baseline-sentinel")
		template := &TalosConfigTemplate{Spec: TalosConfigTemplateSpec{Template: TalosConfigTemplateResource{Spec: spec}}}
		_, err = template.ValidateCreate(context.Background(), template)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "spec.template.spec")
		assert.NotContains(t, err.Error(), "private-baseline-sentinel")
	}
}
