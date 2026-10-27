// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"reflect"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
)

// managementEffectiveInput validates the producer's one canonical effective
// policy against the live authority. TCP input is required: a live child alone
// cannot prove desired shared/override policy. Standalone TC defaults to itself.
// The existing configinput renderer is the only renderer of this validated input.
func managementEffectiveInput(request ManagementObservationRequest, authority *unstructured.Unstructured, m *clusterv1.Machine, tc *bootstrapv1.TalosConfig) (*bootstrapv1.TalosConfig, error) {
	if len(request.EffectiveTalosConfigs) == 0 {
		if request.Authority.Kind == "TalosControlPlane" {
			return nil, managementError("effective TCP input required")
		}
		return tc.DeepCopy(), nil
	}
	if len(request.EffectiveTalosConfigs) != len(request.Machines) {
		return nil, managementError("effective input inventory")
	}
	var effective *bootstrapv1.TalosConfig
	seen := map[string]bool{}
	for _, candidate := range request.EffectiveTalosConfigs {
		if candidate == nil || candidate.Namespace != request.Authority.Namespace || seen[candidate.Name] {
			return nil, managementError("effective input namespace or duplicate identity")
		}
		seen[candidate.Name] = true
		associated := false
		for _, actual := range request.TalosConfigs {
			if actual != nil && actual.Name == candidate.Name && actual.Namespace == candidate.Namespace && actual.UID == candidate.UID {
				associated = true
			}
		}
		if !associated {
			return nil, managementError("effective input identity")
		}
		if candidate.Name == tc.Name && candidate.UID == tc.UID {
			effective = candidate
		}
	}
	if effective == nil {
		return nil, managementError("effective input identity")
	}
	expected := tc.Spec.DeepCopy()
	if request.Authority.Kind == "TalosControlPlane" {
		shared, found, err := unstructured.NestedMap(authority.Object, "spec", "controlPlaneConfig", "controlplane")
		if err != nil || !found {
			return nil, managementError("live authority shared configuration")
		}
		base := &bootstrapv1.TalosConfigSpec{}
		if runtime.DefaultUnstructuredConverter.FromUnstructured(shared, base) != nil {
			return nil, managementError("authority shared configuration decode")
		}
		if base.Management != nil || len(bootstrapv1.ValidateConfigInputs(field.NewPath("controlPlaneConfig", "controlplane"), *base)) != 0 {
			return nil, managementError("authority shared input validation")
		}
		expected = base.DeepCopy()
		overrides, _, err := unstructured.NestedSlice(authority.Object, "spec", "machineConfigOverrides")
		if err != nil {
			return nil, managementError("authority overrides decode")
		}
		inventory := &bootstrapv1.ManagementSpec{Mode: bootstrapv1.ManagementModeObserve, InventoryRevision: request.Spec.InventoryRevision}
		for _, entry := range overrides {
			value, ok := entry.(map[string]any)
			if !ok {
				return nil, managementError("authority override")
			}
			name, nameOK := value["name"].(string)
			uid, uidOK := value["uid"].(string)
			if !nameOK || !uidOK {
				return nil, managementError("authority override identity")
			}
			inventory.Machines = append(inventory.Machines, bootstrapv1.MachineIdentityReference{Name: name, UID: types.UID(uid)})
			declared := false
			for _, pin := range request.Spec.Machines {
				if pin.Name == name && string(pin.UID) == uid {
					declared = true
				}
			}
			if !declared {
				return nil, managementError("authority override outside inventory")
			}
			composed := base.DeepCopy()
			hasConfig, hasEndpoint := false, false
			if raw, exists := value["config"]; exists && raw != nil {
				config, ok := raw.(map[string]any)
				if !ok {
					return nil, managementError("authority overlay decode")
				}
				if base.MachineConfig != nil {
					return nil, managementError("authority overlay conflicts with shared MachineConfig")
				}
				overlay := &bootstrapv1.MachineConfigOverlay{}
				if runtime.DefaultUnstructuredConverter.FromUnstructured(config, overlay) != nil {
					return nil, managementError("authority overlay decode")
				}
				composed.MachineConfig = overlay
				hasConfig = true
			}
			if raw, exists := value["talosEndpoint"]; exists && raw != nil {
				endpoint, ok := raw.(map[string]any)
				if !ok {
					return nil, managementError("authority endpoint decode")
				}
				target := &bootstrapv1.TalosEndpoint{}
				if runtime.DefaultUnstructuredConverter.FromUnstructured(endpoint, target) != nil {
					return nil, managementError("authority endpoint decode")
				}
				composed.TalosEndpoint = target
				hasEndpoint = true
			}
			if !hasConfig && !hasEndpoint || len(bootstrapv1.ValidateConfigInputs(field.NewPath("machineConfigOverrides"), *composed)) != 0 {
				return nil, managementError("authority effective override validation")
			}
			if name == m.Name {
				if uid != string(m.UID) {
					return nil, managementError("authority override UID")
				}
				expected = composed
			}
		}
		if len(overrides) != 0 && len(bootstrapv1.ValidateManagementSpec(field.NewPath("machineConfigOverrides"), inventory)) != 0 {
			return nil, managementError("authority override identity validation")
		}
	}
	normalized := effective.DeepCopy()
	normalized.Spec.Management = nil
	expected.Management = nil
	if len(bootstrapv1.ValidateConfigInputs(field.NewPath("effectiveTalosConfigs"), normalized.Spec)) != 0 {
		return nil, managementError("effective input validation")
	}
	if !reflect.DeepEqual(normalized.Spec, *expected) {
		return nil, managementError("effective input differs from live authority policy")
	}
	return normalized, nil
}
