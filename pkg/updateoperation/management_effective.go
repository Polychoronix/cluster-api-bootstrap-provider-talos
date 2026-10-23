// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"reflect"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
)

// managementEffectiveInput validates the producer's one canonical effective
// policy against the live authority. It never resolves a second namespace or
// renders independently: the existing configinput renderer consumes the result.
func managementEffectiveInput(request ManagementObservationRequest, authority *unstructured.Unstructured, m *clusterv1.Machine, tc *bootstrapv1.TalosConfig) (*bootstrapv1.TalosConfig, error) {
	if len(request.EffectiveTalosConfigs) == 0 {
		return tc.DeepCopy(), nil
	}
	if len(request.EffectiveTalosConfigs) != len(request.Machines) {
		return nil, managementError("effective input inventory")
	}
	var effective *bootstrapv1.TalosConfig
	for _, candidate := range request.EffectiveTalosConfigs {
		if candidate == nil || candidate.Namespace != request.Authority.Namespace {
			return nil, managementError("effective input namespace")
		}
		if candidate.Name == tc.Name && candidate.UID == tc.UID {
			if effective != nil {
				return nil, managementError("duplicate effective input")
			}
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
		expected = &bootstrapv1.TalosConfigSpec{}
		if runtime.DefaultUnstructuredConverter.FromUnstructured(shared, expected) != nil {
			return nil, managementError("authority shared configuration decode")
		}
		overrides, _, err := unstructured.NestedSlice(authority.Object, "spec", "machineConfigOverrides")
		if err != nil {
			return nil, managementError("authority overrides decode")
		}
		for _, entry := range overrides {
			value, ok := entry.(map[string]any)
			if !ok {
				return nil, managementError("authority override")
			}
			name, _ := value["name"].(string)
			if name != m.Name {
				continue
			}
			uid, _ := value["uid"].(string)
			if uid != string(m.UID) {
				return nil, managementError("authority override UID")
			}
			if config, ok := value["config"].(map[string]any); ok {
				overlay := &bootstrapv1.MachineConfigOverlay{}
				if runtime.DefaultUnstructuredConverter.FromUnstructured(config, overlay) != nil {
					return nil, managementError("authority overlay decode")
				}
				expected.MachineConfig = overlay
			}
			if endpoint, ok := value["talosEndpoint"].(map[string]any); ok {
				target := &bootstrapv1.TalosEndpoint{}
				if runtime.DefaultUnstructuredConverter.FromUnstructured(endpoint, target) != nil {
					return nil, managementError("authority endpoint decode")
				}
				expected.TalosEndpoint = target
			}
		}
	}
	normalized := effective.DeepCopy()
	normalized.Spec.Management = nil
	expected.Management = nil
	if !reflect.DeepEqual(normalized.Spec, *expected) {
		return nil, managementError("effective input differs from live authority policy")
	}
	return normalized, nil
}
