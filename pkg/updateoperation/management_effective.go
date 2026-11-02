// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"reflect"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
)

// managementEffectiveInput validates the producer's one canonical effective
// policy against the live authority. TCP shared intent is required; the exact
// native child's Talos transport remains per-Machine. Standalone TC uses itself.
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
		if len(bootstrapv1.ValidateConfigInputs(field.NewPath("controlPlaneConfig", "controlplane"), *base)) != 0 {
			return nil, managementError("authority shared input validation")
		}
		expected = base.DeepCopy()
		expected.TalosEndpoint = tc.Spec.TalosEndpoint
	}
	normalized := effective.DeepCopy()
	normalized.Spec.Manage = false
	expected.Manage = false
	if len(bootstrapv1.ValidateConfigInputs(field.NewPath("effectiveTalosConfigs"), normalized.Spec)) != 0 {
		return nil, managementError("effective input validation")
	}
	if !reflect.DeepEqual(normalized.Spec, *expected) {
		return nil, managementError("effective input differs from live authority policy")
	}
	return normalized, nil
}
