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

// ControlPlaneMemberLabel is the Machine label a TalosControlPlane sets on the Machine it
// created for a declared `spec.controlPlaneConfig.members` entry. It mirrors
// cluster-api-control-plane-provider-talos api/v1beta1.MemberLabel, which this module cannot
// import (the control-plane provider depends on this one).
const ControlPlaneMemberLabel = "controlplane.cluster.x-k8s.io/member"

// controlPlaneMemberPatch mirrors cluster-api-control-plane-provider-talos
// api/v1beta1.ControlPlaneMemberPatch field for field. Keep both in lockstep; the contract
// test in management_effective_test.go pins the merge result.
type controlPlaneMemberPatch struct {
	ConfigPatches            []bootstrapv1.ConfigPatches      `json:"configPatches,omitempty"`
	StrategicPatches         []string                         `json:"strategicPatches,omitempty"`
	StrategicPatchSecretRefs []bootstrapv1.SecretKeyReference `json:"strategicPatchSecretRefs,omitempty"`
	TalosEndpoint            *bootstrapv1.TalosEndpoint       `json:"talosEndpoint,omitempty"`
}

type controlPlaneMember struct {
	Name             string                   `json:"name"`
	TalosConfigPatch *controlPlaneMemberPatch `json:"talosConfigPatch,omitempty"`
}

// managementAuthorityPolicy derives the TalosConfig spec a TalosControlPlane authority
// intends for one Machine from the live authority object: the shared `controlplane` spec
// plus, when the authority declares members, the overlay of the member the Machine occupies
// (its `controlplane.cluster.x-k8s.io/member` label, else its name). The merge mirrors
// cluster-api-control-plane-provider-talos api/v1beta1.(*TalosControlPlaneSpec).EffectiveTalosConfigSpec
// exactly: configPatches, strategicPatches and strategicPatchSecretRefs of the overlay are
// appended after the base's, talosEndpoint is overridden when set. A Machine that belongs to
// no declared member while members are declared has no derivable policy.
func managementAuthorityPolicy(authority *unstructured.Unstructured, m *clusterv1.Machine) (*bootstrapv1.TalosConfigSpec, error) {
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
	raw, found, err := unstructured.NestedSlice(authority.Object, "spec", "controlPlaneConfig", "members")
	if err != nil {
		return nil, managementError("live authority members")
	}
	if !found || len(raw) == 0 {
		return base, nil
	}
	name := m.Labels[ControlPlaneMemberLabel]
	if name == "" {
		name = m.Name
	}
	for _, entry := range raw {
		object, ok := entry.(map[string]any)
		if !ok {
			return nil, managementError("live authority members decode")
		}
		member := controlPlaneMember{}
		if runtime.DefaultUnstructuredConverter.FromUnstructured(object, &member) != nil {
			return nil, managementError("live authority members decode")
		}
		if member.Name != name {
			continue
		}
		return mergeControlPlaneMember(base, member.TalosConfigPatch), nil
	}
	return nil, managementError("Machine outside declared members")
}

// mergeControlPlaneMember is the overlay merge; see managementAuthorityPolicy.
func mergeControlPlaneMember(base *bootstrapv1.TalosConfigSpec, patch *controlPlaneMemberPatch) *bootstrapv1.TalosConfigSpec {
	result := base.DeepCopy()
	if patch == nil {
		return result
	}
	result.ConfigPatches = appendCopied(result.ConfigPatches, patch.ConfigPatches)
	result.StrategicPatches = appendCopied(result.StrategicPatches, patch.StrategicPatches)
	result.StrategicPatchSecretRefs = appendCopied(result.StrategicPatchSecretRefs, patch.StrategicPatchSecretRefs)
	if patch.TalosEndpoint != nil {
		endpoint := *patch.TalosEndpoint
		result.TalosEndpoint = &endpoint
	}
	return result
}

func appendCopied[T any](base, extra []T) []T {
	if len(extra) == 0 {
		return base
	}
	out := make([]T, 0, len(base)+len(extra))
	out = append(out, base...)
	return append(out, extra...)
}

// managementEffectiveInput validates the producer's one canonical effective
// policy against the live authority. TCP shared intent plus the Machine's member
// overlay is required; the exact native child's Talos transport remains
// per-Machine. Standalone TC uses itself. The existing configinput renderer is
// the only renderer of this validated input.
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
		policy, err := managementAuthorityPolicy(authority, m)
		if err != nil {
			return nil, err
		}
		expected = policy
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
