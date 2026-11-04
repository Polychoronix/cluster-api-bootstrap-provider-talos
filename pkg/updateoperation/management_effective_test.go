// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
)

// membersAuthority is a TalosControlPlane shaped like cluster-api-control-plane-provider-talos
// renders it with `spec.controlPlaneConfig.members`: a shared base plus one overlay per host.
func membersAuthority(t *testing.T, base bootstrapv1.TalosConfigSpec) *unstructured.Unstructured {
	t.Helper()
	shared, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&base)
	require.NoError(t, err)
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "controlplane.cluster.x-k8s.io/v1beta1",
		"kind":       "TalosControlPlane",
		"metadata":   map[string]any{"namespace": "tenant", "name": "controlplane", "uid": "authority-uid"},
		"spec": map[string]any{
			"manage": true,
			"controlPlaneConfig": map[string]any{
				"controlplane": shared,
				"members": []any{
					map[string]any{
						"name":                   "host-a",
						"infrastructureSelector": map[string]any{"caph.craci.dev/node": "host-a"},
						"talosConfigPatch": map[string]any{
							"strategicPatches":         []any{`{"apiVersion":"v1alpha1","kind":"HostnameConfig","hostname":"host-a"}`},
							"strategicPatchSecretRefs": []any{map[string]any{"name": "host-a-tailscale", "key": "patch"}},
							"talosEndpoint":            map[string]any{"address": "host-a-talos-access:50000", "serverName": "host-a.example.test"},
						},
					},
					map[string]any{
						"name": "host-b",
						"talosConfigPatch": map[string]any{
							"strategicPatches": []any{`{"apiVersion":"v1alpha1","kind":"HostnameConfig","hostname":"host-b"}`},
						},
					},
				},
			},
		},
	}}
}

func membersBase() bootstrapv1.TalosConfigSpec {
	return bootstrapv1.TalosConfigSpec{
		GenerateType:             "controlplane",
		TalosVersion:             "v1.14.2",
		StrategicPatches:         []string{`{"apiVersion":"v1alpha1","kind":"KubeProxyConfig","enabled":false}`},
		StrategicPatchSecretRefs: []bootstrapv1.SecretKeyReference{{Name: "registry-auth", Key: "patch"}},
		Manage:                   true,
	}
}

// cacpptEffectiveSpec is the merge cluster-api-control-plane-provider-talos performs in
// api/v1beta1.(*TalosControlPlaneSpec).EffectiveTalosConfigSpec, spelled out so a drift
// between the two implementations fails here rather than on a live control plane.
func cacpptEffectiveSpec(base bootstrapv1.TalosConfigSpec, patches []string, refs []bootstrapv1.SecretKeyReference, endpoint *bootstrapv1.TalosEndpoint) bootstrapv1.TalosConfigSpec {
	result := *base.DeepCopy()
	result.StrategicPatches = append(append([]string{}, result.StrategicPatches...), patches...)
	result.StrategicPatchSecretRefs = append(append([]bootstrapv1.SecretKeyReference{}, result.StrategicPatchSecretRefs...), refs...)
	if endpoint != nil {
		e := *endpoint
		result.TalosEndpoint = &e
	}
	return result
}

func TestAuthorityPolicyMirrorsControlPlaneMemberMerge(t *testing.T) {
	base := membersBase()
	authority := membersAuthority(t, base)
	endpoint := &bootstrapv1.TalosEndpoint{Address: "host-a-talos-access:50000", ServerName: "host-a.example.test"}
	want := cacpptEffectiveSpec(base,
		[]string{`{"apiVersion":"v1alpha1","kind":"HostnameConfig","hostname":"host-a"}`},
		[]bootstrapv1.SecretKeyReference{{Name: "host-a-tailscale", Key: "patch"}}, endpoint)

	labelled := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Name: "cluster-g8fjk", Labels: map[string]string{ControlPlaneMemberLabel: "host-a"}}}
	got, err := managementAuthorityPolicy(authority, labelled)
	require.NoError(t, err)
	require.Equal(t, want, *got, "member resolved through the member label")

	named := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Name: "host-b"}}
	got, err = managementAuthorityPolicy(authority, named)
	require.NoError(t, err)
	require.Equal(t, cacpptEffectiveSpec(base, []string{`{"apiVersion":"v1alpha1","kind":"HostnameConfig","hostname":"host-b"}`}, nil, nil), *got, "member resolved through the Machine name; base refs and endpoint untouched")

	_, err = managementAuthorityPolicy(authority, &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Name: "cluster-zzzzz"}})
	require.ErrorContains(t, err, "Machine outside declared members")

	plain := membersAuthority(t, base)
	unstructured.RemoveNestedField(plain.Object, "spec", "controlPlaneConfig", "members")
	got, err = managementAuthorityPolicy(plain, labelled)
	require.NoError(t, err)
	require.Equal(t, base, *got, "no members declared: the shared spec is the policy")
}

func TestEffectiveInputAcceptsMemberOverlay(t *testing.T) {
	base := membersBase()
	authority := membersAuthority(t, base)
	m := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "cluster-g8fjk", Labels: map[string]string{ControlPlaneMemberLabel: "host-a"}}}
	live := &bootstrapv1.TalosConfig{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "cluster-g8fjk", UID: "tc-uid"}}
	live.Spec = base
	live.Spec.TalosEndpoint = &bootstrapv1.TalosEndpoint{Address: "host-a-talos-access:50000", ServerName: "host-a.example.test"}
	effective := live.DeepCopy()
	effective.Spec = cacpptEffectiveSpec(base,
		[]string{`{"apiVersion":"v1alpha1","kind":"HostnameConfig","hostname":"host-a"}`},
		[]bootstrapv1.SecretKeyReference{{Name: "host-a-tailscale", Key: "patch"}}, live.Spec.TalosEndpoint)
	request := ManagementObservationRequest{
		Authority:             ManagementAuthority{Namespace: "tenant", APIVersion: "controlplane.cluster.x-k8s.io/v1beta1", Kind: "TalosControlPlane", Name: "controlplane", UID: "authority-uid"},
		Machines:              []*clusterv1.Machine{m},
		TalosConfigs:          []*bootstrapv1.TalosConfig{live},
		EffectiveTalosConfigs: []*bootstrapv1.TalosConfig{effective},
	}

	normalized, err := managementEffectiveInput(request, authority, m, live)
	require.NoError(t, err, "the member overlay is the live authority policy for this Machine")
	require.False(t, normalized.Spec.Manage)

	// The shared base alone (what a pre-members check expected) is no longer the policy for
	// a member Machine.
	baseOnly := live.DeepCopy()
	request.EffectiveTalosConfigs = []*bootstrapv1.TalosConfig{baseOnly}
	_, err = managementEffectiveInput(request, authority, m, live)
	require.ErrorContains(t, err, "effective input differs from live authority policy")
}
