// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"testing"
)

func setEffectivePolicy(t *testing.T, c client.Client, reader client.Reader, r *ManagementObservationRequest, sharedPatch bool, nodePatch bool) {
	t.Helper()
	tcp := &unstructured.Unstructured{}
	tcp.SetAPIVersion(r.Authority.APIVersion)
	tcp.SetKind(r.Authority.Kind)
	require.NoError(t, reader.Get(t.Context(), client.ObjectKey{Namespace: r.Authority.Namespace, Name: r.Authority.Name}, tcp))
	shared := r.TalosConfigs[0].Spec.DeepCopy()
	shared.Management = nil
	if sharedPatch {
		shared.StrategicPatchSecretRefs = []bootstrapv1.SecretKeyReference{{Name: "shared-policy", Key: "patch"}}
	}
	sharedMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(shared)
	require.NoError(t, err)
	tcp.Object["spec"].(map[string]any)["controlPlaneConfig"] = map[string]any{"controlplane": sharedMap}
	overrides := []any{}
	r.EffectiveTalosConfigs = nil
	for i, tc := range r.TalosConfigs {
		overlay := &bootstrapv1.MachineConfigOverlay{ConfigSecretRef: tc.Spec.ConfigSecretRef}
		if nodePatch && i == 0 {
			overlay.StrategicPatchSecretRefs = []bootstrapv1.SecretKeyReference{{Name: "node-policy", Key: "patch"}}
		}
		effective := tc.DeepCopy()
		effective.Spec = *shared.DeepCopy()
		effective.Spec.MachineConfig = overlay
		effective.Spec.TalosEndpoint = tc.Spec.TalosEndpoint
		effective.Spec.Management = nil
		r.EffectiveTalosConfigs = append(r.EffectiveTalosConfigs, effective)
		configMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(overlay)
		require.NoError(t, err)
		endpointMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(tc.Spec.TalosEndpoint)
		require.NoError(t, err)
		overrides = append(overrides, map[string]any{"name": r.Machines[i].Name, "uid": string(r.Machines[i].UID), "config": configMap, "talosEndpoint": endpointMap})
	}
	tcp.Object["spec"].(map[string]any)["machineConfigOverrides"] = overrides
	require.NoError(t, c.Update(t.Context(), tcp))
}
func TestEffectivePolicySemanticsNotLiveTCStructuralEquality(t *testing.T) {
	c, r, _ := managementTLSFixture(t, 2)
	setEffectivePolicy(t, c, c, &r, false, false)
	require.NotEqual(t, r.TalosConfigs[0].Spec, r.EffectiveTalosConfigs[0].Spec)
	observed, err := ObserveManagement(t.Context(), c, r)
	require.NoError(t, err)
	require.True(t, observed.Summary().InventoryVerified)
	require.NotEqual(t, string(observed.safety.Members[0].ConfigSpec), string(observed.safety.Members[0].EffectiveSpec))
}
func TestEffectiveSharedOrOverridePrivatePolicyMismatchNoWrites(t *testing.T) {
	for _, node := range []bool{false, true} {
		t.Run(map[bool]string{false: "shared", true: "override"}[node], func(t *testing.T) {
			c, r, _ := managementTLSFixture(t, 1)
			name := "shared-policy"
			if node {
				name = "node-policy"
			}
			require.NoError(t, c.Create(t.Context(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: r.Authority.Namespace, Name: name}, Data: map[string][]byte{"patch": []byte("machine:\n  env:\n    PRIVATE_EFFECTIVE_POLICY: mismatch\n")}}))
			setEffectivePolicy(t, c, c, &r, !node, node)
			observed, err := ObserveManagement(t.Context(), c, r)
			require.Error(t, err)
			require.Nil(t, observed)
			require.NotContains(t, err.Error(), "PRIVATE_EFFECTIVE_POLICY")
			_, err = ReadManagementStatus(t.Context(), c, r.Authority)
			require.ErrorIs(t, err, ErrManagementRecordNotFound)
		})
	}
}
func TestEffectiveSecretRotationInvalidatesInitialApproval(t *testing.T) {
	c, r, _ := managementTLSFixture(t, 1)
	require.NoError(t, c.Create(t.Context(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: r.Authority.Namespace, Name: "shared-policy"}, Data: map[string][]byte{"patch": []byte("machine:\n  type: controlplane\n")}}))
	setEffectivePolicy(t, c, c, &r, true, false)
	o, err := ObserveManagement(t.Context(), c, r)
	require.NoError(t, err)
	status, err := RecordManagementObservation(t.Context(), c, c, o)
	require.NoError(t, err)
	r.Spec.Mode = bootstrapv1.ManagementModeManaged
	r.Spec.Approval = &bootstrapv1.ObservationApproval{Revision: r.Spec.InventoryRevision, ObservationID: status.Observation.ID}
	tcp := &unstructured.Unstructured{}
	tcp.SetAPIVersion(r.Authority.APIVersion)
	tcp.SetKind(r.Authority.Kind)
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: r.Authority.Namespace, Name: r.Authority.Name}, tcp))
	managementMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(r.Spec)
	require.NoError(t, err)
	tcp.Object["spec"].(map[string]any)["management"] = managementMap
	require.NoError(t, c.Update(t.Context(), tcp))
	patch := &corev1.Secret{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: r.Authority.Namespace, Name: "shared-policy"}, patch))
	patch.Data["patch"] = append(patch.Data["patch"], []byte("\n# rotate unchanged semantics\n")...)
	require.NoError(t, c.Update(t.Context(), patch))
	_, err = PrepareManagementAssociation(t.Context(), c, c, r)
	require.Error(t, err)
}
