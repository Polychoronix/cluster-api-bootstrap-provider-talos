// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"testing"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestManagementMissingEffectiveTCPInputsCannotApproveLiveChild(t *testing.T) {
	c, request, _ := managementTLSFixture(t, 1)
	tcp := &unstructured.Unstructured{}
	tcp.SetAPIVersion(request.Authority.APIVersion)
	tcp.SetKind(request.Authority.Kind)
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: request.Authority.Namespace, Name: request.Authority.Name}, tcp))
	shared := tcp.Object["spec"].(map[string]any)["controlPlaneConfig"].(map[string]any)["controlplane"].(map[string]any)
	shared["strategicPatchSecretRefs"] = []any{map[string]any{"name": "unapplied-policy", "key": "patch"}}
	require.NoError(t, c.Create(t.Context(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: request.Authority.Namespace, Name: "unapplied-policy"}, Data: map[string][]byte{"patch": []byte("machine:\n  env:\n    PRIVATE_UNAPPLIED_POLICY: desired\n")}}))
	require.NoError(t, c.Update(t.Context(), tcp))
	request.EffectiveTalosConfigs = nil
	observation, err := ObserveManagement(t.Context(), c, request)
	require.ErrorContains(t, err, "effective TCP input required")
	require.Nil(t, observation)
	require.NotContains(t, err.Error(), "PRIVATE_UNAPPLIED_POLICY")
	_, err = ReadManagementStatus(t.Context(), c, request.Authority)
	require.ErrorIs(t, err, ErrManagementRecordNotFound)
	request.Spec.Mode = bootstrapv1.ManagementModeManaged
	request.Spec.Approval = &bootstrapv1.ObservationApproval{Revision: request.Spec.InventoryRevision, ObservationID: "unobserved"}
	_, err = ActivateManagement(t.Context(), c, c, request)
	require.ErrorContains(t, err, "effective TCP input required")
	for i, actual := range request.Machines {
		live := actual.DeepCopy()
		require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(actual), live))
		require.Equal(t, actual.Spec, live.Spec)
		require.Equal(t, actual.OwnerReferences, live.OwnerReferences)
		tc := request.TalosConfigs[i].DeepCopy()
		require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(tc), tc))
		require.Equal(t, request.TalosConfigs[i].Spec, tc.Spec)
	}
}

func TestManagementUntypedOverrideValidationRejectsDuplicatesAndConflicts(t *testing.T) {
	for _, invalid := range []string{"duplicate-name", "duplicate-uid", "shared-overlay-conflict", "malformed-overlay", "malformed-endpoint", "empty-override", "invalid-shared-reference", "invalid-overlay-reference"} {
		t.Run(invalid, func(t *testing.T) {
			c, request, _ := managementTLSFixture(t, 1)
			tcp := &unstructured.Unstructured{}
			tcp.SetAPIVersion(request.Authority.APIVersion)
			tcp.SetKind(request.Authority.Kind)
			require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: request.Authority.Namespace, Name: request.Authority.Name}, tcp))
			policy := tcp.Object["spec"].(map[string]any)
			shared := policy["controlPlaneConfig"].(map[string]any)["controlplane"].(map[string]any)
			overrides := policy["machineConfigOverrides"].([]any)
			override := overrides[0].(map[string]any)
			switch invalid {
			case "duplicate-name":
				policy["machineConfigOverrides"] = append(overrides, override)
			case "duplicate-uid":
				duplicate := map[string]any{"name": "other-node", "uid": override["uid"], "talosEndpoint": override["talosEndpoint"]}
				policy["machineConfigOverrides"] = append(overrides, duplicate)
			case "shared-overlay-conflict":
				shared["machineConfig"] = map[string]any{"configRevision": "shared-slot"}
			case "malformed-overlay":
				override["config"] = "PRIVATE_BAD_OVERLAY"
			case "malformed-endpoint":
				override["talosEndpoint"] = int64(50000)
			case "empty-override":
				delete(override, "config")
				delete(override, "talosEndpoint")
			case "invalid-shared-reference":
				shared["configSecretRef"] = map[string]any{"name": "outside/baseline", "key": "config"}
			case "invalid-overlay-reference":
				override["config"].(map[string]any)["strategicPatchSecretRefs"] = []any{map[string]any{"name": "outside/patch", "key": "patch"}}
			}
			require.NoError(t, c.Update(t.Context(), tcp))
			observation, err := ObserveManagement(t.Context(), c, request)
			require.Error(t, err)
			require.Nil(t, observation)
			require.NotContains(t, err.Error(), "PRIVATE_BAD_OVERLAY")
			_, err = ReadManagementStatus(t.Context(), c, request.Authority)
			require.ErrorIs(t, err, ErrManagementRecordNotFound)
		})
	}
}

func TestManagementStandaloneEffectiveInputDefaultsToOwnLiveSpec(t *testing.T) {
	tc := &bootstrapv1.TalosConfig{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "worker", UID: "tc"}, Spec: bootstrapv1.TalosConfigSpec{GenerateType: "worker"}}
	effective, err := managementEffectiveInput(ManagementObservationRequest{Authority: ManagementAuthority{Kind: "TalosConfig"}}, nil, nil, tc)
	require.NoError(t, err)
	require.Equal(t, tc.Spec, effective.Spec)
	require.NotSame(t, tc, effective)
}
