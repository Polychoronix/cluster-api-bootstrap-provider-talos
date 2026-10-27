// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"testing"
)

func TestManagementLiveAuthorityPolicyChangesInvalidateObservation(t *testing.T) {
	for _, policy := range []string{"version", "replicas", "machineTemplate", "controlPlaneConfig", "machineConfigOverrides"} {
		t.Run(policy, func(t *testing.T) {
			c, r, _ := managementTLSFixture(t, 1)
			o, err := ObserveManagement(t.Context(), c, r)
			require.NoError(t, err)
			first, err := RecordManagementObservation(t.Context(), c, c, o)
			require.NoError(t, err)
			tcp := &unstructured.Unstructured{}
			tcp.SetAPIVersion(r.Authority.APIVersion)
			tcp.SetKind(r.Authority.Kind)
			require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: r.Authority.Namespace, Name: r.Authority.Name}, tcp))
			spec := tcp.Object["spec"].(map[string]any)
			switch policy {
			case "version":
				spec[policy] = "v1.38.0"
			case "replicas":
				spec[policy] = int64(5)
			case "machineTemplate":
				spec[policy] = map[string]any{"spec": map[string]any{"infrastructureRef": map[string]any{"name": "different-native-template", "apiGroup": "native.example", "kind": "NativeMachineTemplate"}}}
			case "controlPlaneConfig":
				spec[policy] = map[string]any{"controlplane": map[string]any{"configSecretRef": map[string]any{"name": "different-baseline", "key": "config"}}}
			case "machineConfigOverrides":
				spec[policy] = []any{map[string]any{"name": "node-0", "uid": "machine-0", "talosEndpoint": map[string]any{"address": "different.example:50000", "serverName": "different.example"}}}
			}
			require.NoError(t, c.Update(t.Context(), tcp))
			next, err := ObserveManagement(t.Context(), c, r)
			if policy == "controlPlaneConfig" || policy == "machineConfigOverrides" {
				require.Error(t, err, "changed desired configuration cannot silently observe unchanged live TC")
				require.Nil(t, next)
				retained, lookupErr := ReadManagementStatus(t.Context(), c, r.Authority)
				require.NoError(t, lookupErr)
				require.Equal(t, first.Observation.ID, retained.Observation.ID)
				return
			}
			require.NoError(t, err)
			second, err := RecordManagementObservation(t.Context(), c, c, next)
			require.NoError(t, err)
			require.NotEqual(t, first.Observation.ID, second.Observation.ID)
		})
	}
}
func TestManagementAssociationRejectsChangedAuthorityPolicy(t *testing.T) {
	c, r := approveManagementFixture(t, 1)
	_, err := PrepareManagementAssociation(t.Context(), c, c, r)
	require.NoError(t, err)
	tcp := &unstructured.Unstructured{}
	tcp.SetAPIVersion(r.Authority.APIVersion)
	tcp.SetKind(r.Authority.Kind)
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: r.Authority.Namespace, Name: r.Authority.Name}, tcp))
	tcp.Object["spec"].(map[string]any)["version"] = "v1.38.0"
	require.NoError(t, c.Update(t.Context(), tcp))
	_, err = PrepareManagementAssociation(t.Context(), c, c, r)
	require.Error(t, err)
}

func TestManagementObservationRejectsStaleExplicitConfigSpec(t *testing.T) {
	c, request, _ := managementTLSFixture(t, 1)
	stale := request.TalosConfigs[0].DeepCopy()
	stale.Spec.ConfigRevision = "stale-request"
	request.TalosConfigs[0] = stale
	_, err := ObserveManagement(t.Context(), c, request)
	require.Error(t, err)
}
