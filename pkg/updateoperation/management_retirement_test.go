// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"encoding/json"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"testing"
	"time"
)

func deletingManagementAuthority(t *testing.T, c client.Client, a ManagementAuthority) *unstructured.Unstructured {
	t.Helper()
	authority := &unstructured.Unstructured{}
	authority.SetAPIVersion(a.APIVersion)
	authority.SetKind(a.Kind)
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: a.Namespace, Name: a.Name}, authority))
	authority.SetFinalizers([]string{"test.retirement/finalizer"})
	require.NoError(t, c.Update(t.Context(), authority))
	require.NoError(t, c.Delete(t.Context(), authority))
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(authority), authority))
	require.NotNil(t, authority.GetDeletionTimestamp())
	return authority
}
func TestManagementRetirementActivatedVersusObservePauseForeignAndPending(t *testing.T) {
	for _, kind := range []string{"clean", "Observe", "paused", "foreign", "MachineUpdating", "TCUpdating", "InfraUpdating", "PreparedOwner", "ConsumedOwner"} {
		t.Run(kind, func(t *testing.T) {
			c, r := activateManagementFixture(t)
			a := r.Authority
			authority := deletingManagementAuthority(t, c, a)
			m := r.Machines[0].DeepCopy()
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(m), m))
			tc := r.TalosConfigs[0].DeepCopy()
			switch kind {
			case "Observe":
				authority.Object["spec"].(map[string]any)["management"].(map[string]any)["mode"] = "Observe"
				require.NoError(t, c.Update(t.Context(), authority))
			case "paused":
				authority.SetAnnotations(map[string]string{"cluster.x-k8s.io/paused": ""})
				require.NoError(t, c.Update(t.Context(), authority))
			case "foreign":
				m.OwnerReferences[0].UID = "foreign"
				require.NoError(t, c.Update(t.Context(), m))
			case "MachineUpdating":
				m.Annotations = map[string]string{"runtime.cluster.x-k8s.io/pending-hooks": "UpdateMachine"}
				require.NoError(t, c.Update(t.Context(), m))
			case "TCUpdating":
				tc.Annotations = map[string]string{clusterv1.UpdateInProgressAnnotation: ""}
				require.NoError(t, c.Update(t.Context(), tc))
			case "InfraUpdating":
				infra := &unstructured.Unstructured{}
				infra.SetAPIVersion("infrastructure.example.test/v1beta1")
				infra.SetKind("MetalMachine")
				require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: m.Namespace, Name: m.Name}, infra))
				infra.SetAnnotations(map[string]string{clusterv1.UpdateInProgressAnnotation: ""})
				require.NoError(t, c.Update(t.Context(), infra))
			case "PreparedOwner", "ConsumedOwner":
				intent := OwnerPlanIntent{Version: OwnerIntentVersion, Phase: OwnerPrepared, TalosConfig: managementObject(bootstrapv1.GroupVersion.Group, "TalosConfig", tc), Owner: ObjectIdentity{APIGroup: managementGroup(a.APIVersion), Kind: a.Kind, Name: a.Name, UID: a.UID}}
				if kind == "ConsumedOwner" {
					intent.Phase = OwnerConsumed
				}
				data, err := json.Marshal(intent)
				require.NoError(t, err)
				secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: tc.Namespace, Name: "prepared-owner", Annotations: map[string]string{ProvenanceAnnotation: OwnerIntentProvenanceValue}, OwnerReferences: []metav1.OwnerReference{{APIVersion: a.APIVersion, Kind: a.Kind, Name: a.Name, UID: a.UID, Controller: new(true)}}}, Data: map[string][]byte{OwnerIntentKey: data}}
				require.NoError(t, c.Create(t.Context(), secret))
				tc.Annotations = map[string]string{OwnerPlanNameAnnotation: secret.Name, OwnerPlanUIDAnnotation: string(secret.UID)}
				require.NoError(t, c.Update(t.Context(), tc))
			}
			err := ValidateManagementRetirement(t.Context(), c, a)
			if kind == "clean" || kind == "ConsumedOwner" {
				require.NoError(t, err)
				require.Error(t, ValidateManagementActivation(t.Context(), c, a, r.Spec), "retirement never relaxes runtime deletion validator")
			} else {
				require.Error(t, err)
			}
		})
	}
}
func TestRetirementObservedReceiptRequiresConsumption(t *testing.T) {
	c, r := activateManagementFixture(t)
	deletingManagementAuthority(t, c, r.Authority)
	m := r.Machines[0]
	tc := r.TalosConfigs[0]
	now := metav1.Now()
	deadline := metav1.NewTime(now.Add(time.Minute))
	j := &Journal{Version: SchemaVersion, TransactionID: "done", Machine: managementObject("cluster.x-k8s.io", "Machine", m), TalosConfig: managementObject(bootstrapv1.GroupVersion.Group, "TalosConfig", tc), Infrastructure: ObjectIdentity{APIGroup: "infrastructure.example.test", Kind: "MetalMachine", Name: m.Name, UID: "infra"}, NodeName: m.Status.NodeRef.Name, NodeUID: "node-uid", NodeProviderID: m.Spec.ProviderID, Configuration: []byte("private-config"), InputFreshness: []byte("private-freshness"), Phase: Observed, CompletedAt: &now, Steps: []OperationStep{{Kind: Apply, Phase: StepObserved, ApplyOutcome: NoReboot, IssuerID: "issuer", IssuedAt: &now, Deadline: &deadline, IssuedBootID: "boot"}}}
	secret, err := CreateJournal(t.Context(), c, m.Namespace, j)
	require.NoError(t, err)
	receipt := &CompletionReceipt{Version: SchemaVersion, TransactionID: j.TransactionID, MachineUID: j.Machine.UID, TalosConfigUID: j.TalosConfig.UID, InfrastructureUID: j.Infrastructure.UID, InputFreshness: j.InputFreshness, Phase: Observed, CompletedAt: now}
	data, err := json.Marshal(receipt)
	require.NoError(t, err)
	secret.Data[ReceiptKey] = data
	require.NoError(t, c.Update(t.Context(), secret))
	require.Error(t, ValidateManagementRetirement(t.Context(), c, r.Authority), "Observed receipt without core/owner consumption cannot retire")
	secret.Data[ConsumedReceiptKey] = data
	require.NoError(t, c.Update(t.Context(), secret))
	require.NoError(t, ValidateManagementRetirement(t.Context(), c, r.Authority))
}
