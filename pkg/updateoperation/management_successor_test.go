// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"encoding/json"
	"testing"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestManagementNativeSuccessorRequiresRealAcceptedSeed(t *testing.T) {
	c, request, _ := managementTLSFixture(t, 1)
	o, e := ObserveManagement(t.Context(), c, request)
	require.NoError(t, e)
	status, e := RecordManagementObservation(t.Context(), c, c, o)
	require.NoError(t, e)
	request.Spec.Mode = bootstrapv1.ManagementModeManaged
	request.Spec.Approval = &bootstrapv1.ObservationApproval{Revision: request.Spec.InventoryRevision, ObservationID: status.Observation.ID}
	tcp := &unstructured.Unstructured{}
	tcp.SetAPIVersion(request.Authority.APIVersion)
	tcp.SetKind(request.Authority.Kind)
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: "tenant", Name: "controlplane"}, tcp))
	raw, e := runtime.DefaultUnstructuredConverter.ToUnstructured(request.Spec)
	require.NoError(t, e)
	tcp.Object["spec"].(map[string]any)["management"] = raw
	require.NoError(t, c.Update(t.Context(), tcp))
	_, e = associateAll(t, c, c, request)
	require.NoError(t, e)
	m := request.Machines[0].DeepCopy()
	m.Name = "native"
	m.UID = ""
	m.ResourceVersion = ""
	m.Spec.InfrastructureRef.Name = "native"
	m.Spec.Bootstrap.ConfigRef.Name = "native"
	seedName := "native-seed"
	m.Spec.Bootstrap.DataSecretName = &seedName
	m.OwnerReferences = []metav1.OwnerReference{{APIVersion: request.Authority.APIVersion, Kind: request.Authority.Kind, Name: request.Authority.Name, UID: request.Authority.UID, Controller: new(true)}}
	require.NoError(t, c.Create(t.Context(), m))
	owner := metav1.OwnerReference{APIVersion: "cluster.x-k8s.io/v1beta2", Kind: "Machine", Name: m.Name, UID: m.UID, Controller: new(true)}
	tc := request.TalosConfigs[0].DeepCopy()
	tc.Name = "native"
	tc.UID = ""
	tc.ResourceVersion = ""
	tc.OwnerReferences = []metav1.OwnerReference{owner}
	tc.Annotations = map[string]string{InitialControlPlaneNameAnnotation: request.Authority.Name, InitialControlPlaneUIDAnnotation: string(request.Authority.UID)}
	require.NoError(t, c.Create(t.Context(), tc))
	infra := &unstructured.Unstructured{}
	infra.SetAPIVersion("infrastructure.example.test/v1beta1")
	infra.SetKind("MetalMachine")
	infra.SetNamespace("tenant")
	infra.SetName("native")
	infra.SetOwnerReferences([]metav1.OwnerReference{owner})
	infra.Object["spec"] = map[string]any{"providerID": m.Spec.ProviderID}
	require.NoError(t, c.Create(t.Context(), infra))
	seed := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: seedName, OwnerReferences: []metav1.OwnerReference{{APIVersion: bootstrapv1.GroupVersion.String(), Kind: "TalosConfig", Name: tc.Name, UID: tc.UID, Controller: new(true)}}}, Data: map[string][]byte{"value": []byte("original")}}
	require.NoError(t, c.Create(t.Context(), seed))
	require.Error(t, ValidateManagedMachine(t.Context(), c, m, tc), "lineage markers alone cannot create consent")
	accepted, e := json.Marshal(AcceptedIntent{MachineUID: m.UID, TalosConfigUID: tc.UID})
	require.NoError(t, e)
	seed.Data[AcceptedIntentKey] = accepted
	require.NoError(t, c.Update(t.Context(), seed))
	require.NoError(t, ValidateManagedMachine(t.Context(), c, m, tc))
}
