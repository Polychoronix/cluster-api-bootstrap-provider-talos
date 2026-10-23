// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"testing"
)

func approveManagementFixture(t *testing.T, count int) (*assigningClient, ManagementObservationRequest) {
	t.Helper()
	c, request, _ := managementTLSFixture(t, count)
	o, err := ObserveManagement(t.Context(), c, request)
	require.NoError(t, err)
	status, err := RecordManagementObservation(t.Context(), c, c, o)
	require.NoError(t, err)
	request.Spec.Mode = bootstrapv1.ManagementModeManaged
	request.Spec.Approval = &bootstrapv1.ObservationApproval{Revision: request.Spec.InventoryRevision, ObservationID: status.Observation.ID}
	tcp := &unstructured.Unstructured{}
	tcp.SetAPIVersion(request.Authority.APIVersion)
	tcp.SetKind(request.Authority.Kind)
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: request.Authority.Namespace, Name: request.Authority.Name}, tcp))
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(request.Spec)
	require.NoError(t, err)
	tcp.Object["spec"] = map[string]any{"management": raw}
	require.NoError(t, c.Update(t.Context(), tcp))
	return c, request
}
func associateAll(t *testing.T, c client.Client, reader client.Reader, request ManagementObservationRequest) (*bootstrapv1.ManagementStatus, error) {
	t.Helper()
	prepared, err := PrepareManagementAssociation(t.Context(), c, reader, request)
	if err != nil {
		return nil, err
	}
	for _, m := range request.Machines {
		live := m.DeepCopy()
		if err := reader.Get(t.Context(), client.ObjectKeyFromObject(m), live); err != nil {
			return nil, err
		}
		live.OwnerReferences = prepared.DesiredOwners(m.UID)
		if err := c.Update(t.Context(), live); err != nil {
			return nil, err
		}
	}
	return CompleteManagementAssociation(t.Context(), c, reader, request, prepared)
}
func activateManagementFixture(t *testing.T) (*assigningClient, ManagementObservationRequest) {
	t.Helper()
	c, r := approveManagementFixture(t, 1)
	_, err := associateAll(t, c, c, r)
	require.NoError(t, err)
	return c, r
}
func TestManagementAssociationPartialRestartNoActivatedLeak(t *testing.T) {
	c, r := approveManagementFixture(t, 2)
	prepared, err := PrepareManagementAssociation(t.Context(), c, c, r)
	require.NoError(t, err)
	require.Error(t, ValidateManagementActivation(t.Context(), c, r.Authority, r.Spec))
	require.Error(t, func() error { _, e := ActivateManagement(t.Context(), c, c, r); return e }())
	first := r.Machines[0].DeepCopy()
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(first), first))
	first.OwnerReferences = prepared.DesiredOwners(first.UID)
	require.NoError(t, c.Update(t.Context(), first))
	_, err = CompleteManagementAssociation(t.Context(), c, c, r, prepared)
	require.Error(t, err)
	require.Error(t, ValidateManagementActivation(t.Context(), c, r.Authority, r.Spec))
	restarted, err := PrepareManagementAssociation(t.Context(), c, c, r)
	require.NoError(t, err)
	second := r.Machines[1].DeepCopy()
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(second), second))
	second.OwnerReferences = restarted.DesiredOwners(second.UID)
	require.NoError(t, c.Update(t.Context(), second))
	status, err := CompleteManagementAssociation(t.Context(), c, c, r, restarted)
	require.NoError(t, err)
	require.Equal(t, r.Spec.Approval.ObservationID, status.Observation.ID)
	require.Equal(t, r.Spec.InventoryRevision, status.ActivatedRevision)
	require.NoError(t, ValidateManagementActivation(t.Context(), c, r.Authority, r.Spec))
}
func TestManagementPrivateRecordSurvivesDeletedTCPDeclaration(t *testing.T) {
	c, r := activateManagementFixture(t)
	m := r.Machines[0].DeepCopy()
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(m), m))
	tcp := &unstructured.Unstructured{}
	tcp.SetAPIVersion(r.Authority.APIVersion)
	tcp.SetKind(r.Authority.Kind)
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: r.Authority.Namespace, Name: r.Authority.Name}, tcp))
	tcp.Object["spec"] = map[string]any{}
	delete(tcp.Object, "status")
	require.NoError(t, c.Update(t.Context(), tcp))
	_, _, err := ManagementAuthorityForMachine(t.Context(), c, m, r.TalosConfigs[0])
	require.Error(t, err)
	require.Error(t, ValidateManagedMachine(t.Context(), c, m, r.TalosConfigs[0]))
}
func TestManagementPrivateRecordSurvivesDeletedStandaloneDeclaration(t *testing.T) {
	c, a, o := managementStoreFixture(t)
	_, err := RecordManagementObservation(t.Context(), c, c, o)
	require.NoError(t, err)
	tc := &bootstrapv1.TalosConfig{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: a.Namespace, Name: a.Name}, tc))
	tc.Spec.Management = nil
	tc.Status.Management = nil
	require.NoError(t, c.Update(t.Context(), tc))
	_, _, _, err = readManagement(t.Context(), c, a)
	require.NoError(t, err)
	m := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: a.Namespace, Name: "node", UID: "machine"}}
	m.Spec.ClusterName = "cluster"
	m.Spec.Bootstrap.ConfigRef = clusterv1.ContractVersionedObjectReference{APIGroup: bootstrapv1.GroupVersion.Group, Kind: "TalosConfig", Name: tc.Name}
	tc.OwnerReferences = []metav1.OwnerReference{{APIVersion: clusterv1.GroupVersion.String(), Kind: "Machine", Name: m.Name, UID: m.UID, Controller: new(true)}}
	require.NoError(t, c.Update(t.Context(), tc))
	require.NoError(t, c.Create(t.Context(), &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: a.Namespace, Name: "cluster", UID: "cluster-uid"}}))
	require.NoError(t, c.Create(t.Context(), m))
	_, _, err = ManagementAuthorityForMachine(t.Context(), c, m, tc)
	require.Error(t, err)
}
