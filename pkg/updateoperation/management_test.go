// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// assigningClient models only API-assigned UID/RV for store unit tests; the
// disposable API test exercises the real immutable Secret/CAS contract.
type assigningClient struct {
	client.Client
	next int
}

func (c *assigningClient) Create(ctx context.Context, o client.Object, opts ...client.CreateOption) error {
	c.next++
	o.SetUID(types.UID(fmt.Sprintf("uid-%d", c.next)))
	return c.Client.Create(ctx, o, opts...)
}
func managementStoreFixture(t *testing.T) (*assigningClient, ManagementAuthority, *ManagementObservation) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, bootstrapv1.AddToScheme(scheme))
	require.NoError(t, clusterv1.AddToScheme(scheme))
	tc := &bootstrapv1.TalosConfig{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "config", UID: "config-uid"}}
	c := &assigningClient{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc).Build()}
	a := ManagementAuthority{Namespace: tc.Namespace, APIVersion: bootstrapv1.GroupVersion.String(), Kind: "TalosConfig", Name: tc.Name, UID: tc.UID}
	o := &ManagementObservation{safety: managementSafety{Authority: a, ClusterUID: "cluster-uid", ClusterName: "cluster", Revision: "inventory-1", Members: []managementMember{{NodeName: "node", NodeUID: "node-uid", Runtime: managementRuntime{Configuration: []byte("SECRET-SNAPSHOT-SENTINEL"), BootID: "boot", OSVersion: "v1.14.2"}}}}, summary: ManagementObservationSummary{InventoryVerified: true, MachinesReady: true}}
	return c, a, o
}
func TestManagementStableOpaqueSnapshotAndRecreatedSameRevision(t *testing.T) {
	c, a, o := managementStoreFixture(t)
	first, e := RecordManagementObservation(t.Context(), c, c, o)
	require.NoError(t, e)
	require.NotEmpty(t, first.Observation.ID)
	require.NotContains(t, first.Observation.ID, "SECRET")
	second, e := RecordManagementObservation(t.Context(), c, c, o)
	require.NoError(t, e)
	require.Equal(t, first, second)
	_, record, payload, e := readManagement(t.Context(), c, a)
	require.NoError(t, e)
	var captured managementSafety
	require.NoError(t, json.Unmarshal(payload, &captured))
	require.Equal(t, []byte("SECRET-SNAPSHOT-SENTINEL"), captured.Members[0].Runtime.Configuration)
	snapshot := &corev1.Secret{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: a.Namespace, Name: record.SnapshotName}, snapshot))
	require.True(t, *snapshot.Immutable)
	require.NoError(t, c.Delete(t.Context(), snapshot))
	replacement := snapshot.DeepCopy()
	replacement.ResourceVersion = ""
	replacement.UID = ""
	require.NoError(t, c.Create(t.Context(), replacement))
	require.NotEqual(t, record.SnapshotUID, replacement.UID)
	_, _, _, e = readManagement(t.Context(), c, a)
	require.Error(t, e)
	require.NotContains(t, e.Error(), "SECRET-SNAPSHOT")
	_, e = RecordManagementObservation(t.Context(), c, c, o)
	require.Error(t, e, "same revision must not replace lost snapshot")
}
func TestManagementChangedUnapprovedInputsGetNewID(t *testing.T) {
	c, _, o := managementStoreFixture(t)
	first, e := RecordManagementObservation(t.Context(), c, c, o)
	require.NoError(t, e)
	o.safety.Members[0].NodeUID = "new-node"
	second, e := RecordManagementObservation(t.Context(), c, c, o)
	require.NoError(t, e)
	require.NotEqual(t, first.Observation.ID, second.Observation.ID)
	require.Equal(t, first.Observation.Revision, second.Observation.Revision)
}
func TestManagementActivatedPayloadCannotChange(t *testing.T) {
	c, a, o := managementStoreFixture(t)
	_, e := RecordManagementObservation(t.Context(), c, c, o)
	require.NoError(t, e)
	secret, r, _, e := readManagement(t.Context(), c, a)
	require.NoError(t, e)
	r.Activated = true
	data, e := json.Marshal(r)
	require.NoError(t, e)
	require.NoError(t, PatchData(t.Context(), c, secret, map[string][]byte{managementRecordKey: data}))
	o.safety.Members[0].Runtime.Configuration = []byte("changed-secret")
	_, e = RecordManagementObservation(t.Context(), c, c, o)
	require.Error(t, e)
	require.NotContains(t, e.Error(), "changed-secret")
}
func TestManagementPausedAuthorityDoesNotRecordOrRefresh(t *testing.T) {
	c, a, o := managementStoreFixture(t)
	first, e := RecordManagementObservation(t.Context(), c, c, o)
	require.NoError(t, e)
	tc := &bootstrapv1.TalosConfig{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: a.Namespace, Name: a.Name}, tc))
	tc.Annotations = map[string]string{"cluster.x-k8s.io/paused": "", "reconcile.fluxcd.io/requestedAt": "new"}
	require.NoError(t, c.Update(t.Context(), tc))
	_, e = RecordManagementObservation(t.Context(), c, c, o)
	require.ErrorIs(t, e, ErrManagementPaused)
	_, r, _, e := readManagement(t.Context(), c, a)
	require.NoError(t, e)
	require.Equal(t, first.Observation.ID, r.ObservationID)
	require.Equal(t, first.Observation.ObservedAt, r.ObservedAt)
}
func TestManagementObservationFormattingRedactsPrivatePayload(t *testing.T) {
	_, _, o := managementStoreFixture(t)
	require.NotContains(t, fmt.Sprintf("%v %#v", o, o), "SECRET-SNAPSHOT")
	encoded, e := json.Marshal(o)
	require.NoError(t, e)
	require.NotContains(t, string(encoded), "SECRET-SNAPSHOT")
}
