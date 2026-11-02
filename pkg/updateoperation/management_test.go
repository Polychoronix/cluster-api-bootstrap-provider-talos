// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"context"
	"encoding/json"
	"fmt"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"
)

type assigningClient struct {
	client.Client
	next int
}

func (c *assigningClient) Create(ctx context.Context, o client.Object, options ...client.CreateOption) error {
	c.next++
	o.SetUID(types.UID(fmt.Sprintf("uid-%d", c.next)))
	return c.Client.Create(ctx, o, options...)
}
func managementStoreFixture(t *testing.T) (*assigningClient, ManagementAuthority, *ManagementObservation) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, bootstrapv1.AddToScheme(scheme))
	require.NoError(t, clusterv1.AddToScheme(scheme))
	tc := &bootstrapv1.TalosConfig{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "config", UID: "config-uid"}}
	c := &assigningClient{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc).Build()}
	authority := ManagementAuthority{Namespace: tc.Namespace, APIVersion: bootstrapv1.GroupVersion.String(), Kind: "TalosConfig", Name: tc.Name, UID: tc.UID}
	observation := &ManagementObservation{safety: managementSafety{Authority: authority, ClusterUID: "cluster-uid", ClusterName: "cluster", Members: []managementMember{{NodeName: "node", NodeUID: "node-uid", Runtime: managementRuntime{Configuration: []byte("PRIVATE-SNAPSHOT-SENTINEL"), BootID: "boot", OSVersion: "v1.14.2"}}}}, summary: ManagementObservationSummary{InventoryVerified: true, MachinesReady: true}}
	return c, authority, observation
}
func TestActivationSnapshotIdentityAndRedaction(t *testing.T) {
	c, a, o := managementStoreFixture(t)
	first, err := RecordManagementObservation(t.Context(), c, c, o)
	require.NoError(t, err)
	require.NotEmpty(t, first.ID)
	require.False(t, first.Activated)
	again, err := RecordManagementObservation(t.Context(), c, c, o)
	require.NoError(t, err)
	require.Equal(t, first.ID, again.ID)
	encoded, err := json.Marshal(o)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "PRIVATE-SNAPSHOT")
	_, record, _, err := readManagement(t.Context(), c, a)
	require.NoError(t, err)
	snapshot := &corev1.Secret{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: a.Namespace, Name: record.SnapshotName}, snapshot))
	require.True(t, *snapshot.Immutable)
	require.NoError(t, c.Delete(t.Context(), snapshot))
	replacement := snapshot.DeepCopy()
	replacement.ResourceVersion = ""
	replacement.UID = ""
	require.NoError(t, c.Create(t.Context(), replacement))
	_, _, _, err = readManagement(t.Context(), c, a)
	require.Error(t, err)
}
func TestActivatedSnapshotNeverSilentlyBecomesNewDesiredInputs(t *testing.T) {
	c, a, o := managementStoreFixture(t)
	_, err := RecordManagementObservation(t.Context(), c, c, o)
	require.NoError(t, err)
	secret, record, _, err := readManagement(t.Context(), c, a)
	require.NoError(t, err)
	record.Activated = true
	encoded, err := json.Marshal(record)
	require.NoError(t, err)
	require.NoError(t, PatchData(t.Context(), c, secret, map[string][]byte{managementRecordKey: encoded}))
	o.safety.Members[0].Runtime.Configuration = []byte("NOT-ACCEPTED-DESIRED")
	_, err = RecordManagementObservation(t.Context(), c, c, o)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "NOT-ACCEPTED")
}

func TestNativeGenerationSourceMatchesResolverIdentity(t *testing.T) {
	bundle, err := secrets.NewBundle(secrets.NewClock(), config.TalosVersion1_14)
	require.NoError(t, err)
	data, err := yaml.Marshal(bundle)
	require.NoError(t, err)
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "cluster-talos", UID: "bundle-uid", ResourceVersion: "1"}, Data: map[string][]byte{"bundle": data}}
	tc := &bootstrapv1.TalosConfig{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "worker", Labels: map[string]string{clusterv1.ClusterNameLabel: "cluster"}}, Spec: bootstrapv1.TalosConfigSpec{GenerateType: "worker", TalosVersion: "v1.14"}}
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, bootstrapv1.AddToScheme(scheme))
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	resolved, err := configinput.Resolve(t.Context(), reader, tc)
	require.NoError(t, err)
	require.Equal(t, resolved.Sources(), []configinput.SourceIdentity{managementSource(secret, "")})
	original := managementSource(secret, "")
	secret.Data["extra"] = []byte("rotated-generation-source")
	require.NotEqual(t, original.Digest, managementSource(secret, "").Digest, "whole generation Secret data is bound")
}
