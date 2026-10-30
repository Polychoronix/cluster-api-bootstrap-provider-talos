// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"os"
	"path/filepath"
	"testing"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func TestManagementImmutableSnapshotDisposableAPI(t *testing.T) {
	assets := os.Getenv("KUBEBUILDER_ASSETS")
	if assets == "" || testing.Short() {
		t.Skip("set KUBEBUILDER_ASSETS for disposable management API")
	}
	env := &envtest.Environment{UseExistingCluster: ptr.To(false), BinaryAssetsDirectory: assets, CRDDirectoryPaths: []string{filepath.Join("..", "..", "config", "crd", "bases")}}
	cfg, e := env.Start()
	require.NoError(t, e)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, bootstrapv1.AddToScheme(scheme))
	c, e := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, e)
	require.NoError(t, c.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "management-api"}}))
	tc := &bootstrapv1.TalosConfig{ObjectMeta: metav1.ObjectMeta{Namespace: "management-api", Name: "config"}, Spec: bootstrapv1.TalosConfigSpec{GenerateType: "none", Data: "version: v1alpha1\nmachine:\n  type: worker\n"}}
	require.NoError(t, c.Create(t.Context(), tc))
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(tc), tc))
	a := ManagementAuthority{Namespace: tc.Namespace, APIVersion: bootstrapv1.GroupVersion.String(), Kind: "TalosConfig", Name: tc.Name, UID: tc.UID}
	o := &ManagementObservation{safety: managementSafety{Authority: a, ClusterUID: "cluster", ClusterName: "cluster", Revision: "import-1", Members: []managementMember{{Runtime: managementRuntime{Configuration: []byte("PRIVATE-API-SNAPSHOT")}}}}, summary: ManagementObservationSummary{InventoryVerified: true}}
	first, e := RecordManagementObservation(t.Context(), c, c, o)
	require.NoError(t, e)
	second, e := RecordManagementObservation(t.Context(), c, c, o)
	require.NoError(t, e)
	require.Equal(t, first, second)
	_, record, _, e := readManagement(t.Context(), c, a)
	require.NoError(t, e)
	snapshot := &corev1.Secret{}
	key := client.ObjectKey{Namespace: a.Namespace, Name: record.SnapshotName}
	require.NoError(t, c.Get(t.Context(), key, snapshot))
	before := snapshot.DeepCopy()
	snapshot.Data[managementSnapshotKey] = []byte("changed")
	require.Error(t, c.Update(t.Context(), snapshot), "real API must reject immutable snapshot data")
	require.NoError(t, c.Get(t.Context(), key, snapshot))
	require.Equal(t, before.Data, snapshot.Data)
	require.NoError(t, c.Delete(t.Context(), snapshot))
	replacement := before.DeepCopy()
	replacement.UID = ""
	replacement.ResourceVersion = ""
	require.NoError(t, c.Create(t.Context(), replacement))
	_, _, _, e = readManagement(t.Context(), c, a)
	require.Error(t, e, "same name/revision replacement UID must not satisfy approval")
}
