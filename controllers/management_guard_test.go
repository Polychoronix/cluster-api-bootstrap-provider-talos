// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package controllers

import (
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"
)

func TestManagementMissingAPIReaderCannotSelectNormalFlow(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, bootstrapv1.AddToScheme(scheme))
	tc := &bootstrapv1.TalosConfig{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "tc"}, Spec: bootstrapv1.TalosConfigSpec{GenerateType: "worker"}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc).Build()
	r := &TalosConfigReconciler{Client: c, Scheme: scheme}
	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(tc)})
	require.ErrorContains(t, err, "APIReader required")
}
func TestManagementCustomSeedBeforeNodeRefDoesNotGenerate(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, bootstrapv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, clusterv1.AddToScheme(scheme))
	m := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "worker", UID: "machine"}}
	m.Spec.ClusterName = "cluster"
	m.Spec.Bootstrap.DataSecretName = new("external-seed")
	m.Spec.Bootstrap.ConfigRef = clusterv1.ContractVersionedObjectReference{APIGroup: bootstrapv1.GroupVersion.Group, Kind: "TalosConfig", Name: "tc"}
	tc := &bootstrapv1.TalosConfig{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "tc", UID: "tc", OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(m, clusterv1.GroupVersion.WithKind("Machine"))}}, Spec: bootstrapv1.TalosConfigSpec{GenerateType: "worker"}}
	cluster := &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "cluster", UID: "cluster"}}
	seed := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "external-seed"}, Data: map[string][]byte{"value": []byte("private-original-seed")}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(tc).WithObjects(m, tc, cluster, seed).Build()
	r := &TalosConfigReconciler{Client: c, APIReader: c, Scheme: scheme, Log: ctrl.Log}
	result, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(tc)})
	require.NoError(t, err)
	require.Positive(t, result.RequeueAfter)
	secrets := &corev1.SecretList{}
	require.NoError(t, c.List(t.Context(), secrets))
	require.Len(t, secrets.Items, 1)
	require.Equal(t, seed.Data, secrets.Items[0].Data)
	got := &bootstrapv1.TalosConfig{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(tc), got))
	require.Empty(t, got.Status.DataSecretName)
}
