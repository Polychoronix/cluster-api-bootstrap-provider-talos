// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package controllers

import (
	"testing"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestManagementPausedMachineOnlyRefreshesPausedCondition(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, bootstrapv1.AddToScheme(scheme))
	require.NoError(t, clusterv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	controller := true
	tc := &bootstrapv1.TalosConfig{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "config", UID: "tc", Generation: 9, Annotations: map[string]string{"reconcile.fluxcd.io/requestedAt": "new"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: clusterv1.GroupVersion.String(), Kind: "Machine", Name: "worker", UID: "machine", Controller: &controller}}}, Spec: bootstrapv1.TalosConfigSpec{GenerateType: "none", Management: &bootstrapv1.ManagementSpec{Mode: bootstrapv1.ManagementModeObserve, InventoryRevision: "inventory", Machines: []bootstrapv1.MachineIdentityReference{{Name: "worker", UID: "machine"}}}}, Status: bootstrapv1.TalosConfigStatus{ObservedGeneration: 3, LastHandledReconcileAt: "old", Management: &bootstrapv1.ManagementStatus{Observation: &bootstrapv1.ObservationStatus{Revision: "old", ID: "opaque", ObservedAt: metav1.Now()}}}}
	m := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "worker", UID: "machine", Annotations: map[string]string{clusterv1.PausedAnnotation: ""}}}
	m.Spec.ClusterName = "cluster"
	cluster := &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "cluster", UID: "cluster"}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(tc).WithObjects(tc, m, cluster).Build()
	r := &TalosConfigReconciler{Client: c, APIReader: c, Scheme: scheme, Log: ctrl.Log}
	before := &bootstrapv1.TalosConfig{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(tc), before))
	_, e := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(tc)})
	require.NoError(t, e)
	got := &bootstrapv1.TalosConfig{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(tc), got))
	require.Equal(t, before.Status.ObservedGeneration, got.Status.ObservedGeneration)
	require.Equal(t, before.Status.Management, got.Status.Management)
	require.Equal(t, "old", got.Status.LastHandledReconcileAt)
	require.Len(t, got.Status.Conditions, 1)
	require.Equal(t, clusterv1.PausedCondition, got.Status.Conditions[0].Type)
	secrets := &corev1.SecretList{}
	require.NoError(t, c.List(t.Context(), secrets))
	require.Empty(t, secrets.Items)
}

func TestManagementUnassociatedObserveDoesNotGenerate(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, bootstrapv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	tc := &bootstrapv1.TalosConfig{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "config", UID: "tc"}, Spec: bootstrapv1.TalosConfigSpec{GenerateType: "worker", Management: &bootstrapv1.ManagementSpec{Mode: bootstrapv1.ManagementModeObserve, InventoryRevision: "inventory", Machines: []bootstrapv1.MachineIdentityReference{{Name: "worker", UID: "machine"}}}}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(tc).WithObjects(tc).Build()
	r := &TalosConfigReconciler{Client: c, APIReader: c, Scheme: scheme, Log: ctrl.Log}
	_, e := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(tc)})
	require.NoError(t, e)
	secrets := &corev1.SecretList{}
	require.NoError(t, c.List(t.Context(), secrets))
	require.Empty(t, secrets.Items)
	got := &bootstrapv1.TalosConfig{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(tc), got))
	require.Empty(t, got.Status.DataSecretName)
	require.Nil(t, got.Status.Initialization.DataSecretCreated)
}
