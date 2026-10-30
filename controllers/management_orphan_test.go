// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package controllers

import (
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"
)

func TestManagementRetainedPrivateRecordCannotRenderAfterPublicRemoval(t *testing.T) {
	for _, cp := range []bool{false, true} {
		t.Run(map[bool]string{false: "standalone", true: "TCP"}[cp], func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, bootstrapv1.AddToScheme(scheme))
			require.NoError(t, clusterv1.AddToScheme(scheme))
			require.NoError(t, corev1.AddToScheme(scheme))
			m := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "worker", UID: "machine"}}
			m.Spec.ClusterName = "cluster"
			m.Spec.Bootstrap.ConfigRef = clusterv1.ContractVersionedObjectReference{APIGroup: bootstrapv1.GroupVersion.Group, Kind: "TalosConfig", Name: "tc"}
			m.Spec.Bootstrap.DataSecretName = new("original")
			tc := &bootstrapv1.TalosConfig{ObjectMeta: metav1.ObjectMeta{Namespace: m.Namespace, Name: "tc", UID: "tc", OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(m, clusterv1.GroupVersion.WithKind("Machine"))}}, Spec: bootstrapv1.TalosConfigSpec{GenerateType: "worker"}}
			cluster := &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: m.Namespace, Name: "cluster", UID: "cluster"}}
			authorityName := "tc"
			authorityKind := "TalosConfig"
			authorityVersion := bootstrapv1.GroupVersion.String()
			authorityUID := tc.UID
			objects := []client.Object{tc, m, cluster, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: m.Namespace, Name: "original"}, Data: map[string][]byte{"value": []byte("private-seed")}}}
			if cp {
				m.Labels = map[string]string{clusterv1.MachineControlPlaneLabel: ""}
				m.OwnerReferences = []metav1.OwnerReference{{APIVersion: "controlplane.cluster.x-k8s.io/v1beta1", Kind: "TalosControlPlane", Name: "cp", UID: "cp", Controller: new(true)}}
				tcp := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "controlplane.cluster.x-k8s.io/v1beta1", "kind": "TalosControlPlane", "metadata": map[string]any{"namespace": "tenant", "name": "cp", "uid": "cp"}, "spec": map[string]any{}}}
				objects = append(objects, tcp)
				authorityName = "cp"
				authorityKind = "TalosControlPlane"
				authorityVersion = tcp.GetAPIVersion()
				authorityUID = tcp.GetUID()
			}
			record := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: m.Namespace, Name: authorityName + "-management", Annotations: map[string]string{op.ProvenanceAnnotation: "talos-management"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: authorityVersion, Kind: authorityKind, Name: authorityName, UID: authorityUID, Controller: new(true)}}}, Data: map[string][]byte{"management": []byte("retained-invalid-private")}}
			objects = append(objects, record)
			c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(tc).WithObjects(objects...).Build()
			r := &TalosConfigReconciler{Client: c, APIReader: c, Scheme: scheme, Log: ctrl.Log}
			_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(tc)})
			require.NoError(t, err)
			got := &bootstrapv1.TalosConfig{}
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(tc), got))
			require.Empty(t, got.Status.DataSecretName)
			secrets := &corev1.SecretList{}
			require.NoError(t, c.List(t.Context(), secrets))
			require.Len(t, secrets.Items, 2)
		})
	}
}
