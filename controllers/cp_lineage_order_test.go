// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package controllers

import (
	"context"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kubernetesScheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"os"
	"path/filepath"
	"runtime"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"testing"
	"time"
)

func TestFreshCPBootstrapWaitsForInfrastructureOwnershipTransfer(t *testing.T) {
	assets := os.Getenv("KUBEBUILDER_ASSETS")
	if assets == "" || testing.Short() {
		t.Skip("explicit isolated API assets required")
	}
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)
	repo := filepath.Dir(filepath.Dir(file))
	cache := os.Getenv("GOMODCACHE")
	if cache == "" {
		home, err := os.UserHomeDir()
		require.NoError(t, err)
		cache = filepath.Join(home, "go/pkg/mod")
	}
	makeCRD := func(group, kind, plural string) *apiextensionsv1.CustomResourceDefinition {
		return &apiextensionsv1.CustomResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: plural + "." + group}, Spec: apiextensionsv1.CustomResourceDefinitionSpec{Group: group, Names: apiextensionsv1.CustomResourceDefinitionNames{Kind: kind, Plural: plural}, Scope: apiextensionsv1.NamespaceScoped, Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{Name: "v1beta1", Served: true, Storage: true, Schema: &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{Type: "object", XPreserveUnknownFields: ptr.To(true)}}}}}}
	}
	scheme := kubernetesScheme.Scheme
	environment := &envtest.Environment{UseExistingCluster: ptr.To(false), BinaryAssetsDirectory: assets, Scheme: scheme, CRDDirectoryPaths: []string{filepath.Join(repo, "config/crd/bases"), filepath.Join(cache, "sigs.k8s.io/cluster-api@v1.14.2/core/config/crd/bases")}, CRDs: []*apiextensionsv1.CustomResourceDefinition{makeCRD("infrastructure.cluster.x-k8s.io", "MetalMachine", "metalmachines"), makeCRD("controlplane.cluster.x-k8s.io", "TalosControlPlane", "taloscontrolplanes")}, ErrorIfCRDPathMissing: true}
	cfg, err := environment.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, environment.Stop()) })
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	ctx := context.Background()
	namespace := "cp-order"
	require.NoError(t, c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}))
	cluster := &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "cluster"}, Spec: clusterv1.ClusterSpec{ControlPlaneEndpoint: clusterv1.APIEndpoint{Host: "api.example", Port: 6443}}}
	require.NoError(t, c.Create(ctx, cluster))
	cluster.Status.Initialization.InfrastructureProvisioned = ptr.To(true)
	require.NoError(t, c.Status().Update(ctx, cluster))
	cp := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "controlplane.cluster.x-k8s.io/v1beta1", "kind": "TalosControlPlane", "metadata": map[string]any{"name": "cp", "namespace": namespace}, "spec": map[string]any{"machineTemplate": map[string]any{"spec": map[string]any{"infrastructureRef": map[string]any{"apiGroup": "infrastructure.cluster.x-k8s.io", "kind": "MetalMachineTemplate", "name": "infra-template"}}}}}}
	require.NoError(t, c.Create(ctx, cp))
	machine := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "machine", OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(cp, schema.GroupVersion{Group: "controlplane.cluster.x-k8s.io", Version: "v1beta1"}.WithKind("TalosControlPlane"))}}, Spec: clusterv1.MachineSpec{ClusterName: cluster.Name, Version: "v1.37.0", Bootstrap: clusterv1.Bootstrap{ConfigRef: clusterv1.ContractVersionedObjectReference{APIGroup: bootstrapv1.GroupVersion.Group, Kind: "TalosConfig", Name: "config"}}, InfrastructureRef: clusterv1.ContractVersionedObjectReference{APIGroup: "infrastructure.cluster.x-k8s.io", Kind: "MetalMachine", Name: "infra"}}}
	require.NoError(t, c.Create(ctx, machine))
	originalOwner := metav1.OwnerReference{APIVersion: cp.GetAPIVersion(), Kind: cp.GetKind(), Name: cp.GetName(), UID: cp.GetUID()}
	infra := &unstructured.Unstructured{}
	infra.SetAPIVersion("infrastructure.cluster.x-k8s.io/v1beta1")
	infra.SetKind("MetalMachine")
	infra.SetName("infra")
	infra.SetNamespace(namespace)
	infra.SetOwnerReferences([]metav1.OwnerReference{originalOwner})
	infra.SetAnnotations(map[string]string{clusterv1.TemplateClonedFromNameAnnotation: "infra-template", clusterv1.TemplateClonedFromGroupKindAnnotation: (schema.GroupKind{Group: "infrastructure.cluster.x-k8s.io", Kind: "MetalMachineTemplate"}).String()})
	require.NoError(t, c.Create(ctx, infra))
	data, _ := userSuppliedConfig(t, config.TalosVersion1_14)
	require.NoError(t, c.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "baseline"}, Data: map[string][]byte{"config": []byte(data)}}))
	tc := &bootstrapv1.TalosConfig{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "config", OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(machine, clusterv1.GroupVersion.WithKind("Machine"))}, Annotations: map[string]string{op.InitialControlPlaneNameAnnotation: cp.GetName(), op.InitialControlPlaneUIDAnnotation: string(cp.GetUID())}}, Spec: bootstrapv1.TalosConfigSpec{GenerateType: "controlplane", TalosVersion: "v1.14", ConfigSecretRef: &bootstrapv1.SecretKeyReference{Name: "baseline", Key: "config"}}}
	require.NoError(t, c.Create(ctx, tc))
	reconciler := &TalosConfigReconciler{Client: c, APIReader: c, Scheme: scheme, Log: ctrl.Log.WithName("cp-order-test")}
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(tc)}
	result, err := reconciler.Reconcile(ctx, req)
	require.NoError(t, err)
	require.Positive(t, result.RequeueAfter, "recognized initial CP must wait for core infrastructure owner transfer")
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(tc), tc))
	require.Empty(t, tc.Status.DataSecretName)
	require.False(t, ptr.Deref(tc.Status.Initialization.DataSecretCreated, false))
	seed := &corev1.Secret{}
	require.True(t, apierrors.IsNotFound(c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: "machine-bootstrap-data"}, seed)))
	beforeUID := tc.UID
	infra.SetOwnerReferences([]metav1.OwnerReference{*metav1.NewControllerRef(machine, clusterv1.GroupVersion.WithKind("Machine"))})
	require.NoError(t, c.Update(ctx, infra))
	result, err = reconciler.Reconcile(ctx, req)
	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(tc), tc))
	require.Equal(t, beforeUID, tc.UID)
	require.True(t, ptr.Deref(tc.Status.Initialization.DataSecretCreated, false))
	require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: tc.Status.DataSecretName}, seed))
	intent, err := op.DecodeAcceptedIntent(seed.Data[op.AcceptedIntentKey])
	require.NoError(t, err)
	require.Equal(t, machine.UID, intent.MachineUID)
	require.Equal(t, tc.UID, intent.TalosConfigUID)
	before := seed.DeepCopy()
	_, err = reconciler.Reconcile(ctx, req)
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(seed), seed))
	require.Equal(t, before.Data, seed.Data)
	require.Equal(t, before.UID, seed.UID)
	_ = time.Second
}
