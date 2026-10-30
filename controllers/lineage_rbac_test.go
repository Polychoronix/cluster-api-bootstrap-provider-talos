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
	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	kubernetesScheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	"os"
	"path/filepath"
	"runtime"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"
	"testing"
	"time"
)

func TestInitialBootstrapProductionSchemeAndServiceAccountRBAC(t *testing.T) {
	assets := os.Getenv("KUBEBUILDER_ASSETS")
	if assets == "" || testing.Short() {
		t.Skip("explicit disposable assets required")
	}
	_, source, _, ok := runtime.Caller(0)
	require.True(t, ok)
	repo := filepath.Dir(filepath.Dir(source))
	cache := os.Getenv("GOMODCACHE")
	if cache == "" {
		home, err := os.UserHomeDir()
		require.NoError(t, err)
		cache = filepath.Join(home, "go/pkg/mod")
	}
	group := "infrastructure.cluster.x-k8s.io"
	infraCRD := &apiextensionsv1.CustomResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: "metalmachines." + group}, Spec: apiextensionsv1.CustomResourceDefinitionSpec{Group: group, Names: apiextensionsv1.CustomResourceDefinitionNames{Kind: "MetalMachine", Plural: "metalmachines"}, Scope: apiextensionsv1.NamespaceScoped, Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{Name: "v1beta1", Served: true, Storage: true, Schema: &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{Type: "object", XPreserveUnknownFields: ptr.To(true)}}}}}}
	// This is the actual production shared scheme; no fixture-only AddToScheme.
	scheme := kubernetesScheme.Scheme
	environment := &envtest.Environment{UseExistingCluster: ptr.To(false), BinaryAssetsDirectory: assets, Scheme: scheme, CRDDirectoryPaths: []string{filepath.Join(repo, "config/crd/bases"), filepath.Join(cache, "sigs.k8s.io/cluster-api@v1.14.2/core/config/crd/bases")}, CRDs: []*apiextensionsv1.CustomResourceDefinition{infraCRD}, ErrorIfCRDPathMissing: true}
	cfg, err := environment.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, environment.Stop()) })
	admin, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	ctx := context.Background()
	namespace := "lineage-rbac"
	require.NoError(t, admin.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}))
	require.NoError(t, admin.Create(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "provider"}}))
	roleData, err := os.ReadFile(filepath.Join(repo, "config/rbac/role.yaml"))
	require.NoError(t, err)
	role := &rbacv1.ClusterRole{}
	require.NoError(t, yaml.Unmarshal(roleData, role))
	role.Name = "lineage-provider-role"
	require.NoError(t, admin.Create(ctx, role))
	kube, err := kubernetes.NewForConfig(cfg)
	require.NoError(t, err)
	token, err := kube.CoreV1().ServiceAccounts(namespace).CreateToken(ctx, "provider", &authv1.TokenRequest{Spec: authv1.TokenRequestSpec{ExpirationSeconds: ptr.To(int64(600))}}, metav1.CreateOptions{})
	require.NoError(t, err)
	scoped := rest.CopyConfig(cfg)
	scoped.CertData = nil
	scoped.KeyData = nil
	scoped.CertFile = ""
	scoped.KeyFile = ""
	scoped.BearerToken = token.Status.Token
	provider, err := client.New(scoped, client.Options{Scheme: scheme})
	require.NoError(t, err)
	err = provider.List(ctx, &apiextensionsv1.CustomResourceDefinitionList{})
	require.True(t, apierrors.IsForbidden(err), "token-only SA without binding must fail typed CRD lookup")
	require.NoError(t, admin.Create(ctx, &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "lineage-provider-binding"}, RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: role.Name}, Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Namespace: namespace, Name: "provider"}}}))
	require.Eventually(t, func() bool { return provider.List(ctx, &apiextensionsv1.CustomResourceDefinitionList{}) == nil }, 10*time.Second, 50*time.Millisecond, "generated role must authorize actual SA typed CRD list")
	denied := infraCRD.DeepCopy()
	denied.Name = "denied." + group
	err = provider.Create(ctx, denied)
	require.True(t, apierrors.IsForbidden(err), "CRD read role must not authorize creation")
	cluster := &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "cluster"}, Spec: clusterv1.ClusterSpec{ControlPlaneEndpoint: clusterv1.APIEndpoint{Host: "api.example", Port: 6443}}}
	require.NoError(t, admin.Create(ctx, cluster))
	cluster.Status.Initialization.InfrastructureProvisioned = ptr.To(true)
	require.NoError(t, admin.Status().Update(ctx, cluster))
	set := &clusterv1.MachineSet{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "set"}, Spec: clusterv1.MachineSetSpec{ClusterName: cluster.Name, Selector: metav1.LabelSelector{MatchLabels: map[string]string{"test": "lineage"}}, Template: clusterv1.MachineTemplateSpec{ObjectMeta: clusterv1.ObjectMeta{Labels: map[string]string{"test": "lineage"}}, Spec: clusterv1.MachineSpec{ClusterName: cluster.Name, Bootstrap: clusterv1.Bootstrap{ConfigRef: clusterv1.ContractVersionedObjectReference{APIGroup: bootstrapv1.GroupVersion.Group, Kind: "TalosConfigTemplate", Name: "baseline-template"}}, InfrastructureRef: clusterv1.ContractVersionedObjectReference{APIGroup: group, Kind: "MetalMachineTemplate", Name: "infra-template"}}}}}
	require.NoError(t, admin.Create(ctx, set))
	machine := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "machine", OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(set, clusterv1.GroupVersion.WithKind("MachineSet"))}}, Spec: clusterv1.MachineSpec{ClusterName: cluster.Name, Version: "v1.37.0", Bootstrap: clusterv1.Bootstrap{ConfigRef: clusterv1.ContractVersionedObjectReference{APIGroup: bootstrapv1.GroupVersion.Group, Kind: "TalosConfig", Name: "config"}}, InfrastructureRef: clusterv1.ContractVersionedObjectReference{APIGroup: group, Kind: "MetalMachine", Name: "infra"}}}
	require.NoError(t, admin.Create(ctx, machine))
	infra := &unstructured.Unstructured{}
	infra.SetAPIVersion(group + "/v1beta1")
	infra.SetKind("MetalMachine")
	infra.SetName("infra")
	infra.SetNamespace(namespace)
	infra.SetOwnerReferences([]metav1.OwnerReference{*metav1.NewControllerRef(machine, clusterv1.GroupVersion.WithKind("Machine"))})
	infra.SetAnnotations(map[string]string{clusterv1.TemplateClonedFromNameAnnotation: "infra-template", clusterv1.TemplateClonedFromGroupKindAnnotation: (schema.GroupKind{Group: group, Kind: "MetalMachineTemplate"}).String()})
	require.NoError(t, admin.Create(ctx, infra))
	data, _ := userSuppliedConfig(t, config.TalosVersion1_14)
	require.NoError(t, admin.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "baseline"}, Data: map[string][]byte{"config": []byte(data)}}))
	tc := &bootstrapv1.TalosConfig{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "config", OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(machine, clusterv1.GroupVersion.WithKind("Machine"))}, Annotations: map[string]string{clusterv1.TemplateClonedFromNameAnnotation: "baseline-template", clusterv1.TemplateClonedFromGroupKindAnnotation: (schema.GroupKind{Group: bootstrapv1.GroupVersion.Group, Kind: "TalosConfigTemplate"}).String()}}, Spec: bootstrapv1.TalosConfigSpec{GenerateType: "controlplane", TalosVersion: "v1.14", ConfigSecretRef: &bootstrapv1.SecretKeyReference{Name: "baseline", Key: "config"}}}
	require.NoError(t, admin.Create(ctx, tc))
	r := &TalosConfigReconciler{Client: provider, APIReader: provider, Scheme: scheme, Log: ctrl.Log.WithName("service-account-smoke")}
	_, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(tc)})
	require.NoError(t, err)
	require.NoError(t, admin.Get(ctx, client.ObjectKeyFromObject(tc), tc))
	require.NotEmpty(t, tc.Status.DataSecretName)
	output := &corev1.Secret{}
	require.NoError(t, admin.Get(ctx, client.ObjectKey{Namespace: namespace, Name: tc.Status.DataSecretName}, output))
	intent, err := op.DecodeAcceptedIntent(output.Data[op.AcceptedIntentKey])
	require.NoError(t, err)
	require.Equal(t, machine.UID, intent.MachineUID)
	require.Equal(t, tc.UID, intent.TalosConfigUID)
}
