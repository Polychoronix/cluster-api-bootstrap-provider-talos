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
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func TestManagementAssociationTwoMemberDisposableAPIRestart(t *testing.T) {
	assets := os.Getenv("KUBEBUILDER_ASSETS")
	if assets == "" || testing.Short() {
		t.Skip("explicit disposable association API assets required")
	}
	fixture, request, _ := managementTLSFixture(t, 2)
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, bootstrapv1.AddToScheme(scheme))
	require.NoError(t, clusterv1.AddToScheme(scheme))
	require.NoError(t, apiextensionsv1.AddToScheme(scheme))
	crd := func(group, kind, plural string) *apiextensionsv1.CustomResourceDefinition {
		return &apiextensionsv1.CustomResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: plural + "." + group}, Spec: apiextensionsv1.CustomResourceDefinitionSpec{Group: group, Names: apiextensionsv1.CustomResourceDefinitionNames{Kind: kind, Plural: plural}, Scope: apiextensionsv1.NamespaceScoped, Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{Name: "v1beta1", Served: true, Storage: true, Schema: &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{Type: "object", Properties: map[string]apiextensionsv1.JSONSchemaProps{"spec": {Type: "object", XPreserveUnknownFields: new(true)}}}}}}}}
	}
	capi := filepath.Join(os.Getenv("HOME"), "go", "pkg", "mod", "sigs.k8s.io", "cluster-api@v1.14.2", "core", "config", "crd", "bases")
	env := &envtest.Environment{UseExistingCluster: new(false), BinaryAssetsDirectory: assets, CRDDirectoryPaths: []string{filepath.Join("..", "..", "config", "crd", "bases"), capi}, CRDs: []*apiextensionsv1.CustomResourceDefinition{crd("controlplane.cluster.x-k8s.io", "TalosControlPlane", "taloscontrolplanes"), crd("infrastructure.example.test", "MetalMachine", "metalmachines")}}
	cfg, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })
	api, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	require.NoError(t, api.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "tenant"}}))
	secrets := &corev1.SecretList{}
	require.NoError(t, fixture.List(t.Context(), secrets))
	for _, s := range secrets.Items {
		s.UID = ""
		s.ResourceVersion = ""
		s.OwnerReferences = nil
		require.NoError(t, api.Create(t.Context(), &s))
	}
	cluster := request.Cluster.DeepCopy()
	cluster.UID = ""
	cluster.ResourceVersion = ""
	require.NoError(t, api.Create(t.Context(), cluster))
	request.Cluster = cluster
	request.Spec.Machines = nil
	for i, old := range request.Machines {
		m := old.DeepCopy()
		saved := m.Status
		m.UID = ""
		m.ResourceVersion = ""
		m.Status = clusterv1.MachineStatus{}
		require.NoError(t, api.Create(t.Context(), m))
		m.Status = saved
		require.NoError(t, api.Status().Update(t.Context(), m))
		request.Machines[i] = m
		request.Spec.Machines = append(request.Spec.Machines, bootstrapv1.MachineIdentityReference{Name: m.Name, UID: m.UID})
		tc := request.TalosConfigs[i].DeepCopy()
		tc.UID = ""
		tc.ResourceVersion = ""
		tc.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(m, clusterv1.GroupVersion.WithKind("Machine"))}
		require.NoError(t, api.Create(t.Context(), tc))
		request.TalosConfigs[i] = tc
		infra := &unstructured.Unstructured{}
		infra.SetAPIVersion("infrastructure.example.test/v1beta1")
		infra.SetKind("MetalMachine")
		require.NoError(t, fixture.Get(t.Context(), client.ObjectKey{Namespace: "tenant", Name: m.Name}, infra))
		infra.SetUID("")
		infra.SetResourceVersion("")
		infra.SetOwnerReferences([]metav1.OwnerReference{*metav1.NewControllerRef(m, clusterv1.GroupVersion.WithKind("Machine"))})
		require.NoError(t, api.Create(t.Context(), infra))
	}
	tcp := &unstructured.Unstructured{}
	tcp.SetAPIVersion(request.Authority.APIVersion)
	tcp.SetKind(request.Authority.Kind)
	tcp.SetNamespace("tenant")
	tcp.SetName(request.Authority.Name)
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(request.Spec)
	require.NoError(t, err)
	tcp.Object["spec"] = map[string]any{"management": raw}
	require.NoError(t, api.Create(t.Context(), tcp))
	request.Authority.UID = tcp.GetUID()
	sharedPolicy := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "shared-policy"}, Data: map[string][]byte{"patch": []byte("machine:\n  env:\n    PRIVATE_SHARED_POLICY: mismatch\n")}}
	nodePolicy := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "node-policy"}, Data: map[string][]byte{"patch": []byte("machine:\n  type: controlplane\n")}}
	require.NoError(t, api.Create(t.Context(), sharedPolicy))
	require.NoError(t, api.Create(t.Context(), nodePolicy))
	setEffectivePolicy(t, api, api, &request, true, true)
	_, err = ObserveManagement(t.Context(), api, request)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "PRIVATE_SHARED_POLICY")
	sharedPolicy.Data["patch"] = []byte("machine:\n  type: controlplane\n")
	require.NoError(t, api.Update(t.Context(), sharedPolicy))
	nodePolicy.Data["patch"] = []byte("machine:\n  env:\n    PRIVATE_NODE_POLICY: mismatch\n")
	require.NoError(t, api.Update(t.Context(), nodePolicy))
	_, err = ObserveManagement(t.Context(), api, request)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "PRIVATE_NODE_POLICY")
	nodePolicy.Data["patch"] = []byte("machine:\n  type: controlplane\n")
	require.NoError(t, api.Update(t.Context(), nodePolicy))
	require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(tcp), tcp))
	observed, err := ObserveManagement(t.Context(), api, request)
	require.NoError(t, err)
	status, err := RecordManagementObservation(t.Context(), api, api, observed)
	require.NoError(t, err)
	request.Spec.Mode = bootstrapv1.ManagementModeManaged
	request.Spec.Approval = &bootstrapv1.ObservationApproval{Revision: request.Spec.InventoryRevision, ObservationID: status.Observation.ID}
	raw, err = runtime.DefaultUnstructuredConverter.ToUnstructured(request.Spec)
	require.NoError(t, err)
	tcp.Object["spec"].(map[string]any)["management"] = raw
	require.NoError(t, api.Update(t.Context(), tcp))
	prepared, err := PrepareManagementAssociation(t.Context(), api, api, request)
	require.NoError(t, err)
	first := request.Machines[0].DeepCopy()
	first.OwnerReferences = prepared.DesiredOwners(first.UID)
	require.NoError(t, api.Update(t.Context(), first))
	_, err = CompleteManagementAssociation(t.Context(), api, api, request, prepared)
	require.Error(t, err)
	require.Error(t, ValidateManagementActivation(t.Context(), api, request.Authority, request.Spec))
	restarted, err := PrepareManagementAssociation(t.Context(), api, api, request)
	require.NoError(t, err)
	second := request.Machines[1].DeepCopy()
	second.OwnerReferences = restarted.DesiredOwners(second.UID)
	require.NoError(t, api.Update(t.Context(), second))
	activated, err := CompleteManagementAssociation(t.Context(), api, api, request, restarted)
	require.NoError(t, err)
	require.Equal(t, status.Observation.ID, activated.Observation.ID)
	require.NoError(t, ValidateManagementActivation(t.Context(), api, request.Authority, request.Spec))
}
