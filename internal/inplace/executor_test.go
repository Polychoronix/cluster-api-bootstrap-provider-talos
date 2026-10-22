// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package inplace

import (
	"context"
	"encoding/json"
	"fmt"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/utils/ptr"
	"net/http"
	"net/http/httptest"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	runtimehooks "sigs.k8s.io/cluster-api/api/runtime/hooks/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"strings"
	"sync"
	"testing"
	"time"
)

type durableNode struct {
	mu                                               sync.Mutex
	observation                                      NodeObservation
	outcome                                          op.ApplyOutcome
	applyCount, pullCount, installCount, rebootCount int
	installed                                        string
	installGate                                      chan struct{}
	installErr                                       error
}

func (n *durableNode) Observe(context.Context) (NodeObservation, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	v := n.observation
	v.Configuration = append([]byte(nil), v.Configuration...)
	return v, nil
}
func (n *durableNode) Version(context.Context) (string, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.observation.OSVersion, nil
}
func (n *durableNode) Apply(_ context.Context, data []byte) (op.ApplyOutcome, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.applyCount++
	n.observation.Configuration = append([]byte(nil), data...)
	if n.outcome == op.RebootRequired {
		n.observation.BootID += "-apply"
	}
	return n.outcome, nil
}
func (n *durableNode) PullInstaller(_ context.Context, image string) (string, string, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.pullCount++
	if strings.Contains(image, "@") {
		return image, "sha256:fixed", nil
	}
	return image, "sha256:fixed", nil
}
func (n *durableNode) Install(ctx context.Context, image string) (int32, error) {
	n.mu.Lock()
	n.installCount++
	n.installed = image
	gate := n.installGate
	err := n.installErr
	n.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return -1, ctx.Err()
		}
	}
	return 0, err
}
func (n *durableNode) Reboot(context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.rebootCount++
	n.observation.BootID += "-upgrade"
	n.observation.SchematicID = "new-schematic"
	return nil
}
func (n *durableNode) Close() error { return nil }

type durableFixture struct {
	handler      *Handler
	management   client.Client
	node         *durableNode
	request      *runtimehooks.UpdateMachineRequest
	config       *bootstrapv1.TalosConfig
	machine      *clusterv1.Machine
	cluster      *clusterv1.Cluster
	output       *corev1.Secret
	workloadNode *corev1.Node
}

type uidAssigningClient struct{ client.Client }

func (c uidAssigningClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if obj.GetUID() == "" {
		obj.SetUID(types.UID("created-" + obj.GetName()))
	}
	return c.Client.Create(ctx, obj, opts...)
}

func newDurableFixture(t *testing.T, imageChanged bool) *durableFixture {
	t.Helper()
	namespace := "test"
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, clusterv1.AddToScheme(scheme))
	require.NoError(t, bootstrapv1.AddToScheme(scheme))
	require.NoError(t, apiextensionsv1.AddToScheme(scheme))
	m := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "machine", UID: "machine-uid", Annotations: map[string]string{clusterv1.UpdateInProgressAnnotation: ""}}, Spec: clusterv1.MachineSpec{ClusterName: "cluster", Version: "v1.37.0", ProviderID: "metal://node", Bootstrap: clusterv1.Bootstrap{DataSecretName: ptr.To("bootstrap"), ConfigRef: clusterv1.ContractVersionedObjectReference{APIGroup: bootstrapv1.GroupVersion.Group, Kind: "TalosConfig", Name: "config"}}, InfrastructureRef: clusterv1.ContractVersionedObjectReference{APIGroup: "infra.test", Kind: "MetalMachine", Name: "infra"}}, Status: clusterv1.MachineStatus{NodeRef: clusterv1.MachineNodeReference{Name: "node"}}}
	cfg := &bootstrapv1.TalosConfig{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "config", UID: "config-uid", Generation: 2, Labels: map[string]string{clusterv1.ClusterNameLabel: "cluster"}, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(m, clusterv1.GroupVersion.WithKind("Machine"))}, Annotations: map[string]string{clusterv1.UpdateInProgressAnnotation: ""}}, Spec: bootstrapv1.TalosConfigSpec{GenerateType: "worker", TalosVersion: "v1.14", TalosEndpoint: &bootstrapv1.TalosEndpoint{Address: "node:50000", ServerName: "node"}}, Status: bootstrapv1.TalosConfigStatus{DataSecretName: "bootstrap"}}
	cluster := &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "cluster", UID: "cluster-uid"}, Spec: clusterv1.ClusterSpec{ControlPlaneEndpoint: clusterv1.APIEndpoint{Host: "api.example", Port: 6443}}}
	infra := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "infra.test/v1beta1", "kind": "MetalMachine", "metadata": map[string]any{"name": "infra", "namespace": namespace, "uid": "infra-uid", "ownerReferences": []any{map[string]any{"apiVersion": clusterv1.GroupVersion.String(), "kind": "Machine", "name": m.Name, "uid": string(m.UID), "controller": true}}}, "spec": map[string]any{}, "status": map[string]any{}}}
	crd := &apiextensionsv1.CustomResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: "metalmachines.infra.test"}, Spec: apiextensionsv1.CustomResourceDefinitionSpec{Group: "infra.test", Names: apiextensionsv1.CustomResourceDefinitionNames{Kind: "MetalMachine", Plural: "metalmachines"}, Scope: apiextensionsv1.NamespaceScoped, Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{Name: "v1beta1", Served: true, Storage: true}}}}
	bundle, err := secrets.NewBundle(secrets.NewFixedClock(time.Now()), config.TalosVersion1_14)
	require.NoError(t, err)
	encoded, err := yaml.Marshal(bundle)
	require.NoError(t, err)
	generation := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "cluster-talos", UID: "bundle-uid"}, Data: map[string][]byte{"bundle": encoded}}
	workload := &corev1.Node{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Node"}, ObjectMeta: metav1.ObjectMeta{Name: "node", UID: "node-uid"}, Spec: corev1.NodeSpec{ProviderID: m.Spec.ProviderID}, Status: corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{KubeletVersion: m.Spec.Version}, Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api":
			json.NewEncoder(w).Encode(metav1.APIVersions{TypeMeta: metav1.TypeMeta{Kind: "APIVersions", APIVersion: "v1"}, Versions: []string{"v1"}})
		case "/apis":
			json.NewEncoder(w).Encode(metav1.APIGroupList{TypeMeta: metav1.TypeMeta{Kind: "APIGroupList", APIVersion: "v1"}})
		case "/api/v1":
			json.NewEncoder(w).Encode(metav1.APIResourceList{TypeMeta: metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"}, GroupVersion: "v1", APIResources: []metav1.APIResource{{Name: "nodes", Kind: "Node", Verbs: []string{"get", "list"}}}})
		case "/api/v1/nodes/node":
			json.NewEncoder(w).Encode(workload)
		case "/readyz":
			w.Write([]byte("ok"))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	kube := clientcmdapi.NewConfig()
	kube.Clusters["test"] = &clientcmdapi.Cluster{Server: server.URL}
	kube.AuthInfos["test"] = &clientcmdapi.AuthInfo{}
	kube.Contexts["test"] = &clientcmdapi.Context{Cluster: "test", AuthInfo: "test"}
	kube.CurrentContext = "test"
	value, err := clientcmd.Write(*kube)
	require.NoError(t, err)
	kubeSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "cluster-kubeconfig"}, Data: map[string][]byte{"value": value}}
	management := fake.NewClientBuilder().WithScheme(scheme).WithObjects(m, cfg, cluster, infra, crd, generation, kubeSecret).Build()
	resolved, err := configinput.Resolve(t.Context(), management, cfg)
	require.NoError(t, err)
	rendered, err := configinput.Render(resolved, configinput.RenderContext{Machine: m, Cluster: cluster})
	require.NoError(t, err)
	target := op.ImageIdentity{Reference: "factory.example/metal-installer/new-schematic:v1.14.0"}
	prior := target
	if imageChanged {
		prior.Reference = "factory.example/metal-installer/old-schematic:v1.14.0"
		cfg.Spec.StrategicPatches = []string{fmt.Sprintf("apiVersion: v1alpha1\nkind: UnattendedInstallConfig\ninstaller:\n  image: %s\nprovisioning:\n  diskSelector:\n    match: system_disk\n", target.Reference)}
		require.NoError(t, management.Update(t.Context(), cfg))
		resolved, err = configinput.Resolve(t.Context(), management, cfg)
		require.NoError(t, err)
		rendered, err = configinput.Render(resolved, configinput.RenderContext{Machine: m, Cluster: cluster})
		require.NoError(t, err)
	} else {
		prior = op.ImageIdentity{}
	}
	accepted, err := op.EncodeAcceptedIntent(op.AcceptedIntent{MachineUID: m.UID, TalosConfigUID: cfg.UID, Image: prior})
	require.NoError(t, err)
	output := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "bootstrap", UID: "bootstrap-uid", OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(cfg, bootstrapv1.GroupVersion.WithKind("TalosConfig"))}}, Data: map[string][]byte{"value": rendered.Data(), configinput.FreshnessKey: rendered.Freshness(), op.AcceptedIntentKey: accepted}}
	require.NoError(t, management.Create(t.Context(), output))
	node := &durableNode{observation: NodeObservation{Configuration: rendered.Data(), BootID: "boot-old", OSVersion: "v1.14.0", KubeletImage: "registry/kubelet:v1.37.0"}, outcome: op.NoReboot}
	handler := NewHandler(uidAssigningClient{management}, management, func(context.Context, types.NamespacedName, bootstrapv1.TalosEndpoint) (OperationNode, error) {
		return node, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { handler.Start(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	require.Eventually(t, func() bool {
		handler.runner.mu.Lock()
		defer handler.runner.mu.Unlock()
		return handler.runner.ctx != nil
	}, time.Second, time.Millisecond)
	request := &runtimehooks.UpdateMachineRequest{Desired: runtimehooks.UpdateMachineRequestObjects{Machine: *m.DeepCopy(), BootstrapConfig: runtime.RawExtension{Object: cfg.DeepCopy()}}}
	return &durableFixture{handler: handler, management: management, node: node, request: request, config: cfg, machine: m, cluster: cluster, output: output, workloadNode: workload}
}
func (f *durableFixture) poll(t *testing.T) *runtimehooks.UpdateMachineResponse {
	t.Helper()
	response := &runtimehooks.UpdateMachineResponse{}
	if err := f.handler.executeUpdate(t.Context(), f.request, response); err != nil {
		t.Fatalf("executor: %v", err)
	}
	return response
}
func (f *durableFixture) journal(t *testing.T) (*corev1.Secret, *op.Journal) {
	t.Helper()
	secret, j, err := op.ReadJournal(t.Context(), f.management, f.config.Namespace, objectIdentity(bootstrapv1.GroupVersion.Group, "TalosConfig", f.config))
	require.NoError(t, err)
	return secret, j
}
func TestDurableNoRebootUpdateDoesNotRepeatApply(t *testing.T) {
	f := newDurableFixture(t, false)
	require.Eventually(t, func() bool {
		response := f.poll(t)
		require.Equal(t, runtimehooks.ResponseStatusSuccess, response.Status, response.Message)
		return response.RetryAfterSeconds == 0
	}, 5*time.Second, 10*time.Millisecond)
	f.node.mu.Lock()
	require.Equal(t, 1, f.node.applyCount)
	require.Zero(t, f.node.installCount)
	f.node.mu.Unlock()
	_, j := f.journal(t)
	require.Equal(t, op.Observed, j.Phase)
	receipt, err := op.ReadReceipt(t.Context(), f.management, f.config.Namespace, j.TalosConfig, j.TransactionID)
	require.NoError(t, err)
	require.Equal(t, j.Machine.UID, receipt.MachineUID)
}
func TestSameVersionChangedIntentRunsOneDurableUpgradeAcrossPolls(t *testing.T) {
	f := newDurableFixture(t, true)
	f.node.installGate = make(chan struct{})
	require.Eventually(t, func() bool {
		response := f.poll(t)
		require.Equal(t, runtimehooks.ResponseStatusSuccess, response.Status, response.Message)
		f.node.mu.Lock()
		defer f.node.mu.Unlock()
		return f.node.installCount == 1
	}, 5*time.Second, 10*time.Millisecond)
	for range 3 {
		response := f.poll(t)
		require.Positive(t, response.RetryAfterSeconds)
	}
	f.node.mu.Lock()
	require.Equal(t, 1, f.node.installCount)
	require.Contains(t, f.node.installed, "@sha256:fixed")
	close(f.node.installGate)
	f.node.mu.Unlock()
	require.Eventually(t, func() bool {
		response := f.poll(t)
		require.Equal(t, runtimehooks.ResponseStatusSuccess, response.Status, response.Message)
		return response.RetryAfterSeconds == 0
	}, 5*time.Second, 10*time.Millisecond)
	f.node.mu.Lock()
	defer f.node.mu.Unlock()
	require.Equal(t, 1, f.node.applyCount)
	require.Equal(t, 1, f.node.installCount)
	require.Equal(t, 1, f.node.rebootCount)
}
