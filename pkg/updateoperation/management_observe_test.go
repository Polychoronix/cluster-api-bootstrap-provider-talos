// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	cosiapi "github.com/cosi-project/runtime/api/v1alpha1"
	"github.com/cosi-project/runtime/pkg/resource"
	"github.com/cosi-project/runtime/pkg/state"
	"github.com/cosi-project/runtime/pkg/state/impl/inmem"
	"github.com/cosi-project/runtime/pkg/state/impl/namespaced"
	cosiserver "github.com/cosi-project/runtime/pkg/state/protobuf/server"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/encoder"
	"github.com/siderolabs/talos/pkg/machinery/config/generate"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"
	configresource "github.com/siderolabs/talos/pkg/machinery/resources/config"
	k8sresource "github.com/siderolabs/talos/pkg/machinery/resources/k8s"
	runtimeresource "github.com/siderolabs/talos/pkg/machinery/resources/runtime"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/emptypb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type managementRPC struct {
	machineapi.UnimplementedMachineServiceServer
	members []*machineapi.EtcdMember
	self    uint64
	leader  uint64
	nodeUID types.UID
}

func (s *managementRPC) EtcdMemberList(context.Context, *machineapi.EtcdMemberListRequest) (*machineapi.EtcdMemberListResponse, error) {
	return &machineapi.EtcdMemberListResponse{Messages: []*machineapi.EtcdMembers{{Members: s.members}}}, nil
}
func (s *managementRPC) EtcdStatus(context.Context, *emptypb.Empty) (*machineapi.EtcdStatusResponse, error) {
	leader := s.leader
	if leader == 0 {
		leader = 1
	}
	return &machineapi.EtcdStatusResponse{Messages: []*machineapi.EtcdStatus{{MemberStatus: &machineapi.EtcdMemberStatus{MemberId: s.self, Leader: leader, RaftTerm: 2, RaftAppliedIndex: 3}}}}, nil
}
func (s *managementRPC) ServiceList(context.Context, *emptypb.Empty) (*machineapi.ServiceListResponse, error) {
	return &machineapi.ServiceListResponse{Messages: []*machineapi.ServiceList{{Services: []*machineapi.ServiceInfo{{Id: "etcd", Health: &machineapi.ServiceHealth{Healthy: true}}}}}}, nil
}

func managementTLSFixture(t *testing.T, count int) (*assigningClient, ManagementObservationRequest, []*managementRPC) {
	t.Helper()
	bundle, e := secrets.NewBundle(secrets.NewFixedClock(time.Now()), config.TalosVersion1_14)
	require.NoError(t, e)
	input, e := generate.NewInput("cluster", "https://api.example.test:6443", "1.37.0", generate.WithVersionContract(config.TalosVersion1_14), generate.WithSecretsBundle(bundle))
	require.NoError(t, e)
	input.PodNet = []string{"10.244.0.0/16"}
	input.ServiceNet = []string{"10.96.0.0/12"}
	talosConfig, e := input.Talosconfig()
	require.NoError(t, e)
	identity, e := talosConfig.Bytes()
	require.NoError(t, e)
	// The generated Talos client certificate cannot serve as a server certificate.
	// Issue an actual server certificate with the imported issuing CA/key.
	ca, e := tls.X509KeyPair(bundle.Certs.OS.Crt, bundle.Certs.OS.Key)
	require.NoError(t, e)
	parent, e := x509.ParseCertificate(ca.Certificate[0])
	require.NoError(t, e)
	serverCertificate := managementServerCertificate(t, parent, ca.PrivateKey)
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(bundle.Certs.OS.Crt))
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, bootstrapv1.AddToScheme(scheme))
	require.NoError(t, clusterv1.AddToScheme(scheme))
	cluster := &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "cluster", UID: "cluster-uid"}}
	cluster.Spec.ControlPlaneEndpoint = clusterv1.APIEndpoint{Host: "api.example.test", Port: 6443}
	cluster.Spec.ClusterNetwork.Pods.CIDRBlocks = input.PodNet
	cluster.Spec.ClusterNetwork.Services.CIDRBlocks = input.ServiceNet
	spec := &bootstrapv1.ManagementSpec{Mode: bootstrapv1.ManagementModeObserve, InventoryRevision: "inventory-1"}
	authority := ManagementAuthority{Namespace: "tenant", APIVersion: "controlplane.cluster.x-k8s.io/v1beta1", Kind: "TalosControlPlane", Name: "controlplane", UID: "authority-uid"}
	tcp := &unstructured.Unstructured{Object: map[string]any{"apiVersion": authority.APIVersion, "kind": authority.Kind, "metadata": map[string]any{"namespace": authority.Namespace, "name": authority.Name, "uid": string(authority.UID)}}}
	objects := []client.Object{cluster, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "cluster-talosconfig", UID: "identity-uid"}, Data: map[string][]byte{"talosconfig": identity}}}
	crd := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition", "metadata": map[string]any{"name": "metalmachines.infrastructure.example.test"}, "spec": map[string]any{"group": "infrastructure.example.test", "names": map[string]any{"kind": "MetalMachine"}, "versions": []any{map[string]any{"name": "v1beta1", "served": true, "storage": true}}}}}
	objects = append(objects, crd)
	request := ManagementObservationRequest{Authority: authority, Cluster: cluster, Spec: spec}
	nodes := []corev1.Node{}
	servers := []*managementRPC{}
	memberView := []*machineapi.EtcdMember{}
	for i := 0; i < count; i++ {
		memberView = append(memberView, &machineapi.EtcdMember{Id: uint64(i + 1), Hostname: fmt.Sprintf("node-%d", i), PeerUrls: []string{fmt.Sprintf("https://node-%d:2380", i)}, ClientUrls: []string{fmt.Sprintf("https://node-%d:2379", i)}})
	}
	for i := 0; i < count; i++ {
		name := fmt.Sprintf("node-%d", i)
		uid := types.UID(fmt.Sprintf("machine-%d", i))
		controller := true
		m := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: name, UID: uid, Labels: map[string]string{clusterv1.MachineControlPlaneLabel: ""}}}
		m.Spec.ClusterName = cluster.Name
		m.Spec.ProviderID = "metal://" + name
		m.Spec.Version = "v1.37.0"
		m.Spec.InfrastructureRef = clusterv1.ContractVersionedObjectReference{APIGroup: "infrastructure.example.test", Kind: "MetalMachine", Name: name}
		m.Spec.Bootstrap.ConfigRef = clusterv1.ContractVersionedObjectReference{APIGroup: bootstrapv1.GroupVersion.Group, Kind: "TalosConfig", Name: name}
		seedName := name + "-original"
		m.Spec.Bootstrap.DataSecretName = &seedName
		m.Status.NodeRef = clusterv1.MachineNodeReference{Name: name}
		owner := metav1.OwnerReference{APIVersion: clusterv1.GroupVersion.String(), Kind: "Machine", Name: name, UID: uid, Controller: &controller}
		node := corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID("node-uid-" + name), Labels: map[string]string{"node-role.kubernetes.io/control-plane": ""}}, Spec: corev1.NodeSpec{ProviderID: m.Spec.ProviderID}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}, NodeInfo: corev1.NodeSystemInfo{KubeletVersion: "v1.37.0", BootID: "ba144cb9-b310-43f2-8126-d3e7313791a6"}}}
		nodes = append(nodes, node)
		provider, e := input.Config(machine.TypeControlPlane)
		require.NoError(t, e)
		data, e := provider.EncodeBytes(encoder.WithComments(encoder.CommentsDisabled))
		require.NoError(t, e)
		resources := state.WrapCore(namespaced.NewState(inmem.Build))
		boot := runtimeresource.NewBootID()
		boot.TypedSpec().BootID = node.Status.NodeInfo.BootID
		version := runtimeresource.NewVersion()
		version.TypedSpec().Version = "v1.14.2"
		kubelet := k8sresource.NewKubeletStatus(k8sresource.NamespaceName, "kubelet")
		kubelet.TypedSpec().Image = "registry.k8s.io/kubelet:v1.37.0"
		for _, r := range []resource.Resource{configresource.NewMachineConfig(provider), boot, version, kubelet} {
			require.NoError(t, resources.Create(t.Context(), r))
		}
		rpc := &managementRPC{members: memberView, self: uint64(i + 1)}
		servers = append(servers, rpc)
		listener, e := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, e)
		server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{serverCertificate}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots})))
		cosiapi.RegisterStateServer(server, cosiserver.NewState(resources))
		machineapi.RegisterMachineServiceServer(server, rpc)
		go server.Serve(listener)
		t.Cleanup(func() { server.Stop(); listener.Close() })
		tc := &bootstrapv1.TalosConfig{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: name, UID: types.UID("tc-" + name), OwnerReferences: []metav1.OwnerReference{owner}}, Spec: bootstrapv1.TalosConfigSpec{GenerateType: "controlplane", TalosVersion: "v1.14.2", ConfigSecretRef: &bootstrapv1.SecretKeyReference{Name: name + "-baseline", Key: "config"}, TalosEndpoint: &bootstrapv1.TalosEndpoint{Address: listener.Addr().String(), ServerName: "talos.example.test"}}}
		infra := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "infrastructure.example.test/v1beta1", "kind": "MetalMachine", "metadata": map[string]any{"namespace": "tenant", "name": name, "uid": "infra-" + name}, "spec": map[string]any{"providerID": m.Spec.ProviderID}}}
		infra.SetOwnerReferences([]metav1.OwnerReference{owner})
		objects = append(objects, m, tc, infra, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: name + "-baseline", UID: types.UID("baseline-" + name)}, Data: map[string][]byte{"config": data}}, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: seedName, UID: types.UID("seed-" + name)}, Data: map[string][]byte{"value": data}})
		request.Machines = append(request.Machines, m)
		request.TalosConfigs = append(request.TalosConfigs, tc)
		spec.Machines = append(spec.Machines, bootstrapv1.MachineIdentityReference{Name: name, UID: uid})
	}
	workload := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api" {
			json.NewEncoder(w).Encode(map[string]any{"kind": "APIVersions", "apiVersion": "v1", "versions": []string{"v1"}})
			return
		}
		if r.URL.Path == "/apis" {
			json.NewEncoder(w).Encode(map[string]any{"kind": "APIGroupList", "apiVersion": "v1", "groups": []any{}})
			return
		}
		if r.URL.Path == "/api/v1" {
			json.NewEncoder(w).Encode(map[string]any{"kind": "APIResourceList", "apiVersion": "v1", "groupVersion": "v1", "resources": []any{map[string]any{"name": "nodes", "singularName": "node", "namespaced": false, "kind": "Node", "verbs": []string{"get", "list"}}}})
			return
		}
		if r.URL.Path == "/readyz" {
			w.Write([]byte("ok"))
			return
		}
		if strings.HasSuffix(r.URL.Path, "/nodes") {
			json.NewEncoder(w).Encode(&corev1.NodeList{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "NodeList"}, Items: nodes})
			return
		}
		for i, n := range nodes {
			if strings.HasSuffix(r.URL.Path, "/nodes/"+n.Name) {
				if servers[i].nodeUID != "" {
					n.UID = servers[i].nodeUID
				}
				n.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Node"}
				json.NewEncoder(w).Encode(n)
				return
			}
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(workload.Close)
	kube := fmt.Sprintf("apiVersion: v1\nkind: Config\nclusters:\n- name: cluster\n  cluster:\n    server: %s\n    insecure-skip-tls-verify: true\ncontexts:\n- name: cluster\n  context:\n    cluster: cluster\n    user: user\ncurrent-context: cluster\nusers:\n- name: user\n  user: {}\n", workload.URL)
	objects = append(objects, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "cluster-kubeconfig", UID: "kubeconfig-uid"}, Data: map[string][]byte{"value": []byte(kube)}})
	managementMap, e := runtime.DefaultUnstructuredConverter.ToUnstructured(spec)
	require.NoError(t, e)
	tcp.Object["spec"] = map[string]any{"management": managementMap}
	objects = append(objects, tcp)
	management := &assigningClient{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()}
	setEffectivePolicy(t, management, management, &request, false, false)
	return management, request, servers
}

func TestObserveManagementUnownedTCPGenericTopologyAndApproval(t *testing.T) {
	for _, count := range []int{1, 5} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			c, request, servers := managementTLSFixture(t, count)
			o, e := ObserveManagement(t.Context(), c, request)
			require.NoError(t, e)
			require.Len(t, o.safety.Etcd, count)
			require.True(t, o.Summary().InventoryVerified)
			first, e := RecordManagementObservation(t.Context(), c, c, o)
			require.NoError(t, e)
			request.Spec.Mode = bootstrapv1.ManagementModeManaged
			request.Spec.Approval = &bootstrapv1.ObservationApproval{Revision: request.Spec.InventoryRevision, ObservationID: first.Observation.ID}
			tcp := &unstructured.Unstructured{}
			tcp.SetAPIVersion(request.Authority.APIVersion)
			tcp.SetKind(request.Authority.Kind)
			require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: "tenant", Name: "controlplane"}, tcp))
			m, e := runtime.DefaultUnstructuredConverter.ToUnstructured(request.Spec)
			require.NoError(t, e)
			tcp.Object["spec"].(map[string]any)["management"] = m
			tcp.SetGeneration(tcp.GetGeneration() + 1)
			tcp.SetAnnotations(map[string]string{"reconcile.fluxcd.io/requestedAt": "approve"})
			require.NoError(t, c.Update(t.Context(), tcp))
			activated, e := associateAll(t, c, c, request)
			require.NoError(t, e)
			require.Equal(t, first.Observation.ID, activated.Observation.ID)
			require.Equal(t, "inventory-1", activated.ActivatedRevision)
			servers[0].members = append(servers[0].members, &machineapi.EtcdMember{Id: 100, Hostname: "unaccounted", PeerUrls: []string{"https://extra:2380"}, ClientUrls: []string{"https://extra:2379"}})
			_, e = ObserveManagement(t.Context(), c, request)
			require.Error(t, e)
		})
	}
}

func managementServerCertificate(t *testing.T, parent *x509.Certificate, key any) tls.Certificate {
	t.Helper()
	public, private, e := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, e)
	template := &x509.Certificate{SerialNumber: big.NewInt(42), Subject: pkix.Name{CommonName: "talos.example.test"}, DNSNames: []string{"talos.example.test"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, e := x509.CreateCertificate(rand.Reader, template, parent, public, key)
	require.NoError(t, e)
	return tls.Certificate{Certificate: [][]byte{der, parent.Raw}, PrivateKey: private}
}
