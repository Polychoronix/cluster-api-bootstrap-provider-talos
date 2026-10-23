// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package controllers

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"math/big"
	"net"
	"os"
	"path/filepath"
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
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/configloader"
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
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	runtime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

func TestManagementControllerObserveApproveAndDesiredOutputDisposableAPI(t *testing.T) {
	assets := os.Getenv("KUBEBUILDER_ASSETS")
	if assets == "" || testing.Short() {
		t.Skip("explicit disposable management API assets required")
	}
	capiCRDs := filepath.Join(os.Getenv("HOME"), "go", "pkg", "mod", "sigs.k8s.io", "cluster-api@v1.14.2", "core", "config", "crd", "bases")
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, clusterv1.AddToScheme(scheme))
	require.NoError(t, bootstrapv1.AddToScheme(scheme))
	require.NoError(t, apiextensionsv1.AddToScheme(scheme))
	infraCRD := &apiextensionsv1.CustomResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: "metalmachines.infrastructure.example.test"}, Spec: apiextensionsv1.CustomResourceDefinitionSpec{Group: "infrastructure.example.test", Names: apiextensionsv1.CustomResourceDefinitionNames{Kind: "MetalMachine", Plural: "metalmachines"}, Scope: apiextensionsv1.NamespaceScoped, Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{Name: "v1beta1", Served: true, Storage: true, Schema: &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{Type: "object", Properties: map[string]apiextensionsv1.JSONSchemaProps{"spec": {Type: "object", XPreserveUnknownFields: new(true)}}}}}}}}
	env := &envtest.Environment{UseExistingCluster: new(false), BinaryAssetsDirectory: assets, CRDDirectoryPaths: []string{filepath.Join("..", "config", "crd", "bases"), capiCRDs}, CRDs: []*apiextensionsv1.CustomResourceDefinition{infraCRD}}
	cfg, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })
	api, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	require.NoError(t, api.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "management-smoke"}}))
	bundle, err := secrets.NewBundle(secrets.NewFixedClock(time.Now()), config.TalosVersion1_14)
	require.NoError(t, err)
	input, err := generate.NewInput("cluster", "https://api.example.test:6443", "1.37.0", generate.WithVersionContract(config.TalosVersion1_14), generate.WithSecretsBundle(bundle))
	require.NoError(t, err)
	provider, err := input.Config(machine.TypeWorker)
	require.NoError(t, err)
	baselineBytes, err := provider.EncodeBytes(encoder.WithComments(encoder.CommentsDisabled))
	require.NoError(t, err)
	resources := state.WrapCore(namespaced.NewState(inmem.Build))
	boot := runtimeresource.NewBootID()
	boot.TypedSpec().BootID = "ba144cb9-b310-43f2-8126-d3e7313791a6"
	version := runtimeresource.NewVersion()
	version.TypedSpec().Version = "v1.14.2"
	kubelet := k8sresource.NewKubeletStatus(k8sresource.NamespaceName, "actual")
	kubelet.TypedSpec().Image = "registry.k8s.io/kubelet:v1.37.0"
	for _, r := range []resource.Resource{configresource.NewMachineConfig(provider), boot, version, kubelet} {
		require.NoError(t, resources.Create(t.Context(), r))
	}
	ca, err := tls.X509KeyPair(bundle.Certs.OS.Crt, bundle.Certs.OS.Key)
	require.NoError(t, err)
	parent, err := x509.ParseCertificate(ca.Certificate[0])
	require.NoError(t, err)
	public, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{SerialNumber: big.NewInt(5), Subject: pkix.Name{CommonName: "talos.example.test"}, DNSNames: []string{"talos.example.test"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, parent, public, ca.PrivateKey)
	require.NoError(t, err)
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(bundle.Certs.OS.Crt))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{{Certificate: [][]byte{der, parent.Raw}, PrivateKey: private}}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots})))
	cosiapi.RegisterStateServer(server, cosiserver.NewState(resources))
	go server.Serve(listener)
	t.Cleanup(func() { server.Stop(); listener.Close() })
	cluster := &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: "management-smoke", Name: "cluster"}, Spec: clusterv1.ClusterSpec{ControlPlaneEndpoint: clusterv1.APIEndpoint{Host: "api.example.test", Port: 6443}}}
	require.NoError(t, api.Create(t.Context(), cluster))
	cluster.Status.Initialization.InfrastructureProvisioned = new(true)
	require.NoError(t, api.Status().Update(t.Context(), cluster))
	m := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: cluster.Namespace, Name: "worker"}, Spec: clusterv1.MachineSpec{ClusterName: cluster.Name, Version: "v1.37.0", ProviderID: "metal://worker", Bootstrap: clusterv1.Bootstrap{DataSecretName: new("original-seed"), ConfigRef: clusterv1.ContractVersionedObjectReference{APIGroup: bootstrapv1.GroupVersion.Group, Kind: "TalosConfig", Name: "worker"}}, InfrastructureRef: clusterv1.ContractVersionedObjectReference{APIGroup: "infrastructure.example.test", Kind: "MetalMachine", Name: "worker"}}}
	require.NoError(t, api.Create(t.Context(), m))
	m.Status.NodeRef = clusterv1.MachineNodeReference{Name: "worker"}
	require.NoError(t, api.Status().Update(t.Context(), m))
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker"}, Spec: corev1.NodeSpec{ProviderID: m.Spec.ProviderID}}
	require.NoError(t, api.Create(t.Context(), node))
	node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
	node.Status.NodeInfo.KubeletVersion = "v1.37.0"
	node.Status.NodeInfo.BootID = boot.TypedSpec().BootID
	require.NoError(t, api.Status().Update(t.Context(), node))
	infra := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "infrastructure.example.test/v1beta1", "kind": "MetalMachine", "metadata": map[string]any{"namespace": m.Namespace, "name": m.Name}, "spec": map[string]any{"providerID": m.Spec.ProviderID}}}
	infra.SetOwnerReferences([]metav1.OwnerReference{*metav1.NewControllerRef(m, clusterv1.GroupVersion.WithKind("Machine"))})
	require.NoError(t, api.Create(t.Context(), infra))
	tc := &bootstrapv1.TalosConfig{ObjectMeta: metav1.ObjectMeta{Namespace: m.Namespace, Name: m.Name, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(m, clusterv1.GroupVersion.WithKind("Machine"))}}, Spec: bootstrapv1.TalosConfigSpec{GenerateType: "worker", TalosVersion: "v1.14.2", ConfigSecretRef: &bootstrapv1.SecretKeyReference{Name: "baseline", Key: "config"}, TalosEndpoint: &bootstrapv1.TalosEndpoint{Address: listener.Addr().String(), ServerName: "talos.example.test"}, Management: &bootstrapv1.ManagementSpec{Mode: bootstrapv1.ManagementModeObserve, InventoryRevision: "import-1", Machines: []bootstrapv1.MachineIdentityReference{{Name: m.Name, UID: m.UID}}}}}
	require.NoError(t, api.Create(t.Context(), tc))
	seed := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: m.Namespace, Name: "original-seed"}, Data: map[string][]byte{"value": baselineBytes}}
	require.NoError(t, api.Create(t.Context(), seed))
	identity := importedIdentitySecret(t, &TalosConfigScope{Config: tc, Cluster: cluster}, bundle)
	require.NoError(t, api.Create(t.Context(), identity))
	identityBefore := identity.DeepCopy()
	kube := clientcmdapi.Config{Clusters: map[string]*clientcmdapi.Cluster{"workload": {Server: cfg.Host, CertificateAuthorityData: cfg.CAData}}, AuthInfos: map[string]*clientcmdapi.AuthInfo{"client": {ClientCertificateData: cfg.CertData, ClientKeyData: cfg.KeyData}}, Contexts: map[string]*clientcmdapi.Context{"workload": {Cluster: "workload", AuthInfo: "client"}}, CurrentContext: "workload"}
	kubeBytes, err := clientcmd.Write(kube)
	require.NoError(t, err)
	require.NoError(t, api.Create(t.Context(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: m.Namespace, Name: cluster.Name + "-kubeconfig"}, Data: map[string][]byte{"value": kubeBytes}}))
	manager, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: "0", LeaderElection: false})
	require.NoError(t, err)
	reconciler := &TalosConfigReconciler{Client: manager.GetClient(), Scheme: scheme, Log: ctrl.Log}
	ctx, cancel := context.WithCancel(t.Context())
	require.NoError(t, reconciler.SetupWithManager(ctx, manager, controller.Options{MaxConcurrentReconciles: 1}))
	done := make(chan error, 1)
	go func() { done <- manager.Start(ctx) }()
	t.Cleanup(func() { cancel(); require.NoError(t, <-done) })
	require.Eventually(t, func() bool {
		live := &bootstrapv1.TalosConfig{}
		if api.Get(t.Context(), client.ObjectKeyFromObject(tc), live) != nil {
			return false
		}
		for _, c := range live.Status.Conditions {
			if c.Type == bootstrapv1.InventoryVerifiedCondition && c.Status == metav1.ConditionFalse {
				return true
			}
		}
		return false
	}, 20*time.Second, 50*time.Millisecond)
	// Correct a missing Secret without changing TC/requestedAt: existing bounded retry must observe it.
	require.NoError(t, api.Create(t.Context(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: m.Namespace, Name: "baseline"}, Data: map[string][]byte{"config": baselineBytes}}))
	require.Eventually(t, func() bool {
		if api.Get(t.Context(), client.ObjectKeyFromObject(tc), tc) != nil {
			return false
		}
		return tc.Status.Management != nil && tc.Status.Management.Observation != nil
	}, 30*time.Second, 50*time.Millisecond)
	id := tc.Status.Management.Observation.ID
	require.Empty(t, tc.Status.DataSecretName)
	tc.Spec.Management.Mode = bootstrapv1.ManagementModeManaged
	tc.Spec.Management.Approval = &bootstrapv1.ObservationApproval{Revision: "import-1", ObservationID: id}
	require.NoError(t, api.Update(t.Context(), tc))
	require.Eventually(t, func() bool {
		return api.Get(t.Context(), client.ObjectKeyFromObject(tc), tc) == nil && tc.Status.Management.ActivatedRevision == "import-1"
	}, 20*time.Second, 50*time.Millisecond)
	require.Equal(t, id, tc.Status.Management.Observation.ID)
	tc.Annotations = map[string]string{clusterv1.UpdateInProgressAnnotation: ""}
	tc.Spec.ConfigRevision = "registry-1"
	tc.Spec.StrategicPatches = []string{"machine:\n  env:\n    MANAGEMENT_PRIVATE_SENTINEL: safe\n"}
	require.NoError(t, api.Update(t.Context(), tc))
	output := &corev1.Secret{}
	require.Eventually(t, func() bool {
		return api.Get(t.Context(), client.ObjectKey{Namespace: m.Namespace, Name: tc.Name + "-update-data"}, output) == nil && strings.Contains(string(output.Data["value"]), "MANAGEMENT_PRIVATE_SENTINEL")
	}, 20*time.Second, 50*time.Millisecond)
	accepted, err := op.DecodeAcceptedIntent(output.Data[op.AcceptedIntentKey])
	require.NoError(t, err)
	require.Equal(t, m.UID, accepted.MachineUID)
	require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(seed), seed))
	require.Equal(t, baselineBytes, seed.Data["value"])
	require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(identity), identity))
	require.Equal(t, identityBefore.Data, identity.Data)
	parsed, err := configloader.NewFromBytes(output.Data["value"])
	require.NoError(t, err)
	require.Equal(t, "safe", parsed.Machine().Env()["MANAGEMENT_PRIVATE_SENTINEL"])
	publicJSON, err := json.Marshal(tc.Status)
	require.NoError(t, err)
	require.NotContains(t, string(publicJSON), "MANAGEMENT_PRIVATE_SENTINEL")
}
