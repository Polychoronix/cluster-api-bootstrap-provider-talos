// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package inplace

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"

	cosiapi "github.com/cosi-project/runtime/api/v1alpha1"
	"github.com/cosi-project/runtime/pkg/resource"
	"github.com/cosi-project/runtime/pkg/state"
	"github.com/cosi-project/runtime/pkg/state/impl/inmem"
	"github.com/cosi-project/runtime/pkg/state/impl/namespaced"
	cosiserver "github.com/cosi-project/runtime/pkg/state/protobuf/server"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	"github.com/siderolabs/talos/pkg/machinery/config/configloader"
	"github.com/siderolabs/talos/pkg/machinery/config/generate"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	configresource "github.com/siderolabs/talos/pkg/machinery/resources/config"
	k8sresource "github.com/siderolabs/talos/pkg/machinery/resources/k8s"
	runtimeresource "github.com/siderolabs/talos/pkg/machinery/resources/runtime"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	runtimehooks "sigs.k8s.io/cluster-api/api/runtime/hooks/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type recoveringManagedNode struct {
	*durableNode
	workload *corev1.Node
}

func (n *recoveringManagedNode) Apply(ctx context.Context, data []byte) (op.ApplyOutcome, error) {
	outcome, err := n.durableNode.Apply(ctx, data)
	n.workload.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}
	return outcome, err
}

func activateDurableFixture(t *testing.T, f *durableFixture) {
	t.Helper()
	generation := &corev1.Secret{}
	require.NoError(t, f.management.Get(t.Context(), client.ObjectKey{Namespace: f.cluster.Namespace, Name: "cluster-talos"}, generation))
	bundle := &secrets.Bundle{}
	require.NoError(t, yaml.Unmarshal(generation.Data["bundle"], bundle))
	bundle.Clock = secrets.NewFixedClock(time.Now())
	input, err := generate.NewInput("cluster", "https://api.example:6443", "1.37.0", generate.WithSecretsBundle(bundle))
	require.NoError(t, err)
	talosconfig, err := input.Talosconfig()
	require.NoError(t, err)
	identity, err := talosconfig.Bytes()
	require.NoError(t, err)
	ca, err := tls.X509KeyPair(bundle.Certs.OS.Crt, bundle.Certs.OS.Key)
	require.NoError(t, err)
	parent, err := x509.ParseCertificate(ca.Certificate[0])
	require.NoError(t, err)
	public, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	cert := &x509.Certificate{SerialNumber: big.NewInt(12), Subject: pkix.Name{CommonName: "managed.test"}, DNSNames: []string{"managed.test"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, cert, parent, public, ca.PrivateKey)
	require.NoError(t, err)
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(bundle.Certs.OS.Crt))
	provider, err := configloader.NewFromBytes(f.seed.Data["value"])
	require.NoError(t, err)
	resources := state.WrapCore(namespaced.NewState(inmem.Build))
	boot := runtimeresource.NewBootID()
	boot.TypedSpec().BootID = f.node.observation.BootID
	f.workloadNode.Status.NodeInfo.BootID = boot.TypedSpec().BootID
	version := runtimeresource.NewVersion()
	version.TypedSpec().Version = "v1.14.0"
	kubelet := k8sresource.NewKubeletStatus(k8sresource.NamespaceName, "actual")
	kubelet.TypedSpec().Image = f.node.observation.KubeletImage
	for _, r := range []resource.Resource{configresource.NewMachineConfig(provider), boot, version, kubelet} {
		require.NoError(t, resources.Create(t.Context(), r))
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{{Certificate: [][]byte{der, parent.Raw}, PrivateKey: key}}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots})))
	cosiapi.RegisterStateServer(server, cosiserver.NewState(resources))
	go server.Serve(listener)
	t.Cleanup(func() { server.Stop(); listener.Close() })
	identitySecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: f.cluster.Namespace, Name: "cluster-talosconfig", UID: "imported-identity"}, Data: map[string][]byte{"talosconfig": identity}}
	require.NoError(t, f.management.Create(t.Context(), identitySecret))
	kube := &corev1.Secret{}
	require.NoError(t, f.management.Get(t.Context(), client.ObjectKey{Namespace: f.cluster.Namespace, Name: "cluster-kubeconfig"}, kube))
	kube.UID = "workload-identity"
	require.NoError(t, f.management.Update(t.Context(), kube))
	cfg := f.config.DeepCopy()
	desiredSpec := *cfg.Spec.DeepCopy()
	cfg.Spec = f.seedSpec
	cfg.Spec.TalosEndpoint = &bootstrapv1.TalosEndpoint{Address: listener.Addr().String(), ServerName: "managed.test"}
	cfg.Spec.Manage = true
	require.NoError(t, f.management.Update(t.Context(), cfg))
	a := op.ManagementAuthority{Namespace: cfg.Namespace, APIVersion: bootstrapv1.GroupVersion.String(), Kind: "TalosConfig", Name: cfg.Name, UID: cfg.UID}
	request := op.ManagementObservationRequest{Authority: a, Cluster: f.cluster, Manage: true, Machines: []*clusterv1.Machine{f.machine}, TalosConfigs: []*bootstrapv1.TalosConfig{cfg}}
	_, err = op.ActivateManagement(t.Context(), uidAssigningClient{f.management}, f.management, request)
	require.NoError(t, err)
	desiredSpec.Manage = true
	desiredSpec.TalosEndpoint = cfg.Spec.TalosEndpoint
	cfg.Spec = desiredSpec
	require.NoError(t, f.management.Update(t.Context(), cfg))
	inputs, err := configinput.Resolve(t.Context(), f.management, cfg)
	require.NoError(t, err)
	rendered, err := configinput.Render(inputs, configinput.RenderContext{Machine: f.machine, Cluster: f.cluster})
	require.NoError(t, err)
	desired := f.output.DeepCopy()
	desired.Data = map[string][]byte{"value": rendered.Data(), configinput.FreshnessKey: rendered.Freshness()}
	if desired.Name == f.seed.Name {
		desired.Name = "desired-config"
		desired.UID = "desired-uid"
		desired.ResourceVersion = ""
		require.NoError(t, f.management.Create(t.Context(), desired))
	} else {
		require.NoError(t, f.management.Update(t.Context(), desired))
	}
	cfg.Status.DataSecretName = desired.Name
	require.NoError(t, f.management.Update(t.Context(), cfg))
	f.output = desired
	f.config = cfg
	f.request.Desired.BootstrapConfig = runtime.RawExtension{Object: cfg.DeepCopy()}
}

func TestManagedRebootNotReadyRemainsPendingAndRecovers(t *testing.T) {
	f := newDurableFixture(t, false)
	f.node.outcome = op.RebootRequired
	recovering := &recoveringManagedNode{durableNode: f.node, workload: f.workloadNode}
	f.handler.newNodeClient = func(context.Context, types.NamespacedName, bootstrapv1.TalosEndpoint) (OperationNode, error) {
		return recovering, nil
	}
	f.poll(t)
	f.poll(t)
	require.Eventually(t, func() bool { f.node.mu.Lock(); defer f.node.mu.Unlock(); return f.node.applyCount == 1 }, time.Second, time.Millisecond)
	for range 4 {
		response := f.poll(t)
		require.Equal(t, runtimehooks.ResponseStatusSuccess, response.Status)
		require.Positive(t, response.RetryAfterSeconds)
		_, journal := f.journal(t)
		require.NotEqual(t, op.Ambiguous, journal.Phase)
	}
	secret, journal := f.journal(t)
	require.Empty(t, secret.Data[op.ReceiptKey])
	require.NotEqual(t, op.Observed, journal.Phase)
	f.node.mu.Lock()
	require.Equal(t, 1, f.node.applyCount)
	require.Zero(t, f.node.rebootCount)
	f.node.mu.Unlock()
	f.workloadNode.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
	require.Eventually(t, func() bool { return f.poll(t).RetryAfterSeconds == 0 }, 3*time.Second, time.Millisecond)
	_, journal = f.journal(t)
	require.Equal(t, op.Observed, journal.Phase)
	f.node.mu.Lock()
	require.Equal(t, 1, f.node.applyCount)
	f.node.mu.Unlock()
}

func TestManagedNotReadyPreflightDoesNotIssue(t *testing.T) {
	f := newDurableFixture(t, false)
	f.workloadNode.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}
	for range 3 {
		response := f.poll(t)
		require.Equal(t, runtimehooks.ResponseStatusSuccess, response.Status)
		require.Positive(t, response.RetryAfterSeconds)
	}
	f.node.mu.Lock()
	require.Zero(t, f.node.applyCount)
	require.Zero(t, f.node.rebootCount)
	f.node.mu.Unlock()
	_, journal := f.journal(t)
	require.Equal(t, op.Planned, journal.Phase)
	require.Equal(t, op.StepPlanned, journal.Steps[0].Phase)
	f.workloadNode.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
	require.Eventually(t, func() bool { return f.poll(t).RetryAfterSeconds == 0 }, 3*time.Second, time.Millisecond)
	f.node.mu.Lock()
	require.Equal(t, 1, f.node.applyCount)
	f.node.mu.Unlock()
}
