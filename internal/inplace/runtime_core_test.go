// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package inplace_test

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	cosiapi "github.com/cosi-project/runtime/api/v1alpha1"
	"github.com/cosi-project/runtime/pkg/resource"
	"github.com/cosi-project/runtime/pkg/state"
	"github.com/cosi-project/runtime/pkg/state/impl/inmem"
	"github.com/cosi-project/runtime/pkg/state/impl/namespaced"
	cosiserver "github.com/cosi-project/runtime/pkg/state/protobuf/server"
	"github.com/go-logr/logr"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	"github.com/siderolabs/talos/pkg/machinery/config/configloader"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	configresource "github.com/siderolabs/talos/pkg/machinery/resources/config"
	k8sresource "github.com/siderolabs/talos/pkg/machinery/resources/k8s"
	runtimeresource "github.com/siderolabs/talos/pkg/machinery/resources/runtime"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/types/known/emptypb"
	corev1 "k8s.io/api/core/v1"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/watch"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	metadataclient "k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	clusterv1beta1 "sigs.k8s.io/cluster-api/api/core/v1beta1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	runtimecatalog "sigs.k8s.io/cluster-api/api/runtime/catalog"
	hooks "sigs.k8s.io/cluster-api/api/runtime/hooks/v1alpha1"
	runtimev1alpha1 "sigs.k8s.io/cluster-api/api/runtime/v1alpha1"
	runtimev1 "sigs.k8s.io/cluster-api/api/runtime/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/kubeconfig"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	bootstrapv1alpha3 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1alpha3"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/controllers"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/internal/imagefactory"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/internal/inplace"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
)

const runtimeCorePrivate = "runtime-core-private-registry-never-public"
const runtimeCoreNodeSNI = "talos-runtime-core.example.test"
const runtimeCoreKubeVersion = "v1.35.0"
const runtimeCoreOSVersion = "v1.14.0"
const runtimeCoreNextOSVersion = "v1.14.2"
const runtimeCoreSchematic = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const runtimeCoreNextSchematic = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
const runtimeCoreDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

// Explicit opt-in assets are never downloaded by tests. The source/binary prelude
// is documented in the integration recipe; these tests cannot use ambient clusters.
func runtimeCoreInputs(t *testing.T) (assets, source, binary, caph, repo string) {
	t.Helper()
	assets = os.Getenv("KUBEBUILDER_ASSETS")
	binary = os.Getenv("CAPI_RUNTIME_CORE_BINARY")
	source = os.Getenv("CAPI_RUNTIME_CORE_SOURCE")
	caph = os.Getenv("CAPI_RUNTIME_CAPH_CRDS")
	if assets == "" && binary == "" && source == "" && caph == "" {
		t.Skip("set KUBEBUILDER_ASSETS, CAPI_RUNTIME_CORE_BINARY, CAPI_RUNTIME_CORE_SOURCE and CAPI_RUNTIME_CAPH_CRDS for isolated actual-core integration")
	}
	for name, value := range map[string]string{"KUBEBUILDER_ASSETS": assets, "CAPI_RUNTIME_CORE_BINARY": binary, "CAPI_RUNTIME_CORE_SOURCE": source, "CAPI_RUNTIME_CAPH_CRDS": caph} {
		require.NotEmpty(t, value, "all explicit local prerequisites required: "+name)
		require.True(t, filepath.IsAbs(value), name+" must be absolute")
	}
	for _, p := range []string{binary, filepath.Join(assets, "kube-apiserver"), filepath.Join(assets, "etcd"), filepath.Join(source, "core/config/crd/bases/cluster.x-k8s.io_machines.yaml"), filepath.Join(caph, "infrastructure.cluster.x-k8s.io_hetznerbaremetalmachines.yaml")} {
		_, err := os.Stat(p)
		require.NoError(t, err)
	}
	_, filename, _, ok := goruntime.Caller(0)
	require.True(t, ok)
	repo = filepath.Dir(filepath.Dir(filepath.Dir(filename)))
	return
}

type runtimeCoreLogBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}
type runtimeCoreLog struct {
	shared *runtimeCoreLogBuffer
	values []any
	name   string
}

func (l *runtimeCoreLog) root() *runtimeCoreLogBuffer {
	if l.shared == nil {
		l.shared = &runtimeCoreLogBuffer{}
	}
	return l.shared
}
func (l *runtimeCoreLog) Write(data []byte) (int, error) {
	b := l.root()
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(data)
}
func (l *runtimeCoreLog) String() string {
	b := l.root()
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}
func (l *runtimeCoreLog) Init(logr.RuntimeInfo) { l.root() }
func (l *runtimeCoreLog) Enabled(int) bool      { return true }
func (l *runtimeCoreLog) Info(_ int, message string, kv ...any) {
	_, _ = fmt.Fprintln(l, l.name, message, append(append([]any(nil), l.values...), kv...))
}
func (l *runtimeCoreLog) Error(err error, message string, kv ...any) {
	_, _ = fmt.Fprintln(l, l.name, message, err, append(append([]any(nil), l.values...), kv...))
}
func (l *runtimeCoreLog) WithValues(kv ...any) logr.LogSink {
	return &runtimeCoreLog{shared: l.root(), values: append(append([]any(nil), l.values...), kv...), name: l.name}
}
func (l *runtimeCoreLog) WithName(name string) logr.LogSink {
	joined := name
	if l.name != "" {
		joined = l.name + "/" + name
	}
	return &runtimeCoreLog{shared: l.root(), values: append([]any(nil), l.values...), name: joined}
}

func runtimeCoreScheme(t *testing.T) *kruntime.Scheme {
	t.Helper()
	s := kruntime.NewScheme()
	for _, add := range []func(*kruntime.Scheme) error{clientgoscheme.AddToScheme, apiextv1.AddToScheme, clusterv1.AddToScheme, clusterv1beta1.AddToScheme, runtimev1.AddToScheme, runtimev1alpha1.AddToScheme, bootstrapv1.AddToScheme, bootstrapv1alpha3.AddToScheme} {
		require.NoError(t, add(s))
	}
	return s
}
func runtimeCoreEventually(t *testing.T, what string, duration time.Duration, f func() error) {
	t.Helper()
	deadline := time.Now().Add(duration)
	var err error
	for time.Now().Before(deadline) {
		if err = f(); err == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
	t.Fatal(what + ": deadline reached")
}
func runtimeCoreFreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return p
}
func runtimeCoreObject(groupVersion, kind, namespace, name string, spec map[string]any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{"apiVersion": groupVersion, "kind": kind, "metadata": map[string]any{"namespace": namespace, "name": name}, "spec": spec}}
}

func runtimeCoreCA(t *testing.T) (*x509.Certificate, crypto.Signer, []byte) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "runtime-core synthetic CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, key)
	require.NoError(t, err)
	crt, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return crt, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
func runtimeCoreCertificate(t *testing.T, ca *x509.Certificate, key crypto.Signer, name string, usage x509.ExtKeyUsage) (tls.Certificate, []byte, []byte) {
	t.Helper()
	pub, childKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	require.NoError(t, err)
	tmpl := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: name}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}}
	if usage == x509.ExtKeyUsageServerAuth {
		if ip := net.ParseIP(name); ip != nil {
			tmpl.IPAddresses = []net.IP{ip}
		} else {
			tmpl.DNSNames = []string{name}
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, pub, key)
	require.NoError(t, err)
	pk, err := x509.MarshalPKCS8PrivateKey(childKey)
	require.NoError(t, err)
	cp := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	kp := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pk})
	cert, err := tls.X509KeyPair(cp, kp)
	require.NoError(t, err)
	return cert, cp, kp
}
func runtimeCoreCertDir(t *testing.T) (string, []byte) {
	t.Helper()
	ca, key, pemCA := runtimeCoreCA(t)
	_, crt, pk := runtimeCoreCertificate(t, ca, key, "127.0.0.1", x509.ExtKeyUsageServerAuth)
	dir := t.TempDir()
	for n, b := range map[string][]byte{"tls.crt": crt, "tls.key": pk, "ca.crt": pemCA} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, n), b, 0600))
	}
	return dir, pemCA
}

// The external node is a controlled gRPC/COSI simulation; production NodeClient
// remains real, mTLS-authenticated and routed. No physical/CAPH success is claimed.
type runtimeCoreNode struct {
	machineapi.UnimplementedMachineServiceServer
	mu                                  sync.Mutex
	resources                           state.State
	active                              []byte
	boot, osVersion, schematic, kubelet string
	extension                           bool
	imageRef                            string
	pulledRefs                          []string
	installedRef                        string
	applies, pulls, installs, reboots   int
	installStarted                      chan struct{}
	installAck                          chan struct{}
	pendingConfig                       []byte
	peerAuthenticated                   bool
}

func runtimeCoreNodeNew(t *testing.T) *runtimeCoreNode {
	t.Helper()
	return &runtimeCoreNode{resources: state.WrapCore(namespaced.NewState(inmem.Build)), boot: "boot-initial", osVersion: runtimeCoreOSVersion, schematic: runtimeCoreSchematic, kubelet: "ghcr.io/siderolabs/kubelet:" + runtimeCoreKubeVersion, installStarted: make(chan struct{}), installAck: make(chan struct{})}
}
func (n *runtimeCoreNode) Version(ctx context.Context, _ *emptypb.Empty) (*machineapi.VersionResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if p, ok := peer.FromContext(ctx); ok {
		if auth, ok := p.AuthInfo.(credentials.TLSInfo); ok {
			n.peerAuthenticated = len(auth.State.VerifiedChains) > 0 && len(auth.State.PeerCertificates) > 0 && auth.State.ServerName == runtimeCoreNodeSNI
		}
	}
	return &machineapi.VersionResponse{Messages: []*machineapi.Version{{Version: &machineapi.VersionInfo{Tag: n.osVersion}}}}, nil
}
func (n *runtimeCoreNode) ApplyConfiguration(ctx context.Context, r *machineapi.ApplyConfigurationRequest) (*machineapi.ApplyConfigurationResponse, error) {
	n.mu.Lock()
	n.applies++
	n.pendingConfig = append([]byte(nil), r.Data...)
	n.mu.Unlock()
	if err := n.setActive(ctx, r.Data); err != nil {
		return nil, err
	}
	return &machineapi.ApplyConfigurationResponse{Messages: []*machineapi.ApplyConfiguration{{Mode: machineapi.ApplyConfigurationRequest_NO_REBOOT, Warnings: []string{runtimeCorePrivate}}}}, nil
}
func (n *runtimeCoreNode) authenticated() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.peerAuthenticated
}
func (n *runtimeCoreNode) Pull(r *machineapi.ImageServicePullRequest, s grpc.ServerStreamingServer[machineapi.ImageServicePullResponse]) error {
	n.mu.Lock()
	n.pulls++
	n.imageRef = r.ImageRef
	n.pulledRefs = append(n.pulledRefs, r.ImageRef)
	n.mu.Unlock()
	return s.Send(&machineapi.ImageServicePullResponse{Response: &machineapi.ImageServicePullResponse_Name{Name: r.ImageRef}})
}

type runtimeCoreImages struct {
	machineapi.UnimplementedImageServiceServer
	node *runtimeCoreNode
}

func (s *runtimeCoreImages) Pull(r *machineapi.ImageServicePullRequest, stream grpc.ServerStreamingServer[machineapi.ImageServicePullResponse]) error {
	return s.node.Pull(r, stream)
}
func (s *runtimeCoreImages) List(_ *machineapi.ImageServiceListRequest, stream grpc.ServerStreamingServer[machineapi.ImageServiceListResponse]) error {
	s.node.mu.Lock()
	ref := s.node.imageRef
	s.node.mu.Unlock()
	return stream.Send(&machineapi.ImageServiceListResponse{Name: ref, Digest: runtimeCoreDigest})
}

type runtimeCoreLifecycle struct {
	machineapi.UnimplementedLifecycleServiceServer
	node *runtimeCoreNode
}

func (s *runtimeCoreLifecycle) Upgrade(request *machineapi.LifecycleServiceUpgradeRequest, stream grpc.ServerStreamingServer[machineapi.LifecycleServiceUpgradeResponse]) error {
	return s.node.install(request, stream)
}
func (n *runtimeCoreNode) install(request *machineapi.LifecycleServiceUpgradeRequest, s grpc.ServerStreamingServer[machineapi.LifecycleServiceUpgradeResponse]) error {
	n.mu.Lock()
	n.installs++
	n.installedRef = request.GetSource().GetImageName()
	if n.installs == 1 {
		close(n.installStarted)
	}
	n.mu.Unlock()
	if err := s.Send(&machineapi.LifecycleServiceUpgradeResponse{Progress: &machineapi.LifecycleServiceInstallProgress{Response: &machineapi.LifecycleServiceInstallProgress_Message{Message: runtimeCorePrivate}}}); err != nil {
		return err
	}
	select {
	case <-s.Context().Done():
		return s.Context().Err()
	case <-n.installAck:
	}
	return s.Send(&machineapi.LifecycleServiceUpgradeResponse{Progress: &machineapi.LifecycleServiceInstallProgress{Response: &machineapi.LifecycleServiceInstallProgress_ExitCode{ExitCode: 0}}})
}
func (n *runtimeCoreNode) Reboot(_ context.Context, _ *machineapi.RebootRequest) (*machineapi.RebootResponse, error) {
	n.mu.Lock()
	n.reboots++
	n.mu.Unlock()
	return &machineapi.RebootResponse{Messages: []*machineapi.Reboot{{ActorId: "test-reboot-ack"}}}, nil
}
func (n *runtimeCoreNode) setActive(ctx context.Context, data []byte) error {
	p, err := configloader.NewFromBytes(data)
	if err != nil {
		return errors.New("test external node rejected configuration")
	}
	n.mu.Lock()
	n.active = append([]byte(nil), data...)
	n.mu.Unlock()
	r := configresource.NewMachineConfig(p)
	existing, err := n.resources.Get(ctx, r.Metadata())
	if state.IsNotFoundError(err) {
		return n.resources.Create(ctx, r)
	}
	if err != nil {
		return err
	}
	*r.Metadata() = *existing.Metadata()
	return n.resources.Update(ctx, r)
}
func (n *runtimeCoreNode) setRuntime(ctx context.Context) error {
	n.mu.Lock()
	boot, version, schematic, kubelet, extension := n.boot, n.osVersion, n.schematic, n.kubelet, n.extension
	n.mu.Unlock()
	b := runtimeresource.NewBootID()
	b.TypedSpec().BootID = boot
	v := runtimeresource.NewVersion()
	v.TypedSpec().Version = version
	s := runtimeresource.NewImageFactorySchematic(runtimeresource.NamespaceName, runtimeresource.ImageFactorySchematicID)
	s.TypedSpec().SchematicID = schematic
	k := k8sresource.NewKubeletStatus(k8sresource.NamespaceName, "kubelet")
	k.TypedSpec().Image = kubelet
	for _, r := range []resource.Resource{s, v, k, b} {
		old, err := n.resources.Get(ctx, r.Metadata())
		if state.IsNotFoundError(err) {
			if err = n.resources.Create(ctx, r); err != nil {
				return err
			}
		} else {
			if err != nil {
				return err
			}
			*r.Metadata() = *old.Metadata()
			if err = n.resources.Update(ctx, r); err != nil {
				return err
			}
		}
	}
	if extension {
		e := runtimeresource.NewExtensionStatus(runtimeresource.NamespaceName, "tailscale")
		e.TypedSpec().Metadata.Name = "tailscale"
		e.TypedSpec().Metadata.Version = "1.0.0"
		e.TypedSpec().Image = "tailscale-1.0.0.sqsh"
		if _, err := n.resources.Get(ctx, e.Metadata()); state.IsNotFoundError(err) {
			if err = n.resources.Create(ctx, e); err != nil {
				return err
			}
		}
	}
	return nil
}
func (n *runtimeCoreNode) counts() (int, int, int, int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.applies, n.pulls, n.installs, n.reboots
}
func runtimeCoreStartNode(t *testing.T, node *runtimeCoreNode, bundle *secrets.Bundle, cluster string) (bootstrapv1.TalosEndpoint, []byte) {
	t.Helper()
	ca, err := bundle.Certs.OS.GetCert()
	require.NoError(t, err)
	key, err := bundle.Certs.OS.GetKey()
	require.NoError(t, err)
	signer, ok := key.(crypto.Signer)
	require.True(t, ok)
	cert, _, _ := runtimeCoreCertificate(t, ca, signer, runtimeCoreNodeSNI, x509.ExtKeyUsageServerAuth)
	_, clientCert, clientKey := runtimeCoreCertificate(t, ca, signer, "runtime-core-client", x509.ExtKeyUsageClientAuth)
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots})))
	machineapi.RegisterMachineServiceServer(server, node)
	machineapi.RegisterImageServiceServer(server, &runtimeCoreImages{node: node})
	machineapi.RegisterLifecycleServiceServer(server, &runtimeCoreLifecycle{node: node})
	cosiapi.RegisterStateServer(server, cosiserver.NewState(node.resources))
	done := make(chan error, 1)
	go func() { done <- server.Serve(l) }()
	t.Cleanup(func() { server.Stop(); _ = l.Close(); <-done })
	identity, err := yaml.Marshal(map[string]any{"context": cluster, "contexts": map[string]any{cluster: map[string]any{"ca": base64.StdEncoding.EncodeToString(bundle.Certs.OS.Crt), "crt": base64.StdEncoding.EncodeToString(clientCert), "key": base64.StdEncoding.EncodeToString(clientKey)}}})
	require.NoError(t, err)
	return bootstrapv1.TalosEndpoint{Address: l.Addr().String(), ServerName: runtimeCoreNodeSNI}, identity
}

func runtimeCoreStartProcess(t *testing.T, ctx context.Context, binary string, e *envtest.Environment, logs *runtimeCoreLog) {
	t.Helper()
	cfgPath := filepath.Join(t.TempDir(), "management.kubeconfig")
	require.NoError(t, os.WriteFile(cfgPath, e.KubeConfig, 0600))
	health := runtimeCoreFreePort(t)
	args := []string{"--kubeconfig=" + cfgPath, "--feature-gates=RuntimeSDK=true,InPlaceUpdates=true,ClusterTopology=false,MachinePool=false", "--leader-elect=false", "--webhook-port=" + strconv.Itoa(e.WebhookInstallOptions.LocalServingPort), "--webhook-cert-dir=" + e.WebhookInstallOptions.LocalServingCertDir, "--health-addr=127.0.0.1:" + strconv.Itoa(health), "--diagnostics-address=0", "--cluster-concurrency=1", "--clustercache-concurrency=1", "--machine-concurrency=1", "--machineset-concurrency=1", "--machinedeployment-concurrency=1", "--extensionconfig-concurrency=1", "--clusterresourceset-concurrency=1", "--machinehealthcheck-concurrency=1", "--v=2"}
	cmd := exec.CommandContext(ctx, binary, args...)
	for _, env := range os.Environ() {
		if !strings.HasPrefix(env, "KUBECONFIG=") && !strings.HasPrefix(env, "KUBERNETES_SERVICE_") && !strings.HasPrefix(env, "POD_NAMESPACE=") && !strings.HasPrefix(env, "USE_EXISTING_CLUSTER=") {
			cmd.Env = append(cmd.Env, env)
		}
	}
	cmd.Stdout = logs
	cmd.Stderr = logs
	require.NoError(t, cmd.Start())
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(20 * time.Second):
			_ = cmd.Process.Kill()
			<-done
			t.Error("actual core process did not stop gracefully")
		}
	})
	h := &http.Client{Timeout: time.Second}
	runtimeCoreEventually(t, "core webhook readiness", 20*time.Second, func() error {
		r, err := h.Get("http://127.0.0.1:" + strconv.Itoa(health) + "/readyz")
		if err != nil {
			return err
		}
		_ = r.Body.Close()
		if r.StatusCode != 200 {
			return fmt.Errorf("core webhook status %d", r.StatusCode)
		}
		return nil
	})
}

func runtimeCoreInstallCAPH(t *testing.T, cfg *rest.Config, scheme *kruntime.Scheme, path string) {
	t.Helper()
	options := envtest.CRDInstallOptions{Scheme: scheme, Paths: []string{path}, ErrorIfPathMissing: true}
	require.NoError(t, envtest.ReadCRDFiles(&options))
	for _, crd := range options.CRDs {
		if crd.Labels == nil {
			crd.Labels = map[string]string{}
		}
		crd.Labels["cluster.x-k8s.io/v1beta1"] = "v1beta1"
	}
	options.Paths = nil
	_, err := envtest.InstallCRDs(cfg, options)
	require.NoError(t, err)
}

func runtimeCoreHealthyNode(t *testing.T, ctx context.Context, c client.Client, node *corev1.Node, ready bool) {
	t.Helper()
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(node), node))
	old := node.DeepCopy()
	r := corev1.ConditionFalse
	if ready {
		r = corev1.ConditionTrue
	}
	node.Status.NodeInfo = corev1.NodeSystemInfo{KubeletVersion: runtimeCoreKubeVersion, OSImage: "Talos " + runtimeCoreOSVersion, BootID: "boot-initial"}
	node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: r, LastTransitionTime: metav1.Now()}, {Type: corev1.NodeMemoryPressure, Status: corev1.ConditionFalse, LastTransitionTime: metav1.Now()}, {Type: corev1.NodeDiskPressure, Status: corev1.ConditionFalse, LastTransitionTime: metav1.Now()}, {Type: corev1.NodePIDPressure, Status: corev1.ConditionFalse, LastTransitionTime: metav1.Now()}}
	require.NoError(t, c.Status().Patch(ctx, node, client.MergeFrom(old)))
}

type runtimeCoreInventory map[string]types.UID

func runtimeCoreSnapshot(t *testing.T, ctx context.Context, c client.Client, namespace string) runtimeCoreInventory {
	t.Helper()
	out := runtimeCoreInventory{}
	for _, gk := range [][2]string{{clusterv1.GroupVersion.String(), "MachineList"}, {clusterv1.GroupVersion.String(), "MachineSetList"}, {bootstrapv1.GroupVersion.String(), "TalosConfigList"}, {"infrastructure.cluster.x-k8s.io/v1beta1", "HetznerBareMetalMachineList"}} {
		l := &unstructured.UnstructuredList{}
		l.SetAPIVersion(gk[0])
		l.SetKind(gk[1])
		require.NoError(t, c.List(ctx, l, client.InNamespace(namespace)))
		for _, item := range l.Items {
			if gk[1] != "MachineSetList" {
				require.True(t, item.GetDeletionTimestamp().IsZero(), "unexpected deletion")
			}
			out[gk[1]+"/"+item.GetName()] = item.GetUID()
		}
	}
	nodes := &corev1.NodeList{}
	require.NoError(t, c.List(ctx, nodes))
	for _, n := range nodes.Items {
		out["Node/"+n.Name] = n.UID
	}
	return out
}
func runtimeCoreAssertInventory(t *testing.T, ctx context.Context, c client.Client, namespace string, expected runtimeCoreInventory) {
	t.Helper()
	actual := runtimeCoreSnapshot(t, ctx, c, namespace)
	require.Equal(t, expected, actual, "all Machine/MS/TC/Infra/Node inventory must remain exact; no replacement or hidden extra provisioning graph")
}

// Watches transfer only PartialObjectMetadata, never private configuration/Secret payloads.
type runtimeCoreTransitions struct {
	mu              sync.Mutex
	failure         error
	seen            map[string]types.UID
	msBudget        int
	done            []chan struct{}
	retired         map[string]types.UID
	desiredTemplate string
	liveSets        map[string]*clusterv1.MachineSet
}

func runtimeCoreWatchTransitions(t *testing.T, ctx context.Context, cfg *rest.Config, namespace string, initial runtimeCoreInventory, seed *corev1.Secret) *runtimeCoreTransitions {
	t.Helper()
	m, err := metadataclient.NewForConfig(cfg)
	require.NoError(t, err)
	tracker := &runtimeCoreTransitions{seen: map[string]types.UID{}, retired: map[string]types.UID{}, liveSets: map[string]*clusterv1.MachineSet{}, msBudget: 1}
	for key, uid := range initial {
		tracker.seen[key] = uid
	}
	tracker.seen["Secret/"+seed.Name] = seed.UID
	resources := []struct {
		gvr        schema.GroupVersionResource
		kind       string
		namespaced bool
		selector   string
	}{
		{schema.GroupVersionResource{Group: clusterv1.GroupVersion.Group, Version: "v1beta2", Resource: "machines"}, "MachineList", true, ""},
		{schema.GroupVersionResource{Group: bootstrapv1.GroupVersion.Group, Version: "v1beta1", Resource: "talosconfigs"}, "TalosConfigList", true, ""},
		{schema.GroupVersionResource{Group: "infrastructure.cluster.x-k8s.io", Version: "v1beta1", Resource: "hetznerbaremetalmachines"}, "HetznerBareMetalMachineList", true, ""},
		{schema.GroupVersionResource{Version: "v1", Resource: "nodes"}, "Node", false, ""},
		{schema.GroupVersionResource{Version: "v1", Resource: "secrets"}, "Secret", true, "metadata.name=" + seed.Name},
	}
	for _, entry := range resources {
		var api metadataclient.ResourceInterface = m.Resource(entry.gvr)
		if entry.namespaced {
			api = m.Resource(entry.gvr).Namespace(namespace)
		}
		list, err := api.List(ctx, metav1.ListOptions{FieldSelector: entry.selector})
		require.NoError(t, err)
		for i := range list.Items {
			tracker.observe(entry.kind, watch.Modified, &list.Items[i])
		}
		stream, err := api.Watch(ctx, metav1.ListOptions{ResourceVersion: list.ResourceVersion, FieldSelector: entry.selector, AllowWatchBookmarks: true})
		require.NoError(t, err)
		done := make(chan struct{})
		tracker.done = append(tracker.done, done)
		go func(kind string, w watch.Interface, done chan struct{}) {
			defer close(done)
			defer w.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case event, ok := <-w.ResultChan():
					if !ok {
						if ctx.Err() == nil {
							tracker.fail(errors.New("metadata invariant watch ended unexpectedly"))
						}
						return
					}
					if event.Type == watch.Bookmark {
						continue
					}
					if event.Type == watch.Error {
						tracker.fail(errors.New("metadata invariant watch error"))
						return
					}
					obj, ok := event.Object.(*metav1.PartialObjectMetadata)
					if !ok {
						tracker.fail(errors.New("metadata invariant watch returned non-metadata object"))
						return
					}
					tracker.observe(kind, event.Type, obj)
				}
			}
		}(entry.kind, stream, done)
	}
	typed, err := client.NewWithWatch(cfg, client.Options{Scheme: runtimeCoreScheme(t)})
	require.NoError(t, err)
	sets := &clusterv1.MachineSetList{}
	require.NoError(t, typed.List(ctx, sets, client.InNamespace(namespace)))
	for _, ms := range sets.Items {
		tracker.desiredTemplate = ms.Spec.Template.Spec.Bootstrap.ConfigRef.Name
	}
	setStream, err := typed.Watch(ctx, &clusterv1.MachineSetList{ListMeta: metav1.ListMeta{ResourceVersion: sets.ResourceVersion}}, client.InNamespace(namespace))
	require.NoError(t, err)
	setDone := make(chan struct{})
	tracker.done = append(tracker.done, setDone)
	go func() {
		defer close(setDone)
		defer setStream.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-setStream.ResultChan():
				if !ok {
					if ctx.Err() == nil {
						tracker.fail(errors.New("typed MS watch ended unexpectedly"))
					}
					return
				}
				if event.Type == watch.Bookmark {
					continue
				}
				ms, ok := event.Object.(*clusterv1.MachineSet)
				if !ok {
					tracker.fail(errors.New("typed MS watch failed"))
					return
				}
				tracker.observeSet(ctx, typed, namespace, event.Type, ms)
			}
		}
	}()
	return tracker
}
func (t *runtimeCoreTransitions) fail(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.failure == nil {
		t.failure = err
	}
}
func (t *runtimeCoreTransitions) observe(kind string, event watch.EventType, obj *metav1.PartialObjectMetadata) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.failure != nil {
		return
	}
	key := kind + "/" + obj.Name
	if event == watch.Deleted || obj.DeletionTimestamp != nil {
		t.failure = fmt.Errorf("unexpected graph deletion: %s", key)
		return
	}
	uid, exists := t.seen[key]
	if exists {
		if uid != obj.UID {
			t.failure = fmt.Errorf("unexpected graph replacement: %s", key)
		}
		return
	}
	if kind != "MachineSetList" {
		t.failure = fmt.Errorf("unexpected extra graph object: %s", key)
		return
	}
	count := 0
	for name := range t.seen {
		if strings.HasPrefix(name, "MachineSetList/") {
			count++
		}
	}
	if count >= t.msBudget {
		t.failure = errors.New("unexpected extra MachineSet beyond immutable revision budget")
		return
	}
	t.seen[key] = obj.UID
}
func (t *runtimeCoreTransitions) allowRevision() { t.mu.Lock(); t.msBudget++; t.mu.Unlock() }
func (t *runtimeCoreTransitions) assert(tst *testing.T) {
	tst.Helper()
	t.mu.Lock()
	err := t.failure
	t.mu.Unlock()
	require.NoError(tst, err)
}

func (t *runtimeCoreTransitions) observeSet(ctx context.Context, c client.Client, namespace string, event watch.EventType, ms *clusterv1.MachineSet) {
	key := "MachineSetList/" + ms.Name
	if event != watch.Deleted && ms.DeletionTimestamp == nil {
		t.observe("MachineSetList", event, &metav1.PartialObjectMetadata{ObjectMeta: ms.ObjectMeta})
		t.mu.Lock()
		t.liveSets[key] = ms.DeepCopy()
		t.mu.Unlock()
		return
	}
	t.mu.Lock()
	uid, known := t.seen[key]
	desired := t.desiredTemplate
	prior := t.liveSets[key]
	t.mu.Unlock()
	eligible := ms
	if prior != nil {
		eligible = prior
	}
	if !known || uid != ms.UID || eligible.UID != ms.UID || eligible.Spec.Template.Spec.Bootstrap.ConfigRef.Name == desired || eligible.Spec.Replicas == nil || *eligible.Spec.Replicas != 0 || eligible.Status.Replicas == nil || *eligible.Status.Replicas != 0 || eligible.Generation > eligible.Status.ObservedGeneration {
		t.fail(fmt.Errorf("ineligible old MachineSet retirement: %s known=%t prior=%t ref=%s desired=%s spec=%d status=%d gen=%d observed=%d", key, known, prior != nil, eligible.Spec.Template.Spec.Bootstrap.ConfigRef.Name, desired, ptr.Deref(eligible.Spec.Replicas, -1), ptr.Deref(eligible.Status.Replicas, -1), eligible.Generation, eligible.Status.ObservedGeneration))
		return
	}
	machines := &clusterv1.MachineList{}
	if err := c.List(ctx, machines, client.InNamespace(namespace)); err != nil {
		t.fail(errors.New("MS retirement owner inventory unavailable"))
		return
	}
	for _, m := range machines.Items {
		for _, owner := range m.OwnerReferences {
			if owner.Kind == "MachineSet" && owner.UID == ms.UID {
				t.fail(errors.New("Machine still owned by retiring MachineSet"))
				return
			}
		}
	}
	t.mu.Lock()
	t.retired[key] = ms.UID
	t.mu.Unlock()
}
func (t *runtimeCoreTransitions) setDesiredTemplate(name string) {
	t.mu.Lock()
	t.desiredTemplate = name
	t.mu.Unlock()
}
func (t *runtimeCoreTransitions) isRetired(name string, uid types.UID) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.retired[name] == uid
}

type runtimeCoreProxy struct {
	mu         sync.Mutex
	updatePath string
	updates    int
	server     *httptest.Server
}

func runtimeCoreStartProxy(t *testing.T, target string, caData []byte) *runtimeCoreProxy {
	t.Helper()
	u, err := url.Parse(target)
	require.NoError(t, err)
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(caData))
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}
	p := &runtimeCoreProxy{}
	reverse := httputil.NewSingleHostReverseProxy(u)
	reverse.Transport = transport
	reverse.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) {
		http.Error(w, "runtime transport failed", http.StatusBadGateway)
	}
	p.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reverse.ServeHTTP(w, r)
		p.mu.Lock()
		if p.updatePath != "" && r.URL.Path == p.updatePath {
			p.updates++
		}
		p.mu.Unlock()
	}))
	t.Cleanup(func() { p.server.Close(); transport.CloseIdleConnections() })
	return p
}
func (p *runtimeCoreProxy) setDiscoveredUpdatePath(ec *runtimev1.ExtensionConfig) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, handler := range ec.Status.Handlers {
		if handler.RequestHook.Hook == "UpdateMachine" {
			parts := strings.Split(handler.RequestHook.APIVersion, "/")
			if len(parts) == 2 {
				p.updatePath = runtimecatalog.GVHToPath(runtimecatalog.GroupVersionHook{Group: parts[0], Version: parts[1], Hook: "UpdateMachine"}, strings.SplitN(handler.Name, ".", 2)[0])
			}
		}
	}
}
func (p *runtimeCoreProxy) count() int { p.mu.Lock(); defer p.mu.Unlock(); return p.updates }
func runtimeCoreProxyCA(p *runtimeCoreProxy) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: p.server.Certificate().Raw})
}

type runtimeCoreFixture struct {
	t                *testing.T
	ctx              context.Context
	c                client.Client
	environment      *envtest.Environment
	namespace        string
	manager          ctrl.Manager
	logs             *runtimeCoreLog
	proxy            *runtimeCoreProxy
	external         *runtimeCoreNode
	cluster          *clusterv1.Cluster
	md               *clusterv1.MachineDeployment
	machine          *clusterv1.Machine
	ms               *clusterv1.MachineSet
	tc               *bootstrapv1.TalosConfig
	infra            *unstructured.Unstructured
	node             *corev1.Node
	seed             *corev1.Secret
	originalSeed     *corev1.Secret
	accepted         *updateoperation.AcceptedIntent
	initialInventory runtimeCoreInventory
	initialTCSpec    bootstrapv1.TalosConfigSpec
	factoryURL       string
	rotations        int
	history          map[string]types.UID
	transitions      *runtimeCoreTransitions
}

func runtimeCoreEnvironment(t *testing.T, assets, source, repo, caph string) (*envtest.Environment, *kruntime.Scheme, client.Client, envtest.WebhookInstallOptions) {
	t.Helper()
	s := runtimeCoreScheme(t)
	e := &envtest.Environment{UseExistingCluster: ptr.To(false), BinaryAssetsDirectory: assets, Scheme: s, ErrorIfCRDPathMissing: true, CRDDirectoryPaths: []string{filepath.Join(source, "core/config/crd/bases")}, WebhookInstallOptions: envtest.WebhookInstallOptions{Paths: []string{filepath.Join(source, "core/config/webhook/manifests.yaml")}, LocalServingHost: "127.0.0.1", IgnoreSchemeConvertible: true}, ControlPlaneStartTimeout: time.Minute, ControlPlaneStopTimeout: time.Minute}
	cfg, err := e.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, e.Stop()) })
	runtimeCoreInstallCAPH(t, cfg, s, caph)
	// All core schemas use core conversion; CABPT schemas use a different actual CABPT webhook.
	opts := envtest.WebhookInstallOptions{Paths: []string{filepath.Join(repo, "config/webhook/manifests.yaml")}, LocalServingHost: "127.0.0.1"}
	require.NoError(t, opts.PrepWithoutInstalling())
	require.NoError(t, opts.Install(cfg))
	providerCRDs := envtest.CRDInstallOptions{Scheme: s, Paths: []string{filepath.Join(repo, "config/crd/bases")}, ErrorIfPathMissing: true, WebhookOptions: opts}
	require.NoError(t, envtest.ReadCRDFiles(&providerCRDs))
	for _, crd := range providerCRDs.CRDs {
		if crd.Labels == nil {
			crd.Labels = map[string]string{}
		}
		crd.Labels["cluster.x-k8s.io/v1beta2"] = "v1beta1"
		crd.Labels["cluster.x-k8s.io/v1beta1"] = "v1alpha3"
	}
	providerCRDs.Paths = nil
	_, err = envtest.InstallCRDs(cfg, providerCRDs)
	require.NoError(t, err)
	// This is an explicitly synthetic external-observer test substrate, not CAPH installed CP state.
	// The real CAPH schemas above remain unmodified except release contract labels.
	observer := &apiextv1.CustomResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: "observedcontrolplanes.controlplane.cluster.x-k8s.io", Labels: map[string]string{"cluster.x-k8s.io/v1beta2": "v1alpha1"}}, Spec: apiextv1.CustomResourceDefinitionSpec{Group: "controlplane.cluster.x-k8s.io", Names: apiextv1.CustomResourceDefinitionNames{Kind: "ObservedControlPlane", ListKind: "ObservedControlPlaneList", Plural: "observedcontrolplanes"}, Scope: apiextv1.NamespaceScoped, Versions: []apiextv1.CustomResourceDefinitionVersion{{Name: "v1alpha1", Served: true, Storage: true, Subresources: &apiextv1.CustomResourceSubresources{Status: &apiextv1.CustomResourceSubresourceStatus{}}, Schema: &apiextv1.CustomResourceValidation{OpenAPIV3Schema: &apiextv1.JSONSchemaProps{Type: "object", Properties: map[string]apiextv1.JSONSchemaProps{"spec": {Type: "object", XPreserveUnknownFields: ptr.To(true)}, "status": {Type: "object", XPreserveUnknownFields: ptr.To(true)}}}}}}}}
	_, err = envtest.InstallCRDs(cfg, envtest.CRDInstallOptions{CRDs: []*apiextv1.CustomResourceDefinition{observer}})
	require.NoError(t, err)
	c, err := client.New(cfg, client.Options{Scheme: s})
	require.NoError(t, err)
	return e, s, c, opts
}

func runtimeCoreFactory(t *testing.T) (imagefactory.API, string) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/versions":
			_ = json.NewEncoder(w).Encode([]string{runtimeCoreOSVersion, runtimeCoreNextOSVersion})
		case strings.HasSuffix(r.URL.Path, "/extensions/official"):
			_ = json.NewEncoder(w).Encode([]map[string]string{{"name": "siderolabs/tailscale"}})
		case r.Method == http.MethodPost && r.URL.Path == "/schematics":
			data, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			if err != nil {
				http.Error(w, "read", 400)
				return
			}
			id := runtimeCoreSchematic
			if bytes.Contains(data, []byte("tailscale")) {
				id = runtimeCoreNextSchematic
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"id": id})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	api, err := imagefactory.NewClient(server.URL, server.Client())
	require.NoError(t, err)
	return api, server.URL
}

func runtimeCoreStartCABPT(t *testing.T, ctx context.Context, cfg *rest.Config, s *kruntime.Scheme, opts envtest.WebhookInstallOptions, factory imagefactory.API, logs *runtimeCoreLog) (ctrl.Manager, int, []byte) {
	t.Helper()
	logger := logr.New(logs)
	manager, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: s, Logger: logger, Metrics: metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: "0", LeaderElection: false, WebhookServer: webhook.NewServer(webhook.Options{Host: opts.LocalServingHost, Port: opts.LocalServingPort, CertDir: opts.LocalServingCertDir})})
	require.NoError(t, err)
	r := &controllers.TalosConfigReconciler{Client: manager.GetClient(), Log: logger, Scheme: s, ImageFactory: factory, MachinePoolInPlaceUpdates: false}
	require.NoError(t, r.SetupWithManager(ctx, manager, controller.Options{MaxConcurrentReconciles: 1}))
	require.NoError(t, (&bootstrapv1.TalosConfig{}).SetupWebhookWithManager(manager))
	require.NoError(t, (&bootstrapv1.TalosConfigTemplate{}).SetupWebhookWithManager(manager))
	runtimePort := runtimeCoreFreePort(t)
	dir, ca := runtimeCoreCertDir(t)
	h := inplace.NewHandler(manager.GetClient(), manager.GetAPIReader(), inplace.NewOperationNodeClient(manager.GetAPIReader()))
	require.NoError(t, inplace.AddToManager(manager, h, inplace.Options{Host: "127.0.0.1", Port: runtimePort, CertDir: dir}))
	managerCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- manager.Start(managerCtx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(30 * time.Second):
			t.Error("actual CABPT runtime/controller manager did not stop")
		}
	})
	waitCtx, stop := context.WithTimeout(ctx, 20*time.Second)
	defer stop()
	require.True(t, manager.GetCache().WaitForCacheSync(waitCtx))
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(ca))
	tr := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}
	t.Cleanup(tr.CloseIdleConnections)
	hc := &http.Client{Transport: tr, Timeout: time.Second}
	runtimeCoreEventually(t, "actual CABPT runtime TLS ready", 20*time.Second, func() error {
		r, err := hc.Get("https://127.0.0.1:" + strconv.Itoa(runtimePort) + "/not-a-hook")
		if err != nil {
			return err
		}
		r.Body.Close()
		if r.StatusCode != http.StatusNotFound {
			return errors.New("runtime listener not serving")
		}
		return nil
	})
	return manager, runtimePort, ca
}

func runtimeCoreNewFixture(t *testing.T, assets, source, binary, caph, repo string) *runtimeCoreFixture {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	logs := &runtimeCoreLog{shared: &runtimeCoreLogBuffer{}}
	// This fixture runs only in a dedicated test-binary child; global logging cannot leak to other suites.
	ctrl.SetLogger(logr.New(logs))
	e, s, c, webhookOpts := runtimeCoreEnvironment(t, assets, source, repo, caph)
	ctx = ctrl.LoggerInto(ctx, logr.New(logs))
	factory, factoryURL := runtimeCoreFactory(t)
	manager, port, ca := runtimeCoreStartCABPT(t, ctx, e.Config, s, webhookOpts, factory, logs)
	proxy := runtimeCoreStartProxy(t, "https://127.0.0.1:"+strconv.Itoa(port), ca)
	coreLogs := &runtimeCoreLog{shared: &runtimeCoreLogBuffer{}}
	runtimeCoreStartProcess(t, ctx, binary, e, coreLogs)
	namespace := "runtime-core-" + strings.ToLower(fmt.Sprint(time.Now().UnixNano()))
	require.NoError(t, c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace, Labels: map[string]string{"runtime-core": "test"}}}))
	ec := &runtimev1.ExtensionConfig{ObjectMeta: metav1.ObjectMeta{Name: "runtime-core"}, Spec: runtimev1.ExtensionConfigSpec{ClientConfig: runtimev1.ClientConfig{URL: proxy.server.URL, CABundle: runtimeCoreProxyCA(proxy)}, NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"runtime-core": "test"}}}}
	require.NoError(t, c.Create(ctx, ec))
	runtimeCoreEventually(t, "real core discovers actual CABPT handlers", 30*time.Second, func() error {
		if err := c.Get(ctx, client.ObjectKeyFromObject(ec), ec); err != nil {
			return err
		}
		found := map[string]bool{}
		for _, h := range ec.Status.Handlers {
			if h.RequestHook.APIVersion == hooks.GroupVersion.String() {
				found[h.RequestHook.Hook] = true
			}
		}
		if len(ec.Status.Handlers) != 3 || !found["CanUpdateMachine"] || !found["CanUpdateMachineSet"] || !found["UpdateMachine"] {
			return fmt.Errorf("actual Discovery handler inventory %d", len(ec.Status.Handlers))
		}
		return nil
	})
	proxy.setDiscoveredUpdatePath(ec)
	f := &runtimeCoreFixture{t: t, ctx: ctx, c: c, environment: e, namespace: namespace, manager: manager, logs: logs, proxy: proxy, external: runtimeCoreNodeNew(t), factoryURL: factoryURL}
	t.Cleanup(func() {
		require.NotContains(t, logs.String(), runtimeCorePrivate)
		require.NotContains(t, coreLogs.String(), runtimeCorePrivate)
	})
	clusterName := "runtime-core"
	workloadConfig := kubeconfig.FromEnvTestConfig(e.Config, &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: clusterName}})
	require.NoError(t, c.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: clusterName + "-kubeconfig", Namespace: namespace, Labels: map[string]string{clusterv1.ClusterNameLabel: clusterName}}, Type: "cluster.x-k8s.io/secret", Data: map[string][]byte{"value": workloadConfig}}))
	infraCluster := runtimeCoreObject("infrastructure.cluster.x-k8s.io/v1beta1", "HetznerCluster", namespace, clusterName, map[string]any{"controlPlaneRegions": []any{"fsn1"}, "hetznerSecretRef": map[string]any{"name": "unused-no-provider", "key": map[string]any{"hcloudToken": "token"}}, "sshKeys": map[string]any{}, "hcloudNetwork": map[string]any{"enabled": false}, "controlPlaneLoadBalancer": map[string]any{"enabled": false}})
	require.NoError(t, c.Create(ctx, infraCluster))
	beforeInfra := infraCluster.DeepCopy()
	infraCluster.Object["status"] = map[string]any{"ready": true}
	require.NoError(t, c.Status().Patch(ctx, infraCluster, client.MergeFrom(beforeInfra)))
	cp := runtimeCoreObject("controlplane.cluster.x-k8s.io/v1alpha1", "ObservedControlPlane", namespace, clusterName, map[string]any{"clusterName": clusterName})
	require.NoError(t, c.Create(ctx, cp))
	beforeCP := cp.DeepCopy()
	cp.Object["status"] = map[string]any{"initialization": map[string]any{"controlPlaneInitialized": true}, "externalManagedControlPlane": true, "conditions": []any{map[string]any{"type": "Available", "status": "True", "reason": "TestExternalNodeObserved", "lastTransitionTime": metav1.Now().Format(time.RFC3339), "observedGeneration": cp.GetGeneration()}}}
	require.NoError(t, c.Status().Patch(ctx, cp, client.MergeFrom(beforeCP)))
	apiURL, err := url.Parse(e.Config.Host)
	require.NoError(t, err)
	host, p, err := net.SplitHostPort(apiURL.Host)
	require.NoError(t, err)
	apiPort, err := strconv.Atoi(p)
	require.NoError(t, err)
	f.cluster = &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace}, Spec: clusterv1.ClusterSpec{ControlPlaneEndpoint: clusterv1.APIEndpoint{Host: host, Port: int32(apiPort)}, ControlPlaneRef: clusterv1.ContractVersionedObjectReference{APIGroup: "controlplane.cluster.x-k8s.io", Kind: "ObservedControlPlane", Name: clusterName}, InfrastructureRef: clusterv1.ContractVersionedObjectReference{APIGroup: "infrastructure.cluster.x-k8s.io", Kind: "HetznerCluster", Name: clusterName}}}
	require.NoError(t, c.Create(ctx, f.cluster))
	runtimeCoreEventually(t, "real core initializes fixture Cluster", 30*time.Second, func() error {
		if err := c.Get(ctx, client.ObjectKeyFromObject(f.cluster), f.cluster); err != nil {
			return err
		}
		if !ptr.Deref(f.cluster.Status.Initialization.InfrastructureProvisioned, false) || !conditions.IsTrue(f.cluster, clusterv1.ClusterControlPlaneInitializedCondition) {
			return errors.New("Cluster initialization not observed")
		}
		return nil
	})
	bundle, err := secrets.NewBundle(secrets.NewClock(), nil)
	require.NoError(t, err)
	bundleBytes, err := yaml.Marshal(bundle)
	require.NoError(t, err)
	require.NoError(t, c.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: clusterName + "-talos", Namespace: namespace, Labels: map[string]string{clusterv1.ClusterNameLabel: clusterName}}, Data: map[string][]byte{"bundle": bundleBytes}}))
	endpoint, identity := runtimeCoreStartNode(t, f.external, bundle, clusterName)
	require.NoError(t, c.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: clusterName + "-talosconfig", Namespace: namespace}, Data: map[string][]byte{"talosconfig": identity}}))
	probe, err := inplace.NewOperationNodeClient(c)(ctx, types.NamespacedName{Namespace: namespace, Name: clusterName}, endpoint)
	require.NoError(t, err)
	version, err := probe.Version(ctx)
	require.NoError(t, err)
	require.Equal(t, runtimeCoreOSVersion, version)
	require.NoError(t, probe.Close())
	require.True(t, f.external.authenticated())
	spec := bootstrapv1.TalosConfigSpec{GenerateType: "worker", TalosVersion: runtimeCoreOSVersion, ConfigRevision: "initial", TalosEndpoint: &endpoint, ImageFactory: &bootstrapv1.ImageFactorySpec{}, Hostname: bootstrapv1.HostnameSpec{Source: bootstrapv1.HostnameSourceMachineName}}
	template := &bootstrapv1.TalosConfigTemplate{ObjectMeta: metav1.ObjectMeta{Name: "worker-initial", Namespace: namespace}, Spec: bootstrapv1.TalosConfigTemplateSpec{Template: bootstrapv1.TalosConfigTemplateResource{Spec: spec}}}
	require.NoError(t, c.Create(ctx, template))
	f.initialTCSpec = spec
	infraTemplate := runtimeCoreObject("infrastructure.cluster.x-k8s.io/v1beta1", "HetznerBareMetalMachineTemplate", namespace, "worker-installed", map[string]any{"template": map[string]any{"spec": map[string]any{"installImage": map[string]any{"image": map[string]any{"url": "https://example.invalid/never-installed.raw"}, "partitions": []any{}}}}})
	require.NoError(t, c.Create(ctx, infraTemplate))
	f.md = &clusterv1.MachineDeployment{ObjectMeta: metav1.ObjectMeta{Name: "workers", Namespace: namespace}, Spec: clusterv1.MachineDeploymentSpec{ClusterName: clusterName, Replicas: ptr.To[int32](1), Selector: metav1.LabelSelector{MatchLabels: map[string]string{"runtime-core-worker": "true"}}, Template: clusterv1.MachineTemplateSpec{ObjectMeta: clusterv1.ObjectMeta{Labels: map[string]string{"runtime-core-worker": "true", clusterv1.ClusterNameLabel: clusterName}}, Spec: clusterv1.MachineSpec{ClusterName: clusterName, Version: runtimeCoreKubeVersion, Bootstrap: clusterv1.Bootstrap{ConfigRef: clusterv1.ContractVersionedObjectReference{APIGroup: bootstrapv1.GroupVersion.Group, Kind: "TalosConfigTemplate", Name: template.Name}}, InfrastructureRef: clusterv1.ContractVersionedObjectReference{APIGroup: "infrastructure.cluster.x-k8s.io", Kind: "HetznerBareMetalMachineTemplate", Name: infraTemplate.GetName()}}}}}
	require.NoError(t, c.Create(ctx, f.md))
	runtimeCoreEventually(t, "real core creates exactly one initial worker", 30*time.Second, func() error {
		list := &clusterv1.MachineList{}
		if err := c.List(ctx, list, client.InNamespace(namespace)); err != nil {
			return err
		}
		if len(list.Items) != 1 {
			return fmt.Errorf("initial Machine count %d", len(list.Items))
		}
		f.machine = list.Items[0].DeepCopy()
		return nil
	})
	f.tc = &bootstrapv1.TalosConfig{}
	runtimeCoreEventually(t, "actual CABPT provider generates initial worker seed", 30*time.Second, func() error {
		if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: f.machine.Spec.Bootstrap.ConfigRef.Name}, f.tc); err != nil {
			return err
		}
		if f.tc.Status.DataSecretName == "" || !ptr.Deref(f.tc.Status.Initialization.DataSecretCreated, false) {
			return errors.New("initial seed not generated")
		}
		return nil
	})
	f.seed = &corev1.Secret{}
	require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: f.tc.Status.DataSecretName}, f.seed))
	f.originalSeed = f.seed.DeepCopy()
	require.NotEmpty(t, f.seed.Data[configinput.FreshnessKey])
	accepted, err := updateoperation.DecodeAcceptedIntent(f.seed.Data[updateoperation.AcceptedIntentKey])
	require.NoError(t, err, "initial acceptedIntent must originate in actual provider generation, never fixture writes")
	require.Equal(t, f.machine.UID, accepted.MachineUID)
	require.Equal(t, f.tc.UID, accepted.TalosConfigUID)
	require.Empty(t, accepted.TransactionID)
	require.True(t, metav1.IsControlledBy(f.seed, f.tc))
	require.Equal(t, f.machine.Name+"-bootstrap-data", f.seed.Name)
	f.accepted = accepted
	require.NoError(t, f.external.setActive(ctx, f.seed.Data["value"]))
	provider, err := configloader.NewFromBytes(f.seed.Data["value"])
	require.NoError(t, err)
	f.external.mu.Lock()
	f.external.kubelet = provider.K8sKubeletConfig().Image()
	f.external.mu.Unlock()
	require.NoError(t, f.external.setRuntime(ctx))
	f.infra = runtimeCoreObject("infrastructure.cluster.x-k8s.io/v1beta1", "HetznerBareMetalMachine", namespace, f.machine.Spec.InfrastructureRef.Name, nil)
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(f.infra), f.infra))
	infraBefore := f.infra.DeepCopy()
	require.NoError(t, unstructured.SetNestedField(f.infra.Object, "hcloud://bm-2873475", "spec", "providerID"))
	require.NoError(t, c.Patch(ctx, f.infra, client.MergeFrom(infraBefore)))
	infraBefore = f.infra.DeepCopy()
	f.infra.Object["status"] = map[string]any{"ready": true}
	require.NoError(t, c.Status().Patch(ctx, f.infra, client.MergeFrom(infraBefore)))
	require.NotNil(t, provider.NetworkHostnameConfig())
	require.Equal(t, f.machine.Name, provider.NetworkHostnameConfig().Hostname(), "generated Talos hostname must match MachineName policy")
	f.node = &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: f.machine.Name}, Spec: corev1.NodeSpec{ProviderID: "hcloud://bm-2873475"}}
	require.NoError(t, c.Create(ctx, f.node))
	runtimeCoreHealthyNode(t, ctx, c, f.node, true)
	runtimeCoreEventually(t, "actual observed worker/MS Ready Available UpToDate", 45*time.Second, func() error {
		if err := c.Get(ctx, client.ObjectKeyFromObject(f.machine), f.machine); err != nil {
			return err
		}
		for _, condition := range []string{clusterv1.MachineReadyCondition, clusterv1.MachineAvailableCondition, clusterv1.MachineUpToDateCondition} {
			if !conditions.IsTrue(f.machine, condition) {
				return fmt.Errorf("Machine condition %s not true", condition)
			}
		}
		list := &clusterv1.MachineSetList{}
		if err := c.List(ctx, list, client.InNamespace(namespace)); err != nil {
			return err
		}
		if len(list.Items) != 1 {
			return errors.New("unexpected initial MS inventory")
		}
		f.ms = list.Items[0].DeepCopy()
		if ptr.Deref(f.ms.Status.ReadyReplicas, 0) != 1 || ptr.Deref(f.ms.Status.AvailableReplicas, 0) != 1 || ptr.Deref(f.ms.Status.UpToDateReplicas, 0) != 1 {
			return errors.New("actual MS observed replica gates not met")
		}
		if !f.machine.Status.NodeRef.IsDefined() || f.machine.Status.NodeRef.Name != f.node.Name || f.machine.Spec.ProviderID != f.node.Spec.ProviderID {
			return errors.New("actual NodeRef/providerID not matched")
		}
		return nil
	})
	f.initialInventory = runtimeCoreSnapshot(t, ctx, c, namespace)
	require.Len(t, f.initialInventory, 5, "exactly Machine/MS/TC/Infra/Node initially")
	watchCtx, stopWatches := context.WithCancel(ctx)
	f.transitions = runtimeCoreWatchTransitions(t, watchCtx, e.Config, namespace, f.initialInventory, f.seed)
	t.Cleanup(func() {
		stopWatches()
		for _, done := range f.transitions.done {
			<-done
		}
		f.transitions.assert(t)
	})
	for end := time.Now().Add(2 * time.Second); time.Now().Before(end); time.Sleep(100 * time.Millisecond) {
		f.transitions.assert(t)
		runtimeCoreAssertInventory(t, ctx, c, namespace, f.initialInventory)
	}
	return f
}

func (f *runtimeCoreFixture) rotate(t *testing.T, revision string, upgrade bool) {
	t.Helper()
	private := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "registry-" + revision, Namespace: f.namespace}, Data: map[string][]byte{"patch": []byte("apiVersion: v1alpha1\nkind: RegistryAuthConfig\nname: ghcr.io\nusername: test-user\npassword: " + runtimeCorePrivate + "\n")}}
	require.NoError(t, f.c.Create(f.ctx, private))
	spec := *f.initialTCSpec.DeepCopy()
	spec.ConfigRevision = revision
	spec.StrategicPatchSecretRefs = []bootstrapv1.SecretKeyReference{{Name: private.Name, Key: "patch"}}
	if upgrade {
		spec.TalosVersion = runtimeCoreOSVersion
		spec.ImageFactory = &bootstrapv1.ImageFactorySpec{Extensions: []string{"siderolabs/tailscale"}}
	}
	f.transitions.assert(t)
	f.transitions.allowRevision()
	f.rotations++
	template := &bootstrapv1.TalosConfigTemplate{ObjectMeta: metav1.ObjectMeta{Name: "worker-" + revision, Namespace: f.namespace}, Spec: bootstrapv1.TalosConfigTemplateSpec{Template: bootstrapv1.TalosConfigTemplateResource{Spec: spec}}}
	require.NoError(t, f.c.Create(f.ctx, template))
	require.NoError(t, f.c.Get(f.ctx, client.ObjectKeyFromObject(f.md), f.md))
	before := f.md.DeepCopy()
	f.md.Spec.Template.Spec.Bootstrap.ConfigRef.Name = template.Name
	f.transitions.setDesiredTemplate(template.Name)
	f.md.Spec.Rollout.Strategy.RollingUpdate.MaxSurge = ptr.To(intstr.FromInt32(0))
	f.md.Spec.Rollout.Strategy.RollingUpdate.MaxUnavailable = ptr.To(intstr.FromInt32(1))
	require.NoError(t, f.c.Patch(f.ctx, f.md, client.MergeFrom(before)))
	runtimeCoreEventually(t, "core moves same Machine to reviewed desired MS", 30*time.Second, func() error {
		if err := f.c.Get(f.ctx, client.ObjectKeyFromObject(f.tc), f.tc); err != nil {
			return err
		}
		if f.tc.Spec.ConfigRevision != revision {
			return errors.New("desired config revision not propagated")
		}
		return nil
	})
}
func (f *runtimeCoreFixture) assertIdentities(t *testing.T, allowDesiredMS bool) {
	t.Helper()
	snapshot := runtimeCoreSnapshot(t, f.ctx, f.c, f.namespace)
	for key, uid := range f.initialInventory {
		if _, present := snapshot[key]; !present && strings.HasPrefix(key, "MachineSetList/") && f.transitions.isRetired(key, uid) {
			continue
		}
		require.Equal(t, uid, snapshot[key], "initial graph identity must survive: "+key)
	}
	f.transitions.assert(t)
	expected := len(f.initialInventory)
	if allowDesiredMS {
		expected += f.rotations
	}
	require.LessOrEqual(t, len(snapshot), expected, "no hidden additional graph")
	if f.history == nil {
		f.history = map[string]types.UID{}
	}
	for name, uid := range f.history {
		if _, present := snapshot[name]; !present && f.transitions.isRetired(name, uid) {
			continue
		}
		require.Equal(t, uid, snapshot[name], "core revision-history MS identity preserved while present")
	}
	for name, uid := range snapshot {
		if strings.HasPrefix(name, "MachineSetList/") {
			f.history[name] = uid
		}
	}
	for key := range snapshot {
		if _, original := f.initialInventory[key]; original {
			continue
		}
		require.True(t, allowDesiredMS && strings.HasPrefix(key, "MachineSetList/"), "only one core-created desired MS is allowed")
	}
	require.NoError(t, f.c.Get(f.ctx, client.ObjectKeyFromObject(f.machine), f.machine))
	require.NoError(t, f.c.Get(f.ctx, client.ObjectKeyFromObject(f.tc), f.tc))
	require.NoError(t, f.c.Get(f.ctx, client.ObjectKeyFromObject(f.infra), f.infra))
	require.NoError(t, f.c.Get(f.ctx, client.ObjectKeyFromObject(f.node), f.node))
	require.NoError(t, f.c.Get(f.ctx, client.ObjectKeyFromObject(f.seed), f.seed))
	require.Equal(t, f.originalSeed.UID, f.seed.UID)
	require.Equal(t, f.originalSeed.Name, ptr.Deref(f.machine.Spec.Bootstrap.DataSecretName, ""))
	require.Equal(t, f.tc.Name, f.machine.Spec.Bootstrap.ConfigRef.Name)
	require.Equal(t, f.infra.GetName(), f.machine.Spec.InfrastructureRef.Name)
	require.Equal(t, f.node.Name, f.machine.Status.NodeRef.Name)
	require.Equal(t, f.node.Spec.ProviderID, f.machine.Spec.ProviderID)
	require.True(t, f.machine.DeletionTimestamp.IsZero())
	require.True(t, f.infra.GetDeletionTimestamp().IsZero())
	require.Equal(t, "hcloud://bm-2873475", f.node.Spec.ProviderID)
	require.Equal(t, "hcloud://bm-2873475", f.machine.Spec.ProviderID)
	// Replica assignments legitimately change during core's move transaction; steady old0/new1 is checked after bounded settlement.
	// Fresh CABPT-owned seed.value may be regenerated; accepted old image intent may not advance early.
	public := []client.Object{f.machine, f.tc, f.md}
	for _, obj := range public {
		b, err := json.Marshal(obj)
		require.NoError(t, err)
		require.NotContains(t, string(b), runtimeCorePrivate)
	}
	events := &corev1.EventList{}
	require.NoError(t, f.c.List(f.ctx, events, client.InNamespace(f.namespace)))
	b, err := json.Marshal(events)
	require.NoError(t, err)
	require.NotContains(t, string(b), runtimeCorePrivate)
}
func (f *runtimeCoreFixture) readJournal(t *testing.T) (*corev1.Secret, *updateoperation.Journal) {
	t.Helper()
	s, j, err := updateoperation.ReadJournal(f.ctx, f.c, f.namespace, updateoperation.ObjectIdentity{APIGroup: bootstrapv1.GroupVersion.Group, Kind: "TalosConfig", Name: f.tc.Name, UID: f.tc.UID})
	require.NoError(t, err)
	return s, j
}
func (f *runtimeCoreFixture) assertPending(t *testing.T) {
	t.Helper()
	f.assertIdentities(t, true)
	s, j := f.readJournal(t)
	require.Equal(t, updateoperation.Planned, j.Phase, "normal recovery wait must remain active, never Ambiguous/terminal")
	for _, step := range j.Steps {
		require.NotEqual(t, updateoperation.StepAmbiguous, step.Phase, "held recovery gate must not cause ambiguous operation")
	}
	require.Empty(t, s.Data[updateoperation.ReceiptKey], "receipt must not exist before actual recovery")
	require.Contains(t, f.machine.Annotations, clusterv1.UpdateInProgressAnnotation)
	accepted, err := updateoperation.DecodeAcceptedIntent(f.seed.Data[updateoperation.AcceptedIntentKey])
	require.NoError(t, err)
	require.Equal(t, f.accepted.Image, accepted.Image, "old accepted image intent persists through render/Issued")
}
func (f *runtimeCoreFixture) awaitCompleted(t *testing.T) {
	t.Helper()
	runtimeCoreEventually(t, "actual handler/core complete only observed recovery", 180*time.Second, func() error {
		if err := f.c.Get(f.ctx, client.ObjectKeyFromObject(f.machine), f.machine); err != nil {
			return err
		}
		if _, pending := f.machine.Annotations[clusterv1.UpdateInProgressAnnotation]; pending {
			s, j, err := updateoperation.ReadJournal(f.ctx, f.c, f.namespace, updateoperation.ObjectIdentity{APIGroup: bootstrapv1.GroupVersion.Group, Kind: "TalosConfig", Name: f.tc.Name, UID: f.tc.UID})
			if err != nil {
				return errors.New("core update pending; durable journal not readable")
			}
			phases := make([]string, 0, len(j.Steps))
			for _, step := range j.Steps {
				phases = append(phases, string(step.Kind)+":"+string(step.Phase))
			}
			if j.Phase == updateoperation.Ambiguous {
				t.Fatal("actual handler operation became Ambiguous")
			}
			return fmt.Errorf("core update pending: phase=%s steps=%v polls=%d receipt=%t", j.Phase, phases, f.proxy.count(), len(s.Data[updateoperation.ReceiptKey]) > 0)
		}
		for _, typ := range []string{clusterv1.MachineReadyCondition, clusterv1.MachineAvailableCondition, clusterv1.MachineUpToDateCondition} {
			if !conditions.IsTrue(f.machine, typ) {
				return fmt.Errorf("Machine %s not healthy", typ)
			}
		}
		return nil
	})
	f.assertIdentities(t, true)
	s, j := f.readJournal(t)
	require.Equal(t, updateoperation.Observed, j.Phase)
	require.NotEmpty(t, s.Data[updateoperation.ReceiptKey])
	receipt, err := updateoperation.ReadReceipt(f.ctx, f.c, f.namespace, j.TalosConfig, j.TransactionID)
	require.NoError(t, err)
	require.Equal(t, f.machine.UID, receipt.MachineUID)
	require.Equal(t, f.infra.GetUID(), receipt.InfrastructureUID)
	accepted, err := updateoperation.DecodeAcceptedIntent(f.seed.Data[updateoperation.AcceptedIntentKey])
	require.NoError(t, err)
	require.Equal(t, j.TargetImage, accepted.Image)
	require.Equal(t, j.TransactionID, accepted.TransactionID)
	f.external.mu.Lock()
	authenticated := f.external.peerAuthenticated
	f.external.mu.Unlock()
	require.True(t, authenticated, "actual node mTLS client and SNI were verified")
}

func (f *runtimeCoreFixture) settledHistory(t *testing.T) {
	t.Helper()
	runtimeCoreEventually(t, "exact core immutable-template revision history settled", 30*time.Second, func() error {
		f.transitions.assert(t)
		list := &clusterv1.MachineSetList{}
		if err := f.c.List(f.ctx, list, client.InNamespace(f.namespace)); err != nil {
			return err
		}
		if len(list.Items) < 1 || len(list.Items) > f.rotations+1 {
			return errors.New("unexpected MachineSet history count")
		}
		current := 0
		for _, ms := range list.Items {
			if !metav1.IsControlledBy(&ms, f.md) {
				return errors.New("unexpected MS owner")
			}
			if ms.Spec.Template.Spec.Bootstrap.ConfigRef.Name == f.md.Spec.Template.Spec.Bootstrap.ConfigRef.Name {
				current++
				if ptr.Deref(ms.Spec.Replicas, 0) != 1 {
					return errors.New("desired MS replica not settled")
				}
			} else if ptr.Deref(ms.Spec.Replicas, 0) != 0 {
				return errors.New("old MS replicas not zero")
			}
		}
		if current != 1 {
			return errors.New("not exactly one desired MachineSet")
		}
		return nil
	})
	for end := time.Now().Add(2 * time.Second); time.Now().Before(end); time.Sleep(100 * time.Millisecond) {
		f.assertIdentities(t, true)
	}
}

func (f *runtimeCoreFixture) holdRecoveryGate(t *testing.T, label string) {
	t.Helper()
	// First wait for actual mutation acknowledgments, not just a bookkeeping poll.
	runtimeCoreEventually(t, "mutation acknowledgments before held "+label, 90*time.Second, func() error {
		f.assertPending(t)
		_, j := f.readJournal(t)
		for _, step := range j.Steps {
			if step.Phase == updateoperation.StepPlanned || step.Phase == updateoperation.Issued {
				return errors.New("mutation still unacknowledged")
			}
		}
		return nil
	})
	polls := f.proxy.count()
	runtimeCoreEventually(t, "real hook evaluates held "+label, 100*time.Second, func() error {
		f.assertPending(t)
		if f.proxy.count()-polls < 2 {
			return errors.New("waiting for two completed core hook polls beyond mutation acknowledgments")
		}
		return nil
	})
	f.assertPending(t)
}

func TestRuntimeCoreChainedRegistryThenDelayedInstall(t *testing.T) {
	assets, source, binary, caph, repo := runtimeCoreInputs(t)
	if os.Getenv("CABPT_RUNTIME_CORE_CHILD") != "1" {
		self, err := os.Executable()
		require.NoError(t, err)
		command := exec.Command(self, "-test.v", "-test.run=^TestRuntimeCoreChainedRegistryThenDelayedInstall$", "-test.timeout=15m")
		for _, item := range os.Environ() {
			if !strings.HasPrefix(item, "CABPT_RUNTIME_CORE_CHILD=") {
				command.Env = append(command.Env, item)
			}
		}
		command.Env = append(command.Env, "CABPT_RUNTIME_CORE_CHILD=1")
		var output runtimeCoreLog
		command.Stdout = &output
		command.Stderr = &output
		err = command.Run()
		text := output.String()
		require.NotContains(t, text, runtimeCorePrivate, "all actual runtime/handler log surfaces must remain private")
		if err != nil {
			t.Fatalf("isolated actual runtime test failed: %v\n%s", err, text)
		}
		t.Log("isolated actual-core runtime child completed")
		return
	}
	f := runtimeCoreNewFixture(t, assets, source, binary, caph, repo)
	f.rotate(t, "registry", false)
	runtimeCoreEventually(t, "actual private registry apply RPC", 70*time.Second, func() error {
		f.transitions.assert(t)
		a, _, _, _ := f.external.counts()
		if a == 0 {
			return errors.New("no real ApplyConfiguration")
		}
		return nil
	})
	f.awaitCompleted(t)
	f.settledHistory(t)
	a0, p0, i0, r0 := f.external.counts()
	require.Equal(t, 1, a0, "single issuer for config-only apply")
	require.Zero(t, p0)
	require.Zero(t, i0)
	require.Zero(t, r0)
	require.Positive(t, f.proxy.count())
	f.external.mu.Lock()
	require.Contains(t, string(f.external.active), runtimeCorePrivate)
	f.external.mu.Unlock()
	_, priorJournal := f.readJournal(t)
	firstTransaction := priorJournal.TransactionID
	require.NoError(t, f.c.Get(f.ctx, client.ObjectKeyFromObject(f.seed), f.seed))
	accepted, err := updateoperation.DecodeAcceptedIntent(f.seed.Data[updateoperation.AcceptedIntentKey])
	require.NoError(t, err)
	f.accepted = accepted
	pollsBeforeInstall := f.proxy.count()
	f.rotate(t, "install", true)
	select {
	case <-f.external.installStarted:
	case <-time.After(180 * time.Second):
		t.Fatal("actual Lifecycle install was never issued")
	}
	f.assertPending(t)
	_, j := f.readJournal(t)
	require.NotEqual(t, firstTransaction, j.TransactionID, "normal provider receipt consumption must enable a new transaction on the same Machine")
	issuer := ""
	for _, step := range j.Steps {
		if step.Kind == updateoperation.Install {
			require.Equal(t, updateoperation.Issued, step.Phase)
			require.NotEmpty(t, step.IssuerID)
			issuer = step.IssuerID
		}
	}
	require.NotEmpty(t, issuer)
	runtimeCoreEventually(t, "at least three real core hook polls while install pending", 70*time.Second, func() error {
		f.assertPending(t)
		if f.proxy.count()-pollsBeforeInstall < 3 {
			return errors.New("waiting for actual core UpdateMachine polls")
		}
		return nil
	})
	_, j = f.readJournal(t)
	for _, step := range j.Steps {
		if step.Kind == updateoperation.Install {
			require.Equal(t, issuer, step.IssuerID)
			require.Equal(t, updateoperation.Issued, step.Phase)
		}
	}
	a, p, i, r := f.external.counts()
	require.Equal(t, 2, a)
	require.Equal(t, 2, p, "desired installer pull followed by immutable digest key materialization")
	require.Equal(t, 1, i)
	require.Zero(t, r)
	f.external.mu.Lock()
	refs := append([]string(nil), f.external.pulledRefs...)
	installed := f.external.installedRef
	f.external.mu.Unlock()
	require.Equal(t, []string{j.TargetImage.Reference, strings.SplitN(j.TargetImage.Reference, "@", 2)[0] + "@" + runtimeCoreDigest}, refs)
	require.Equal(t, refs[1], installed, "Lifecycle installation uses immutable pulled key")
	close(f.external.installAck)
	runtimeCoreEventually(t, "install ack leads to one reboot request", 90*time.Second, func() error {
		_, _, _, r := f.external.counts()
		if r != 1 {
			return errors.New("reboot not yet issued")
		}
		return nil
	})
	f.assertPending(t)
	// Same-version upgrade: each stage holds exactly one observed recovery gate.
	desiredConfig := append([]byte(nil), j.Configuration...)
	previousConfig := append([]byte(nil), f.originalSeed.Data["value"]...)
	f.external.mu.Lock()
	f.external.osVersion = runtimeCoreOSVersion
	f.external.schematic = runtimeCoreNextSchematic
	f.external.extension = true
	f.external.mu.Unlock()
	require.NoError(t, f.external.setRuntime(f.ctx))
	require.NoError(t, f.external.setActive(f.ctx, desiredConfig))
	f.holdRecoveryGate(t, "unchanged boot identity")
	f.external.mu.Lock()
	f.external.boot = "boot-after-upgrade"
	f.external.schematic = runtimeCoreSchematic
	f.external.mu.Unlock()
	require.NoError(t, f.external.setRuntime(f.ctx))
	f.holdRecoveryGate(t, "wrong schematic")
	f.external.mu.Lock()
	f.external.schematic = runtimeCoreNextSchematic
	f.external.extension = false
	f.external.mu.Unlock()
	require.NoError(t, f.external.resources.Destroy(f.ctx, resource.NewMetadata(runtimeresource.NamespaceName, runtimeresource.ExtensionStatusType, "tailscale", resource.VersionUndefined)))
	require.NoError(t, f.external.setRuntime(f.ctx))
	f.holdRecoveryGate(t, "missing extension identity")
	require.NoError(t, f.external.setActive(f.ctx, previousConfig))
	f.external.mu.Lock()
	f.external.extension = true
	f.external.mu.Unlock()
	require.NoError(t, f.external.setRuntime(f.ctx))
	f.holdRecoveryGate(t, "active configuration not converged")
	runtimeCoreHealthyNode(t, f.ctx, f.c, f.node, false)
	require.NoError(t, f.external.setActive(f.ctx, desiredConfig))
	f.holdRecoveryGate(t, "workload Node not Ready")
	runtimeCoreHealthyNode(t, f.ctx, f.c, f.node, true)
	f.awaitCompleted(t)
	f.settledHistory(t)
	a, p, i, r = f.external.counts()
	require.Equal(t, 2, a)
	require.Equal(t, 2, p)
	require.Equal(t, 1, i)
	require.Equal(t, 1, r)
}

func TestRuntimeCoreRegistrarSeparateNamespaceAndCARotation(t *testing.T) {
	assets, source, _, _, _ := runtimeCoreInputs(t)
	s := runtimeCoreScheme(t)
	e := &envtest.Environment{UseExistingCluster: ptr.To(false), BinaryAssetsDirectory: assets, Scheme: s, ErrorIfCRDPathMissing: true, CRDDirectoryPaths: []string{filepath.Join(source, "core/config/crd/bases")}}
	cfg, err := e.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, e.Stop()) })
	c, err := client.New(cfg, client.Options{Scheme: s})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	caDir := t.TempDir()
	caPath := filepath.Join(caDir, "ca.crt")
	_, _, ca := runtimeCoreCA(t)
	require.NoError(t, os.WriteFile(caPath, ca, 0600))
	registrar := inplace.NewRegistrar(c, inplace.RegistrarOptions{Name: "registrar-test", ServiceName: "cabpt-runtime-extension-service", ServiceNamespace: "alternate-provider-namespace", ServicePort: 443, CACertPath: caPath, ResyncPeriod: 100 * time.Millisecond})
	done := make(chan error, 1)
	go func() { done <- registrar.Start(ctx) }()
	t.Cleanup(func() { cancel(); require.NoError(t, <-done) })
	ec := &runtimev1.ExtensionConfig{ObjectMeta: metav1.ObjectMeta{Name: "registrar-test"}}
	runtimeCoreEventually(t, "actual Registrar namespace/service/CA", 10*time.Second, func() error {
		if err := c.Get(ctx, client.ObjectKeyFromObject(ec), ec); err != nil {
			return err
		}
		if ec.Spec.ClientConfig.Service.Namespace != "alternate-provider-namespace" || ec.Spec.ClientConfig.Service.Name != "cabpt-runtime-extension-service" || ptr.Deref(ec.Spec.ClientConfig.Service.Port, 0) != 443 || !bytes.Equal(ec.Spec.ClientConfig.CABundle, ca) || ec.Spec.ClientConfig.URL != "" {
			return errors.New("registrar does not match actual namespace/service/CA")
		}
		return nil
	})
	uid := ec.UID
	before := ec.DeepCopy()
	ec.Spec.NamespaceSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"reviewed": "namespace"}}
	require.NoError(t, c.Patch(ctx, ec, client.MergeFrom(before)))
	_, _, nextCA := runtimeCoreCA(t)
	tmp := filepath.Join(caDir, "next.crt")
	require.NoError(t, os.WriteFile(tmp, nextCA, 0600))
	require.NoError(t, os.Rename(tmp, caPath))
	runtimeCoreEventually(t, "actual Registrar rotated CA preserves scoped selector and UID", 10*time.Second, func() error {
		if err := c.Get(ctx, client.ObjectKeyFromObject(ec), ec); err != nil {
			return err
		}
		if !bytes.Equal(ec.Spec.ClientConfig.CABundle, nextCA) {
			return errors.New("CA not rotated")
		}
		require.Equal(t, uid, ec.UID)
		require.Equal(t, "namespace", ec.Spec.NamespaceSelector.MatchLabels["reviewed"])
		return nil
	})
	list := &runtimev1.ExtensionConfigList{}
	require.NoError(t, c.List(ctx, list))
	require.Len(t, list.Items, 1)
	require.Empty(t, ec.Status.Handlers, "Registrar metadata test must not fabricate Discovery or install stale EC into real core URL API")
}
