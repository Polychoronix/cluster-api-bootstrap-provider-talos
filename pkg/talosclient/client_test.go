// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package talosclient_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	machineryclient "github.com/siderolabs/talos/pkg/machinery/client"
	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/types/known/emptypb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/talosclient"
)

const (
	serverName      = "talos-api.example.test"
	privateSentinel = "PRIVATE-IDENTITY-SENTINEL"
)

var clusterKey = types.NamespacedName{Namespace: "tenant-a", Name: "cluster-a"}

type authority struct {
	certificate *x509.Certificate
	key         ed25519.PrivateKey
	pem         []byte
}

func newAuthority(t *testing.T) authority {
	t.Helper()
	public, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "in-memory Talos CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, key)
	require.NoError(t, err)
	certificate, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	return authority{certificate: certificate, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

func (ca authority) issue(t *testing.T, name string, usage x509.ExtKeyUsage) (tls.Certificate, []byte, []byte) {
	t.Helper()

	return ca.issueDuring(t, name, usage, time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
}

func (ca authority) issueDuring(t *testing.T, name string, usage x509.ExtKeyUsage, notBefore, notAfter time.Time) (tls.Certificate, []byte, []byte) {
	t.Helper()
	public, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
	}
	if usage == x509.ExtKeyUsageServerAuth {
		template.DNSNames = []string{name}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.certificate, public, ca.key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	certificatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	certificate, err := tls.X509KeyPair(certificatePEM, keyPEM)
	require.NoError(t, err)

	return certificate, certificatePEM, keyPEM
}

func identityContext(t *testing.T, ca authority) *clientconfig.Context {
	t.Helper()
	_, certificate, key := ca.issue(t, "talos-client", x509.ExtKeyUsageClientAuth)

	return &clientconfig.Context{
		Endpoints: []string{"ignored.example.test:50000"},
		Nodes:     []string{"ignored-node-1", "ignored-node-2"},
		CA:        base64.StdEncoding.EncodeToString(ca.pem),
		Crt:       base64.StdEncoding.EncodeToString(certificate),
		Key:       base64.StdEncoding.EncodeToString(key),
		ProxyURL:  "https://ignored-proxy.example.test",
		Auth: clientconfig.Auth{
			Basic:    &clientconfig.Basic{Username: "ignored", Password: privateSentinel},
			SideroV1: &clientconfig.SideroV1{Identity: privateSentinel},
		},
	}
}

func identitySecret(t *testing.T, identity *clientconfig.Context) *corev1.Secret {
	t.Helper()
	data, err := (&clientconfig.Config{
		Context: "selected",
		Contexts: map[string]*clientconfig.Context{
			"selected":     identity,
			"not-selected": {CA: privateSentinel, Crt: privateSentinel, Key: privateSentinel},
		},
	}).Bytes()
	require.NoError(t, err)

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: clusterKey.Namespace, Name: clusterKey.Name + "-talosconfig"},
		Data:       map[string][]byte{"talosconfig": data},
	}
}

func secretReader(t *testing.T, secrets ...*corev1.Secret) client.Reader {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	builder := fake.NewClientBuilder().WithScheme(scheme)
	for _, secret := range secrets {
		builder = builder.WithObjects(secret)
	}

	return builder.Build()
}

type observation struct {
	metadata           metadata.MD
	serverName         string
	clientCertificates int
	verifiedChains     int
}

type versionServer struct {
	machineapi.UnimplementedMachineServiceServer
	calls chan observation
}

func (s *versionServer) Version(ctx context.Context, _ *emptypb.Empty) (*machineapi.VersionResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	seen := observation{metadata: md.Copy()}
	if p, ok := peer.FromContext(ctx); ok {
		if info, ok := p.AuthInfo.(credentials.TLSInfo); ok {
			seen.serverName = info.State.ServerName
			seen.clientCertificates = len(info.State.PeerCertificates)
			seen.verifiedChains = len(info.State.VerifiedChains)
		}
	}
	s.calls <- seen

	return &machineapi.VersionResponse{Messages: []*machineapi.Version{{Version: &machineapi.VersionInfo{Tag: "v1.14.0"}}}}, nil
}

func startVersionServer(t *testing.T, ca authority) (string, <-chan observation) {
	t.Helper()

	return startVersionServerWithClientCA(t, ca, ca)
}

func startVersionServerWithClientCA(t *testing.T, ca, clientCA authority) (string, <-chan observation) {
	t.Helper()
	certificate, _, _ := ca.issue(t, serverName, x509.ExtKeyUsageServerAuth)
	roots := x509.NewCertPool()
	roots.AddCert(clientCA.certificate)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{certificate},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    roots,
	})))
	service := &versionServer{calls: make(chan observation, 16)}
	machineapi.RegisterMachineServiceServer(server, service)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		<-done
	})

	return listener.Addr().String(), service.calls
}

// This proxy forwards raw TCP, so TLS terminates only at the Version server.
// Its distinct listening address proves Address is not inferred from the SAN.
func startTCPProxy(t *testing.T, destination string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var mutex sync.Mutex
	connections := map[net.Conn]struct{}{}
	var handlers sync.WaitGroup
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			connection, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			mutex.Lock()
			connections[connection] = struct{}{}
			mutex.Unlock()
			handlers.Add(1)
			go func() {
				defer handlers.Done()
				defer func() {
					_ = connection.Close()
					mutex.Lock()
					delete(connections, connection)
					mutex.Unlock()
				}()
				upstream, dialErr := net.DialTimeout("tcp", destination, time.Second)
				if dialErr != nil {
					return
				}
				copied := make(chan struct{}, 2)
				forward := func(to, from net.Conn) {
					_, _ = io.Copy(to, from)
					_ = connection.Close()
					_ = upstream.Close()
					copied <- struct{}{}
				}
				go forward(upstream, connection)
				go forward(connection, upstream)
				<-copied
				<-copied
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-acceptDone
		mutex.Lock()
		for connection := range connections {
			_ = connection.Close()
		}
		mutex.Unlock()
		handlers.Wait()
	})

	return listener.Addr().String()
}

func TestNewVersionDirectAndProxy(t *testing.T) {
	for _, proxied := range []bool{false, true} {
		name := "direct"
		if proxied {
			name = "proxy"
		}
		t.Run(name, func(t *testing.T) {
			ca := newAuthority(t)
			address, calls := startVersionServer(t, ca)
			endpoint := bootstrapv1.TalosEndpoint{Address: address, ServerName: serverName}
			if proxied {
				endpoint.Address = startTCPProxy(t, address)
				endpoint.Node = "10.42.0.17"
			}
			secret := identitySecret(t, identityContext(t, ca))
			original := secret.DeepCopy()
			reader := secretReader(t, secret)
			talos, err := talosclient.New(context.Background(), reader, clusterKey, endpoint)
			require.NoError(t, err)
			t.Cleanup(func() { _ = talos.Close() })
			assert.Equal(t, []string{endpoint.Address}, talos.GetEndpoints())

			for range 2 {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("trace", "preserved", "node", "stale-node", "nodes", "stale-aggregate"))
				response, rpcErr := talos.Version(talosclient.WithNode(ctx, endpoint))
				cancel()
				require.NoError(t, rpcErr)
				require.Len(t, response.Messages, 1)
				assert.Equal(t, "v1.14.0", response.Messages[0].Version.Tag)
				select {
				case seen := <-calls:
					assert.Equal(t, serverName, seen.serverName)
					assert.Equal(t, 1, seen.clientCertificates)
					assert.Positive(t, seen.verifiedChains)
					assert.Equal(t, []string{"preserved"}, seen.metadata.Get("trace"))
					assert.Equal(t, []string{"Talos"}, seen.metadata.Get("runtime"))
					assert.Empty(t, seen.metadata.Get("nodes"))
					assert.Empty(t, seen.metadata.Get("authorization"))
					if proxied {
						assert.Equal(t, []string{endpoint.Node}, seen.metadata.Get("node"))
					} else {
						assert.Empty(t, seen.metadata.Get("node"))
					}
				case <-time.After(time.Second):
					t.Fatal("Version server did not observe RPC")
				}
			}
			stored := &corev1.Secret{}
			require.NoError(t, reader.Get(context.Background(), client.ObjectKeyFromObject(secret), stored))
			assert.True(t, bytes.Equal(original.Data["talosconfig"], stored.Data["talosconfig"]), "identity Secret bytes must remain unchanged")
			assert.Equal(t, original.OwnerReferences, stored.OwnerReferences)
		})
	}
}

func TestNewVersionRejectsInvalidPeerIdentity(t *testing.T) {
	for _, failure := range []string{"server SAN", "server CA", "client CA"} {
		t.Run(failure, func(t *testing.T) {
			ca := newAuthority(t)
			serverCA, clientCA := ca, ca
			if failure == "server CA" {
				serverCA = newAuthority(t)
			}
			if failure == "client CA" {
				clientCA = newAuthority(t)
			}
			address, calls := startVersionServerWithClientCA(t, serverCA, clientCA)
			identity := identityContext(t, ca)
			endpoint := bootstrapv1.TalosEndpoint{Address: address, ServerName: serverName}
			if failure == "server SAN" {
				endpoint.ServerName = "wrong.example.test"
			}
			talos, err := talosclient.New(context.Background(), secretReader(t, identitySecret(t, identity)), clusterKey, endpoint)
			require.NoError(t, err)
			t.Cleanup(func() { _ = talos.Close() })
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err = talos.Version(talosclient.WithNode(ctx, endpoint))
			require.Error(t, err)
			assert.NotContains(t, err.Error(), privateSentinel)
			select {
			case <-calls:
				t.Fatal("Version RPC reached service despite invalid peer identity")
			default:
			}
		})
	}
}

func TestVersionServerRequiresClientCertificate(t *testing.T) {
	ca := newAuthority(t)
	address, calls := startVersionServer(t, ca)
	roots := x509.NewCertPool()
	roots.AddCert(ca.certificate)
	// Negative control: a correctly verified server is still not enough to
	// authenticate. New rejects this identity before dialing (covered below).
	talos, err := machineryclient.New(context.Background(),
		machineryclient.WithEndpoints(address),
		machineryclient.WithTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: serverName}),
		machineryclient.WithGRPCDialOptions(grpc.WithNoProxy()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = talos.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = talos.Version(ctx)
	require.Error(t, err)
	select {
	case <-calls:
		t.Fatal("Version RPC reached service without client authentication")
	default:
	}
}

func TestNewVersionUnreachable(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	endpoint := bootstrapv1.TalosEndpoint{Address: listener.Addr().String(), ServerName: serverName, Node: "10.42.0.17"}
	talos, err := talosclient.New(context.Background(), secretReader(t, identitySecret(t, identityContext(t, newAuthority(t)))), clusterKey, endpoint)
	require.NoError(t, err)
	t.Cleanup(func() { _ = talos.Close() })
	require.NoError(t, listener.Close())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = talos.Version(talosclient.WithNode(ctx, endpoint))
	require.Error(t, err)
	assert.NotContains(t, err.Error(), privateSentinel)
}

func TestNewIdentityErrorsAreRedacted(t *testing.T) {
	identity := identityContext(t, newAuthority(t))
	valid := identitySecret(t, identity)
	endpoint := bootstrapv1.TalosEndpoint{Address: "127.0.0.1:50000", ServerName: serverName}
	tests := []struct {
		name   string
		mutate func(*corev1.Secret)
		stage  string
	}{
		{name: "missing key", mutate: func(s *corev1.Secret) { delete(s.Data, "talosconfig") }, stage: "empty"},
		{name: "empty", mutate: func(s *corev1.Secret) { s.Data["talosconfig"] = nil }, stage: "empty"},
		{name: "whitespace", mutate: func(s *corev1.Secret) { s.Data["talosconfig"] = []byte(" \n\t") }, stage: "empty"},
		{name: "invalid YAML", mutate: func(s *corev1.Secret) { s.Data["talosconfig"] = []byte("context: [" + privateSentinel) }, stage: "parse"},
		{name: "invalid context type", mutate: func(s *corev1.Secret) {
			s.Data["talosconfig"] = []byte("context: selected\ncontexts:\n  selected: " + privateSentinel)
		}, stage: "parse"},
		{name: "null context", mutate: func(s *corev1.Secret) {
			s.Data["talosconfig"] = []byte("context: selected\ncontexts:\n  selected: null")
		}, stage: "identity validation"},
		{name: "null unused context", mutate: func(s *corev1.Secret) {
			s.Data["talosconfig"] = []byte("context: selected\ncontexts:\n  selected: null\n  unused: null")
		}, stage: "identity validation"},
		{name: "no default", mutate: func(s *corev1.Secret) { replaceConfig(t, s, func(c *clientconfig.Config) { c.Context = "" }) }, stage: "identity validation"},
		{name: "missing default", mutate: func(s *corev1.Secret) {
			replaceConfig(t, s, func(c *clientconfig.Config) { c.Context = privateSentinel })
		}, stage: "identity validation"},
		{name: "missing CA", mutate: func(s *corev1.Secret) { replaceIdentity(t, s, func(c *clientconfig.Context) { c.CA = "" }) }, stage: "identity validation"},
		{name: "missing certificate", mutate: func(s *corev1.Secret) { replaceIdentity(t, s, func(c *clientconfig.Context) { c.Crt = "" }) }, stage: "identity validation"},
		{name: "missing private key", mutate: func(s *corev1.Secret) { replaceIdentity(t, s, func(c *clientconfig.Context) { c.Key = "" }) }, stage: "identity validation"},
		{name: "invalid CA base64", mutate: func(s *corev1.Secret) {
			replaceIdentity(t, s, func(c *clientconfig.Context) { c.CA = privateSentinel })
		}, stage: "CA validation"},
		{name: "invalid CA PEM", mutate: func(s *corev1.Secret) {
			replaceIdentity(t, s, func(c *clientconfig.Context) { c.CA = base64.StdEncoding.EncodeToString([]byte(privateSentinel)) })
		}, stage: "CA validation"},
		{name: "leaf used as CA", mutate: func(s *corev1.Secret) { replaceIdentity(t, s, func(c *clientconfig.Context) { c.CA = c.Crt }) }, stage: "CA validation"},
		{name: "invalid certificate", mutate: func(s *corev1.Secret) {
			replaceIdentity(t, s, func(c *clientconfig.Context) { c.Crt = base64.StdEncoding.EncodeToString([]byte(privateSentinel)) })
		}, stage: "client certificate validation"},
		{name: "invalid key base64", mutate: func(s *corev1.Secret) {
			replaceIdentity(t, s, func(c *clientconfig.Context) { c.Key = privateSentinel })
		}, stage: "client certificate validation"},
		{name: "invalid key PEM", mutate: func(s *corev1.Secret) {
			replaceIdentity(t, s, func(c *clientconfig.Context) { c.Key = base64.StdEncoding.EncodeToString([]byte(privateSentinel)) })
		}, stage: "client certificate validation"},
		{name: "mismatched key", mutate: func(s *corev1.Secret) {
			replaceIdentity(t, s, func(c *clientconfig.Context) { c.Key = identityContext(t, newAuthority(t)).Key })
		}, stage: "client certificate validation"},
		{name: "untrusted client certificate", mutate: func(s *corev1.Secret) {
			replaceIdentity(t, s, func(c *clientconfig.Context) {
				other := identityContext(t, newAuthority(t))
				c.Crt, c.Key = other.Crt, other.Key
			})
		}, stage: "client certificate validation"},
		{name: "unrelated CA", mutate: func(s *corev1.Secret) {
			replaceIdentity(t, s, func(c *clientconfig.Context) { c.CA = base64.StdEncoding.EncodeToString(newAuthority(t).pem) })
		}, stage: "client certificate validation"},
		{name: "server-only client certificate", mutate: func(s *corev1.Secret) {
			ca := newAuthority(t)
			_, certificate, key := ca.issue(t, serverName, x509.ExtKeyUsageServerAuth)
			replaceIdentity(t, s, func(c *clientconfig.Context) {
				c.CA = base64.StdEncoding.EncodeToString(ca.pem)
				c.Crt = base64.StdEncoding.EncodeToString(certificate)
				c.Key = base64.StdEncoding.EncodeToString(key)
			})
		}, stage: "client certificate validation"},
		{name: "expired client certificate", mutate: func(s *corev1.Secret) {
			ca := newAuthority(t)
			_, certificate, key := ca.issueDuring(t, "expired", x509.ExtKeyUsageClientAuth, time.Now().Add(-time.Hour), time.Now().Add(-time.Minute))
			replaceIdentity(t, s, func(c *clientconfig.Context) {
				c.CA = base64.StdEncoding.EncodeToString(ca.pem)
				c.Crt = base64.StdEncoding.EncodeToString(certificate)
				c.Key = base64.StdEncoding.EncodeToString(key)
			})
		}, stage: "client certificate validation"},
		{name: "future client certificate", mutate: func(s *corev1.Secret) {
			ca := newAuthority(t)
			_, certificate, key := ca.issueDuring(t, "future", x509.ExtKeyUsageClientAuth, time.Now().Add(time.Minute), time.Now().Add(time.Hour))
			replaceIdentity(t, s, func(c *clientconfig.Context) {
				c.CA = base64.StdEncoding.EncodeToString(ca.pem)
				c.Crt = base64.StdEncoding.EncodeToString(certificate)
				c.Key = base64.StdEncoding.EncodeToString(key)
			})
		}, stage: "client certificate validation"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			secret := valid.DeepCopy()
			test.mutate(secret)
			talos, err := talosclient.New(context.Background(), secretReader(t, secret), clusterKey, endpoint)
			assert.Nil(t, talos)
			require.Error(t, err)
			assert.Equal(t, "talos client: Secret tenant-a/cluster-a-talosconfig key talosconfig: "+test.stage+" failed", err.Error())
			assert.NotContains(t, err.Error(), privateSentinel)
			assert.Nil(t, errors.Unwrap(err))
			ca, validationErr := talosclient.ValidateIdentity(secret.Data["talosconfig"])
			assert.Nil(t, ca)
			require.Error(t, validationErr)
			assert.Equal(t, "talos client: "+test.stage+" failed", validationErr.Error())
			assert.NotContains(t, validationErr.Error(), privateSentinel)
			assert.Nil(t, errors.Unwrap(validationErr))
		})
	}
}

func replaceConfig(t *testing.T, secret *corev1.Secret, mutate func(*clientconfig.Config)) {
	t.Helper()
	var config clientconfig.Config
	require.NoError(t, yaml.NewDecoder(bytes.NewReader(secret.Data["talosconfig"])).Decode(&config))
	mutate(&config)
	data, err := config.Bytes()
	require.NoError(t, err)
	secret.Data["talosconfig"] = data
}

func replaceIdentity(t *testing.T, secret *corev1.Secret, mutate func(*clientconfig.Context)) {
	t.Helper()
	replaceConfig(t, secret, func(config *clientconfig.Config) { mutate(config.Contexts[config.Context]) })
}

type failingReader struct {
	client.Reader
	key types.NamespacedName
}

func (r *failingReader) Get(_ context.Context, key client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
	r.key = key

	return errors.New(privateSentinel)
}

func TestNewIdentityLookupIsNamespaceLocalAndRedacted(t *testing.T) {
	endpoint := bootstrapv1.TalosEndpoint{Address: "127.0.0.1:50000", ServerName: serverName}
	foreign := identitySecret(t, identityContext(t, newAuthority(t)))
	foreign.Namespace = "tenant-b"
	for _, secrets := range [][]*corev1.Secret{nil, {foreign}} {
		talos, err := talosclient.New(context.Background(), secretReader(t, secrets...), clusterKey, endpoint)
		assert.Nil(t, talos)
		require.Error(t, err)
		assert.Equal(t, "talos client: Secret tenant-a/cluster-a-talosconfig key talosconfig: lookup failed", err.Error())
		assert.Nil(t, errors.Unwrap(err))
	}
	reader := &failingReader{}
	talos, err := talosclient.New(context.Background(), reader, clusterKey, endpoint)
	assert.Nil(t, talos)
	require.Error(t, err)
	assert.Equal(t, types.NamespacedName{Namespace: "tenant-a", Name: "cluster-a-talosconfig"}, reader.key)
	assert.NotContains(t, err.Error(), privateSentinel)
	assert.Nil(t, errors.Unwrap(err))
}

func TestNewRejectsInvalidPublicEndpoint(t *testing.T) {
	reader := secretReader(t, identitySecret(t, identityContext(t, newAuthority(t))))
	for _, endpoint := range []bootstrapv1.TalosEndpoint{
		{Address: "127.0.0.1", ServerName: serverName},
		{Address: "https://127.0.0.1:50000", ServerName: serverName},
		{Address: "127.0.0.1:0", ServerName: serverName},
		{Address: "127.0.0.1:65536", ServerName: serverName},
		{Address: "127.0.0.1:50000", ServerName: ""},
		{Address: "127.0.0.1:50000", ServerName: serverName + ":50000"},
		{Address: "127.0.0.1:50000", ServerName: serverName, Node: "node-a,node-b"},
	} {
		talos, err := talosclient.New(context.Background(), reader, clusterKey, endpoint)
		assert.Nil(t, talos)
		require.Error(t, err)
		assert.Equal(t, "talos client: endpoint validation failed", err.Error())
	}
	endpoint := bootstrapv1.TalosEndpoint{Address: "[::1]:50000", ServerName: "::1"}
	talos, err := talosclient.New(context.Background(), reader, clusterKey, endpoint)
	require.NoError(t, err)
	t.Cleanup(func() { _ = talos.Close() })
	assert.Equal(t, []string{endpoint.Address}, talos.GetEndpoints())
}

func TestNewRejectsInvalidIdentityReference(t *testing.T) {
	endpoint := bootstrapv1.TalosEndpoint{Address: "127.0.0.1:50000", ServerName: serverName}
	reader := &failingReader{}
	for _, key := range []types.NamespacedName{
		{Namespace: "", Name: "cluster-a"},
		{Namespace: "tenant-a", Name: ""},
		{Namespace: "tenant-a/tenant-b", Name: "cluster-a"},
		{Namespace: "tenant-a", Name: "../" + privateSentinel},
	} {
		talos, err := talosclient.New(context.Background(), reader, key, endpoint)
		assert.Nil(t, talos)
		require.Error(t, err)
		assert.Equal(t, "talos client: identity reference validation failed", err.Error())
		assert.Empty(t, reader.key)
	}
	talos, err := talosclient.New(context.Background(), nil, clusterKey, endpoint)
	assert.Nil(t, talos)
	require.Error(t, err)
	assert.Equal(t, "talos client: identity reference validation failed", err.Error())
}

func TestWithNodePreservesContextAndReplacesRouting(t *testing.T) {
	type contextKey struct{}
	parent := context.WithValue(context.Background(), contextKey{}, "preserved")
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("trace", "preserved", "nodes", "node-a", "nodes", "node-b", "node", "old"))
	proxied := talosclient.WithNode(ctx, bootstrapv1.TalosEndpoint{Node: "node-c"})
	md, _ := metadata.FromOutgoingContext(proxied)
	assert.Equal(t, []string{"node-c"}, md.Get("node"))
	assert.Empty(t, md.Get("nodes"))
	assert.Equal(t, []string{"preserved"}, md.Get("trace"))
	assert.Equal(t, "preserved", proxied.Value(contextKey{}))
	original, _ := metadata.FromOutgoingContext(ctx)
	assert.Equal(t, []string{"node-a", "node-b"}, original.Get("nodes"))
	assert.Equal(t, []string{"old"}, original.Get("node"))
	direct := talosclient.WithNode(proxied, bootstrapv1.TalosEndpoint{})
	md, _ = metadata.FromOutgoingContext(direct)
	assert.Empty(t, md.Get("node"))
	assert.Empty(t, md.Get("nodes"))
	assert.Equal(t, []string{"preserved"}, md.Get("trace"))
	assert.Same(t, parent, talosclient.WithNode(parent, bootstrapv1.TalosEndpoint{}))
	cancel()
	assert.ErrorIs(t, proxied.Err(), context.Canceled)
	assert.ErrorIs(t, direct.Err(), context.Canceled)
}

func TestNewIgnoresFilesystemAndEnvironmentalIdentity(t *testing.T) {
	directory := t.TempDir()
	t.Setenv("TALOS_HOME", directory)
	t.Setenv("TALOSCONFIG", filepath.Join(directory, "must-not-create-talosconfig"))
	t.Setenv("SIDERO_SERVICE_ACCOUNT_KEY", privateSentinel)
	t.Setenv("OMNI_SERVICE_ACCOUNT_KEY", privateSentinel)
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	t.Setenv("ALL_PROXY", "http://127.0.0.1:1")
	t.Setenv("NO_PROXY", "")
	ca := newAuthority(t)
	address, calls := startVersionServer(t, ca)
	endpoint := bootstrapv1.TalosEndpoint{Address: address, ServerName: serverName}
	secret := identitySecret(t, identityContext(t, ca))
	replaceConfig(t, secret, func(config *clientconfig.Config) { config.Contexts["unused-null"] = nil })
	talos, err := talosclient.New(context.Background(), secretReader(t, secret), clusterKey, endpoint)
	require.NoError(t, err)
	t.Cleanup(func() { _ = talos.Close() })
	// Even inspecting the machinery client cannot trigger WithDefaultConfig
	// or expose the original credentials, endpoints or alternative auth.
	publicContext := talos.GetConfigContext()
	require.NotNil(t, publicContext)
	assert.Empty(t, publicContext.CA)
	assert.Empty(t, publicContext.Crt)
	assert.Empty(t, publicContext.Key)
	assert.Nil(t, publicContext.Auth.Basic)
	assert.Nil(t, publicContext.Auth.SideroV1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = talos.Version(talosclient.WithNode(ctx, endpoint))
	require.NoError(t, err)
	select {
	case seen := <-calls:
		assert.Empty(t, seen.metadata.Get("authorization"))
		assert.Equal(t, serverName, seen.serverName)
	case <-time.After(time.Second):
		t.Fatal("Version server did not observe RPC")
	}
	entries, err := os.ReadDir(directory)
	require.NoError(t, err)
	assert.Empty(t, entries, "client must not create any default configuration or auth files")
}

func TestValidateIdentityReturnsDecodedCAWithoutMutation(t *testing.T) {
	ca := newAuthority(t)
	secret := identitySecret(t, identityContext(t, ca))
	data := secret.Data["talosconfig"]
	original := bytes.Clone(data)
	decoded, err := talosclient.ValidateIdentity(data)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(ca.pem, decoded), "validator must return the imported CA exactly")
	assert.True(t, bytes.Equal(original, data), "validator must not mutate talosconfig bytes")
}

func TestValidateIdentityAcceptsIntermediateClientChain(t *testing.T) {
	root := newAuthority(t)
	public, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(3),
		Subject:               pkix.Name{CommonName: "in-memory intermediate CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, root.certificate, public, root.key)
	require.NoError(t, err)
	certificate, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	intermediate := authority{certificate: certificate, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
	identity := identityContext(t, intermediate)
	leaf, err := base64.StdEncoding.DecodeString(identity.Crt)
	require.NoError(t, err)
	identity.Crt = base64.StdEncoding.EncodeToString(append(leaf, intermediate.pem...))
	identity.CA = base64.StdEncoding.EncodeToString(root.pem)
	secret := identitySecret(t, identity)
	decoded, err := talosclient.ValidateIdentity(secret.Data["talosconfig"])
	require.NoError(t, err)
	assert.True(t, bytes.Equal(root.pem, decoded), "intermediate chain must validate against imported root")
	address, calls := startVersionServer(t, root)
	endpoint := bootstrapv1.TalosEndpoint{Address: address, ServerName: serverName}
	talos, err := talosclient.New(context.Background(), secretReader(t, secret), clusterKey, endpoint)
	require.NoError(t, err)
	t.Cleanup(func() { _ = talos.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = talos.Version(talosclient.WithNode(ctx, endpoint))
	require.NoError(t, err)
	select {
	case seen := <-calls:
		assert.Equal(t, 2, seen.clientCertificates)
		assert.Positive(t, seen.verifiedChains)
	case <-time.After(time.Second):
		t.Fatal("Version server did not observe intermediate-chain RPC")
	}
	identity.Crt = base64.StdEncoding.EncodeToString(leaf)
	secret = identitySecret(t, identity)
	decoded, err = talosclient.ValidateIdentity(secret.Data["talosconfig"])
	assert.Nil(t, decoded)
	require.Error(t, err)
	assert.Equal(t, "talos client: client certificate validation failed", err.Error())
	assert.Nil(t, errors.Unwrap(err))
}
