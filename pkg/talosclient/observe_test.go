// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package talosclient_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
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
	"github.com/siderolabs/talos/pkg/machinery/api/common"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	configapi "github.com/siderolabs/talos/pkg/machinery/api/resource/config"
	machineryclient "github.com/siderolabs/talos/pkg/machinery/client"
	"github.com/siderolabs/talos/pkg/machinery/config/configloader"
	configresource "github.com/siderolabs/talos/pkg/machinery/resources/config"
	k8sresource "github.com/siderolabs/talos/pkg/machinery/resources/k8s"
	runtimeresource "github.com/siderolabs/talos/pkg/machinery/resources/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/talosclient"
)

const observedConfiguration = `version: v1alpha1
machine:
  type: worker
---
apiVersion: v1alpha1
kind: RegistryAuthConfig
name: registry.example
username: private-user
password: PRIVATE-IDENTITY-SENTINEL
---
apiVersion: v1alpha1
kind: KubeletConfig
image: desired.example/kubelet:v1.36.0
`
const kernelBootID = "ba144cb9-b310-43f2-8126-d3e7313791a6"
const observedSchematic = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

type observerCOSI struct {
	*cosiserver.State
	get  func(context.Context, *cosiapi.GetRequest) (*cosiapi.GetResponse, error)
	list func(*cosiapi.ListRequest, cosiapi.State_ListServer) error
}

func (s *observerCOSI) Get(ctx context.Context, request *cosiapi.GetRequest) (*cosiapi.GetResponse, error) {
	if s.get != nil {
		return s.get(ctx, request)
	}
	return s.State.Get(ctx, request)
}

func (s *observerCOSI) List(request *cosiapi.ListRequest, stream cosiapi.State_ListServer) error {
	if s.list != nil {
		return s.list(request, stream)
	}
	return s.State.List(request, stream)
}

type observerMachine struct {
	machineapi.UnimplementedMachineServiceServer
	read func(*machineapi.ReadRequest, grpc.ServerStreamingServer[common.Data]) error
}

func (s *observerMachine) Read(request *machineapi.ReadRequest, stream grpc.ServerStreamingServer[common.Data]) error {
	if s.read != nil {
		return s.read(request, stream)
	}
	return stream.Send(&common.Data{Bytes: []byte(kernelBootID + "\n")})
}

type observerCall struct {
	method string
	seen   observation
}

type observerFixture struct {
	client    *machineryclient.Client
	endpoint  bootstrapv1.TalosEndpoint
	resources state.State
	cosi      *observerCOSI
	machine   *observerMachine
	mu        sync.Mutex
	calls     []observerCall
}

func newObserverFixture(t *testing.T, node string) *observerFixture {
	t.Helper()
	ca := newAuthority(t)
	certificate, _, _ := ca.issue(t, serverName, x509.ExtKeyUsageServerAuth)
	roots := x509.NewCertPool()
	roots.AddCert(ca.certificate)
	f := &observerFixture{resources: state.WrapCore(namespaced.NewState(inmem.Build)), machine: &observerMachine{}}
	f.cosi = &observerCOSI{State: cosiserver.NewState(f.resources)}
	record := func(ctx context.Context, method string) {
		md, _ := metadata.FromIncomingContext(ctx)
		seen := observation{metadata: md.Copy()}
		if p, ok := peer.FromContext(ctx); ok {
			if info, ok := p.AuthInfo.(credentials.TLSInfo); ok {
				seen.serverName = info.State.ServerName
				seen.clientCertificates = len(info.State.PeerCertificates)
				seen.verifiedChains = len(info.State.VerifiedChains)
			}
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		f.calls = append(f.calls, observerCall{method: method, seen: seen})
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots})),
		grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			record(ctx, info.FullMethod)
			return handler(ctx, request)
		}),
		grpc.StreamInterceptor(func(service any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			record(stream.Context(), info.FullMethod)
			return handler(service, stream)
		}),
	)
	cosiapi.RegisterStateServer(server, f.cosi)
	machineapi.RegisterMachineServiceServer(server, f.machine)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-done })
	f.endpoint = bootstrapv1.TalosEndpoint{Address: listener.Addr().String(), ServerName: serverName, Node: node}
	f.client, err = talosclient.New(t.Context(), secretReader(t, identitySecret(t, identityContext(t, ca))), clusterKey, f.endpoint)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.client.Close() })
	provider, err := configloader.NewFromBytes([]byte(observedConfiguration))
	require.NoError(t, err)
	boot := runtimeresource.NewBootID()
	boot.TypedSpec().BootID = kernelBootID
	version := runtimeresource.NewVersion()
	version.TypedSpec().Version = "v1.14.0"
	kubelet := k8sresource.NewKubeletStatus(k8sresource.NamespaceName, "arbitrary-actual-id")
	kubelet.TypedSpec().Image = "actual.example/kubelet:v1.36.0"
	for _, item := range []resource.Resource{configresource.NewMachineConfig(provider), boot, version, kubelet} {
		require.NoError(t, f.resources.Create(t.Context(), item))
	}
	return f
}

func observerContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return metadata.NewOutgoingContext(ctx, metadata.Pairs("node", "stale", "nodes", "stale-other", "trace", "kept"))
}

func addObservedExtension(t *testing.T, f *observerFixture, id, name, version, image string) {
	t.Helper()
	extension := runtimeresource.NewExtensionStatus(runtimeresource.NamespaceName, id)
	extension.TypedSpec().Metadata.Name = name
	extension.TypedSpec().Metadata.Version = version
	extension.TypedSpec().Image = image
	require.NoError(t, f.resources.Create(t.Context(), extension))
}

func requireObserverFailure(t *testing.T, result *talosclient.Observation, err error) {
	t.Helper()
	require.Error(t, err)
	assert.Nil(t, result, "partial private observations must not escape")
	assert.NotContains(t, err.Error(), privateSentinel)
	assert.Nil(t, errors.Unwrap(err))
}

func TestObserveActiveCompleteConfigurationAndTLSRouting(t *testing.T) {
	for _, node := range []string{"", "selected-node"} {
		t.Run("node="+node, func(t *testing.T) {
			f := newObserverFixture(t, node)
			staged, err := configloader.NewFromBytes([]byte("version: v1alpha1\nmachine:\n  type: worker\n"))
			require.NoError(t, err)
			persistent := configresource.NewMachineConfigWithID(staged, configresource.PersistentID)
			require.NoError(t, f.resources.Create(t.Context(), persistent))
			schematic := runtimeresource.NewImageFactorySchematic(runtimeresource.NamespaceName, runtimeresource.ImageFactorySchematicID)
			schematic.TypedSpec().SchematicID = observedSchematic
			require.NoError(t, f.resources.Create(t.Context(), schematic))
			addObservedExtension(t, f, "z", "zfs", "2.0", "zfs.sqsh")
			addObservedExtension(t, f, "a", "tailscale", "1.0", "tailscale.sqsh")
			addObservedExtension(t, f, "virtual", "schematic", observedSchematic, "")
			result, err := talosclient.Observe(observerContext(t), f.client, f.endpoint)
			require.NoError(t, err)
			assert.Equal(t, []byte(observedConfiguration), result.Configuration)
			assert.Equal(t, kernelBootID, result.BootID)
			assert.Equal(t, "v1.14.0", result.OSVersion)
			assert.Equal(t, observedSchematic, result.SchematicID)
			assert.Equal(t, "actual.example/kubelet:v1.36.0", result.KubeletImage)
			assert.Equal(t, []talosclient.ExtensionObservation{{Name: "tailscale", Version: "1.0", Image: "tailscale.sqsh"}, {Name: "zfs", Version: "2.0", Image: "zfs.sqsh"}}, result.Extensions)
			for _, formatted := range []string{fmt.Sprint(result), fmt.Sprintf("%+v", *result), fmt.Sprintf("%#v", result), fmt.Sprintf("%#v", *result)} {
				assert.NotContains(t, formatted, privateSentinel)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			require.Len(t, f.calls, 7)
			for _, call := range f.calls {
				assert.Contains(t, []string{"/cosi.resource.State/Get", "/cosi.resource.State/List"}, call.method)
				assert.Equal(t, serverName, call.seen.serverName)
				assert.Equal(t, 1, call.seen.clientCertificates)
				assert.Positive(t, call.seen.verifiedChains)
				assert.Empty(t, call.seen.metadata.Get("nodes"))
				if node == "" {
					assert.Empty(t, call.seen.metadata.Get("node"))
				} else {
					assert.Equal(t, []string{node}, call.seen.metadata.Get("node"))
				}
				assert.Equal(t, []string{"kept"}, call.seen.metadata.Get("trace"))
			}
		})
	}
}

func TestObserveMissingNewerResourcesUsesOnlyActualLegacyEvidence(t *testing.T) {
	f := newObserverFixture(t, "legacy-node")
	require.NoError(t, f.resources.Destroy(t.Context(), runtimeresource.NewBootID().Metadata()))
	require.NoError(t, f.resources.Destroy(t.Context(), k8sresource.NewKubeletStatus(k8sresource.NamespaceName, "arbitrary-actual-id").Metadata()))
	addObservedExtension(t, f, "0", "schematic", observedSchematic, "")
	reads := 0
	f.machine.read = func(request *machineapi.ReadRequest, stream grpc.ServerStreamingServer[common.Data]) error {
		reads++
		assert.Equal(t, "/proc/sys/kernel/random/boot_id", request.GetPath())
		if err := stream.Send(&common.Data{Bytes: []byte(kernelBootID[:10])}); err != nil {
			return err
		}
		return stream.Send(&common.Data{Bytes: []byte(kernelBootID[10:] + "\n")})
	}
	result, err := talosclient.Observe(observerContext(t), f.client, f.endpoint)
	require.NoError(t, err)
	assert.Equal(t, kernelBootID, result.BootID)
	assert.Empty(t, result.KubeletImage, "desired config must not supply the missing actual kubelet image")
	assert.Equal(t, observedSchematic, result.SchematicID)
	assert.Empty(t, result.Extensions)
	assert.Equal(t, 2, reads)
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, call := range f.calls {
		assert.Equal(t, []string{"legacy-node"}, call.seen.metadata.Get("node"))
		assert.Empty(t, call.seen.metadata.Get("nodes"))
		assert.NotContains(t, call.method, "Containers")
	}
}

func TestObserveMissingSchematicIsUnknown(t *testing.T) {
	f := newObserverFixture(t, "")
	result, err := talosclient.Observe(observerContext(t), f.client, f.endpoint)
	require.NoError(t, err)
	assert.Empty(t, result.SchematicID)
}

func TestObserveRequiredResourceFailures(t *testing.T) {
	for _, pointer := range []resource.Pointer{configresource.NewMachineConfig(nil).Metadata(), runtimeresource.NewVersion().Metadata()} {
		t.Run(pointer.Type(), func(t *testing.T) {
			f := newObserverFixture(t, "")
			require.NoError(t, f.resources.Destroy(t.Context(), pointer))
			result, err := talosclient.Observe(observerContext(t), f.client, f.endpoint)
			requireObserverFailure(t, result, err)
		})
	}
}

func TestObserveEmptyWitnesses(t *testing.T) {
	for _, empty := range []resource.Resource{runtimeresource.NewBootID(), runtimeresource.NewVersion(), k8sresource.NewKubeletStatus(k8sresource.NamespaceName, "arbitrary-actual-id"), runtimeresource.NewImageFactorySchematic(runtimeresource.NamespaceName, runtimeresource.ImageFactorySchematicID)} {
		t.Run(empty.Metadata().Type(), func(t *testing.T) {
			f := newObserverFixture(t, "")
			if _, err := f.resources.Get(t.Context(), empty.Metadata()); err == nil {
				require.NoError(t, f.resources.Destroy(t.Context(), empty.Metadata()))
			}
			require.NoError(t, f.resources.Create(t.Context(), empty))
			result, err := talosclient.Observe(observerContext(t), f.client, f.endpoint)
			requireObserverFailure(t, result, err)
		})
	}
}

func TestObserveDeniedCOSIIsRedactedAndNeverFallback(t *testing.T) {
	for _, kind := range []string{runtimeresource.BootIDType, configresource.MachineConfigType, runtimeresource.VersionType, runtimeresource.ImageFactorySchematicType, runtimeresource.ExtensionStatusType, k8sresource.KubeletStatusType} {
		t.Run(kind, func(t *testing.T) {
			f := newObserverFixture(t, "")
			f.cosi.get = func(ctx context.Context, request *cosiapi.GetRequest) (*cosiapi.GetResponse, error) {
				if request.GetType() == kind {
					return nil, status.Error(codes.PermissionDenied, privateSentinel)
				}
				return f.cosi.State.Get(ctx, request)
			}
			f.cosi.list = func(request *cosiapi.ListRequest, stream cosiapi.State_ListServer) error {
				if request.GetType() == kind {
					return status.Error(codes.PermissionDenied, privateSentinel)
				}
				return f.cosi.State.List(request, stream)
			}
			result, err := talosclient.Observe(observerContext(t), f.client, f.endpoint)
			requireObserverFailure(t, result, err)
			f.mu.Lock()
			defer f.mu.Unlock()
			for _, call := range f.calls {
				assert.NotContains(t, call.method, "/Read")
			}
		})
	}
}

func TestObserveKernelBootReadFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name string
		data string
		err  error
		md   *common.Metadata
	}{
		{name: "denied", err: status.Error(codes.PermissionDenied, privateSentinel)},
		{name: "empty"},
		{name: "truncated", data: kernelBootID[:30]},
		{name: "missing-newline", data: kernelBootID},
		{name: "oversized", data: kernelBootID + "\n" + privateSentinel},
		{name: "malformed", data: strings.Repeat("x", 36) + "\n"},
		{name: "nil-uuid", data: "00000000-0000-0000-0000-000000000000\n"},
		{name: "legacy-error", data: kernelBootID + "\n", md: &common.Metadata{Error: privateSentinel}},
		{name: "status-error", data: kernelBootID + "\n", md: &common.Metadata{Status: &statuspb.Status{Code: int32(codes.PermissionDenied), Message: privateSentinel}}},
		{name: "error-after-full-data", data: kernelBootID + "\n", err: status.Error(codes.DataLoss, privateSentinel)},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newObserverFixture(t, "")
			require.NoError(t, f.resources.Destroy(t.Context(), runtimeresource.NewBootID().Metadata()))
			f.machine.read = func(request *machineapi.ReadRequest, stream grpc.ServerStreamingServer[common.Data]) error {
				assert.Equal(t, "/proc/sys/kernel/random/boot_id", request.GetPath())
				if err := stream.Send(&common.Data{Bytes: []byte(test.data), Metadata: test.md}); err != nil {
					return err
				}
				return test.err
			}
			result, err := talosclient.Observe(observerContext(t), f.client, f.endpoint)
			requireObserverFailure(t, result, err)
		})
	}
}

func TestObserveRejectsChangingBoot(t *testing.T) {
	for _, kernel := range []bool{false, true} {
		t.Run(fmt.Sprint(kernel), func(t *testing.T) {
			f := newObserverFixture(t, "")
			reads := 0
			if kernel {
				require.NoError(t, f.resources.Destroy(t.Context(), runtimeresource.NewBootID().Metadata()))
				f.machine.read = func(_ *machineapi.ReadRequest, stream grpc.ServerStreamingServer[common.Data]) error {
					reads++
					id := kernelBootID
					if reads == 2 {
						id = "dcaec73f-c61c-450e-b2c3-e0df875e8d10"
					}
					return stream.Send(&common.Data{Bytes: []byte(id + "\n")})
				}
			} else {
				f.cosi.get = func(ctx context.Context, request *cosiapi.GetRequest) (*cosiapi.GetResponse, error) {
					if request.GetType() == runtimeresource.BootIDType {
						reads++
						if reads == 2 {
							boot, err := f.resources.Get(ctx, runtimeresource.NewBootID().Metadata())
							if err != nil {
								return nil, err
							}
							boot.(*runtimeresource.BootID).TypedSpec().BootID = "changed"
							if err := f.resources.Update(ctx, boot); err != nil {
								return nil, err
							}
						}
					}
					return f.cosi.State.Get(ctx, request)
				}
			}
			result, err := talosclient.Observe(observerContext(t), f.client, f.endpoint)
			requireObserverFailure(t, result, err)
			assert.Equal(t, 2, reads)
		})
	}
}

func TestObserveStrictExtensionIdentities(t *testing.T) {
	for _, test := range []struct {
		name, extensionName, version, image string
		duplicate                           bool
	}{
		{name: "missing-name", version: "1", image: "extension.sqsh"},
		{name: "missing-version", extensionName: "tailscale", image: "extension.sqsh"},
		{name: "missing-image", extensionName: "tailscale", version: "1"},
		{name: "whitespace-image", extensionName: "tailscale", version: "1", image: "  "},
		{name: "duplicate-real", extensionName: "tailscale", version: "1", image: "extension.sqsh", duplicate: true},
		{name: "duplicate-virtual", extensionName: "schematic", version: observedSchematic, duplicate: true},
		{name: "malformed-virtual", extensionName: "schematic", version: "not-a-schematic"},
		{name: "empty-virtual", extensionName: "schematic"},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newObserverFixture(t, "")
			addObservedExtension(t, f, "one", test.extensionName, test.version, test.image)
			if test.duplicate {
				addObservedExtension(t, f, "two", test.extensionName, test.version, test.image)
			}
			result, err := talosclient.Observe(observerContext(t), f.client, f.endpoint)
			requireObserverFailure(t, result, err)
		})
	}
}

func TestObserveRejectsInconsistentSchematicAndMultipleKubelets(t *testing.T) {
	for _, kind := range []string{"schematic", "kubelet"} {
		t.Run(kind, func(t *testing.T) {
			f := newObserverFixture(t, "")
			if kind == "schematic" {
				schematic := runtimeresource.NewImageFactorySchematic(runtimeresource.NamespaceName, runtimeresource.ImageFactorySchematicID)
				schematic.TypedSpec().SchematicID = strings.Repeat("a", 64)
				require.NoError(t, f.resources.Create(t.Context(), schematic))
				addObservedExtension(t, f, "virtual", "schematic", observedSchematic, "")
			} else {
				kubelet := k8sresource.NewKubeletStatus(k8sresource.NamespaceName, "second")
				kubelet.TypedSpec().Image = "other.example/kubelet:v1.36.0"
				require.NoError(t, f.resources.Create(t.Context(), kubelet))
			}
			result, err := talosclient.Observe(observerContext(t), f.client, f.endpoint)
			requireObserverFailure(t, result, err)
		})
	}
}

func TestObserveRedactsMalformedActiveConfiguration(t *testing.T) {
	f := newObserverFixture(t, "")
	f.cosi.get = func(ctx context.Context, request *cosiapi.GetRequest) (*cosiapi.GetResponse, error) {
		response, err := f.cosi.State.Get(ctx, request)
		if err == nil && request.GetType() == configresource.MachineConfigType {
			response.Resource.Spec.ProtoSpec, err = proto.Marshal(&configapi.MachineConfigSpec{YamlMarshalled: []byte(privateSentinel + ": [invalid yaml")})
		}
		return response, err
	}
	result, err := talosclient.Observe(observerContext(t), f.client, f.endpoint)
	requireObserverFailure(t, result, err)
}

func TestObserveMissingKubeletResourceTypeIsUnknown(t *testing.T) {
	f := newObserverFixture(t, "")
	f.cosi.list = func(request *cosiapi.ListRequest, stream cosiapi.State_ListServer) error {
		if request.GetType() == k8sresource.KubeletStatusType {
			return status.Error(codes.NotFound, privateSentinel)
		}
		return f.cosi.State.List(request, stream)
	}
	result, err := talosclient.Observe(observerContext(t), f.client, f.endpoint)
	require.NoError(t, err)
	assert.Empty(t, result.KubeletImage)
}

func TestObserveActiveDocumentContainerWithoutLegacyMachineDocument(t *testing.T) {
	f := newObserverFixture(t, "")
	require.NoError(t, f.resources.Destroy(t.Context(), configresource.NewMachineConfig(nil).Metadata()))
	data := []byte("apiVersion: v1alpha1\nkind: RegistryAuthConfig\nname: registry.example\nusername: private-user\npassword: " + privateSentinel + "\n")
	provider, err := configloader.NewFromBytes(data)
	require.NoError(t, err)
	require.NoError(t, f.resources.Create(t.Context(), configresource.NewMachineConfig(provider)))
	result, err := talosclient.Observe(observerContext(t), f.client, f.endpoint)
	require.NoError(t, err)
	assert.Equal(t, data, result.Configuration)
}

func TestObserveSecondKernelBootReadMustBeComplete(t *testing.T) {
	for _, denied := range []bool{false, true} {
		t.Run(fmt.Sprint(denied), func(t *testing.T) {
			f := newObserverFixture(t, "")
			require.NoError(t, f.resources.Destroy(t.Context(), runtimeresource.NewBootID().Metadata()))
			reads := 0
			f.machine.read = func(_ *machineapi.ReadRequest, stream grpc.ServerStreamingServer[common.Data]) error {
				reads++
				if reads == 2 {
					if denied {
						return status.Error(codes.PermissionDenied, privateSentinel)
					}
					return stream.Send(&common.Data{})
				}
				return stream.Send(&common.Data{Bytes: []byte(kernelBootID + "\n")})
			}
			result, err := talosclient.Observe(observerContext(t), f.client, f.endpoint)
			requireObserverFailure(t, result, err)
			assert.Equal(t, 2, reads)
		})
	}
}
