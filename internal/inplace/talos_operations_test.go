// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package inplace

import (
	"context"
	"errors"
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
	cosiclient "github.com/cosi-project/runtime/pkg/state/protobuf/client"
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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
)

const operationPrivateSentinel = "operation-private-config-never-public"
const operationFinalName = "registry.example/installer@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const operationImageDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const operationConfig = `version: v1alpha1
machine:
  type: worker
---
apiVersion: v1alpha1
kind: RegistryAuthConfig
name: registry.example
username: private-user
password: operation-private-config-never-public
---
apiVersion: v1alpha1
kind: KubeletConfig
image: registry.example/kubelet:v1.36.0
`

type operationRPCRecord struct {
	method  string
	routing metadata.MD
}

type operationRPCRecorder struct {
	mu    sync.Mutex
	calls []operationRPCRecord
}

func (r *operationRPCRecorder) record(ctx context.Context, method string) {
	md, _ := metadata.FromIncomingContext(ctx)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, operationRPCRecord{method: method, routing: md.Copy()})
}

func (r *operationRPCRecorder) snapshot() []operationRPCRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]operationRPCRecord(nil), r.calls...)
}

type operationMachineServer struct {
	machineapi.UnimplementedMachineServiceServer
	applyResponse  *machineapi.ApplyConfigurationResponse
	applyError     error
	rebootResponse *machineapi.RebootResponse
	rebootError    error
	mu             sync.Mutex
	applied        []*machineapi.ApplyConfigurationRequest
	rebooted       []*machineapi.RebootRequest
}

func (s *operationMachineServer) ApplyConfiguration(_ context.Context, request *machineapi.ApplyConfigurationRequest) (*machineapi.ApplyConfigurationResponse, error) {
	s.mu.Lock()
	s.applied = append(s.applied, proto.Clone(request).(*machineapi.ApplyConfigurationRequest))
	s.mu.Unlock()
	return s.applyResponse, s.applyError
}

func (s *operationMachineServer) Reboot(_ context.Context, request *machineapi.RebootRequest) (*machineapi.RebootResponse, error) {
	s.mu.Lock()
	s.rebooted = append(s.rebooted, proto.Clone(request).(*machineapi.RebootRequest))
	s.mu.Unlock()
	return s.rebootResponse, s.rebootError
}

type operationImageServer struct {
	machineapi.UnimplementedImageServiceServer
	pullResponses []*machineapi.ImageServicePullResponse
	pullError     error
	listResponses []*machineapi.ImageServiceListResponse
	listError     error
	mu            sync.Mutex
	pulled        []*machineapi.ImageServicePullRequest
	listed        []*machineapi.ImageServiceListRequest
}

func (s *operationImageServer) Pull(request *machineapi.ImageServicePullRequest, stream grpc.ServerStreamingServer[machineapi.ImageServicePullResponse]) error {
	s.mu.Lock()
	s.pulled = append(s.pulled, proto.Clone(request).(*machineapi.ImageServicePullRequest))
	s.mu.Unlock()
	for _, response := range s.pullResponses {
		if err := stream.Send(response); err != nil {
			return err
		}
	}
	return s.pullError
}

func (s *operationImageServer) List(request *machineapi.ImageServiceListRequest, stream grpc.ServerStreamingServer[machineapi.ImageServiceListResponse]) error {
	s.mu.Lock()
	s.listed = append(s.listed, proto.Clone(request).(*machineapi.ImageServiceListRequest))
	s.mu.Unlock()
	for _, response := range s.listResponses {
		if err := stream.Send(response); err != nil {
			return err
		}
	}
	return s.listError
}

type operationLifecycleServer struct {
	machineapi.UnimplementedLifecycleServiceServer
	responses []*machineapi.LifecycleServiceUpgradeResponse
	err       error
	mu        sync.Mutex
	installed []*machineapi.LifecycleServiceUpgradeRequest
}

func (s *operationLifecycleServer) Upgrade(request *machineapi.LifecycleServiceUpgradeRequest, stream grpc.ServerStreamingServer[machineapi.LifecycleServiceUpgradeResponse]) error {
	s.mu.Lock()
	s.installed = append(s.installed, proto.Clone(request).(*machineapi.LifecycleServiceUpgradeRequest))
	s.mu.Unlock()
	for _, response := range s.responses {
		if err := stream.Send(response); err != nil {
			return err
		}
	}
	return s.err
}

type operationCOSIServer struct {
	*cosiserver.State
	get  func(context.Context, *cosiapi.GetRequest) (*cosiapi.GetResponse, error)
	list func(*cosiapi.ListRequest, cosiapi.State_ListServer) error
}

func (s *operationCOSIServer) Get(ctx context.Context, request *cosiapi.GetRequest) (*cosiapi.GetResponse, error) {
	if s.get != nil {
		return s.get(ctx, request)
	}
	return s.State.Get(ctx, request)
}

func (s *operationCOSIServer) List(request *cosiapi.ListRequest, stream cosiapi.State_ListServer) error {
	if s.list != nil {
		return s.list(request, stream)
	}
	return s.State.List(request, stream)
}

type operationRPCFixture struct {
	node      *talosNodeClient
	machine   *operationMachineServer
	images    *operationImageServer
	lifecycle *operationLifecycleServer
	cosi      *operationCOSIServer
	resources state.State
	recorder  *operationRPCRecorder
}

// All production methods use real generated gRPC clients, including COSI's
// protobuf adapter over a real namespaced in-memory state. Only server responses
// are controlled; no fake production client or operation method is substituted.
func newOperationRPCFixture(t *testing.T, endpoint bootstrapv1.TalosEndpoint) *operationRPCFixture {
	t.Helper()
	fixture := &operationRPCFixture{
		machine: &operationMachineServer{
			applyResponse:  &machineapi.ApplyConfigurationResponse{Messages: []*machineapi.ApplyConfiguration{{Mode: machineapi.ApplyConfigurationRequest_NO_REBOOT}}},
			rebootResponse: &machineapi.RebootResponse{Messages: []*machineapi.Reboot{{ActorId: "reboot-actor"}}},
		},
		images: &operationImageServer{
			pullResponses: []*machineapi.ImageServicePullResponse{{Response: &machineapi.ImageServicePullResponse_Name{Name: operationFinalName}}},
			listResponses: []*machineapi.ImageServiceListResponse{{Name: operationFinalName, Digest: operationImageDigest}},
		},
		lifecycle: &operationLifecycleServer{responses: []*machineapi.LifecycleServiceUpgradeResponse{progressExit(0)}},
		resources: state.WrapCore(namespaced.NewState(inmem.Build)),
		recorder:  &operationRPCRecorder{},
	}
	fixture.cosi = &operationCOSIServer{State: cosiserver.NewState(fixture.resources)}
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(
		grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			fixture.recorder.record(ctx, info.FullMethod)
			return handler(ctx, request)
		}),
		grpc.StreamInterceptor(func(service any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			fixture.recorder.record(stream.Context(), info.FullMethod)
			return handler(service, stream)
		}),
	)
	machineapi.RegisterMachineServiceServer(server, fixture.machine)
	machineapi.RegisterImageServiceServer(server, fixture.images)
	machineapi.RegisterLifecycleServiceServer(server, fixture.lifecycle)
	cosiapi.RegisterStateServer(server, fixture.cosi)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		<-serveDone
	})
	connection, err := grpc.NewClient("passthrough:///operation-in-memory",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = connection.Close() })
	fixture.node = &talosNodeClient{
		client: &machineryclient.Client{
			MachineClient:   machineapi.NewMachineServiceClient(connection),
			ImageClient:     machineapi.NewImageServiceClient(connection),
			LifecycleClient: machineapi.NewLifecycleServiceClient(connection),
			COSI:            state.WrapCore(cosiclient.NewAdapter(cosiapi.NewStateClient(connection))),
		},
		endpoint: endpoint,
	}
	return fixture
}

func populateOperationResources(t *testing.T, fixture *operationRPCFixture, includeSchematic bool) {
	t.Helper()
	provider, err := configloader.NewFromBytes([]byte(operationConfig))
	require.NoError(t, err)
	boot := runtimeresource.NewBootID()
	boot.TypedSpec().BootID = "actual-boot-id"
	version := runtimeresource.NewVersion()
	version.TypedSpec().Version = "v1.14.0"
	kubelet := k8sresource.NewKubeletStatus(k8sresource.NamespaceName, "actual-kubelet-status-id")
	kubelet.TypedSpec().Image = "registry.example/kubelet:v1.36.0"
	resources := []resource.Resource{boot, configresource.NewMachineConfig(provider), version, kubelet}
	if includeSchematic {
		schematic := runtimeresource.NewImageFactorySchematic(runtimeresource.NamespaceName, runtimeresource.ImageFactorySchematicID)
		schematic.TypedSpec().SchematicID = "actual-schematic"
		resources = append(resources, schematic)
	}
	for _, item := range resources {
		require.NoError(t, fixture.resources.Create(t.Context(), item))
	}
}

func operationContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return metadata.NewOutgoingContext(ctx, metadata.Pairs("node", "stale-node", "nodes", "stale-other-node", "trace", "kept"))
}

func requireOperationRedacted(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	assert.NotContains(t, err.Error(), operationPrivateSentinel)
	assert.Nil(t, errors.Unwrap(err), "private upstream errors must not be retained as causes")
}

func TestTalosOperationObserveActiveCompleteConfiguration(t *testing.T) {
	fixture := newOperationRPCFixture(t, bootstrapv1.TalosEndpoint{Node: "selected-node"})
	populateOperationResources(t, fixture, true)
	staged, err := configloader.NewFromBytes([]byte("version: v1alpha1\nmachine:\n  type: worker\n"))
	require.NoError(t, err)
	require.NoError(t, fixture.resources.Create(t.Context(), configresource.NewMachineConfigWithID(staged, configresource.PersistentID)))
	for _, item := range []struct{ id, name, version, image string }{
		{"extension-z", "zeta", "2.0", "zeta-layer.sqsh"},
		{"extension-a", "alpha", "1.0", "alpha-layer.sqsh"},
	} {
		extension := runtimeresource.NewExtensionStatus(runtimeresource.NamespaceName, item.id)
		extension.TypedSpec().Metadata.Name = item.name
		extension.TypedSpec().Metadata.Version = item.version
		extension.TypedSpec().Image = item.image
		extension.TypedSpec().Metadata.Description = operationPrivateSentinel
		require.NoError(t, fixture.resources.Create(t.Context(), extension))
	}
	observation, err := fixture.node.Observe(operationContext(t))
	require.NoError(t, err)
	assert.Equal(t, []byte(operationConfig), observation.Configuration, "must retain every active document and private field, not persistent config")
	assert.Equal(t, "actual-boot-id", observation.BootID)
	assert.Equal(t, "v1.14.0", observation.OSVersion)
	assert.Equal(t, "actual-schematic", observation.SchematicID)
	assert.Equal(t, "registry.example/kubelet:v1.36.0", observation.KubeletImage)
	assert.Equal(t, []ExtensionObservation{
		{Name: "alpha", Version: "1.0", Image: "alpha-layer.sqsh"},
		{Name: "zeta", Version: "2.0", Image: "zeta-layer.sqsh"},
	}, observation.Extensions)
}

func TestTalosOperationObserveMissingOptionalSchematic(t *testing.T) {
	fixture := newOperationRPCFixture(t, bootstrapv1.TalosEndpoint{})
	populateOperationResources(t, fixture, false)
	observation, err := fixture.node.Observe(operationContext(t))
	require.NoError(t, err)
	assert.Empty(t, observation.SchematicID)
	assert.Empty(t, observation.Extensions)
}

func TestTalosOperationObserveFailsClosedOnRequiredResource(t *testing.T) {
	for _, test := range []struct {
		name    string
		pointer resource.Pointer
	}{
		{"active-config", resource.NewMetadata(configresource.NamespaceName, configresource.MachineConfigType, configresource.ActiveID, resource.VersionUndefined)},
		{"boot-id", runtimeresource.NewBootID().Metadata()},
		{"version", runtimeresource.NewVersion().Metadata()},
		{"kubelet", k8sresource.NewKubeletStatus(k8sresource.NamespaceName, "actual-kubelet-status-id").Metadata()},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newOperationRPCFixture(t, bootstrapv1.TalosEndpoint{})
			populateOperationResources(t, fixture, false)
			require.NoError(t, fixture.resources.Destroy(t.Context(), test.pointer))
			observation, err := fixture.node.Observe(operationContext(t))
			require.Error(t, err)
			assert.Equal(t, NodeObservation{}, observation, "must not expose partially proved identities")
		})
	}
}

func TestTalosOperationObserveFailsClosedOnEmptyWitnesses(t *testing.T) {
	for _, test := range []struct {
		name  string
		empty resource.Resource
	}{
		{"boot-id", runtimeresource.NewBootID()},
		{"version", runtimeresource.NewVersion()},
		{"kubelet", k8sresource.NewKubeletStatus(k8sresource.NamespaceName, "actual-kubelet-status-id")},
		{"extension", runtimeresource.NewExtensionStatus(runtimeresource.NamespaceName, "incomplete-extension")},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newOperationRPCFixture(t, bootstrapv1.TalosEndpoint{})
			populateOperationResources(t, fixture, false)
			if test.name != "extension" {
				require.NoError(t, fixture.resources.Destroy(t.Context(), test.empty.Metadata()))
			}
			require.NoError(t, fixture.resources.Create(t.Context(), test.empty))
			observation, err := fixture.node.Observe(operationContext(t))
			require.Error(t, err)
			assert.Equal(t, NodeObservation{}, observation)
		})
	}
}

func TestTalosOperationObserveRedactsCOSIAndParserErrors(t *testing.T) {
	for _, stage := range []string{"schematic-permission", "malformed-config"} {
		t.Run(stage, func(t *testing.T) {
			fixture := newOperationRPCFixture(t, bootstrapv1.TalosEndpoint{})
			populateOperationResources(t, fixture, true)
			fixture.cosi.get = func(ctx context.Context, request *cosiapi.GetRequest) (*cosiapi.GetResponse, error) {
				if stage == "schematic-permission" && request.GetType() == runtimeresource.ImageFactorySchematicType {
					return nil, status.Error(codes.PermissionDenied, operationPrivateSentinel)
				}
				response, err := fixture.cosi.State.Get(ctx, request)
				if err == nil && stage == "malformed-config" && request.GetType() == configresource.MachineConfigType {
					response.Resource.Spec.ProtoSpec, err = proto.Marshal(&configapi.MachineConfigSpec{YamlMarshalled: []byte(operationPrivateSentinel + ": [invalid yaml")})
				}
				return response, err
			}
			observation, err := fixture.node.Observe(operationContext(t))
			requireOperationRedacted(t, err)
			assert.Equal(t, NodeObservation{}, observation)
		})
	}
}

func TestTalosOperationObserveRejectsBootChange(t *testing.T) {
	fixture := newOperationRPCFixture(t, bootstrapv1.TalosEndpoint{})
	populateOperationResources(t, fixture, false)
	bootReads := 0
	fixture.cosi.get = func(ctx context.Context, request *cosiapi.GetRequest) (*cosiapi.GetResponse, error) {
		if request.GetType() == runtimeresource.BootIDType {
			bootReads++
			if bootReads == 2 {
				boot, err := fixture.resources.Get(ctx, runtimeresource.NewBootID().Metadata())
				if err != nil {
					return nil, err
				}
				boot.(*runtimeresource.BootID).TypedSpec().BootID = "different-boot-id"
				if err = fixture.resources.Update(ctx, boot); err != nil {
					return nil, err
				}
			}
		}
		return fixture.cosi.State.Get(ctx, request)
	}
	observation, err := fixture.node.Observe(operationContext(t))
	require.ErrorContains(t, err, "changed during observation")
	assert.Equal(t, NodeObservation{}, observation)
}

func TestTalosOperationApplyChosenModeAndSafeMetadata(t *testing.T) {
	for _, test := range []struct {
		name     string
		messages []*machineapi.ApplyConfiguration
		rpcError error
		outcome  updateoperation.ApplyOutcome
	}{
		{"no-reboot", []*machineapi.ApplyConfiguration{{Mode: machineapi.ApplyConfigurationRequest_NO_REBOOT, Warnings: []string{operationPrivateSentinel}, ModeDetails: operationPrivateSentinel}}, nil, updateoperation.NoReboot},
		{"reboot", []*machineapi.ApplyConfiguration{{Mode: machineapi.ApplyConfigurationRequest_REBOOT}}, nil, updateoperation.RebootRequired},
		{"auto", []*machineapi.ApplyConfiguration{{Mode: machineapi.ApplyConfigurationRequest_AUTO}}, nil, updateoperation.Unknown},
		{"staged", []*machineapi.ApplyConfiguration{{Mode: machineapi.ApplyConfigurationRequest_STAGED}}, nil, updateoperation.Unknown},
		{"try", []*machineapi.ApplyConfiguration{{Mode: machineapi.ApplyConfigurationRequest_TRY}}, nil, updateoperation.Unknown},
		{"unrecognized", []*machineapi.ApplyConfiguration{{Mode: machineapi.ApplyConfigurationRequest_Mode(99)}}, nil, updateoperation.Unknown},
		{"empty", nil, nil, updateoperation.Unknown},
		{"multiple", []*machineapi.ApplyConfiguration{{Mode: machineapi.ApplyConfigurationRequest_NO_REBOOT}, {Mode: machineapi.ApplyConfigurationRequest_NO_REBOOT}}, nil, updateoperation.Unknown},
		{"metadata-error", []*machineapi.ApplyConfiguration{{Mode: machineapi.ApplyConfigurationRequest_NO_REBOOT, Metadata: &common.Metadata{Error: operationPrivateSentinel}}}, nil, updateoperation.Unknown},
		{"status-only-error", []*machineapi.ApplyConfiguration{{Mode: machineapi.ApplyConfigurationRequest_NO_REBOOT, Metadata: &common.Metadata{Status: status.New(codes.InvalidArgument, operationPrivateSentinel).Proto()}}}, nil, updateoperation.Unknown},
		{"transport-error", nil, status.Error(codes.Unavailable, operationPrivateSentinel), updateoperation.Unknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newOperationRPCFixture(t, bootstrapv1.TalosEndpoint{})
			fixture.machine.applyResponse = &machineapi.ApplyConfigurationResponse{Messages: test.messages}
			fixture.machine.applyError = test.rpcError
			outcome, err := fixture.node.Apply(operationContext(t), []byte(operationConfig))
			assert.Equal(t, test.outcome, outcome)
			if test.outcome == updateoperation.Unknown {
				requireOperationRedacted(t, err)
			} else {
				require.NoError(t, err)
			}
			fixture.machine.mu.Lock()
			defer fixture.machine.mu.Unlock()
			require.Len(t, fixture.machine.applied, 1)
			assert.Equal(t, machineapi.ApplyConfigurationRequest_AUTO, fixture.machine.applied[0].Mode)
			assert.Equal(t, []byte(operationConfig), fixture.machine.applied[0].Data)
			assert.Empty(t, fixture.machine.rebooted)
		})
	}
}

func TestTalosOperationPullUsesFinalNameAndExactListedDigest(t *testing.T) {
	fixture := newOperationRPCFixture(t, bootstrapv1.TalosEndpoint{})
	fixture.images.pullResponses = append([]*machineapi.ImageServicePullResponse{{Response: &machineapi.ImageServicePullResponse_PullProgress{PullProgress: &machineapi.ImageServicePullProgress{}}}}, fixture.images.pullResponses...)
	fixture.images.listResponses = append([]*machineapi.ImageServiceListResponse{{Name: "registry.example/installer:requested-tag", Digest: "not-the-pulled-digest"}}, fixture.images.listResponses...)
	name, digest, err := fixture.node.PullInstaller(operationContext(t), "registry.example/installer:requested-tag")
	require.NoError(t, err)
	assert.Equal(t, operationFinalName, name)
	assert.Equal(t, operationImageDigest, digest)
	fixture.images.mu.Lock()
	defer fixture.images.mu.Unlock()
	require.Len(t, fixture.images.pulled, 1)
	require.Len(t, fixture.images.listed, 1)
	assert.Equal(t, "registry.example/installer:requested-tag", fixture.images.pulled[0].ImageRef)
	assert.Equal(t, common.ContainerdNamespace_NS_SYSTEM, fixture.images.pulled[0].Containerd.Namespace)
	assert.Equal(t, common.ContainerDriver_CONTAINERD, fixture.images.listed[0].Containerd.Driver)
	assert.Equal(t, common.ContainerdNamespace_NS_SYSTEM, fixture.images.listed[0].Containerd.Namespace)
}

func TestTalosOperationPullFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name      string
		configure func(*operationImageServer)
	}{
		{"no-final-name", func(s *operationImageServer) { s.pullResponses = nil }},
		{"empty-final-name", func(s *operationImageServer) {
			s.pullResponses = []*machineapi.ImageServicePullResponse{{Response: &machineapi.ImageServicePullResponse_Name{}}}
		}},
		{"conflicting-final-name", func(s *operationImageServer) {
			s.pullResponses = append(s.pullResponses, &machineapi.ImageServicePullResponse{Response: &machineapi.ImageServicePullResponse_Name{Name: "different"}})
		}},
		{"pull-stream-error-after-name", func(s *operationImageServer) { s.pullError = status.Error(codes.Unavailable, operationPrivateSentinel) }},
		{"missing-exact-name", func(s *operationImageServer) { s.listResponses[0].Name = "different" }},
		{"missing-digest", func(s *operationImageServer) { s.listResponses[0].Digest = "" }},
		{"duplicate-name", func(s *operationImageServer) {
			s.listResponses = append(s.listResponses, proto.Clone(s.listResponses[0]).(*machineapi.ImageServiceListResponse))
		}},
		{"list-stream-error-after-match", func(s *operationImageServer) { s.listError = status.Error(codes.Unavailable, operationPrivateSentinel) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newOperationRPCFixture(t, bootstrapv1.TalosEndpoint{})
			test.configure(fixture.images)
			name, digest, err := fixture.node.PullInstaller(operationContext(t), "registry.example/installer:requested-tag")
			requireOperationRedacted(t, err)
			assert.Empty(t, name)
			assert.Empty(t, digest)
		})
	}
}

func TestTalosOperationInstallRequiresExplicitExitAndNeverReboots(t *testing.T) {
	for _, test := range []struct {
		name        string
		responses   []*machineapi.LifecycleServiceUpgradeResponse
		streamError error
		code        int32
		succeeds    bool
	}{
		{"explicit-zero", []*machineapi.LifecycleServiceUpgradeResponse{progressMessage(operationPrivateSentinel), progressExit(0)}, nil, 0, true},
		{"explicit-nonzero", []*machineapi.LifecycleServiceUpgradeResponse{progressMessage(operationPrivateSentinel), progressExit(7)}, nil, 7, false},
		{"eof", nil, nil, -1, false},
		{"progress-then-eof", []*machineapi.LifecycleServiceUpgradeResponse{progressMessage(operationPrivateSentinel)}, nil, -1, false},
		{"empty-progress", []*machineapi.LifecycleServiceUpgradeResponse{{}}, nil, -1, false},
		{"stream-error", []*machineapi.LifecycleServiceUpgradeResponse{progressMessage(operationPrivateSentinel)}, status.Error(codes.Unavailable, operationPrivateSentinel), -1, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newOperationRPCFixture(t, bootstrapv1.TalosEndpoint{})
			fixture.lifecycle.responses, fixture.lifecycle.err = test.responses, test.streamError
			code, err := fixture.node.Install(operationContext(t), operationFinalName)
			assert.Equal(t, test.code, code)
			if test.succeeds {
				require.NoError(t, err)
			} else {
				requireOperationRedacted(t, err)
			}
			fixture.lifecycle.mu.Lock()
			defer fixture.lifecycle.mu.Unlock()
			require.Len(t, fixture.lifecycle.installed, 1)
			assert.Equal(t, operationFinalName, fixture.lifecycle.installed[0].Source.ImageName)
			assert.Equal(t, common.ContainerdNamespace_NS_SYSTEM, fixture.lifecycle.installed[0].Containerd.Namespace)
			for _, call := range fixture.recorder.snapshot() {
				assert.Equal(t, machineapi.LifecycleService_Upgrade_FullMethodName, call.method)
			}
		})
	}
}

func TestTalosOperationRebootAcknowledgesOnlySingleSuccessfulResponse(t *testing.T) {
	for _, test := range []struct {
		name     string
		messages []*machineapi.Reboot
		rpcError error
		succeeds bool
	}{
		{"acknowledged", []*machineapi.Reboot{{ActorId: "actual-actor"}}, nil, true},
		{"empty", nil, nil, false},
		{"multiple", []*machineapi.Reboot{{}, {}}, nil, false},
		{"metadata-error", []*machineapi.Reboot{{Metadata: &common.Metadata{Error: operationPrivateSentinel}}}, nil, false},
		{"status-only-error", []*machineapi.Reboot{{Metadata: &common.Metadata{Status: status.New(codes.FailedPrecondition, operationPrivateSentinel).Proto()}}}, nil, false},
		{"transport-error", nil, status.Error(codes.Unavailable, operationPrivateSentinel), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newOperationRPCFixture(t, bootstrapv1.TalosEndpoint{})
			fixture.machine.rebootResponse = &machineapi.RebootResponse{Messages: test.messages}
			fixture.machine.rebootError = test.rpcError
			err := fixture.node.Reboot(operationContext(t))
			if test.succeeds {
				require.NoError(t, err)
			} else {
				requireOperationRedacted(t, err)
			}
			fixture.machine.mu.Lock()
			defer fixture.machine.mu.Unlock()
			require.Len(t, fixture.machine.rebooted, 1)
			assert.Equal(t, machineapi.RebootRequest_DEFAULT, fixture.machine.rebooted[0].Mode)
			assert.Empty(t, fixture.machine.applied)
		})
	}
}

func TestTalosOperationRoutesEveryRPCAndCOSIReadToExactlySelectedNode(t *testing.T) {
	for _, selected := range []string{"", "selected-node"} {
		t.Run("node="+selected, func(t *testing.T) {
			fixture := newOperationRPCFixture(t, bootstrapv1.TalosEndpoint{Node: selected})
			populateOperationResources(t, fixture, false)
			ctx := operationContext(t)
			_, err := fixture.node.Observe(ctx)
			require.NoError(t, err)
			_, err = fixture.node.Apply(ctx, []byte(operationConfig))
			require.NoError(t, err)
			name, _, err := fixture.node.PullInstaller(ctx, "registry.example/installer:requested-tag")
			require.NoError(t, err)
			_, err = fixture.node.Install(ctx, name)
			require.NoError(t, err)
			require.NoError(t, fixture.node.Reboot(ctx))
			calls := fixture.recorder.snapshot()
			require.GreaterOrEqual(t, len(calls), 12)
			for _, call := range calls {
				assert.Empty(t, call.routing.Get("nodes"), call.method)
				if selected == "" {
					assert.Empty(t, call.routing.Get("node"), call.method)
				} else {
					assert.Equal(t, []string{selected}, call.routing.Get("node"), call.method)
				}
				assert.Equal(t, []string{"kept"}, call.routing.Get("trace"), call.method)
				assert.True(t, strings.HasPrefix(call.method, "/machine.") || strings.HasPrefix(call.method, "/cosi."), call.method)
			}
		})
	}
}

func TestTalosOperationObserveWithoutLegacyMachineDocument(t *testing.T) {
	fixture := newOperationRPCFixture(t, bootstrapv1.TalosEndpoint{})
	populateOperationResources(t, fixture, false)
	require.NoError(t, fixture.resources.Destroy(t.Context(), configresource.NewMachineConfig(nil).Metadata()))
	data := []byte("apiVersion: v1alpha1\nkind: KubeletConfig\nimage: registry.example/kubelet:v1.36.0\n")
	provider, err := configloader.NewFromBytes(data)
	require.NoError(t, err)
	require.NoError(t, fixture.resources.Create(t.Context(), configresource.NewMachineConfig(provider)))
	observation, err := fixture.node.Observe(operationContext(t))
	require.NoError(t, err)
	assert.Equal(t, data, observation.Configuration)
}

func TestTalosOperationObserveRejectsMultipleKubeletStatuses(t *testing.T) {
	fixture := newOperationRPCFixture(t, bootstrapv1.TalosEndpoint{})
	populateOperationResources(t, fixture, false)
	kubelet := k8sresource.NewKubeletStatus(k8sresource.NamespaceName, "second-kubelet-status")
	kubelet.TypedSpec().Image = "registry.example/kubelet:v1.36.0"
	require.NoError(t, fixture.resources.Create(t.Context(), kubelet))
	observation, err := fixture.node.Observe(operationContext(t))
	require.Error(t, err)
	assert.Equal(t, NodeObservation{}, observation)
}

func TestTalosOperationObserveRedactsListErrors(t *testing.T) {
	for _, failedType := range []resource.Type{runtimeresource.ExtensionStatusType, k8sresource.KubeletStatusType} {
		t.Run(failedType, func(t *testing.T) {
			fixture := newOperationRPCFixture(t, bootstrapv1.TalosEndpoint{})
			populateOperationResources(t, fixture, false)
			fixture.cosi.list = func(request *cosiapi.ListRequest, stream cosiapi.State_ListServer) error {
				if request.GetType() == failedType {
					return status.Error(codes.PermissionDenied, operationPrivateSentinel)
				}
				return fixture.cosi.State.List(request, stream)
			}
			observation, err := fixture.node.Observe(operationContext(t))
			requireOperationRedacted(t, err)
			assert.Equal(t, NodeObservation{}, observation)
		})
	}
}

func TestTalosOperationCanceledContextNeverAcknowledgesSuccess(t *testing.T) {
	fixture := newOperationRPCFixture(t, bootstrapv1.TalosEndpoint{Node: "selected-node"})
	populateOperationResources(t, fixture, false)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	observation, err := fixture.node.Observe(ctx)
	require.Error(t, err)
	assert.Equal(t, NodeObservation{}, observation)
	outcome, err := fixture.node.Apply(ctx, []byte(operationConfig))
	require.Error(t, err)
	assert.Equal(t, updateoperation.Unknown, outcome)
	name, digest, err := fixture.node.PullInstaller(ctx, operationFinalName)
	require.Error(t, err)
	assert.Empty(t, name)
	assert.Empty(t, digest)
	code, err := fixture.node.Install(ctx, operationFinalName)
	require.Error(t, err)
	assert.Equal(t, int32(-1), code)
	require.Error(t, fixture.node.Reboot(ctx))
}
