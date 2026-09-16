// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package inplace

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/siderolabs/talos/pkg/machinery/api/common"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// fakeStream serves canned items then EOF (or err), standing in for a server-streaming client.
type fakeStream[T any] struct {
	grpc.ClientStream

	items []*T
	err   error
}

func (s *fakeStream[T]) Recv() (*T, error) {
	if len(s.items) == 0 {
		if s.err != nil {
			return nil, s.err
		}

		return nil, io.EOF
	}

	item := s.items[0]
	s.items = s.items[1:]

	return item, nil
}

type fakeImages struct {
	machineapi.ImageServiceClient

	pulled []string
	err    error
}

func (f *fakeImages) Pull(_ context.Context, in *machineapi.ImageServicePullRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[machineapi.ImageServicePullResponse], error) {
	f.pulled = append(f.pulled, in.ImageRef)

	if f.err != nil {
		return nil, f.err
	}

	return &fakeStream[machineapi.ImageServicePullResponse]{}, nil
}

type fakeLifecycle struct {
	machineapi.LifecycleServiceClient

	request  *machineapi.LifecycleServiceUpgradeRequest
	progress []*machineapi.LifecycleServiceUpgradeResponse
}

func (f *fakeLifecycle) Upgrade(_ context.Context, in *machineapi.LifecycleServiceUpgradeRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[machineapi.LifecycleServiceUpgradeResponse], error) {
	f.request = in

	return &fakeStream[machineapi.LifecycleServiceUpgradeResponse]{items: f.progress}, nil
}

type fakeMachine struct {
	machineapi.MachineServiceClient

	reboots []*machineapi.RebootRequest
}

func (f *fakeMachine) Reboot(_ context.Context, in *machineapi.RebootRequest, _ ...grpc.CallOption) (*machineapi.RebootResponse, error) {
	f.reboots = append(f.reboots, in)

	return &machineapi.RebootResponse{}, nil
}

func progressMessage(msg string) *machineapi.LifecycleServiceUpgradeResponse {
	return &machineapi.LifecycleServiceUpgradeResponse{Progress: &machineapi.LifecycleServiceInstallProgress{
		Response: &machineapi.LifecycleServiceInstallProgress_Message{Message: msg},
	}}
}

func progressExit(code int32) *machineapi.LifecycleServiceUpgradeResponse {
	return &machineapi.LifecycleServiceUpgradeResponse{Progress: &machineapi.LifecycleServiceInstallProgress{
		Response: &machineapi.LifecycleServiceInstallProgress_ExitCode{ExitCode: code},
	}}
}

const lifecycleImage = "factory.talos.dev/metal-installer/abc:v1.14.0"

func TestLifecycleUpgradePullsInstallsAndReboots(t *testing.T) {
	t.Parallel()

	images := &fakeImages{}
	lifecycle := &fakeLifecycle{progress: []*machineapi.LifecycleServiceUpgradeResponse{progressMessage("unpacking"), progressExit(0)}}
	machine := &fakeMachine{}

	require.NoError(t, lifecycleUpgrade(context.Background(), upgradeClients{images: images, lifecycle: lifecycle, machine: machine}, lifecycleImage))

	assert.Equal(t, []string{lifecycleImage}, images.pulled)
	require.NotNil(t, lifecycle.request)
	assert.Equal(t, lifecycleImage, lifecycle.request.Source.ImageName)
	assert.Equal(t, common.ContainerDriver_CONTAINERD, lifecycle.request.Containerd.Driver)
	assert.Equal(t, common.ContainerdNamespace_NS_SYSTEM, lifecycle.request.Containerd.Namespace)
	require.Len(t, machine.reboots, 1)
	assert.Equal(t, machineapi.RebootRequest_DEFAULT, machine.reboots[0].Mode)
}

func TestLifecycleUpgradeFailsOnANonZeroExitCode(t *testing.T) {
	t.Parallel()

	lifecycle := &fakeLifecycle{progress: []*machineapi.LifecycleServiceUpgradeResponse{progressMessage("verifying image"), progressExit(2)}}
	machine := &fakeMachine{}

	err := lifecycleUpgrade(context.Background(), upgradeClients{images: &fakeImages{}, lifecycle: lifecycle, machine: machine}, lifecycleImage)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "exited with code 2")
	assert.Contains(t, err.Error(), "verifying image", "the last progress message tells the operator what failed")
	assert.Empty(t, machine.reboots, "a failed install must not reboot")
}

func TestLifecycleUpgradeFailsWhenTheStreamEndsWithoutAnExitCode(t *testing.T) {
	t.Parallel()

	lifecycle := &fakeLifecycle{progress: []*machineapi.LifecycleServiceUpgradeResponse{progressMessage("unpacking")}}
	machine := &fakeMachine{}

	err := lifecycleUpgrade(context.Background(), upgradeClients{images: &fakeImages{}, lifecycle: lifecycle, machine: machine}, lifecycleImage)

	require.Error(t, err)
	assert.Empty(t, machine.reboots)
}

func TestLifecycleUpgradeStopsWhenThePullFails(t *testing.T) {
	t.Parallel()

	lifecycle := &fakeLifecycle{}

	err := lifecycleUpgrade(context.Background(), upgradeClients{images: &fakeImages{err: errors.New("registry unreachable")}, lifecycle: lifecycle, machine: &fakeMachine{}}, lifecycleImage)

	require.ErrorContains(t, err, "registry unreachable")
	assert.Nil(t, lifecycle.request, "no install without the image")
}

func TestNodeClientUpgradeRunsTheLifecycleSequence(t *testing.T) {
	t.Parallel()

	machine := &fakeMachine{}
	node := &talosNodeClient{upgrade: upgradeClients{
		images:    &fakeImages{},
		lifecycle: &fakeLifecycle{progress: []*machineapi.LifecycleServiceUpgradeResponse{progressExit(0)}},
		machine:   machine,
	}}

	require.NoError(t, node.Upgrade(context.Background(), lifecycleImage))
	assert.Len(t, machine.reboots, 1)
}
