// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package inplace

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/cosi-project/runtime/pkg/resource"
	"github.com/cosi-project/runtime/pkg/safe"
	"github.com/cosi-project/runtime/pkg/state"
	"github.com/siderolabs/talos/pkg/machinery/api/common"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	configresource "github.com/siderolabs/talos/pkg/machinery/resources/config"
	k8sresource "github.com/siderolabs/talos/pkg/machinery/resources/k8s"
	runtimeresource "github.com/siderolabs/talos/pkg/machinery/resources/runtime"

	endpointclient "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/talosclient"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
)

var _ OperationNode = (*talosNodeClient)(nil)

// Observe reads the active document container, not the possibly staged persistent
// config. All identities are runtime observations; Talos exposes no booted
// installer OCI digest, and a kubelet image is not workload Node readiness.
func (t *talosNodeClient) Observe(ctx context.Context) (NodeObservation, error) {
	ctx = endpointclient.WithNode(ctx, t.endpoint)
	var observation NodeObservation

	boot, err := safe.StateGetByID[*runtimeresource.BootID](ctx, t.client.COSI, runtimeresource.BootIDID)
	if err != nil || boot == nil || strings.TrimSpace(boot.TypedSpec().BootID) == "" {
		return NodeObservation{}, errors.New("Talos boot identity observation failed")
	}
	observation.BootID = boot.TypedSpec().BootID

	active, err := safe.StateGetByID[*configresource.MachineConfig](ctx, t.client.COSI, configresource.ActiveID)
	if err != nil || active == nil || active.Provider() == nil {
		return NodeObservation{}, errors.New("Talos active configuration observation failed")
	}
	observation.Configuration, err = active.Provider().Bytes()
	if err != nil || len(observation.Configuration) == 0 {
		return NodeObservation{}, errors.New("Talos active configuration serialization failed")
	}

	version, err := safe.StateGetByID[*runtimeresource.Version](ctx, t.client.COSI, "version")
	if err != nil || version == nil || strings.TrimSpace(version.TypedSpec().Version) == "" {
		return NodeObservation{}, errors.New("Talos runtime version observation failed")
	}
	observation.OSVersion = version.TypedSpec().Version

	schematic, err := safe.StateGetByID[*runtimeresource.ImageFactorySchematic](ctx, t.client.COSI, runtimeresource.ImageFactorySchematicID)
	switch {
	case state.IsNotFoundError(err):
		// Non-Image-Factory installations legitimately have no schematic resource.
	case err != nil || schematic == nil:
		return NodeObservation{}, errors.New("Talos schematic observation failed")
	default:
		observation.SchematicID = schematic.TypedSpec().SchematicID
	}

	extensionKind := resource.NewMetadata(runtimeresource.NamespaceName, runtimeresource.ExtensionStatusType, "", resource.VersionUndefined)
	extensions, err := t.client.COSI.List(ctx, extensionKind)
	if err != nil {
		return NodeObservation{}, errors.New("Talos extension observation failed")
	}
	observation.Extensions = make([]ExtensionObservation, 0, len(extensions.Items))
	for _, item := range extensions.Items {
		extension, ok := item.(*runtimeresource.ExtensionStatus)
		if !ok || extension == nil {
			return NodeObservation{}, errors.New("Talos extension observation failed")
		}
		layer := extension.TypedSpec()
		if layer.Metadata.Name == "" || layer.Metadata.Version == "" || layer.Image == "" {
			return NodeObservation{}, errors.New("Talos extension identity observation failed")
		}
		observation.Extensions = append(observation.Extensions, ExtensionObservation{Name: layer.Metadata.Name, Version: layer.Metadata.Version, Image: layer.Image})
	}
	slices.SortFunc(observation.Extensions, func(a, b ExtensionObservation) int {
		if n := strings.Compare(a.Name, b.Name); n != 0 {
			return n
		}
		if n := strings.Compare(a.Version, b.Version); n != 0 {
			return n
		}
		return strings.Compare(a.Image, b.Image)
	})

	// Machinery 1.14 does not export a singleton ID for KubeletStatus. Require
	// exactly one actual resource instead of guessing an ID or using desired config.
	kubeletKind := resource.NewMetadata(k8sresource.NamespaceName, k8sresource.KubeletStatusType, "", resource.VersionUndefined)
	kubelets, err := t.client.COSI.List(ctx, kubeletKind)
	if err != nil || len(kubelets.Items) != 1 {
		return NodeObservation{}, errors.New("Talos kubelet runtime observation failed")
	}
	kubelet, ok := kubelets.Items[0].(*k8sresource.KubeletStatus)
	if !ok || kubelet == nil || strings.TrimSpace(kubelet.TypedSpec().Image) == "" {
		return NodeObservation{}, errors.New("Talos kubelet runtime observation failed")
	}
	observation.KubeletImage = kubelet.TypedSpec().Image

	// A reboot during these reads would combine evidence from different boots.
	boot, err = safe.StateGetByID[*runtimeresource.BootID](ctx, t.client.COSI, runtimeresource.BootIDID)
	if err != nil || boot == nil || boot.TypedSpec().BootID != observation.BootID {
		return NodeObservation{}, errors.New("Talos boot identity changed during observation")
	}

	return observation, nil
}

// Apply records only the single node's chosen mode. AUTO in the request is not
// evidence of either outcome. Warnings, mode details and remote errors can carry
// configuration bytes and must never be retained or wrapped.
func (t *talosNodeClient) Apply(ctx context.Context, data []byte) (updateoperation.ApplyOutcome, error) {
	ctx = endpointclient.WithNode(ctx, t.endpoint)
	response, err := t.client.MachineClient.ApplyConfiguration(ctx, &machineapi.ApplyConfigurationRequest{
		Data: data,
		Mode: machineapi.ApplyConfigurationRequest_AUTO,
	})
	if err != nil {
		return updateoperation.Unknown, errors.New("Talos configuration apply failed")
	}
	messages := response.GetMessages()
	if len(messages) != 1 || messages[0] == nil || operationMetadataFailed(messages[0].GetMetadata()) {
		return updateoperation.Unknown, errors.New("Talos configuration apply acknowledgment is unknown")
	}
	switch messages[0].GetMode() {
	case machineapi.ApplyConfigurationRequest_NO_REBOOT:
		return updateoperation.NoReboot, nil
	case machineapi.ApplyConfigurationRequest_REBOOT: //nolint:staticcheck // actual chosen response mode, not a deprecated request
		return updateoperation.RebootRequired, nil
	default:
		return updateoperation.Unknown, errors.New("Talos configuration apply outcome is unknown")
	}
}

// PullInstaller consumes the final name returned by Pull and resolves only that
// exact name in the system ImageService.List. This proves pulled content, not the
// digest of an already booted operating system.
func (t *talosNodeClient) PullInstaller(ctx context.Context, image string) (name, digest string, err error) {
	ctx = endpointclient.WithNode(ctx, t.endpoint)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	pull, err := t.client.ImageClient.Pull(ctx, &machineapi.ImageServicePullRequest{Containerd: systemContainerd, ImageRef: image})
	if err != nil {
		return "", "", errors.New("Talos installer pull failed")
	}
	for {
		response, receiveErr := pull.Recv()
		if errors.Is(receiveErr, io.EOF) {
			break
		}
		if receiveErr != nil || response == nil {
			return "", "", errors.New("Talos installer pull acknowledgment is unknown")
		}
		if final, ok := response.GetResponse().(*machineapi.ImageServicePullResponse_Name); ok {
			if final.Name == "" || (name != "" && name != final.Name) {
				return "", "", errors.New("Talos installer pull identity is unknown")
			}
			name = final.Name
		}
	}
	if name == "" {
		return "", "", errors.New("Talos installer pull returned no final image name")
	}

	listed, err := t.client.ImageClient.List(ctx, &machineapi.ImageServiceListRequest{Containerd: systemContainerd})
	if err != nil {
		return "", "", errors.New("Talos installer digest observation failed")
	}
	matches := 0
	for {
		response, receiveErr := listed.Recv()
		if errors.Is(receiveErr, io.EOF) {
			break
		}
		if receiveErr != nil || response == nil {
			return "", "", errors.New("Talos installer digest observation failed")
		}
		if response.GetName() == name {
			matches++
			digest = response.GetDigest()
		}
	}
	if matches != 1 || strings.TrimSpace(digest) == "" {
		return "", "", errors.New("Talos pulled installer digest is unknown")
	}

	return name, digest, nil
}

// Install performs only the lifecycle Upgrade installation phase. A received
// explicit ExitCode=0 is the only successful acknowledgment; EOF is unknown.
// Reboot is a separate persisted operation, never implicit here.
func (t *talosNodeClient) Install(ctx context.Context, imageName string) (int32, error) {
	ctx = endpointclient.WithNode(ctx, t.endpoint)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	install, err := t.client.LifecycleClient.Upgrade(ctx, &machineapi.LifecycleServiceUpgradeRequest{
		Containerd: systemContainerd,
		Source:     &machineapi.InstallArtifactsSource{ImageName: imageName},
	})
	if err != nil {
		return -1, errors.New("Talos installer installation acknowledgment is unknown")
	}
	for {
		response, receiveErr := install.Recv()
		if receiveErr != nil || response == nil {
			return -1, errors.New("Talos installer installation ended without an acknowledged exit code")
		}
		if exit, ok := response.GetProgress().GetResponse().(*machineapi.LifecycleServiceInstallProgress_ExitCode); ok {
			if exit.ExitCode != 0 {
				return exit.ExitCode, fmt.Errorf("Talos installer installation exited with code %d", exit.ExitCode)
			}

			return 0, nil
		}
	}
}

// Reboot acknowledges the actual MachineService request. Completion still needs
// an independently observed changed BootID and target runtime identities.
func (t *talosNodeClient) Reboot(ctx context.Context) error {
	ctx = endpointclient.WithNode(ctx, t.endpoint)
	response, err := t.client.MachineClient.Reboot(ctx, &machineapi.RebootRequest{Mode: machineapi.RebootRequest_DEFAULT})
	if err != nil {
		return errors.New("Talos reboot acknowledgment is unknown")
	}
	messages := response.GetMessages()
	if len(messages) != 1 || messages[0] == nil || operationMetadataFailed(messages[0].GetMetadata()) {
		return errors.New("Talos reboot acknowledgment is unknown")
	}

	return nil
}

func operationMetadataFailed(metadata *common.Metadata) bool { //nolint:staticcheck // legacy metadata remains present in machinery 1.14 responses
	return metadata != nil && (metadata.GetError() != "" || metadata.GetStatus().GetCode() != 0)
}
