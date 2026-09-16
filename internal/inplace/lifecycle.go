// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package inplace

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/siderolabs/talos/pkg/machinery/api/common"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	"google.golang.org/grpc"
)

// upgradeClients is the slice of the Talos API a lifecycle upgrade drives.
type upgradeClients struct {
	images    machineapi.ImageServiceClient
	lifecycle machineapi.LifecycleServiceClient
	machine   machineapi.MachineServiceClient
}

// systemContainerd is where installer images live. talosctl upgrade targets the same instance.
var systemContainerd = &common.ContainerdInstance{
	Driver:    common.ContainerDriver_CONTAINERD,
	Namespace: common.ContainerdNamespace_NS_SYSTEM,
}

// lifecycleUpgrade upgrades a node the way talosctl 1.14 does: pull the installer image into the
// system containerd, stream the install until it reports an exit code, then reboot. Talos cordons
// and drains the node itself as part of its reboot sequence. It returns once the reboot has been
// requested, which can be many minutes after it was called.
func lifecycleUpgrade(ctx context.Context, c upgradeClients, image string) error {
	pull, err := c.images.Pull(ctx, &machineapi.ImageServicePullRequest{Containerd: systemContainerd, ImageRef: image})
	if err != nil {
		return fmt.Errorf("failed to start pulling %q: %w", image, err)
	}

	if err := drain(pull); err != nil {
		return fmt.Errorf("failed to pull %q: %w", image, err)
	}

	install, err := c.lifecycle.Upgrade(ctx, &machineapi.LifecycleServiceUpgradeRequest{
		Containerd: systemContainerd,
		Source:     &machineapi.InstallArtifactsSource{ImageName: image},
	})
	if err != nil {
		return fmt.Errorf("failed to start the upgrade to %q: %w", image, err)
	}

	if err := waitForInstall(install, image); err != nil {
		return err
	}

	if _, err := c.machine.Reboot(ctx, &machineapi.RebootRequest{Mode: machineapi.RebootRequest_DEFAULT}); err != nil {
		return fmt.Errorf("failed to reboot into %q: %w", image, err)
	}

	return nil
}

// waitForInstall consumes the install stream until it reports an exit code.
func waitForInstall(stream grpc.ServerStreamingClient[machineapi.LifecycleServiceUpgradeResponse], image string) error {
	var last string

	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("the upgrade to %q ended without an exit code; last message: %q", image, last)
		}

		if err != nil {
			return fmt.Errorf("the upgrade to %q failed: %w", image, err)
		}

		switch progress := resp.GetProgress().GetResponse().(type) {
		case *machineapi.LifecycleServiceInstallProgress_Message:
			last = progress.Message
		case *machineapi.LifecycleServiceInstallProgress_ExitCode:
			if progress.ExitCode != 0 {
				return fmt.Errorf("the upgrade to %q exited with code %d: %s", image, progress.ExitCode, last)
			}

			return nil
		}
	}
}

// drain reads a pull stream to its end; progress is not interesting, errors are.
func drain(stream grpc.ServerStreamingClient[machineapi.ImageServicePullResponse]) error {
	for {
		if _, err := stream.Recv(); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return err
		}
	}
}
