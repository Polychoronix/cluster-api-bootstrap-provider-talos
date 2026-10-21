// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package configinput

import (
	"strings"

	"github.com/Masterminds/semver/v3"
	"github.com/siderolabs/talos/pkg/machinery/config/configloader"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"
)

// validateBaselineVersion does not change an imported configuration. Reviewed patches or a
// rotated baseline must explicitly match the requested Machine version before any operation.
func validateBaselineVersion(data []byte, target string) error {
	if target == "" {
		return nil
	}
	want, err := semver.StrictNewVersion(strings.TrimPrefix(target, "v"))
	if err != nil {
		return failure("Machine", "Kubernetes version validation")
	}
	provider, err := configloader.NewFromBytes(data)
	if err != nil {
		return failure("baseline", "Kubernetes version parse")
	}
	images := []string{provider.K8sKubeletConfig().Image()}
	if provider.Machine().Type() == machine.TypeControlPlane || provider.Machine().Type() == machine.TypeInit {
		if api := provider.K8sAPIServerConfig(); api != nil {
			images = append(images, api.Image())
		}
		if cm := provider.K8sControllerManagerConfig(); cm != nil && cm.Enabled() {
			images = append(images, cm.Image())
		}
		if scheduler := provider.K8sSchedulerConfig(); scheduler != nil && scheduler.Enabled() {
			images = append(images, scheduler.Image())
		}
	}
	for _, image := range images {
		// Talos 1.14 requires a version tag even with a digest. Registry names/ports are irrelevant.
		if at := strings.IndexByte(image, '@'); at >= 0 {
			image = image[:at]
		}
		colon := strings.LastIndexByte(image, ':')
		if colon < 0 || strings.Contains(image[colon:], "/") {
			return failure("baseline", "Kubernetes component version tag required")
		}
		version, err := semver.StrictNewVersion(strings.TrimPrefix(image[colon+1:], "v"))
		if err != nil || !version.Equal(want) {
			return failure("baseline", "reviewed Kubernetes component version must match Machine target")
		}
	}
	return nil
}
