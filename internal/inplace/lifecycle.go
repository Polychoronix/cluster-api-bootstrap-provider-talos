// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package inplace

import "github.com/siderolabs/talos/pkg/machinery/api/common"

// Installer artifacts use the same system containerd namespace as talosctl.
var systemContainerd = &common.ContainerdInstance{Driver: common.ContainerDriver_CONTAINERD, Namespace: common.ContainerdNamespace_NS_SYSTEM}
