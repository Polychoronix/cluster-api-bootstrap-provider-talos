// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package inplace

import machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"

func progressMessage(message string) *machineapi.LifecycleServiceUpgradeResponse {
	return &machineapi.LifecycleServiceUpgradeResponse{Progress: &machineapi.LifecycleServiceInstallProgress{Response: &machineapi.LifecycleServiceInstallProgress_Message{Message: message}}}
}
func progressExit(code int32) *machineapi.LifecycleServiceUpgradeResponse {
	return &machineapi.LifecycleServiceUpgradeResponse{Progress: &machineapi.LifecycleServiceInstallProgress{Response: &machineapi.LifecycleServiceInstallProgress_ExitCode{ExitCode: code}}}
}
