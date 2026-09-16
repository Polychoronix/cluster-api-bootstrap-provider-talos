// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"fmt"

	"github.com/siderolabs/talos/pkg/machinery/config"
)

// unattendedInstallPatch is the document form of the installer image. The generator emits no
// install document when CABPT passes no install disk, so this patch creates it. Validation
// requires a disk selector: system_disk names the disk a running node booted from, which is a
// no-op on an installed node and the right target for an in-place upgrade. Users override it
// with a strategic patch of their own on the document.
const unattendedInstallPatch = `apiVersion: v1alpha1
kind: UnattendedInstallConfig
installer:
  image: %s
provisioning:
  diskSelector:
    match: system_disk
`

// legacyInstallPatch is the machine.install.image form for Talos versions that predate the
// UnattendedInstallConfig document.
const legacyInstallPatch = "machine:\n  install:\n    image: %s\n"

// installImagePatch renders the installer image resolveInstallerImage derived from
// spec.imageFactory as a strategic merge patch in the form the target Talos reads: the
// UnattendedInstallConfig document for contracts that know it (1.14 and an unset talosVersion),
// the legacy machine.install.image field before that.
//
// It is applied ahead of any user-supplied strategic patches so that an explicit patch in the
// TalosConfig still wins. The resolved image is a good default, not an override of intent.
func installImagePatch(contract *config.VersionContract, image string) string {
	if contract.UnattendedInstallConfig() {
		return fmt.Sprintf(unattendedInstallPatch, image)
	}

	return fmt.Sprintf(legacyInstallPatch, image)
}
