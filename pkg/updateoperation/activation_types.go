// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package updateoperation

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// MachineIdentity is derived from live inventory, never a second public consent declaration.
type MachineIdentity struct {
	Name string
	UID  types.UID
}

// ActivationStatus is internal controller bookkeeping; the public API exposes one condition.
type ActivationStatus struct {
	ID         string
	ObservedAt metav1.Time
	Activated  bool
}
