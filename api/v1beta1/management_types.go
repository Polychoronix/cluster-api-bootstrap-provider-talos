// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// ManagementMode selects observation or explicitly approved management.
type ManagementMode string

const (
	ManagementModeObserve        ManagementMode = "Observe"
	ManagementModeManaged        ManagementMode = "Managed"
	InventoryVerifiedCondition                  = "InventoryVerified"
	ManagementActivatedCondition                = "ManagementActivated"
)

// ManagementSpec binds management to an explicit Machine inventory and observation.
// +kubebuilder:validation:XValidation:rule="self.mode != 'Managed' || (has(self.approval) && self.approval.revision == self.inventoryRevision)",message="Managed requires approval matching inventoryRevision"
type ManagementSpec struct {
	// Mode defaults to Observe only when management is present.
	// +kubebuilder:default=Observe
	// +kubebuilder:validation:Enum=Observe;Managed
	Mode ManagementMode `json:"mode,omitempty"`
	// InventoryRevision identifies this non-secret inventory.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-zA-Z0-9][a-zA-Z0-9._-]*$`
	InventoryRevision string `json:"inventoryRevision"`
	// Machines pins names to immutable Kubernetes identities.
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=1000
	Machines []MachineIdentityReference `json:"machines"`
	// Approval explicitly authorizes the observed revision.
	// +optional
	Approval *ObservationApproval `json:"approval,omitempty"`
}

// MachineIdentityReference identifies a namespace-local Machine, not just its name.
type MachineIdentityReference struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	Name string `json:"name"`
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-zA-Z0-9][a-zA-Z0-9._-]*$`
	UID types.UID `json:"uid"`
}

// ObservationApproval names the exact observation approved by the operator.
type ObservationApproval struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-zA-Z0-9][a-zA-Z0-9._-]*$`
	Revision string `json:"revision"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-zA-Z0-9][a-zA-Z0-9._-]*$`
	ObservationID string `json:"observationID"`
}

// ManagementStatus records observation and activation without private configuration.
type ManagementStatus struct {
	// +optional
	Observation *ObservationStatus `json:"observation,omitempty"`
	// +optional
	ActivatedRevision string `json:"activatedRevision,omitempty"`
}

// ObservationStatus identifies a completed observation of the inventory.
type ObservationStatus struct {
	Revision   string      `json:"revision"`
	ID         string      `json:"id"`
	ObservedAt metav1.Time `json:"observedAt"`
}
