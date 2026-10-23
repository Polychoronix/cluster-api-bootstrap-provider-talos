// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"bytes"
	"context"
	"encoding/json"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"reflect"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type managementAssociationState struct {
	Machine   ObjectIdentity          `json:"machine"`
	Before    []metav1.OwnerReference `json:"before"`
	After     []metav1.OwnerReference `json:"after"`
	Completed bool                    `json:"completed"`
}

// ManagementAssociation is the private frozen aggregate initial owner change.
// The caller applies only these ownerReferences with its supported no-create
// Machine UID/RV CAS. Partial transfer never activates management.
type ManagementAssociation struct {
	authority     ManagementAuthority
	observationID string
	states        []managementAssociationState
}

func (a *ManagementAssociation) CurrentOwners(uid types.UID) []metav1.OwnerReference {
	for _, s := range a.states {
		if s.Machine.UID == uid {
			return append([]metav1.OwnerReference(nil), s.Before...)
		}
	}
	return nil
}
func (a *ManagementAssociation) DesiredOwners(uid types.UID) []metav1.OwnerReference {
	for _, s := range a.states {
		if s.Machine.UID == uid {
			return append([]metav1.OwnerReference(nil), s.After...)
		}
	}
	return nil
}
func (*ManagementAssociation) String() string {
	return "<private approved Talos ownership association>"
}
func (a *ManagementAssociation) GoString() string { return a.String() }

// PrepareManagementAssociation verifies the original manifest approval and full
// current safety snapshot, then freezes all Machine owner deltas WITHOUT setting
// activated. Repeated calls accept only original or exactly attested owners.
func PrepareManagementAssociation(ctx context.Context, c client.Client, reader client.Reader, request ManagementObservationRequest) (*ManagementAssociation, error) {
	if request.Authority.Kind != "TalosControlPlane" || request.Spec == nil || request.Spec.Mode != bootstrapv1.ManagementModeManaged || request.Spec.Approval == nil {
		return nil, managementError("association approval required")
	}
	if err := validateManagementAuthority(ctx, reader, request.Authority); err != nil {
		return nil, err
	}
	secret, record, payload, err := readManagement(ctx, reader, request.Authority)
	if err != nil {
		return nil, err
	}
	if record.Activated {
		return nil, managementError("association already activated")
	}
	if record.Revision != request.Spec.InventoryRevision || request.Spec.Approval.Revision != record.Revision || request.Spec.Approval.ObservationID != record.ObservationID {
		return nil, managementError("association approval changed")
	}
	safety := &managementSafety{}
	if json.Unmarshal(payload, safety) != nil {
		return nil, managementError("association snapshot")
	}
	states := make([]managementAssociationState, 0, len(safety.Members))
	for _, member := range safety.Members {
		before := append([]metav1.OwnerReference(nil), member.Machine.OwnerReferences...)
		alreadyOwned := false
		for _, owner := range before {
			if owner.Controller != nil && *owner.Controller {
				if owner.APIVersion != request.Authority.APIVersion || owner.Kind != request.Authority.Kind || owner.Name != request.Authority.Name || owner.UID != request.Authority.UID {
					return nil, managementError("association existing foreign controller")
				}
				alreadyOwned = true
			}
		}
		after := append([]metav1.OwnerReference(nil), before...)
		if !alreadyOwned {
			after = append(after, metav1.OwnerReference{APIVersion: request.Authority.APIVersion, Kind: request.Authority.Kind, Name: request.Authority.Name, UID: request.Authority.UID, Controller: new(true), BlockOwnerDeletion: new(true)})
		}
		states = append(states, managementAssociationState{Machine: member.Machine, Before: before, After: after})
	}
	if len(record.Associations) != 0 && !sameAssociationIntent(record.Associations, states) {
		return nil, managementError("association stored delta invalid")
	}
	attestation := &ManagementAssociation{authority: request.Authority, observationID: record.ObservationID, states: states}
	if err := validateAssociationObservation(ctx, reader, request, attestation, payload, false); err != nil {
		return nil, err
	}
	if len(record.Associations) == 0 {
		record.Associations = states
		data, err := json.Marshal(record)
		if err != nil || PatchData(ctx, c, secret, map[string][]byte{managementRecordKey: data}) != nil {
			return nil, managementError("association prepare CAS")
		}
	}
	return attestation, nil
}

// CompleteManagementAssociation reobserves ALL configured members with exact
// attested desired owners and unchanged safety, then CAS activates the original
// observation. It never mutates Machines or replaces the immutable snapshot.
func CompleteManagementAssociation(ctx context.Context, c client.Client, reader client.Reader, request ManagementObservationRequest, a *ManagementAssociation) (*bootstrapv1.ManagementStatus, error) {
	if a == nil || a.authority != request.Authority || request.Spec == nil || request.Spec.Mode != bootstrapv1.ManagementModeManaged || request.Spec.Approval == nil {
		return nil, managementError("association attestation missing")
	}
	if err := validateManagementAuthority(ctx, reader, request.Authority); err != nil {
		return nil, err
	}
	secret, record, payload, err := readManagement(ctx, reader, a.authority)
	if err != nil {
		return nil, err
	}
	if record.ObservationID != a.observationID || record.Revision != request.Spec.InventoryRevision || request.Spec.Approval.Revision != record.Revision || request.Spec.Approval.ObservationID != record.ObservationID || !sameAssociationIntent(record.Associations, a.states) {
		return nil, managementError("association attestation changed")
	}
	if err := validateAssociationObservation(ctx, reader, request, a, payload, true); err != nil {
		return nil, err
	}
	if !record.Activated {
		record.Activated = true
		for i := range record.Associations {
			record.Associations[i].Completed = true
		}
		data, err := json.Marshal(record)
		if err != nil || PatchData(ctx, c, secret, map[string][]byte{managementRecordKey: data}) != nil {
			return nil, managementError("association activation CAS")
		}
	}
	return managementStatus(record), nil
}

func validateAssociationObservation(ctx context.Context, reader client.Reader, request ManagementObservationRequest, a *ManagementAssociation, payload []byte, complete bool) error {
	observed, err := ObserveManagement(ctx, reader, request)
	if err != nil {
		return err
	}
	if len(observed.safety.Members) != len(a.states) {
		return managementError("association inventory changed")
	}
	for i := range observed.safety.Members {
		member := &observed.safety.Members[i]
		found := false
		for _, state := range a.states {
			if member.Machine.Name == state.Machine.Name && member.Machine.UID == state.Machine.UID {
				current := member.Machine.OwnerReferences
				if complete {
					if !reflect.DeepEqual(current, state.After) {
						return managementError("association incomplete")
					}
				} else if !reflect.DeepEqual(current, state.Before) && !reflect.DeepEqual(current, state.After) {
					return managementError("association unauthorized owner change")
				}
				member.Machine.OwnerReferences = append([]metav1.OwnerReference(nil), state.Before...)
				found = true
			}
		}
		if !found {
			return managementError("association unknown member")
		}
	}
	encoded, err := json.Marshal(observed.safety)
	if err != nil || !bytes.Equal(encoded, payload) {
		return managementError("association safety changed")
	}
	return nil
}

func sameAssociationIntent(a, b []managementAssociationState) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !reflect.DeepEqual(a[i].Machine, b[i].Machine) || !reflect.DeepEqual(a[i].Before, b[i].Before) || !reflect.DeepEqual(a[i].After, b[i].After) {
			return false
		}
	}
	return true
}
