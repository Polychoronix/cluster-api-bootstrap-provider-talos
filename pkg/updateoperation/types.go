// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package updateoperation stores private, UID-bound requested Talos operation receipts.
package updateoperation

import (
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	JournalKey                 = "journal"
	ReceiptKey                 = "receipt"
	ConsumedReceiptKey         = "consumedReceipt"
	LastReceiptKey             = "lastReceipt"
	OwnerIntentKey             = "ownerIntent"
	OwnerIntentVersion         = "v1"
	SchemaVersion              = "v1"
	OwnerIntentProvenanceValue = "cacppt"
	AcceptedIntentKey          = "acceptedIntent"
	OwnerPlanNameAnnotation    = "bootstrap.cluster.x-k8s.io/update-plan-secret"
	OwnerPlanUIDAnnotation     = "bootstrap.cluster.x-k8s.io/update-plan-uid"
	ProvenanceAnnotation       = "bootstrap.cluster.x-k8s.io/update-operation-created-by"
	ProvenanceValue            = "cabpt"
)

type Phase string

const (
	Planned   Phase = "Planned"
	Observed  Phase = "Observed"
	Ambiguous Phase = "Ambiguous"
)

type StepPhase string

const (
	StepPlanned   StepPhase = "Planned"
	Issued        StepPhase = "Issued"
	Acknowledged  StepPhase = "Acknowledged"
	StepObserved  StepPhase = "Observed"
	StepAmbiguous StepPhase = "Ambiguous"
)

type OperationKind string

const (
	Apply         OperationKind = "Apply"
	PullInstaller OperationKind = "PullInstaller"
	Install       OperationKind = "Install"
	Reboot        OperationKind = "Reboot"
)

type ApplyOutcome string

const (
	Unknown        ApplyOutcome = "Unknown"
	NoReboot       ApplyOutcome = "NoReboot"
	RebootRequired ApplyOutcome = "RebootRequired"
)

type ObjectIdentity struct {
	APIGroup        string                  `json:"apiGroup"`
	Kind            string                  `json:"kind"`
	Name            string                  `json:"name"`
	UID             types.UID               `json:"uid"`
	OwnerReferences []metav1.OwnerReference `json:"ownerReferences"`
}
type OwnerPlanReference struct {
	Name string    `json:"name"`
	UID  types.UID `json:"uid"`
}
type ImageIdentity struct {
	Reference   string   `json:"reference"`
	Digest      string   `json:"digest,omitempty"`
	SchematicID string   `json:"schematicID,omitempty"`
	Extensions  []string `json:"extensions,omitempty"`
}
type OperationStep struct {
	Kind              OperationKind `json:"kind"`
	Phase             StepPhase     `json:"phase"`
	IssuerID          string        `json:"issuerID,omitempty"`
	IssuedAt          *metav1.Time  `json:"issuedAt,omitempty"`
	Deadline          *metav1.Time  `json:"deadline,omitempty"`
	IssuedBootID      string        `json:"issuedBootID,omitempty"`
	ObservedBootID    string        `json:"observedBootID,omitempty"`
	ApplyOutcome      ApplyOutcome  `json:"applyOutcome,omitempty"`
	PulledImageName   string        `json:"pulledImageName,omitempty"`
	PulledImageDigest string        `json:"pulledImageDigest,omitempty"`
	InstallExitCode   *int32        `json:"installExitCode,omitempty"`
}
type AcceptedIntent struct {
	MachineUID     types.UID     `json:"machineUID"`
	TalosConfigUID types.UID     `json:"talosConfigUID"`
	Image          ImageIdentity `json:"image"`
	TransactionID  string        `json:"transactionID,omitempty"`
}
type Journal struct {
	Version                  string                       `json:"version"`
	TransactionID            string                       `json:"transactionID"`
	Machine                  ObjectIdentity               `json:"machine"`
	TalosConfig              ObjectIdentity               `json:"talosConfig"`
	Infrastructure           ObjectIdentity               `json:"infrastructure"`
	OwnerPlan                *OwnerPlanReference          `json:"ownerPlan,omitempty"`
	Inputs                   []configinput.SourceIdentity `json:"inputs"`
	InputFreshness           []byte                       `json:"inputFreshness"`
	Configuration            []byte                       `json:"configuration"`
	Phase                    Phase                        `json:"phase"`
	Steps                    []OperationStep              `json:"steps"`
	PriorAcceptedImageIntent ImageIdentity                `json:"priorAcceptedImageIntent"`
	TargetImage              ImageIdentity                `json:"targetImage"`
	TargetKubernetesVersion  string                       `json:"targetKubernetesVersion"`
	Endpoint                 bootstrapv1.TalosEndpoint    `json:"endpoint"`
	NodeName                 string                       `json:"nodeName"`
	NodeUID                  types.UID                    `json:"nodeUID"`
	NodeProviderID           string                       `json:"nodeProviderID"`
	ControlPlane             []ObjectIdentity             `json:"controlPlane,omitempty"`
	CompletedAt              *metav1.Time                 `json:"completedAt,omitempty"`
}
type CompletionReceipt struct {
	Version           string      `json:"version"`
	TransactionID     string      `json:"transactionID"`
	MachineUID        types.UID   `json:"machineUID"`
	TalosConfigUID    types.UID   `json:"talosConfigUID"`
	InfrastructureUID types.UID   `json:"infrastructureUID"`
	InputFreshness    []byte      `json:"inputFreshness"`
	Phase             Phase       `json:"phase"`
	CompletedAt       metav1.Time `json:"completedAt"`
}

// OwnerPlanIntent is the narrow executor seam inside an owner Prepared/Released plan.
// Source identities are captured at Prepared; final freshness is frozen only after writes.
type OwnerPlanIntent struct {
	Version        string                       `json:"version"`
	TransactionID  string                       `json:"transactionID"`
	Machine        ObjectIdentity               `json:"machine"`
	TalosConfig    ObjectIdentity               `json:"talosConfig"`
	Infrastructure ObjectIdentity               `json:"infrastructure"`
	Inputs         []configinput.SourceIdentity `json:"inputs"`
	InputFreshness []byte                       `json:"inputFreshness"`
	Phase          OwnerPhase                   `json:"phase"`
	ControlPlane   []ObjectIdentity             `json:"controlPlane,omitempty"`
	Owner          ObjectIdentity               `json:"owner"`
}

type OwnerPhase string

const (
	OwnerPrepared OwnerPhase = "Prepared"
	OwnerReleased OwnerPhase = "Released"
	OwnerConsumed OwnerPhase = "Consumed"
)
