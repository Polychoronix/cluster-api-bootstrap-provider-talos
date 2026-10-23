// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
	endpointclient "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/talosclient"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const managementProvenance = "talos-management"
const managementSnapshotKey = "snapshot"
const managementRecordKey = "management"

var ErrManagementPaused = errors.New("Talos management reconciliation is paused")

// ManagementAuthority identifies the live object whose single manifest approval
// governs observation. Namespace is explicit and every input must be local to it.
type ManagementAuthority struct {
	Namespace  string    `json:"namespace"`
	APIVersion string    `json:"apiVersion"`
	Kind       string    `json:"kind"`
	Name       string    `json:"name"`
	UID        types.UID `json:"uid"`
}

// ManagementObservationRequest supplies explicit live pins; ObserveManagement
// re-fetches them with reader (the manager APIReader), never an ownership cache.
// A TCP need not yet own the Machines. Their TC and InfraMachine must already be
// controlled by each exact Machine; reads never manufacture those associations.
type ManagementObservationRequest struct {
	Authority    ManagementAuthority
	Cluster      *clusterv1.Cluster
	Spec         *bootstrapv1.ManagementSpec
	Machines     []*clusterv1.Machine
	TalosConfigs []*bootstrapv1.TalosConfig
}

type ManagementObservationSummary struct {
	Observation       bootstrapv1.ObservationStatus
	InventoryVerified bool
	MachinesReady     bool
	EtcdHealthy       bool
}

type managementMember struct {
	Machine            ObjectIdentity               `json:"machine"`
	TalosConfig        ObjectIdentity               `json:"talosConfig"`
	Infrastructure     ObjectIdentity               `json:"infrastructure"`
	NodeName           string                       `json:"nodeName"`
	NodeUID            types.UID                    `json:"nodeUID"`
	ProviderID         string                       `json:"providerID"`
	MachineSpec        json.RawMessage              `json:"machineSpec"`
	ConfigSpec         json.RawMessage              `json:"configSpec"`
	InfrastructureSpec json.RawMessage              `json:"infrastructureSpec"`
	Sources            []configinput.SourceIdentity `json:"sources"`
	Seed               configinput.SourceIdentity   `json:"seed"`
	Runtime            managementRuntime            `json:"runtime"`
}

type managementRuntime struct {
	Configuration []byte                                `json:"configuration"`
	BootID        string                                `json:"bootID"`
	OSVersion     string                                `json:"osVersion"`
	KubeletImage  string                                `json:"kubeletImage,omitempty"`
	SchematicID   string                                `json:"schematicID,omitempty"`
	Extensions    []endpointclient.ExtensionObservation `json:"extensions"`
}

type managementSafety struct {
	Authority     ManagementAuthority          `json:"authority"`
	ClusterUID    types.UID                    `json:"clusterUID"`
	ClusterName   string                       `json:"clusterName"`
	ClusterInputs json.RawMessage              `json:"clusterInputs"`
	Revision      string                       `json:"revision"`
	Inputs        []configinput.SourceIdentity `json:"inputs"`
	Members       []managementMember           `json:"members"`
	Etcd          []managementEtcdMember       `json:"etcd,omitempty"`
}

type managementEtcdMember struct {
	MemberID   uint64    `json:"memberID"`
	Hostname   string    `json:"hostname"`
	PeerURLs   []string  `json:"peerURLs"`
	ClientURLs []string  `json:"clientURLs"`
	IsLearner  bool      `json:"isLearner"`
	MachineUID types.UID `json:"machineUID"`
	NodeUID    types.UID `json:"nodeUID"`
}

// ManagementObservation has no public encoding surface. Its complete sensitive
// payload is serialized only by this package into immutable Kubernetes Secrets.
type ManagementObservation struct {
	safety  managementSafety
	summary ManagementObservationSummary
}

func (o *ManagementObservation) Summary() ManagementObservationSummary {
	if o == nil {
		return ManagementObservationSummary{}
	}
	return o.summary
}
func (*ManagementObservation) String() string     { return "<private Talos management observation>" }
func (o *ManagementObservation) GoString() string { return o.String() }
func (*ManagementObservation) MarshalJSON() ([]byte, error) {
	return []byte(`"<private Talos management observation>"`), nil
}

type managementRecord struct {
	Version        string              `json:"version"`
	Authority      ManagementAuthority `json:"authority"`
	Revision       string              `json:"revision"`
	ObservationID  string              `json:"observationID"`
	ObservedAt     metav1.Time         `json:"observedAt"`
	SnapshotName   string              `json:"snapshotName"`
	SnapshotUID    types.UID           `json:"snapshotUID"`
	SnapshotDigest [32]byte            `json:"snapshotDigest"`
	Activated      bool                `json:"activated"`
}

func managementError(stage string) error          { return fmt.Errorf("Talos management %s failed", stage) }
func managementName(a ManagementAuthority) string { return a.Name + "-management" }
func managementOwned(s *corev1.Secret, a ManagementAuthority) bool {
	if s.Namespace != a.Namespace || s.Annotations[ProvenanceAnnotation] != managementProvenance {
		return false
	}
	for _, r := range s.OwnerReferences {
		if r.APIVersion == a.APIVersion && r.Kind == a.Kind && r.Name == a.Name && r.UID == a.UID && r.Controller != nil && *r.Controller {
			return true
		}
	}
	return false
}
func managementSecret(a ManagementAuthority, name string, data map[string][]byte) *corev1.Secret {
	controller := true
	return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: a.Namespace, Name: name, Annotations: map[string]string{ProvenanceAnnotation: managementProvenance}, OwnerReferences: []metav1.OwnerReference{{APIVersion: a.APIVersion, Kind: a.Kind, Name: a.Name, UID: a.UID, Controller: &controller}}}, Data: data}
}
func managementStatus(r *managementRecord) *bootstrapv1.ManagementStatus {
	s := &bootstrapv1.ManagementStatus{Observation: &bootstrapv1.ObservationStatus{Revision: r.Revision, ID: r.ObservationID, ObservedAt: r.ObservedAt}}
	if r.Activated {
		s.ActivatedRevision = r.Revision
	}
	return s
}
func readManagement(ctx context.Context, reader client.Reader, a ManagementAuthority) (*corev1.Secret, *managementRecord, []byte, error) {
	s := &corev1.Secret{}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: a.Namespace, Name: managementName(a)}, s); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, nil, ErrNotFound
		}
		return nil, nil, nil, managementError("record lookup")
	}
	r := &managementRecord{}
	if !managementOwned(s, a) || json.Unmarshal(s.Data[managementRecordKey], r) != nil || r.Version != "v1" || r.Authority != a || r.ObservationID == "" || r.Revision == "" || r.SnapshotUID == "" {
		return nil, nil, nil, managementError("record provenance")
	}
	r.ObservedAt.Time = r.ObservedAt.Time.UTC()
	snapshot := &corev1.Secret{}
	if reader.Get(ctx, client.ObjectKey{Namespace: a.Namespace, Name: r.SnapshotName}, snapshot) != nil || !managementOwned(snapshot, a) || snapshot.UID != r.SnapshotUID || snapshot.Immutable == nil || !*snapshot.Immutable || sha256.Sum256(snapshot.Data[managementSnapshotKey]) != r.SnapshotDigest {
		return nil, nil, nil, managementError("immutable snapshot identity")
	}
	return s, r, snapshot.Data[managementSnapshotKey], nil
}

// RecordManagementObservation writes only authority-owned private records.
// Equal safety content reuses its opaque ID; changed unapproved content gets a
// new immutable snapshot. A lost/replaced approved snapshot is never recreated.
func RecordManagementObservation(ctx context.Context, c client.Client, reader client.Reader, o *ManagementObservation) (*bootstrapv1.ManagementStatus, error) {
	if o == nil || !o.summary.InventoryVerified {
		return nil, managementError("verified observation required")
	}
	if err := validateManagementAuthority(ctx, reader, o.safety.Authority); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(o.safety)
	if err != nil {
		return nil, managementError("snapshot encoding")
	}
	s, r, previous, err := readManagement(ctx, reader, o.safety.Authority)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	if errors.Is(err, ErrNotFound) {
		authority := &unstructured.Unstructured{}
		authority.SetAPIVersion(o.safety.Authority.APIVersion)
		authority.SetKind(o.safety.Authority.Kind)
		if reader.Get(ctx, client.ObjectKey{Namespace: o.safety.Authority.Namespace, Name: o.safety.Authority.Name}, authority) != nil {
			return nil, managementError("authority record lookup")
		}
		if _, found, _ := unstructured.NestedMap(authority.Object, "spec", "management", "approval"); found {
			return nil, managementError("approved record missing")
		}
		if revision, _, _ := unstructured.NestedString(authority.Object, "status", "management", "activatedRevision"); revision != "" {
			return nil, managementError("activated record missing")
		}
	}
	if err == nil && bytes.Equal(previous, payload) {
		o.summary.Observation = *managementStatus(r).Observation
		return managementStatus(r), nil
	}
	if err == nil && r.Activated {
		return nil, managementError("activated safety snapshot changed")
	}
	id := uuid.NewString()
	immutable := true
	snapshot := managementSecret(o.safety.Authority, managementName(o.safety.Authority)+"-"+id, map[string][]byte{managementSnapshotKey: payload})
	snapshot.Immutable = &immutable
	if c.Create(ctx, snapshot) != nil {
		return nil, managementError("snapshot create")
	}
	next := &managementRecord{Version: "v1", Authority: o.safety.Authority, Revision: o.safety.Revision, ObservationID: id, ObservedAt: metav1.NewTime(time.Now().UTC().Truncate(time.Second)), SnapshotName: snapshot.Name, SnapshotUID: snapshot.UID, SnapshotDigest: sha256.Sum256(payload)}
	encoded, e := json.Marshal(next)
	if e != nil {
		return nil, managementError("record encoding")
	}
	if s == nil {
		s = managementSecret(o.safety.Authority, managementName(o.safety.Authority), map[string][]byte{managementRecordKey: encoded})
		if c.Create(ctx, s) != nil {
			return nil, managementError("record create conflict")
		}
	} else if PatchData(ctx, c, s, map[string][]byte{managementRecordKey: encoded}) != nil {
		return nil, managementError("record CAS")
	}
	o.summary.Observation = *managementStatus(next).Observation
	return managementStatus(next), nil
}

// ActivateManagement obtains a new uncached observation immediately before
// consuming the manifest approval. Approval, mode and generation are not safety
// content, so approving unchanged inputs does not change the observation ID.
func ActivateManagement(ctx context.Context, c client.Client, reader client.Reader, request ManagementObservationRequest) (*bootstrapv1.ManagementStatus, error) {
	if request.Spec == nil || request.Spec.Mode != bootstrapv1.ManagementModeManaged || request.Spec.Approval == nil {
		return nil, managementError("manifest approval required")
	}
	o, err := ObserveManagement(ctx, reader, request)
	if err != nil {
		return nil, err
	}
	s, r, payload, err := readManagement(ctx, reader, request.Authority)
	if err != nil {
		return nil, err
	}
	current, e := json.Marshal(o.safety)
	if e != nil || !bytes.Equal(payload, current) || request.Spec.InventoryRevision != r.Revision || request.Spec.Approval.Revision != r.Revision || request.Spec.Approval.ObservationID != r.ObservationID {
		return nil, managementError("approval does not match current observation")
	}
	if !r.Activated {
		r.Activated = true
		data, e := json.Marshal(r)
		if e != nil || PatchData(ctx, c, s, map[string][]byte{managementRecordKey: data}) != nil {
			return nil, managementError("activation CAS")
		}
	}
	return managementStatus(r), nil
}

// ValidateManagementActivation checks the exact live authority and immutable
// activation record before a consumer issues a new action. It never writes.
func ValidateManagementActivation(ctx context.Context, reader client.Reader, a ManagementAuthority, spec *bootstrapv1.ManagementSpec) error {
	if err := validateManagementAuthority(ctx, reader, a); err != nil {
		return err
	}
	if spec == nil || spec.Mode != bootstrapv1.ManagementModeManaged || spec.Approval == nil {
		return managementError("management is not activated")
	}
	_, r, payload, err := readManagement(ctx, reader, a)
	if err != nil {
		return err
	}
	if !r.Activated || r.Revision != spec.InventoryRevision || spec.Approval.Revision != r.Revision || spec.Approval.ObservationID != r.ObservationID {
		return managementError("activation identity")
	}
	safety := &managementSafety{}
	if json.Unmarshal(payload, safety) != nil {
		return managementError("activation snapshot decode")
	}
	cluster := &clusterv1.Cluster{}
	if reader.Get(ctx, client.ObjectKey{Namespace: a.Namespace, Name: safety.ClusterName}, cluster) != nil || cluster.UID != safety.ClusterUID {
		return managementError("activated Cluster identity")
	}
	if managementPaused(cluster) || (cluster.Spec.Paused != nil && *cluster.Spec.Paused) {
		return ErrManagementPaused
	}
	if current, err := managementClusterInputs(cluster); err != nil || !bytes.Equal(current, safety.ClusterInputs) {
		return managementError("activated Cluster inputs changed")
	}
	for _, source := range safety.Inputs {
		secret := &corev1.Secret{}
		if reader.Get(ctx, client.ObjectKey{Namespace: a.Namespace, Name: source.Name}, secret) != nil || !managementSame(managementSource(secret, source.Key), source) {
			return managementError("activated management source changed")
		}
	}
	return nil
}

func managementSource(s *corev1.Secret, key string) configinput.SourceIdentity {
	return configinput.SourceIdentity{Name: s.Name, Key: key, UID: s.UID, ResourceVersion: s.ResourceVersion, Digest: sha256.Sum256(s.Data[key])}
}
func managementObject(group, kind string, o client.Object) ObjectIdentity {
	return ObjectIdentity{APIGroup: group, Kind: kind, Name: o.GetName(), UID: o.GetUID(), OwnerReferences: append([]metav1.OwnerReference(nil), o.GetOwnerReferences()...)}
}
func managementController(o client.Object, m *clusterv1.Machine) bool {
	r := metav1.GetControllerOf(o)
	return r != nil && r.APIVersion == clusterv1.GroupVersion.String() && r.Kind == "Machine" && r.Name == m.Name && r.UID == m.UID
}
func managementSame(a, b any) bool { return reflect.DeepEqual(a, b) }
func managementPaused(o client.Object) bool {
	_, ok := o.GetAnnotations()[clusterv1.PausedAnnotation]
	return ok
}
func managementGroup(version string) string { return strings.SplitN(version, "/", 2)[0] }
