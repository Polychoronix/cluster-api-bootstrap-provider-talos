// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"reflect"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var ErrNotFound = errors.New("Talos update operation journal not found")

func safe(stage string) error {
	return fmt.Errorf("Talos update operation %s failed; intervention may be required", stage)
}
func SecretName(config ObjectIdentity) string { return config.Name + "-update-operation" }
func validIdentity(o ObjectIdentity) bool {
	return o.APIGroup != "" && o.Kind != "" && o.Name != "" && o.UID != ""
}
func validateOwner(s *corev1.Secret, config ObjectIdentity) bool {
	if !validIdentity(config) || s.Annotations[ProvenanceAnnotation] != ProvenanceValue {
		return false
	}
	for _, o := range s.OwnerReferences {
		if o.UID == config.UID && o.Name == config.Name && o.Kind == config.Kind && o.APIVersion == config.APIGroup+"/v1beta1" && o.Controller != nil && *o.Controller {
			return true
		}
	}
	return false
}
func validReceipt(r *CompletionReceipt) bool {
	return r != nil && r.Version == SchemaVersion && r.TransactionID != "" && r.MachineUID != "" && r.TalosConfigUID != "" && r.InfrastructureUID != "" && r.Phase == Observed && len(r.InputFreshness) > 0 && !r.CompletedAt.IsZero()
}
func ReadReceipt(ctx context.Context, reader client.Reader, namespace string, config ObjectIdentity, transactionID string) (*CompletionReceipt, error) {
	s := &corev1.Secret{}
	if reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: SecretName(config)}, s) != nil {
		return nil, safe("receipt lookup")
	}
	if !validateOwner(s, config) {
		return nil, safe("receipt provenance")
	}
	r := &CompletionReceipt{}
	if json.Unmarshal(s.Data[ReceiptKey], r) != nil || !validReceipt(r) || transactionID == "" || r.TransactionID != transactionID || r.TalosConfigUID != config.UID {
		return nil, safe("receipt identity")
	}
	j := &Journal{}
	if json.Unmarshal(s.Data[JournalKey], j) != nil || validateJournal(j) != nil || !receiptMatches(r, j) {
		return nil, safe("receipt journal mismatch")
	}
	return r, nil
}
func ReadOwnerIntent(ctx context.Context, reader client.Reader, namespace string, ref OwnerPlanReference) (*OwnerPlanIntent, error) {
	s := &corev1.Secret{}
	if reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: ref.Name}, s) != nil {
		return nil, safe("owner intent lookup")
	}
	if ref.UID == "" || s.UID != ref.UID {
		return nil, safe("owner intent UID")
	}
	i := &OwnerPlanIntent{}
	if json.Unmarshal(s.Data[OwnerIntentKey], i) != nil {
		return nil, safe("owner intent decode")
	}
	if i.Version != OwnerIntentVersion || i.TransactionID == "" || i.Phase != OwnerReleased || len(i.InputFreshness) == 0 || !validIdentity(i.Machine) || !validIdentity(i.TalosConfig) || !validIdentity(i.Infrastructure) || !validIdentity(i.Owner) {
		return nil, safe("owner intent not released")
	}
	if s.Annotations[ProvenanceAnnotation] != OwnerIntentProvenanceValue {
		return nil, safe("owner intent provenance")
	}
	found := false
	for _, o := range s.OwnerReferences {
		if o.Name == i.Owner.Name && o.Kind == i.Owner.Kind && o.UID == i.Owner.UID && o.APIVersion == i.Owner.APIGroup+"/v1beta1" && o.Controller != nil && *o.Controller {
			found = true
		}
	}
	if !found {
		return nil, safe("owner intent ownership")
	}
	return i, nil
}
func ReadJournal(ctx context.Context, reader client.Reader, namespace string, config ObjectIdentity) (*corev1.Secret, *Journal, error) {
	s := &corev1.Secret{}
	err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: SecretName(config)}, s)
	if apierrors.IsNotFound(err) {
		return nil, nil, ErrNotFound
	}
	if err != nil {
		return nil, nil, safe("journal lookup")
	}
	if !validateOwner(s, config) {
		return nil, nil, safe("journal provenance")
	}
	j := &Journal{}
	if json.Unmarshal(s.Data[JournalKey], j) != nil || j.TalosConfig.UID != config.UID || j.TransactionID == "" {
		return nil, nil, safe("journal decode")
	}
	if err := validateJournal(j); err != nil {
		return nil, nil, err
	}
	return s, j, nil
}
func CreateJournal(ctx context.Context, c client.Client, namespace string, j *Journal) (*corev1.Secret, error) {
	if err := validateJournal(j); err != nil {
		return nil, err
	}
	data, e := json.Marshal(j)
	if e != nil {
		return nil, safe("journal encoding")
	}
	controller := true
	s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: SecretName(j.TalosConfig), Annotations: map[string]string{ProvenanceAnnotation: ProvenanceValue}, OwnerReferences: []metav1.OwnerReference{{APIVersion: j.TalosConfig.APIGroup + "/v1beta1", Kind: j.TalosConfig.Kind, Name: j.TalosConfig.Name, UID: j.TalosConfig.UID, Controller: &controller}}}, Data: map[string][]byte{JournalKey: data}}
	if c.Create(ctx, s) != nil {
		return nil, safe("journal create conflict")
	}
	return s, nil
}
func receiptMatches(r *CompletionReceipt, j *Journal) bool {
	return validReceipt(r) && j.Phase == Observed && r.TransactionID == j.TransactionID && r.MachineUID == j.Machine.UID && r.TalosConfigUID == j.TalosConfig.UID && r.InfrastructureUID == j.Infrastructure.UID && bytes.Equal(r.InputFreshness, j.InputFreshness) && j.CompletedAt != nil && j.CompletedAt.Equal(&r.CompletedAt)
}

// WriteJournal performs no-create UID/resourceVersion compare-and-swap against the original snapshot.
func WriteJournal(ctx context.Context, c client.Client, s *corev1.Secret, j *Journal, r *CompletionReceipt) error {
	if j == nil || s == nil || s.UID == "" || s.ResourceVersion == "" || !validateOwner(s, j.TalosConfig) {
		return safe("journal CAS identity")
	}
	old := &Journal{}
	if json.Unmarshal(s.Data[JournalKey], old) != nil || old.TransactionID != j.TransactionID {
		return safe("journal transaction change")
	}
	if len(s.Data[ReceiptKey]) > 0 {
		if r != nil {
			return safe("unconsumed receipt overwrite")
		}
		if !reflect.DeepEqual(old, j) {
			return safe("receipted journal immutable")
		}
	}
	if err := validateTransition(old, j); err != nil {
		return err
	}
	if r != nil && !receiptMatches(r, j) {
		return safe("receipt journal mismatch")
	}
	data, e := json.Marshal(j)
	if e != nil {
		return safe("journal encoding")
	}
	next := make(map[string][]byte, len(s.Data)+1)
	for k, v := range s.Data {
		next[k] = v
	}
	next[JournalKey] = data
	if r != nil {
		next[ReceiptKey], e = json.Marshal(r)
		if e != nil {
			return safe("receipt encoding")
		}
	}
	return PatchData(ctx, c, s, next)
}

// ErrSecretConflict is a compare-and-swap that lost: the Secret's UID or
// resourceVersion no longer matched the copy the write was based on.
var ErrSecretConflict = safe("Secret CAS conflict")

// PatchData is an exact UID/resourceVersion-guarded Secret update, never an upsert.
// Only a lost compare-and-swap is reported as ErrSecretConflict; a write cut short by
// its context, or rejected for any other reason, says so, so a slow caller is never
// mistaken for a concurrent writer.
func PatchData(ctx context.Context, c client.Client, s *corev1.Secret, next map[string][]byte) error {
	if s == nil || s.UID == "" || s.ResourceVersion == "" {
		return safe("Secret CAS identity")
	}
	ops := []map[string]any{{"op": "test", "path": "/metadata/uid", "value": s.UID}, {"op": "test", "path": "/metadata/resourceVersion", "value": s.ResourceVersion}, {"op": "replace", "path": "/data", "value": next}}
	payload, e := json.Marshal(ops)
	if e != nil {
		return safe("Secret CAS encoding")
	}
	err := c.Patch(ctx, s.DeepCopy(), client.RawPatch(types.JSONPatchType, payload))
	switch {
	case err == nil:
		return nil
	case ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
		return safe("Secret write interrupted by the caller's deadline")
	case apierrors.IsConflict(err) || apierrors.IsInvalid(err) || apierrors.IsNotFound(err):
		// A failed JSON patch "test" (UID or resourceVersion moved) is 422 Invalid.
		return ErrSecretConflict
	default:
		if reason := apierrors.ReasonForError(err); reason != "" {
			return safe(fmt.Sprintf("Secret write rejected (%s)", reason))
		}
		return safe("Secret write rejected")
	}
}

// Unissued reports whether no step of j was ever issued to the node: every step is
// still Planned and carries no issuance witness. Such a journal has had no effect on
// the node, so a failure before issuance is never an ambiguous outcome.
func Unissued(j *Journal) bool {
	if j == nil || len(j.Steps) == 0 {
		return false
	}
	for _, s := range j.Steps {
		if s.Phase != StepPlanned || s.IssuedAt != nil || s.IssuerID != "" {
			return false
		}
	}
	return true
}

// DiscardUnissuedJournal deletes a journal none of whose steps was issued, so the next
// UpdateMachine call plans afresh from current inputs. The stored journal (not the
// caller's copy) must be unissued and unreceipted; deletion is guarded by UID and
// resourceVersion, so a concurrent issuance (a CAS write) makes it fail instead.
func DiscardUnissuedJournal(ctx context.Context, c client.Client, s *corev1.Secret, transactionID string) error {
	stored := &Journal{}
	if s == nil || json.Unmarshal(s.Data[JournalKey], stored) != nil || stored.TransactionID != transactionID || len(s.Data[ReceiptKey]) > 0 || !Unissued(stored) {
		return safe("unissued journal discard")
	}
	return deleteJournal(ctx, c, s, stored)
}

// deleteJournal removes the journal Secret exactly as read: owner-checked, UID and
// resourceVersion guarded, so a concurrent write makes it fail instead.
func deleteJournal(ctx context.Context, c client.Client, s *corev1.Secret, stored *Journal) error {
	if s.UID == "" || s.ResourceVersion == "" || !validateOwner(s, stored.TalosConfig) {
		return safe("journal discard identity")
	}
	uid, version := s.UID, s.ResourceVersion
	if c.Delete(ctx, s.DeepCopy(), client.Preconditions{UID: &uid, ResourceVersion: &version}) != nil {
		return safe("journal delete conflict")
	}
	return nil
}
