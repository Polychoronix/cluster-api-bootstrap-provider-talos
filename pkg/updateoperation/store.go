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
	return r != nil && r.TransactionID != "" && r.MachineUID != "" && r.TalosConfigUID != "" && r.InfrastructureUID != "" && r.Phase == Observed && len(r.InputFreshness) > 0 && !r.CompletedAt.IsZero()
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
		if o.Name == i.Owner.Name && o.Kind == i.Owner.Kind && o.UID == i.Owner.UID {
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
	return s, j, nil
}
func CreateJournal(ctx context.Context, c client.Client, namespace string, j *Journal) (*corev1.Secret, error) {
	if j == nil || j.TransactionID == "" || !validIdentity(j.Machine) || !validIdentity(j.TalosConfig) || !validIdentity(j.Infrastructure) || j.NodeUID == "" || j.NodeProviderID == "" || len(j.InputFreshness) == 0 {
		return nil, safe("journal initial identity")
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
		if old.TransactionID != j.TransactionID {
			return safe("unconsumed transaction")
		}
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

// PatchData is an exact UID/resourceVersion-guarded Secret update, never an upsert.
func PatchData(ctx context.Context, c client.Client, s *corev1.Secret, next map[string][]byte) error {
	if s == nil || s.UID == "" || s.ResourceVersion == "" {
		return safe("Secret CAS identity")
	}
	ops := []map[string]any{{"op": "test", "path": "/metadata/uid", "value": s.UID}, {"op": "test", "path": "/metadata/resourceVersion", "value": s.ResourceVersion}, {"op": "replace", "path": "/data", "value": next}}
	payload, e := json.Marshal(ops)
	if e != nil {
		return safe("Secret CAS encoding")
	}
	if c.Patch(ctx, s.DeepCopy(), client.RawPatch(types.JSONPatchType, payload)) != nil {
		return safe("Secret CAS conflict")
	}
	return nil
}
