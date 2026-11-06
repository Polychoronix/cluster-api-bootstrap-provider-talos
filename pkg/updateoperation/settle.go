// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ConfigurationDiff lists the document identities (apiVersion/kind[/name], never content)
// on which two machine configurations differ, regardless of document order. It is the
// comparator management observation and drift reporting use.
func ConfigurationDiff(actual, desired []byte) ([]string, error) {
	return managementConfigDiff(actual, desired)
}

// ConfigurationDigest is an order-insensitive digest of a machine configuration's
// documents: two configurations with an empty ConfigurationDiff have the same digest. It
// is stored only in private operation journals.
func ConfigurationDigest(data []byte) (string, error) {
	documents, err := managementConfigDocuments(data)
	if err != nil {
		return "", err
	}
	keys := make([]string, 0, len(documents))
	for key := range documents {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	h := sha256.New()
	field := func(b []byte) {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(b)))
		h.Write(n[:])
		h.Write(b)
	}
	for _, key := range keys {
		field([]byte(key))
		field(documents[key])
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// AmbiguousApplyStep returns the index of the Apply step whose effect on the node an
// observation can settle, or why the Ambiguous journal cannot be settled that way. The
// step must have been issued; an issued step whose acknowledgment never arrived is only
// settled after its deadline, when no runner can still act on it.
func AmbiguousApplyStep(j *Journal, now time.Time) (int, error) {
	if j == nil || j.Phase != Ambiguous {
		return -1, fmt.Errorf("journal is not ambiguous")
	}
	index := -1
	for i, s := range j.Steps {
		if s.Kind == Apply {
			index = i
		}
	}
	if index < 0 {
		return -1, fmt.Errorf("no apply step")
	}
	s := j.Steps[index]
	switch s.Phase {
	case Issued, StepAmbiguous:
		if s.Deadline == nil || !now.After(s.Deadline.Time) {
			return -1, fmt.Errorf("issued apply may still be in flight until its deadline")
		}
	case Acknowledged, StepObserved:
	default:
		return -1, fmt.Errorf("apply step was never issued")
	}
	if s.IssuerID == "" || s.IssuedAt == nil || s.IssuedBootID == "" {
		return -1, fmt.Errorf("apply step issuance witness missing")
	}
	for i, other := range j.Steps {
		if i != index && other.Phase != StepPlanned && other.Phase != StepObserved {
			return -1, fmt.Errorf("a %s step outcome is unknown", other.Kind)
		}
	}
	return index, nil
}

// ApplyNeverAcknowledged reports whether no acknowledgment exists for the Apply step at
// index and nothing after it was issued: only then can a node still running the
// pre-apply configuration prove the apply never landed.
func ApplyNeverAcknowledged(j *Journal, index int) bool {
	if j == nil || index < 0 || index >= len(j.Steps) {
		return false
	}
	s := j.Steps[index]
	if (s.Phase != StepAmbiguous && s.Phase != Issued) || s.ApplyOutcome != Unknown {
		return false
	}
	for i, other := range j.Steps {
		if i != index && other.Phase != StepPlanned {
			return false
		}
	}
	return true
}

// SettleAmbiguousJournal completes an Ambiguous journal whose apply was proven by
// observation: it is the only transition out of Ambiguous. Every step becomes Observed;
// steps already Observed are unchanged, and the settled Apply step keeps its exact
// issuance witness and any acknowledged outcome. The receipt is written in the same
// compare-and-swap, so the journal is never Observed without it.
func SettleAmbiguousJournal(ctx context.Context, c client.Client, s *corev1.Secret, next *Journal, r *CompletionReceipt) error {
	if next == nil || s == nil || s.UID == "" || s.ResourceVersion == "" || !validateOwner(s, next.TalosConfig) {
		return safe("settled journal identity")
	}
	if len(s.Data[ReceiptKey]) > 0 {
		return safe("settled journal already receipted")
	}
	old := &Journal{}
	if json.Unmarshal(s.Data[JournalKey], old) != nil || validateJournal(old) != nil || old.TransactionID != next.TransactionID || old.Phase != Ambiguous {
		return safe("settled journal state")
	}
	if validateJournal(next) != nil || next.Phase != Observed || !receiptMatches(r, next) {
		return safe("settled journal completion")
	}
	if !reflect.DeepEqual(frozenPlan(old), frozenPlan(next)) || len(old.Steps) != len(next.Steps) {
		return safe("frozen operation intent changed")
	}
	for i, a := range old.Steps {
		b := next.Steps[i]
		if a.Phase == StepObserved {
			if !reflect.DeepEqual(a, b) {
				return safe("journal observed step changed")
			}
			continue
		}
		if a.Kind != Apply || b.Kind != Apply || b.Phase != StepObserved || (a.Phase != StepAmbiguous && a.Phase != Issued && a.Phase != Acknowledged) {
			return safe("settled step not an observed apply")
		}
		if a.IssuerID != b.IssuerID || !sameTime(a.IssuedAt, b.IssuedAt) || !sameTime(a.Deadline, b.Deadline) || a.IssuedBootID != b.IssuedBootID {
			return safe("journal issuer changed")
		}
		if a.ApplyOutcome != Unknown && a.ApplyOutcome != b.ApplyOutcome || b.ApplyOutcome != NoReboot && b.ApplyOutcome != RebootRequired {
			return safe("settled apply outcome")
		}
		if b.ObservedBootID == "" || a.ObservedBootID != "" && a.ObservedBootID != b.ObservedBootID {
			return safe("settled boot witness")
		}
	}
	data, err := json.Marshal(next)
	if err != nil {
		return safe("journal encoding")
	}
	receipt, err := json.Marshal(r)
	if err != nil {
		return safe("receipt encoding")
	}
	updated := make(map[string][]byte, len(s.Data)+2)
	for k, v := range s.Data {
		updated[k] = v
	}
	updated[JournalKey], updated[ReceiptKey] = data, receipt
	return PatchData(ctx, c, s, updated)
}

// DiscardUnlandedJournal deletes an Ambiguous journal whose Apply step was never
// acknowledged and whose node still runs the pre-apply configuration: the apply never
// landed, so the next UpdateMachine call plans afresh. The stored journal is re-checked
// and the deletion is UID and resourceVersion guarded.
func DiscardUnlandedJournal(ctx context.Context, c client.Client, s *corev1.Secret, transactionID string, now time.Time) error {
	stored := &Journal{}
	if s == nil || json.Unmarshal(s.Data[JournalKey], stored) != nil || stored.TransactionID != transactionID || len(s.Data[ReceiptKey]) > 0 {
		return safe("unlanded journal discard")
	}
	index, err := AmbiguousApplyStep(stored, now)
	if err != nil || !ApplyNeverAcknowledged(stored, index) {
		return safe("unlanded journal discard")
	}
	return deleteJournal(ctx, c, s, stored)
}
