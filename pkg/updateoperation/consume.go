// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"bytes"
	"context"
	"encoding/json"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ConsumeReceipt records the exact completed receipt after the caller verifies the core
// cleanup handshake. It preserves journal and receipt; no new work is authorized here.
func ConsumeReceipt(ctx context.Context, c client.Client, s *corev1.Secret, j *Journal) error {
	if s == nil || j == nil || !validateOwner(s, j.TalosConfig) || validateJournal(j) != nil {
		return safe("receipt consume identity")
	}
	receipt := &CompletionReceipt{}
	if json.Unmarshal(s.Data[ReceiptKey], receipt) != nil || !receiptMatches(receipt, j) {
		return safe("receipt consume mismatch")
	}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return safe("receipt consume encoding")
	}
	if bytes.Equal(s.Data[ConsumedReceiptKey], encoded) {
		return nil
	}
	next := make(map[string][]byte, len(s.Data)+1)
	for key, value := range s.Data {
		next[key] = value
	}
	next[ConsumedReceiptKey] = encoded
	return PatchData(ctx, c, s, next)
}

// BeginNextJournal is the only transaction replacement path. The prior receipt must
// already be durably consumed and is retained as lastReceipt. UIDs cannot change.
func BeginNextJournal(ctx context.Context, c client.Client, s *corev1.Secret, j *Journal) error {
	if s == nil || j == nil || validateJournal(j) != nil || j.Phase != Planned || !validateOwner(s, j.TalosConfig) {
		return safe("next journal identity")
	}
	old := &Journal{}
	receipt := &CompletionReceipt{}
	consumed := &CompletionReceipt{}
	if json.Unmarshal(s.Data[JournalKey], old) != nil || validateJournal(old) != nil || old.Phase != Observed || json.Unmarshal(s.Data[ReceiptKey], receipt) != nil || !receiptMatches(receipt, old) || json.Unmarshal(s.Data[ConsumedReceiptKey], consumed) != nil || !receiptMatches(consumed, old) {
		return safe("prior receipt unconsumed")
	}
	if old.TransactionID == j.TransactionID || old.Machine.UID != j.Machine.UID || old.TalosConfig.UID != j.TalosConfig.UID || old.Infrastructure.UID != j.Infrastructure.UID || old.NodeUID != j.NodeUID || old.NodeProviderID != j.NodeProviderID {
		return safe("next transaction identity changed")
	}
	encoded, err := json.Marshal(j)
	if err != nil {
		return safe("next journal encoding")
	}
	next := make(map[string][]byte, len(s.Data))
	for key, value := range s.Data {
		next[key] = value
	}
	next[LastReceiptKey] = s.Data[ReceiptKey]
	delete(next, ReceiptKey)
	delete(next, ConsumedReceiptKey)
	next[JournalKey] = encoded
	return PatchData(ctx, c, s, next)
}
