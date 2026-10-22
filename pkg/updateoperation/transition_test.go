// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation_test

import (
	"context"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"testing"
)

func TestFrozenPlanRejectsIntentMutationWithinTransaction(t *testing.T) {
	for name, mutate := range map[string]func(*op.Journal){"Machine": func(j *op.Journal) { j.Machine.UID = "replacement" }, "Infra": func(j *op.Journal) { j.Infrastructure.Kind = "Other" }, "Node": func(j *op.Journal) { j.NodeUID = "replacement" }, "config": func(j *op.Journal) { j.Configuration = []byte("new") }, "freshness": func(j *op.Journal) { j.InputFreshness = []byte("new") }, "image": func(j *op.Journal) { j.TargetImage.Reference = "other" }} {
		t.Run(name, func(t *testing.T) {
			j := operationJournal()
			s := operationSecret(t, j)
			c := &operationRecordingClient{Client: operationClient(t, s)}
			mutate(j)
			require.Error(t, op.WriteJournal(context.Background(), c, s, j, nil))
			require.Zero(t, c.patches)
		})
	}
}
func TestObservedReceiptRequiresMatchingJournalAndCannotChange(t *testing.T) {
	j := operationJournal()
	r := operationReceipt(j)
	completeJournal(j, r.CompletedAt)
	s := operationSecret(t, j)
	s.Data[op.ReceiptKey] = encodedOperation(t, r)
	c := operationClient(t, s)
	j.TargetImage.Reference = "corrupted"
	require.Error(t, op.WriteJournal(t.Context(), c, s, j, nil))
	j.Phase = op.Planned
	s.Data[op.JournalKey] = encodedOperation(t, j)
	_, err := op.ReadReceipt(t.Context(), operationClient(t, s), "tenant", j.TalosConfig, r.TransactionID)
	require.Error(t, err)
}
func TestConsumedReceiptAllowsNextTransactionWithoutDeletingJournal(t *testing.T) {
	j := operationJournal()
	r := operationReceipt(j)
	completeJournal(j, r.CompletedAt)
	s := operationSecret(t, j)
	s.Data[op.ReceiptKey] = encodedOperation(t, r)
	c := operationClient(t, s)
	next := operationJournal()
	next.TransactionID = "transaction-two"
	next.InputFreshness = []byte("next")
	require.Error(t, op.BeginNextJournal(t.Context(), c, s, next))
	require.NoError(t, op.ConsumeReceipt(t.Context(), c, s, j))
	live := &corev1.Secret{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(s), live))
	require.NoError(t, op.ConsumeReceipt(t.Context(), c, live, j))
	require.NoError(t, op.BeginNextJournal(t.Context(), c, live, next))
	after, got, err := op.ReadJournal(t.Context(), c, "tenant", next.TalosConfig)
	require.NoError(t, err)
	require.Equal(t, next.TransactionID, got.TransactionID)
	require.Empty(t, after.Data[op.ReceiptKey])
	require.NotEmpty(t, after.Data[op.LastReceiptKey])
	require.Equal(t, s.UID, after.UID)
}
func TestSchemaVersionAndOwnerControllerCannotBeOmitted(t *testing.T) {
	j := operationJournal()
	s := operationSecret(t, j)
	j.Version = ""
	s.Data[op.JournalKey] = encodedOperation(t, j)
	_, _, err := op.ReadJournal(t.Context(), operationClient(t, s), "tenant", j.TalosConfig)
	require.Error(t, err)
	intent := ownerOperationIntent()
	plan := ownerOperationSecret(t, intent)
	plan.OwnerReferences[0].Controller = nil
	_, err = op.ReadOwnerIntent(t.Context(), operationClient(t, plan), "tenant", op.OwnerPlanReference{Name: plan.Name, UID: plan.UID})
	require.Error(t, err)
	plan = ownerOperationSecret(t, intent)
	plan.OwnerReferences[0].APIVersion = "wrong.group/v1beta1"
	_, err = op.ReadOwnerIntent(t.Context(), operationClient(t, plan), "tenant", op.OwnerPlanReference{Name: plan.Name, UID: plan.UID})
	require.Error(t, err)
}
