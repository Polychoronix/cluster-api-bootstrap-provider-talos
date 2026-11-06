// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package inplace

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	runtimehooks "sigs.k8s.io/cluster-api/api/runtime/hooks/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func journalAbsent(t *testing.T, f *durableFixture) bool {
	t.Helper()
	err := f.management.Get(t.Context(), client.ObjectKey{Namespace: f.config.Namespace, Name: f.config.Name + "-update-operation"}, &corev1.Secret{})
	return apierrors.IsNotFound(err)
}

// A planned journal that fails before any step was issued (here: the Node it was planned
// against was replaced) is discarded, not marked Ambiguous, and nothing reaches the node.
func TestUnissuedJournalFailureIsRetryableNotAmbiguous(t *testing.T) {
	f := newDurableFixture(t, false)
	f.poll(t)
	_, j := f.journal(t)
	require.True(t, op.Unissued(j))
	f.workloadNode.UID = types.UID("recreated-node")
	response := &runtimehooks.UpdateMachineResponse{}
	f.handler.DoUpdateMachine(t.Context(), f.request, response)
	require.Equal(t, runtimehooks.ResponseStatusFailure, response.Status)
	require.True(t, journalAbsent(t, f), "an unissued journal is discarded, never left Ambiguous")
	f.node.mu.Lock()
	defer f.node.mu.Unlock()
	require.Zero(t, f.node.applyCount)
}

// The craci.10 leftover: a journal marked Ambiguous although its only step was never
// issued. It is discarded and re-planned instead of demanding intervention.
func TestLegacyAmbiguousUnissuedJournalIsReplanned(t *testing.T) {
	f := newDurableFixture(t, false)
	f.poll(t)
	secret, j := f.journal(t)
	j.Phase = op.Ambiguous
	encoded, err := json.Marshal(j)
	require.NoError(t, err)
	next := map[string][]byte{}
	for k, v := range secret.Data {
		next[k] = v
	}
	next[op.JournalKey] = encoded
	require.NoError(t, op.PatchData(t.Context(), f.management, secret, next))
	_, stored := f.journal(t)
	require.Equal(t, op.Ambiguous, stored.Phase)

	response := &runtimehooks.UpdateMachineResponse{}
	f.handler.DoUpdateMachine(t.Context(), f.request, response)
	require.Equal(t, runtimehooks.ResponseStatusFailure, response.Status)
	require.NotContains(t, response.Message, "intervention")
	require.True(t, journalAbsent(t, f))

	require.Positive(t, f.poll(t).RetryAfterSeconds, "the next call plans afresh")
	_, replanned := f.journal(t)
	require.Equal(t, op.Planned, replanned.Phase)
	require.Eventually(t, func() bool { return f.poll(t).RetryAfterSeconds == 0 }, 5*time.Second, time.Millisecond)
	f.node.mu.Lock()
	defer f.node.mu.Unlock()
	require.Equal(t, 1, f.node.applyCount)
}

// Once a step was issued, a failure whose outcome cannot be verified stays Ambiguous.
func TestIssuedJournalFailureStaysAmbiguous(t *testing.T) {
	f := newDurableFixture(t, false)
	f.poll(t)
	secret, j := f.journal(t)
	issued := metav1.NewTime(time.Now().Add(-time.Minute).UTC().Truncate(time.Second))
	deadline := metav1.NewTime(time.Now().Add(time.Hour).UTC().Truncate(time.Second))
	j.Steps[0].Phase = op.Issued
	j.Steps[0].IssuerID = "lost-process"
	j.Steps[0].IssuedAt = &issued
	j.Steps[0].Deadline = &deadline
	j.Steps[0].IssuedBootID = "boot-old"
	require.NoError(t, op.WriteJournal(t.Context(), f.management, secret, j, nil))
	require.False(t, op.Unissued(j))
	f.workloadNode.UID = types.UID("recreated-node")
	response := &runtimehooks.UpdateMachineResponse{}
	f.handler.DoUpdateMachine(t.Context(), f.request, response)
	require.Equal(t, runtimehooks.ResponseStatusFailure, response.Status)
	_, j = f.journal(t)
	require.Equal(t, op.Ambiguous, j.Phase)
	require.Equal(t, op.StepAmbiguous, j.Steps[0].Phase)
}

func TestDiscardUnissuedJournalRefusesIssuedOrForeign(t *testing.T) {
	f := newDurableFixture(t, false)
	f.poll(t)
	secret, j := f.journal(t)
	require.Error(t, op.DiscardUnissuedJournal(t.Context(), f.management, secret, "other-transaction"))
	issued := metav1.NewTime(time.Now().UTC().Truncate(time.Second))
	deadline := metav1.NewTime(issued.Add(time.Hour))
	j.Steps[0].Phase = op.Issued
	j.Steps[0].IssuerID = "process"
	j.Steps[0].IssuedAt = &issued
	j.Steps[0].Deadline = &deadline
	j.Steps[0].IssuedBootID = "boot"
	require.NoError(t, op.WriteJournal(t.Context(), f.management, secret, j, nil))
	secret, _ = f.journal(t)
	err := op.DiscardUnissuedJournal(t.Context(), f.management, secret, j.TransactionID)
	require.Error(t, err)
	require.False(t, errors.Is(err, op.ErrNotFound))
	require.False(t, journalAbsent(t, f))
}

// The craci.9 live path: the operation was planned, then validation of the activated
// management failed before issuance (record deleted for re-activation). rejectStalePending
// used to mark the planned journal Ambiguous; it is now discarded and nothing is applied.
func TestPlannedJournalWithFailedActivationIsDiscarded(t *testing.T) {
	f := newDurableFixture(t, false)
	f.poll(t)
	_, j := f.journal(t)
	require.Equal(t, op.Planned, j.Phase)
	activation := &corev1.Secret{}
	require.NoError(t, f.management.Get(t.Context(), client.ObjectKey{Namespace: f.config.Namespace, Name: f.config.Name + "-management"}, activation))
	require.NoError(t, f.management.Delete(t.Context(), activation))
	response := &runtimehooks.UpdateMachineResponse{}
	f.handler.DoUpdateMachine(t.Context(), f.request, response)
	require.Equal(t, runtimehooks.ResponseStatusFailure, response.Status)
	require.True(t, journalAbsent(t, f))
	f.node.mu.Lock()
	defer f.node.mu.Unlock()
	require.Zero(t, f.node.applyCount)
}
