// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package inplace

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	runtimehooks "sigs.k8s.io/cluster-api/api/runtime/hooks/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (f *durableFixture) evaluationsIdle() bool {
	f.handler.evaluationsMu.Lock()
	defer f.handler.evaluationsMu.Unlock()
	return len(f.handler.evaluations) == 0
}

// Cluster API cancels a hook request after its timeout. The evaluation it started must
// still finish its durable step instead of failing every write on the cancelled context.
func TestUpdateEvaluationOutlivesTheHookRequest(t *testing.T) {
	f := newDurableFixture(t, false)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	response := &runtimehooks.UpdateMachineResponse{}
	f.handler.DoUpdateMachine(ctx, f.request, response)
	require.Equal(t, runtimehooks.ResponseStatusSuccess, response.Status, response.Message)
	require.Eventually(t, f.evaluationsIdle, 5*time.Second, time.Millisecond)
	_, j := f.journal(t)
	require.Equal(t, op.Planned, j.Phase, "the operation was planned although the request was already gone")
}

// A call arriving while an evaluation for the same Machine runs joins it: one
// evaluation, every waiting caller gets its outcome, a caller whose request ends first
// is asked to call again.
func TestUpdateEvaluationsAreSingleFlightPerMachine(t *testing.T) {
	f := newDurableFixture(t, false)
	gate := make(chan struct{})
	f.node.mu.Lock()
	f.node.observeGate = gate
	f.node.mu.Unlock()

	first := &runtimehooks.UpdateMachineResponse{}
	firstDone := make(chan struct{})
	go func() { f.handler.DoUpdateMachine(t.Context(), f.request, first); close(firstDone) }()
	require.Eventually(t, func() bool { f.node.mu.Lock(); defer f.node.mu.Unlock(); return f.node.observeCount == 1 }, 5*time.Second, time.Millisecond)

	short, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	impatient := &runtimehooks.UpdateMachineResponse{}
	f.handler.DoUpdateMachine(short, f.request, impatient)
	require.Equal(t, runtimehooks.ResponseStatusSuccess, impatient.Status)
	require.Positive(t, impatient.RetryAfterSeconds)
	require.Contains(t, impatient.Message, "continues after the hook request ended")

	joined := &runtimehooks.UpdateMachineResponse{}
	joinedDone := make(chan struct{})
	go func() { f.handler.DoUpdateMachine(t.Context(), f.request, joined); close(joinedDone) }()
	time.Sleep(20 * time.Millisecond)
	close(gate)
	<-firstDone
	<-joinedDone
	require.Equal(t, first, joined)
	require.Equal(t, "operation planned", first.Message)
	f.node.mu.Lock()
	defer f.node.mu.Unlock()
	require.Equal(t, 1, f.node.observeCount, "the joined calls did not evaluate again")
}

// journalBumpingClient changes the journal Secret's metadata (and so its
// resourceVersion) right before the first management-record write, i.e. between the
// hook's initial journal read and its completion write.
type journalBumpingClient struct {
	client.Client
	journal client.ObjectKey
	bumped  atomic.Bool
}

func (c *journalBumpingClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if strings.HasSuffix(obj.GetName(), "-management") && c.bumped.CompareAndSwap(false, true) {
		secret := &corev1.Secret{}
		if err := c.Client.Get(ctx, c.journal, secret); err != nil {
			return err
		}
		if secret.Labels == nil {
			secret.Labels = map[string]string{}
		}
		secret.Labels["touched"] = "metadata-only"
		if err := c.Client.Update(ctx, secret); err != nil {
			return err
		}
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

// The completion write compares-and-swaps against the journal as it is when written,
// so a resourceVersion that moved during the (long) completion evaluation without the
// journal changing does not fail it.
func TestCompletionCASUsesFreshJournal(t *testing.T) {
	f := newDurableFixture(t, false)
	bumping := &journalBumpingClient{Client: f.handler.client, journal: client.ObjectKey{Namespace: f.config.Namespace, Name: f.config.Name + "-update-operation"}}
	f.handler.client = bumping
	require.Eventually(t, func() bool { return f.poll(t).RetryAfterSeconds == 0 }, 5*time.Second, time.Millisecond)
	require.True(t, bumping.bumped.Load(), "the journal was touched during completion")
	_, j := f.journal(t)
	require.Equal(t, op.Observed, j.Phase)
	_, err := op.ReadReceipt(t.Context(), f.management, f.config.Namespace, j.TalosConfig, j.TransactionID)
	require.NoError(t, err)
}
