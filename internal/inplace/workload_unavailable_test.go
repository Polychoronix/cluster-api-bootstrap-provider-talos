// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package inplace

import (
	"context"
	"errors"
	"testing"
	"time"

	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtimehooks "sigs.k8s.io/cluster-api/api/runtime/hooks/v1alpha1"
)

// An unreachable workload API during an issued transaction is waited out: the hook asks
// to be called again and the journal is neither marked Ambiguous nor discarded.
func TestWorkloadOutageDuringIssuedOperationWaits(t *testing.T) {
	f := newDurableFixture(t, false)
	f.poll(t)
	secret, j := f.journal(t)
	issued := metav1.NewTime(time.Now().Add(-time.Minute).UTC().Truncate(time.Second))
	deadline := metav1.NewTime(time.Now().Add(time.Hour).UTC().Truncate(time.Second))
	j.Steps[0].Phase = op.Issued
	j.Steps[0].IssuerID = "other-process"
	j.Steps[0].IssuedAt = &issued
	j.Steps[0].Deadline = &deadline
	j.Steps[0].IssuedBootID = "boot-old"
	require.NoError(t, op.WriteJournal(t.Context(), f.management, secret, j, nil))

	f.workloadDown.Store(true)
	for range 3 {
		response := &runtimehooks.UpdateMachineResponse{}
		f.handler.DoUpdateMachine(t.Context(), f.request, response)
		require.Equal(t, runtimehooks.ResponseStatusSuccess, response.Status, response.Message)
		require.Positive(t, response.RetryAfterSeconds)
		require.Contains(t, response.Message, "waiting for the workload API")
	}
	_, j = f.journal(t)
	require.Equal(t, op.Planned, j.Phase)
	require.Equal(t, op.Issued, j.Steps[0].Phase)
}

// A planned journal survives a workload outage too: nothing is discarded or re-planned.
func TestWorkloadOutageBeforeIssuanceKeepsPlan(t *testing.T) {
	f := newDurableFixture(t, false)
	f.poll(t)
	_, planned := f.journal(t)
	f.workloadDown.Store(true)
	response := &runtimehooks.UpdateMachineResponse{}
	f.handler.DoUpdateMachine(t.Context(), f.request, response)
	require.Equal(t, runtimehooks.ResponseStatusSuccess, response.Status, response.Message)
	require.Positive(t, response.RetryAfterSeconds)
	_, j := f.journal(t)
	require.Equal(t, planned.TransactionID, j.TransactionID)
	f.node.mu.Lock()
	defer f.node.mu.Unlock()
	require.Zero(t, f.node.applyCount)
}

// The issuing runner's guard waits for the workload API (bounded by the step deadline)
// instead of turning an unreachable API into an ambiguous outcome.
func TestIssuedGuardWaitsForWorkloadAPI(t *testing.T) {
	f := newDurableFixture(t, false)
	f.poll(t)
	_, j := f.journal(t)
	previous := workloadRetryInterval
	workloadRetryInterval = 10 * time.Millisecond
	t.Cleanup(func() { workloadRetryInterval = previous })

	f.workloadDown.Store(true)
	short, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	err := f.handler.guardIssuedWaiting(short, f.config.Namespace, j)
	require.True(t, errors.Is(err, op.ErrWorkloadUnavailable), "%v", err)

	go func() { time.Sleep(50 * time.Millisecond); f.workloadDown.Store(false) }()
	require.NoError(t, f.handler.guardIssuedWaiting(t.Context(), f.config.Namespace, j))
}
