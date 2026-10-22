// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package inplace

import (
	"context"
	"encoding/json"
	"errors"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	runtimehooks "sigs.k8s.io/cluster-api/api/runtime/hooks/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"testing"
	"time"
)

func TestIssuedUnknownAfterRestartStopsAtDeadlineWithoutReissue(t *testing.T) {
	f := newDurableFixture(t, false)
	require.Positive(t, f.poll(t).RetryAfterSeconds)
	secret, j := f.journal(t)
	issued := metav1.NewTime(time.Now().Add(-time.Hour).UTC().Truncate(time.Second))
	deadline := metav1.NewTime(time.Now().Add(-time.Minute).UTC().Truncate(time.Second))
	j.Steps[0].Phase = op.Issued
	j.Steps[0].IssuerID = "lost-process"
	j.Steps[0].IssuedAt = &issued
	j.Steps[0].Deadline = &deadline
	j.Steps[0].IssuedBootID = "boot-old"
	require.NoError(t, op.WriteJournal(t.Context(), f.management, secret, j, nil))
	restarted := NewHandler(f.handler.client, f.management, f.handler.newNodeClient)
	response := &runtimehooks.UpdateMachineResponse{}
	restarted.DoUpdateMachine(t.Context(), f.request, response)
	require.Equal(t, runtimehooks.ResponseStatusFailure, response.Status)
	_, j = f.journal(t)
	require.Equal(t, op.Ambiguous, j.Phase)
	f.node.mu.Lock()
	defer f.node.mu.Unlock()
	require.Zero(t, f.node.applyCount)
}
func TestOtherHandlerDoesNotDeclareActiveIssuerAmbiguous(t *testing.T) {
	f := newDurableFixture(t, true)
	f.node.installGate = make(chan struct{})
	require.Eventually(t, func() bool { f.poll(t); f.node.mu.Lock(); defer f.node.mu.Unlock(); return f.node.installCount == 1 }, 5*time.Second, time.Millisecond)
	second := NewHandler(f.handler.client, f.management, f.handler.newNodeClient)
	response := &runtimehooks.UpdateMachineResponse{}
	second.DoUpdateMachine(t.Context(), f.request, response)
	require.Equal(t, runtimehooks.ResponseStatusSuccess, response.Status, response.Message)
	require.Positive(t, response.RetryAfterSeconds)
	_, j := f.journal(t)
	require.Equal(t, op.Planned, j.Phase)
	close(f.node.installGate)
}
func TestRebootConfigRequiresActualBootWitness(t *testing.T) {
	f := newDurableFixture(t, false)
	f.node.outcome = op.RebootRequired
	require.Eventually(t, func() bool { return f.poll(t).RetryAfterSeconds == 0 }, 5*time.Second, time.Millisecond)
	_, j := f.journal(t)
	require.Equal(t, op.RebootRequired, j.Steps[0].ApplyOutcome)
	require.NotEqual(t, j.Steps[0].IssuedBootID, j.Steps[0].ObservedBootID)
}
func TestUnchangedHistoricalImageDoesNotCorrectRuntimeDrift(t *testing.T) {
	f := newDurableFixture(t, false)
	f.node.mu.Lock()
	f.node.observation.OSVersion = "v1.13.9"
	f.node.mu.Unlock()
	require.Eventually(t, func() bool { return f.poll(t).RetryAfterSeconds == 0 }, 5*time.Second, time.Millisecond)
	f.node.mu.Lock()
	defer f.node.mu.Unlock()
	require.Zero(t, f.node.pullCount)
	require.Zero(t, f.node.installCount)
	require.Zero(t, f.node.rebootCount)
}

type failReceiptClient struct {
	client.Client
	failed bool
}

func (c *failReceiptClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if !c.failed {
		data, err := patch.Data(obj)
		if err != nil {
			return err
		}
		var operations []struct {
			Value map[string][]byte `json:"value"`
		}
		_ = json.Unmarshal(data, &operations)
		for _, operation := range operations {
			if len(operation.Value[op.ReceiptKey]) > 0 {
				c.failed = true
				return errors.New("lost-receipt-write")
			}
		}
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}
func TestReceiptFailureAfterAcceptedIntentDoesNotReissueOperations(t *testing.T) {
	f := newDurableFixture(t, false)
	fault := &failReceiptClient{Client: f.handler.client}
	f.handler.client = fault
	require.Eventually(t, func() bool {
		response := &runtimehooks.UpdateMachineResponse{}
		f.handler.DoUpdateMachine(t.Context(), f.request, response)
		return fault.failed
	}, 5*time.Second, time.Millisecond)
	output := &corev1.Secret{}
	require.NoError(t, f.management.Get(t.Context(), client.ObjectKeyFromObject(f.output), output))
	intent, err := op.DecodeAcceptedIntent(output.Data[op.AcceptedIntentKey])
	require.NoError(t, err)
	require.NotEmpty(t, intent.TransactionID)
	secret, j := f.journal(t)
	require.Empty(t, secret.Data[op.ReceiptKey])
	require.Equal(t, op.Planned, j.Phase)
	require.Eventually(t, func() bool { return f.poll(t).RetryAfterSeconds == 0 }, 5*time.Second, time.Millisecond)
	f.node.mu.Lock()
	defer f.node.mu.Unlock()
	require.Equal(t, 1, f.node.applyCount)
}
func TestRecreatedSameNameNodeNeverCompletes(t *testing.T) {
	f := newDurableFixture(t, false)
	f.poll(t)
	f.workloadNode.UID = types.UID("recreated-node")
	response := &runtimehooks.UpdateMachineResponse{}
	f.handler.DoUpdateMachine(t.Context(), f.request, response)
	require.Equal(t, runtimehooks.ResponseStatusFailure, response.Status)
	_, j := f.journal(t)
	require.Equal(t, op.Ambiguous, j.Phase)
}
func TestAdoptedOutputWithoutAcceptedIntentStopsBeforeApply(t *testing.T) {
	f := newDurableFixture(t, false)
	output := &corev1.Secret{}
	require.NoError(t, f.management.Get(t.Context(), client.ObjectKeyFromObject(f.output), output))
	delete(output.Data, op.AcceptedIntentKey)
	require.NoError(t, f.management.Update(t.Context(), output))
	response := &runtimehooks.UpdateMachineResponse{}
	f.handler.DoUpdateMachine(t.Context(), f.request, response)
	require.Equal(t, runtimehooks.ResponseStatusFailure, response.Status)
	f.node.mu.Lock()
	defer f.node.mu.Unlock()
	require.Zero(t, f.node.applyCount)
}
