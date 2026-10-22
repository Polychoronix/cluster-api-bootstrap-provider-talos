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
	runtimehooks "sigs.k8s.io/cluster-api/api/runtime/hooks/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"testing"
	"time"
)

type acknowledgmentFaultClient struct {
	client.Client
	failed bool
}

func (c *acknowledgmentFaultClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	data, err := patch.Data(obj)
	if err != nil {
		return err
	}
	var operations []struct {
		Value map[string][]byte `json:"value"`
	}
	_ = json.Unmarshal(data, &operations)
	for _, operation := range operations {
		j := &op.Journal{}
		if json.Unmarshal(operation.Value[op.JournalKey], j) == nil {
			for _, step := range j.Steps {
				if step.Phase == op.Acknowledged {
					c.failed = true
					return errors.New("lost-ack-persistence")
				}
			}
		}
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}
func TestAcknowledgmentPersistenceLossCannotRepeatIssuedApply(t *testing.T) {
	f := newDurableFixture(t, false)
	fault := &acknowledgmentFaultClient{Client: f.handler.client}
	f.handler.client = fault
	require.Eventually(t, func() bool {
		response := f.poll(t)
		require.Positive(t, response.RetryAfterSeconds)
		return fault.failed
	}, 5*time.Second, time.Millisecond)
	_, j := f.journal(t)
	require.Equal(t, op.Issued, j.Steps[0].Phase)
	for range 3 {
		require.Positive(t, f.poll(t).RetryAfterSeconds)
	}
	f.node.mu.Lock()
	defer f.node.mu.Unlock()
	require.Equal(t, 1, f.node.applyCount)
}
func TestAcknowledgedRecoveryDeadlineStopsWithoutAnotherApply(t *testing.T) {
	f := newDurableFixture(t, false)
	f.poll(t)
	secret, j := f.journal(t)
	issued := metav1.NewTime(time.Now().Add(-time.Hour).UTC().Truncate(time.Second))
	deadline := metav1.NewTime(time.Now().Add(-time.Minute).UTC().Truncate(time.Second))
	j.Steps[0].Phase = op.Acknowledged
	j.Steps[0].ApplyOutcome = op.NoReboot
	j.Steps[0].IssuerID = "old-process"
	j.Steps[0].IssuedAt = &issued
	j.Steps[0].Deadline = &deadline
	j.Steps[0].IssuedBootID = "boot-old"
	require.NoError(t, op.WriteJournal(t.Context(), f.management, secret, j, nil))
	response := &runtimehooks.UpdateMachineResponse{}
	f.handler.DoUpdateMachine(t.Context(), f.request, response)
	require.Equal(t, runtimehooks.ResponseStatusFailure, response.Status)
	secret, j = f.journal(t)
	require.Equal(t, op.Ambiguous, j.Phase)
	require.Empty(t, secret.Data[op.ReceiptKey])
	f.node.mu.Lock()
	defer f.node.mu.Unlock()
	require.Zero(t, f.node.applyCount)
}

type acceptedIntentFaultClient struct {
	client.Client
	failed bool
}

func (c *acceptedIntentFaultClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if !c.failed && obj.GetName() == "bootstrap" {
		c.failed = true
		return errors.New("accepted-intent-CAS-loss")
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}
func TestAcceptedIntentFailureDoesNotPublishReceiptOrReissue(t *testing.T) {
	f := newDurableFixture(t, false)
	fault := &acceptedIntentFaultClient{Client: f.handler.client}
	f.handler.client = fault
	require.Eventually(t, func() bool {
		response := &runtimehooks.UpdateMachineResponse{}
		f.handler.DoUpdateMachine(t.Context(), f.request, response)
		return fault.failed
	}, 5*time.Second, time.Millisecond)
	secret, j := f.journal(t)
	require.Equal(t, op.Planned, j.Phase)
	require.Empty(t, secret.Data[op.ReceiptKey])
	output := &corev1.Secret{}
	require.NoError(t, f.management.Get(t.Context(), client.ObjectKeyFromObject(f.output), output))
	intent, err := op.DecodeAcceptedIntent(output.Data[op.AcceptedIntentKey])
	require.NoError(t, err)
	require.Empty(t, intent.TransactionID)
	require.Eventually(t, func() bool { return f.poll(t).RetryAfterSeconds == 0 }, 5*time.Second, time.Millisecond)
	f.node.mu.Lock()
	defer f.node.mu.Unlock()
	require.Equal(t, 1, f.node.applyCount)
}
