// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package inplace

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtimehooks "sigs.k8s.io/cluster-api/api/runtime/hooks/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Settlement outcomes, logged once per transaction.
const (
	settledApplied     = "applied"
	settledNeverLanded = "never-landed"
	settledUnresolved  = "unresolved"
)

// settleAmbiguous decides an Ambiguous journal from what the node runs now. The live
// configuration equal to the operation's requested configuration (document by document,
// order-insensitive) proves the apply landed: once the normal recovery witnesses hold the
// journal completes as Observed with its receipt. A never-acknowledged apply whose node
// still runs the configuration recorded when the operation was planned never landed: the
// journal is discarded and re-planned. Anything else stays Ambiguous for intervention.
// The caller has already passed the identity guard and the pause check.
func (h *Handler) settleAmbiguous(ctx context.Context, s *operationSnapshot, secret *corev1.Secret, j *op.Journal, resp *runtimehooks.UpdateMachineResponse) error {
	index, err := op.AmbiguousApplyStep(j, time.Now())
	if err != nil {
		return h.unresolved(ctx, j, err.Error())
	}
	node, err := h.newNodeClient(ctx, client.ObjectKey{Namespace: s.cluster.Namespace, Name: s.cluster.Name}, j.Endpoint)
	if err != nil {
		return h.unresolved(ctx, j, "node not reachable")
	}
	observation, err := node.Observe(ctx)
	node.Close()
	if err != nil {
		return h.unresolved(ctx, j, "node not observable")
	}
	differing, err := op.ConfigurationDiff(observation.Configuration, j.Configuration)
	if err != nil {
		return h.unresolved(ctx, j, "live configuration unreadable")
	}
	if len(differing) == 0 {
		return h.settleApplied(ctx, s, secret, j, index, observation, resp)
	}
	if j.BaselineDigest != "" {
		live, err := op.ConfigurationDigest(observation.Configuration)
		if err == nil && live == j.BaselineDigest {
			if !op.ApplyNeverAcknowledged(j, index) {
				return h.unresolved(ctx, j, "node runs the pre-apply configuration although the apply was acknowledged")
			}
			if err := op.DiscardUnlandedJournal(ctx, h.client, secret, j.TransactionID, time.Now()); err != nil {
				return err
			}
			h.reportSettlement(ctx, j, settledNeverLanded, "node runs the pre-apply configuration; re-planned on the next call")
			respondRetry(resp, "ambiguous operation never landed; re-planning")
			return nil
		}
	}
	return h.unresolved(ctx, j, "live configuration matches neither the requested nor the pre-apply configuration; differs from the requested one in "+strings.Join(differing, ", "))
}

// settleApplied completes a journal whose apply observation proved landed. A
// never-acknowledged outcome is taken from the boot witness: an unchanged boot means the
// configuration was applied without a reboot, a changed one that the node rebooted since
// issuance. The normal recovery checks and image acceptance run before the single write.
func (h *Handler) settleApplied(ctx context.Context, s *operationSnapshot, secret *corev1.Secret, j *op.Journal, index int, observation NodeObservation, resp *runtimehooks.UpdateMachineResponse) error {
	for i, step := range j.Steps {
		if i != index && step.Phase != op.StepObserved {
			return h.unresolved(ctx, j, "the apply landed but later steps were never issued")
		}
	}
	if observation.BootID == "" {
		return h.unresolved(ctx, j, "node boot identity not observed")
	}
	next := *j
	next.Steps = append([]op.OperationStep(nil), j.Steps...)
	step := &next.Steps[index]
	if step.Phase != op.StepObserved {
		if step.ApplyOutcome != op.NoReboot && step.ApplyOutcome != op.RebootRequired {
			step.ApplyOutcome = op.NoReboot
			if observation.BootID != step.IssuedBootID {
				step.ApplyOutcome = op.RebootRequired
			}
		}
		if step.ObservedBootID == "" {
			step.ObservedBootID = observation.BootID
		}
		step.Phase = op.StepObserved
	}
	if err := h.recovered(ctx, s, &next, observation); err != nil {
		respondRetry(resp, "ambiguous operation applied; settling once recovered: "+err.Error())
		return nil
	}
	if err := h.guardIssued(ctx, s.machine.Namespace, &next); err != nil {
		if errors.Is(err, op.ErrWorkloadUnavailable) {
			respondRetry(resp, "waiting for the workload API: "+err.Error())
			return nil
		}
		return h.unresolved(ctx, j, "frozen identities changed: "+err.Error())
	}
	if err := h.acceptObservedImage(ctx, s, &next); err != nil {
		return err
	}
	now := metav1.NewTime(time.Now().UTC().Truncate(time.Second))
	next.Phase = op.Observed
	next.CompletedAt = &now
	receipt := &op.CompletionReceipt{Version: op.SchemaVersion, TransactionID: next.TransactionID, MachineUID: next.Machine.UID, TalosConfigUID: next.TalosConfig.UID, InfrastructureUID: next.Infrastructure.UID, InputFreshness: next.InputFreshness, Phase: op.Observed, CompletedAt: now}
	if err := op.SettleAmbiguousJournal(ctx, h.client, secret, &next, receipt); err != nil {
		return err
	}
	h.reportSettlement(ctx, j, settledApplied, "applyOutcome "+string(step.ApplyOutcome))
	resp.Status = runtimehooks.ResponseStatusSuccess
	resp.RetryAfterSeconds = 0
	return nil
}

// unresolved keeps the journal Ambiguous; every later call observes again.
func (h *Handler) unresolved(ctx context.Context, j *op.Journal, reason string) error {
	h.reportSettlement(ctx, j, settledUnresolved, reason)
	return fmt.Errorf("operation outcome ambiguous; intervention required: %s", reason)
}

// reportSettlement logs each distinct settlement outcome of a transaction once per process.
func (h *Handler) reportSettlement(ctx context.Context, j *op.Journal, outcome, detail string) {
	if _, seen := h.settlements.LoadOrStore(j.TransactionID+"\x00"+outcome+"\x00"+detail, struct{}{}); seen {
		return
	}
	ctrl.LoggerFrom(ctx).Info("ambiguous in-place operation settlement", "talosConfig", j.TalosConfig.Name, "transactionID", j.TransactionID, "outcome", outcome, "detail", detail)
}
