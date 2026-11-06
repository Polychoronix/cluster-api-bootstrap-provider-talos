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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	runtimehooks "sigs.k8s.io/cluster-api/api/runtime/hooks/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func withWatchdog(seed []byte, timeout string) []byte {
	return append(append([]byte(nil), seed...), []byte("\n---\napiVersion: v1alpha1\nkind: WatchdogTimerConfig\ndevice: /dev/watchdog0\ntimeout: "+timeout+"\n")...)
}

func (f *durableFixture) setLive(configuration []byte) {
	f.node.mu.Lock()
	defer f.node.mu.Unlock()
	f.node.observation.Configuration = append([]byte(nil), configuration...)
}

// plannedOnBaseline plans an operation while the node runs baseline, then records its
// Apply step as issued and ambiguous the way craci.10 left it.
func plannedOnBaseline(t *testing.T, f *durableFixture, baseline []byte, step op.OperationStep) *op.Journal {
	t.Helper()
	f.setLive(baseline)
	f.poll(t)
	secret, j := f.journal(t)
	digest, err := op.ConfigurationDigest(baseline)
	require.NoError(t, err)
	require.Equal(t, digest, j.BaselineDigest, "planning records the pre-apply configuration")
	step.Kind = op.Apply
	j.Steps[0] = step
	j.Phase = op.Ambiguous
	require.NoError(t, op.WriteJournal(t.Context(), f.management, secret, j, nil))
	_, j = f.journal(t)
	return j
}

func issuedStep(phase op.StepPhase, outcome op.ApplyOutcome, bootID string, deadline time.Time) op.OperationStep {
	issued := metav1.NewTime(deadline.Add(-30 * time.Minute).UTC().Truncate(time.Second))
	until := metav1.NewTime(deadline.UTC().Truncate(time.Second))
	return op.OperationStep{Phase: phase, ApplyOutcome: outcome, IssuerID: "craci.10-process", IssuedAt: &issued, Deadline: &until, IssuedBootID: bootID}
}

func (f *durableFixture) bootID() string {
	f.node.mu.Lock()
	defer f.node.mu.Unlock()
	return f.node.observation.BootID
}

// The apply landed (the node runs the requested configuration): the journal completes as
// Observed with its receipt without another apply, the outcome taken from the boot witness.
func TestAmbiguousApplyThatLandedIsSettledObserved(t *testing.T) {
	for _, tc := range []struct {
		name      string
		issuedOn  func(f *durableFixture) string
		wantApply op.ApplyOutcome
	}{
		{"same boot", (*durableFixture).bootID, op.NoReboot},
		{"rebooted since issuance", func(*durableFixture) string { return "boot-before-issuance" }, op.RebootRequired},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDurableFixture(t, false)
			desired := append([]byte(nil), f.output.Data["value"]...)
			ambiguous := plannedOnBaseline(t, f, withWatchdog(desired, "2m"), issuedStep(op.StepAmbiguous, op.Unknown, tc.issuedOn(f), time.Now().Add(-time.Minute)))
			f.setLive(desired)

			response := &runtimehooks.UpdateMachineResponse{}
			f.handler.DoUpdateMachine(t.Context(), f.request, response)
			require.Equal(t, runtimehooks.ResponseStatusSuccess, response.Status, response.Message)
			require.Zero(t, response.RetryAfterSeconds)
			_, j := f.journal(t)
			require.Equal(t, op.Observed, j.Phase)
			require.Equal(t, ambiguous.TransactionID, j.TransactionID)
			require.Equal(t, op.StepObserved, j.Steps[0].Phase)
			require.Equal(t, tc.wantApply, j.Steps[0].ApplyOutcome)
			require.Equal(t, ambiguous.Steps[0].IssuedAt, j.Steps[0].IssuedAt)
			require.Equal(t, f.bootID(), j.Steps[0].ObservedBootID)
			_, err := op.ReadReceipt(t.Context(), f.management, f.config.Namespace, j.TalosConfig, j.TransactionID)
			require.NoError(t, err)
			require.Zero(t, f.poll(t).RetryAfterSeconds)
			f.node.mu.Lock()
			defer f.node.mu.Unlock()
			require.Zero(t, f.node.applyCount)
		})
	}
}

// The node still runs the configuration recorded when the operation was planned and the
// apply was never acknowledged: it never landed, so the journal is discarded and re-planned.
func TestAmbiguousApplyThatNeverLandedIsReplanned(t *testing.T) {
	f := newDurableFixture(t, false)
	baseline := withWatchdog(f.output.Data["value"], "2m")
	ambiguous := plannedOnBaseline(t, f, baseline, issuedStep(op.StepAmbiguous, op.Unknown, f.bootID(), time.Now().Add(-time.Minute)))

	response := &runtimehooks.UpdateMachineResponse{}
	f.handler.DoUpdateMachine(t.Context(), f.request, response)
	require.Equal(t, runtimehooks.ResponseStatusSuccess, response.Status, response.Message)
	require.Positive(t, response.RetryAfterSeconds)
	require.Contains(t, response.Message, "never landed")
	require.True(t, journalAbsent(t, f))

	require.Positive(t, f.poll(t).RetryAfterSeconds)
	_, replanned := f.journal(t)
	require.NotEqual(t, ambiguous.TransactionID, replanned.TransactionID)
	require.Equal(t, op.Planned, replanned.Phase)
	require.Eventually(t, func() bool { return f.poll(t).RetryAfterSeconds == 0 }, 5*time.Second, time.Millisecond)
	f.node.mu.Lock()
	defer f.node.mu.Unlock()
	require.Equal(t, 1, f.node.applyCount)
}

// Anything observation cannot decide stays Ambiguous and names why.
func TestAmbiguousApplyUnresolvedStaysAmbiguous(t *testing.T) {
	for _, tc := range []struct {
		name   string
		step   func(f *durableFixture) op.OperationStep
		live   func(f *durableFixture, baseline []byte) []byte
		reason string
	}{
		{
			name: "matches neither",
			step: func(f *durableFixture) op.OperationStep {
				return issuedStep(op.StepAmbiguous, op.Unknown, f.bootID(), time.Now().Add(-time.Minute))
			},
			live:   func(f *durableFixture, _ []byte) []byte { return withWatchdog(f.output.Data["value"], "5m") },
			reason: "matches neither",
		},
		{
			name: "acknowledged apply but pre-apply configuration live",
			step: func(f *durableFixture) op.OperationStep {
				return issuedStep(op.StepAmbiguous, op.NoReboot, f.bootID(), time.Now().Add(-time.Minute))
			},
			live:   func(_ *durableFixture, baseline []byte) []byte { return baseline },
			reason: "although the apply was acknowledged",
		},
		{
			name: "issued apply still within its deadline",
			step: func(f *durableFixture) op.OperationStep {
				return issuedStep(op.StepAmbiguous, op.Unknown, f.bootID(), time.Now().Add(time.Hour))
			},
			live:   func(_ *durableFixture, baseline []byte) []byte { return baseline },
			reason: "in flight",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDurableFixture(t, false)
			baseline := withWatchdog(f.output.Data["value"], "2m")
			ambiguous := plannedOnBaseline(t, f, baseline, tc.step(f))
			f.setLive(tc.live(f, baseline))
			for range 2 {
				err := f.handler.executeUpdate(t.Context(), f.request, &runtimehooks.UpdateMachineResponse{})
				require.ErrorContains(t, err, "operation outcome ambiguous; intervention required")
				require.ErrorContains(t, err, tc.reason)
			}
			_, j := f.journal(t)
			require.Equal(t, ambiguous, j)
			f.node.mu.Lock()
			defer f.node.mu.Unlock()
			require.Zero(t, f.node.applyCount)
		})
	}
}

func TestAmbiguousNeitherNamesDifferingDocuments(t *testing.T) {
	f := newDurableFixture(t, false)
	baseline := withWatchdog(f.output.Data["value"], "2m")
	plannedOnBaseline(t, f, baseline, issuedStep(op.StepAmbiguous, op.Unknown, f.bootID(), time.Now().Add(-time.Minute)))
	f.setLive(withWatchdog(f.output.Data["value"], "5m"))
	err := f.handler.executeUpdate(t.Context(), f.request, &runtimehooks.UpdateMachineResponse{})
	require.ErrorContains(t, err, "differs from the requested one in v1alpha1/WatchdogTimerConfig")
}

// Pause and identity-class guards run before settlement and keep the journal untouched.
func TestAmbiguousSettlementRespectsPauseAndIdentity(t *testing.T) {
	f := newDurableFixture(t, false)
	desired := append([]byte(nil), f.output.Data["value"]...)
	ambiguous := plannedOnBaseline(t, f, withWatchdog(desired, "2m"), issuedStep(op.StepAmbiguous, op.Unknown, f.bootID(), time.Now().Add(-time.Minute)))
	f.setLive(desired)

	machine := &clusterv1.Machine{}
	require.NoError(t, f.management.Get(t.Context(), client.ObjectKeyFromObject(f.machine), machine))
	machine.Annotations[clusterv1.PausedAnnotation] = ""
	require.NoError(t, f.management.Update(t.Context(), machine))
	err := f.handler.executeUpdate(t.Context(), f.request, &runtimehooks.UpdateMachineResponse{})
	require.True(t, errors.Is(err, op.ErrManagementPaused), "%v", err)
	_, j := f.journal(t)
	require.Equal(t, ambiguous, j)

	delete(machine.Annotations, clusterv1.PausedAnnotation)
	require.NoError(t, f.management.Update(t.Context(), machine))
	f.workloadNode.UID = types.UID("recreated-node")
	err = f.handler.executeUpdate(t.Context(), f.request, &runtimehooks.UpdateMachineResponse{})
	require.ErrorContains(t, err, "operation ambiguous; intervention required")
	_, j = f.journal(t)
	require.Equal(t, op.Ambiguous, j.Phase)
	require.Equal(t, ambiguous.Steps, j.Steps)
}

// The settle transition accepts only an observed apply with its exact issuance witness.
func TestSettleAmbiguousJournalRefusesRewrittenIssuance(t *testing.T) {
	f := newDurableFixture(t, false)
	plannedOnBaseline(t, f, withWatchdog(f.output.Data["value"], "2m"), issuedStep(op.StepAmbiguous, op.Unknown, f.bootID(), time.Now().Add(-time.Minute)))
	secret, j := f.journal(t)
	now := metav1.NewTime(time.Now().UTC().Truncate(time.Second))
	next := *j
	next.Steps = append([]op.OperationStep(nil), j.Steps...)
	next.Steps[0].Phase = op.StepObserved
	next.Steps[0].ApplyOutcome = op.NoReboot
	next.Steps[0].ObservedBootID = f.bootID()
	next.Phase = op.Observed
	next.CompletedAt = &now
	receipt := &op.CompletionReceipt{Version: op.SchemaVersion, TransactionID: next.TransactionID, MachineUID: next.Machine.UID, TalosConfigUID: next.TalosConfig.UID, InfrastructureUID: next.Infrastructure.UID, InputFreshness: next.InputFreshness, Phase: op.Observed, CompletedAt: now}
	rewritten := next
	rewritten.Steps = append([]op.OperationStep(nil), next.Steps...)
	rewritten.Steps[0].IssuerID = "someone-else"
	require.Error(t, op.SettleAmbiguousJournal(t.Context(), f.management, secret, &rewritten, receipt))
	replanned := next
	replanned.BaselineDigest = "other"
	require.Error(t, op.SettleAmbiguousJournal(t.Context(), f.management, secret, &replanned, receipt))
	require.NoError(t, op.SettleAmbiguousJournal(t.Context(), f.management, secret, &next, receipt))
	secret, _ = f.journal(t)
	require.Error(t, op.SettleAmbiguousJournal(t.Context(), f.management, secret, &next, receipt), "an Observed journal is not ambiguous")
}

// A journal planned before craci.12 carries no baseline: a landed apply still settles,
// a node that does not run the requested configuration stays Ambiguous.
func TestLegacyAmbiguousJournalWithoutBaseline(t *testing.T) {
	f := newDurableFixture(t, false)
	desired := append([]byte(nil), f.output.Data["value"]...)
	baseline := withWatchdog(desired, "2m")
	plannedOnBaseline(t, f, baseline, issuedStep(op.StepAmbiguous, op.Unknown, f.bootID(), time.Now().Add(-time.Minute)))
	secret, j := f.journal(t)
	j.BaselineDigest = ""
	encoded, err := json.Marshal(j)
	require.NoError(t, err)
	next := map[string][]byte{}
	for k, v := range secret.Data {
		next[k] = v
	}
	next[op.JournalKey] = encoded
	require.NoError(t, op.PatchData(t.Context(), f.management, secret, next))

	err = f.handler.executeUpdate(t.Context(), f.request, &runtimehooks.UpdateMachineResponse{})
	require.ErrorContains(t, err, "matches neither")
	require.False(t, journalAbsent(t, f))

	f.setLive(desired)
	require.Zero(t, f.poll(t).RetryAfterSeconds)
	_, j = f.journal(t)
	require.Equal(t, op.Observed, j.Phase)
}
