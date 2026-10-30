// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package inplace

import (
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	runtimehooks "sigs.k8s.io/cluster-api/api/runtime/hooks/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"testing"
	"time"
)

func TestAcknowledgedSourceRotationStopsBeforeReceipt(t *testing.T) {
	f := newDurableFixture(t, false)
	require.Eventually(t, func() bool { f.poll(t); _, j := f.journal(t); return j.Steps[0].Phase == op.StepObserved }, 5*time.Second, time.Millisecond)
	bundle := &corev1.Secret{}
	require.NoError(t, f.management.Get(t.Context(), client.ObjectKey{Namespace: f.config.Namespace, Name: "cluster-talos"}, bundle))
	bundle.Data["rotation"] = []byte("changed")
	require.NoError(t, f.management.Update(t.Context(), bundle))
	response := &runtimehooks.UpdateMachineResponse{}
	f.handler.DoUpdateMachine(t.Context(), f.request, response)
	require.Equal(t, runtimehooks.ResponseStatusFailure, response.Status)
	secret, j := f.journal(t)
	require.Equal(t, op.Ambiguous, j.Phase)
	require.Empty(t, secret.Data[op.ReceiptKey])
	f.node.mu.Lock()
	defer f.node.mu.Unlock()
	require.Equal(t, 1, f.node.applyCount)
}
func TestAcceptedIntentRejectsSameUIDRewrittenOutput(t *testing.T) {
	f := newDurableFixture(t, false)
	f.poll(t)
	_, j := f.journal(t)
	s, ready, err := f.handler.snapshot(t.Context(), &f.request.Desired.Machine, f.request.Desired.BootstrapConfig)
	require.NoError(t, err)
	require.True(t, ready)
	output := &corev1.Secret{}
	require.NoError(t, f.management.Get(t.Context(), client.ObjectKeyFromObject(f.output), output))
	before := append([]byte(nil), output.Data[op.AcceptedIntentKey]...)
	output.Data["value"] = []byte("changed-private-data")
	output.Data[configinput.FreshnessKey] = []byte("new-request")
	require.NoError(t, f.management.Update(t.Context(), output))
	require.Error(t, f.handler.acceptIntent(t.Context(), s, j))
	require.NoError(t, f.management.Get(t.Context(), client.ObjectKeyFromObject(output), output))
	require.Equal(t, before, output.Data[op.AcceptedIntentKey])
}
func TestInitialAndConsumedNextRequestWaitWithoutAmbiguity(t *testing.T) {
	initial := newUpdateFixture(t, "v1.14.0", "installer:v1.14.0", "stale-hash", "")
	response := &runtimehooks.UpdateMachineResponse{}
	initial.handler.DoUpdateMachine(t.Context(), initial.request, response)
	require.Equal(t, runtimehooks.ResponseStatusSuccess, response.Status)
	require.Positive(t, response.RetryAfterSeconds)
	f := newDurableFixture(t, false)
	require.Eventually(t, func() bool { return f.poll(t).RetryAfterSeconds == 0 }, 5*time.Second, time.Millisecond)
	m := f.machine.DeepCopy()
	require.NoError(t, f.management.Get(t.Context(), client.ObjectKeyFromObject(m), m))
	delete(m.Annotations, clusterv1.UpdateInProgressAnnotation)
	require.NoError(t, f.management.Update(t.Context(), m))
	c := f.config.DeepCopy()
	require.NoError(t, f.management.Get(t.Context(), client.ObjectKeyFromObject(c), c))
	delete(c.Annotations, clusterv1.UpdateInProgressAnnotation)
	require.NoError(t, f.management.Update(t.Context(), c))
	require.NoError(t, f.handler.consumeMachineReceipt(t.Context(), m))
	c.Generation++
	c.Spec.ConfigRevision = "next-render-pending"
	c.Annotations[clusterv1.UpdateInProgressAnnotation] = ""
	require.NoError(t, f.management.Update(t.Context(), c))
	f.request.Desired.BootstrapConfig = runtime.RawExtension{Object: c}
	response = &runtimehooks.UpdateMachineResponse{}
	f.handler.DoUpdateMachine(t.Context(), f.request, response)
	require.Equal(t, runtimehooks.ResponseStatusSuccess, response.Status, response.Message)
	require.Positive(t, response.RetryAfterSeconds)
	_, j := f.journal(t)
	require.Equal(t, op.Observed, j.Phase)
}
