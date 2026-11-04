// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package controllers

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
)

type driftFixture struct {
	reconciler *TalosConfigReconciler
	client     client.Client
	tc         *bootstrapv1.TalosConfig
	machine    *clusterv1.Machine
	cluster    *clusterv1.Cluster
	calls      int
	report     *op.ConfigDriftReport
	err        error
}

func newDriftFixture(t *testing.T, interval time.Duration) *driftFixture {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, bootstrapv1.AddToScheme(scheme))
	require.NoError(t, clusterv1.AddToScheme(scheme))
	f := &driftFixture{
		tc:      &bootstrapv1.TalosConfig{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "node-0", UID: "tc-uid", Generation: 3}},
		machine: &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "node-0"}},
		cluster: &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "cluster"}},
	}
	f.client = fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.tc).WithStatusSubresource(f.tc).Build()
	f.reconciler = &TalosConfigReconciler{
		Client:                   f.client,
		APIReader:                f.client,
		Log:                      ctrl.Log.WithName("test"),
		Scheme:                   scheme,
		ConfigDriftCheckInterval: interval,
		DriftObserver: func(context.Context, client.Reader, *clusterv1.Machine, *bootstrapv1.TalosConfig, *clusterv1.Cluster) (*op.ConfigDriftReport, error) {
			f.calls++
			return f.report, f.err
		},
	}
	return f
}

func (f *driftFixture) run(t *testing.T) (ctrl.Result, *metav1.Condition) {
	t.Helper()
	tc := &bootstrapv1.TalosConfig{}
	require.NoError(t, f.client.Get(context.Background(), client.ObjectKeyFromObject(f.tc), tc))
	result := f.reconciler.reconcileConfigDrift(context.Background(), tc, f.machine, f.cluster)
	require.NoError(t, f.client.Get(context.Background(), client.ObjectKeyFromObject(f.tc), tc))
	return result, conditions.Get(tc, bootstrapv1.ConfigDriftCondition)
}

func TestConfigDriftConditionTransitions(t *testing.T) {
	f := newDriftFixture(t, 30*time.Minute)

	f.report = &op.ConfigDriftReport{}
	result, condition := f.run(t)
	require.Equal(t, 1, f.calls)
	require.Equal(t, 30*time.Minute, result.RequeueAfter)
	require.NotNil(t, condition)
	require.Equal(t, metav1.ConditionFalse, condition.Status)
	require.Equal(t, bootstrapv1.NoConfigDriftReason, condition.Reason)
	require.Equal(t, int64(3), condition.ObservedGeneration)

	// Within the interval the node is not dialed again; the requeue is the remainder.
	result, _ = f.run(t)
	require.Equal(t, 1, f.calls)
	require.Greater(t, result.RequeueAfter, 29*time.Minute)

	f.reconciler.driftChecked.Delete(f.tc.UID)
	f.report = &op.ConfigDriftReport{Differing: []string{"v1alpha1/KubeNodeConfig", "v1alpha1/RawVolumeConfig/ceph-data-osd"}}
	_, condition = f.run(t)
	require.Equal(t, 2, f.calls)
	require.Equal(t, metav1.ConditionTrue, condition.Status)
	require.Equal(t, bootstrapv1.ConfigDriftedReason, condition.Reason)
	require.Equal(t, "live configuration differs from the TalosConfig render in: v1alpha1/KubeNodeConfig, v1alpha1/RawVolumeConfig/ceph-data-osd", condition.Message)

	// An unreachable node is unknown, not drifted; the previous verdict is not kept.
	f.reconciler.driftChecked.Delete(f.tc.UID)
	f.report, f.err = nil, &op.StageError{Stage: "Talos transport"}
	_, condition = f.run(t)
	require.Equal(t, metav1.ConditionUnknown, condition.Status)
	require.Equal(t, bootstrapv1.DriftObservationPendingReason, condition.Reason)

	f.reconciler.driftChecked.Delete(f.tc.UID)
	f.report, f.err = nil, errors.New("render failed")
	_, condition = f.run(t)
	require.Equal(t, metav1.ConditionUnknown, condition.Status)

	f.reconciler.driftChecked.Delete(f.tc.UID)
	f.report, f.err = &op.ConfigDriftReport{}, nil
	_, condition = f.run(t)
	require.Equal(t, metav1.ConditionFalse, condition.Status)
}

func TestConfigDriftDisabledByZeroInterval(t *testing.T) {
	f := newDriftFixture(t, 0)
	result, condition := f.run(t)
	require.Zero(t, f.calls)
	require.Zero(t, result.RequeueAfter)
	require.Nil(t, condition)
}

// During an in-place update the owner converges drift through the transaction: the
// condition goes to observation pending (never a stale True that would offer the next
// update), and the timer resets so the node is re-observed as soon as it ends.
func TestConfigDriftPendingDuringInPlaceUpdateThenReobserved(t *testing.T) {
	f := newDriftFixture(t, time.Minute)
	f.report = &op.ConfigDriftReport{Differing: []string{"v1alpha1/KubeNodeConfig"}}
	_, condition := f.run(t)
	require.Equal(t, 1, f.calls)
	require.Equal(t, metav1.ConditionTrue, condition.Status)

	tc := &bootstrapv1.TalosConfig{}
	require.NoError(t, f.client.Get(context.Background(), client.ObjectKeyFromObject(f.tc), tc))
	tc.Annotations = map[string]string{clusterv1.UpdateInProgressAnnotation: ""}
	require.NoError(t, f.client.Update(context.Background(), tc))
	result, condition := f.run(t)
	require.Equal(t, 1, f.calls, "no observation while the transaction runs")
	require.Zero(t, result.RequeueAfter)
	require.Equal(t, metav1.ConditionUnknown, condition.Status)
	require.Equal(t, bootstrapv1.DriftObservationPendingReason, condition.Reason)
	require.Equal(t, "in-place update in progress; re-observed when it completes", condition.Message)

	require.NoError(t, f.client.Get(context.Background(), client.ObjectKeyFromObject(f.tc), tc))
	tc.Annotations = nil
	require.NoError(t, f.client.Update(context.Background(), tc))
	f.report = &op.ConfigDriftReport{}
	_, condition = f.run(t)
	require.Equal(t, 2, f.calls, "re-observed immediately, not after the interval")
	require.Equal(t, metav1.ConditionFalse, condition.Status)
}

// A paused TalosConfig (cluster.x-k8s.io/paused) is never activated or converged, but its
// drift is still observed and reported so the operator sees what an unpause converges.
func TestPausedTalosConfigReportsDriftWithoutActivating(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, bootstrapv1.AddToScheme(scheme))
	require.NoError(t, clusterv1.AddToScheme(scheme))
	cluster := &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "cluster"}}
	machine := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "node-0", UID: "machine-uid"}}
	machine.Spec.ClusterName = cluster.Name
	machine.Status.NodeRef = clusterv1.MachineNodeReference{Name: "node-0"}
	controller := true
	tc := &bootstrapv1.TalosConfig{ObjectMeta: metav1.ObjectMeta{
		Namespace:       "tenant",
		Name:            "node-0",
		UID:             "tc-uid",
		Annotations:     map[string]string{clusterv1.PausedAnnotation: "true"},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: clusterv1.GroupVersion.String(), Kind: "Machine", Name: machine.Name, UID: machine.UID, Controller: &controller}},
	}, Spec: bootstrapv1.TalosConfigSpec{GenerateType: "worker", TalosVersion: "v1.14", Manage: true}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, machine, tc).WithStatusSubresource(tc, machine).Build()
	calls := 0
	r := &TalosConfigReconciler{
		Client: c, APIReader: c, Log: ctrl.Log.WithName("test"), Scheme: scheme, ConfigDriftCheckInterval: time.Minute,
		DriftObserver: func(context.Context, client.Reader, *clusterv1.Machine, *bootstrapv1.TalosConfig, *clusterv1.Cluster) (*op.ConfigDriftReport, error) {
			calls++
			return &op.ConfigDriftReport{Differing: []string{"v1alpha1/KubeNodeConfig"}}, nil
		},
	}

	handled, result, err := r.reconcileManagementEarly(context.Background(), tc)
	require.NoError(t, err)
	require.True(t, handled, "paused: nothing else runs")
	require.Equal(t, time.Minute, result.RequeueAfter)
	require.Equal(t, 1, calls)

	got := &bootstrapv1.TalosConfig{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(tc), got))
	require.True(t, conditions.IsTrue(got, clusterv1.PausedCondition))
	drift := conditions.Get(got, bootstrapv1.ConfigDriftCondition)
	require.NotNil(t, drift)
	require.Equal(t, metav1.ConditionTrue, drift.Status)
	require.Nil(t, conditions.Get(got, bootstrapv1.ActivationReadyCondition), "no activation while paused")
}
