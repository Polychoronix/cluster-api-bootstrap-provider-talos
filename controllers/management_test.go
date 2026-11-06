// This Source Code Form is subject to the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package controllers

import (
	"context"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"testing"
)

func TestStandaloneBootstrapThenObservationDoesNotRequireSeedReceipt(t *testing.T) {
	f := newImageFactoryFixture(t, bootstrapv1.TalosConfigSpec{GenerateType: "worker", TalosVersion: "v1.14", Manage: true}, nil)
	machine := &clusterv1.Machine{}
	require.NoError(t, f.client.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: "machine-1"}, machine))
	machine.Spec.Bootstrap.ConfigRef = clusterv1.ContractVersionedObjectReference{APIGroup: bootstrapv1.GroupVersion.Group, Kind: "TalosConfig", Name: "machine-1"}
	require.NoError(t, f.client.Update(context.Background(), machine))
	cfg := f.config(t)
	cfg.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(machine, clusterv1.GroupVersion.WithKind("Machine"))}
	require.NoError(t, f.client.Update(context.Background(), cfg))
	require.NoError(t, f.reconcile(t))
	cfg = f.config(t)
	require.True(t, ptr.Deref(cfg.Status.Initialization.DataSecretCreated, false))
	seed := f.secret(t, cfg.Status.DataSecretName)
	require.NotContains(t, seed.Data, "acceptedIntent")
	require.Equal(t, cfg.UID, metav1.GetControllerOf(seed).UID)
	machine.Spec.Bootstrap.DataSecretName = ptr.To(cfg.Status.DataSecretName)
	require.NoError(t, f.client.Update(context.Background(), machine))
	for range 3 {
		_, err := f.reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cfg)})
		require.NoError(t, err)
		cfg = f.config(t)
		require.True(t, ptr.Deref(cfg.Status.Initialization.DataSecretCreated, false))
		for _, c := range cfg.Status.Conditions {
			require.NotEqual(t, "ManagementRequired", c.Reason)
		}
	}
	after := f.secret(t, seed.Name)
	require.Equal(t, seed.Data, after.Data)
}

// An observation that cannot complete must say which stage stopped it: the condition
// message carries the fixed stage name and the reason stays ObservationPending for
// precondition/transport failures.
func TestObservationFailureSurfacesStage(t *testing.T) {
	f := newImageFactoryFixture(t, bootstrapv1.TalosConfigSpec{GenerateType: "worker", TalosVersion: "v1.14", Manage: true}, nil)
	machine := &clusterv1.Machine{}
	require.NoError(t, f.client.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: "machine-1"}, machine))
	machine.Spec.Bootstrap.ConfigRef = clusterv1.ContractVersionedObjectReference{APIGroup: bootstrapv1.GroupVersion.Group, Kind: "TalosConfig", Name: "machine-1"}
	require.NoError(t, f.client.Update(context.Background(), machine))
	cfg := f.config(t)
	cfg.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(machine, clusterv1.GroupVersion.WithKind("Machine"))}
	require.NoError(t, f.client.Update(context.Background(), cfg))
	require.NoError(t, f.reconcile(t))
	cfg = f.config(t)
	require.True(t, ptr.Deref(cfg.Status.Initialization.DataSecretCreated, false))

	// A Machine that already has a Node makes the controller observe instead of bootstrap;
	// the fixture has no workload kubeconfig, so observation stops at a precondition.
	require.NoError(t, f.client.Get(context.Background(), client.ObjectKeyFromObject(machine), machine))
	machine.Spec.Bootstrap.DataSecretName = ptr.To(cfg.Status.DataSecretName)
	machine.Spec.ProviderID = "hcloud://bm-1"
	machine.Status.NodeRef = clusterv1.MachineNodeReference{Name: "machine-1"}
	require.NoError(t, f.client.Update(context.Background(), machine))

	_, err := f.reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cfg)})
	require.NoError(t, err)
	cfg = f.config(t)
	var activation *metav1.Condition
	for i := range cfg.Status.Conditions {
		if cfg.Status.Conditions[i].Type == bootstrapv1.ActivationReadyCondition {
			activation = &cfg.Status.Conditions[i]
		}
	}
	require.NotNil(t, activation)
	require.Equal(t, metav1.ConditionFalse, activation.Status)
	require.Equal(t, "ObservationPending", activation.Reason)
	require.Regexp(t, `^Talos management .+ failed$`, activation.Message)
}
