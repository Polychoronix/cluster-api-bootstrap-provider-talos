// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package controllers

import (
	"context"
	"errors"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/patch"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"time"
)

// Bootstrap may precede a live Node; every requested update requires one activated observation.
func (r *TalosConfigReconciler) reconcileManagementEarly(ctx context.Context, tc *bootstrapv1.TalosConfig) (bool, ctrl.Result, error) {
	if r.APIReader == nil {
		return true, ctrl.Result{}, errors.New("Talos activation APIReader required")
	}
	if hasManagementPause(tc) {
		return true, ctrl.Result{}, r.managementPausedStatus(ctx, tc)
	}
	owner := metav1.GetControllerOf(tc)
	if owner == nil || owner.Kind != "Machine" || owner.APIVersion != clusterv1.GroupVersion.String() {
		return false, ctrl.Result{}, nil
	}
	m := &clusterv1.Machine{}
	if r.APIReader.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: owner.Name}, m) != nil || m.UID != owner.UID {
		return true, ctrl.Result{RequeueAfter: 2 * time.Second}, r.activationStatus(ctx, tc, false, "AssociationPending")
	}
	cluster := &clusterv1.Cluster{}
	if r.APIReader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: m.Spec.ClusterName}, cluster) != nil {
		return true, ctrl.Result{}, errors.New("Talos activation Cluster lookup failed")
	}
	if hasManagementPause(m) || hasManagementPause(cluster) || cluster.Spec.Paused != nil && *cluster.Spec.Paused {
		return true, ctrl.Result{}, r.managementPausedStatus(ctx, tc)
	}
	// Real first bootstrap is not an in-place operation and cannot require a Node that does not exist yet.
	if m.Spec.Bootstrap.DataSecretName == nil && !m.Status.NodeRef.IsDefined() {
		return false, ctrl.Result{}, nil
	}
	authority, manage, err := op.ManagementAuthorityForMachine(ctx, r.APIReader, m, tc)
	if err != nil {
		return true, ctrl.Result{RequeueAfter: 2 * time.Second}, r.activationStatus(ctx, tc, false, "AssociationPending")
	}
	if !m.Status.NodeRef.IsDefined() {
		if err := r.activationStatus(ctx, tc, false, "NodePending"); err != nil {
			return true, ctrl.Result{}, err
		}
		return bootstrapv1.IsInPlaceUpdate(tc), ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}
	if authority.Kind == "TalosControlPlane" {
		if !manage {
			return true, ctrl.Result{RequeueAfter: 2 * time.Second}, r.activationStatus(ctx, tc, false, "ConsentAbsent")
		}
		if err := op.ValidateManagedMachine(ctx, r.APIReader, m, tc); err != nil {
			return true, ctrl.Result{RequeueAfter: 2 * time.Second}, r.activationStatus(ctx, tc, false, "ActivationPending")
		}
		if err := r.activationStatusMessage(ctx, tc, true, "Activated", activatedBaselineMessage(op.MemberBaseline(ctx, r.APIReader, m, tc))); err != nil {
			return true, ctrl.Result{}, err
		}
		return !bootstrapv1.IsInPlaceUpdate(tc), ctrl.Result{}, nil
	}
	if state, err := op.ReadManagementStatus(ctx, r.APIReader, authority); err == nil && state.Activated && manage {
		if err = op.ValidateManagedMachine(ctx, r.APIReader, m, tc); err != nil {
			return true, ctrl.Result{RequeueAfter: 2 * time.Second}, r.activationStatus(ctx, tc, false, "ActivationInvalid")
		}
		if err = r.activationStatus(ctx, tc, true, "Activated"); err != nil {
			return true, ctrl.Result{}, err
		}
		return !bootstrapv1.IsInPlaceUpdate(tc), ctrl.Result{}, nil
	}
	request := op.ManagementObservationRequest{Authority: authority, Cluster: cluster, Manage: manage, Machines: []*clusterv1.Machine{m}, TalosConfigs: []*bootstrapv1.TalosConfig{tc}}
	observation, err := op.ObserveManagement(ctx, r.APIReader, request)
	if err != nil {
		reason := "ObservationPending"
		if op.IsBaselineMismatch(err) {
			reason = "BaselineMismatch"
		}
		return true, ctrl.Result{RequeueAfter: 2 * time.Second}, r.activationFailure(ctx, tc, reason, err)
	}
	if _, err = op.RecordManagementObservation(ctx, r.Client, r.APIReader, observation); err != nil {
		return true, ctrl.Result{}, err
	}
	if !manage {
		return true, ctrl.Result{RequeueAfter: 2 * time.Second}, r.activationStatus(ctx, tc, false, "ConsentAbsent")
	}
	if _, err = op.ActivateManagement(ctx, r.Client, r.APIReader, request); err != nil {
		return true, ctrl.Result{RequeueAfter: 2 * time.Second}, r.activationStatus(ctx, tc, false, "ActivationPending")
	}
	return true, ctrl.Result{}, r.activationStatus(ctx, tc, true, "Activated")
}
func hasManagementPause(o client.Object) bool {
	_, ok := o.GetAnnotations()[clusterv1.PausedAnnotation]
	return ok
}
func (r *TalosConfigReconciler) managementPausedStatus(ctx context.Context, tc *bootstrapv1.TalosConfig) error {
	helper, err := patch.NewHelper(tc, r.Client)
	if err != nil {
		return err
	}
	conditions.Set(tc, metav1.Condition{Type: clusterv1.PausedCondition, Status: metav1.ConditionTrue, Reason: clusterv1.PausedReason, ObservedGeneration: tc.Generation})
	return helper.Patch(ctx, tc, patch.WithOwnedConditions{Conditions: []string{clusterv1.PausedCondition}})
}
func (r *TalosConfigReconciler) activationStatus(ctx context.Context, tc *bootstrapv1.TalosConfig, active bool, reason string) error {
	return r.activationStatusMessage(ctx, tc, active, reason, "")
}

func (r *TalosConfigReconciler) activationStatusMessage(ctx context.Context, tc *bootstrapv1.TalosConfig, active bool, reason, message string) error {
	helper, err := patch.NewHelper(tc, r.Client)
	if err != nil {
		return err
	}
	status := metav1.ConditionFalse
	if active {
		status = metav1.ConditionTrue
	}
	conditions.Set(tc, metav1.Condition{Type: bootstrapv1.ActivationReadyCondition, Status: status, Reason: reason, Message: message, ObservedGeneration: tc.Generation})
	return helper.Patch(ctx, tc, patch.WithStatusObservedGeneration{}, patch.WithOwnedConditions{Conditions: []string{bootstrapv1.ActivationReadyCondition}})
}

// activatedBaselineMessage explains an activation whose node still runs its own
// TalosConfig's render rather than the authority's effective policy.
func activatedBaselineMessage(baseline string) string {
	if baseline == op.BaselineCurrent {
		return "live configuration matches the current TalosConfig render; the owner's in-place update converges it to the effective policy"
	}
	return ""
}

// activationFailure publishes why an observation did not complete. Stage errors carry only
// fixed stage names (pkg/updateoperation.StageError), so the message exposes no private
// input, configuration bytes or transport detail. The log line is emitted once per distinct
// message per object; the 2s requeue would otherwise repeat it indefinitely.
func (r *TalosConfigReconciler) activationFailure(ctx context.Context, tc *bootstrapv1.TalosConfig, reason string, cause error) error {
	message := cause.Error()
	if len(message) > 256 {
		message = message[:256]
	}
	if previous := conditions.Get(tc, bootstrapv1.ActivationReadyCondition); previous == nil || previous.Message != message || previous.Reason != reason {
		r.Log.Info("Talos observation not complete", "talosconfig", client.ObjectKeyFromObject(tc).String(), "reason", reason, "stage", message)
	}
	helper, err := patch.NewHelper(tc, r.Client)
	if err != nil {
		return err
	}
	conditions.Set(tc, metav1.Condition{Type: bootstrapv1.ActivationReadyCondition, Status: metav1.ConditionFalse, Reason: reason, Message: message, ObservedGeneration: tc.Generation})
	return helper.Patch(ctx, tc, patch.WithStatusObservedGeneration{}, patch.WithOwnedConditions{Conditions: []string{bootstrapv1.ActivationReadyCondition}})
}
