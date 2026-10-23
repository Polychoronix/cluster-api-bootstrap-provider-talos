// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package controllers

import (
	"context"
	"errors"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/patch"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// reconcileManagementEarly precedes normal bootstrap status/deletion/client
// work. It only observes and writes its own status/private snapshot. True means
// normal rendering must not run on this pass.
func (r *TalosConfigReconciler) reconcileManagementEarly(ctx context.Context, tc *bootstrapv1.TalosConfig) (bool, ctrl.Result, error) {
	if r.APIReader == nil {
		if tc.Spec.Management != nil {
			return true, ctrl.Result{}, errors.New("Talos management APIReader required")
		}
		return false, ctrl.Result{}, nil
	}
	if _, paused := tc.Annotations[clusterv1.PausedAnnotation]; paused {
		return true, ctrl.Result{}, r.managementPausedStatus(ctx, tc)
	}
	if tc.Spec.Management != nil && len(tc.Spec.Management.Machines) == 1 {
		pin := tc.Spec.Management.Machines[0]
		declared := &clusterv1.Machine{}
		if r.APIReader.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: pin.Name}, declared) == nil && declared.UID == pin.UID {
			if hasManagementPause(declared) {
				return true, ctrl.Result{}, r.managementPausedStatus(ctx, tc)
			}
			cluster := &clusterv1.Cluster{}
			if r.APIReader.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: declared.Spec.ClusterName}, cluster) == nil && ((cluster.Spec.Paused != nil && *cluster.Spec.Paused) || hasManagementPause(cluster)) {
				return true, ctrl.Result{}, r.managementPausedStatus(ctx, tc)
			}
		}
	}
	owner := metav1.GetControllerOf(tc)
	if owner == nil || owner.Kind != "Machine" || owner.APIVersion != clusterv1.GroupVersion.String() {
		if tc.Spec.Management != nil {
			return true, ctrl.Result{}, r.managementStatus(ctx, tc, nil, false, false, "AssociationPending")
		}
		return false, ctrl.Result{}, nil
	}
	m := &clusterv1.Machine{}
	if r.APIReader.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: owner.Name}, m) != nil || m.UID != owner.UID {
		if tc.Spec.Management != nil {
			return true, ctrl.Result{}, r.managementStatus(ctx, tc, nil, false, false, "AssociationPending")
		}
		return false, ctrl.Result{}, nil
	}
	cluster := &clusterv1.Cluster{}
	if r.APIReader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: m.Spec.ClusterName}, cluster) != nil {
		return true, ctrl.Result{}, errors.New("Talos management Cluster lookup failed")
	}
	if (cluster.Spec.Paused != nil && *cluster.Spec.Paused) || hasManagementPause(cluster) || hasManagementPause(m) {
		return true, ctrl.Result{}, r.managementPausedStatus(ctx, tc)
	}
	a, spec, e := op.ManagementAuthorityForMachine(ctx, r.APIReader, m, tc)
	if errors.Is(e, op.ErrManagementPaused) {
		return true, ctrl.Result{}, r.managementPausedStatus(ctx, tc)
	}
	if e != nil {
		if tc.Spec.Management != nil || tc.Status.Management != nil {
			return true, ctrl.Result{}, r.managementStatus(ctx, tc, nil, false, false, "AuthorityInvalid")
		}
		// Normal fresh CP creation may not yet have the TCP controller attached.
		return false, ctrl.Result{}, nil
	}
	if spec == nil {
		if tc.Status.Management != nil {
			return true, ctrl.Result{}, r.managementStatus(ctx, tc, nil, false, false, "ManagementDeclarationMissing")
		}
		// A custom/imported original seed cannot select normal generation merely by
		// omitting management. A CABPT-owned seed remains the existing fresh path.
		if m.Spec.Bootstrap.DataSecretName != nil && m.Status.NodeRef.IsDefined() {
			seed := &corev1.Secret{}
			if r.APIReader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: *m.Spec.Bootstrap.DataSecretName}, seed) != nil {
				return true, ctrl.Result{}, errors.New("Talos management original seed lookup failed")
			}
			ref := metav1.GetControllerOf(seed)
			if ref == nil || ref.Kind != "TalosConfig" || ref.UID != tc.UID {
				return true, ctrl.Result{}, r.managementStatus(ctx, tc, nil, false, false, "ManagementRequired")
			}
		}
		return false, ctrl.Result{}, nil
	}
	if a.Kind == "TalosControlPlane" {
		if spec.Mode != bootstrapv1.ManagementModeManaged {
			return true, ctrl.Result{}, r.managementStatus(ctx, tc, nil, false, false, "AggregateManagementPending")
		}
		if err := op.ValidateManagedMachine(ctx, r.APIReader, m, tc); err != nil {
			if errors.Is(err, op.ErrManagementPaused) {
				return true, ctrl.Result{}, r.managementPausedStatus(ctx, tc)
			}
			return true, ctrl.Result{}, r.managementStatus(ctx, tc, nil, false, false, "AggregateManagementPending")
		}
		tc.Status.Management = &bootstrapv1.ManagementStatus{ActivatedRevision: spec.InventoryRevision}
		return !bootstrapv1.IsInPlaceUpdate(tc), ctrl.Result{}, nil
	}
	request := op.ManagementObservationRequest{Authority: a, Cluster: cluster, Spec: spec, Machines: []*clusterv1.Machine{m}, TalosConfigs: []*bootstrapv1.TalosConfig{tc}}
	if spec.Mode == bootstrapv1.ManagementModeManaged {
		if tc.Status.Management != nil && tc.Status.Management.ActivatedRevision == spec.InventoryRevision {
			if e := op.ValidateManagedMachine(ctx, r.APIReader, m, tc); e != nil {
				if errors.Is(e, op.ErrManagementPaused) {
					return true, ctrl.Result{}, r.managementPausedStatus(ctx, tc)
				}
				return true, ctrl.Result{}, r.managementStatus(ctx, tc, nil, false, false, "ActivationInvalid")
			}
			return !bootstrapv1.IsInPlaceUpdate(tc), ctrl.Result{}, nil
		}
		status, e := op.ActivateManagement(ctx, r.Client, r.APIReader, request)
		if e != nil {
			if errors.Is(e, op.ErrManagementPaused) {
				return true, ctrl.Result{}, r.managementPausedStatus(ctx, tc)
			}
			return true, ctrl.Result{}, r.managementStatus(ctx, tc, nil, false, false, "ApprovalNotCurrent")
		}
		if e := r.managementStatus(ctx, tc, status, true, true, "Activated"); e != nil {
			return true, ctrl.Result{}, e
		}
		// Activation records ownership only. In-place generation later writes a
		// separate desired Secret; do not mutate bootstrap/identity on activation.
		return !bootstrapv1.IsInPlaceUpdate(tc), ctrl.Result{}, nil
	}
	observed, e := op.ObserveManagement(ctx, r.APIReader, request)
	if e != nil {
		if errors.Is(e, op.ErrManagementPaused) {
			return true, ctrl.Result{}, r.managementPausedStatus(ctx, tc)
		}
		return true, ctrl.Result{}, r.managementStatus(ctx, tc, nil, false, false, "ObservationIncomplete")
	}
	status, e := op.RecordManagementObservation(ctx, r.Client, r.APIReader, observed)
	if e != nil {
		if errors.Is(e, op.ErrManagementPaused) {
			return true, ctrl.Result{}, r.managementPausedStatus(ctx, tc)
		}
		return true, ctrl.Result{}, r.managementStatus(ctx, tc, nil, false, false, "SnapshotInvalid")
	}
	return true, ctrl.Result{}, r.managementStatus(ctx, tc, status, true, false, "Observed")
}
func hasManagementPause(o client.Object) bool {
	_, ok := o.GetAnnotations()[clusterv1.PausedAnnotation]
	return ok
}
func (r *TalosConfigReconciler) managementPausedStatus(ctx context.Context, tc *bootstrapv1.TalosConfig) error {
	helper, e := patch.NewHelper(tc, r.Client)
	if e != nil {
		return e
	}
	conditions.Set(tc, metav1.Condition{Type: clusterv1.PausedCondition, Status: metav1.ConditionTrue, Reason: clusterv1.PausedReason, ObservedGeneration: tc.Generation})
	return helper.Patch(ctx, tc, patch.WithOwnedConditions{Conditions: []string{clusterv1.PausedCondition}})
}
func (r *TalosConfigReconciler) managementStatus(ctx context.Context, tc *bootstrapv1.TalosConfig, status *bootstrapv1.ManagementStatus, verified, activated bool, reason string) error {
	helper, e := patch.NewHelper(tc, r.Client)
	if e != nil {
		return e
	}
	if status != nil {
		tc.Status.Management = status
	}
	value := metav1.ConditionFalse
	if verified {
		value = metav1.ConditionTrue
	}
	conditions.Set(tc, metav1.Condition{Type: bootstrapv1.InventoryVerifiedCondition, Status: value, Reason: reason, ObservedGeneration: tc.Generation})
	value = metav1.ConditionFalse
	if activated {
		value = metav1.ConditionTrue
	}
	conditions.Set(tc, metav1.Condition{Type: bootstrapv1.ManagementActivatedCondition, Status: value, Reason: reason, ObservedGeneration: tc.Generation})
	conditions.Set(tc, metav1.Condition{Type: clusterv1.PausedCondition, Status: metav1.ConditionFalse, Reason: clusterv1.NotPausedReason, ObservedGeneration: tc.Generation})
	// Observed inventory is not bootstrap provisioning or operational Ready.
	tc.Status.LastHandledReconcileAt = tc.Annotations["reconcile.fluxcd.io/requestedAt"]
	return helper.Patch(ctx, tc, patch.WithStatusObservedGeneration{}, patch.WithOwnedConditions{Conditions: []string{bootstrapv1.InventoryVerifiedCondition, bootstrapv1.ManagementActivatedCondition, clusterv1.PausedCondition}})
}
