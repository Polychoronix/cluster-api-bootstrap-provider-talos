// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package controllers

import (
	"context"
	"errors"
	"fmt"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var errInitialBootstrapLineagePending = errors.New("initial bootstrap is waiting for infrastructure ownership transfer")

// initialBootstrapLineage verifies controller-created initial CAPI bootstrap
// provenance. It is not a claim that a physical server is newly provisioned.
func (r *TalosConfigReconciler) initialBootstrapLineage(ctx context.Context, scope *TalosConfigScope) (bool, error) {
	if scope.ConfigOwner.IsMachinePool() {
		return false, nil
	}
	m := &clusterv1.Machine{}
	if err := r.liveReader().Get(ctx, client.ObjectKey{Namespace: scope.Config.Namespace, Name: scope.ConfigOwner.GetName()}, m); err != nil {
		return false, fmt.Errorf("initial bootstrap Machine lineage lookup failed")
	}
	if m.UID == "" || m.UID != scope.ConfigOwner.GetUID() || m.Spec.Bootstrap.DataSecretName != nil || m.Status.NodeRef.IsDefined() || !m.DeletionTimestamp.IsZero() {
		return false, nil
	}
	c := &bootstrapv1.TalosConfig{}
	if err := r.liveReader().Get(ctx, client.ObjectKeyFromObject(scope.Config), c); err != nil {
		return false, fmt.Errorf("initial bootstrap config lineage lookup failed")
	}
	if c.UID == "" || c.UID != scope.Config.UID || m.Spec.Bootstrap.ConfigRef.APIGroup != bootstrapv1.GroupVersion.Group || m.Spec.Bootstrap.ConfigRef.Kind != "TalosConfig" || m.Spec.Bootstrap.ConfigRef.Name != c.Name {
		return false, nil
	}
	configOwner := metav1.GetControllerOf(c)
	if configOwner == nil || configOwner.UID != m.UID || configOwner.Name != m.Name || configOwner.Kind != "Machine" {
		return false, nil
	}
	configOwnerGV, err := schema.ParseGroupVersion(configOwner.APIVersion)
	if err != nil || configOwnerGV.Group != clusterv1.GroupVersion.Group {
		return false, nil
	}
	owner := metav1.GetControllerOf(m)
	if owner == nil {
		return false, nil
	}
	switch owner.Kind {
	case "MachineSet":
		ownerGV, err := schema.ParseGroupVersion(owner.APIVersion)
		if err != nil || ownerGV.Group != clusterv1.GroupVersion.Group {
			return false, nil
		}
		set := &clusterv1.MachineSet{}
		if err := r.liveReader().Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: owner.Name}, set); err != nil {
			return false, fmt.Errorf("initial bootstrap MachineSet lineage lookup failed")
		}
		if set.UID != owner.UID || !set.DeletionTimestamp.IsZero() {
			return false, nil
		}
		ref := set.Spec.Template.Spec.Bootstrap.ConfigRef
		if ref.APIGroup != bootstrapv1.GroupVersion.Group || ref.Kind != "TalosConfigTemplate" || c.Annotations[clusterv1.TemplateClonedFromNameAnnotation] != ref.Name || c.Annotations[clusterv1.TemplateClonedFromGroupKindAnnotation] != (schema.GroupKind{Group: ref.APIGroup, Kind: ref.Kind}).String() {
			return false, nil
		}
		return r.initialInfraLineage(ctx, m, *owner, set.Spec.Template.Spec.InfrastructureRef)
	case "TalosControlPlane":
		ownerGV, err := schema.ParseGroupVersion(owner.APIVersion)
		if err != nil || ownerGV.Group != "controlplane.cluster.x-k8s.io" {
			return false, nil
		}
		cp := &unstructured.Unstructured{}
		cp.SetAPIVersion(owner.APIVersion)
		cp.SetKind(owner.Kind)
		if err := r.liveReader().Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: owner.Name}, cp); err != nil {
			return false, fmt.Errorf("initial bootstrap controlplane lineage lookup failed")
		}
		if cp.GetUID() != owner.UID || cp.GetDeletionTimestamp() != nil {
			return false, nil
		}
		if c.Annotations[updateoperation.InitialControlPlaneNameAnnotation] != owner.Name || c.Annotations[updateoperation.InitialControlPlaneUIDAnnotation] != string(owner.UID) {
			return false, nil
		}
		var template clusterv1.ContractVersionedObjectReference
		group, _, _ := unstructured.NestedString(cp.Object, "spec", "machineTemplate", "spec", "infrastructureRef", "apiGroup")
		kind, _, _ := unstructured.NestedString(cp.Object, "spec", "machineTemplate", "spec", "infrastructureRef", "kind")
		name, _, _ := unstructured.NestedString(cp.Object, "spec", "machineTemplate", "spec", "infrastructureRef", "name")
		template = clusterv1.ContractVersionedObjectReference{APIGroup: group, Kind: kind, Name: name}
		return r.initialInfraLineage(ctx, m, *owner, template)
	default:
		return false, nil
	}
}
func (r *TalosConfigReconciler) liveReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}
