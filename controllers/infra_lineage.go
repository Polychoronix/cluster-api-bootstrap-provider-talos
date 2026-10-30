// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package controllers

import (
	"context"
	"fmt"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *TalosConfigReconciler) initialInfraLineage(ctx context.Context, m *clusterv1.Machine, owner metav1.OwnerReference, template clusterv1.ContractVersionedObjectReference) (bool, error) {
	if template.APIGroup != m.Spec.InfrastructureRef.APIGroup || template.Kind != m.Spec.InfrastructureRef.Kind+"Template" || template.Name == "" {
		return false, nil
	}
	crds := &apiextensionsv1.CustomResourceDefinitionList{}
	if r.liveReader().List(ctx, crds) != nil {
		return false, fmt.Errorf("initial infrastructure contract lookup failed")
	}
	version := ""
	for _, crd := range crds.Items {
		if crd.Spec.Group == m.Spec.InfrastructureRef.APIGroup && crd.Spec.Names.Kind == m.Spec.InfrastructureRef.Kind {
			for _, v := range crd.Spec.Versions {
				if v.Served && v.Storage {
					version = v.Name
				}
			}
		}
	}
	if version == "" {
		return false, nil
	}
	infra := &unstructured.Unstructured{}
	infra.SetAPIVersion(m.Spec.InfrastructureRef.APIGroup + "/" + version)
	infra.SetKind(m.Spec.InfrastructureRef.Kind)
	if r.liveReader().Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: m.Spec.InfrastructureRef.Name}, infra) != nil {
		return false, fmt.Errorf("initial infrastructure lineage lookup failed")
	}
	if infra.GetUID() == "" || infra.GetDeletionTimestamp() != nil || infra.GetAnnotations()[clusterv1.TemplateClonedFromNameAnnotation] != template.Name || infra.GetAnnotations()[clusterv1.TemplateClonedFromGroupKindAnnotation] != (schema.GroupKind{Group: template.APIGroup, Kind: template.Kind}).String() {
		return false, nil
	}
	controller := metav1.GetControllerOf(infra)
	if controller != nil && controller.UID == m.UID && controller.Kind == "Machine" && controller.Name == m.Name {
		gv, err := schema.ParseGroupVersion(controller.APIVersion)
		if err == nil && gv.Group == clusterv1.GroupVersion.Group {
			return true, nil
		}
		return false, nil
	}
	if controller != nil {
		return false, nil
	}
	for _, ref := range infra.GetOwnerReferences() {
		if ref.UID == owner.UID && ref.Name == owner.Name && ref.Kind == owner.Kind && ref.APIVersion == owner.APIVersion {
			// Core reconciles bootstrap before infrastructure; exact creator lineage
			// can be recognized while Machine ownership transfer is pending.
			// This is wait evidence only, never permission to initialize intent.
			return false, errInitialBootstrapLineagePending
		}
	}
	return false, nil
}
