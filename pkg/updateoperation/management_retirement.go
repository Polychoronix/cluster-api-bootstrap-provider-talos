// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var errManagementInfraNotFound = errors.New("Talos management infrastructure object not found")

// ValidateManagementRetirement authorizes only the already-requested native
// deletion lifecycle. Unlike normal management validation it requires a deleting
// authority, accepts expected disappeared children, and never authorizes config,
// bootstrap, Apply/Upgrade/Reboot, new snapshot or activation. It performs reads.
func ValidateManagementRetirement(ctx context.Context, reader client.Reader, a ManagementAuthority) error {
	if reader == nil || a.Namespace == "" || a.Name == "" || a.UID == "" || (a.Kind != "TalosControlPlane" && a.Kind != "TalosConfig") {
		return managementError("retirement authority")
	}
	if a.Kind == "TalosControlPlane" && managementGroup(a.APIVersion) != "controlplane.cluster.x-k8s.io" || a.Kind == "TalosConfig" && managementGroup(a.APIVersion) != bootstrapv1.GroupVersion.Group {
		return managementError("retirement authority group")
	}
	authority := &unstructured.Unstructured{}
	authority.SetAPIVersion(a.APIVersion)
	authority.SetKind(a.Kind)
	if reader.Get(ctx, client.ObjectKey{Namespace: a.Namespace, Name: a.Name}, authority) != nil || authority.GetUID() != a.UID || authority.GetDeletionTimestamp().IsZero() {
		return managementError("retirement requires exact deleting authority")
	}
	if managementPaused(authority) {
		return ErrManagementPaused
	}
	raw, found, err := unstructured.NestedMap(authority.Object, "spec", "management")
	var spec *bootstrapv1.ManagementSpec
	if err != nil {
		return managementError("retirement management decode")
	}
	if found {
		spec = &bootstrapv1.ManagementSpec{}
		if runtime.DefaultUnstructuredConverter.FromUnstructured(raw, spec) != nil {
			return managementError("retirement management decode")
		}
		if spec.Mode != bootstrapv1.ManagementModeManaged {
			return managementError("Observe cannot retire authority")
		}
	}
	_, record, payload, err := readManagement(ctx, reader, a)
	if err != nil && err != ErrNotFound {
		return err
	}
	var safety *managementSafety
	if err == nil {
		if !record.Activated || spec == nil || spec.InventoryRevision != record.Revision || spec.Approval == nil || spec.Approval.Revision != record.Revision || spec.Approval.ObservationID != record.ObservationID {
			return managementError("retirement activation required")
		}
		for _, association := range record.Associations {
			if !association.Completed {
				return managementError("retirement ownership association pending")
			}
		}
		safety = &managementSafety{}
		if json.Unmarshal(payload, safety) != nil {
			return managementError("retirement snapshot decode")
		}
		cluster := &clusterv1.Cluster{}
		if reader.Get(ctx, client.ObjectKey{Namespace: a.Namespace, Name: safety.ClusterName}, cluster) != nil || cluster.UID != safety.ClusterUID {
			return managementError("retirement Cluster identity")
		}
		if managementPaused(cluster) || (cluster.Spec.Paused != nil && *cluster.Spec.Paused) {
			return ErrManagementPaused
		}
	} else if spec != nil {
		return managementError("retirement management record missing")
	}
	list := &clusterv1.MachineList{}
	if reader.List(ctx, list, client.InNamespace(a.Namespace)) != nil {
		return managementError("retirement Machine inventory")
	}
	known := map[string]managementMember{}
	if safety != nil {
		for _, member := range safety.Members {
			known[member.Machine.Name] = member
			if err := retirementJournalIdle(ctx, reader, a.Namespace, member.TalosConfig); err != nil {
				return err
			}
		}
	}
	for i := range list.Items {
		m := &list.Items[i]
		owner := metav1.GetControllerOf(m)
		owned := a.Kind == "TalosControlPlane" && owner != nil && owner.APIVersion == a.APIVersion && owner.Kind == a.Kind && owner.Name == a.Name && owner.UID == a.UID
		pin, declared := known[m.Name]
		if declared && m.UID != pin.Machine.UID {
			return managementError("retirement Machine UID replaced")
		}
		if declared && !owned && a.Kind == "TalosControlPlane" {
			return managementError("retirement Machine foreign owner")
		}
		if a.Kind == "TalosConfig" {
			owned = m.Spec.Bootstrap.ConfigRef.APIGroup == bootstrapv1.GroupVersion.Group && m.Spec.Bootstrap.ConfigRef.Kind == "TalosConfig" && m.Spec.Bootstrap.ConfigRef.Name == a.Name
		}
		if !owned {
			continue
		}
		if managementPaused(m) {
			return ErrManagementPaused
		}
		if _, updating := m.Annotations[clusterv1.UpdateInProgressAnnotation]; updating || strings.Contains(m.Annotations["runtime.cluster.x-k8s.io/pending-hooks"], "UpdateMachine") {
			return managementError("retirement active Machine update")
		}
		if err := retirementJournalByName(ctx, reader, m.Namespace, m.Spec.Bootstrap.ConfigRef.Name); err != nil {
			return err
		}
		cluster := &clusterv1.Cluster{}
		if reader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: m.Spec.ClusterName}, cluster) != nil {
			return managementError("retirement Cluster lookup")
		}
		if safety != nil && cluster.UID != safety.ClusterUID {
			return managementError("retirement Cluster changed")
		}
		if managementPaused(cluster) || (cluster.Spec.Paused != nil && *cluster.Spec.Paused) {
			return ErrManagementPaused
		}
		tc := &bootstrapv1.TalosConfig{}
		getErr := reader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: m.Spec.Bootstrap.ConfigRef.Name}, tc)
		if apierrors.IsNotFound(getErr) && m.DeletionTimestamp != nil {
			continue
		}
		if getErr != nil || !managementController(tc, m) {
			return managementError("retirement bootstrap owner")
		}
		if managementPaused(tc) {
			return ErrManagementPaused
		}
		if retirementObjectUpdating(tc) {
			return managementError("retirement bootstrap update pending")
		}
		if err := retirementOwnerIntentIdle(ctx, reader, tc); err != nil {
			return err
		}
		if declared {
			if tc.UID != pin.TalosConfig.UID || m.Spec.ProviderID != pin.ProviderID || m.Spec.InfrastructureRef.Name != pin.Infrastructure.Name || m.Spec.InfrastructureRef.Kind != pin.Infrastructure.Kind || m.Spec.InfrastructureRef.APIGroup != pin.Infrastructure.APIGroup {
				return managementError("retirement imported associations changed")
			}
		}
		if declared {
			if m.Spec.Bootstrap.DataSecretName == nil || *m.Spec.Bootstrap.DataSecretName != pin.Seed.Name {
				return managementError("retirement original seed ref changed")
			}
			seed := &corev1.Secret{}
			if reader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: pin.Seed.Name}, seed) != nil || !sameManagementIdentity(managementSource(seed, pin.Seed.Key), pin.Seed) {
				return managementError("retirement original seed identity changed")
			}
		}
		if !declared {
			if err := ValidateFreshBootstrapIntent(ctx, reader, m, tc); err != nil {
				return err
			}
		}
		if err := retirementJournalIdle(ctx, reader, m.Namespace, managementObject(bootstrapv1.GroupVersion.Group, "TalosConfig", tc)); err != nil {
			return err
		}
		infra, infraErr := managementInfrastructureObject(ctx, reader, m)
		if infraErr != nil {
			if m.DeletionTimestamp == nil || infraErr != errManagementInfraNotFound {
				return infraErr
			}
		} else {
			if managementPaused(infra) {
				return ErrManagementPaused
			}
			if retirementObjectUpdating(infra) {
				return managementError("retirement infrastructure update pending")
			}
			if !managementController(infra, m) {
				return managementError("retirement infrastructure foreign owner")
			}
			if declared && infra.GetUID() != pin.Infrastructure.UID {
				return managementError("retirement infrastructure UID replaced")
			}
		}
		// Expected deleting/missing workload Node is not consent revocation. Core's
		// deletion transaction owns drain/membership/physical-provider completion.
	}
	return nil
}

func retirementJournalIdle(ctx context.Context, reader client.Reader, namespace string, config ObjectIdentity) error {
	secret, journal, err := ReadJournal(ctx, reader, namespace, config)
	if err == ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	if journal.Phase != Observed {
		return managementError("retirement active or ambiguous operation")
	}
	if _, err := ReadReceipt(ctx, reader, namespace, config, journal.TransactionID); err != nil {
		return err
	}
	consumed := &CompletionReceipt{}
	if json.Unmarshal(secret.Data[ConsumedReceiptKey], consumed) != nil || !receiptMatches(consumed, journal) {
		return managementError("retirement operation receipt not consumed")
	}
	return nil
}

func retirementJournalByName(ctx context.Context, reader client.Reader, namespace, name string) error {
	if name == "" {
		return managementError("retirement bootstrap association")
	}
	config := ObjectIdentity{APIGroup: bootstrapv1.GroupVersion.Group, Kind: "TalosConfig", Name: name}
	secret := &corev1.Secret{}
	err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: SecretName(config)}, secret)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return managementError("retirement operation lookup")
	}
	owner := metav1.GetControllerOf(secret)
	if owner == nil || owner.APIVersion != bootstrapv1.GroupVersion.String() || owner.Kind != config.Kind || owner.Name != name || owner.UID == "" {
		return managementError("retirement operation owner")
	}
	config.UID = owner.UID
	return retirementJournalIdle(ctx, reader, namespace, config)
}

func retirementObjectUpdating(o client.Object) bool {
	if _, present := o.GetAnnotations()[clusterv1.UpdateInProgressAnnotation]; present {
		return true
	}
	return strings.Contains(o.GetAnnotations()["runtime.cluster.x-k8s.io/pending-hooks"], "UpdateMachine")
}

func retirementOwnerIntentIdle(ctx context.Context, reader client.Reader, tc *bootstrapv1.TalosConfig) error {
	name := tc.Annotations[OwnerPlanNameAnnotation]
	uid := tc.Annotations[OwnerPlanUIDAnnotation]
	if name == "" && uid == "" {
		return nil
	}
	if name == "" || uid == "" {
		return managementError("retirement owner intent ref")
	}
	secret := &corev1.Secret{}
	if reader.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: name}, secret) != nil || string(secret.UID) != uid || secret.Annotations[ProvenanceAnnotation] != OwnerIntentProvenanceValue {
		return managementError("retirement owner intent identity")
	}
	intent := &OwnerPlanIntent{}
	if json.Unmarshal(secret.Data[OwnerIntentKey], intent) != nil || intent.Version != OwnerIntentVersion || intent.TalosConfig.UID != tc.UID || intent.TalosConfig.Name != tc.Name || intent.Phase != OwnerConsumed {
		return managementError("retirement owner intent not consumed")
	}
	owner := metav1.GetControllerOf(secret)
	if owner == nil || owner.APIVersion != intent.Owner.APIGroup+"/v1beta1" || owner.Kind != intent.Owner.Kind || owner.Name != intent.Owner.Name || owner.UID != intent.Owner.UID {
		return managementError("retirement owner intent provenance")
	}
	return nil
}
