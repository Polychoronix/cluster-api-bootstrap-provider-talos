// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"context"
	"encoding/json"
	"errors"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strings"
)

var errManagementInfraNotFound = errors.New("Talos infrastructure object not found")

// Explicit CAPI deletion remains separate from requested config/OS operations.
func ValidateManagementRetirement(ctx context.Context, reader client.Reader, a ManagementAuthority) error {
	if reader == nil || a.Namespace == "" || a.Name == "" || a.UID == "" {
		return managementError("retirement authority")
	}
	authority := &unstructured.Unstructured{}
	authority.SetAPIVersion(a.APIVersion)
	authority.SetKind(a.Kind)
	if reader.Get(ctx, client.ObjectKey{Namespace: a.Namespace, Name: a.Name}, authority) != nil || authority.GetUID() != a.UID || authority.GetDeletionTimestamp().IsZero() {
		return managementError("exact deleting authority required")
	}
	if managementPaused(authority) {
		return ErrManagementPaused
	}
	if retirementObjectUpdating(authority) {
		return managementError("retirement authority busy")
	}
	machines := &clusterv1.MachineList{}
	if reader.List(ctx, machines, client.InNamespace(a.Namespace)) != nil {
		return managementError("retirement inventory lookup")
	}
	for i := range machines.Items {
		m := &machines.Items[i]
		owner := metav1.GetControllerOf(m)
		if a.Kind == "TalosControlPlane" && (owner == nil || owner.APIVersion != a.APIVersion || owner.Name != a.Name || owner.UID != a.UID || owner.Kind != a.Kind) {
			continue
		}
		if a.Kind == "TalosConfig" && m.Spec.Bootstrap.ConfigRef.Name != a.Name {
			continue
		}
		if managementPaused(m) {
			return ErrManagementPaused
		}
		if retirementObjectUpdating(m) {
			return managementError("retirement member busy")
		}
		if err := retirementJournalByName(ctx, reader, a.Namespace, m.Spec.Bootstrap.ConfigRef.Name); err != nil {
			return err
		}
		tc := &bootstrapv1.TalosConfig{}
		err := reader.Get(ctx, client.ObjectKey{Namespace: a.Namespace, Name: m.Spec.Bootstrap.ConfigRef.Name}, tc)
		if apierrors.IsNotFound(err) && !m.DeletionTimestamp.IsZero() {
			continue
		}
		if err != nil || !managementController(tc, m) {
			return managementError("retirement bootstrap owner")
		}
		if managementPaused(tc) {
			return ErrManagementPaused
		}
		if retirementObjectUpdating(tc) {
			return managementError("retirement bootstrap busy")
		}
		if err := retirementOwnerIntentIdle(ctx, reader, tc); err != nil {
			return err
		}
		infra, err := managementInfrastructure(ctx, reader, m)
		if err != nil {
			if !m.DeletionTimestamp.IsZero() && errors.Is(err, errManagementInfraNotFound) {
				continue
			}
			return err
		}
		if managementPaused(infra) {
			return ErrManagementPaused
		}
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
	_, present := o.GetAnnotations()[clusterv1.UpdateInProgressAnnotation]
	return present || strings.Contains(o.GetAnnotations()["runtime.cluster.x-k8s.io/pending-hooks"], "UpdateMachine")
}
func retirementOwnerIntentIdle(ctx context.Context, reader client.Reader, tc *bootstrapv1.TalosConfig) error {
	name, uid := tc.Annotations[OwnerPlanNameAnnotation], tc.Annotations[OwnerPlanUIDAnnotation]
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
