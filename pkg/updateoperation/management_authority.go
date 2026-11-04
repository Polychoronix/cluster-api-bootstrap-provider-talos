// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"context"
	"encoding/json"
	corev1 "k8s.io/api/core/v1"
	"reflect"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ManagementAuthorityForMachine resolves exact live consent, not seed provenance.
func ManagementAuthorityForMachine(ctx context.Context, reader client.Reader, m *clusterv1.Machine, tc *bootstrapv1.TalosConfig) (ManagementAuthority, bool, error) {
	if reader == nil || m == nil || tc == nil || m.Namespace != tc.Namespace || m.Spec.Bootstrap.ConfigRef.APIGroup != bootstrapv1.GroupVersion.Group || m.Spec.Bootstrap.ConfigRef.Kind != "TalosConfig" || m.Spec.Bootstrap.ConfigRef.Name != tc.Name || !managementController(tc, m) {
		return ManagementAuthority{}, false, managementError("Machine bootstrap association")
	}
	if managementPaused(m) || managementPaused(tc) {
		return ManagementAuthority{}, false, ErrManagementPaused
	}
	cluster := &clusterv1.Cluster{}
	if reader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: m.Spec.ClusterName}, cluster) != nil {
		return ManagementAuthority{}, false, managementError("Cluster lookup")
	}
	if managementPaused(cluster) || cluster.Spec.Paused != nil && *cluster.Spec.Paused {
		return ManagementAuthority{}, false, ErrManagementPaused
	}
	if _, cp := m.Labels[clusterv1.MachineControlPlaneLabel]; cp {
		owner := metav1.GetControllerOf(m)
		if owner == nil || owner.Kind != "TalosControlPlane" || managementGroup(owner.APIVersion) != "controlplane.cluster.x-k8s.io" {
			return ManagementAuthority{}, false, managementError("control-plane authority association")
		}
		a := ManagementAuthority{Namespace: m.Namespace, APIVersion: owner.APIVersion, Kind: owner.Kind, Name: owner.Name, UID: owner.UID}
		tcp := &unstructured.Unstructured{}
		tcp.SetAPIVersion(owner.APIVersion)
		tcp.SetKind(owner.Kind)
		if reader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: owner.Name}, tcp) != nil || tcp.GetUID() != owner.UID || !tcp.GetDeletionTimestamp().IsZero() {
			return a, false, managementError("live control-plane authority")
		}
		if managementPaused(tcp) {
			return a, false, ErrManagementPaused
		}
		manage, _, err := unstructured.NestedBool(tcp.Object, "spec", "manage")
		return a, manage, err
	}
	return ManagementAuthority{Namespace: tc.Namespace, APIVersion: bootstrapv1.GroupVersion.String(), Kind: "TalosConfig", Name: tc.Name, UID: tc.UID}, tc.Spec.Manage, nil
}

// ValidateManagedMachine checks only reads before new runtime/receipt work. For
// fresh resources absent management the existing positive generation provenance
// remains required by the normal renderer/executor; an original external seed is
// never promoted into accepted intent by this function.
func ValidateManagedMachine(ctx context.Context, reader client.Reader, m *clusterv1.Machine, tc *bootstrapv1.TalosConfig) error {
	if reader == nil {
		return managementError("APIReader required")
	}
	if m == nil || tc == nil {
		return managementError("Machine lookup")
	}
	if managementPaused(m) || managementPaused(tc) {
		return ErrManagementPaused
	}
	cluster := &clusterv1.Cluster{}
	if reader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: m.Spec.ClusterName}, cluster) != nil {
		return managementError("Cluster lookup")
	}
	if managementPaused(cluster) || (cluster.Spec.Paused != nil && *cluster.Spec.Paused) {
		return ErrManagementPaused
	}
	a, spec, e := ManagementAuthorityForMachine(ctx, reader, m, tc)
	if e != nil {
		return e
	}
	if !spec {
		return managementError("management consent absent")
	}
	if e := ValidateManagementActivation(ctx, reader, a, spec); e != nil {
		return e
	}
	_, _, payload, e := readManagement(ctx, reader, a)
	if e != nil {
		return e
	}
	safety := &managementSafety{}
	if json.Unmarshal(payload, safety) != nil {
		return managementError("activation snapshot decode")
	}
	for _, member := range safety.Members {
		if member.Machine.Name == m.Name && member.Machine.UID == m.UID && member.TalosConfig.Name == tc.Name && member.TalosConfig.UID == tc.UID {
			if member.ProviderID != m.Spec.ProviderID || member.NodeName != m.Status.NodeRef.Name || member.Infrastructure.Name != m.Spec.InfrastructureRef.Name || member.Infrastructure.Kind != m.Spec.InfrastructureRef.Kind || member.Infrastructure.APIGroup != m.Spec.InfrastructureRef.APIGroup {
				return managementError("activated association changed")
			}
			expectedOwners := member.Machine.OwnerReferences
			if !reflect.DeepEqual(m.OwnerReferences, expectedOwners) {
				return managementError("activated owner association changed")
			}
			infra, e := managementInfrastructure(ctx, reader, m)
			if e != nil {
				return e
			}
			if managementPaused(infra) {
				return ErrManagementPaused
			}
			if infra.GetUID() != member.Infrastructure.UID {
				return managementError("activated infrastructure identity")
			}
			if err := validateManagementNode(ctx, reader, safety, &member); err != nil {
				return err
			}
			if member.Seed.Name == "" || m.Spec.Bootstrap.DataSecretName == nil || *m.Spec.Bootstrap.DataSecretName != member.Seed.Name {
				return managementError("original seed reference changed")
			}
			seed := &corev1.Secret{}
			if reader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: member.Seed.Name}, seed) != nil || !sameManagementIdentity(managementSource(seed, member.Seed.Key), member.Seed) {
				return managementError("original seed identity changed")
			}
			return nil
		}
	}
	return managementError("Machine outside activated inventory")
}

// AcceptedImage reads the single activated UID-bound image state, never an output receipt.
func AcceptedImage(ctx context.Context, reader client.Reader, m *clusterv1.Machine, tc *bootstrapv1.TalosConfig) (ImageIdentity, error) {
	a, manage, err := ManagementAuthorityForMachine(ctx, reader, m, tc)
	if err != nil {
		return ImageIdentity{}, err
	}
	if err = ValidateManagementActivation(ctx, reader, a, manage); err != nil {
		return ImageIdentity{}, err
	}
	_, record, payload, err := readManagement(ctx, reader, a)
	if err != nil {
		return ImageIdentity{}, err
	}
	safety := &managementSafety{}
	if json.Unmarshal(payload, safety) != nil {
		return ImageIdentity{}, managementError("activation snapshot decode")
	}
	for _, member := range safety.Members {
		if member.Machine.UID == m.UID && member.TalosConfig.UID == tc.UID {
			image, ok := record.AcceptedImages[m.UID]
			if !ok {
				return ImageIdentity{}, managementError("accepted image unavailable")
			}
			return image, nil
		}
	}
	return ImageIdentity{}, managementError("accepted inventory")
}

// RecordAcceptedImage advances only after a UID-bound operation was observed.
func RecordAcceptedImage(ctx context.Context, c client.Client, reader client.Reader, m *clusterv1.Machine, tc *bootstrapv1.TalosConfig, image ImageIdentity) error {
	if _, err := AcceptedImage(ctx, reader, m, tc); err != nil {
		return err
	}
	a, _, err := ManagementAuthorityForMachine(ctx, reader, m, tc)
	if err != nil {
		return err
	}
	secret, record, _, err := readManagement(ctx, reader, a)
	if err != nil {
		return err
	}
	record.AcceptedImages[m.UID] = image
	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return PatchData(ctx, c, secret, map[string][]byte{managementRecordKey: encoded})
}

func validateManagementMemberOwners(ctx context.Context, reader client.Reader, m *clusterv1.Machine, tc *bootstrapv1.TalosConfig) error {
	if !managementController(tc, m) {
		return managementError("native bootstrap owner")
	}
	infra, err := managementInfrastructure(ctx, reader, m)
	if err != nil {
		return err
	}
	if managementPaused(infra) {
		return ErrManagementPaused
	}
	return nil
}

// MemberBaseline reports which rendering the Machine's node matched when its authority
// was activated (BaselineEffective, BaselineCurrent, BaselineDrift) and, for drift, the
// document identities that differed from the effective policy; "" when the Machine is not
// part of a readable activation snapshot.
func MemberBaseline(ctx context.Context, reader client.Reader, m *clusterv1.Machine, tc *bootstrapv1.TalosConfig) (string, []string) {
	if reader == nil || m == nil || tc == nil {
		return "", nil
	}
	a, _, err := ManagementAuthorityForMachine(ctx, reader, m, tc)
	if err != nil {
		return "", nil
	}
	_, _, payload, err := readManagement(ctx, reader, a)
	if err != nil {
		return "", nil
	}
	safety := &managementSafety{}
	if json.Unmarshal(payload, safety) != nil {
		return "", nil
	}
	for _, member := range safety.Members {
		if member.Machine.Name == m.Name && member.Machine.UID == m.UID && member.TalosConfig.Name == tc.Name && member.TalosConfig.UID == tc.UID {
			return member.Baseline, member.Drift
		}
	}
	return "", nil
}
