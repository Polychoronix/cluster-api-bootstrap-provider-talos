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
	"k8s.io/apimachinery/pkg/runtime"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ManagementAuthorityForMachine resolves the one live consent authority. CP
// children read the exact TCP controller object; they never inherit consent from
// a name, a noncontroller reference or a second child approval.
func ManagementAuthorityForMachine(ctx context.Context, reader client.Reader, m *clusterv1.Machine, tc *bootstrapv1.TalosConfig) (ManagementAuthority, *bootstrapv1.ManagementSpec, error) {
	if reader == nil {
		return ManagementAuthority{}, nil, managementError("APIReader required")
	}
	if m == nil || tc == nil || m.Namespace != tc.Namespace || m.Spec.Bootstrap.ConfigRef.APIGroup != bootstrapv1.GroupVersion.Group || m.Spec.Bootstrap.ConfigRef.Kind != "TalosConfig" || m.Spec.Bootstrap.ConfigRef.Name != tc.Name || !managementController(tc, m) {
		return ManagementAuthority{}, nil, managementError("Machine bootstrap association")
	}
	if managementPaused(m) || managementPaused(tc) {
		return ManagementAuthority{}, nil, ErrManagementPaused
	}
	cluster := &clusterv1.Cluster{}
	if reader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: m.Spec.ClusterName}, cluster) != nil {
		return ManagementAuthority{}, nil, managementError("Cluster lookup")
	}
	if managementPaused(cluster) || (cluster.Spec.Paused != nil && *cluster.Spec.Paused) {
		return ManagementAuthority{}, nil, ErrManagementPaused
	}
	if _, cp := m.Labels[clusterv1.MachineControlPlaneLabel]; cp {
		if tc.Spec.Management != nil {
			return ManagementAuthority{}, nil, managementError("independent control-plane consent")
		}
		owner := metav1.GetControllerOf(m)
		if owner == nil || owner.Kind != "TalosControlPlane" || managementGroup(owner.APIVersion) != "controlplane.cluster.x-k8s.io" {
			return ManagementAuthority{}, nil, managementError("control-plane authority association")
		}
		a := ManagementAuthority{Namespace: m.Namespace, APIVersion: owner.APIVersion, Kind: owner.Kind, Name: owner.Name, UID: owner.UID}
		tcp := &unstructured.Unstructured{}
		tcp.SetAPIVersion(owner.APIVersion)
		tcp.SetKind(owner.Kind)
		if reader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: owner.Name}, tcp) != nil || tcp.GetUID() != owner.UID || !tcp.GetDeletionTimestamp().IsZero() {
			return a, nil, managementError("live control-plane authority")
		}
		if managementPaused(tcp) {
			return a, nil, ErrManagementPaused
		}
		spec, found, e := unstructured.NestedMap(tcp.Object, "spec", "management")
		if e != nil {
			return a, nil, managementError("authority management decode")
		}
		if !found {
			if _, _, _, err := readManagement(ctx, reader, a); err == nil {
				return a, nil, managementError("management declaration removed")
			} else if err != ErrNotFound {
				return a, nil, err
			}
			return a, nil, nil
		}
		management := &bootstrapv1.ManagementSpec{}
		if runtime.DefaultUnstructuredConverter.FromUnstructured(spec, management) != nil {
			return a, nil, managementError("authority management decode")
		}
		return a, management, nil
	}
	a := ManagementAuthority{Namespace: tc.Namespace, APIVersion: bootstrapv1.GroupVersion.String(), Kind: "TalosConfig", Name: tc.Name, UID: tc.UID}
	if tc.Spec.Management == nil {
		if _, _, _, err := readManagement(ctx, reader, a); err == nil {
			return a, nil, managementError("management declaration removed")
		} else if err != ErrNotFound {
			return a, nil, err
		}
	}
	return a, tc.Spec.Management, nil
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
	if spec == nil {
		// Existing provider-generated accepted intent is checked by the runtime
		// transaction. An orphaned adoption record must not select the fresh path.
		if _, _, _, e := readManagement(ctx, reader, a); e == nil {
			return managementError("management declaration removed")
		} else if e != ErrNotFound {
			return e
		}
		return ValidateFreshBootstrapIntent(ctx, reader, m, tc)
	}
	if e := ValidateManagementActivation(ctx, reader, a, spec); e != nil {
		return e
	}
	if a.Kind == "TalosControlPlane" && tc.Annotations[InitialControlPlaneNameAnnotation] == a.Name && tc.Annotations[InitialControlPlaneUIDAnnotation] == string(a.UID) && m.Spec.Bootstrap.DataSecretName != nil {
		seed := &corev1.Secret{}
		if reader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: *m.Spec.Bootstrap.DataSecretName}, seed) == nil {
			owner := metav1.GetControllerOf(seed)
			if accepted, err := DecodeAcceptedIntent(seed.Data[AcceptedIntentKey]); owner != nil && owner.APIVersion == bootstrapv1.GroupVersion.String() && owner.Kind == "TalosConfig" && owner.Name == tc.Name && owner.UID == tc.UID && err == nil && accepted.MachineUID == m.UID && accepted.TalosConfigUID == tc.UID {
				if err := validateManagementMemberOwners(ctx, reader, m, tc); err != nil {
					return err
				}
				return nil
			}
		}
	}
	_, record, payload, e := readManagement(ctx, reader, a)
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
			for _, association := range record.Associations {
				if association.Machine.UID == m.UID && association.Completed {
					expectedOwners = association.After
				}
			}
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
			if m.Spec.Bootstrap.DataSecretName == nil || *m.Spec.Bootstrap.DataSecretName != member.Seed.Name {
				return managementError("original seed reference changed")
			}
			seed := &corev1.Secret{}
			if reader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: member.Seed.Name}, seed) != nil || !managementSame(managementSource(seed, member.Seed.Key), member.Seed) {
				return managementError("original seed identity changed")
			}
			return nil
		}
	}
	return managementError("Machine outside activated inventory")
}

// ManagementAcceptedIntent supplies the original reviewed image intent for a
// newly separated desired output. It never changes the installation seed.
func ManagementAcceptedIntent(ctx context.Context, reader client.Reader, a ManagementAuthority, m *clusterv1.Machine, tc *bootstrapv1.TalosConfig) (AcceptedIntent, error) {
	_, r, payload, e := readManagement(ctx, reader, a)
	if e != nil {
		return AcceptedIntent{}, e
	}
	if !r.Activated {
		return AcceptedIntent{}, managementError("activation required")
	}
	safety := &managementSafety{}
	if json.Unmarshal(payload, safety) != nil {
		return AcceptedIntent{}, managementError("snapshot decode")
	}
	for _, member := range safety.Members {
		if member.Machine.UID == m.UID && member.TalosConfig.UID == tc.UID {
			var originalSpec bootstrapv1.TalosConfigSpec
			if json.Unmarshal(member.ConfigSpec, &originalSpec) != nil {
				return AcceptedIntent{}, managementError("original configuration intent")
			}
			image, e := ConfigurationImage(member.Runtime.Configuration, originalSpec)
			if e != nil {
				return AcceptedIntent{}, managementError("reviewed original installer identity")
			}
			return AcceptedIntent{MachineUID: m.UID, TalosConfigUID: tc.UID, Image: image}, nil
		}
	}
	return AcceptedIntent{}, managementError("accepted inventory")
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
