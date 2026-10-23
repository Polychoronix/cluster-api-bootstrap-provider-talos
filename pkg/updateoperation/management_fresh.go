// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"context"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ValidateFreshBootstrapIntent permits absence of an adoption declaration only
// for an actual provider-created seed carrying accepted intent for these UIDs.
// A seed reference, NodeRef, Ready bit or absent public status is not provenance.
func ValidateFreshBootstrapIntent(ctx context.Context, reader client.Reader, m *clusterv1.Machine, tc *bootstrapv1.TalosConfig) error {
	if reader == nil || m == nil || tc == nil || m.Spec.Bootstrap.DataSecretName == nil {
		return managementError("fresh accepted bootstrap required")
	}
	seed := &corev1.Secret{}
	if reader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: *m.Spec.Bootstrap.DataSecretName}, seed) != nil {
		return managementError("fresh bootstrap seed lookup")
	}
	owner := metav1.GetControllerOf(seed)
	if owner == nil || owner.APIVersion != bootstrapv1.GroupVersion.String() || owner.Kind != "TalosConfig" || owner.Name != tc.Name || owner.UID != tc.UID {
		return managementError("fresh bootstrap seed owner")
	}
	intent, err := DecodeAcceptedIntent(seed.Data[AcceptedIntentKey])
	if err != nil || intent.MachineUID != m.UID || intent.TalosConfigUID != tc.UID {
		return managementError("fresh bootstrap accepted intent")
	}
	return nil
}

// ReadManagementStatus validates an existing private record independently of
// public management/status presence. ErrManagementRecordNotFound is the only
// absent result; malformed/foreign/replaced immutable records are hard errors.
// A returned ActivatedRevision is observation, never authorization to act.
func ReadManagementStatus(ctx context.Context, reader client.Reader, a ManagementAuthority) (*bootstrapv1.ManagementStatus, error) {
	if err := validateManagementAuthority(ctx, reader, a); err != nil {
		return nil, err
	}
	_, record, _, err := readManagement(ctx, reader, a)
	if err != nil {
		if err == ErrNotFound {
			return nil, ErrManagementRecordNotFound
		}
		return nil, err
	}
	return managementStatus(record), nil
}
