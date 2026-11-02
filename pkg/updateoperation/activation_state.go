// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"context"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ReadManagementStatus exposes only private record state, never private snapshot bytes.
func ReadManagementStatus(ctx context.Context, reader client.Reader, a ManagementAuthority) (*ActivationStatus, error) {
	if err := validateManagementAuthority(ctx, reader, a); err != nil {
		return nil, err
	}
	_, record, _, err := readManagement(ctx, reader, a)
	if err == ErrNotFound {
		return nil, ErrManagementRecordNotFound
	}
	if err != nil {
		return nil, err
	}
	return managementStatus(record), nil
}

// IsActivated is a local cache-free check; it never substitutes for full validation before mutation.
func IsActivated(ctx context.Context, reader client.Reader, tc *bootstrapv1.TalosConfig) bool {
	a := ManagementAuthority{Namespace: tc.Namespace, APIVersion: bootstrapv1.GroupVersion.String(), Kind: "TalosConfig", Name: tc.Name, UID: tc.UID}
	status, err := ReadManagementStatus(ctx, reader, a)
	return err == nil && status.Activated
}
