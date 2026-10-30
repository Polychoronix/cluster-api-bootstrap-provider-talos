// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"context"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// RequireNoManagementRecord guards the fresh creation path even before an
// association can be resolved. Any retained record, including malformed or
// foreign content, is a hard hold rather than absence-based authorization.
func RequireNoManagementRecord(ctx context.Context, reader client.Reader, a ManagementAuthority) error {
	if reader == nil {
		return managementError("APIReader required")
	}
	secret := &corev1.Secret{}
	err := reader.Get(ctx, client.ObjectKey{Namespace: a.Namespace, Name: managementName(a)}, secret)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return managementError("management record lookup")
	}
	return managementError("management declaration removed with retained record")
}
