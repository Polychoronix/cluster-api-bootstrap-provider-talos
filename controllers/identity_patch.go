// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package controllers

import (
	"context"
	"encoding/json"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func patchIdentitySnapshot(ctx context.Context, c client.Client, original *corev1.Secret, value []byte) error {
	operations := []map[string]any{{"op": "test", "path": "/metadata/uid", "value": original.UID}, {"op": "test", "path": "/metadata/resourceVersion", "value": original.ResourceVersion}, {"op": "replace", "path": "/data/talosconfig", "value": value}}
	payload, err := json.Marshal(operations)
	if err != nil {
		return identityError("conditional patch encoding")
	}
	if err := c.Patch(ctx, original.DeepCopy(), client.RawPatch(types.JSONPatchType, payload)); err != nil {
		return identityError("conditional update conflict; revalidation required")
	}
	return nil
}
