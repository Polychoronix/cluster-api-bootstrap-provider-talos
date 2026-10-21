// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package controllers

import (
	"context"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"
)

func TestIdentityReplacementBetweenReadAndWriteRetainsExternalBytes(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	original := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "cluster-talosconfig", UID: "original", ResourceVersion: "1"}, Data: map[string][]byte{"talosconfig": []byte("original-validated")}}
	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(original).Build()
	race := &identityRaceClient{Client: base}
	race.onPatch = func(ctx context.Context, o client.Object) {
		require.NoError(t, base.Delete(ctx, original))
		external := original.DeepCopy()
		external.UID = "foreign-replacement"
		external.ResourceVersion = ""
		external.Data["talosconfig"] = []byte("external-must-survive")
		external.Annotations = nil
		require.NoError(t, base.Create(ctx, external))
		race.onPatch = nil
	}
	require.Error(t, patchIdentitySnapshot(context.Background(), race, original, []byte("must-not-overwrite")))
	actual := &corev1.Secret{}
	require.NoError(t, base.Get(context.Background(), client.ObjectKeyFromObject(original), actual))
	require.Equal(t, []byte("external-must-survive"), actual.Data["talosconfig"])
	require.Equal(t, metav1.ObjectMeta{Namespace: actual.Namespace, Name: actual.Name}.Namespace, "tenant")
}
