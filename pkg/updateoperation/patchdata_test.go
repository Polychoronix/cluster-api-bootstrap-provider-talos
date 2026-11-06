// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type failingPatchClient struct {
	client.Client
	err error
}

func (c failingPatchClient) Patch(context.Context, client.Object, client.Patch, ...client.PatchOption) error {
	return c.err
}

// Only a lost compare-and-swap is a conflict; an interrupted or otherwise rejected
// write must not read as a concurrent writer.
func TestPatchDataClassifiesFailures(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	c := &assigningClient{Client: fake.NewClientBuilder().WithScheme(scheme).Build()}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "journal"}, Data: map[string][]byte{"k": []byte("v")}}
	require.NoError(t, c.Create(t.Context(), secret))
	stale := secret.DeepCopy()
	secret.Labels = map[string]string{"moved": "true"}
	require.NoError(t, c.Update(t.Context(), secret))

	require.Error(t, PatchData(t.Context(), c, stale, map[string][]byte{"k": []byte("w")}), "a moved resourceVersion fails the CAS")
	// What kube-apiserver answers when a JSON patch "test" op fails (handlers/patch.go).
	lost := apierrors.NewGenericServerResponse(http.StatusUnprocessableEntity, "", schema.GroupResource{}, "", "testing value /metadata/resourceVersion failed", 0, false)
	err := PatchData(t.Context(), failingPatchClient{Client: c, err: lost}, stale, nil)
	require.True(t, errors.Is(err, ErrSecretConflict), "%v", err)

	fresh := &corev1.Secret{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(secret), fresh))
	require.NoError(t, PatchData(t.Context(), c, fresh, map[string][]byte{"k": []byte("w")}))

	interrupted := failingPatchClient{Client: c, err: fmt.Errorf("Patch: %w", context.DeadlineExceeded)}
	err = PatchData(t.Context(), interrupted, fresh, nil)
	require.False(t, errors.Is(err, ErrSecretConflict))
	require.ErrorContains(t, err, "interrupted by the caller's deadline")

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	err = PatchData(canceled, failingPatchClient{Client: c, err: errors.New("transport closed")}, fresh, nil)
	require.False(t, errors.Is(err, ErrSecretConflict))
	require.ErrorContains(t, err, "interrupted")

	err = PatchData(t.Context(), failingPatchClient{Client: c, err: errors.New("secret text from server")}, fresh, nil)
	require.False(t, errors.Is(err, ErrSecretConflict))
	require.NotContains(t, err.Error(), "secret text")
}
