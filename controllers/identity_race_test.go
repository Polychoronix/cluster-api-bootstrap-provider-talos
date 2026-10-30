// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package controllers

import (
	"context"
	"github.com/go-logr/logr"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"testing"
)

type identityRaceClient struct {
	client.Client
	onCreate func(context.Context, client.Object)
	onPatch  func(context.Context, client.Object)
}

func (c *identityRaceClient) Create(ctx context.Context, o client.Object, opts ...client.CreateOption) error {
	if c.onCreate != nil {
		c.onCreate(ctx, o)
	}
	return c.Client.Create(ctx, o, opts...)
}
func (c *identityRaceClient) Patch(ctx context.Context, o client.Object, p client.Patch, opts ...client.PatchOption) error {
	if c.onPatch != nil {
		c.onPatch(ctx, o)
	}
	return c.Client.Patch(ctx, o, p, opts...)
}
func TestIdentityConcurrentExternalCreationIsNeverTakenOver(t *testing.T) {
	f := newImageFactoryFixture(t, bootstrapv1.TalosConfigSpec{GenerateType: "controlplane", TalosVersion: "v1.14"}, nil)
	require.NoError(t, f.reconcile(t))
	scope := importedIdentityScope(t, f)
	base := f.client
	external := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testClusterName + "-talosconfig"}, Data: map[string][]byte{"talosconfig": []byte("external-bytes")}}
	race := &identityRaceClient{Client: base}
	race.onCreate = func(ctx context.Context, o client.Object) {
		if o.GetName() == external.Name {
			require.NoError(t, base.Create(ctx, external))
		}
	}
	f.reconciler.Client = race
	require.Error(t, f.reconciler.reconcileClientConfig(context.Background(), logr.Discard(), scope))
	actual := &corev1.Secret{}
	require.NoError(t, base.Get(context.Background(), client.ObjectKeyFromObject(external), actual))
	require.Equal(t, external.Data, actual.Data)
	require.Empty(t, actual.Annotations)
}
func TestCAConcurrentExternalCreationFailsMismatch(t *testing.T) {
	_, bundle := userSuppliedConfig(t, config.TalosVersion1_14)
	f := newImageFactoryFixture(t, bootstrapv1.TalosConfigSpec{GenerateType: "worker"}, nil)
	scope := importedIdentityScope(t, f)
	base := f.client
	external := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testClusterName + "-ca"}, Data: map[string][]byte{"tls.crt": []byte("foreign"), "tls.key": []byte("foreign")}}
	race := &identityRaceClient{Client: base}
	race.onCreate = func(ctx context.Context, o client.Object) {
		if o.GetName() == external.Name {
			require.NoError(t, base.Create(ctx, external))
		}
	}
	f.reconciler.Client = race
	require.Error(t, f.reconciler.writeK8sCASecret(context.Background(), scope, bundle.Certs.K8s))
	actual := &corev1.Secret{}
	require.NoError(t, base.Get(context.Background(), client.ObjectKeyFromObject(external), actual))
	require.Equal(t, external.Data, actual.Data)
}
