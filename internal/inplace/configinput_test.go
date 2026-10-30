// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package inplace

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
)

func TestRuntimeUsesLiveConfigOutputNotInstallationSeed(t *testing.T) {
	f := newUpdateFixture(t, "v1.13.0", "ghcr.io/siderolabs/installer:v1.13.0", freshHash(t), "")
	ctx := context.Background()
	machine := &clusterv1.Machine{}
	require.NoError(t, f.handler.client.Get(ctx, types.NamespacedName{Namespace: "default", Name: "machine-1"}, machine))
	machine.Spec.Bootstrap.DataSecretName = ptr.To("original-seed")
	require.NoError(t, f.handler.client.Update(ctx, machine))
	seed := &corev1.Secret{}
	require.NoError(t, f.handler.client.Get(ctx, types.NamespacedName{Namespace: "default", Name: "original-seed"}, seed))
	seed.Data["value"] = []byte("immutable-original-installation")
	require.NoError(t, f.handler.client.Update(ctx, seed))
	f.request.Desired.Machine.Spec.Bootstrap.DataSecretName = ptr.To(seed.Name)
	data, fresh, err := f.handler.desiredConfig(ctx, &f.request.Desired.Machine, f.request.Desired.BootstrapConfig)
	require.NoError(t, err)
	require.True(t, fresh)
	generated := &corev1.Secret{}
	require.NoError(t, f.handler.client.Get(ctx, types.NamespacedName{Namespace: "default", Name: "machine-1-bootstrap-data"}, generated))
	require.Equal(t, generated.Data["value"], data, "runtime and bootstrap consume identical rendered bytes")
	require.NotEqual(t, seed.Data["value"], data)
	config := &bootstrapv1.TalosConfig{}
	require.NoError(t, f.handler.client.Get(ctx, types.NamespacedName{Namespace: "default", Name: "machine-1"}, config))
	config.Generation++
	require.NoError(t, f.handler.client.Update(ctx, config))
	_, fresh, err = f.handler.desiredConfig(ctx, &f.request.Desired.Machine, f.request.Desired.BootstrapConfig)
	require.NoError(t, err)
	require.False(t, fresh, "a new desired generation must never apply old output")
	require.NoError(t, f.handler.client.Get(ctx, types.NamespacedName{Namespace: seed.Namespace, Name: seed.Name}, seed))
	require.Equal(t, []byte("immutable-original-installation"), seed.Data["value"])
	require.Empty(t, seed.Data[configinput.FreshnessKey])
}
