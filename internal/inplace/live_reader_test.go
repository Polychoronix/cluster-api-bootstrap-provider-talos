// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package inplace

import (
	"context"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"testing"
)

type liveConfigReader struct{ client.Reader }

func (r liveConfigReader) Get(ctx context.Context, key client.ObjectKey, o client.Object, opts ...client.GetOption) error {
	if err := r.Reader.Get(ctx, key, o, opts...); err != nil {
		return err
	}
	if cfg, ok := o.(*bootstrapv1.TalosConfig); ok {
		cfg.Spec.ConfigRevision = "actual-new-intent"
		cfg.Generation++
	}
	return nil
}
func TestRuntimeDoesNotTrustCachedDesiredConfiguration(t *testing.T) {
	f := newUpdateFixture(t, "v1.13.0", "ghcr.io/siderolabs/installer:v1.13.0", freshHash(t), "")
	f.handler.reader = liveConfigReader{Reader: f.handler.client}
	data, fresh, err := f.handler.desiredConfig(context.Background(), &f.request.Desired.Machine, f.request.Desired.BootstrapConfig)
	require.NoError(t, err)
	require.False(t, fresh)
	require.Empty(t, data)
}
