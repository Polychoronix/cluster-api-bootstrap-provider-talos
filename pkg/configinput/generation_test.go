// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package configinput_test

import (
	"context"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"testing"
	"time"
)

func TestGenerationSnapshotPinsExactlyRenderedBundle(t *testing.T) {
	ctx := ownerContext(t)
	cfg := inputConfig()
	cfg.Spec.ConfigSecretRef = nil
	cfg.Spec.GenerateType = "controlplane"
	old, e := secrets.NewBundle(secrets.NewFixedClock(time.Now()), config.TalosVersion1_14)
	require.NoError(t, e)
	data, e := yaml.Marshal(old)
	require.NoError(t, e)
	source := inputSecret("cluster-talos", nil)
	source.Data = map[string][]byte{"bundle": data}
	reader := inputClient(t, source)
	resolved, e := configinput.Resolve(context.Background(), reader, cfg)
	require.NoError(t, e)
	newer, e := secrets.NewBundle(secrets.NewFixedClock(time.Now()), config.TalosVersion1_14)
	require.NoError(t, e)
	data, e = yaml.Marshal(newer)
	require.NoError(t, e)
	live := &corev1.Secret{}
	require.NoError(t, reader.Get(context.Background(), client.ObjectKey{Namespace: "tenant", Name: "cluster-talos"}, live))
	live.Data["bundle"] = data
	require.NoError(t, reader.Update(context.Background(), live))
	rendered, e := configinput.Render(resolved, ctx)
	require.NoError(t, e)
	output := &corev1.Secret{Data: map[string][]byte{"value": rendered.Data(), configinput.FreshnessKey: rendered.Freshness()}}
	fresh, e := configinput.IsFresh(context.Background(), reader, cfg, ctx, output)
	require.NoError(t, e)
	require.False(t, fresh, "old bundle snapshot cannot claim latest source freshness")
	require.Contains(t, string(rendered.Data()), old.Secrets.BootstrapToken)
	require.NotContains(t, string(rendered.Data()), newer.Secrets.BootstrapToken)
}
