// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package configinput_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
)

func TestFreshnessBindsLiveInputsAndIndependentRevisions(t *testing.T) {
	t.Parallel()
	cases := []string{"shared revision", "machine revision", "both revisions", "config UID", "config generation", "machine UID", "cluster UID", "cluster generation", "cluster endpoint", "cluster network", "machine version", "installer", "Secret rotation", "Secret resourceVersion", "Secret replacement UID", "patch order", "missing source"}
	for _, change := range cases {
		t.Run(change, func(t *testing.T) {
			ctx := ownerContext(t)
			cfg := inputConfig()
			cfg.Spec.ConfigRevision = "shared-1"
			cfg.Spec.MachineConfig = &bootstrapv1.MachineConfigOverlay{ConfigRevision: "machine-1"}
			cfg.Spec.StrategicPatchSecretRefs = []bootstrapv1.SecretKeyReference{{Name: "patch-one", Key: "config"}, {Name: "patch-two", Key: "config"}}
			reader := inputClient(t, cfg, inputSecret("base", completeBaseline(t, ctx)), inputSecret("patch-one", []byte(hostnamePatch("one"))), inputSecret("patch-two", []byte(hostnamePatch("two"))))
			rendered := renderInput(t, reader, cfg, ctx)
			output := &corev1.Secret{Data: map[string][]byte{"value": rendered.Data(), configinput.FreshnessKey: rendered.Freshness()}}
			fresh, err := configinput.IsFresh(context.Background(), reader, cfg, ctx, output)
			require.NoError(t, err)
			require.True(t, fresh)
			switch change {
			case "shared revision":
				cfg.Spec.ConfigRevision = "shared-2"
			case "machine revision":
				cfg.Spec.MachineConfig.ConfigRevision = "machine-2"
			case "both revisions":
				cfg.Spec.ConfigRevision = "shared-2"
				cfg.Spec.MachineConfig.ConfigRevision = "machine-2"
			case "config UID":
				cfg.UID = "replacement-config-uid"
			case "config generation":
				cfg.Generation++
			case "machine UID":
				ctx.Machine.UID = "replacement-machine-uid"
			case "cluster UID":
				ctx.Cluster.UID = "replacement-cluster-uid"
			case "cluster generation":
				ctx.Cluster.Generation++
			case "cluster endpoint":
				ctx.Cluster.Spec.ControlPlaneEndpoint.Host = "new-api.example.com"
			case "cluster network":
				ctx.Cluster.Spec.ClusterNetwork.Pods.CIDRBlocks = []string{"172.16.0.0/16"}
			case "machine version":
				ctx.Machine.Spec.Version = "v1.37.1"
			case "installer":
				ctx.InstallerImage = "factory.talos.dev/metal-installer/abc:v1.14.0"
			case "patch order":
				cfg.Spec.StrategicPatchSecretRefs[0], cfg.Spec.StrategicPatchSecretRefs[1] = cfg.Spec.StrategicPatchSecretRefs[1], cfg.Spec.StrategicPatchSecretRefs[0]
			case "Secret rotation", "Secret resourceVersion", "Secret replacement UID", "missing source":
				secret := &corev1.Secret{}
				require.NoError(t, reader.Get(context.Background(), types.NamespacedName{Namespace: "tenant", Name: "patch-one"}, secret))
				switch change {
				case "missing source":
					require.NoError(t, reader.Delete(context.Background(), secret))
				case "Secret replacement UID":
					require.NoError(t, reader.Delete(context.Background(), secret))
					secret.UID = "replacement-secret-uid"
					secret.ResourceVersion = ""
					require.NoError(t, reader.Create(context.Background(), secret))
				default:
					if change == "Secret rotation" {
						secret.Data["config"] = []byte(hostnamePatch("rotated-same-name"))
					} else {
						secret.Labels = map[string]string{"changed": "metadata-only"}
					}
					require.NoError(t, reader.Update(context.Background(), secret))
				}
			}
			// Read the latest desired config, as the normal controller caller does.
			if change == "config UID" {
				live := &bootstrapv1.TalosConfig{}
				require.NoError(t, reader.Get(context.Background(), types.NamespacedName{Namespace: cfg.Namespace, Name: cfg.Name}, live))
				require.NoError(t, reader.Delete(context.Background(), live))
				cfg.ResourceVersion = ""
				require.NoError(t, reader.Create(context.Background(), cfg))
			} else if change == "shared revision" || change == "machine revision" || change == "both revisions" || change == "config generation" || change == "patch order" {
				live := &bootstrapv1.TalosConfig{}
				require.NoError(t, reader.Get(context.Background(), types.NamespacedName{Namespace: cfg.Namespace, Name: cfg.Name}, live))
				live.Spec = cfg.Spec
				live.Generation = cfg.Generation
				require.NoError(t, reader.Update(context.Background(), live))
			}
			live := &bootstrapv1.TalosConfig{}
			require.NoError(t, reader.Get(context.Background(), types.NamespacedName{Namespace: cfg.Namespace, Name: cfg.Name}, live))
			fresh, err = configinput.IsFresh(context.Background(), reader, live, ctx, output)
			assert.False(t, fresh, change)
			if change == "missing source" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestFreshnessRejectsMissingOutputAndOwnerMetadata(t *testing.T) {
	t.Parallel()
	ctx := ownerContext(t)
	cfg := inputConfig()
	reader := inputClient(t, inputSecret("base", completeBaseline(t, ctx)))
	rendered := renderInput(t, reader, cfg, ctx)
	for _, output := range []*corev1.Secret{nil, {}, {Data: map[string][]byte{"value": rendered.Data()}}, {Data: map[string][]byte{configinput.FreshnessKey: rendered.Freshness()}}, {Data: map[string][]byte{"value": rendered.Data(), configinput.FreshnessKey: []byte("invalid")}}} {
		fresh, err := configinput.IsFresh(context.Background(), reader, cfg, ctx, output)
		require.NoError(t, err)
		assert.False(t, fresh)
	}
	output := &corev1.Secret{Data: map[string][]byte{"value": rendered.Data(), configinput.FreshnessKey: rendered.Freshness()}}
	for _, owner := range []string{"config", "machine", "cluster"} {
		cfg := cfg.DeepCopy()
		ctx := ctx
		ctx.Machine = ctx.Machine.DeepCopy()
		ctx.Cluster = ctx.Cluster.DeepCopy()
		switch owner {
		case "config":
			cfg.UID = ""
			cfg.Generation = 0
		case "machine":
			ctx.Machine.UID = ""
		case "cluster":
			ctx.Cluster.UID = ""
		}
		fresh, _ := configinput.IsFresh(context.Background(), reader, cfg, ctx, output)
		assert.False(t, fresh, "missing owner metadata must never pass")
	}
}
