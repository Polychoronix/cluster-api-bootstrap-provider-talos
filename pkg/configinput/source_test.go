// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package configinput_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	machineryconfig "github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
)

func TestSourcesReturnsOwnedOrderedEffectiveReferences(t *testing.T) {
	t.Parallel()
	cfg := inputConfig()
	cfg.Spec.ConfigSecretRef.Name = "unused-shared-baseline"
	cfg.Spec.StrategicPatchSecretRefs = []bootstrapv1.SecretKeyReference{{Name: "shared-one", Key: "config"}, {Name: "shared-two", Key: "config"}}
	cfg.Spec.MachineConfig = &bootstrapv1.MachineConfigOverlay{
		ConfigSecretRef:          &bootstrapv1.SecretKeyReference{Name: "machine-base", Key: "config"},
		StrategicPatchSecretRefs: []bootstrapv1.SecretKeyReference{{Name: "machine-patch", Key: "config"}},
	}
	sources := []*corev1.Secret{inputSecret("machine-base", []byte("baseline-private")), inputSecret("shared-one", []byte("shared-one-private")), inputSecret("shared-two", []byte("shared-two-private")), inputSecret("machine-patch", []byte("machine-private"))}
	objects := make([]client.Object, len(sources))
	want := make([]configinput.SourceIdentity, len(sources))
	for i, source := range sources {
		objects[i] = source
		want[i] = configinput.SourceIdentity{Name: source.Name, Key: "config", UID: source.UID, ResourceVersion: source.ResourceVersion, Digest: sha256.Sum256(source.Data["config"])}
	}
	reader := &recordingReader{Reader: inputClient(t, objects...)}
	resolved, err := configinput.Resolve(context.Background(), reader, cfg)
	require.NoError(t, err)
	got := resolved.Sources()
	assert.Equal(t, want, got)
	for i, key := range reader.keys {
		assert.Equal(t, "tenant", key.Namespace)
		assert.Equal(t, want[i].Name, key.Name)
	}
	ctx := ownerContext(t)
	before, err := resolved.Freshness(ctx)
	require.NoError(t, err)
	got[0].Name = "mutated-copy"
	got[0].UID = "mutated-uid"
	got[0].Digest[0] ^= 0xff
	got[0], got[1] = got[1], got[0]
	assert.Equal(t, want, resolved.Sources(), "returned receipt and fixed-size digest must not alias private snapshot")
	after, err := resolved.Freshness(ctx)
	require.NoError(t, err)
	assert.Equal(t, before, after)
	encoded, err := json.Marshal(want[0])
	require.NoError(t, err)
	var decoded configinput.SourceIdentity
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.Equal(t, want[0], decoded)
	var fixedDigest [32]byte = decoded.Digest
	assert.Equal(t, sha256.Sum256(sources[0].Data["config"]), fixedDigest)
}

func TestSourcesInlineBaselineAndGenerationBundleIdentities(t *testing.T) {
	t.Parallel()
	cfg := inputConfig()
	cfg.Spec.ConfigSecretRef = nil
	cfg.Spec.Data = "inline-private-baseline"
	resolved, err := configinput.Resolve(context.Background(), inputClient(t), cfg)
	require.NoError(t, err)
	assert.Empty(t, resolved.Sources(), "inline content is bound by spec, not a fabricated Secret receipt")
	cfg.Spec.Data = ""
	cfg.Spec.GenerateType = "controlplane"
	bundle, err := secrets.NewBundle(secrets.NewFixedClock(time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)), machineryconfig.TalosVersion1_14)
	require.NoError(t, err)
	encodedBundle, err := yaml.Marshal(bundle)
	require.NoError(t, err)
	source := inputSecret("cluster-talos", nil)
	source.Data = map[string][]byte{"bundle": encodedBundle, "private-extra": []byte(privateSentinel)}
	resolved, err = configinput.Resolve(context.Background(), inputClient(t, source), cfg)
	require.NoError(t, err)
	encodedData, err := json.Marshal(source.Data)
	require.NoError(t, err)
	assert.Equal(t, []configinput.SourceIdentity{{Name: source.Name, UID: source.UID, ResourceVersion: source.ResourceVersion, Digest: sha256.Sum256(encodedData)}}, resolved.Sources(), "generation identity binds complete Secret data, not an arbitrary single key")
}

func TestFreshnessHelperReturnsOwnedFrozenSnapshot(t *testing.T) {
	t.Parallel()
	ctx := ownerContext(t)
	cfg := inputConfig()
	source := inputSecret("base", []byte("private-baseline"))
	reader := inputClient(t, source)
	resolved, err := configinput.Resolve(context.Background(), reader, cfg)
	require.NoError(t, err)
	first, err := resolved.Freshness(ctx)
	require.NoError(t, err)
	want := append([]byte(nil), first...)
	first[0] ^= 0xff
	second, err := resolved.Freshness(ctx)
	require.NoError(t, err)
	assert.Equal(t, want, second, "caller mutation must not change subsequent freshness")
	cfg.Generation++
	cfg.Spec.ConfigRevision = "new-revision"
	live := &corev1.Secret{}
	require.NoError(t, reader.Get(context.Background(), client.ObjectKeyFromObject(source), live))
	live.Data["config"] = []byte("rotated-baseline")
	require.NoError(t, reader.Update(context.Background(), live))
	stillFrozen, err := resolved.Freshness(ctx)
	require.NoError(t, err)
	assert.Equal(t, want, stillFrozen, "Prepared receipt stays frozen despite caller/API changes")
	latest, err := configinput.Resolve(context.Background(), reader, cfg)
	require.NoError(t, err)
	freshness, err := latest.Freshness(ctx)
	require.NoError(t, err)
	assert.False(t, bytes.Equal(want, freshness), "new resolution binds current desired spec and sources")
	assert.NotContains(t, string(want), "private-baseline")
	assert.NotContains(t, string(freshness), "rotated-baseline")
}

func TestFreshnessHelperMatchesRenderedReceipt(t *testing.T) {
	t.Parallel()
	ctx := ownerContext(t)
	cfg := inputConfig()
	reader := inputClient(t, inputSecret("base", completeBaseline(t, ctx)))
	resolved, err := configinput.Resolve(context.Background(), reader, cfg)
	require.NoError(t, err)
	freshness, err := resolved.Freshness(ctx)
	require.NoError(t, err)
	rendered, err := configinput.Render(resolved, ctx)
	require.NoError(t, err)
	assert.Equal(t, rendered.Freshness(), freshness, "owner helper and renderer must use exactly the same private identity")
	output := &corev1.Secret{Data: map[string][]byte{"value": rendered.Data(), configinput.FreshnessKey: freshness}}
	fresh, err := configinput.IsFresh(context.Background(), reader, cfg, ctx, output)
	require.NoError(t, err)
	assert.True(t, fresh)
}

func TestFreshnessHelperRejectsMissingOwnersWithoutPrivateCause(t *testing.T) {
	t.Parallel()
	resolved, err := configinput.Resolve(context.Background(), inputClient(t, inputSecret("base", []byte(privateSentinel))), inputConfig())
	require.NoError(t, err)
	for _, missing := range []string{"machine", "cluster", "both"} {
		t.Run(missing, func(t *testing.T) {
			ctx := ownerContext(t)
			if missing != "cluster" {
				ctx.Machine = nil
			}
			if missing != "machine" {
				ctx.Cluster = nil
			}
			freshness, err := resolved.Freshness(ctx)
			require.Error(t, err)
			assert.Nil(t, freshness)
			assert.Nil(t, errors.Unwrap(err))
			assert.NotContains(t, fmt.Sprintf("%v %+v %#v", err, err, err), privateSentinel)
		})
	}
}
