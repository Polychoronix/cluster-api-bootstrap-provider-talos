// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package configinput_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
)

type recordingReader struct {
	client.Reader
	keys []types.NamespacedName
}

func (r *recordingReader) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	r.keys = append(r.keys, key)
	return r.Reader.Get(ctx, key, object, options...)
}

func TestResolveNamespaceFenceAndEffectiveBaseline(t *testing.T) {
	t.Parallel()
	ctx := ownerContext(t)
	cfg := inputConfig()
	cfg.Spec.ConfigSecretRef.Name = "unused-missing-shared"
	cfg.Spec.MachineConfig = &bootstrapv1.MachineConfigOverlay{ConfigSecretRef: &bootstrapv1.SecretKeyReference{Name: "machine-base", Key: "config"}}
	cfg.Spec.StrategicPatchSecretRefs = []bootstrapv1.SecretKeyReference{{Name: "shared-patch", Key: "config"}}
	cfg.Spec.MachineConfig.StrategicPatchSecretRefs = []bootstrapv1.SecretKeyReference{{Name: "machine-patch", Key: "config"}}
	reader := &recordingReader{Reader: inputClient(t, inputSecret("machine-base", completeBaseline(t, ctx)), inputSecret("shared-patch", []byte(hostnamePatch("shared"))), inputSecret("machine-patch", []byte(hostnamePatch("machine"))))}
	resolved, err := configinput.Resolve(context.Background(), reader, cfg)
	require.NoError(t, err)
	assert.True(t, resolved.HasBaseline())
	assert.Equal(t, []types.NamespacedName{{Namespace: "tenant", Name: "machine-base"}, {Namespace: "tenant", Name: "shared-patch"}, {Namespace: "tenant", Name: "machine-patch"}}, reader.keys)
	// Cross-namespace same-name data is never used as a fallback.
	foreign := inputSecret("base", completeBaseline(t, ctx))
	foreign.Namespace = "foreign"
	_, err = configinput.Resolve(context.Background(), inputClient(t, foreign), inputConfig())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "lookup")
	for _, name := range []string{"foreign/base", "/etc/talos", "{{ .Machine.Name }}"} {
		cfg := inputConfig()
		cfg.Spec.ConfigSecretRef.Name = name
		reader := &recordingReader{Reader: inputClient(t)}
		_, err := configinput.Resolve(context.Background(), reader, cfg)
		require.Error(t, err)
		assert.Empty(t, reader.keys, "invalid names must be rejected before a read")
	}
}

func TestResolveAndRenderFailuresAreStageSafe(t *testing.T) {
	t.Parallel()
	ctx := ownerContext(t)
	for _, location := range []string{"baseline", "shared patch", "machine patch"} {
		for _, state := range []string{"missing Secret", "missing key", "empty", "whitespace", "invalid"} {
			t.Run(location+"/"+state, func(t *testing.T) {
				cfg := inputConfig()
				objects := []client.Object{}
				if location != "baseline" {
					objects = append(objects, inputSecret("base", completeBaseline(t, ctx)))
				}
				ref := bootstrapv1.SecretKeyReference{Name: "target", Key: "config"}
				switch location {
				case "baseline":
					cfg.Spec.ConfigSecretRef = &ref
				case "shared patch":
					cfg.Spec.StrategicPatchSecretRefs = []bootstrapv1.SecretKeyReference{ref}
				case "machine patch":
					cfg.Spec.MachineConfig = &bootstrapv1.MachineConfigOverlay{StrategicPatchSecretRefs: []bootstrapv1.SecretKeyReference{ref}}
				}
				secret := inputSecret("target", nil)
				switch state {
				case "missing key":
					secret.Data = map[string][]byte{"other": []byte(privateSentinel)}
				case "whitespace":
					secret.Data["config"] = []byte(" \n\t")
				case "invalid":
					secret.Data["config"] = []byte("machine: [" + privateSentinel)
				}
				if state != "missing Secret" {
					objects = append(objects, secret)
				}
				resolved, err := configinput.Resolve(context.Background(), inputClient(t, objects...), cfg)
				if state == "invalid" {
					require.NoError(t, err)
					_, err = configinput.Render(resolved, ctx)
				}
				require.Error(t, err)
				assert.Nil(t, errors.Unwrap(err), "private parser causes must never be unwrap-able")
				assert.NotContains(t, fmt.Sprintf("%v %+v %#v", err, err, err), privateSentinel)
			})
		}
	}
}

func TestResolveSnapshotDoesNotObserveLaterDesiredOrSecretMutation(t *testing.T) {
	t.Parallel()
	ctx := ownerContext(t)
	cfg := inputConfig()
	cfg.Spec.StrategicPatches = []string{hostnamePatch("captured")}
	reader := inputClient(t, inputSecret("base", completeBaseline(t, ctx)))
	resolved, err := configinput.Resolve(context.Background(), reader, cfg)
	require.NoError(t, err)
	cfg.Spec.StrategicPatches[0] = hostnamePatch("later-desired")
	secret := &corev1.Secret{}
	require.NoError(t, reader.Get(context.Background(), types.NamespacedName{Namespace: "tenant", Name: "base"}, secret))
	secret.Data["config"] = []byte("invalid-later-" + privateSentinel)
	require.NoError(t, reader.Update(context.Background(), secret))
	output, err := configinput.Render(resolved, ctx)
	require.NoError(t, err)
	assert.Equal(t, "captured", renderedHostname(t, output))
}

func TestPublicBaselineCompletenessAndExclusivity(t *testing.T) {
	t.Parallel()
	for _, data := range []string{"", " \n\t"} {
		cfg := inputConfig()
		cfg.Spec.ConfigSecretRef = nil
		cfg.Spec.Data = data
		_, err := configinput.Resolve(context.Background(), inputClient(t), cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty")
	}
	ctx := ownerContext(t)
	cfg := inputConfig()
	cfg.Spec.ConfigSecretRef = nil
	cfg.Spec.Data = string(completeBaseline(t, ctx))
	output := renderInput(t, inputClient(t), cfg, ctx)
	assert.ElementsMatch(t, documentValues(t, []byte(cfg.Spec.Data)), documentValues(t, output.Data()))
	cfg.Spec.ConfigSecretRef = &bootstrapv1.SecretKeyReference{Name: "base", Key: "config"}
	_, err := configinput.Resolve(context.Background(), inputClient(t), cfg)
	require.Error(t, err)
	assert.NotContains(t, fmt.Sprintf("%v %+v %#v", err, err, err), privateSentinel)
}
