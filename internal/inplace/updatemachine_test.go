// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package inplace

import (
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	runtimehooks "sigs.k8s.io/cluster-api/api/runtime/hooks/v1alpha1"
	"testing"
)

func TestUpdateMachineWaitsForPrivateResolvedConfiguration(t *testing.T) {
	f := newDurableFixture(t, false)
	f.output.Data[configinput.FreshnessKey] = []byte("stale-request")
	require.NoError(t, f.management.Update(t.Context(), f.output))
	response := &runtimehooks.UpdateMachineResponse{}
	f.handler.DoUpdateMachine(t.Context(), f.request, response)
	require.Equal(t, runtimehooks.ResponseStatusSuccess, response.Status)
	require.Positive(t, response.RetryAfterSeconds)
}
func TestRuntimeFreshnessPinsRecordedInstallerImage(t *testing.T) {
	f := newDurableFixture(t, false)
	image := "factory.example/metal-installer/schematic:v1.14.0"
	cfg := f.config.DeepCopy()
	cfg.Spec.ImageFactory = &bootstrapv1.ImageFactorySpec{}
	cfg.Status.ImageFactory = &bootstrapv1.ImageFactoryStatus{InstallerImage: image}
	cfg.Status.Conditions = append(cfg.Status.Conditions, metav1.Condition{Type: bootstrapv1.ImageFactoryResolvedCondition, Status: metav1.ConditionTrue, Reason: "FixtureResolved", ObservedGeneration: cfg.Generation, LastTransitionTime: metav1.Now()})
	f.request.Desired.BootstrapConfig = runtime.RawExtension{Object: cfg}
	require.NoError(t, f.management.Update(t.Context(), cfg))
	inputs, err := configinput.Resolve(t.Context(), f.management, cfg)
	require.NoError(t, err)
	context := configinput.RenderContext{Machine: f.machine, Cluster: f.cluster, InstallerImage: image}
	rendered, err := configinput.Render(inputs, context)
	require.NoError(t, err)
	f.output.Data = map[string][]byte{"value": rendered.Data(), configinput.FreshnessKey: rendered.Freshness()}
	require.NoError(t, f.management.Update(t.Context(), f.output))
	_, fresh, err := f.handler.desiredConfig(t.Context(), &f.request.Desired.Machine, f.request.Desired.BootstrapConfig)
	require.NoError(t, err)
	require.True(t, fresh)
	context.InstallerImage = ""
	fingerprint, err := inputs.Freshness(context)
	require.NoError(t, err)
	f.output.Data[configinput.FreshnessKey] = fingerprint
	require.NoError(t, f.management.Update(t.Context(), f.output))
	_, fresh, err = f.handler.desiredConfig(t.Context(), &f.request.Desired.Machine, f.request.Desired.BootstrapConfig)
	require.NoError(t, err)
	require.False(t, fresh)
}
func TestImageTag(t *testing.T) {
	for _, item := range []struct{ image, tag string }{{"registry.local:5000/installer:v1.13.0", "v1.13.0"}, {"installer@sha256:abc", ""}, {"installer:v1.14.0@sha256:abc", "v1.14.0"}} {
		require.Equal(t, item.tag, imageTag(item.image))
	}
}
