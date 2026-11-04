// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
)

const (
	baselineLabelPatch = `{"apiVersion":"v1alpha1","kind":"KubeNodeConfig","labels":{"node-role.kubernetes.io/storage":""}}`
	baselineOtherPatch = `{"apiVersion":"v1alpha1","kind":"KubeNodeConfig","labels":{"example.test/other":""}}`
)

// setAuthorityPolicy makes the fixture's TalosControlPlane intend `spec` for its single
// Machine, as the control-plane provider does when the rendered policy moves on.
func setAuthorityPolicy(t *testing.T, c client.Client, request ManagementObservationRequest, spec bootstrapv1.TalosConfigSpec) {
	t.Helper()
	tcp := &unstructured.Unstructured{}
	tcp.SetAPIVersion(request.Authority.APIVersion)
	tcp.SetKind(request.Authority.Kind)
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: request.Authority.Namespace, Name: request.Authority.Name}, tcp))
	shared, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&spec)
	require.NoError(t, err)
	tcp.Object["spec"].(map[string]any)["controlPlaneConfig"] = map[string]any{"controlplane": shared}
	require.NoError(t, c.Update(t.Context(), tcp))
}

// A node that still runs what its own TalosConfig rendered is accepted when the authority's
// effective policy has moved on: that is the state the owner's in-place update starts from.
func TestObservationAcceptsCurrentTalosConfigBaseline(t *testing.T) {
	c, request, _ := managementTLSFixture(t, 1)
	effective := request.TalosConfigs[0].DeepCopy()
	effective.Spec.StrategicPatches = append(effective.Spec.StrategicPatches, baselineLabelPatch)
	setAuthorityPolicy(t, c, request, effective.Spec)
	request.EffectiveTalosConfigs = []*bootstrapv1.TalosConfig{effective}

	observation, err := ObserveManagement(t.Context(), c, request)
	require.NoError(t, err)
	require.Equal(t, []string{"node-0"}, observation.Summary().CurrentBaselines)
	require.Equal(t, BaselineCurrent, observation.safety.Members[0].Baseline)

	// With the node already converged on the effective policy nothing is pending.
	effective.Spec.StrategicPatches = request.TalosConfigs[0].Spec.StrategicPatches
	setAuthorityPolicy(t, c, request, effective.Spec)
	observation, err = ObserveManagement(t.Context(), c, request)
	require.NoError(t, err)
	require.Empty(t, observation.Summary().CurrentBaselines)
	require.Equal(t, BaselineEffective, observation.safety.Members[0].Baseline)
}

// A node that matches neither the effective policy nor its own TalosConfig is drift to
// converge, not an activation blocker: the observation succeeds, records the member as
// drifted with the differing document identities (never content), and the owner's
// in-place update converges it.
func TestObservationActivatesDriftedNodeAndNamesDocuments(t *testing.T) {
	c, request, _ := managementTLSFixture(t, 1)
	live := request.TalosConfigs[0]
	stored := &bootstrapv1.TalosConfig{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(live), stored))
	stored.Spec.StrategicPatches = append(stored.Spec.StrategicPatches, baselineOtherPatch)
	require.NoError(t, c.Update(t.Context(), stored))
	live.Spec = stored.Spec
	effective := stored.DeepCopy()
	effective.Spec.StrategicPatches = append(append([]string{}, request.TalosConfigs[0].Spec.StrategicPatches[:len(request.TalosConfigs[0].Spec.StrategicPatches)-1]...), baselineLabelPatch)
	setAuthorityPolicy(t, c, request, effective.Spec)
	request.EffectiveTalosConfigs = []*bootstrapv1.TalosConfig{effective}

	observation, err := ObserveManagement(t.Context(), c, request)
	require.NoError(t, err)
	require.Equal(t, map[string][]string{"node-0": {"v1alpha1/KubeNodeConfig"}}, observation.Summary().Drifted)
	require.Empty(t, observation.Summary().CurrentBaselines)
	require.Equal(t, BaselineDrift, observation.safety.Members[0].Baseline)
	require.Equal(t, []string{"v1alpha1/KubeNodeConfig"}, observation.safety.Members[0].Drift)
	for _, id := range observation.safety.Members[0].Drift {
		require.NotContains(t, id, "node-role", "document content never leaks")
	}

	// Activation records the drift; the snapshot reports it back per Machine.
	tcp := &unstructured.Unstructured{}
	tcp.SetAPIVersion(request.Authority.APIVersion)
	tcp.SetKind(request.Authority.Kind)
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: request.Authority.Namespace, Name: request.Authority.Name}, tcp))
	tcp.Object["spec"].(map[string]any)["manage"] = true
	require.NoError(t, c.Update(t.Context(), tcp))
	request.Manage = true
	activated, err := ActivateManagement(t.Context(), c, c, request)
	require.NoError(t, err)
	require.True(t, activated.Activated)
	baseline, drift := MemberBaseline(t.Context(), c, request.Machines[0], request.TalosConfigs[0])
	require.Equal(t, BaselineDrift, baseline)
	require.Equal(t, []string{"v1alpha1/KubeNodeConfig"}, drift)
}

func TestConfigDiffIgnoresDocumentOrderAndNamesDifferences(t *testing.T) {
	a := "version: v1alpha1\nmachine:\n  type: controlplane\n  token: a\ncluster:\n  controlPlane:\n    endpoint: https://127.0.0.1:1\n---\napiVersion: v1alpha1\nkind: KubeProxyConfig\nenabled: false\n---\napiVersion: v1alpha1\nkind: VolumeConfig\nname: EPHEMERAL\nprovisioning:\n  maxSize: 10GiB\n"
	reordered := "apiVersion: v1alpha1\nkind: VolumeConfig\nname: EPHEMERAL\nprovisioning:\n  maxSize: 10GiB\n---\nversion: v1alpha1\nmachine:\n  type: controlplane\n  token: a\ncluster:\n  controlPlane:\n    endpoint: https://127.0.0.1:1\n---\napiVersion: v1alpha1\nkind: KubeProxyConfig\nenabled: false\n"
	changed := "version: v1alpha1\nmachine:\n  type: controlplane\n  token: a\ncluster:\n  controlPlane:\n    endpoint: https://127.0.0.1:1\n---\napiVersion: v1alpha1\nkind: KubeProxyConfig\nenabled: true\n---\napiVersion: v1alpha1\nkind: WatchdogTimerConfig\ndevice: /dev/watchdog0\n"

	same, err := managementConfigDiff([]byte(a), []byte(reordered))
	require.NoError(t, err)
	require.Empty(t, same)

	differing, err := managementConfigDiff([]byte(a), []byte(changed))
	require.NoError(t, err)
	require.Equal(t, []string{"v1alpha1/KubeProxyConfig", "v1alpha1/VolumeConfig/EPHEMERAL", "v1alpha1/WatchdogTimerConfig"}, differing)
	for _, key := range differing {
		require.NotContains(t, key, "token", "keys name documents, never content")
	}
}

func TestStageErrorDetailIsBounded(t *testing.T) {
	require.Equal(t, "Talos management x failed", managementError("x").Error())
	long := managementErrorDetail("x", strings.Repeat("a", 2*stageDetailLimit)).Error()
	require.LessOrEqual(t, len(long), len("Talos management x failed: ")+stageDetailLimit+2)
	require.True(t, strings.HasPrefix(long, "Talos management x failed: aaaa"))
	var stage *StageError
	require.True(t, errors.As(managementErrorDetail("active Talos trust identity", "d"), &stage))
	require.True(t, IsBaselineMismatch(stage), "identity-class failures stay hard blocks")
	require.False(t, IsBaselineMismatch(managementError("active configuration differs from baseline")), "configuration differences are drift, not a block")
}
