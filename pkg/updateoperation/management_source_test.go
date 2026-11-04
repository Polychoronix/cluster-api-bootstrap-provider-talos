// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v4"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
)

// withEndpoints rewrites a talosconfig's endpoint list, as CABPT does when control-plane
// membership changes, leaving the trust material untouched.
func withEndpoints(t *testing.T, data []byte, endpoints ...string) []byte {
	t.Helper()
	var config clientconfig.Config
	require.NoError(t, yaml.NewDecoder(bytes.NewReader(data)).Decode(&config))
	config.Contexts[config.Context].Endpoints = endpoints
	out, err := yaml.Marshal(&config)
	require.NoError(t, err)
	return out
}

func withCA(t *testing.T, data []byte, ca string) []byte {
	t.Helper()
	var config clientconfig.Config
	require.NoError(t, yaml.NewDecoder(bytes.NewReader(data)).Decode(&config))
	config.Contexts[config.Context].CA = ca
	out, err := yaml.Marshal(&config)
	require.NoError(t, err)
	return out
}

func TestTalosconfigSourcePinsTrustIdentityNotEndpoints(t *testing.T) {
	c, request, _ := managementTLSFixture(t, 1)
	identity := &corev1.Secret{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: "tenant", Name: "cluster-talosconfig"}, identity))
	recorded := managementIdentitySource(identity, "talosconfig")
	require.NotEqual(t, [32]byte{}, recorded.Identity)

	regenerated := identity.DeepCopy()
	regenerated.Data["talosconfig"] = withEndpoints(t, identity.Data["talosconfig"], "10.0.0.1", "10.0.0.2")
	require.NotEqual(t, identity.Data["talosconfig"], regenerated.Data["talosconfig"])
	require.True(t, sameManagementSource(regenerated, recorded), "endpoint regeneration keeps the record valid")

	swapped := identity.DeepCopy()
	swapped.Data["talosconfig"] = withCA(t, identity.Data["talosconfig"], "Zm9v")
	require.False(t, sameManagementSource(swapped, recorded), "a different CA invalidates the record")

	// Snapshots taken before the identity pin carry only the whole-data digest: they keep
	// validating against the unchanged Secret and fail on any change, as they always did.
	legacy := managementSource(identity, "talosconfig")
	require.Equal(t, [32]byte{}, legacy.Identity)
	require.True(t, sameManagementSource(identity, legacy))
	require.False(t, sameManagementSource(regenerated, legacy))

	// Through the activation record: regenerate endpoints after activation, the Machine
	// stays validly managed; swap the CA and it does not.
	tcp := &unstructured.Unstructured{}
	tcp.SetAPIVersion(request.Authority.APIVersion)
	tcp.SetKind(request.Authority.Kind)
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: request.Authority.Namespace, Name: request.Authority.Name}, tcp))
	tcp.Object["spec"].(map[string]any)["manage"] = true
	require.NoError(t, c.Update(t.Context(), tcp))
	request.Manage = true
	_, err := ActivateManagement(t.Context(), c, c, request)
	require.NoError(t, err)
	require.NoError(t, ValidateManagedMachine(t.Context(), c, request.Machines[0], request.TalosConfigs[0]))

	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(identity), identity))
	identity.Data["talosconfig"] = regenerated.Data["talosconfig"]
	require.NoError(t, c.Update(t.Context(), identity))
	require.NoError(t, ValidateManagedMachine(t.Context(), c, request.Machines[0], request.TalosConfigs[0]), "endpoint regeneration must not invalidate activation")

	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(identity), identity))
	identity.Data["talosconfig"] = swapped.Data["talosconfig"]
	require.NoError(t, c.Update(t.Context(), identity))
	err = ValidateManagedMachine(t.Context(), c, request.Machines[0], request.TalosConfigs[0])
	require.ErrorContains(t, err, "activated management source changed")
}
