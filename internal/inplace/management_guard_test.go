// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package inplace

import (
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	runtimehooks "sigs.k8s.io/cluster-api/api/runtime/hooks/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"testing"
)

func TestMalformedActivationCannotFallBackToNativeSeed(t *testing.T) {
	f := newDurableFixture(t, false)
	record := &corev1.Secret{}
	require.NoError(t, f.management.Get(t.Context(), client.ObjectKey{Namespace: f.config.Namespace, Name: f.config.Name + "-management"}, record))
	record.Data["management"] = []byte("private-invalid-record")
	require.NoError(t, f.management.Update(t.Context(), record))
	_, _, err := f.handler.desiredConfig(t.Context(), &f.request.Desired.Machine, f.request.Desired.BootstrapConfig)
	require.Error(t, err)
	response := &runtimehooks.UpdateMachineResponse{}
	f.handler.DoUpdateMachine(t.Context(), f.request, response)
	require.Equal(t, runtimehooks.ResponseStatusFailure, response.Status)
	f.node.mu.Lock()
	defer f.node.mu.Unlock()
	require.Zero(t, f.node.applyCount)
}
