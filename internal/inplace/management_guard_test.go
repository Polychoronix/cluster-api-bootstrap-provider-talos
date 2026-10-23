// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package inplace

import (
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"testing"
)

func TestRuntimeDeletedManagementFieldsCannotBypassRetainedRecord(t *testing.T) {
	f := newUpdateFixture(t, "v1.14.0", "installer:v1.14.0", freshHash(t), "")
	tc := &bootstrapv1.TalosConfig{}
	require.NoError(t, f.handler.client.Get(t.Context(), client.ObjectKey{Namespace: "default", Name: "machine-1"}, tc))
	// Even a malformed/private orphan record must hard-hold instead of selecting
	// fresh accepted seed logic when spec/status management are absent.
	record := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: tc.Namespace, Name: tc.Name + "-management", Annotations: map[string]string{op.ProvenanceAnnotation: "talos-management"}, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(tc, bootstrapv1.GroupVersion.WithKind("TalosConfig"))}}, Data: map[string][]byte{"management": []byte("private-invalid-record")}}
	require.NoError(t, f.handler.client.Create(t.Context(), record))
	_, _, err := f.handler.desiredConfig(t.Context(), &f.request.Desired.Machine, f.request.Desired.BootstrapConfig)
	require.Error(t, err)
	machine := &clusterv1.Machine{}
	require.NoError(t, f.handler.client.Get(t.Context(), client.ObjectKey{Namespace: tc.Namespace, Name: "machine-1"}, machine))
	require.Error(t, f.handler.consumeMachineReceipt(t.Context(), machine))
}
