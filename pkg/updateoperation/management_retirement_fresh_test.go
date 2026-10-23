// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"testing"
)

func TestFreshNativeDeletingAuthorityRequiresProvedAcceptedSeed(t *testing.T) {
	c, r, _ := managementTLSFixture(t, 1)
	tcp := &unstructured.Unstructured{}
	tcp.SetAPIVersion(r.Authority.APIVersion)
	tcp.SetKind(r.Authority.Kind)
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: r.Authority.Namespace, Name: r.Authority.Name}, tcp))
	tcp.Object["spec"] = map[string]any{}
	require.NoError(t, c.Update(t.Context(), tcp))
	m := r.Machines[0].DeepCopy()
	m.OwnerReferences = []metav1.OwnerReference{{APIVersion: r.Authority.APIVersion, Kind: r.Authority.Kind, Name: r.Authority.Name, UID: r.Authority.UID, Controller: new(true)}}
	require.NoError(t, c.Update(t.Context(), m))
	deletingManagementAuthority(t, c, r.Authority)
	require.Error(t, ValidateManagementRetirement(t.Context(), c, r.Authority), "custom seed is not fresh native lineage")
	tc := r.TalosConfigs[0]
	seed := &corev1.Secret{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: m.Namespace, Name: *m.Spec.Bootstrap.DataSecretName}, seed))
	seed.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(tc, bootstrapv1.GroupVersion.WithKind("TalosConfig"))}
	intent, err := EncodeAcceptedIntent(AcceptedIntent{MachineUID: m.UID, TalosConfigUID: tc.UID})
	require.NoError(t, err)
	seed.Data[AcceptedIntentKey] = intent
	require.NoError(t, c.Update(t.Context(), seed))
	require.NoError(t, ValidateManagementRetirement(t.Context(), c, r.Authority))
}
