// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"testing"
)

func TestManagedAdoptedNodeUIDAndReviewedNextRequest(t *testing.T) {
	c, r, servers := managementTLSFixture(t, 1)
	o, err := ObserveManagement(t.Context(), c, r)
	require.NoError(t, err)
	status, err := RecordManagementObservation(t.Context(), c, c, o)
	require.NoError(t, err)
	r.Spec.Mode = "Managed"
	r.Spec.Approval = &bootstrapv1.ObservationApproval{Revision: r.Spec.InventoryRevision, ObservationID: status.Observation.ID}
	tcp := &unstructured.Unstructured{}
	tcp.SetAPIVersion(r.Authority.APIVersion)
	tcp.SetKind(r.Authority.Kind)
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: r.Authority.Namespace, Name: r.Authority.Name}, tcp))
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(r.Spec)
	require.NoError(t, err)
	tcp.Object["spec"].(map[string]any)["management"] = raw
	require.NoError(t, c.Update(t.Context(), tcp))
	_, err = associateAll(t, c, c, r)
	require.NoError(t, err)
	m := r.Machines[0].DeepCopy()
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(m), m))
	tc := r.TalosConfigs[0].DeepCopy()
	require.NoError(t, ValidateManagedMachine(t.Context(), c, m, tc))
	// A reviewed next target is not another initial-adoption approval.
	tcp.Object["spec"].(map[string]any)["version"] = "v1.38.0"
	require.NoError(t, c.Update(t.Context(), tcp))
	tc.Spec.ConfigRevision = "next-reviewed"
	require.NoError(t, c.Update(t.Context(), tc))
	baseline := &corev1.Secret{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: tc.Namespace, Name: tc.Spec.ConfigSecretRef.Name}, baseline))
	baseline.Data["config"] = append(baseline.Data["config"], []byte("\n# reviewed next source\n")...)
	require.NoError(t, c.Update(t.Context(), baseline))
	require.NoError(t, ValidateManagedMachine(t.Context(), c, m, tc))
	seed := &corev1.Secret{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: m.Namespace, Name: *m.Spec.Bootstrap.DataSecretName}, seed))
	seed.Annotations = map[string]string{"metadata-only": "changed"}
	require.NoError(t, c.Update(t.Context(), seed))
	require.NoError(t, ValidateManagedMachine(t.Context(), c, m, tc))
	servers[0].nodeUID = "recreated-same-name-node"
	require.Error(t, ValidateManagedMachine(t.Context(), c, m, tc))
}
