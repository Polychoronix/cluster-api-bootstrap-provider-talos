// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"testing"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestManagementActualEtcdRejectsLearnerDuplicateAndUnseen(t *testing.T) {
	for _, kind := range []string{"learner", "duplicate", "unseen"} {
		t.Run(kind, func(t *testing.T) {
			c, request, servers := managementTLSFixture(t, 1)
			switch kind {
			case "learner":
				servers[0].members[0].IsLearner = true
			case "duplicate":
				servers[0].members = append(servers[0].members, servers[0].members[0])
			case "unseen":
				servers[0].members = append(servers[0].members, &machineapi.EtcdMember{Id: 9, Hostname: "unknown", PeerUrls: []string{"https://unknown:2380"}, ClientUrls: []string{"https://unknown:2379"}})
			}
			observed, e := ObserveManagement(t.Context(), c, request)
			require.Error(t, e)
			require.Nil(t, observed)
		})
	}
}
func TestManagementApprovalRejectsStaleSourceAndUID(t *testing.T) {
	for _, kind := range []string{"source", "MachineUID", "TCUID", "approval"} {
		t.Run(kind, func(t *testing.T) {
			c, request, _ := managementTLSFixture(t, 1)
			o, e := ObserveManagement(t.Context(), c, request)
			require.NoError(t, e)
			status, e := RecordManagementObservation(t.Context(), c, c, o)
			require.NoError(t, e)
			request.Spec.Mode = bootstrapv1.ManagementModeManaged
			request.Spec.Approval = &bootstrapv1.ObservationApproval{Revision: request.Spec.InventoryRevision, ObservationID: status.Observation.ID}
			tcp := &unstructured.Unstructured{}
			tcp.SetAPIVersion(request.Authority.APIVersion)
			tcp.SetKind(request.Authority.Kind)
			require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: "tenant", Name: "controlplane"}, tcp))
			raw, e := runtime.DefaultUnstructuredConverter.ToUnstructured(request.Spec)
			require.NoError(t, e)
			tcp.Object["spec"] = map[string]any{"management": raw}
			require.NoError(t, c.Update(t.Context(), tcp))
			switch kind {
			case "source":
				s := &corev1.Secret{}
				require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: "tenant", Name: "node-0-baseline"}, s))
				s.Data["config"] = append(s.Data["config"], []byte("\n# changed input identity\n")...)
				require.NoError(t, c.Update(t.Context(), s))
			case "MachineUID":
				m := request.Machines[0].DeepCopy()
				require.NoError(t, c.Delete(t.Context(), m))
				m.ResourceVersion = ""
				m.UID = ""
				require.NoError(t, c.Create(t.Context(), m))
			case "TCUID":
				tc := request.TalosConfigs[0].DeepCopy()
				require.NoError(t, c.Delete(t.Context(), tc))
				tc.ResourceVersion = ""
				tc.UID = ""
				require.NoError(t, c.Create(t.Context(), tc))
			case "approval":
				request.Spec.Approval.ObservationID = "wrong"
				raw, e = runtime.DefaultUnstructuredConverter.ToUnstructured(request.Spec)
				require.NoError(t, e)
				tcp.Object["spec"] = map[string]any{"management": raw}
				require.NoError(t, c.Update(t.Context(), tcp))
			}
			_, e = ActivateManagement(t.Context(), c, c, request)
			require.Error(t, e)
		})
	}
}
