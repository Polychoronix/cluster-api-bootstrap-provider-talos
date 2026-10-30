// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"testing"
)

func TestManagementAssociationPendingRejectsStaleSourceAndForeignOwner(t *testing.T) {
	for _, kind := range []string{"source", "foreignOwner"} {
		t.Run(kind, func(t *testing.T) {
			c, r := approveManagementFixture(t, 2)
			prepared, err := PrepareManagementAssociation(t.Context(), c, c, r)
			require.NoError(t, err)
			switch kind {
			case "source":
				s := &corev1.Secret{}
				require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: r.Authority.Namespace, Name: "node-0-baseline"}, s))
				s.Data["config"] = append(s.Data["config"], []byte("\n# rotation\n")...)
				require.NoError(t, c.Update(t.Context(), s))
			case "foreignOwner":
				m := r.Machines[0].DeepCopy()
				require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(m), m))
				m.OwnerReferences = []metav1.OwnerReference{{APIVersion: r.Authority.APIVersion, Kind: r.Authority.Kind, Name: "foreign", UID: "foreign", Controller: new(true)}}
				require.NoError(t, c.Update(t.Context(), m))
			}
			_, err = PrepareManagementAssociation(t.Context(), c, c, r)
			require.Error(t, err)
			_, err = CompleteManagementAssociation(t.Context(), c, c, r, prepared)
			require.Error(t, err)
			require.Error(t, ValidateManagementActivation(t.Context(), c, r.Authority, r.Spec))
			_, record, _, err := readManagement(t.Context(), c, r.Authority)
			require.NoError(t, err)
			require.False(t, record.Activated)
			require.Equal(t, r.Spec.Approval.ObservationID, record.ObservationID)
		})
	}
}
