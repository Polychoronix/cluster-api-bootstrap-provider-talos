// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v1alpha3_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilconversion "sigs.k8s.io/cluster-api/util/conversion"

	alpha "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1alpha3"
	beta "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
)

func TestManagementServedRoundTrip(t *testing.T) {
	hub := &beta.TalosConfig{
		Spec: beta.TalosConfigSpec{Management: &beta.ManagementSpec{
			Mode: beta.ManagementModeManaged, InventoryRevision: "inventory-1",
			Machines: []beta.MachineIdentityReference{{Name: "machine", UID: "uid-1"}},
			Approval: &beta.ObservationApproval{Revision: "inventory-1", ObservationID: "observation-1"},
		}},
		Status: beta.TalosConfigStatus{
			LastHandledReconcileAt: "request-1",
			Management: &beta.ManagementStatus{ActivatedRevision: "inventory-1", Observation: &beta.ObservationStatus{
				Revision: "inventory-1", ID: "observation-1", ObservedAt: metav1.NewTime(time.Unix(100, 0).UTC()),
			}},
		},
	}
	spoke := &alpha.TalosConfig{}
	require.NoError(t, spoke.ConvertFrom(hub))
	assert.Equal(t, alpha.ManagementModeManaged, spoke.Spec.Management.Mode)
	// Edits from a served alpha client must win over conversion-data.
	spoke.Spec.Management.InventoryRevision = "inventory-2"
	spoke.Spec.Management.Approval.Revision = "inventory-2"
	spoke.Spec.Management.Approval.ObservationID = "observation-2"
	spoke.Status.Management.Observation.ID = "observation-2"
	spoke.Status.Management.ActivatedRevision = "inventory-2"
	spoke.Status.LastHandledReconcileAt = "request-2"
	for _, stash := range []bool{true, false} {
		if !stash {
			delete(spoke.Annotations, utilconversion.DataAnnotation)
		}
		restored := &beta.TalosConfig{}
		require.NoError(t, spoke.ConvertTo(restored))
		assert.Equal(t, "inventory-2", restored.Spec.Management.InventoryRevision)
		assert.Equal(t, "observation-2", restored.Spec.Management.Approval.ObservationID)
		assert.Equal(t, "observation-2", restored.Status.Management.Observation.ID)
		assert.Equal(t, hub.Status.Management.Observation.ObservedAt, restored.Status.Management.Observation.ObservedAt)
		assert.Equal(t, "inventory-2", restored.Status.Management.ActivatedRevision)
		assert.Equal(t, "request-2", restored.Status.LastHandledReconcileAt)
	}
}
