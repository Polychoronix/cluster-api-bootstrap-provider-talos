// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"testing"
)

func TestReadManagementStatusAbsentRetainedAndMalformed(t *testing.T) {
	c, a, o := managementStoreFixture(t)
	_, err := ReadManagementStatus(t.Context(), c, a)
	require.ErrorIs(t, err, ErrManagementRecordNotFound)
	first, err := RecordManagementObservation(t.Context(), c, c, o)
	require.NoError(t, err)
	tc := &bootstrapv1.TalosConfig{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: a.Namespace, Name: a.Name}, tc))
	tc.Spec.Management = nil
	tc.Status.Management = nil
	require.NoError(t, c.Update(t.Context(), tc))
	retained, err := ReadManagementStatus(t.Context(), c, a)
	require.NoError(t, err)
	require.Equal(t, first.Observation.ID, retained.Observation.ID)
	require.Empty(t, retained.ActivatedRevision)
	secret, _, _, err := readManagement(t.Context(), c, a)
	require.NoError(t, err)
	secret.Data[managementRecordKey] = []byte("malformed-private")
	require.NoError(t, c.Update(t.Context(), secret))
	_, err = ReadManagementStatus(t.Context(), c, a)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrManagementRecordNotFound)
}
