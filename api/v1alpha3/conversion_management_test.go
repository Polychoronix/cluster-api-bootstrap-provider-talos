// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package v1alpha3_test

import (
	alpha "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1alpha3"
	beta "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/stretchr/testify/require"
	utilconversion "sigs.k8s.io/cluster-api/util/conversion"
	"testing"
)

func TestManageConsentServedRoundTrip(t *testing.T) {
	hub := &beta.TalosConfig{Spec: beta.TalosConfigSpec{GenerateType: "worker", Manage: true}}
	spoke := &alpha.TalosConfig{}
	require.NoError(t, spoke.ConvertFrom(hub))
	require.True(t, spoke.Spec.Manage)
	spoke.Spec.Manage = false
	for _, stash := range []bool{true, false} {
		if !stash {
			delete(spoke.Annotations, utilconversion.DataAnnotation)
		}
		restored := &beta.TalosConfig{}
		require.NoError(t, spoke.ConvertTo(restored))
		require.False(t, restored.Spec.Manage)
	}
}
