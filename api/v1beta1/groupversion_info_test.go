// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v1beta1_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"

	bootstrapv1beta1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
)

func TestAddToSchemeRegistersEveryKind(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, bootstrapv1beta1.AddToScheme(scheme))

	for _, kind := range []string{"TalosConfig", "TalosConfigList", "TalosConfigTemplate", "TalosConfigTemplateList"} {
		assert.True(t, scheme.Recognizes(bootstrapv1beta1.GroupVersion.WithKind(kind)), kind)
	}
}
