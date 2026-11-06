// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package configinput_test

import (
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"strings"
	"testing"
)

func TestStandardGenerationBundleRejectsMissingEmptyInvalidIncomplete(t *testing.T) {
	for _, data := range []map[string][]byte{{}, {"bundle": nil}, {"bundle": []byte(" ")}, {"bundle": []byte("certs: [" + privateSentinel)}, {"bundle": []byte("{}")}, {"certs": []byte("{}"), "kubeSecrets": []byte("{}"), "trustdInfo": []byte("{}")}} {
		secret := inputSecret("cluster-talos", nil)
		secret.Data = data
		bundle, err := configinput.DecodeGenerationSecret(secret, config.TalosVersion1_14)
		require.Error(t, err)
		require.Nil(t, bundle)
		require.True(t, strings.Contains(err.Error(), "tenant/cluster-talos/"))
		require.NotContains(t, err.Error(), privateSentinel)
	}
	_, err := configinput.DecodeGenerationSecret((*corev1.Secret)(nil), config.TalosVersion1_14)
	require.Error(t, err)
}
