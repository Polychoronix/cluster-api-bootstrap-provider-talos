// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"testing"

	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/configloader"
	"github.com/siderolabs/talos/pkg/machinery/config/configpatcher"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testInstaller = "factory.talos.dev/metal-installer/abc:v1.14.0"

// minimalWorker is enough v1alpha1 for the strategic patcher to merge into.
const minimalWorker = "version: v1alpha1\nmachine:\n  type: worker\n  token: a.b\ncluster:\n  controlPlane:\n    endpoint: https://1.2.3.4:6443\n"

func applyInstallImagePatch(t *testing.T, contract *config.VersionContract) config.Provider {
	t.Helper()

	patch, err := configpatcher.LoadPatch([]byte(installImagePatch(contract, testInstaller)))
	require.NoError(t, err)

	out, err := configpatcher.Apply(configpatcher.WithBytes([]byte(minimalWorker)), []configpatcher.Patch{patch})
	require.NoError(t, err)

	data, err := out.Bytes()
	require.NoError(t, err)

	provider, err := configloader.NewFromBytes(data)
	require.NoError(t, err)

	return provider
}

func TestInstallImagePatchRendersTheDocumentFrom114(t *testing.T) {
	t.Parallel()

	for name, contract := range map[string]*config.VersionContract{
		"v1.14":   config.TalosVersion1_14,
		"current": config.TalosVersionCurrent,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			provider := applyInstallImagePatch(t, contract)

			require.NotNil(t, provider.UnattendedInstallConfig())
			assert.Equal(t, testInstaller, provider.UnattendedInstallConfig().InstallerImage())
			assert.Equal(t, "system_disk", provider.UnattendedInstallConfig().VolumeSelector().String())
			assert.Empty(t, provider.Machine().Install().Image(), "no legacy field alongside the document")
		})
	}
}

func TestInstallImagePatchRendersTheLegacyFieldBefore114(t *testing.T) {
	t.Parallel()

	provider := applyInstallImagePatch(t, config.TalosVersion1_13)

	assert.Nil(t, provider.UnattendedInstallConfig())
	assert.Equal(t, testInstaller, provider.Machine().Install().Image())
}

func TestVersionContractFor(t *testing.T) {
	t.Parallel()

	contract, err := versionContractFor("")
	require.NoError(t, err)
	assert.Nil(t, contract, "unset means the current Talos")

	contract, err = versionContractFor("v1.13")
	require.NoError(t, err)
	assert.Equal(t, config.TalosVersion1_13, contract)

	_, err = versionContractFor("nonsense")
	assert.Error(t, err)
}
