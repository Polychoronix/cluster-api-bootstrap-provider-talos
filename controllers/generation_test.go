// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/configloader"
	"github.com/siderolabs/talos/pkg/machinery/config/encoder"
	"github.com/siderolabs/talos/pkg/machinery/config/generate"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"
	"github.com/siderolabs/talos/pkg/machinery/nethelpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	capiv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	bootstrapv1beta1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
)

// secret returns a Secret from the fixture's cluster namespace.
func (f *imageFactoryFixture) secret(t *testing.T, name string) *corev1.Secret {
	t.Helper()

	secret := &corev1.Secret{}
	require.NoError(t, f.client.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: name}, secret))

	return secret
}

// setClusterNetwork records pod and service CIDRs on the fixture's Cluster.
func (f *imageFactoryFixture) setClusterNetwork(t *testing.T, pods, services string) {
	t.Helper()

	cluster := &capiv1.Cluster{}
	require.NoError(t, f.client.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: testClusterName}, cluster))

	cluster.Spec.ClusterNetwork.Pods.CIDRBlocks = []string{pods}
	cluster.Spec.ClusterNetwork.Services.CIDRBlocks = []string{services}

	require.NoError(t, f.client.Update(context.Background(), cluster))
}

// TestReconcileRendersClusterNetworkCIDRs pins the Cluster's pod and service CIDRs into the
// generated configuration, whichever configuration shape the Talos version contract uses.
func TestReconcileRendersClusterNetworkCIDRs(t *testing.T) {
	t.Parallel()

	for _, version := range []string{"v1.13", "v1.14"} {
		t.Run(version, func(t *testing.T) {
			t.Parallel()

			f := newImageFactoryFixture(t, bootstrapv1beta1.TalosConfigSpec{GenerateType: "worker", TalosVersion: version}, nil)
			f.setClusterNetwork(t, "10.244.0.0/16", "10.96.0.0/12")

			require.NoError(t, f.reconcile(t))

			data := f.bootstrapData(t)
			assert.Contains(t, data, "10.244.0.0/16", "pod CIDR must be rendered")
			assert.Contains(t, data, "10.96.0.0/12", "service CIDR must be rendered")
		})
	}
}

// TestReconcileWritesTheTalosconfigSecret checks that a control plane config, once its bootstrap
// data exists, publishes the cluster's talosconfig for the control plane provider to pick up.
func TestReconcileWritesTheTalosconfigSecret(t *testing.T) {
	t.Parallel()

	f := newImageFactoryFixture(t, bootstrapv1beta1.TalosConfigSpec{GenerateType: "controlplane", TalosVersion: "v1.14"}, nil)

	require.NoError(t, f.reconcile(t), "first reconcile renders the bootstrap data")
	require.NoError(t, f.reconcile(t), "second reconcile maintains the cluster-wide talosconfig")

	secret := f.secret(t, testClusterName+"-talosconfig")
	assert.NotEmpty(t, secret.Data["talosconfig"])
}

// userSuppliedConfig renders a complete control plane configuration for the given contract,
// the way an operator would hand one to CABPT with generateType "none".
func userSuppliedConfig(t *testing.T, contract *config.VersionContract) (string, *secrets.Bundle) {
	t.Helper()

	bundle, err := secrets.NewBundle(secrets.NewFixedClock(time.Now()), contract)
	require.NoError(t, err)

	input, err := generate.NewInput(testClusterName, "https://1.2.3.4:6443", "1.37.0",
		generate.WithVersionContract(contract), generate.WithSecretsBundle(bundle))
	require.NoError(t, err)

	cfg, err := input.Config(machine.TypeControlPlane)
	require.NoError(t, err)

	data, err := cfg.EncodeString(encoder.WithComments(encoder.CommentsDisabled))
	require.NoError(t, err)

	return data, bundle
}

// TestReconcileUserConfigWritesTheKubernetesCA covers generateType "none": CABPT must recover
// the Kubernetes CA from a user-supplied configuration whether the CA lives in the legacy
// v1alpha1 cluster block or in a KubeAPIServerCAConfig document.
func TestReconcileUserConfigWritesTheKubernetesCA(t *testing.T) {
	t.Parallel()

	for name, contract := range map[string]*config.VersionContract{
		"legacy v1alpha1": config.TalosVersion1_13,
		"multi-document":  config.TalosVersion1_14,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			data, bundle := userSuppliedConfig(t, contract)

			f := newImageFactoryFixture(t, bootstrapv1beta1.TalosConfigSpec{GenerateType: "none", Data: data}, nil)

			require.NoError(t, f.reconcile(t))

			ca := f.secret(t, testClusterName+"-ca")
			assert.Equal(t, bundle.Certs.K8s.Crt, ca.Data["tls.crt"], "Kubernetes CA certificate must come from the user config")
			assert.Equal(t, bundle.Certs.K8s.Key, ca.Data["tls.key"], "Kubernetes CA key must come from the user config")

			assert.NotEmpty(t, f.bootstrapData(t), "the user config must be handed through as bootstrap data")
		})
	}
}

// TestReconcileRendersTheMachineNameAsHostname covers hostname.source MachineName across the
// two configuration shapes: a HostnameConfig document from Talos 1.12 on, and the legacy
// machine.network.hostname field for older contracts.
func TestReconcileRendersTheMachineNameAsHostname(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		version  string
		document bool
	}{
		{version: "v1.11", document: false},
		{version: "v1.14", document: true},
		// An unset talosVersion means the current Talos, which understands the document.
		{version: "", document: true},
	} {
		t.Run("version="+tc.version, func(t *testing.T) {
			t.Parallel()

			f := newImageFactoryFixture(t, bootstrapv1beta1.TalosConfigSpec{
				GenerateType: "worker",
				TalosVersion: tc.version,
				Hostname:     bootstrapv1beta1.HostnameSpec{Source: bootstrapv1beta1.HostnameSourceMachineName},
			}, nil)

			require.NoError(t, f.reconcile(t))

			data := f.bootstrapData(t)

			// The document takes precedence over the legacy field when both are present, so ask
			// the configuration which hostname wins rather than grepping the YAML.
			provider, err := configloader.NewFromBytes([]byte(data))
			require.NoError(t, err)
			require.NotNil(t, provider.NetworkHostnameConfig())
			assert.Equal(t, "machine-1", provider.NetworkHostnameConfig().Hostname())

			if tc.document {
				assert.Contains(t, data, "kind: HostnameConfig")
				assert.Equal(t, nethelpers.AutoHostnameKindOff, provider.NetworkHostnameConfig().AutoHostname(), "an explicit hostname must switch automatic naming off")
			} else {
				assert.NotContains(t, data, "kind: HostnameConfig")
			}
		})
	}
}

// TestReconcileRendersTheInstallerImageAsADocument: on a 1.14 contract the resolved image lands
// in an UnattendedInstallConfig document with the system_disk selector, and no legacy block.
func TestReconcileRendersTheInstallerImageAsADocument(t *testing.T) {
	t.Parallel()

	f := newImageFactoryFixture(t, specWith("v1.14", "siderolabs/nvme-cli"), newFakeFactory())

	require.NoError(t, f.reconcile(t))

	provider, err := configloader.NewFromBytes([]byte(f.bootstrapData(t)))
	require.NoError(t, err)

	image := f.config(t).Status.ImageFactory.InstallerImage
	require.NotEmpty(t, image)
	require.NotNil(t, provider.UnattendedInstallConfig(), "a 1.14 contract must carry the document")
	assert.Equal(t, image, provider.UnattendedInstallConfig().InstallerImage())
	assert.Equal(t, "system_disk", provider.UnattendedInstallConfig().VolumeSelector().String())
	assert.Empty(t, provider.Machine().Install().Image(), "no legacy field alongside the document")
}

// TestReconcileLetsAStrategicPatchOverrideTheDiskSelector: CABPT's document is a default, so a
// user's patch on the same document wins.
func TestReconcileLetsAStrategicPatchOverrideTheDiskSelector(t *testing.T) {
	t.Parallel()

	spec := specWith("v1.14", "siderolabs/nvme-cli")
	spec.StrategicPatches = []string{"apiVersion: v1alpha1\nkind: UnattendedInstallConfig\nprovisioning:\n  diskSelector:\n    match: disk.dev_path == \"/dev/nvme0n1\"\n"}

	f := newImageFactoryFixture(t, spec, newFakeFactory())

	require.NoError(t, f.reconcile(t))

	provider, err := configloader.NewFromBytes([]byte(f.bootstrapData(t)))
	require.NoError(t, err)
	require.NotNil(t, provider.UnattendedInstallConfig())
	assert.Equal(t, `disk.dev_path == "/dev/nvme0n1"`, provider.UnattendedInstallConfig().VolumeSelector().String())
	assert.Equal(t, f.config(t).Status.ImageFactory.InstallerImage, provider.UnattendedInstallConfig().InstallerImage(), "the image survives the user's patch")
}

// TestReconcileWritesOnlyV1Beta2Conditions: the deprecated v1beta1 block is gone and the
// summary condition is maintained in the v1beta2 shape alone.
func TestReconcileWritesOnlyV1Beta2Conditions(t *testing.T) {
	t.Parallel()

	f := newImageFactoryFixture(t, bootstrapv1beta1.TalosConfigSpec{GenerateType: "worker", TalosVersion: "v1.14"}, nil)

	require.NoError(t, f.reconcile(t))

	cfg := f.config(t)
	require.NotEmpty(t, cfg.Status.Conditions)

	raw, err := json.Marshal(cfg.Status)
	require.NoError(t, err)
	assert.NotContains(t, string(raw), `"deprecated"`)
}
