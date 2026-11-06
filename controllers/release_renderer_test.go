// This Source Code Form is subject to the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package controllers

import (
	"bytes"
	"encoding/json"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/configloader"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"testing"
)

// This executes the actual release controller/renderer with synthetic private
// material. It does not model the seed writer or contact a live cluster.
func TestStandaloneReleaseRendererPreservesPrivateMultidocIdentity(t *testing.T) {
	const private = "release-private-fixture-do-not-publish"
	const join = "https://[fd00::2]:6443"
	spec := bootstrapv1.TalosConfigSpec{GenerateType: "worker", TalosVersion: "v1.14", Manage: true, Hostname: bootstrapv1.HostnameSpec{Source: bootstrapv1.HostnameSourceMachineName}, TalosEndpoint: &bootstrapv1.TalosEndpoint{Address: "management-egress:50000", ServerName: "robot-test.example.test"}, StrategicPatchSecretRefs: []bootstrapv1.SecretKeyReference{{Name: "private-worker-patches", Key: "patch"}}}
	f := newImageFactoryFixture(t, spec, nil)
	machine := &clusterv1.Machine{}
	require.NoError(t, f.client.Get(t.Context(), types.NamespacedName{Namespace: testNamespace, Name: "machine-1"}, machine))
	machine.Spec.Version = "v1.35.0"
	machine.Spec.ProviderID = "hcloud://bm-2873475"
	machine.Spec.Bootstrap.ConfigRef = clusterv1.ContractVersionedObjectReference{APIGroup: bootstrapv1.GroupVersion.Group, Kind: "TalosConfig", Name: machine.Name}
	require.NoError(t, f.client.Update(t.Context(), machine))
	cfg := f.config(t)
	cfg.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(machine, clusterv1.GroupVersion.WithKind("Machine"))}
	require.NoError(t, f.client.Update(t.Context(), cfg))
	bundle, err := secrets.NewBundle(secrets.NewClock(), config.TalosVersion1_14)
	require.NoError(t, err)
	encoded, err := yaml.Marshal(bundle)
	require.NoError(t, err)
	require.NoError(t, f.client.Create(t.Context(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testClusterName + "-talos"}, Data: map[string][]byte{"bundle": encoded}}))
	patch := "apiVersion: v1alpha1\nkind: UnattendedInstallConfig\ninstaller:\n  image: factory.example/metal-installer/release-schematic:v1.14.2\nprovisioning:\n  diskSelector:\n    match: system_disk\n---\napiVersion: v1alpha1\nkind: KubeClusterConfig\nendpoint: " + join + "\n---\napiVersion: v1alpha1\nkind: RegistryAuthConfig\nname: ghcr.io\nusername: fixture\npassword: " + private + "\n---\napiVersion: v1alpha1\nkind: ExtensionServiceConfig\nname: tailscale\nenvironment:\n  - TS_AUTHKEY=" + private + "\n  - TS_HOSTNAME=robot-test\n"
	require.NoError(t, f.client.Create(t.Context(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: "private-worker-patches"}, Data: map[string][]byte{"patch": []byte(patch)}}))
	require.NoError(t, f.reconcile(t))
	cfg = f.config(t)
	output := f.secret(t, cfg.Status.DataSecretName)
	require.True(t, metav1.IsControlledBy(output, cfg))
	require.NotContains(t, output.Data, "acceptedIntent")
	provider, err := configloader.NewFromBytes(output.Data["value"])
	require.NoError(t, err)
	require.Equal(t, "worker", provider.Machine().Type().String())
	require.Equal(t, join, provider.K8sClusterConfig().ClusterEndpoint().String())
	require.Equal(t, machine.Name, provider.NetworkHostnameConfig().Hostname())
	require.GreaterOrEqual(t, len(provider.Documents()), 3)
	trusted := false
	for _, ca := range provider.Machine().Security().AcceptedCAs() {
		if ca != nil {
			trusted = trusted || bytes.Equal(bytes.TrimSpace(ca.Crt), bytes.TrimSpace(bundle.Certs.OS.Crt))
		}
	}
	if ca := provider.Machine().Security().IssuingCA(); ca != nil {
		trusted = trusted || bytes.Equal(bytes.TrimSpace(ca.Crt), bytes.TrimSpace(bundle.Certs.OS.Crt))
	}
	require.True(t, trusted, "worker retains the supplied cluster Talos CA")
	require.True(t, bytes.Contains(output.Data["value"], []byte(private)), "private patch reaches the renderer")
	visible, err := json.Marshal(cfg)
	require.NoError(t, err)
	require.False(t, bytes.Contains(visible, []byte(private)), "private material never enters the TalosConfig")
}
