// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package configinput_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/configloader"
	"github.com/siderolabs/talos/pkg/machinery/config/configpatcher"
	"github.com/siderolabs/talos/pkg/machinery/config/encoder"
	"github.com/siderolabs/talos/pkg/machinery/config/generate"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
)

const privateSentinel = "private-config-sentinel-never-public"

func ownerContext(t *testing.T) configinput.RenderContext {
	t.Helper()
	cluster := &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "tenant", UID: "cluster-uid", Generation: 4}}
	cluster.Spec.ControlPlaneEndpoint = clusterv1.APIEndpoint{Host: "api.example.com", Port: 6443}
	cluster.Spec.ClusterNetwork.ServiceDomain = "example.internal"
	cluster.Spec.ClusterNetwork.Pods.CIDRBlocks = []string{"10.244.0.0/16"}
	cluster.Spec.ClusterNetwork.Services.CIDRBlocks = []string{"10.96.0.0/12"}
	owner := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Name: "node", Namespace: "tenant", UID: "machine-uid"}}
	owner.Spec.Version = "v1.37.0"
	owner.Spec.InfrastructureRef.Name = "infra-node"
	return configinput.RenderContext{Machine: owner, Cluster: cluster}
}

func generatedBaseline(t *testing.T, ctx configinput.RenderContext, contract *config.VersionContract, role machine.Type) []byte {
	t.Helper()
	bundle, err := secrets.NewBundle(secrets.NewFixedClock(time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)), contract)
	require.NoError(t, err)
	input, err := generate.NewInput("cluster", "https://api.example.com:6443", "1.37.0",
		generate.WithVersionContract(contract), generate.WithSecretsBundle(bundle), generate.WithDNSDomain("example.internal"))
	require.NoError(t, err)
	input.PodNet = []string{"10.244.0.0/16"}
	input.ServiceNet = []string{"10.96.0.0/12"}
	provider, err := input.Config(role)
	require.NoError(t, err)
	data, err := provider.EncodeBytes(encoder.WithComments(encoder.CommentsDisabled))
	require.NoError(t, err)
	return data
}

func completeBaseline(t *testing.T, ctx configinput.RenderContext) []byte {
	t.Helper()
	data := generatedBaseline(t, ctx, config.TalosVersion1_14, machine.TypeControlPlane)
	patch, err := configpatcher.LoadPatch([]byte(hostnamePatch("imported-host")))
	require.NoError(t, err)
	patched, err := configpatcher.Apply(configpatcher.WithBytes(data), []configpatcher.Patch{patch})
	require.NoError(t, err)
	provider, err := patched.Config()
	require.NoError(t, err)
	data, err = provider.EncodeBytes(encoder.WithComments(encoder.CommentsDisabled))
	require.NoError(t, err)
	return append(data, []byte(`
---
apiVersion: v1alpha1
kind: LinkConfig
name: enp0s1
up: true
mtu: 9000
addresses:
  - address: 192.168.1.100/24
  - address: 2001:db8::1/64
routes:
  - destination: 10.3.5.0/24
    gateway: 10.3.5.1
  - gateway: fe80::1
---
apiVersion: v1alpha1
kind: RegistryMirrorConfig
name: ghcr.io
endpoints:
  - url: https://mirror-one.example.com:5000
  - url: https://mirror-two.example.com:5000
skipFallback: true
---
apiVersion: v1alpha1
kind: RegistryAuthConfig
name: ghcr.io
username: imported-user
password: private-config-sentinel-never-public
---
apiVersion: v1alpha1
kind: UserVolumeConfig
name: ceph-data
provisioning:
  diskSelector:
    match: disk.transport == "nvme" && !system_disk
  minSize: 10GiB
  maxSize: 100GiB
filesystem:
  type: xfs
---
apiVersion: v1alpha1
kind: UserVolumeConfig
name: application-data
provisioning:
  diskSelector:
    match: disk.transport == "sata" && !system_disk
  minSize: 20GiB
  maxSize: 200GiB
filesystem:
  type: xfs
`)...)
}

func inputConfig() *bootstrapv1.TalosConfig {
	return &bootstrapv1.TalosConfig{ObjectMeta: metav1.ObjectMeta{Name: "config", Namespace: "tenant", UID: "config-uid", Generation: 2, Labels: map[string]string{clusterv1.ClusterNameLabel: "cluster"}},
		Spec: bootstrapv1.TalosConfigSpec{GenerateType: "none", TalosVersion: "v1.14", ConfigSecretRef: &bootstrapv1.SecretKeyReference{Name: "base", Key: "config"}}}
}

func inputSecret(name string, data []byte) *corev1.Secret {
	return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tenant", UID: types.UID("secret-" + name), ResourceVersion: "1"}, Data: map[string][]byte{"config": data}}
}

func inputClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, bootstrapv1.AddToScheme(scheme))
	require.NoError(t, clusterv1.AddToScheme(scheme))
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

func renderInput(t *testing.T, reader client.Reader, cfg *bootstrapv1.TalosConfig, ctx configinput.RenderContext) *configinput.Rendered {
	t.Helper()
	resolved, err := configinput.Resolve(context.Background(), reader, cfg)
	require.NoError(t, err)
	rendered, err := configinput.Render(resolved, ctx)
	require.NoError(t, err)
	return rendered
}

func documentValues(t *testing.T, data []byte) []interface{} {
	t.Helper()
	provider, err := configloader.NewFromBytes(data)
	require.NoError(t, err)
	values := make([]interface{}, 0, len(provider.Documents()))
	for _, document := range provider.Documents() {
		encoded, err := json.Marshal(document)
		require.NoError(t, err)
		var value interface{}
		require.NoError(t, json.Unmarshal(encoded, &value))
		values = append(values, value)
	}
	return values
}
