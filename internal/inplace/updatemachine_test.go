// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package inplace

import (
	"context"
	"fmt"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	runtimehooks "sigs.k8s.io/cluster-api/api/runtime/hooks/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"
	"time"
)

type updateFixture struct {
	handler *Handler
	request *runtimehooks.UpdateMachineRequest
}

func newUpdateFixture(t *testing.T, runningVersion, configImage, secretHash, resolvedImage string) *updateFixture {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, clusterv1.AddToScheme(scheme))
	require.NoError(t, bootstrapv1.AddToScheme(scheme))
	machine := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "machine-1", UID: "machine-uid"}, Spec: clusterv1.MachineSpec{ClusterName: "test-cluster", Version: "v1.34.0", Bootstrap: clusterv1.Bootstrap{DataSecretName: ptr.To("original-seed"), ConfigRef: clusterv1.ContractVersionedObjectReference{APIGroup: bootstrapv1.GroupVersion.Group, Kind: "TalosConfig", Name: "machine-1"}}}, Status: clusterv1.MachineStatus{Addresses: []clusterv1.MachineAddress{{Type: clusterv1.MachineInternalIP, Address: "192.168.1.10"}}}}
	configObject := &bootstrapv1.TalosConfig{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "machine-1", UID: "config-uid", Generation: 1, Labels: map[string]string{clusterv1.ClusterNameLabel: "test-cluster"}}, Spec: bootstrapv1.TalosConfigSpec{GenerateType: "worker", TalosVersion: "v1.13"}, Status: bootstrapv1.TalosConfigStatus{DataSecretName: "machine-1-bootstrap-data"}}
	configObject.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(machine, clusterv1.GroupVersion.WithKind("Machine"))}
	if resolvedImage != "" {
		configObject.Status.ImageFactory = &bootstrapv1.ImageFactoryStatus{InstallerImage: resolvedImage}
	}
	cluster := &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "test-cluster", UID: "cluster-uid"}, Spec: clusterv1.ClusterSpec{ControlPlaneEndpoint: clusterv1.APIEndpoint{Host: "192.168.1.1", Port: 6443}}}
	bundle, err := secrets.NewBundle(secrets.NewFixedClock(time.Now()), config.TalosVersion1_13)
	require.NoError(t, err)
	encoded, err := yaml.Marshal(bundle)
	require.NoError(t, err)
	generation := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "test-cluster-talos", UID: "bundle-uid"}, Data: map[string][]byte{"bundle": encoded}}
	output := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: configObject.Status.DataSecretName, UID: "output-uid"}, Data: map[string][]byte{"value": []byte(machineConfigWithImage(configImage))}}
	accepted, err := op.EncodeAcceptedIntent(op.AcceptedIntent{MachineUID: machine.UID, TalosConfigUID: configObject.UID})
	require.NoError(t, err)
	seed := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: machine.Namespace, Name: "original-seed", OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(configObject, bootstrapv1.GroupVersion.WithKind("TalosConfig"))}}, Data: map[string][]byte{"value": []byte("original installed configuration"), op.AcceptedIntentKey: accepted}}
	management := fake.NewClientBuilder().WithScheme(scheme).WithObjects(machine, configObject, cluster, generation, output, seed).Build()
	if secretHash != "stale-hash" {
		inputs, err := configinput.Resolve(context.Background(), management, configObject)
		require.NoError(t, err)
		context := configinput.RenderContext{Machine: machine, Cluster: cluster, InstallerImage: resolvedImage}
		if resolvedImage != "" && secretHash == freshHash(t) {
			context.InstallerImage = ""
		}
		freshness, err := inputs.Freshness(context)
		require.NoError(t, err)
		output.Data[configinput.FreshnessKey] = freshness
		require.NoError(t, management.Update(t.Context(), output))
	}
	request := &runtimehooks.UpdateMachineRequest{Desired: runtimehooks.UpdateMachineRequestObjects{Machine: *machine.DeepCopy(), BootstrapConfig: runtime.RawExtension{Object: configObject.DeepCopy()}}}
	request.Desired.Machine.Status = clusterv1.MachineStatus{}
	handler := NewHandler(management, management, NewOperationNodeClient(management))
	return &updateFixture{handler: handler, request: request}
}
func freshHash(t *testing.T) string { t.Helper(); return "fresh-private-input" }
func machineConfigWithImage(image string) string {
	return fmt.Sprintf("version: v1alpha1\nmachine:\n  type: worker\n  token: aaaaaa.bbbbbbbbbbbbbbbb\n  install:\n    image: %s\ncluster:\n  controlPlane:\n    endpoint: https://192.168.1.1:6443\n  token: aaaaaa.bbbbbbbbbbbbbbbb\n", image)
}
func TestUpdateMachineWaitsForPrivateResolvedConfiguration(t *testing.T) {
	f := newUpdateFixture(t, "v1.13.0", "installer:v1.13.0", "stale-hash", "")
	response := &runtimehooks.UpdateMachineResponse{}
	f.handler.DoUpdateMachine(t.Context(), f.request, response)
	require.Equal(t, runtimehooks.ResponseStatusSuccess, response.Status)
	require.Positive(t, response.RetryAfterSeconds)
}
func TestRuntimeFreshnessPinsRecordedInstallerImage(t *testing.T) {
	image := "factory.example/metal-installer/schematic:v1.13.0"
	f := newUpdateFixture(t, "v1.13.0", image, "fresh-with-image", image)
	_, fresh, err := f.handler.desiredConfig(t.Context(), &f.request.Desired.Machine, f.request.Desired.BootstrapConfig)
	require.NoError(t, err)
	require.True(t, fresh)
	f = newUpdateFixture(t, "v1.13.0", image, freshHash(t), image)
	_, fresh, err = f.handler.desiredConfig(t.Context(), &f.request.Desired.Machine, f.request.Desired.BootstrapConfig)
	require.NoError(t, err)
	require.False(t, fresh)
}
func TestImageTag(t *testing.T) {
	for _, item := range []struct{ image, tag string }{{"registry.local:5000/installer:v1.13.0", "v1.13.0"}, {"installer@sha256:abc", ""}, {"installer:v1.14.0@sha256:abc", "v1.14.0"}} {
		require.Equal(t, item.tag, imageTag(item.image))
	}
}
