// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package inplace

import (
	"fmt"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"testing"
	"time"
)

func TestSameMachineRegistryThenImageUpdateConsumesPriorReceiptAutomatically(t *testing.T) {
	f := newDurableFixture(t, false)
	require.Eventually(t, func() bool { return f.poll(t).RetryAfterSeconds == 0 }, 5*time.Second, time.Millisecond)
	_, first := f.journal(t)
	machine := f.machine.DeepCopy()
	require.NoError(t, f.management.Get(t.Context(), client.ObjectKeyFromObject(machine), machine))
	delete(machine.Annotations, clusterv1.UpdateInProgressAnnotation)
	require.NoError(t, f.management.Update(t.Context(), machine))
	cfg := f.config.DeepCopy()
	require.NoError(t, f.management.Get(t.Context(), client.ObjectKeyFromObject(cfg), cfg))
	delete(cfg.Annotations, clusterv1.UpdateInProgressAnnotation)
	require.NoError(t, f.management.Update(t.Context(), cfg))
	infra := &unstructured.Unstructured{}
	infra.SetAPIVersion("infra.test/v1beta1")
	infra.SetKind("MetalMachine")
	require.NoError(t, f.management.Get(t.Context(), client.ObjectKey{Namespace: cfg.Namespace, Name: "infra"}, infra))
	infra.SetAnnotations(map[string]string{})
	require.NoError(t, f.management.Update(t.Context(), infra))
	require.NoError(t, f.handler.consumeMachineReceipt(t.Context(), machine))
	secret, _ := f.journal(t)
	require.NotEmpty(t, secret.Data[op.ConsumedReceiptKey])
	machine.Annotations[clusterv1.UpdateInProgressAnnotation] = ""
	require.NoError(t, f.management.Update(t.Context(), machine))
	cfg.Annotations[clusterv1.UpdateInProgressAnnotation] = ""
	cfg.Generation++
	cfg.Spec.ConfigRevision = "image-revision"
	cfg.Spec.StrategicPatches = []string{fmt.Sprintf("apiVersion: v1alpha1\nkind: UnattendedInstallConfig\ninstaller:\n  image: factory.example/metal-installer/new-schematic:v1.14.0\nprovisioning:\n  diskSelector:\n    match: system_disk\n")}
	require.NoError(t, f.management.Update(t.Context(), cfg))
	resolved, err := configinput.Resolve(t.Context(), f.management, cfg)
	require.NoError(t, err)
	rendered, err := configinput.Render(resolved, configinput.RenderContext{Machine: machine, Cluster: f.cluster})
	require.NoError(t, err)
	output := &corev1.Secret{}
	require.NoError(t, f.management.Get(t.Context(), client.ObjectKeyFromObject(f.output), output))
	output.Data["value"] = rendered.Data()
	output.Data[configinput.FreshnessKey] = rendered.Freshness()
	require.NoError(t, f.management.Update(t.Context(), output))
	f.request.Desired.Machine = *machine
	f.request.Desired.BootstrapConfig = runtime.RawExtension{Object: cfg}
	require.Eventually(t, func() bool { return f.poll(t).RetryAfterSeconds == 0 }, 5*time.Second, time.Millisecond)
	secret, second := f.journal(t)
	require.NotEqual(t, first.TransactionID, second.TransactionID)
	require.Equal(t, first.Machine.UID, second.Machine.UID)
	require.Equal(t, op.Observed, second.Phase)
	require.NotEmpty(t, secret.Data[op.LastReceiptKey])
	f.node.mu.Lock()
	defer f.node.mu.Unlock()
	require.Equal(t, 2, f.node.applyCount)
	require.Equal(t, 1, f.node.installCount)
}
