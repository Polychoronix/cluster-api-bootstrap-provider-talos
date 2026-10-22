// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package inplace

import (
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"os"
	"path/filepath"
	"runtime"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"testing"
	"time"
)

func TestFinalizationDisposableAPIFrozenOutputAndSources(t *testing.T) {
	assets := os.Getenv("KUBEBUILDER_ASSETS")
	if assets == "" || testing.Short() {
		t.Skip("explicit disposable API assets required")
	}
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)
	repo := filepath.Dir(filepath.Dir(filepath.Dir(file)))
	cache := os.Getenv("GOMODCACHE")
	if cache == "" {
		home, err := os.UserHomeDir()
		require.NoError(t, err)
		cache = filepath.Join(home, "go/pkg/mod")
	}
	scheme := kruntime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, clusterv1.AddToScheme(scheme))
	require.NoError(t, bootstrapv1.AddToScheme(scheme))
	require.NoError(t, apiextensionsv1.AddToScheme(scheme))
	infraCRD := &apiextensionsv1.CustomResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: "metalmachines.infra.test"}, Spec: apiextensionsv1.CustomResourceDefinitionSpec{Group: "infra.test", Names: apiextensionsv1.CustomResourceDefinitionNames{Kind: "MetalMachine", Plural: "metalmachines"}, Scope: apiextensionsv1.NamespaceScoped, Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{Name: "v1beta1", Served: true, Storage: true, Schema: &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{Type: "object", XPreserveUnknownFields: ptr.To(true)}}}}}}
	environment := &envtest.Environment{UseExistingCluster: ptr.To(false), BinaryAssetsDirectory: assets, Scheme: scheme, CRDDirectoryPaths: []string{filepath.Join(repo, "config/crd/bases"), filepath.Join(cache, "sigs.k8s.io/cluster-api@v1.14.2/core/config/crd/bases")}, CRDs: []*apiextensionsv1.CustomResourceDefinition{infraCRD}, ErrorIfCRDPathMissing: true}
	cfg, err := environment.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, environment.Stop()) })
	real, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	require.NoError(t, real.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "test"}}))
	for _, testCase := range []string{"positive", "output", "source", "spec"} {
		t.Run(testCase, func(t *testing.T) {
			fixture := newDurableFixture(t, false)
			// Each case reuses actual local API resource names after bounded cleanup, no ambient cluster.
			cluster := fixture.cluster.DeepCopy()
			cluster.UID = ""
			cluster.ResourceVersion = ""
			require.NoError(t, real.Create(t.Context(), cluster))
			machine := fixture.machine.DeepCopy()
			machine.UID = ""
			machine.ResourceVersion = ""
			machine.Status = clusterv1.MachineStatus{}
			require.NoError(t, real.Create(t.Context(), machine))
			machine.Status.NodeRef = clusterv1.MachineNodeReference{Name: "node"}
			require.NoError(t, real.Status().Update(t.Context(), machine))
			config := fixture.config.DeepCopy()
			config.UID = ""
			config.ResourceVersion = ""
			config.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(machine, clusterv1.GroupVersion.WithKind("Machine"))}
			config.Status = bootstrapv1.TalosConfigStatus{}
			require.NoError(t, real.Create(t.Context(), config))
			config.Status.DataSecretName = "bootstrap"
			require.NoError(t, real.Status().Update(t.Context(), config))
			infra := &unstructured.Unstructured{}
			infra.SetAPIVersion("infra.test/v1beta1")
			infra.SetKind("MetalMachine")
			require.NoError(t, fixture.management.Get(t.Context(), client.ObjectKey{Namespace: "test", Name: "infra"}, infra))
			infra.SetUID("")
			infra.SetResourceVersion("")
			infra.SetOwnerReferences([]metav1.OwnerReference{*metav1.NewControllerRef(machine, clusterv1.GroupVersion.WithKind("Machine"))})
			require.NoError(t, real.Create(t.Context(), infra))
			generation := &corev1.Secret{}
			kube := &corev1.Secret{}
			for _, secret := range []*corev1.Secret{generation, kube} {
				name := "cluster-talos"
				if secret == kube {
					name = "cluster-kubeconfig"
				}
				require.NoError(t, fixture.management.Get(t.Context(), client.ObjectKey{Namespace: "test", Name: name}, secret))
				secret.UID = ""
				secret.ResourceVersion = ""
				require.NoError(t, real.Create(t.Context(), secret))
			}
			resolved, err := configinput.Resolve(t.Context(), real, config)
			require.NoError(t, err)
			rendered, err := configinput.Render(resolved, configinput.RenderContext{Machine: machine, Cluster: cluster})
			require.NoError(t, err)
			accepted, err := op.EncodeAcceptedIntent(op.AcceptedIntent{MachineUID: machine.UID, TalosConfigUID: config.UID, Image: op.ImageIdentity{}})
			require.NoError(t, err)
			output := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: "bootstrap", OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(config, bootstrapv1.GroupVersion.WithKind("TalosConfig"))}}, Data: map[string][]byte{"value": rendered.Data(), configinput.FreshnessKey: rendered.Freshness(), op.AcceptedIntentKey: accepted}}
			require.NoError(t, real.Create(t.Context(), output))
			handler := NewHandler(real, real, fixture.handler.newNodeClient)
			snapshot, ready, err := handler.snapshot(t.Context(), machine, kruntime.RawExtension{Object: config})
			require.NoError(t, err)
			require.True(t, ready)
			journal, err := handler.plan(t.Context(), snapshot, fixture.node.observation)
			require.NoError(t, err)
			now := metav1.NewTime(time.Now().UTC().Truncate(time.Second))
			deadline := metav1.NewTime(now.Add(time.Hour))
			journal.Steps[0] = op.OperationStep{Kind: op.Apply, Phase: op.StepObserved, IssuerID: "completed-operation", IssuedAt: &now, Deadline: &deadline, IssuedBootID: "boot-old", ObservedBootID: "boot-old", ApplyOutcome: op.NoReboot}
			original, err := op.CreateJournal(t.Context(), real, "test", journal)
			require.NoError(t, err)
			original, journal, err = op.ReadJournal(t.Context(), real, "test", journal.TalosConfig)
			require.NoError(t, err)
			// Mutations below are real API writes after recovery observation, before production finalization.
			switch testCase {
			case "output":
				output.Data["value"] = []byte("new-private-data")
				output.Data[configinput.FreshnessKey] = []byte("new-revision")
				require.NoError(t, real.Update(t.Context(), output))
			case "source":
				generation.Data["changed"] = []byte("new-source")
				require.NoError(t, real.Update(t.Context(), generation))
			case "spec":
				config.Spec.ConfigRevision = "changed-after-observation"
				require.NoError(t, real.Update(t.Context(), config))
			}
			err = handler.finalizeObserved(t.Context(), snapshot, original, journal)
			if testCase == "positive" {
				require.NoError(t, err)
				receipt, err := op.ReadReceipt(t.Context(), real, "test", journal.TalosConfig, journal.TransactionID)
				require.NoError(t, err)
				require.Equal(t, machine.UID, receipt.MachineUID)
			} else {
				require.Error(t, err)
				require.NoError(t, real.Get(t.Context(), client.ObjectKeyFromObject(output), output))
				require.Equal(t, accepted, output.Data[op.AcceptedIntentKey])
				live := &corev1.Secret{}
				require.NoError(t, real.Get(t.Context(), client.ObjectKeyFromObject(original), live))
				require.Empty(t, live.Data[op.ReceiptKey])
			}
			// Test-only resources are deleted directly; no provider/CAPH physical controller is installed.
			for _, obj := range []client.Object{original, output, generation, kube, infra, config, machine, cluster} {
				require.NoError(t, real.Delete(t.Context(), obj))
			}
		})
	}
}
