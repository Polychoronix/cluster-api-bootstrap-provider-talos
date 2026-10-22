// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package controllers

import (
	"context"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"testing"
)

func TestFreshSecretBaselineFreezesInitialIntentAndRetainsItOnRequest(t *testing.T) {
	data, _ := userSuppliedConfig(t, config.TalosVersion1_14)
	f := newImageFactoryFixture(t, bootstrapv1.TalosConfigSpec{GenerateType: "controlplane", TalosVersion: "v1.14", ConfigSecretRef: &bootstrapv1.SecretKeyReference{Name: "baseline", Key: "config"}}, nil)
	baseline := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: "baseline"}, Data: map[string][]byte{"config": []byte(data)}}
	require.NoError(t, f.client.Create(context.Background(), baseline))
	set := &clusterv1.MachineSet{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: "initial-set", UID: "set-uid"}, Spec: clusterv1.MachineSetSpec{Template: clusterv1.MachineTemplateSpec{Spec: clusterv1.MachineSpec{Bootstrap: clusterv1.Bootstrap{ConfigRef: clusterv1.ContractVersionedObjectReference{APIGroup: bootstrapv1.GroupVersion.Group, Kind: "TalosConfigTemplate", Name: "baseline-template"}}, InfrastructureRef: clusterv1.ContractVersionedObjectReference{APIGroup: "infra.test", Kind: "MetalMachineTemplate", Name: "infra-template"}}}}}
	require.NoError(t, f.client.Create(context.Background(), set))
	machine := &clusterv1.Machine{}
	require.NoError(t, f.client.Get(context.Background(), client.ObjectKey{Namespace: testNamespace, Name: "machine-1"}, machine))
	machine.Spec.Version = "v1.37.0"
	machine.Spec.Bootstrap.ConfigRef = clusterv1.ContractVersionedObjectReference{APIGroup: bootstrapv1.GroupVersion.Group, Kind: "TalosConfig", Name: "machine-1"}
	machine.Spec.InfrastructureRef = clusterv1.ContractVersionedObjectReference{APIGroup: "infra.test", Kind: "MetalMachine", Name: "infra"}
	machine.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(set, clusterv1.GroupVersion.WithKind("MachineSet"))}
	require.NoError(t, f.client.Update(context.Background(), machine))
	cfg := f.config(t)
	cfg.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(machine, clusterv1.GroupVersion.WithKind("Machine"))}
	cfg.Annotations = map[string]string{clusterv1.TemplateClonedFromNameAnnotation: "baseline-template", clusterv1.TemplateClonedFromGroupKindAnnotation: (schema.GroupKind{Group: bootstrapv1.GroupVersion.Group, Kind: "TalosConfigTemplate"}).String()}
	require.NoError(t, f.client.Update(context.Background(), cfg))
	require.NoError(t, apiextensionsv1.AddToScheme(f.reconciler.Scheme))
	crd := &apiextensionsv1.CustomResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: "metalmachines.infra.test"}, Spec: apiextensionsv1.CustomResourceDefinitionSpec{Group: "infra.test", Names: apiextensionsv1.CustomResourceDefinitionNames{Kind: "MetalMachine"}, Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{Name: "v1beta1", Served: true, Storage: true}}}}
	require.NoError(t, f.client.Create(t.Context(), crd))
	infra := &unstructured.Unstructured{}
	infra.SetAPIVersion("infra.test/v1beta1")
	infra.SetKind("MetalMachine")
	infra.SetName("infra")
	infra.SetNamespace(testNamespace)
	infra.SetUID("infra-uid")
	infra.SetOwnerReferences([]metav1.OwnerReference{*metav1.NewControllerRef(machine, clusterv1.GroupVersion.WithKind("Machine"))})
	infra.SetAnnotations(map[string]string{clusterv1.TemplateClonedFromNameAnnotation: "infra-template", clusterv1.TemplateClonedFromGroupKindAnnotation: (schema.GroupKind{Group: "infra.test", Kind: "MetalMachineTemplate"}).String()})
	require.NoError(t, f.client.Create(t.Context(), infra))
	require.NoError(t, f.reconcile(t))
	output := f.secret(t, "machine-1-bootstrap-data")
	initial := append([]byte(nil), output.Data[op.AcceptedIntentKey]...)
	intent, err := op.DecodeAcceptedIntent(initial)
	require.NoError(t, err)
	require.Equal(t, machine.UID, intent.MachineUID)
	cfg = f.config(t)
	cfg.Annotations[clusterv1.UpdateInProgressAnnotation] = ""
	cfg.Spec.ConfigRevision = "registry-only"
	cfg.Spec.StrategicPatches = []string{"apiVersion: v1alpha1\nkind: HostnameConfig\nauto: off\nhostname: reviewed-node\n"}
	require.NoError(t, f.client.Update(context.Background(), cfg))
	require.NoError(t, f.reconcile(t))
	output = f.secret(t, "machine-1-bootstrap-data")
	require.Equal(t, initial, output.Data[op.AcceptedIntentKey], "rendered request must not advance accepted image intent")
}
