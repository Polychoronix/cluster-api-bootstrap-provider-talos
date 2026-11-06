// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"testing"

	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
)

func TestSecretBaselineControllerPreservesIdentityAndPrivateOutput(t *testing.T) {
	t.Parallel()
	data, bundle := userSuppliedConfig(t, config.TalosVersion1_14)
	f := newImageFactoryFixture(t, bootstrapv1.TalosConfigSpec{GenerateType: "controlplane", TalosVersion: "v1.14", ConfigSecretRef: &bootstrapv1.SecretKeyReference{Name: "baseline", Key: "value"}}, nil)
	baseline := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: "baseline"}, Data: map[string][]byte{"value": []byte(data)}}
	require.NoError(t, f.client.Create(context.Background(), baseline))
	machine := &clusterv1.Machine{}
	require.NoError(t, f.client.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: "machine-1"}, machine))
	machine.Spec.Version = "v1.37.0"
	require.NoError(t, f.client.Update(context.Background(), machine))
	require.NoError(t, f.reconcile(t))
	output := f.secret(t, "machine-1-bootstrap-data")
	require.NotEmpty(t, output.Data[configinput.FreshnessKey])
	require.NotContains(t, output.Annotations, bootstrapv1.InPlaceConfigHashAnnotation)
	ca := f.secret(t, testClusterName+"-ca")
	require.Equal(t, bundle.Certs.K8s.Crt, ca.Data["tls.crt"])
	require.Equal(t, bundle.Certs.K8s.Key, ca.Data["tls.key"])
	identity := f.secret(t, testClusterName+"-talosconfig")
	identity.Annotations = nil // Existing imported credentials never get retroactive provenance.
	identity.OwnerReferences = nil
	require.NoError(t, f.client.Update(context.Background(), identity))
	before := identity.DeepCopy()
	require.NoError(t, f.reconcile(t))
	after := f.secret(t, testClusterName+"-talosconfig")
	require.Equal(t, before.Data, after.Data)
	require.Equal(t, before.OwnerReferences, after.OwnerReferences)
	require.Equal(t, before.Annotations, after.Annotations)
}

func TestAdoptedOriginalSeedIsNeverRenderedOutput(t *testing.T) {
	t.Parallel()
	seed := existingSecret("original-installed-seed", "")
	r, scope := newSecretsScope(t, true, seed)
	scope.Config.UID = types.UID("adopted-config")
	scope.ConfigOwner.Object["spec"].(map[string]interface{})["bootstrap"] = map[string]interface{}{"dataSecretName": bootstrapSecretName}
	scope.Config.Status.DataSecretName = bootstrapSecretName
	name, err := r.writeBootstrapData(context.Background(), scope, []byte("requested-update"), "not-public", []byte("private-freshness"))
	require.NoError(t, err)
	require.NotEqual(t, bootstrapSecretName, name)
	actual := &corev1.Secret{}
	require.NoError(t, r.Client.Get(context.Background(), client.ObjectKey{Namespace: scope.Config.Namespace, Name: bootstrapSecretName}, actual))
	require.Equal(t, []byte("original-installed-seed"), actual.Data["value"])
	require.Empty(t, actual.Data[configinput.FreshnessKey])
	require.NoError(t, r.Client.Get(context.Background(), client.ObjectKey{Namespace: scope.Config.Namespace, Name: name}, actual))
	require.Equal(t, []byte("requested-update"), actual.Data["value"])
	require.Equal(t, []byte("private-freshness"), actual.Data[configinput.FreshnessKey])
	require.Empty(t, actual.Annotations)
}
