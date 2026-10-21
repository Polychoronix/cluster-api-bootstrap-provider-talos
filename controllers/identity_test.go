// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	cryptox509 "github.com/siderolabs/crypto/x509"
	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/configloader"
	"github.com/siderolabs/talos/pkg/machinery/config/encoder"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v4"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/internal/configowner"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/talosclient"
)

const identityPrivateSentinel = "PRIVATE-IMPORTED-CONFIG-SENTINEL"

type identityCapturedLog struct {
	mutex sync.Mutex
	lines []string
}

func (capture *identityCapturedLog) logger() logr.Logger {
	return funcr.New(func(prefix, line string) {
		capture.mutex.Lock()
		defer capture.mutex.Unlock()
		capture.lines = append(capture.lines, prefix+line)
	}, funcr.Options{Verbosity: 10})
}

func (capture *identityCapturedLog) text() string {
	capture.mutex.Lock()
	defer capture.mutex.Unlock()

	return strings.Join(capture.lines, "\n")
}

func importedIdentityScope(t *testing.T, f *imageFactoryFixture) *TalosConfigScope {
	t.Helper()
	cfg := f.config(t)
	owner, err := configowner.GetConfigOwner(context.Background(), f.client, cfg)
	require.NoError(t, err)
	require.NotNil(t, owner)
	cluster := &clusterv1.Cluster{}
	require.NoError(t, f.client.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: testClusterName}, cluster))
	if cluster.UID == "" {
		cluster.UID = "imported-cluster-uid"
		require.NoError(t, f.client.Update(context.Background(), cluster))
	}

	return &TalosConfigScope{Config: cfg, ConfigOwner: owner, Cluster: cluster}
}

func importedIdentitySecret(t *testing.T, scope *TalosConfigScope, bundle *secrets.Bundle) *corev1.Secret {
	t.Helper()
	value, err := genTalosConfigFile(scope.Cluster.Name, bundle, []string{"original-api.example.test:50000", "original-second.example.test:50000"})
	require.NoError(t, err)

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: scope.Cluster.Name + "-talosconfig", Namespace: scope.Config.Namespace,
			Labels:      map[string]string{"external-identity": "true"},
			Annotations: map[string]string{"external-owner": "operator"},
		},
		Data: map[string][]byte{"talosconfig": []byte(value), "external-metadata": []byte(identityPrivateSentinel)},
	}
}

func assertImportedIdentityUnchanged(t *testing.T, f *imageFactoryFixture, before *corev1.Secret) {
	t.Helper()
	after := f.secret(t, before.Name)
	assert.True(t, bytes.Equal(before.Data["talosconfig"], after.Data["talosconfig"]), "external client bytes and endpoint ordering must remain exact")
	assert.True(t, bytes.Equal(before.Data["external-metadata"], after.Data["external-metadata"]), "external data keys must remain untouched")
	assert.Equal(t, before.OwnerReferences, after.OwnerReferences)
	assert.Equal(t, before.Labels, after.Labels)
	assert.Equal(t, before.Annotations, after.Annotations)
	assert.Equal(t, before.ResourceVersion, after.ResourceVersion, "external Secret must not be updated")
}

func TestImportedExternalIdentityNeverAcquiresProviderProvenance(t *testing.T) {
	for _, matchingOwnerAndBundle := range []bool{false, true} {
		name := "unowned external"
		if matchingOwnerAndBundle {
			name = "matching owner and bundle still external"
		}
		t.Run(name, func(t *testing.T) {
			data, bundle := userSuppliedConfig(t, config.TalosVersion1_14)
			f := newImageFactoryFixture(t, bootstrapv1.TalosConfigSpec{GenerateType: "controlplane", TalosVersion: "v1.14"}, nil)
			scope := importedIdentityScope(t, f)
			secret := importedIdentitySecret(t, scope, bundle)
			if matchingOwnerAndBundle {
				secret.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(scope.Cluster, clusterv1.GroupVersion.WithKind("Cluster"))}
				require.NoError(t, f.reconciler.writeSecretsBundleSecret(context.Background(), scope, testClusterName+"-talos", bundle))
			}
			require.NoError(t, f.client.Create(context.Background(), secret))
			before := f.secret(t, secret.Name).DeepCopy()
			assert.False(t, providerCreatedIdentity(before, scope))
			require.NoError(t, f.reconciler.reconcileBaselineIdentity(context.Background(), scope, []byte(data)))
			require.NoError(t, f.reconciler.reconcileClientConfig(context.Background(), f.reconciler.Log, scope))
			assertImportedIdentityUnchanged(t, f, before)
			assert.NotContains(t, f.secret(t, secret.Name).Annotations, identityCreatedAnnotation)
		})
	}
}

func TestImportedBaselineExtractsExactKubernetesCA(t *testing.T) {
	for _, contract := range []*config.VersionContract{config.TalosVersion1_13, config.TalosVersion1_14} {
		data, bundle := userSuppliedConfig(t, contract)
		f := newImageFactoryFixture(t, bootstrapv1.TalosConfigSpec{GenerateType: "controlplane"}, nil)
		scope := importedIdentityScope(t, f)
		require.NoError(t, f.client.Create(context.Background(), importedIdentitySecret(t, scope, bundle)))
		require.NoError(t, f.reconciler.reconcileBaselineIdentity(context.Background(), scope, []byte(data)))
		ca := f.secret(t, testClusterName+"-ca")
		assert.True(t, bytes.Equal(bundle.Certs.K8s.Crt, ca.Data["tls.crt"]), "Kubernetes issuing certificate must be extracted exactly")
		assert.True(t, bytes.Equal(bundle.Certs.K8s.Key, ca.Data["tls.key"]), "Kubernetes issuing key must be extracted exactly")
		before := ca.DeepCopy()
		require.NoError(t, f.reconciler.reconcileBaselineIdentity(context.Background(), scope, []byte(data)))
		assert.Equal(t, before.ResourceVersion, f.secret(t, ca.Name).ResourceVersion)
		missing := &corev1.Secret{}
		assert.True(t, apierrors.IsNotFound(f.client.Get(context.Background(), client.ObjectKey{Namespace: testNamespace, Name: testClusterName + "-talos"}, missing)), "baseline must not generate a new cluster identity bundle")
	}
}

func TestImportedWorkerWithoutIssuingKeyRequiresNamespaceLocalClient(t *testing.T) {
	data, bundle := userSuppliedConfig(t, config.TalosVersion1_14)
	provider, err := configloader.NewFromBytes([]byte(data))
	require.NoError(t, err)
	worker := provider.Clone()
	worker.RawV1Alpha1().MachineConfig.MachineType = machine.TypeWorker.String()
	worker.RawV1Alpha1().MachineConfig.MachineCA = nil
	worker.RawV1Alpha1().MachineConfig.MachineAcceptedCAs = []*cryptox509.PEMEncodedCertificate{{Crt: bundle.Certs.OS.Crt}}
	workerData, err := worker.EncodeBytes(encoder.WithComments(encoder.CommentsDisabled))
	require.NoError(t, err)
	f := newImageFactoryFixture(t, bootstrapv1.TalosConfigSpec{GenerateType: "worker"}, nil)
	scope := importedIdentityScope(t, f)
	err = f.reconciler.reconcileBaselineIdentity(context.Background(), scope, workerData)
	require.Error(t, err)
	assert.Equal(t, "configuration input cluster identity: imported management Secret required failed", err.Error())
	assert.Nil(t, errors.Unwrap(err))
	foreign := importedIdentitySecret(t, scope, bundle)
	foreign.Namespace = "other-tenant"
	require.NoError(t, f.client.Create(context.Background(), foreign))
	err = f.reconciler.reconcileBaselineIdentity(context.Background(), scope, workerData)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "imported management Secret required")
	local := importedIdentitySecret(t, scope, bundle)
	require.NoError(t, f.client.Create(context.Background(), local))
	before := f.secret(t, local.Name).DeepCopy()
	require.NoError(t, f.reconciler.reconcileBaselineIdentity(context.Background(), scope, workerData))
	worker.RawV1Alpha1().MachineConfig.MachineAcceptedCAs = nil
	withoutTrust, err := worker.EncodeBytes(encoder.WithComments(encoder.CommentsDisabled))
	require.NoError(t, err)
	err = f.reconciler.reconcileBaselineIdentity(context.Background(), scope, withoutTrust)
	require.Error(t, err)
	assert.Equal(t, "configuration input cluster identity: Talos accepted CA mismatch failed", err.Error())
	assert.Nil(t, errors.Unwrap(err))
	assertImportedIdentityUnchanged(t, f, before)
}

func TestGeneratedCreationMarkerPermitsMatchingIdentityManagement(t *testing.T) {
	f := newImageFactoryFixture(t, bootstrapv1.TalosConfigSpec{GenerateType: "controlplane", TalosVersion: "v1.14"}, nil)
	scope := importedIdentityScope(t, f)
	bundle, err := f.reconciler.getSecretsBundle(context.Background(), scope, true, config.TalosVersion1_14)
	require.NoError(t, err)
	require.NoError(t, f.reconciler.reconcileClientConfig(context.Background(), f.reconciler.Log, scope))
	identity := f.secret(t, testClusterName+"-talosconfig")
	assert.Equal(t, identityCreatedValue, identity.Annotations[identityCreatedAnnotation])
	assert.True(t, providerCreatedIdentity(identity, scope))
	ca, err := talosclient.ValidateIdentity(identity.Data["talosconfig"])
	require.NoError(t, err)
	assert.True(t, bytes.Equal(bundle.Certs.OS.Crt, ca), "new identity must use provider bundle CA")
	machineObject := &clusterv1.Machine{}
	require.NoError(t, f.client.Get(context.Background(), client.ObjectKey{Namespace: testNamespace, Name: "machine-1"}, machineObject))
	machineObject.Labels = map[string]string{clusterv1.ClusterNameLabel: testClusterName, clusterv1.MachineControlPlaneLabel: ""}
	machineObject.Status.Addresses = clusterv1.MachineAddresses{{Type: clusterv1.MachineInternalIP, Address: "10.42.0.19"}}
	require.NoError(t, f.client.Update(context.Background(), machineObject))
	before := bytes.Clone(identity.Data["talosconfig"])
	require.NoError(t, f.reconciler.reconcileClientConfig(context.Background(), f.reconciler.Log, scope))
	after := f.secret(t, identity.Name)
	assert.False(t, bytes.Equal(before, after.Data["talosconfig"]), "creation-marked identity endpoints must reconcile")
	assert.Contains(t, string(after.Data["talosconfig"]), "10.42.0.19")
	assert.Equal(t, identityCreatedValue, after.Annotations[identityCreatedAnnotation])
	ca, err = talosclient.ValidateIdentity(after.Data["talosconfig"])
	require.NoError(t, err)
	assert.True(t, bytes.Equal(bundle.Certs.OS.Crt, ca), "endpoint management must not replace CA")
	wrongOwner := after.DeepCopy()
	wrongOwner.OwnerReferences[0].UID = "different-cluster"
	assert.False(t, providerCreatedIdentity(wrongOwner, scope))
}

func TestImportedIdentityFailuresAreStageOnlyAndLeaveExternalSecretUntouched(t *testing.T) {
	for _, failure := range []string{"invalid external", "same CA invalid client", "Talos CA mismatch", "Kubernetes CA mismatch", "provider bundle mismatch"} {
		t.Run(failure, func(t *testing.T) {
			data, bundle := userSuppliedConfig(t, config.TalosVersion1_14)
			_, otherBundle := userSuppliedConfig(t, config.TalosVersion1_14)
			f := newImageFactoryFixture(t, bootstrapv1.TalosConfigSpec{GenerateType: "controlplane"}, nil)
			scope := importedIdentityScope(t, f)
			secret := importedIdentitySecret(t, scope, bundle)
			switch failure {
			case "invalid external":
				secret.Data["talosconfig"] = []byte("contexts: [" + identityPrivateSentinel)
			case "same CA invalid client":
				var config clientconfig.Config
				require.NoError(t, yaml.NewDecoder(bytes.NewReader(secret.Data["talosconfig"])).Decode(&config))
				config.Contexts[config.Context].Key = base64.StdEncoding.EncodeToString([]byte(identityPrivateSentinel))
				value, err := config.Bytes()
				require.NoError(t, err)
				secret.Data["talosconfig"] = value
			case "Talos CA mismatch":
				secret = importedIdentitySecret(t, scope, otherBundle)
			case "Kubernetes CA mismatch":
				require.NoError(t, f.client.Create(context.Background(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testClusterName + "-ca"}, Data: map[string][]byte{"tls.crt": []byte(identityPrivateSentinel), "tls.key": []byte(identityPrivateSentinel)}}))
			case "provider bundle mismatch":
				secret.Annotations[identityCreatedAnnotation] = identityCreatedValue
				secret.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(scope.Cluster, clusterv1.GroupVersion.WithKind("Cluster"))}
				require.NoError(t, f.reconciler.writeSecretsBundleSecret(context.Background(), scope, testClusterName+"-talos", otherBundle))
			}
			require.NoError(t, f.client.Create(context.Background(), secret))
			before := f.secret(t, secret.Name).DeepCopy()
			var err error
			if failure == "provider bundle mismatch" {
				err = f.reconciler.reconcileClientConfig(context.Background(), f.reconciler.Log, scope)
			} else {
				err = f.reconciler.reconcileBaselineIdentity(context.Background(), scope, []byte(data))
			}
			require.Error(t, err)
			assert.NotContains(t, err.Error(), identityPrivateSentinel)
			assert.Nil(t, errors.Unwrap(err))
			stage := map[string]string{
				"invalid external":         "talos client: parse failed",
				"same CA invalid client":   "talos client: client certificate validation failed",
				"Talos CA mismatch":        "configuration input cluster identity: Talos CA mismatch failed",
				"Kubernetes CA mismatch":   "configuration input cluster identity: Kubernetes CA mismatch failed",
				"provider bundle mismatch": "configuration input cluster identity: generated bundle mismatch failed",
			}[failure]
			assert.Equal(t, stage, err.Error())
			assertImportedIdentityUnchanged(t, f, before)
		})
	}
}

func TestImportedSecretFailureRedactsControllerPublicObjectsAndLogs(t *testing.T) {
	data, bundle := userSuppliedConfig(t, config.TalosVersion1_14)
	f := newImageFactoryFixture(t, bootstrapv1.TalosConfigSpec{GenerateType: "controlplane", TalosVersion: "v1.14", ConfigSecretRef: &bootstrapv1.SecretKeyReference{Name: "baseline", Key: "value"}, StrategicPatchSecretRefs: []bootstrapv1.SecretKeyReference{{Name: "private-patch", Key: "patch"}}}, nil)
	scope := importedIdentityScope(t, f)
	require.NoError(t, f.client.Create(context.Background(), importedIdentitySecret(t, scope, bundle)))
	require.NoError(t, f.client.Create(context.Background(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: "baseline"}, Data: map[string][]byte{"value": []byte(data)}}))
	patch := []byte("machine: [" + identityPrivateSentinel)
	require.NoError(t, f.client.Create(context.Background(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: "private-patch"}, Data: map[string][]byte{"patch": patch}}))
	capture := &identityCapturedLog{}
	f.reconciler.Log = capture.logger()
	err := f.reconcile(t)
	require.Error(t, err)
	public, marshalErr := json.Marshal(f.config(t))
	require.NoError(t, marshalErr)
	digest := sha256.Sum256(patch)
	for _, visible := range []string{err.Error(), string(public), capture.text()} {
		assert.NotContains(t, visible, identityPrivateSentinel)
		assert.NotContains(t, visible, hex.EncodeToString(digest[:]))
		assert.NotContains(t, visible, string(bundle.Certs.OS.Key))
	}
	assert.Empty(t, f.config(t).Status.DataSecretName)
	assert.Contains(t, err.Error(), "default/private-patch/patch")
}

func TestBaselineIssuedClientRemainsReadOnlyWithoutGeneratedBundle(t *testing.T) {
	data, bundle := userSuppliedConfig(t, config.TalosVersion1_14)
	f := newImageFactoryFixture(t, bootstrapv1.TalosConfigSpec{GenerateType: "controlplane", TalosVersion: "v1.14", ConfigSecretRef: &bootstrapv1.SecretKeyReference{Name: "baseline", Key: "value"}}, nil)
	scope := importedIdentityScope(t, f)
	require.NoError(t, f.reconciler.reconcileBaselineIdentity(context.Background(), scope, []byte(data)))
	identity := f.secret(t, testClusterName+"-talosconfig")
	assert.True(t, providerCreatedIdentity(identity, scope))
	ca, err := talosclient.ValidateIdentity(identity.Data["talosconfig"])
	require.NoError(t, err)
	assert.True(t, bytes.Equal(bundle.Certs.OS.Crt, ca), "baseline client must use exact imported Talos CA")
	before := identity.DeepCopy()
	require.NoError(t, f.reconciler.reconcileClientConfig(context.Background(), f.reconciler.Log, scope))
	assertImportedIdentityUnchanged(t, f, before)
	missing := &corev1.Secret{}
	assert.True(t, apierrors.IsNotFound(f.client.Get(context.Background(), client.ObjectKey{Namespace: testNamespace, Name: testClusterName + "-talos"}, missing)), "baseline-issued client must not implicitly create a generated bundle")
}
