// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/configloader"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	bootstrapv1alpha3 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1alpha3"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
)

// Opt in with KUBEBUILDER_ASSETS=/tmp/cacppt-envtest/k8s/1.36.2-linux-amd64.
// This test always owns a disposable API server and etcd: never an ambient
// kubeconfig, existing cluster, workload node or external runtime hook.
func TestConfigInputControllerDisposableEnvtest(t *testing.T) {
	assets := os.Getenv("KUBEBUILDER_ASSETS")
	if assets == "" {
		t.Skip("set KUBEBUILDER_ASSETS explicitly to run the disposable controller/webhook smoke test")
	}
	for _, binary := range []string{"kube-apiserver", "etcd", "kubectl"} {
		info, err := os.Stat(filepath.Join(assets, binary))
		require.NoError(t, err, "explicit envtest assets must exist")
		require.False(t, info.IsDir())
	}
	t.Setenv("KUBECONFIG", filepath.Join(t.TempDir(), "must-not-read-kubeconfig"))
	t.Setenv("USE_EXISTING_CLUSTER", "false")
	t.Setenv("KUBEBUILDER_ATTACH_CONTROL_PLANE_OUTPUT", "false")
	_, source, _, ok := runtime.Caller(0)
	require.True(t, ok)
	repository := filepath.Dir(filepath.Dir(source))
	moduleCache := os.Getenv("GOMODCACHE")
	if moduleCache == "" {
		goPath := os.Getenv("GOPATH")
		if goPath == "" {
			home, err := os.UserHomeDir()
			require.NoError(t, err)
			goPath = filepath.Join(home, "go")
		}
		moduleCache = filepath.Join(filepath.SplitList(goPath)[0], "pkg", "mod")
	}
	capiCRDs := filepath.Join(moduleCache, "sigs.k8s.io", "cluster-api@v1.14.2", "core", "config", "crd", "bases")
	scheme := kruntime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, clusterv1.AddToScheme(scheme))
	require.NoError(t, bootstrapv1alpha3.AddToScheme(scheme))
	require.NoError(t, bootstrapv1.AddToScheme(scheme))
	environment := &envtest.Environment{
		Scheme:                   scheme,
		BinaryAssetsDirectory:    assets,
		UseExistingCluster:       ptr.To(false),
		ErrorIfCRDPathMissing:    true,
		CRDDirectoryPaths:        []string{filepath.Join(repository, "config", "crd", "bases"), capiCRDs},
		WebhookInstallOptions:    envtest.WebhookInstallOptions{Paths: []string{filepath.Join(repository, "config", "webhook", "manifests.yaml")}},
		ControlPlaneStartTimeout: time.Minute,
		ControlPlaneStopTimeout:  time.Minute,
	}
	apiConfig, err := environment.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, environment.Stop()) })
	capture := &identityCapturedLog{}
	logger := capture.logger()
	manager, err := ctrl.NewManager(apiConfig, ctrl.Options{
		Scheme:                 scheme,
		Logger:                 logger,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		LeaderElection:         false,
		WebhookServer: webhook.NewServer(webhook.Options{
			Host:    environment.WebhookInstallOptions.LocalServingHost,
			Port:    environment.WebhookInstallOptions.LocalServingPort,
			CertDir: environment.WebhookInstallOptions.LocalServingCertDir,
		}),
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	reconciler := &TalosConfigReconciler{Client: manager.GetClient(), Log: logger, Scheme: scheme}
	require.NoError(t, reconciler.SetupWithManager(ctx, manager, controller.Options{MaxConcurrentReconciles: 1}))
	require.NoError(t, (&bootstrapv1.TalosConfig{}).SetupWebhookWithManager(manager))
	require.NoError(t, (&bootstrapv1.TalosConfigTemplate{}).SetupWebhookWithManager(manager))
	managerDone := make(chan error, 1)
	go func() { managerDone <- manager.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-managerDone:
			require.NoError(t, err)
		case <-time.After(20 * time.Second):
			t.Error("disposable manager did not stop")
		}
	})
	cacheCtx, stopCacheWait := context.WithTimeout(ctx, 20*time.Second)
	defer stopCacheWait()
	require.True(t, manager.GetCache().WaitForCacheSync(cacheCtx))
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(environment.WebhookInstallOptions.LocalServingCAData))
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}
	t.Cleanup(transport.CloseIdleConnections)
	webhookClient := &http.Client{Transport: transport, Timeout: time.Second}
	webhookURL := "https://" + net.JoinHostPort(environment.WebhookInstallOptions.LocalServingHost, strconv.Itoa(environment.WebhookInstallOptions.LocalServingPort)) + "/smoke-readiness"
	require.Eventually(t, func() bool {
		response, err := webhookClient.Get(webhookURL)
		if err != nil {
			return false
		}
		_ = response.Body.Close()
		return response.StatusCode == http.StatusNotFound
	}, 20*time.Second, 50*time.Millisecond, "trusted TLS webhook server must be ready")
	apiClient, err := client.New(apiConfig, client.Options{Scheme: scheme})
	require.NoError(t, err)
	const namespace = "configinput-smoke"
	const foreignNamespace = "configinput-foreign"
	for _, name := range []string{namespace, foreignNamespace} {
		require.NoError(t, apiClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}))
	}
	data, bundle := userSuppliedConfig(t, config.TalosVersion1_14)
	cluster := &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: testClusterName}, Spec: clusterv1.ClusterSpec{ControlPlaneEndpoint: clusterv1.APIEndpoint{Host: "1.2.3.4", Port: 6443}}}
	require.NoError(t, apiClient.Create(ctx, cluster))
	cluster.Status.Initialization.InfrastructureProvisioned = ptr.To(true)
	require.NoError(t, apiClient.Status().Update(ctx, cluster))
	machineObject := &clusterv1.Machine{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "machine-1", Labels: map[string]string{clusterv1.ClusterNameLabel: cluster.Name, clusterv1.MachineControlPlaneLabel: ""}},
		Spec: clusterv1.MachineSpec{
			ClusterName:       cluster.Name,
			Version:           "v1.37.0",
			Bootstrap:         clusterv1.Bootstrap{ConfigRef: clusterv1.ContractVersionedObjectReference{APIGroup: bootstrapv1.GroupVersion.Group, Kind: "TalosConfig", Name: "machine-1"}},
			InfrastructureRef: clusterv1.ContractVersionedObjectReference{APIGroup: "infrastructure.cluster.x-k8s.io", Kind: "GenericMachine", Name: "machine-1"},
		},
	}
	require.NoError(t, apiClient.Create(ctx, machineObject))
	cfg := &bootstrapv1.TalosConfig{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: machineObject.Name, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(machineObject, clusterv1.GroupVersion.WithKind("Machine"))}},
		Spec: bootstrapv1.TalosConfigSpec{
			GenerateType: "controlplane", TalosVersion: "v1.14",
			ConfigSecretRef:          &bootstrapv1.SecretKeyReference{Name: "baseline", Key: "value"},
			StrategicPatchSecretRefs: []bootstrapv1.SecretKeyReference{{Name: "private-patch", Key: "patch"}},
		},
	}
	identity := importedIdentitySecret(t, &TalosConfigScope{Config: cfg, Cluster: cluster}, bundle)
	require.NoError(t, apiClient.Create(ctx, identity))
	identityBefore := identity.DeepCopy()
	foreign := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: foreignNamespace, Name: "baseline"}, Data: map[string][]byte{"value": []byte(data)}}
	require.NoError(t, apiClient.Create(ctx, foreign))
	privatePatch := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "private-patch"}, Data: map[string][]byte{"patch": []byte(smokeHostnamePatch("private-node-one"))}}
	require.NoError(t, apiClient.Create(ctx, privatePatch))
	invalid := cfg.DeepCopy()
	invalid.Name = "invalid-reference"
	invalid.Spec.ConfigSecretRef.Name = "other-namespace/baseline"
	err = apiClient.Create(ctx, invalid)
	require.Error(t, err)
	assert.True(t, apierrors.IsInvalid(err))
	assert.Contains(t, err.Error(), "spec.configSecretRef.name", "real schema/admission must reject malformed namespace refs")
	require.NoError(t, apiClient.Create(ctx, cfg))
	configKey := client.ObjectKeyFromObject(cfg)
	readConfig := func() *bootstrapv1.TalosConfig {
		live := &bootstrapv1.TalosConfig{}
		require.NoError(t, apiClient.Get(ctx, configKey, live))
		return live
	}
	require.Eventually(t, func() bool {
		live := &bootstrapv1.TalosConfig{}
		if apiClient.Get(ctx, configKey, live) != nil {
			return false
		}
		for _, condition := range live.Status.Conditions {
			if condition.Type == bootstrapv1.DataSecretAvailableCondition && condition.Status == metav1.ConditionFalse && strings.Contains(condition.Message, "baseline/value: lookup failed") {
				return true
			}
		}
		return false
	}, 20*time.Second, 50*time.Millisecond, "foreign namespace baseline must not resolve")
	assert.Empty(t, readConfig().Status.DataSecretName)
	missingOutput := &corev1.Secret{}
	assert.True(t, apierrors.IsNotFound(apiClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: "machine-1-bootstrap-data"}, missingOutput)))
	baseline := foreign.DeepCopy()
	baseline.Namespace = namespace
	baseline.ResourceVersion = ""
	baseline.UID = ""
	require.NoError(t, apiClient.Create(ctx, baseline))
	readOutput := func() *corev1.Secret {
		output := &corev1.Secret{}
		require.NoError(t, apiClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: "machine-1-bootstrap-data"}, output))
		return output
	}
	require.Eventually(t, func() bool {
		live := &bootstrapv1.TalosConfig{}
		return apiClient.Get(ctx, configKey, live) == nil && ptr.Deref(live.Status.Initialization.DataSecretCreated, false) && live.Status.DataSecretName == "machine-1-bootstrap-data"
	}, 30*time.Second, 100*time.Millisecond, "local baseline must render through the running controller")
	first := readOutput().DeepCopy()
	require.NotEmpty(t, first.Data[configinput.FreshnessKey])
	assert.NotContains(t, first.Annotations, bootstrapv1.InPlaceConfigHashAnnotation)
	assert.Equal(t, "private-node-one", smokeRenderedHostname(t, first))
	served := &bootstrapv1alpha3.TalosConfig{}
	require.NoError(t, apiClient.Get(ctx, configKey, served), "served alpha API must convert through the real TLS webhook")
	require.NotNil(t, served.Spec.ConfigSecretRef)
	assert.Equal(t, "baseline", served.Spec.ConfigSecretRef.Name)
	assert.Equal(t, "value", served.Spec.ConfigSecretRef.Key)
	assert.Empty(t, served.Spec.Data, "conversion must preserve references, not resolve private bytes")
	ca := &corev1.Secret{}
	require.NoError(t, apiClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: cluster.Name + "-ca"}, ca))
	assert.True(t, bytes.Equal(bundle.Certs.K8s.Crt, ca.Data["tls.crt"]))
	assert.True(t, bytes.Equal(bundle.Certs.K8s.Key, ca.Data["tls.key"]))
	freshContext := configinput.RenderContext{Machine: machineObject, Cluster: cluster}
	fresh, err := configinput.IsFresh(ctx, apiClient, readConfig(), freshContext, first)
	require.NoError(t, err)
	assert.True(t, fresh)
	// Even a byte-identical replacement revision is new live source input.
	baseline.Annotations = map[string]string{"smoke-rotation": "second"}
	require.NoError(t, apiClient.Update(ctx, baseline))
	fresh, err = configinput.IsFresh(ctx, apiClient, readConfig(), freshContext, first)
	require.NoError(t, err)
	assert.False(t, fresh, "baseline Secret resourceVersion rotation must invalidate private freshness")
	privatePatch.Data["patch"] = []byte(smokeHostnamePatch("private-node-two"))
	require.NoError(t, apiClient.Update(ctx, privatePatch))
	fresh, err = configinput.IsFresh(ctx, apiClient, readConfig(), freshContext, first)
	require.NoError(t, err)
	assert.False(t, fresh, "same-name Secret rotation must invalidate private freshness")
	require.Eventually(t, func() bool {
		cached := &corev1.Secret{}
		return manager.GetClient().Get(ctx, client.ObjectKeyFromObject(privatePatch), cached) == nil && cached.ResourceVersion == privatePatch.ResourceVersion
	}, 10*time.Second, 50*time.Millisecond, "requested update must resolve the rotated patch from cache")
	assert.True(t, bytes.Equal(first.Data["value"], readOutput().Data["value"]), "rotation alone must not issue a rollout")
	require.Never(t, func() bool {
		output := &corev1.Secret{}
		return apiClient.Get(ctx, client.ObjectKeyFromObject(first), output) == nil && !bytes.Equal(first.Data["value"], output.Data["value"])
	}, time.Second, 50*time.Millisecond, "Secret rotation alone must not rewrite ready bootstrap data")
	// Ordinary user updates are blocked by the real TLS admission webhook.
	unrequested := readConfig()
	unrequested.Spec.ConfigRevision = "unrequested"
	err = apiClient.Update(ctx, unrequested)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "immutable")
	requested := readConfig()
	requested.Annotations = map[string]string{clusterv1.UpdateInProgressAnnotation: ""}
	requested.Spec.ConfigRevision = "reviewed-shared-revision"
	require.NoError(t, apiClient.Update(ctx, requested))
	require.Eventually(t, func() bool {
		output := &corev1.Secret{}
		if apiClient.Get(ctx, client.ObjectKeyFromObject(first), output) != nil || bytes.Equal(output.Data[configinput.FreshnessKey], first.Data[configinput.FreshnessKey]) {
			return false
		}
		live := &bootstrapv1.TalosConfig{}
		if apiClient.Get(ctx, configKey, live) != nil {
			return false
		}
		fresh, err := configinput.IsFresh(ctx, apiClient, live, freshContext, output)
		return err == nil && fresh
	}, 30*time.Second, 100*time.Millisecond, "requested update must regenerate data and private freshness atomically")
	second := readOutput().DeepCopy()
	assert.Equal(t, first.Name, second.Name)
	assert.Equal(t, "private-node-two", smokeRenderedHostname(t, second))
	assert.NotContains(t, second.Annotations, bootstrapv1.InPlaceConfigHashAnnotation)
	requested = readConfig()
	requested.Spec.MachineConfig = &bootstrapv1.MachineConfigOverlay{ConfigRevision: "reviewed-machine-revision"}
	require.NoError(t, apiClient.Update(ctx, requested))
	require.Eventually(t, func() bool {
		output := &corev1.Secret{}
		if apiClient.Get(ctx, client.ObjectKeyFromObject(first), output) != nil {
			return false
		}
		live := &bootstrapv1.TalosConfig{}
		if apiClient.Get(ctx, configKey, live) != nil {
			return false
		}
		fresh, err := configinput.IsFresh(ctx, apiClient, live, freshContext, output)
		return err == nil && fresh && !bytes.Equal(output.Data[configinput.FreshnessKey], second.Data[configinput.FreshnessKey])
	}, 30*time.Second, 100*time.Millisecond, "Machine revision must independently change private freshness")
	assert.True(t, bytes.Equal(second.Data["value"], readOutput().Data["value"]), "revision-only update must preserve configuration semantics")
	badPatch := []byte("machine: [" + identityPrivateSentinel)
	privatePatch.Data["patch"] = badPatch
	require.NoError(t, apiClient.Update(ctx, privatePatch))
	require.Eventually(t, func() bool {
		cached := &corev1.Secret{}
		return manager.GetClient().Get(ctx, client.ObjectKeyFromObject(privatePatch), cached) == nil && cached.ResourceVersion == privatePatch.ResourceVersion
	}, 10*time.Second, 50*time.Millisecond, "controller cache must observe malformed private rotation")
	lastGood := readOutput().DeepCopy()
	requested = readConfig()
	requested.Spec.ConfigRevision = "reviewed-invalid-private-patch"
	require.NoError(t, apiClient.Update(ctx, requested))
	require.Eventually(t, func() bool {
		live := &bootstrapv1.TalosConfig{}
		if apiClient.Get(ctx, configKey, live) != nil {
			return false
		}
		for _, condition := range live.Status.Conditions {
			if condition.Type == bootstrapv1.DataSecretAvailableCondition && condition.Status == metav1.ConditionFalse && strings.Contains(condition.Message, "strategic patch parse") {
				return true
			}
		}
		return false
	}, 20*time.Second, 100*time.Millisecond, "invalid requested private input must fail safely")
	assert.True(t, bytes.Equal(lastGood.Data["value"], readOutput().Data["value"]), "failed resolution must not mutate last good output")
	assert.True(t, bytes.Equal(lastGood.Data[configinput.FreshnessKey], readOutput().Data[configinput.FreshnessKey]))
	identityAfter := &corev1.Secret{}
	require.NoError(t, apiClient.Get(ctx, client.ObjectKeyFromObject(identity), identityAfter))
	assert.True(t, bytes.Equal(identityBefore.Data["talosconfig"], identityAfter.Data["talosconfig"]), "imported identity endpoints/credentials must not be rewritten")
	assert.Equal(t, identityBefore.OwnerReferences, identityAfter.OwnerReferences)
	assert.Equal(t, identityBefore.Annotations, identityAfter.Annotations)
	assert.NotContains(t, identityAfter.Annotations, identityCreatedAnnotation)
	public, err := json.Marshal(readConfig())
	require.NoError(t, err)
	digest := sha256.Sum256(badPatch)
	for _, visible := range []string{string(public), capture.text()} {
		assert.NotContains(t, visible, identityPrivateSentinel)
		assert.NotContains(t, visible, hex.EncodeToString(digest[:]))
		assert.NotContains(t, visible, string(bundle.Certs.OS.Key))
	}
	seed := &corev1.Secret{}
	assert.True(t, apierrors.IsNotFound(apiClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: cluster.Name + "-talos"}, seed)), "baseline must never generate unrelated cluster identity bundle")
}

func smokeHostnamePatch(name string) string {
	return "apiVersion: v1alpha1\nkind: HostnameConfig\nauto: off\nhostname: " + name + "\n"
}

func smokeRenderedHostname(t *testing.T, output *corev1.Secret) string {
	t.Helper()
	provider, err := configloader.NewFromBytes(output.Data["value"])
	require.NoError(t, err)
	require.NotNil(t, provider.NetworkHostnameConfig())

	return provider.NetworkHostnameConfig().Hostname()
}
