// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/generate"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/constants"
	"gopkg.in/yaml.v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	capiv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/feature"
	"sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/cluster-api/util/annotations"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/patch"
	"sigs.k8s.io/cluster-api/util/predicates"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"

	bootstrapv1beta1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/internal/configowner"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/internal/imagefactory"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/internal/inplace"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
	// +kubebuilder:scaffold:imports
)

const (
	controllerName = "cabpt-controller"
)

var (
	defaultVersionContract = config.TalosVersionCurrent
)

// TalosConfigReconciler reconciles a TalosConfig object
type TalosConfigReconciler struct {
	client.Client
	Log              logr.Logger
	Scheme           *runtime.Scheme
	WatchFilterValue string

	// MachinePoolInPlaceUpdates enables re-rendering the bootstrap data of a MachinePool-owned
	// TalosConfig when its spec changes, and applying the result to the pool's running members
	// over the Talos API. With it off a MachinePool behaves as it always has: the rendered
	// configuration is frozen once written.
	MachinePoolInPlaceUpdates bool

	// NodeClientFactory opens a Talos API client for a MachinePool member. When nil, a client
	// built from the cluster's talosconfig secret is used; tests substitute a fake so the update
	// loop can be exercised without hardware.
	NodeClientFactory inplace.NodeClientFactory

	// ImageFactory registers the schematic declared in spec.imageFactory and resolves the
	// Talos version for the installer image. Required when any TalosConfig sets the block;
	// nil is only acceptable for deployments that never do.
	ImageFactory imagefactory.API
}

type TalosConfigScope struct {
	Config      *bootstrapv1beta1.TalosConfig
	ConfigOwner *configowner.ConfigOwner
	Cluster     *capiv1.Cluster
}

func (r *TalosConfigReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager, options controller.Options) error {
	r.Scheme = mgr.GetScheme()

	b := ctrl.NewControllerManagedBy(mgr).
		For(&bootstrapv1beta1.TalosConfig{}).
		WithOptions(options).
		WithEventFilter(predicates.ResourceNotPausedAndHasFilterLabel(r.Scheme, ctrl.LoggerFrom(ctx), r.WatchFilterValue)).
		Watches(
			&capiv1.Machine{},
			handler.EnqueueRequestsFromMapFunc(r.MachineToBootstrapMapFunc),
		)

	if feature.Gates.Enabled(feature.MachinePool) {
		b = b.Watches(
			&capiv1.MachinePool{},
			handler.EnqueueRequestsFromMapFunc(r.MachinePoolToBootstrapMapFunc),
		).WithEventFilter(predicates.ResourceNotPausedAndHasFilterLabel(r.Scheme, ctrl.LoggerFrom(ctx), r.WatchFilterValue))
	}

	b = b.Watches(
		&capiv1.Cluster{},
		handler.EnqueueRequestsFromMapFunc(r.ClusterToTalosConfigs),
		builder.WithPredicates(
			predicates.All(r.Scheme, ctrl.LoggerFrom(ctx),
				predicates.ClusterPausedTransitionsOrInfrastructureProvisioned(r.Scheme, ctrl.LoggerFrom(ctx)),
				predicates.ResourceHasFilterLabel(r.Scheme, ctrl.LoggerFrom(ctx), r.WatchFilterValue),
			),
		),
	)

	if err := b.Complete(r); err != nil {
		return fmt.Errorf("failed setting up with a controller manager: %w", err)
	}

	return nil

}

// +kubebuilder:rbac:groups=bootstrap.cluster.x-k8s.io,resources=talosconfigs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=bootstrap.cluster.x-k8s.io,resources=talosconfigs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=clusters;clusters/status;machines/status,verbs=get;list;watch
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=machines,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=machinepools;machinepools/status,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=runtime.cluster.x-k8s.io,resources=extensionconfigs,verbs=get;list;watch;create;update;patch

func (r *TalosConfigReconciler) Reconcile(ctx context.Context, req ctrl.Request) (_ ctrl.Result, rerr error) {
	log := r.Log.WithName(controllerName).
		WithName(fmt.Sprintf("namespace=%s", req.Namespace)).
		WithName(fmt.Sprintf("talosconfig=%s", req.Name))

	// Lookup the talosconfig config
	config := &bootstrapv1beta1.TalosConfig{}
	if err := r.Client.Get(ctx, req.NamespacedName, config); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		log.Error(err, "failed to get config")
		return ctrl.Result{}, err
	}

	// Initialize the patch helper
	patchHelper, err := patch.NewHelper(config, r.Client)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Always attempt to Patch the TalosConfig object and status after each reconciliation.
	defer func() {
		// always update the readyCondition; the summary is represented using the "1 of x completed" notation.
		conditions.SetSummaryCondition(config, config, bootstrapv1beta1.DataSecretAvailableCondition)

		patchOpts := []patch.Option{
			patch.WithOwnedConditions{
				Conditions: []string{
					bootstrapv1beta1.DataSecretAvailableCondition,
					bootstrapv1beta1.ImageFactoryResolvedCondition,
				},
			},
		}

		// Patch ObservedGeneration only if the reconciliation completed successfully
		if rerr == nil {
			patchOpts = append(patchOpts, patch.WithStatusObservedGeneration{})
		}

		if err := patchHelper.Patch(ctx, config, patchOpts...); err != nil {
			log.Error(err, "failed to patch config")
			if rerr == nil {
				rerr = err
			}
		}
	}()

	// Handle deleted talosconfigs
	// We no longer set finalizers on talosconfigs, but we have to remove previously set finalizers
	if !config.ObjectMeta.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(config)
	}

	// Look up the resource that owns this talosconfig if there is one
	owner, err := configowner.GetConfigOwner(ctx, r.Client, config)
	if err != nil {
		log.Error(err, "could not get owner resource")
		return ctrl.Result{}, err
	}

	if owner == nil {
		log.Info("Waiting for OwnerRef on the talosconfig")
		return ctrl.Result{}, nil
	}

	log = log.WithName(fmt.Sprintf("owner-name=%s", owner.GetName()))

	// Lookup the cluster the machine is associated with
	cluster, err := util.GetClusterByName(ctx, r.Client, owner.GetNamespace(), owner.ClusterName())
	if err != nil {
		if errors.Is(err, util.ErrNoCluster) {
			log.Info(fmt.Sprintf("%s does not belong to a cluster yet, waiting until it's part of a cluster", owner.GetKind()))
			return ctrl.Result{}, nil
		}

		if apierrors.IsNotFound(err) {
			log.Info("Cluster does not exist yet, waiting until it is created")
			return ctrl.Result{}, nil
		}

		log.Error(err, "could not get cluster by machine metadata")

		return ctrl.Result{}, err
	}

	if annotations.IsPaused(cluster, config) {
		log.Info("Reconciliation is paused for this object")
		return ctrl.Result{}, nil
	}

	tcScope := &TalosConfigScope{
		Config:      config,
		ConfigOwner: owner,
		Cluster:     cluster,
	}

	// bail super early if it's already ready
	//
	// An in-place update is the one case where a ready config must be reconciled again: the
	// owning controller has written a new desired spec onto the existing object and the
	// rendered machine configuration has to be regenerated for the UpdateMachine hook to
	// apply. The annotation is removed by the core Machine controller once the update
	// completes, so this falls back to the fast path on its own.
	if ptr.Deref(config.Status.Initialization.DataSecretCreated, false) && !bootstrapv1beta1.IsInPlaceUpdate(config) {
		// A MachinePool's rendered configuration is not frozen the way a Machine's is. The pool
		// keeps one bootstrap data secret for every instance it will ever create, and Cluster API
		// has no in-place update flow to regenerate it, so a ready pool config carries on past
		// this fast path and reconciles the pool itself. That happens after the client config
		// reconcile below, so a ready pool config still gets exactly the status a ready Machine
		// config gets.
		poolUpdate := r.MachinePoolInPlaceUpdates && owner.IsMachinePool()

		if !poolUpdate {
			log.Info("ignoring an already ready config")

			// Nothing else owns MachinePoolInPlaceUpdate, so a pool that was being updated in place
			// until the flag was turned off would keep reporting whatever the loop last wrote.
			if owner.IsMachinePool() {
				markMachinePoolInPlaceDisabled(config)
			}
		}

		conditions.Set(config, metav1.Condition{
			Type:   bootstrapv1beta1.DataSecretAvailableCondition,
			Reason: bootstrapv1beta1.DataSecretAvailableReason,
			Status: metav1.ConditionTrue,
		})

		// reconcile cluster-wide talosconfig
		err = r.reconcileClientConfig(ctx, log, tcScope)

		if err == nil {
			conditions.Set(config, metav1.Condition{
				Type:   bootstrapv1beta1.ClientConfigAvailableCondition,
				Reason: bootstrapv1beta1.ClientConfigAvailableReason,
				Status: metav1.ConditionTrue,
			})
		} else {
			conditions.Set(config, metav1.Condition{
				Type:    bootstrapv1beta1.ClientConfigAvailableCondition,
				Status:  metav1.ConditionFalse,
				Reason:  bootstrapv1beta1.ClientConfigAvailableInternalErrorReason,
				Message: fmt.Sprintf("talosconfig generation failure: %s", err),
			})
		}

		if err != nil || !poolUpdate {
			return ctrl.Result{}, err
		}

		return r.reconcileMachinePool(ctx, log, tcScope)
	}

	// Wait patiently for the infrastructure to be ready
	if !ptr.Deref(cluster.Status.Initialization.InfrastructureProvisioned, false) {
		log.Info("Infrastructure is not ready, waiting until ready.")

		conditions.Set(config, metav1.Condition{
			Type:    bootstrapv1beta1.DataSecretAvailableCondition,
			Status:  metav1.ConditionFalse,
			Reason:  bootstrapv1beta1.DataSecretNotAvailableReason,
			Message: "Waiting for the cluster infrastructure to be ready",
		})

		return ctrl.Result{}, nil
	}

	// Reconcile status for machines that already have a secret reference, but our status isn't up to date.
	// This case solves the pivoting scenario (or a backup restore) which doesn't preserve the status subresource on objects.
	if owner.DataSecretName() != nil && (!ptr.Deref(config.Status.Initialization.DataSecretCreated, false) || config.Status.DataSecretName == "") {
		config.Status.Initialization.DataSecretCreated = ptr.To(true)
		config.Status.DataSecretName = *owner.DataSecretName()

		conditions.Set(config, metav1.Condition{
			Type:   bootstrapv1beta1.DataSecretAvailableCondition,
			Reason: bootstrapv1beta1.DataSecretAvailableReason,
			Status: metav1.ConditionTrue,
		})

		return ctrl.Result{}, nil
	}

	if err = r.reconcileGenerate(ctx, tcScope); err != nil {
		markDataSecretGenerationFailed(config, err)

		return ctrl.Result{}, err
	}

	config.Status.Initialization.DataSecretCreated = ptr.To(true)

	conditions.Set(config, metav1.Condition{
		Type:   bootstrapv1beta1.DataSecretAvailableCondition,
		Reason: bootstrapv1beta1.DataSecretAvailableReason,
		Status: metav1.ConditionTrue,
	})

	return ctrl.Result{}, nil
}

// markDataSecretGenerationFailed records a failure to render the machine configuration on both
// the current and the deprecated v1beta1 condition.
func markDataSecretGenerationFailed(config *bootstrapv1beta1.TalosConfig, err error) {
	conditions.Set(config, metav1.Condition{
		Type:               bootstrapv1beta1.DataSecretAvailableCondition,
		Status:             metav1.ConditionFalse,
		Reason:             bootstrapv1beta1.DataSecretNotAvailableInternalErrorReason,
		Message:            fmt.Sprintf("Data secret generation failed: %s", err),
		ObservedGeneration: config.Generation,
	})
}

func (r *TalosConfigReconciler) reconcileGenerate(ctx context.Context, scope *TalosConfigScope) error {
	installerImage, err := r.resolveInstallerImage(ctx, scope.Config)
	if err != nil {
		return err
	}
	renderContext, err := configRenderContext(scope, installerImage)
	if err != nil {
		return err
	}
	hasBaseline := scope.Config.Spec.GenerateType == "none" || scope.Config.Spec.ConfigSecretRef != nil || scope.Config.Spec.MachineConfig != nil && scope.Config.Spec.MachineConfig.ConfigSecretRef != nil
	if !hasBaseline {
		contract, err := versionContractFor(scope.Config.Spec.TalosVersion)
		if err != nil {
			return fmt.Errorf("configuration input TalosConfig: version contract failed")
		}
		bundle, err := r.getSecretsBundle(ctx, scope, true, contract)
		if err != nil {
			return fmt.Errorf("configuration input cluster identity: resolve failed")
		}
		if err := r.writeK8sCASecret(ctx, scope, bundle.Certs.K8s); err != nil {
			return err
		}
	}
	if scope.Config.Labels == nil {
		scope.Config.Labels = map[string]string{}
	}
	scope.Config.Labels[capiv1.ClusterNameLabel] = scope.Cluster.Name
	inputs, err := configinput.Resolve(ctx, r.Client, scope.Config)
	if err != nil {
		return err
	}
	rendered, err := configinput.Render(inputs, renderContext)
	if err != nil {
		return err
	}
	if inputs.HasBaseline() {
		if err := r.reconcileBaselineIdentity(ctx, scope, rendered.Data()); err != nil {
			return err
		}
	}
	data := rendered.Data()
	if renderContext.Machine.Spec.InfrastructureRef.Kind == "PacketMachine" || renderContext.Machine.Spec.InfrastructureRef.Kind == "PacketMachinePool" {
		data = append([]byte("#!talos\n"), data...)
	}
	configHash, err := renderedConfigHash(scope, installerImage)
	if err != nil {
		return err
	}
	name, err := r.writeBootstrapData(ctx, scope, data, configHash, rendered.Freshness())
	if err != nil {
		return err
	}
	scope.Config.Status.DataSecretName = name
	return nil
}

func configRenderContext(scope *TalosConfigScope, installerImage string) (configinput.RenderContext, error) {
	machine := &capiv1.Machine{}
	if scope.ConfigOwner.IsMachinePool() {
		pool := &capiv1.MachinePool{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(scope.ConfigOwner.Object, pool); err != nil {
			return configinput.RenderContext{}, err
		}
		machine.ObjectMeta = pool.ObjectMeta
		machine.Spec = pool.Spec.Template.Spec
		// Pool members cannot have an individual hostname.
	} else if err := runtime.DefaultUnstructuredConverter.FromUnstructured(scope.ConfigOwner.Object, machine); err != nil {
		return configinput.RenderContext{}, err
	}
	return configinput.RenderContext{Machine: machine, Cluster: scope.Cluster, InstallerImage: installerImage}, nil
}

func (r *TalosConfigReconciler) reconcileDelete(config *bootstrapv1beta1.TalosConfig) (ctrl.Result, error) {
	controllerutil.RemoveFinalizer(config, bootstrapv1beta1.ConfigFinalizer)

	return ctrl.Result{}, nil
}

// versionContractFor parses spec.talosVersion into the contract configuration is generated for.
// Unset means the current Talos, which machinery represents as a nil contract.
func versionContractFor(talosVersion string) (*config.VersionContract, error) {
	if talosVersion == "" {
		return defaultVersionContract, nil
	}

	contract, err := config.ParseContractFromVersion(talosVersion)
	if err != nil {
		return nil, fmt.Errorf("invalid talos-version: %w", err)
	}

	return contract, nil
}

func genTalosConfigFile(clusterName string, bundle *secrets.Bundle, endpoints []string) (string, error) {
	// Only the talosconfig is taken from this input, so the Kubernetes version is irrelevant;
	// the generator nevertheless insists on one.
	in, err := generate.NewInput(clusterName, "https://localhost", constants.DefaultKubernetesVersion, generate.WithSecretsBundle(bundle), generate.WithEndpointList(endpoints))
	if err != nil {
		return "", err
	}

	talosConfig, err := in.Talosconfig()
	if err != nil {
		return "", err
	}

	talosConfigBytes, err := yaml.Marshal(talosConfig)
	if err != nil {
		return "", err
	}

	return string(talosConfigBytes), nil
}

// kubernetesVersion returns the Kubernetes version the machine configuration should be
// rendered for, taken from the config owner and normalised to Talos' unprefixed form.
//
// This is shared between config generation and the in-place config hash so that the hash
// stamped on the bootstrap data secret is derived from exactly the value that was rendered.
func kubernetesVersion(scope *TalosConfigScope) (string, error) {
	// Allow user to override default kube version.
	// This also handles version being formatted like "vX.Y.Z" instead of without leading 'v'
	// TrimPrefix returns the string unchanged if the prefix isn't present.
	if scope.ConfigOwner.IsMachinePool() {
		mp := &capiv1.MachinePool{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(scope.ConfigOwner.Object, mp); err != nil {
			return "", err
		}

		if mp.Spec.Template.Spec.Version != "" {
			return strings.TrimPrefix(mp.Spec.Template.Spec.Version, "v"), nil
		}

		return constants.DefaultKubernetesVersion, nil
	}

	machine := &capiv1.Machine{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(scope.ConfigOwner.Object, machine); err != nil {
		return "", err
	}

	if machine.Spec.Version != "" {
		return strings.TrimPrefix(machine.Spec.Version, "v"), nil
	}

	return constants.DefaultKubernetesVersion, nil
}

// renderedConfigHash returns the hash stamped on the bootstrap data secret for the
// configuration rendered from this scope and installer image.
//
// It exists so the gate that decides whether a MachinePool's secret needs re-rendering derives
// its expected hash from exactly the inputs the renderer uses. A gate that computed a different
// hash would either re-render on every reconcile or never re-render at all.
func renderedConfigHash(scope *TalosConfigScope, installerImage string) (string, error) {
	k8sVersion, err := kubernetesVersion(scope)
	if err != nil {
		return "", err
	}

	return bootstrapv1beta1.InPlaceConfigHash(scope.Config.Spec, k8sVersion, installerImage)
}

// MachineToBootstrapMapFunc is a handler.ToRequestsFunc to be used to enqueue
// request for reconciliation of TalosConfig.
func (r *TalosConfigReconciler) MachineToBootstrapMapFunc(_ context.Context, o client.Object) []ctrl.Request {
	m, ok := o.(*capiv1.Machine)
	if !ok {
		panic(fmt.Sprintf("Expected a Machine but got a %T", o))
	}

	result := []ctrl.Request{}
	if m.Spec.Bootstrap.ConfigRef.IsDefined() && m.Spec.Bootstrap.ConfigRef.GroupKind() == bootstrapv1beta1.GroupVersion.WithKind("TalosConfig").GroupKind() {
		name := client.ObjectKey{Namespace: m.Namespace, Name: m.Spec.Bootstrap.ConfigRef.Name}
		result = append(result, ctrl.Request{NamespacedName: name})
	}
	return result
}

// MachinePoolToBootstrapMapFunc is a handler.ToRequestsFunc to be used to enqueue
// request for reconciliation of TalosConfig.
func (r *TalosConfigReconciler) MachinePoolToBootstrapMapFunc(_ context.Context, o client.Object) []ctrl.Request {
	m, ok := o.(*capiv1.MachinePool)
	if !ok {
		panic(fmt.Sprintf("Expected a MachinePool but got a %T", o))
	}

	result := []ctrl.Request{}
	configRef := m.Spec.Template.Spec.Bootstrap.ConfigRef
	if configRef.IsDefined() && configRef.GroupKind() == bootstrapv1beta1.GroupVersion.WithKind("TalosConfig").GroupKind() {
		name := client.ObjectKey{Namespace: m.Namespace, Name: configRef.Name}
		result = append(result, ctrl.Request{NamespacedName: name})
	}
	return result
}

// ClusterToTalosConfigs is a handler.ToRequestsFunc to be used to enqeue
// requests for reconciliation of TalosConfigs.
func (r *TalosConfigReconciler) ClusterToTalosConfigs(ctx context.Context, o client.Object) []ctrl.Request {
	result := []ctrl.Request{}

	c, ok := o.(*capiv1.Cluster)
	if !ok {
		panic(fmt.Sprintf("Expected a Cluster but got a %T", o))
	}

	selectors := []client.ListOption{
		client.InNamespace(c.Namespace),
		client.MatchingLabels{
			capiv1.ClusterNameLabel: c.Name,
		},
	}

	machineList := &capiv1.MachineList{}
	if err := r.Client.List(ctx, machineList, selectors...); err != nil {
		return nil
	}

	for _, m := range machineList.Items {
		if m.Spec.Bootstrap.ConfigRef.IsDefined() &&
			m.Spec.Bootstrap.ConfigRef.GroupKind() == bootstrapv1beta1.GroupVersion.WithKind("TalosConfig").GroupKind() {
			name := client.ObjectKey{Namespace: m.Namespace, Name: m.Spec.Bootstrap.ConfigRef.Name}
			result = append(result, ctrl.Request{NamespacedName: name})
		}
	}

	if feature.Gates.Enabled(feature.MachinePool) {
		machinePoolList := &capiv1.MachinePoolList{}
		if err := r.Client.List(ctx, machinePoolList, selectors...); err != nil {
			return nil
		}

		for _, mp := range machinePoolList.Items {
			if mp.Spec.Template.Spec.Bootstrap.ConfigRef.IsDefined() &&
				mp.Spec.Template.Spec.Bootstrap.ConfigRef.GroupKind() == bootstrapv1beta1.GroupVersion.WithKind("TalosConfig").GroupKind() {
				name := client.ObjectKey{Namespace: mp.Namespace, Name: mp.Spec.Template.Spec.Bootstrap.ConfigRef.Name}
				result = append(result, ctrl.Request{NamespacedName: name})
			}
		}
	}

	return result
}
