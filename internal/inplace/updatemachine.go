// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package inplace

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/siderolabs/talos/pkg/machinery/config/configloader"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	runtimehooksv1 "sigs.k8s.io/cluster-api/api/runtime/hooks/v1alpha1"

	bootstrapv1beta1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
)

const (
	// waitForConfigSeconds is how long to wait before re-checking whether CABPT has
	// regenerated the bootstrap data for the desired spec.
	waitForConfigSeconds = 10

	// waitForRebootSeconds is how long to wait for a node to come back after a change that
	// Talos decided needed a reboot, or after an upgrade.
	waitForRebootSeconds = 30
)

// DoUpdateMachine performs the in-place update of a single machine.
//
// Cluster API calls this repeatedly until it reports completion, so every step observes the
// node's actual state and acts only on a difference. Returning Success with a non-zero
// RetryAfterSeconds means "still working"; Success with zero means done.
func (h *Handler) DoUpdateMachine(ctx context.Context, req *runtimehooksv1.UpdateMachineRequest, resp *runtimehooksv1.UpdateMachineResponse) {
	log := ctrl.LoggerFrom(ctx)

	machine := req.Desired.Machine

	desiredConfig, ready, err := h.desiredConfig(ctx, &machine, req.Desired.BootstrapConfig)
	if err != nil {
		respondRetryFailure(&resp.CommonRetryResponse, "failed to resolve desired machine configuration", err, log)

		return
	}

	if !ready {
		// The owning controller has written the new desired spec but CABPT has not yet
		// regenerated the data secret. Applying what is there now would push the pre-update
		// configuration and then report success, silently losing the change.
		log.Info("waiting for bootstrap data to be regenerated for the desired spec")

		resp.Status = runtimehooksv1.ResponseStatusSuccess
		resp.Message = "waiting for bootstrap configuration to be regenerated"
		resp.RetryAfterSeconds = waitForConfigSeconds

		return
	}

	endpoints, err := h.machineEndpoints(ctx, &machine)
	if err != nil {
		respondRetryFailure(&resp.CommonRetryResponse, "failed to read machine addresses", err, log)

		return
	}

	if endpoints.Address == "" {
		log.Info("machine has no addresses yet, waiting")

		resp.Status = runtimehooksv1.ResponseStatusSuccess
		resp.Message = "waiting for machine addresses"
		resp.RetryAfterSeconds = waitForConfigSeconds

		return
	}

	clusterKey := types.NamespacedName{
		Namespace: machine.Namespace,
		Name:      machine.Spec.ClusterName,
	}

	node, err := h.newNodeClient(ctx, clusterKey, endpoints)
	if err != nil {
		// The node may simply be rebooting as part of this update; retry rather than fail.
		log.Info("cannot reach Talos API, will retry", "error", err.Error())

		resp.Status = runtimehooksv1.ResponseStatusSuccess
		resp.Message = "waiting for the Talos API to become reachable"
		resp.RetryAfterSeconds = waitForRebootSeconds

		return
	}

	defer node.Close() //nolint:errcheck // best effort on a read-mostly connection

	desiredImage, err := installImage(desiredConfig)
	if err != nil {
		respondRetryFailure(&resp.CommonRetryResponse, "failed to read desired installer image", err, log)

		return
	}

	runningVersion, err := node.Version(ctx)
	if err != nil {
		log.Info("cannot read Talos version, will retry", "error", err.Error())

		resp.Status = runtimehooksv1.ResponseStatusSuccess
		resp.Message = "waiting for the node to report its version"
		resp.RetryAfterSeconds = waitForRebootSeconds

		return
	}

	// Apply the configuration first so that the node has the desired installer image
	// recorded before any upgrade is triggered, and so config-only changes settle without a
	// reboot wherever Talos allows it.
	if err := node.ApplyConfig(ctx, desiredConfig); err != nil {
		respondRetryFailure(&resp.CommonRetryResponse, "failed to apply machine configuration", err, log)

		return
	}
	_, stillFresh, freshnessErr := h.desiredConfig(ctx, &machine, req.Desired.BootstrapConfig)
	if freshnessErr != nil || !stillFresh {
		respondRetryFailure(&resp.CommonRetryResponse, "configuration inputs changed after apply; intervention required", fmt.Errorf("issued operation input identity changed"), log)
		return
	}

	machineKey := types.NamespacedName{Namespace: machine.Namespace, Name: machine.Name}

	if needsUpgrade(desiredImage, runningVersion) {
		h.continueUpgrade(ctx, log, resp, node, upgradeTarget{
			machine:   machineKey,
			cluster:   clusterKey,
			endpoints: endpoints,
			image:     desiredImage,
			running:   runningVersion,
		})

		return
	}

	// The node runs the desired version, so any sequence recorded for it is over.
	h.upgrades.forget(machineKey)

	log.Info("in-place update applied")

	resp.Status = runtimehooksv1.ResponseStatusSuccess
	resp.RetryAfterSeconds = 0
}

// upgradeTarget is what continueUpgrade needs to know about the machine being upgraded.
type upgradeTarget struct {
	machine   types.NamespacedName
	cluster   types.NamespacedName
	endpoints bootstrapv1beta1.TalosEndpoint
	image     string
	running   string
}

// continueUpgrade moves a Talos upgrade along by one hook call.
//
// Nodes before Talos 1.13 upgrade themselves through the machine service and need nothing more
// than the request. Newer nodes are driven through the lifecycle service, whose sequence takes
// minutes; it runs in the background under the tracker, and each call reports on it: started,
// still running, failed (once, then the sequence is retried from scratch) or finished and
// waiting for the node to come back on the new version.
func (h *Handler) continueUpgrade(ctx context.Context, log logr.Logger, resp *runtimehooksv1.UpdateMachineResponse, node NodeClient, target upgradeTarget) {
	if !lifecycleUpgradeSupported(target.running) {
		log.Info("upgrading Talos in place through the machine service", "image", target.image, "running", target.running)

		if err := node.LegacyUpgrade(ctx, target.image); err != nil {
			respondRetryFailure(&resp.CommonRetryResponse, "failed to upgrade Talos", err, log)

			return
		}

		respondRetry(resp, fmt.Sprintf("upgrading Talos to %s", target.image))

		return
	}

	current, tracked := h.upgrades.get(target.machine)

	switch {
	case !tracked:
		log.Info("starting a lifecycle upgrade", "image", target.image, "running", target.running)

		h.upgrades.start(target.machine, target.image, func() error {
			upgradeCtx, cancel := context.WithTimeout(context.Background(), upgradeTimeout)
			defer cancel()

			// The request-scoped client is closed when this hook call returns; the sequence
			// outlives it and needs a connection of its own.
			background, err := h.newNodeClient(upgradeCtx, target.cluster, target.endpoints)
			if err != nil {
				return fmt.Errorf("failed to connect for the upgrade: %w", err)
			}

			defer background.Close() //nolint:errcheck // best effort

			return background.Upgrade(upgradeCtx, target.image)
		})

		respondRetry(resp, fmt.Sprintf("upgrading Talos to %s", target.image))
	case current.err != nil:
		h.upgrades.forget(target.machine)
		respondRetryFailure(&resp.CommonRetryResponse, "failed to upgrade Talos", current.err, log)
	case current.done && time.Since(current.started) > upgradeTimeout:
		// The install finished long ago and the node never came back on the new version.
		log.Info("the node did not come back on the new version, restarting the upgrade", "image", target.image)
		h.upgrades.forget(target.machine)
		respondRetry(resp, fmt.Sprintf("restarting the upgrade to %s", target.image))
	case current.done:
		respondRetry(resp, fmt.Sprintf("waiting for the node to reboot into %s", target.image))
	default:
		respondRetry(resp, fmt.Sprintf("upgrading Talos to %s", target.image))
	}
}

// respondRetry asks Cluster API to call again after the reboot wait.
func respondRetry(resp *runtimehooksv1.UpdateMachineResponse, message string) {
	resp.Status = runtimehooksv1.ResponseStatusSuccess
	resp.Message = message
	resp.RetryAfterSeconds = waitForRebootSeconds
}

// desiredConfig reads the live TalosConfig output and verifies its private resolved-input
// freshness. The Machine's original installation seed remains bootstrap provenance only.
func (h *Handler) desiredConfig(ctx context.Context, requested *clusterv1.Machine, bootstrapConfig runtimeRawExtension) ([]byte, bool, error) {
	machine := &clusterv1.Machine{}
	if err := h.client.Get(ctx, types.NamespacedName{Namespace: requested.Namespace, Name: requested.Name}, machine); err != nil {
		return nil, false, fmt.Errorf("configuration input Machine: lookup failed")
	}
	ref := machine.Spec.Bootstrap.ConfigRef
	if !ref.IsDefined() || ref.Kind != "TalosConfig" || ref.APIGroup != bootstrapv1beta1.GroupVersion.Group {
		return nil, false, fmt.Errorf("configuration input TalosConfig: association required")
	}
	config := &bootstrapv1beta1.TalosConfig{}
	if err := h.client.Get(ctx, types.NamespacedName{Namespace: machine.Namespace, Name: ref.Name}, config); err != nil {
		return nil, false, fmt.Errorf("configuration input TalosConfig: lookup failed")
	}
	obj, err := rawToUnstructured(bootstrapConfig)
	if err != nil || obj == nil {
		return nil, false, fmt.Errorf("configuration input TalosConfig: desired spec required")
	}
	var spec bootstrapv1beta1.TalosConfigSpec
	if err := remarshalInto(obj["spec"], &spec); err != nil {
		return nil, false, fmt.Errorf("configuration input TalosConfig: desired spec decoding failed")
	}
	if !reflect.DeepEqual(spec, config.Spec) || requested.Spec.Version != machine.Spec.Version {
		return nil, false, nil
	}
	for _, condition := range config.Status.Conditions {
		if condition.Type == bootstrapv1beta1.DataSecretAvailableCondition && condition.Status == metav1.ConditionFalse && condition.ObservedGeneration == config.Generation {
			return nil, false, fmt.Errorf("configuration input TalosConfig: requested render validation failed")
		}
	}
	if config.Status.DataSecretName == "" {
		return nil, false, nil
	}
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: machine.Namespace, Name: config.Status.DataSecretName}
	if err := h.client.Get(ctx, key, secret); err != nil {
		return nil, false, fmt.Errorf("configuration input generated Secret: lookup failed")
	}
	cluster := &clusterv1.Cluster{}
	if err := h.client.Get(ctx, types.NamespacedName{Namespace: machine.Namespace, Name: machine.Spec.ClusterName}, cluster); err != nil {
		return nil, false, fmt.Errorf("configuration input Cluster: lookup failed")
	}
	image := ""
	if config.Status.ImageFactory != nil {
		image = config.Status.ImageFactory.InstallerImage
	}
	fresh, err := configinput.IsFresh(ctx, h.client, config, configinput.RenderContext{Machine: machine, Cluster: cluster, InstallerImage: image}, secret)
	if err != nil {
		return nil, false, err
	}
	return secret.Data["value"], fresh, nil
}

// installImage reads the installer image out of a rendered Talos machine configuration, with
// the precedence Talos applies: an UnattendedInstallConfig document first, the legacy
// machine.install.image field otherwise.
func installImage(data []byte) (string, error) {
	provider, err := configloader.NewFromBytes(data)
	if err != nil {
		return "", fmt.Errorf("configuration input rendered: installer parse failed")
	}

	if doc := provider.UnattendedInstallConfig(); doc != nil {
		return doc.InstallerImage(), nil
	}

	if provider.Machine() == nil || provider.Machine().Install() == nil {
		return "", nil
	}

	return provider.Machine().Install().Image(), nil
}

// needsUpgrade reports whether the node has to be upgraded to reach the desired installer
// image.
//
// Talos reports its running version as a tag such as "v1.13.0", and installer images are
// conventionally tagged with exactly that version, so comparing the image tag against the
// running tag avoids rebooting a node that is already on the target version. An image
// without a recognisable version tag is treated as "no upgrade needed": guessing wrong in
// the other direction would reboot the node on every reconcile.
func needsUpgrade(desiredImage, runningVersion string) bool {
	if desiredImage == "" || runningVersion == "" {
		return false
	}

	tag := imageTag(desiredImage)
	if !talosVersionTag.MatchString(tag) {
		// A floating tag such as "latest" carries no version to compare against, so every
		// reconcile after a successful upgrade would see a mismatch and reboot the node again.
		// Treating it as "no upgrade needed" is the safe direction.
		return false
	}

	return tag != runningVersion
}

// talosVersionTag matches an installer tag that names a Talos version, e.g. "v1.13.9". Tags that
// do not, "latest" most of all, cannot be compared against the running version.
var talosVersionTag = regexp.MustCompile(`^v\d+\.\d+\.\d+`)

// imageTag extracts the tag from an image reference, ignoring any digest and being careful
// not to mistake a registry port for a tag.
func imageTag(image string) string {
	if at := strings.LastIndex(image, "@"); at >= 0 {
		image = image[:at]
	}

	colon := strings.LastIndex(image, ":")
	if colon < 0 {
		return ""
	}

	// A colon that precedes a slash belongs to a registry host:port, not a tag.
	if strings.Contains(image[colon:], "/") {
		return ""
	}

	return image[colon+1:]
}

// machineEndpoints returns the addresses usable as Talos API endpoints for a machine.
//
// The Machine is re-read from the API server rather than taken from the hook request. Cluster
// API strips status when building an UpdateMachine request, treating it purely as desired state,
// so the addresses on the request object are always empty and waiting on them never completes.
func (h *Handler) machineEndpoints(ctx context.Context, machine *clusterv1.Machine) (bootstrapv1beta1.TalosEndpoint, error) {
	live := &clusterv1.Machine{}
	key := types.NamespacedName{Namespace: machine.Namespace, Name: machine.Name}
	if err := h.client.Get(ctx, key, live); err != nil {
		return bootstrapv1beta1.TalosEndpoint{}, fmt.Errorf("configuration input Machine: endpoint lookup failed")
	}
	if ref := live.Spec.Bootstrap.ConfigRef; ref.IsDefined() {
		config := &bootstrapv1beta1.TalosConfig{}
		if err := h.client.Get(ctx, types.NamespacedName{Namespace: live.Namespace, Name: ref.Name}, config); err != nil {
			return bootstrapv1beta1.TalosEndpoint{}, fmt.Errorf("configuration input TalosConfig: endpoint lookup failed")
		}
		if config.Spec.TalosEndpoint != nil {
			return *config.Spec.TalosEndpoint, nil
		}
	}
	addresses := MachineAddresses(live)
	if len(addresses) == 0 {
		return bootstrapv1beta1.TalosEndpoint{}, nil
	}
	return bootstrapv1beta1.TalosEndpoint{Address: net.JoinHostPort(addresses[0], "50000"), ServerName: addresses[0]}, nil
}

// MachineAddresses returns the addresses usable as Talos API endpoints for a machine.
//
// It is exported because the MachinePool in-place path in the TalosConfig controller resolves
// endpoints for pool members exactly the same way; Cluster API has no in-place update flow for
// MachinePools, so that path cannot go through the hook handler.
func MachineAddresses(machine *clusterv1.Machine) []string {
	out := make([]string, 0, len(machine.Status.Addresses))

	for _, addr := range machine.Status.Addresses {
		switch addr.Type {
		case clusterv1.MachineInternalIP, clusterv1.MachineExternalIP:
			out = append(out, addr.Address)
		default:
		}
	}

	return out
}
