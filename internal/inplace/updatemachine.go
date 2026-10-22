// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package inplace

import (
	"context"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"net"
	"reflect"
	ctrl "sigs.k8s.io/controller-runtime"
	"strings"

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

// DoUpdateMachine polls the durable requested operation and only completes after recovery.
func (h *Handler) DoUpdateMachine(ctx context.Context, req *runtimehooksv1.UpdateMachineRequest, resp *runtimehooksv1.UpdateMachineResponse) {
	if err := h.executeUpdate(ctx, req, resp); err != nil {
		respondRetryFailure(&resp.CommonRetryResponse, "Talos requested update failed", err, ctrl.LoggerFrom(ctx))
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
	if err := h.reader.Get(ctx, types.NamespacedName{Namespace: requested.Namespace, Name: requested.Name}, machine); err != nil {
		return nil, false, fmt.Errorf("configuration input Machine: lookup failed")
	}
	ref := machine.Spec.Bootstrap.ConfigRef
	if !ref.IsDefined() || ref.Kind != "TalosConfig" || ref.APIGroup != bootstrapv1beta1.GroupVersion.Group {
		return nil, false, fmt.Errorf("configuration input TalosConfig: association required")
	}
	config := &bootstrapv1beta1.TalosConfig{}
	if err := h.reader.Get(ctx, types.NamespacedName{Namespace: machine.Namespace, Name: ref.Name}, config); err != nil {
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
	if config.Spec.ImageFactory != nil {
		resolved := false
		for _, condition := range config.Status.Conditions {
			if condition.Type == bootstrapv1beta1.ImageFactoryResolvedCondition && condition.Status == metav1.ConditionTrue && condition.ObservedGeneration == config.Generation {
				resolved = true
			}
		}
		if !resolved || config.Status.ImageFactory == nil {
			return nil, false, nil
		}
	}
	if config.Status.DataSecretName == "" {
		return nil, false, nil
	}
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: machine.Namespace, Name: config.Status.DataSecretName}
	if err := h.reader.Get(ctx, key, secret); err != nil {
		return nil, false, fmt.Errorf("configuration input generated Secret: lookup failed")
	}
	cluster := &clusterv1.Cluster{}
	if err := h.reader.Get(ctx, types.NamespacedName{Namespace: machine.Namespace, Name: machine.Spec.ClusterName}, cluster); err != nil {
		return nil, false, fmt.Errorf("configuration input Cluster: lookup failed")
	}
	image := ""
	if config.Status.ImageFactory != nil {
		image = config.Status.ImageFactory.InstallerImage
	}
	fresh, err := configinput.IsFresh(ctx, h.reader, config, configinput.RenderContext{Machine: machine, Cluster: cluster, InstallerImage: image}, secret)
	if err != nil {
		return nil, false, err
	}
	return secret.Data["value"], fresh, nil
}

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
	if err := h.reader.Get(ctx, key, live); err != nil {
		return bootstrapv1beta1.TalosEndpoint{}, fmt.Errorf("configuration input Machine: endpoint lookup failed")
	}
	if ref := live.Spec.Bootstrap.ConfigRef; ref.IsDefined() {
		config := &bootstrapv1beta1.TalosConfig{}
		if err := h.reader.Get(ctx, types.NamespacedName{Namespace: live.Namespace, Name: ref.Name}, config); err != nil {
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
