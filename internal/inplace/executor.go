// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package inplace

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"reflect"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	runtimehooks "sigs.k8s.io/cluster-api/api/runtime/hooks/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sort"
	"strings"
	"time"
)

const operationDeadline = 30 * time.Minute

func (h *Handler) executeUpdate(ctx context.Context, req *runtimehooks.UpdateMachineRequest, resp *runtimehooks.UpdateMachineResponse) error {
	s, ready, err := h.snapshot(ctx, &req.Desired.Machine, req.Desired.BootstrapConfig)
	if err != nil {
		return err
	}
	if !ready {
		if err := h.rejectStalePending(ctx, &req.Desired.Machine); err != nil {
			return err
		}
		respondRetry(resp, "waiting for current rendered configuration")
		return nil
	}
	identity := objectIdentity("bootstrap.cluster.x-k8s.io", "TalosConfig", s.config)
	secret, j, err := op.ReadJournal(ctx, h.reader, s.machine.Namespace, identity)
	if errors.Is(err, op.ErrNotFound) {
		node, err := h.newNodeClient(ctx, client.ObjectKey{Namespace: s.cluster.Namespace, Name: s.cluster.Name}, s.endpoint)
		if err != nil {
			return err
		}
		observed, err := node.Observe(ctx)
		node.Close()
		if err != nil {
			return err
		}
		j, err = h.plan(ctx, s, observed)
		if err != nil {
			return err
		}
		if err := validateKubeTransitionFromNode(ctx, h, s, j); err != nil {
			return err
		}
		secret, err = op.CreateJournal(ctx, h.client, s.machine.Namespace, j)
		if err != nil {
			return err
		}
		respondRetry(resp, "operation planned")
		return nil
	}
	if err != nil {
		return err
	}
	if j.Phase == op.Observed && !bytes.Equal(j.InputFreshness, s.output.Data[configinput.FreshnessKey]) {
		if !receiptConsumed(secret, j) {
			return fmt.Errorf("previous operation cleanup receipt not consumed")
		}
		node, err := h.newNodeClient(ctx, client.ObjectKey{Namespace: s.cluster.Namespace, Name: s.cluster.Name}, s.endpoint)
		if err != nil {
			return err
		}
		observed, err := node.Observe(ctx)
		node.Close()
		if err != nil {
			return err
		}
		next, err := h.plan(ctx, s, observed)
		if err != nil {
			return err
		}
		if err := op.BeginNextJournal(ctx, h.client, secret, next); err != nil {
			return err
		}
		respondRetry(resp, "next operation planned")
		return nil
	}
	if err := h.guard(ctx, s, j); err != nil {
		return h.ambiguous(ctx, secret, j, err)
	}
	if j.Phase == op.Ambiguous {
		return fmt.Errorf("operation outcome ambiguous; intervention required")
	}
	if j.Phase == op.Observed {
		if _, err := op.ReadReceipt(ctx, h.reader, s.machine.Namespace, j.TalosConfig, j.TransactionID); err != nil {
			return err
		}
		resp.Status = runtimehooks.ResponseStatusSuccess
		resp.RetryAfterSeconds = 0
		return nil
	}
	for index := range j.Steps {
		step := j.Steps[index]
		switch step.Phase {
		case op.StepPlanned:
			h.runner.mu.Lock()
			started := h.runner.ctx != nil
			h.runner.mu.Unlock()
			if !started {
				return fmt.Errorf("operation executor manager lifecycle unavailable")
			}
			if step.Kind == op.Apply || step.Kind == op.Install || step.Kind == op.Reboot {
				workload, err := h.workloadNode(ctx, s.cluster, j.NodeName)
				if err != nil || workload.UID != j.NodeUID || workload.Spec.ProviderID != j.NodeProviderID {
					return fmt.Errorf("operation preflight workload identity unavailable")
				}
				if workload.DeletionTimestamp != nil || !workloadNodeReady(workload) {
					respondRetry(resp, "waiting for healthy Node before operation issuance")
					return nil
				}
				if _, cp := s.machine.Labels[clusterv1.MachineControlPlaneLabel]; cp {
					if err := h.controlPlaneHealth(ctx, s.cluster, j); err != nil {
						respondRetry(resp, "waiting for healthy control plane before operation issuance")
						return nil
					}
				}
			}
			node, err := h.newNodeClient(ctx, client.ObjectKey{Namespace: s.cluster.Namespace, Name: s.cluster.Name}, j.Endpoint)
			if err != nil {
				return err
			}
			observation, err := node.Observe(ctx)
			node.Close()
			if err != nil {
				return err
			}
			now := metav1.NewTime(time.Now().UTC().Truncate(time.Second))
			deadline := metav1.NewTime(now.Add(operationDeadline))
			j.Steps[index].Phase = op.Issued
			j.Steps[index].IssuerID = h.runner.issuer
			j.Steps[index].IssuedAt = &now
			j.Steps[index].Deadline = &deadline
			j.Steps[index].IssuedBootID = observation.BootID
			if err := op.WriteJournal(ctx, h.client, secret, j, nil); err != nil {
				return err
			}
			frozen := *j
			frozen.Steps = append([]op.OperationStep(nil), j.Steps...)
			h.runner.start(j.TransactionID+"/"+string(step.Kind), func(lifetime context.Context) {
				h.runStep(lifetime, s.machine.Namespace, s.cluster.Name, &frozen, index)
			})
			respondRetry(resp, "requested operation is running")
			return nil
		case op.Issued:
			if step.Deadline == nil || time.Now().After(step.Deadline.Time) {
				return h.ambiguous(ctx, secret, j, fmt.Errorf("issued operation acknowledgment unavailable after deadline"))
			}
			respondRetry(resp, "waiting for durable operation acknowledgment")
			return nil
		case op.Acknowledged:
			effectObserved, err := h.observeStep(ctx, s, j, index)
			if err != nil {
				return h.ambiguous(ctx, secret, j, err)
			}
			if !effectObserved {
				respondRetry(resp, "waiting for actual operation effect")
				return nil
			}
			j.Steps[index].Phase = op.StepObserved
			if err := op.WriteJournal(ctx, h.client, secret, j, nil); err != nil {
				return err
			}
			respondRetry(resp, "operation acknowledged; checking recovery")
			return nil
		case op.StepAmbiguous:
			return fmt.Errorf("operation step ambiguous; intervention required")
		case op.StepObserved:
		default:
			return fmt.Errorf("operation step state invalid")
		}
	}
	for _, step := range j.Steps {
		if step.Deadline == nil || time.Now().After(step.Deadline.Time) {
			return h.ambiguous(ctx, secret, j, fmt.Errorf("operation recovery deadline expired"))
		}
	}
	node, err := h.newNodeClient(ctx, client.ObjectKey{Namespace: s.cluster.Namespace, Name: s.cluster.Name}, j.Endpoint)
	if err != nil {
		respondRetry(resp, "waiting for Talos recovery")
		return nil
	}
	observation, err := node.Observe(ctx)
	node.Close()
	if err != nil {
		respondRetry(resp, "waiting for actual Talos observations")
		return nil
	}
	if err := h.recovered(ctx, s, j, observation); err != nil {
		respondRetry(resp, "waiting for complete requested recovery")
		return nil
	}
	if err := h.finalizeObserved(ctx, s, secret, j); err != nil {
		return err
	}
	resp.Status = runtimehooks.ResponseStatusSuccess
	resp.RetryAfterSeconds = 0
	return nil
}
func validateKubeTransitionFromNode(ctx context.Context, h *Handler, s *operationSnapshot, j *op.Journal) error {
	n, e := h.workloadNode(ctx, s.cluster, j.NodeName)
	if e != nil {
		return e
	}
	return validateKubeTransition(n.Status.NodeInfo.KubeletVersion, j.TargetKubernetesVersion)
}
func (h *Handler) ambiguous(ctx context.Context, s *corev1.Secret, j *op.Journal, cause error) error {
	j.Phase = op.Ambiguous
	for i := range j.Steps {
		if j.Steps[i].Phase == op.Issued {
			j.Steps[i].Phase = op.StepAmbiguous
		}
	}
	if err := op.WriteJournal(ctx, h.client, s, j, nil); err != nil {
		return err
	}
	return fmt.Errorf("operation ambiguous; intervention required")
}
func (h *Handler) runStep(lifetime context.Context, namespace, cluster string, j *op.Journal, index int) {
	step := j.Steps[index]
	ctx, cancel := context.WithDeadline(lifetime, step.Deadline.Time)
	defer cancel()
	var node OperationNode
	err := h.guardIssued(ctx, namespace, j)
	if err == nil {
		node, err = h.newNodeClient(ctx, client.ObjectKey{Namespace: namespace, Name: cluster}, j.Endpoint)
	}
	if err == nil {
		switch step.Kind {
		case op.Apply:
			step.ApplyOutcome, err = node.Apply(ctx, j.Configuration)
		case op.PullInstaller:
			step.PulledImageName, step.PulledImageDigest, err = node.PullInstaller(ctx, j.TargetImage.Reference)
			if err == nil && j.TargetImage.Digest != "" && step.PulledImageDigest != j.TargetImage.Digest {
				err = fmt.Errorf("pulled installer digest mismatch")
			}
			if err == nil {
				pinned := strings.SplitN(step.PulledImageName, "@", 2)[0] + "@" + step.PulledImageDigest
				name, digest, pullErr := node.PullInstaller(ctx, pinned)
				if pullErr != nil || digest != step.PulledImageDigest || name != pinned {
					err = fmt.Errorf("immutable installer key materialization failed")
				} else {
					step.PulledImageName = name
				}
			}
		case op.Install:
			for _, p := range j.Steps {
				if p.Kind == op.PullInstaller {
					var code int32
					code, err = node.Install(ctx, p.PulledImageName)
					step.InstallExitCode = &code
				}
			}
		case op.Reboot:
			err = node.Reboot(ctx)
		}
		node.Close()
	}
	if err == nil {
		err = h.guardIssued(ctx, namespace, j)
	}
	persistCtx, c := context.WithTimeout(context.WithoutCancel(lifetime), 5*time.Second)
	defer c()
	secret, current, e := op.ReadJournal(persistCtx, h.reader, namespace, j.TalosConfig)
	if e != nil || current.TransactionID != j.TransactionID || current.Steps[index].IssuerID != step.IssuerID || current.Steps[index].Phase != op.Issued {
		return
	}
	if err != nil || step.Kind == op.Apply && step.ApplyOutcome == op.Unknown {
		step.Phase = op.StepAmbiguous
		current.Phase = op.Ambiguous
	} else {
		step.Phase = op.Acknowledged
	}
	current.Steps[index] = step
	if e := op.WriteJournal(persistCtx, h.client, secret, current, nil); e != nil {
		ctrl.LoggerFrom(lifetime).Error(fmt.Errorf("durable operation acknowledgment persistence failed"), "operation remains issued; intervention required")
	}
}
func (h *Handler) recovered(ctx context.Context, s *operationSnapshot, j *op.Journal, o NodeObservation) error {
	if !activeConfigurationMatches(o.Configuration, j.Configuration) {
		return fmt.Errorf("active full configuration not converged")
	}
	for _, step := range j.Steps {
		if step.Kind == op.Reboot || step.Kind == op.Apply && step.ApplyOutcome == op.RebootRequired {
			if o.BootID == step.IssuedBootID || o.BootID == "" {
				return fmt.Errorf("actual reboot witness not observed")
			}
		}
	}
	imageChanged := !reflect.DeepEqual(j.PriorAcceptedImageIntent, j.TargetImage)
	if imageChanged {
		if imageTag(j.TargetImage.Reference) != o.OSVersion {
			return fmt.Errorf("target OS version not observed")
		}
		if j.TargetImage.SchematicID != "" && j.TargetImage.SchematicID != o.SchematicID {
			return fmt.Errorf("target schematic not observed")
		}
		expected := normalizedExtensionNames(j.TargetImage.Extensions)
		names := []string{}
		for _, extension := range o.Extensions {
			if extension.Name == "" || extension.Version == "" || extension.Image == "" {
				return fmt.Errorf("installed extension identity unknown")
			}
			names = append(names, strings.TrimPrefix(extension.Name, "siderolabs/"))
		}
		sort.Strings(names)
		if !reflect.DeepEqual(expected, names) {
			return fmt.Errorf("target extensions not observed")
		}
	}
	node, err := h.workloadNode(ctx, s.cluster, j.NodeName)
	if err != nil {
		return err
	}
	if node.UID != j.NodeUID || node.Spec.ProviderID != j.NodeProviderID || node.Status.NodeInfo.KubeletVersion != j.TargetKubernetesVersion || node.DeletionTimestamp != nil {
		return fmt.Errorf("target Node identity/version not recovered")
	}
	ready := false
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
			ready = true
		}
	}
	if !ready {
		return fmt.Errorf("Node not Ready")
	}
	if imageTag(o.KubeletImage) != strings.TrimPrefix(j.TargetKubernetesVersion, "v") && imageTag(o.KubeletImage) != j.TargetKubernetesVersion {
		return fmt.Errorf("actual kubelet runtime image not recovered")
	}
	if _, cp := s.machine.Labels[clusterv1.MachineControlPlaneLabel]; cp {
		return h.controlPlaneHealth(ctx, s.cluster, j)
	}
	return nil
}
func (h *Handler) acceptIntent(ctx context.Context, s *operationSnapshot, j *op.Journal) error {
	output := &corev1.Secret{}
	if h.reader.Get(ctx, client.ObjectKeyFromObject(s.output), output) != nil || output.UID != s.output.UID {
		return fmt.Errorf("accepted intent output replaced")
	}
	if !bytes.Equal(output.Data["value"], j.Configuration) || !bytes.Equal(output.Data[configinput.FreshnessKey], j.InputFreshness) {
		return fmt.Errorf("accepted intent output does not match frozen operation")
	}
	value, err := op.EncodeAcceptedIntent(op.AcceptedIntent{MachineUID: j.Machine.UID, TalosConfigUID: j.TalosConfig.UID, Image: j.TargetImage, TransactionID: j.TransactionID})
	if err != nil {
		return err
	}
	next := map[string][]byte{}
	for k, v := range output.Data {
		next[k] = v
	}
	next[op.AcceptedIntentKey] = value
	return op.PatchData(ctx, h.client, output, next)
}

func (h *Handler) finalizeObserved(ctx context.Context, s *operationSnapshot, secret *corev1.Secret, j *op.Journal) error {
	if err := h.guardIssued(ctx, s.machine.Namespace, j); err != nil {
		return h.ambiguous(ctx, secret, j, err)
	}
	if err := h.acceptIntent(ctx, s, j); err != nil {
		return err
	}
	if err := h.guardIssued(ctx, s.machine.Namespace, j); err != nil {
		return h.ambiguous(ctx, secret, j, err)
	}
	now := metav1.NewTime(time.Now().UTC().Truncate(time.Second))
	j.Phase = op.Observed
	j.CompletedAt = &now
	receipt := &op.CompletionReceipt{Version: op.SchemaVersion, TransactionID: j.TransactionID, MachineUID: j.Machine.UID, TalosConfigUID: j.TalosConfig.UID, InfrastructureUID: j.Infrastructure.UID, InputFreshness: j.InputFreshness, Phase: op.Observed, CompletedAt: now}
	return op.WriteJournal(ctx, h.client, secret, j, receipt)
}

func workloadNodeReady(node *corev1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
