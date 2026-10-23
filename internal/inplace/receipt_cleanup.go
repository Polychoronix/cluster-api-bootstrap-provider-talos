// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package inplace

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strings"
)

func receiptConsumed(secret *corev1.Secret, j *op.Journal) bool {
	receipt := &op.CompletionReceipt{}
	return json.Unmarshal(secret.Data[op.ConsumedReceiptKey], receipt) == nil && receipt.TransactionID == j.TransactionID && receipt.MachineUID == j.Machine.UID && receipt.TalosConfigUID == j.TalosConfig.UID && receipt.InfrastructureUID == j.Infrastructure.UID && bytes.Equal(receipt.InputFreshness, j.InputFreshness)
}
func (h *Handler) consumeMachineReceipt(ctx context.Context, m *clusterv1.Machine) error {
	ref := m.Spec.Bootstrap.ConfigRef
	if !ref.IsDefined() || ref.APIGroup != bootstrapv1.GroupVersion.Group || ref.Kind != "TalosConfig" {
		return nil
	}
	config := &bootstrapv1.TalosConfig{}
	if h.reader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: ref.Name}, config) != nil {
		return fmt.Errorf("receipt cleanup config lookup failed")
	}
	if err := op.ValidateManagedMachine(ctx, h.reader, m, config); err != nil {
		return err
	}
	identity := objectIdentity(bootstrapv1.GroupVersion.Group, "TalosConfig", config)
	secret, j, err := op.ReadJournal(ctx, h.reader, m.Namespace, identity)
	if err == op.ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	if j.Phase != op.Observed || receiptConsumed(secret, j) {
		return nil
	}
	if m.UID != j.Machine.UID || config.UID != j.TalosConfig.UID || !m.DeletionTimestamp.IsZero() {
		return fmt.Errorf("receipt cleanup pinned identity changed")
	}
	if _, updating := m.Annotations[clusterv1.UpdateInProgressAnnotation]; updating || strings.Contains(m.Annotations["runtime.cluster.x-k8s.io/pending-hooks"], "UpdateMachine") {
		return fmt.Errorf("receipt cleanup Machine handshake incomplete")
	}
	if _, updating := config.Annotations[clusterv1.UpdateInProgressAnnotation]; updating {
		return fmt.Errorf("receipt cleanup bootstrap handshake incomplete")
	}
	infra := &unstructured.Unstructured{}
	version, err := contractVersion(ctx, h.reader, j.Infrastructure.APIGroup, j.Infrastructure.Kind)
	if err != nil {
		return err
	}
	infra.SetAPIVersion(j.Infrastructure.APIGroup + "/" + version)
	infra.SetKind(j.Infrastructure.Kind)
	if h.reader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: j.Infrastructure.Name}, infra) != nil || infra.GetUID() != j.Infrastructure.UID {
		return fmt.Errorf("receipt cleanup infrastructure identity changed")
	}
	if _, updating := infra.GetAnnotations()[clusterv1.UpdateInProgressAnnotation]; updating {
		return fmt.Errorf("receipt cleanup infrastructure handshake incomplete")
	}
	output := &corev1.Secret{}
	if h.reader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: config.Status.DataSecretName}, output) != nil {
		return fmt.Errorf("receipt cleanup accepted intent unavailable")
	}
	accepted, err := op.DecodeAcceptedIntent(output.Data[op.AcceptedIntentKey])
	if err != nil || accepted.TransactionID != j.TransactionID || accepted.MachineUID != m.UID || accepted.TalosConfigUID != config.UID {
		return fmt.Errorf("receipt cleanup accepted intent not durable")
	}
	return op.ConsumeReceipt(ctx, h.client, secret, j)
}
func (h *Handler) consumeMachineSetReceipts(ctx context.Context, namespace, name string) error {
	if name == "" {
		return nil
	}
	set := &clusterv1.MachineSet{}
	if h.reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, set) != nil {
		return nil
	}
	list := &clusterv1.MachineList{}
	if h.reader.List(ctx, list, client.InNamespace(namespace), client.MatchingLabels(set.Spec.Selector.MatchLabels)) != nil {
		return fmt.Errorf("receipt cleanup Machine inventory failed")
	}
	for i := range list.Items {
		m := &list.Items[i]
		owner := metav1.GetControllerOf(m)
		if owner == nil || owner.UID != set.UID {
			continue
		}
		if err := h.consumeMachineReceipt(ctx, m); err != nil {
			return err
		}
	}
	return nil
}
func (h *Handler) cleanupReceipts(ctx context.Context) {
	configs := &bootstrapv1.TalosConfigList{}
	if h.reader.List(ctx, configs) != nil {
		return
	}
	for i := range configs.Items {
		c := &configs.Items[i]
		for _, owner := range c.OwnerReferences {
			if owner.Kind != "Machine" {
				continue
			}
			m := &clusterv1.Machine{}
			if h.reader.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: owner.Name}, m) != nil || m.UID != owner.UID {
				continue
			}
			_ = h.consumeMachineReceipt(ctx, m)
		}
	}
}
