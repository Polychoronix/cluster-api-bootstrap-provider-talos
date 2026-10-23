// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"context"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"time"
)

func validateManagementNode(ctx context.Context, reader client.Reader, safety *managementSafety, member *managementMember) error {
	secret := &corev1.Secret{}
	if reader.Get(ctx, client.ObjectKey{Namespace: safety.Authority.Namespace, Name: safety.ClusterName + "-kubeconfig"}, secret) != nil {
		return managementError("activated workload credentials")
	}
	config, err := clientcmd.RESTConfigFromKubeConfig(secret.Data["value"])
	if err != nil {
		return managementError("activated workload credentials parse")
	}
	config.Timeout = 5 * time.Second
	workload, err := client.New(config, client.Options{})
	if err != nil {
		return managementError("activated workload client")
	}
	node := &corev1.Node{}
	if workload.Get(ctx, client.ObjectKey{Name: member.NodeName}, node) != nil || node.UID != member.NodeUID || node.Spec.ProviderID != member.ProviderID || !node.DeletionTimestamp.IsZero() {
		return managementError("activated workload Node identity changed")
	}
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
			return nil
		}
	}
	return managementError("activated workload Node not ready")
}

// Initial observation captures RV to reject source changes during approval.
// Durable identity/seed guards compare UID and actual bytes, not harmless Secret
// metadata-only RV churn. Requested configuration sources use current freshness.
func sameManagementIdentity(a, b configinput.SourceIdentity) bool {
	return a.Name == b.Name && a.Key == b.Key && a.UID == b.UID && a.Digest == b.Digest
}
