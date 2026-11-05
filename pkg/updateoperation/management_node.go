// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"context"

	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// validateManagementNode checks the member's live workload Node against the activated
// snapshot. A failed lookup is unavailability (ErrWorkloadUnavailable), never reported as
// a changed identity; only a Node that is gone or carries another UID/providerID is.
func validateManagementNode(ctx context.Context, reader client.Reader, safety *managementSafety, member *managementMember) error {
	secret := &corev1.Secret{}
	if reader.Get(ctx, client.ObjectKey{Namespace: safety.Authority.Namespace, Name: safety.ClusterName + "-kubeconfig"}, secret) != nil {
		return managementError("activated workload credentials")
	}
	config, err := WorkloadRESTConfig(secret.Data["value"])
	if err != nil {
		return managementError("activated workload credentials parse")
	}
	nodes, err := WorkloadNodes(config)
	if err != nil {
		return managementError("activated workload client")
	}
	node, err := nodes.Get(ctx, member.NodeName, metav1.GetOptions{})
	if err != nil {
		detail, notFound := WorkloadFailure(err)
		if notFound {
			return managementErrorDetail("activated workload Node identity changed", member.NodeName+": "+detail)
		}
		return workloadUnavailable("activated workload Node lookup", member.NodeName+": "+detail)
	}
	if node.UID != member.NodeUID {
		return managementErrorDetail("activated workload Node identity changed", member.NodeName+": UID "+string(member.NodeUID)+" -> "+string(node.UID))
	}
	if node.Spec.ProviderID != member.ProviderID {
		return managementErrorDetail("activated workload Node identity changed", member.NodeName+": providerID "+member.ProviderID+" -> "+node.Spec.ProviderID)
	}
	// Readiness and expected deletion are lifecycle state, not revoked identity.
	// Issuance preflight/recovery owns those health gates; a rebooting same-UID
	// Node remains authorized without permitting another destructive request.
	return nil
}

// Initial observation captures RV to reject source changes during approval.
// Durable identity/seed guards compare UID and actual bytes, not harmless Secret
// metadata-only RV churn. Requested configuration sources use current freshness.
func sameManagementIdentity(a, b configinput.SourceIdentity) bool {
	return a.Name == b.Name && a.Key == b.Key && a.UID == b.UID && a.Digest == b.Digest
}
