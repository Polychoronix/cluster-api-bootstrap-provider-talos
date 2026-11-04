// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"context"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	endpointclient "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/talosclient"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ConfigDriftReport is the outcome of one drift observation: the documents on which the
// node's live configuration and the TalosConfig's render differ, by apiVersion/kind[/name].
type ConfigDriftReport struct {
	Differing []string
}

// Drifted reports whether any document differs.
func (r *ConfigDriftReport) Drifted() bool { return r != nil && len(r.Differing) > 0 }

// ObserveConfigDrift compares the node's live machine configuration with what the
// TalosConfig renders for its Machine today. It is read-only: nothing is applied, and the
// result carries document identities only. Transport and observation failures are returned
// as StageErrors ("Talos transport", "actual Talos observation") so the caller can report
// an unknown verdict rather than drift.
func ObserveConfigDrift(ctx context.Context, reader client.Reader, m *clusterv1.Machine, tc *bootstrapv1.TalosConfig, cluster *clusterv1.Cluster) (*ConfigDriftReport, error) {
	if reader == nil || m == nil || tc == nil || cluster == nil {
		return nil, managementError("drift inventory")
	}
	if tc.Spec.TalosEndpoint == nil {
		return nil, managementError("explicit Talos endpoint")
	}
	spec := tc.Spec.DeepCopy()
	spec.Manage = false
	rendered, err := renderManagedConfig(ctx, reader, tc, spec, m, cluster)
	if err != nil {
		return nil, err
	}
	talos, err := endpointclient.New(ctx, reader, client.ObjectKey{Namespace: cluster.Namespace, Name: cluster.Name}, *tc.Spec.TalosEndpoint)
	if err != nil {
		return nil, managementError("Talos transport")
	}
	defer talos.Close()
	observed, err := endpointclient.Observe(ctx, talos, *tc.Spec.TalosEndpoint)
	if err != nil {
		return nil, managementError("actual Talos observation")
	}
	differing, err := managementConfigDiff(observed.Configuration, rendered)
	if err != nil {
		return nil, err
	}
	return &ConfigDriftReport{Differing: differing}, nil
}

// IsObservationPending reports whether a drift observation failed before the node's
// configuration could be compared, i.e. the verdict is unknown rather than drifted.
func IsObservationPending(err error) bool {
	stage, ok := err.(*StageError)
	return ok && (stage.Stage == "Talos transport" || stage.Stage == "actual Talos observation")
}
