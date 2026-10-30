// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package inplace

import (
	"context"
	"fmt"

	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	talosclient "github.com/siderolabs/talos/pkg/machinery/client"
	"google.golang.org/protobuf/types/known/emptypb"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	bootstrapv1beta1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	endpointclient "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/talosclient"
)

// NodeClient is the slice of the Talos machine API that an in-place update needs.
//
// It exists as an interface so the update state machine can be exercised without hardware:
// the real implementation talks gRPC to a node, and tests substitute a fake. Everything
// that reboots a machine sits behind this boundary.
type NodeClient interface {

	// ApplyConfig applies a machine configuration in AUTO mode, which lets Talos decide
	// whether the change requires a reboot.
	ApplyConfig(ctx context.Context, data []byte) error

	// Close releases the underlying connection.
	Close() error
}

// NodeClientFactory opens a NodeClient for the given machine.
type NodeClientFactory func(ctx context.Context, clusterKey types.NamespacedName, endpoint bootstrapv1beta1.TalosEndpoint) (NodeClient, error)

// talosNodeClient is the gRPC-backed NodeClient.
type talosNodeClient struct {
	client   *talosclient.Client
	endpoint bootstrapv1beta1.TalosEndpoint
}

// NewNodeClient builds a NodeClient for a machine using the cluster's talosconfig secret,
// which CABPT maintains as <cluster>-talosconfig.
func NewNodeClient(c client.Client) NodeClientFactory {
	return func(ctx context.Context, clusterKey types.NamespacedName, endpoint bootstrapv1beta1.TalosEndpoint) (NodeClient, error) {
		tc, err := endpointclient.New(ctx, c, clusterKey, endpoint)
		if err != nil {
			return nil, err
		}
		return &talosNodeClient{client: tc, endpoint: endpoint}, nil
	}
}

func (t *talosNodeClient) Version(ctx context.Context) (string, error) {
	ctx = endpointclient.WithNode(ctx, t.endpoint)
	resp, err := t.client.MachineClient.Version(ctx, &emptypb.Empty{})
	if err != nil || len(resp.GetMessages()) != 1 || resp.Messages[0] == nil || operationMetadataFailed(resp.Messages[0].Metadata) || resp.Messages[0].GetVersion().GetTag() == "" {
		return "", fmt.Errorf("Talos version observation failed")
	}
	return resp.Messages[0].GetVersion().GetTag(), nil
}

func (t *talosNodeClient) ApplyConfig(ctx context.Context, data []byte) error {
	ctx = endpointclient.WithNode(ctx, t.endpoint)
	// AUTO lets Talos work out whether the change can be applied to the running system or
	// needs a reboot. Deciding that here would mean reimplementing Talos' own knowledge of
	// which config fields are hot-reloadable.
	_, err := t.client.ApplyConfiguration(ctx, &machineapi.ApplyConfigurationRequest{
		Data: data,
		Mode: machineapi.ApplyConfigurationRequest_AUTO,
	})
	if err != nil {
		return fmt.Errorf("Talos configuration apply failed")
	}

	return nil
}

func (t *talosNodeClient) Close() error {
	return t.client.Close()
}
