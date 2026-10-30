// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package inplace

import (
	"context"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	endpointclient "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/talosclient"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func NewOperationNodeClient(reader client.Reader) OperationNodeFactory {
	return func(ctx context.Context, cluster types.NamespacedName, endpoint bootstrapv1.TalosEndpoint) (OperationNode, error) {
		c, err := endpointclient.New(ctx, reader, cluster, endpoint)
		if err != nil {
			return nil, err
		}
		return &talosNodeClient{client: c, endpoint: endpoint}, nil
	}
}
