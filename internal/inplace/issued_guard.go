// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package inplace

import (
	"context"
	"fmt"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	"k8s.io/apimachinery/pkg/runtime"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (h *Handler) guardIssued(ctx context.Context, namespace string, j *op.Journal) error {
	m := &clusterv1.Machine{}
	c := &bootstrapv1.TalosConfig{}
	if h.reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: j.Machine.Name}, m) != nil || h.reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: j.TalosConfig.Name}, c) != nil {
		return fmt.Errorf("issued operation live intent unavailable")
	}
	s, fresh, err := h.snapshot(ctx, m, runtime.RawExtension{Object: c})
	if err != nil {
		return err
	}
	if !fresh {
		return fmt.Errorf("issued operation input no longer current")
	}
	return h.guard(ctx, s, j)
}
