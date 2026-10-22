// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package inplace

import (
	"bytes"
	"context"
	"fmt"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	"github.com/siderolabs/talos/pkg/machinery/config/configloader"
	"github.com/siderolabs/talos/pkg/machinery/config/encoder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"time"
)

func activeConfigurationMatches(actual, desired []byte) bool {
	a, err := configloader.NewFromBytes(actual)
	if err != nil {
		return false
	}
	b, err := configloader.NewFromBytes(desired)
	if err != nil {
		return false
	}
	x, err := a.EncodeBytes(encoder.WithComments(encoder.CommentsDisabled))
	if err != nil {
		return false
	}
	y, err := b.EncodeBytes(encoder.WithComments(encoder.CommentsDisabled))
	return err == nil && bytes.Equal(x, y)
}
func (h *Handler) observeStep(ctx context.Context, s *operationSnapshot, j *op.Journal, index int) (bool, error) {
	step := j.Steps[index]
	if step.Deadline == nil || time.Now().After(step.Deadline.Time) {
		return false, fmt.Errorf("operation recovery deadline expired")
	}
	if step.Kind == op.PullInstaller || step.Kind == op.Install {
		return true, nil
	} // persisted digest/list and explicit install exit are effect witnesses
	node, err := h.newNodeClient(ctx, client.ObjectKey{Namespace: s.cluster.Namespace, Name: s.cluster.Name}, j.Endpoint)
	if err != nil {
		return false, nil
	}
	defer node.Close()
	observation, err := node.Observe(ctx)
	if err != nil {
		return false, nil
	}
	if step.Kind == op.Apply && !activeConfigurationMatches(observation.Configuration, j.Configuration) {
		return false, nil
	}
	if step.Kind == op.Reboot || step.Kind == op.Apply && step.ApplyOutcome == op.RebootRequired {
		if observation.BootID == "" || observation.BootID == step.IssuedBootID {
			return false, nil
		}
	}
	j.Steps[index].ObservedBootID = observation.BootID
	return true, nil
}
