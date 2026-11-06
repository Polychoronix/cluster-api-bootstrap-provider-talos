// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package inplace

import (
	"context"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	"k8s.io/apimachinery/pkg/types"
)

type NodeObservation struct {
	Configuration []byte
	BootID        string
	OSVersion     string
	SchematicID   string
	Extensions    []ExtensionObservation
	KubeletImage  string
}

type ExtensionObservation struct {
	Name    string
	Version string
	Image   string
}
type OperationNode interface {
	Observe(context.Context) (NodeObservation, error)
	Apply(context.Context, []byte) (updateoperation.ApplyOutcome, error)
	PullInstaller(context.Context, string) (name, digest string, err error)
	Install(context.Context, string) (int32, error)
	Reboot(context.Context) error
	Version(context.Context) (string, error)
	Close() error
}

type OperationNodeFactory func(context.Context, types.NamespacedName, bootstrapv1.TalosEndpoint) (OperationNode, error)
