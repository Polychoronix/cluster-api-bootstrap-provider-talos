// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package talosclient

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"strings"

	"github.com/cosi-project/runtime/pkg/resource"
	"github.com/cosi-project/runtime/pkg/safe"
	"github.com/cosi-project/runtime/pkg/state"
	"github.com/google/uuid"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	machineryclient "github.com/siderolabs/talos/pkg/machinery/client"
	configresource "github.com/siderolabs/talos/pkg/machinery/resources/config"
	k8sresource "github.com/siderolabs/talos/pkg/machinery/resources/k8s"
	runtimeresource "github.com/siderolabs/talos/pkg/machinery/resources/runtime"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
)

// Observation contains private active configuration and actual runtime identities.
// Configuration must only be stored in a Secret, never logged or formatted.
type Observation struct {
	Configuration []byte
	BootID        string
	OSVersion     string
	KubeletImage  string
	SchematicID   string
	Extensions    []ExtensionObservation
}

// ExtensionObservation identifies an installed, non-virtual extension.
type ExtensionObservation struct {
	Name    string
	Version string
	Image   string
}

// String deliberately hides both configuration and untrusted remote identities.
func (Observation) String() string { return "Talos observation (redacted)" }

// GoString also protects configuration from %#v formatting.
func (Observation) GoString() string { return "Talos observation (redacted)" }

// Observe reads full active configuration, OS version and runtime identities,
// bracketed by boot identity reads. It never issues a lifecycle or mutation RPC.
// Older Talos may lack kubelet status: absence leaves the actual image unknown,
// never substituted from desired config or the incompatible system CRI socket.
func Observe(ctx context.Context, talos *machineryclient.Client, endpoint bootstrapv1.TalosEndpoint) (*Observation, error) {
	if talos == nil || talos.COSI == nil {
		return nil, errors.New("Talos observation client unavailable")
	}
	ctx = WithNode(ctx, endpoint)
	boot, err := observeBoot(ctx, talos, false)
	if err != nil {
		return nil, err
	}
	observation := &Observation{BootID: boot.id}
	active, err := safe.StateGetByID[*configresource.MachineConfig](ctx, talos.COSI, configresource.ActiveID)
	if err != nil || active == nil || active.Provider() == nil {
		return nil, errors.New("Talos active configuration observation failed")
	}
	observation.Configuration, err = active.Provider().Bytes()
	if err != nil || len(bytes.TrimSpace(observation.Configuration)) == 0 {
		return nil, errors.New("Talos active configuration serialization failed")
	}
	version, err := safe.StateGetByID[*runtimeresource.Version](ctx, talos.COSI, "version")
	if err != nil || version == nil || strings.TrimSpace(version.TypedSpec().Version) == "" {
		return nil, errors.New("Talos runtime version observation failed")
	}
	observation.OSVersion = version.TypedSpec().Version

	schematic, err := safe.StateGetByID[*runtimeresource.ImageFactorySchematic](ctx, talos.COSI, runtimeresource.ImageFactorySchematicID)
	schematicMissing := state.IsNotFoundError(err)
	if !schematicMissing {
		if err != nil || schematic == nil || strings.TrimSpace(schematic.TypedSpec().SchematicID) == "" {
			return nil, errors.New("Talos schematic observation failed")
		}
		observation.SchematicID = schematic.TypedSpec().SchematicID
	}
	extensions, err := talos.COSI.List(ctx, resource.NewMetadata(runtimeresource.NamespaceName, runtimeresource.ExtensionStatusType, "", resource.VersionUndefined))
	if err != nil {
		return nil, errors.New("Talos extension observation failed")
	}
	seen := make(map[string]struct{}, len(extensions.Items))
	virtualID := ""
	for _, item := range extensions.Items {
		extension, ok := item.(*runtimeresource.ExtensionStatus)
		if !ok || extension == nil {
			return nil, errors.New("Talos extension observation failed")
		}
		layer := extension.TypedSpec()
		name := layer.Metadata.Name
		if _, duplicate := seen[name]; duplicate {
			return nil, errors.New("Talos duplicate extension identity observation failed")
		}
		seen[name] = struct{}{}
		// The Image Factory reserves this metadata name for its virtual
		// schematic extension; its version is the content-addressed schematic.
		if name == "schematic" {
			if !validSchematicID(layer.Metadata.Version) {
				return nil, errors.New("Talos virtual schematic observation failed")
			}
			virtualID = layer.Metadata.Version
			continue
		}
		if strings.TrimSpace(name) == "" || strings.TrimSpace(layer.Metadata.Version) == "" || strings.TrimSpace(layer.Image) == "" {
			return nil, errors.New("Talos extension identity observation failed")
		}
		observation.Extensions = append(observation.Extensions, ExtensionObservation{Name: name, Version: layer.Metadata.Version, Image: layer.Image})
	}
	if schematicMissing {
		observation.SchematicID = virtualID
	} else if virtualID != "" && virtualID != observation.SchematicID {
		return nil, errors.New("Talos schematic identity observation inconsistent")
	}
	slices.SortFunc(observation.Extensions, func(a, b ExtensionObservation) int { return strings.Compare(a.Name, b.Name) })

	kubelets, err := talos.COSI.List(ctx, resource.NewMetadata(k8sresource.NamespaceName, k8sresource.KubeletStatusType, "", resource.VersionUndefined))
	if err != nil && !state.IsNotFoundError(err) && status.Code(err) != codes.NotFound || len(kubelets.Items) > 1 {
		return nil, errors.New("Talos kubelet runtime observation failed")
	}
	if len(kubelets.Items) == 1 {
		kubelet, ok := kubelets.Items[0].(*k8sresource.KubeletStatus)
		if !ok || kubelet == nil || strings.TrimSpace(kubelet.TypedSpec().Image) == "" {
			return nil, errors.New("Talos kubelet runtime observation failed")
		}
		observation.KubeletImage = kubelet.TypedSpec().Image
	}
	lastBoot, err := observeBoot(ctx, talos, boot.kernel)
	if err != nil || lastBoot.id != boot.id || lastBoot.kernel != boot.kernel {
		return nil, errors.New("Talos boot identity changed during observation")
	}
	return observation, nil
}

func validSchematicID(id string) bool {
	if len(id) != 64 {
		return false
	}
	for _, c := range id {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

type observedBoot struct {
	id     string
	kernel bool
}

func observeBoot(ctx context.Context, talos *machineryclient.Client, kernel bool) (observedBoot, error) {
	if !kernel {
		boot, err := safe.StateGetByID[*runtimeresource.BootID](ctx, talos.COSI, runtimeresource.BootIDID)
		if !state.IsNotFoundError(err) {
			if err != nil || boot == nil || strings.TrimSpace(boot.TypedSpec().BootID) == "" {
				return observedBoot{}, errors.New("Talos boot identity observation failed")
			}
			return observedBoot{id: boot.TypedSpec().BootID}, nil
		}
	}
	if talos.MachineClient == nil {
		return observedBoot{}, errors.New("Talos kernel boot identity observation failed")
	}
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := talos.MachineClient.Read(readCtx, &machineapi.ReadRequest{Path: "/proc/sys/kernel/random/boot_id"})
	if err != nil {
		return observedBoot{}, errors.New("Talos kernel boot identity observation failed")
	}
	// A canonical kernel UUID plus newline is 37 bytes. Read through EOF,
	// rejecting oversized/truncated streams and both modern and legacy errors.
	var data [37]byte
	n := 0
	for {
		chunk, receiveErr := stream.Recv()
		if errors.Is(receiveErr, io.EOF) {
			break
		}
		if receiveErr != nil || chunk == nil {
			return observedBoot{}, errors.New("Talos kernel boot identity observation failed")
		}
		md := chunk.GetMetadata()
		if md != nil && (md.GetError() != "" || md.GetStatus().GetCode() != 0) || len(chunk.GetBytes()) > len(data)-n {
			return observedBoot{}, errors.New("Talos kernel boot identity observation failed")
		}
		n += copy(data[n:], chunk.GetBytes())
	}
	if n != len(data) || data[36] != '\n' {
		return observedBoot{}, errors.New("Talos kernel boot identity observation failed")
	}
	id := string(data[:36])
	parsed, err := uuid.Parse(id)
	if err != nil || parsed == uuid.Nil || parsed.String() != id {
		return observedBoot{}, errors.New("Talos kernel boot identity observation failed")
	}
	return observedBoot{id: id, kernel: true}, nil
}
