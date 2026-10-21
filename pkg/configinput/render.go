// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package configinput

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/configloader"
	"github.com/siderolabs/talos/pkg/machinery/config/configpatcher"
	"github.com/siderolabs/talos/pkg/machinery/config/encoder"
	"github.com/siderolabs/talos/pkg/machinery/config/generate"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"
	"github.com/siderolabs/talos/pkg/machinery/config/validation"
	"github.com/siderolabs/talos/pkg/machinery/constants"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
)

// RenderContext provides generation-only identity plus actual owner inputs.
type RenderContext struct {
	Machine        *clusterv1.Machine
	Cluster        *clusterv1.Cluster
	SecretsBundle  *secrets.Bundle
	InstallerImage string
}

// Rendered contains private output and freshness, never a public marshaling surface.
type Rendered struct{ data, freshness []byte }

func (r *Rendered) Data() []byte      { return r.data }
func (r *Rendered) Freshness() []byte { return r.freshness }
func (r *Rendered) String() string    { return "<private rendered configuration>" }
func (r *Rendered) GoString() string  { return r.String() }
func (r *Rendered) MarshalJSON() ([]byte, error) {
	return []byte(`"<private rendered configuration>"`), nil
}

// Render uses Talos' complete document container and established patch precedence.
func Render(inputs *Resolved, context RenderContext) (*Rendered, error) {
	if inputs == nil || context.Machine == nil || context.Cluster == nil {
		return nil, failure("owner", "render")
	}
	spec := inputs.config.Spec
	contract := config.TalosVersionCurrent
	var err error
	if spec.TalosVersion != "" {
		contract, err = config.ParseContractFromVersion(spec.TalosVersion)
		if err != nil {
			return nil, failure("TalosConfig", "version contract")
		}
	}
	data := inputs.base
	var hostname string
	if len(data) == 0 {
		if context.SecretsBundle == nil {
			return nil, failure("cluster identity", "missing")
		}
		kind, parseErr := machine.ParseType(spec.GenerateType)
		if parseErr != nil || kind == machine.TypeUnknown {
			return nil, failure("TalosConfig", "role")
		}
		version := strings.TrimPrefix(context.Machine.Spec.Version, "v")
		if version == "" {
			version = constants.DefaultKubernetesVersion
		}
		domain := context.Cluster.Spec.ClusterNetwork.ServiceDomain
		if domain == "" {
			domain = constants.DefaultDNSDomain
		}
		opts := []generate.Option{generate.WithDNSDomain(domain), generate.WithVersionContract(contract), generate.WithSecretsBundle(context.SecretsBundle)}
		if contract != nil && contract.Major < 1 && contract.Minor < 14 {
			opts = append(opts, generate.WithClusterDiscovery(false))
		}
		endpoint := context.Cluster.Spec.ControlPlaneEndpoint
		input, generationErr := generate.NewInput(context.Cluster.Name, "https://"+net.JoinHostPort(endpoint.Host, strconv.Itoa(int(endpoint.Port))), version, opts...)
		if generationErr != nil {
			return nil, failure("TalosConfig", "generation")
		}
		if cidrs := context.Cluster.Spec.ClusterNetwork.Pods.CIDRBlocks; cidrs != nil {
			input.PodNet = cidrs
		}
		if cidrs := context.Cluster.Spec.ClusterNetwork.Services.CIDRBlocks; cidrs != nil {
			input.ServiceNet = cidrs
		}
		provider, generationErr := input.Config(kind)
		if generationErr != nil {
			return nil, failure("TalosConfig", "generation")
		}
		data, err = provider.EncodeBytes(encoder.WithComments(encoder.CommentsDisabled))
		if err != nil {
			return nil, failure("TalosConfig", "encoding")
		}
		switch spec.Hostname.Source {
		case bootstrapv1.HostnameSourceMachineName:
			hostname = context.Machine.Name
		case bootstrapv1.HostnameSourceInfrastructureName:
			hostname = context.Machine.Spec.InfrastructureRef.Name
		}
	}
	provider, err := configloader.NewFromBytes(data)
	if err != nil {
		return nil, failure("baseline", "parse")
	}
	if provider.Machine() == nil {
		return nil, failure("baseline", "role")
	}
	if spec.GenerateType == "worker" || spec.GenerateType == "controlplane" {
		kind, _ := machine.ParseType(spec.GenerateType)
		if provider.Machine().Type() != kind {
			return nil, failure("baseline", "role validation")
		}
	}
	data, err = provider.EncodeBytes(encoder.WithComments(encoder.CommentsDisabled))
	if err != nil {
		return nil, failure("baseline", "encoding")
	}
	applyJSON := func(patches []bootstrapv1.ConfigPatches, stage string) error {
		if len(patches) == 0 {
			return nil
		}
		encoded, err := json.Marshal(patches)
		if err != nil {
			return failure(stage, "JSON patch")
		}
		patch, err := jsonpatch.DecodePatch(encoded)
		if err != nil {
			return failure(stage, "JSON patch")
		}
		data, err = configpatcher.JSON6902(data, patch)
		if err != nil {
			return failure(stage, "JSON patch (single-document configuration required)")
		}
		return nil
	}
	applyStrategic := func(values [][]byte, stage string) error {
		if len(values) == 0 {
			return nil
		}
		patches := make([]configpatcher.Patch, 0, len(values))
		for _, value := range values {
			patch, err := configpatcher.LoadPatch(value)
			if err != nil {
				return failure(stage, "strategic patch parse")
			}
			patches = append(patches, patch)
		}
		out, err := configpatcher.Apply(configpatcher.WithBytes(data), patches)
		if err != nil {
			return failure(stage, "strategic patch apply")
		}
		provider, err := out.Config()
		if err != nil {
			return failure(stage, "strategic patch result")
		}
		data, err = provider.EncodeBytes(encoder.WithComments(encoder.CommentsDisabled))
		if err != nil {
			return failure(stage, "encoding")
		}
		return nil
	}
	if err := applyJSON(spec.ConfigPatches, "shared public"); err != nil {
		return nil, err
	}
	values := make([][]byte, 0, len(spec.StrategicPatches)+2)
	if context.InstallerImage != "" {
		values = append(values, []byte(InstallerPatch(contract, context.InstallerImage)))
	}
	for _, patch := range spec.StrategicPatches {
		values = append(values, []byte(patch))
	}
	if hostname != "" {
		var patch []byte
		if contract != nil && (contract.Major < 1 || contract.Major == 1 && contract.Minor < 12) {
			patch = []byte(fmt.Sprintf("machine:\n  network:\n    hostname: %s\n", hostname))
		} else {
			patch = []byte(fmt.Sprintf("apiVersion: v1alpha1\nkind: HostnameConfig\nauto: off\nhostname: %s\n", hostname))
		}
		values = append(values, patch)
	}
	if err := applyStrategic(values, "shared public"); err != nil {
		return nil, err
	}
	if err := applyStrategic(inputs.shared, "shared Secret"); err != nil {
		return nil, err
	}
	if overlay := spec.MachineConfig; overlay != nil {
		if err := applyJSON(overlay.ConfigPatches, "Machine public"); err != nil {
			return nil, err
		}
		values = values[:0]
		for _, patch := range overlay.StrategicPatches {
			values = append(values, []byte(patch))
		}
		if err := applyStrategic(values, "Machine public"); err != nil {
			return nil, err
		}
	}
	if err := applyStrategic(inputs.machine, "Machine Secret"); err != nil {
		return nil, err
	}
	if inputs.HasBaseline() {
		if err := validateBaselineVersion(data, context.Machine.Spec.Version); err != nil {
			return nil, err
		}
	}
	finalConfig, err := configloader.NewFromBytes(data)
	if err != nil {
		return nil, failure("rendered", "parse")
	}
	if _, err := finalConfig.ValidateAsClient(renderMode{}, validation.WithLocal()); err != nil {
		return nil, failure("rendered", "validation")
	}
	freshness, err := inputs.fingerprint(context)
	if err != nil {
		return nil, err
	}
	return &Rendered{data: data, freshness: freshness}, nil
}

type renderMode struct{}

func (renderMode) String() string        { return "configuration input renderer" }
func (renderMode) RequiresInstall() bool { return false }
func (renderMode) InContainer() bool     { return false }

// InstallerPatch preserves Talos' versioned installer document convention and precedence.
func InstallerPatch(contract *config.VersionContract, image string) string {
	if contract.UnattendedInstallConfig() {
		return fmt.Sprintf("apiVersion: v1alpha1\nkind: UnattendedInstallConfig\ninstaller:\n  image: %s\nprovisioning:\n  diskSelector:\n    match: system_disk\n", image)
	}
	return fmt.Sprintf("machine:\n  install:\n    image: %s\n", image)
}
