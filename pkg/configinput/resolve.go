// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package configinput resolves and renders namespace-local Talos configuration inputs.
// Resolved data and freshness are private, borrowed read-only memory, never loggable objects.
package configinput

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"

	machineryconfig "github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
)

// FreshnessKey contains private resolved input metadata in the generated Secret's data.
const FreshnessKey = "inputFreshness"

// Error identifies only a safe reference and stage, never the private underlying cause.
type Error struct{ reference, stage string }

func (e *Error) Error() string {
	return fmt.Sprintf("configuration input %s: %s failed", e.reference, e.stage)
}
func failure(reference, stage string) error { return &Error{reference: reference, stage: stage} }

// Resolved is a private immutable snapshot of desired configuration and Secret inputs.
type Resolved struct {
	config          *bootstrapv1.TalosConfig
	base            []byte
	shared, machine [][]byte
	sources         []SourceIdentity
	bundle          *secrets.Bundle
}

// SourceIdentity is a private resolved source receipt; never publish its digest.
type SourceIdentity struct {
	Name            string    `json:"name"`
	Key             string    `json:"key"`
	UID             types.UID `json:"uid"`
	ResourceVersion string    `json:"resourceVersion"`
	Digest          [32]byte  `json:"digest"`
}

// Resolve snapshots desired inputs and reads only Secrets in the TalosConfig namespace.
func Resolve(ctx context.Context, reader client.Reader, config *bootstrapv1.TalosConfig) (*Resolved, error) {
	if config == nil {
		return nil, failure("TalosConfig", "lookup")
	}
	if errs := bootstrapv1.ValidateConfigInputs(field.NewPath("spec"), config.Spec); len(errs) != 0 {
		return nil, failure("TalosConfig", "reference validation")
	}
	r := &Resolved{config: config.DeepCopy()}
	baseline := r.config.Spec.ConfigSecretRef
	if overlay := r.config.Spec.MachineConfig; overlay != nil && overlay.ConfigSecretRef != nil {
		baseline = overlay.ConfigSecretRef
	}
	load := func(ref bootstrapv1.SecretKeyReference) ([]byte, error) {
		key := types.NamespacedName{Namespace: config.Namespace, Name: ref.Name}
		secret := &corev1.Secret{}
		if err := reader.Get(ctx, key, secret); err != nil {
			return nil, failure(key.String()+"/"+ref.Key, "lookup")
		}
		data := secret.Data[ref.Key]
		if len(bytes.TrimSpace(data)) == 0 {
			return nil, failure(key.String()+"/"+ref.Key, "empty")
		}
		r.sources = append(r.sources, SourceIdentity{Name: ref.Name, Key: ref.Key, UID: secret.UID, ResourceVersion: secret.ResourceVersion, Digest: sha256.Sum256(data)})
		return data, nil
	}
	if baseline != nil {
		data, err := load(*baseline)
		if err != nil {
			return nil, err
		}
		r.base = data
	} else if r.config.Spec.GenerateType == "none" {
		if strings.TrimSpace(r.config.Spec.Data) == "" {
			return nil, failure("TalosConfig", "baseline empty")
		}
		r.base = []byte(r.config.Spec.Data)
	}
	for _, ref := range r.config.Spec.StrategicPatchSecretRefs {
		data, err := load(ref)
		if err != nil {
			return nil, err
		}
		r.shared = append(r.shared, data)
	}
	if overlay := r.config.Spec.MachineConfig; overlay != nil {
		for _, ref := range overlay.StrategicPatchSecretRefs {
			data, err := load(ref)
			if err != nil {
				return nil, err
			}
			r.machine = append(r.machine, data)
		}
	}
	if !r.HasBaseline() {
		clusterName := config.Labels["cluster.x-k8s.io/cluster-name"]
		if clusterName == "" {
			return nil, failure("generation identity", "Cluster association required")
		}
		secret := &corev1.Secret{}
		if err := reader.Get(ctx, client.ObjectKey{Namespace: config.Namespace, Name: clusterName + "-talos"}, secret); err != nil {
			return nil, failure("generation identity", "lookup")
		}
		contract := machineryconfig.TalosVersionCurrent
		if config.Spec.TalosVersion != "" {
			var err error
			contract, err = machineryconfig.ParseContractFromVersion(config.Spec.TalosVersion)
			if err != nil {
				return nil, failure("TalosConfig", "version contract")
			}
		}
		bundle, err := DecodeGenerationSecret(secret, contract)
		if err != nil {
			return nil, err
		}
		r.bundle = bundle
		encoded, err := json.Marshal(secret.Data)
		if err != nil {
			return nil, failure("generation identity", "snapshot")
		}
		r.sources = append(r.sources, SourceIdentity{Name: secret.Name, UID: secret.UID, ResourceVersion: secret.ResourceVersion, Digest: sha256.Sum256(encoded)})
	}
	return r, nil
}

// HasBaseline reports whether generation must be bypassed.
func (r *Resolved) HasBaseline() bool { return r != nil && len(r.base) != 0 }

// String prevents accidental formatted output of private configuration.
func (r *Resolved) String() string   { return "<private resolved configuration>" }
func (r *Resolved) GoString() string { return r.String() }
func (r *Resolved) MarshalJSON() ([]byte, error) {
	return []byte(`"<private resolved configuration>"`), nil
}

func (r *Resolved) baseReference() string {
	ref := r.config.Spec.ConfigSecretRef
	if r.config.Spec.MachineConfig != nil && r.config.Spec.MachineConfig.ConfigSecretRef != nil {
		ref = r.config.Spec.MachineConfig.ConfigSecretRef
	}
	if ref == nil {
		return "baseline"
	}
	return r.config.Namespace + "/" + ref.Name + "/" + ref.Key
}

func (r *Resolved) fingerprint(context RenderContext) ([]byte, error) {
	encoded, err := json.Marshal(r.config.Spec)
	if err != nil {
		return nil, failure("TalosConfig", "freshness")
	}
	metadata := struct {
		ConfigUID         types.UID        `json:"configUID"`
		Generation        int64            `json:"generation"`
		MachineUID        types.UID        `json:"machineUID"`
		ClusterUID        types.UID        `json:"clusterUID"`
		ClusterGeneration int64            `json:"clusterGeneration"`
		SpecDigest        [32]byte         `json:"specDigest"`
		MachineVersion    string           `json:"machineVersion"`
		Hostname          string           `json:"hostname"`
		ClusterDigest     [32]byte         `json:"clusterDigest"`
		InstallerImage    string           `json:"installerImage"`
		Sources           []SourceIdentity `json:"sources"`
	}{ConfigUID: r.config.UID, Generation: r.config.Generation, MachineUID: context.Machine.UID, ClusterUID: context.Cluster.UID, ClusterGeneration: context.Cluster.Generation,
		SpecDigest: sha256.Sum256(encoded), MachineVersion: context.Machine.Spec.Version,
		Hostname: context.Machine.Name + "/" + context.Machine.Spec.InfrastructureRef.Name, InstallerImage: context.InstallerImage, Sources: r.sources}
	clusterInputs, err := json.Marshal(struct {
		Name     string
		Network  any
		Endpoint any
	}{context.Cluster.Name, context.Cluster.Spec.ClusterNetwork, context.Cluster.Spec.ControlPlaneEndpoint})
	if err != nil {
		return nil, failure("Cluster", "freshness")
	}
	metadata.ClusterDigest = sha256.Sum256(clusterInputs)
	return json.Marshal(metadata)
}

// Sources returns an owned source snapshot for a private Prepared transaction.
func (r *Resolved) Sources() []SourceIdentity { return append([]SourceIdentity(nil), r.sources...) }

// Freshness returns the private fingerprint of this exact resolved snapshot.
func (r *Resolved) Freshness(context RenderContext) ([]byte, error) {
	if context.Machine == nil || context.Cluster == nil {
		return nil, failure("owner", "freshness")
	}
	return r.fingerprint(context)
}

// IsFresh re-resolves source identities before accepting a generated output Secret.
func IsFresh(ctx context.Context, reader client.Reader, config *bootstrapv1.TalosConfig, context RenderContext, output *corev1.Secret) (bool, error) {
	if output == nil || len(output.Data["value"]) == 0 || len(output.Data[FreshnessKey]) == 0 {
		return false, nil
	}
	resolved, err := Resolve(ctx, reader, config)
	if err != nil {
		return false, err
	}
	if context.Machine == nil || context.Cluster == nil {
		return false, failure("owner", "freshness")
	}
	expected, err := resolved.fingerprint(context)
	if err != nil {
		return false, err
	}
	return string(expected) == string(output.Data[FreshnessKey]), nil
}
