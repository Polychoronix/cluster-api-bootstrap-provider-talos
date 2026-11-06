// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v1beta1

// SecretKeyReference selects required data in a Secret in the TalosConfig's namespace.
// Neither arbitrary namespaces nor templated names or filesystem paths are supported.
type SecretKeyReference struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	Name string `json:"name"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[-._a-zA-Z0-9]+$`
	// +kubebuilder:validation:XValidation:rule="self != '.' && !self.startsWith('..')",message="must not be '.' or start with '..'"
	Key string `json:"key"`
}

// MachineConfigOverlay is per-node input applied without discarding shared policy.
type MachineConfigOverlay struct {
	// ConfigSecretRef replaces only the base, never shared patches.
	// +optional
	ConfigSecretRef *SecretKeyReference `json:"configSecretRef,omitempty"`
	// +listType=atomic
	// +optional
	ConfigPatches []ConfigPatches `json:"configPatches,omitempty"`
	// +listType=atomic
	// +optional
	StrategicPatches []string `json:"strategicPatches,omitempty"`
	// +listType=atomic
	// +optional
	StrategicPatchSecretRefs []SecretKeyReference `json:"strategicPatchSecretRefs,omitempty"`
	// ConfigRevision is independent of the shared revision and contains no private data.
	// +optional
	ConfigRevision string `json:"configRevision,omitempty"`
}

// TalosEndpoint separates dial transport, certificate identity and optional target routing.
type TalosEndpoint struct {
	// Address is an explicit host:port, with bracketed IPv6 when applicable.
	// +kubebuilder:validation:MinLength=1
	Address string `json:"address"`
	// ServerName is the certificate verification/SNI name without a port.
	// +kubebuilder:validation:MinLength=1
	ServerName string `json:"serverName"`
	// Node identifies a single target when the endpoint is an aggregated Talos proxy.
	// +optional
	Node string `json:"node,omitempty"`
}
