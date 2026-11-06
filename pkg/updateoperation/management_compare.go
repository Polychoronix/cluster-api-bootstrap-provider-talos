// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"bytes"
	"context"
	"sort"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
	coreconfig "github.com/siderolabs/talos/pkg/machinery/config/config"
	"github.com/siderolabs/talos/pkg/machinery/config/configloader"
	"github.com/siderolabs/talos/pkg/machinery/config/container"
	"github.com/siderolabs/talos/pkg/machinery/config/encoder"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// managementConfigDocuments splits a machine configuration into its documents keyed by
// apiVersion/kind[/name], each re-encoded without comments. Two configurations with the
// same documents in a different order compare equal; a live `talosctl patch` appends
// documents where the renderer would have placed them elsewhere.
func managementConfigDocuments(data []byte) (map[string][]byte, error) {
	provider, err := configloader.NewFromBytes(data)
	if err != nil {
		return nil, managementError("active configuration parse")
	}
	ctr, ok := provider.(*container.Container)
	if !ok {
		return nil, managementError("active configuration parse")
	}
	documents := map[string][]byte{}
	for _, document := range ctr.Documents() {
		key := document.APIVersion() + "/" + document.Kind()
		if named, ok := document.(coreconfig.NamedDocument); ok {
			key += "/" + named.Name()
		}
		encoded, err := encoder.NewEncoder(document, encoder.WithComments(encoder.CommentsDisabled)).Encode()
		if err != nil {
			return nil, managementError("active configuration encode")
		}
		if _, duplicate := documents[key]; duplicate {
			return nil, managementError("active configuration document identity")
		}
		documents[key] = encoded
	}
	return documents, nil
}

// managementConfigDiff lists the document keys (apiVersion/kind[/name], never content) on
// which two machine configurations differ: documents only one side has, and documents
// whose encoding differs. An empty list means the configurations are the same set of
// documents regardless of order.
func managementConfigDiff(actual, desired []byte) ([]string, error) {
	left, err := managementConfigDocuments(actual)
	if err != nil {
		return nil, err
	}
	right, err := managementConfigDocuments(desired)
	if err != nil {
		return nil, err
	}
	differing := []string{}
	for key, value := range left {
		if other, ok := right[key]; !ok || !bytes.Equal(value, other) {
			differing = append(differing, key)
		}
	}
	for key := range right {
		if _, ok := left[key]; !ok {
			differing = append(differing, key)
		}
	}
	sort.Strings(differing)
	return differing, nil
}

// renderManagedConfig renders what a TalosConfig spec means for one Machine: the private
// inputs resolved, the reviewed installer applied. The result is private configuration and
// must only be compared, never published.
func renderManagedConfig(ctx context.Context, reader client.Reader, tc *bootstrapv1.TalosConfig, spec *bootstrapv1.TalosConfigSpec, m *clusterv1.Machine, cluster *clusterv1.Cluster) ([]byte, error) {
	normalized := tc.DeepCopy()
	normalized.Spec = *spec.DeepCopy()
	normalized.Spec.Manage = false
	normalized.Generation = 0
	inputs, err := configinput.Resolve(ctx, reader, normalized)
	if err != nil {
		return nil, managementError("private baseline resolution")
	}
	installer := ""
	if normalized.Spec.ImageFactory != nil {
		if tc.Status.ImageFactory == nil || tc.Status.ImageFactory.InstallerImage == "" {
			return nil, managementError("reviewed installer resolution missing")
		}
		installer = tc.Status.ImageFactory.InstallerImage
	}
	rendered, err := configinput.Render(inputs, configinput.RenderContext{Machine: m, Cluster: cluster, InstallerImage: installer})
	if err != nil {
		return nil, managementError("baseline rendering")
	}
	return rendered.Data(), nil
}
