// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package release_test

import (
	"bytes"
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

func repository(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func copyPublicSources(t *testing.T, source, destination string) {
	t.Helper()
	require.NoError(t, filepath.WalkDir(source, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0700)
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, content, 0600)
	}))
}

func TestTaggedLocalReleaseUsesActualKustomizeAndOnlyPublicSources(t *testing.T) {
	tool := os.Getenv("RELEASE_TEST_KUSTOMIZE")
	if tool == "" {
		t.Skip("set RELEASE_TEST_KUSTOMIZE to run the real release renderer")
	}
	binary, err := exec.LookPath(tool)
	require.NoError(t, err, "an explicitly supplied renderer must exist")
	binary, err = filepath.Abs(binary)
	require.NoError(t, err)
	root := t.TempDir()
	copyPublicSources(t, filepath.Join(repository(t), "config"), filepath.Join(root, "config"))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "hack"), 0700))
	script, err := os.ReadFile(filepath.Join(repository(t), "hack", "release-manifests.sh"))
	require.NoError(t, err)
	scriptPath := filepath.Join(root, "hack", "release-manifests.sh")
	require.NoError(t, os.WriteFile(scriptPath, script, 0700))
	// Synthetic ambient files exercise source selection; no real credentials enter the test.
	const excluded = "NOT_A_REAL_CREDENTIAL_RELEASE_SOURCE_SELECTION_SENTINEL"
	for _, name := range []string{"kubeconfig", "talosconfig", "runtime-snapshot.yaml", "test-output.yaml"} {
		require.NoError(t, os.WriteFile(filepath.Join(root, name), []byte(excluded), 0600))
	}
	t.Setenv("KUBECONFIG", filepath.Join(root, "kubeconfig"))
	t.Setenv("TALOSCONFIG", filepath.Join(root, "talosconfig"))
	t.Setenv("KUSTOMIZE", binary)

	const tag = "v0.9.0-craci.1"
	const image = "ghcr.io/polychoronix/cluster-api-talos-controller:" + tag
	artifacts := filepath.Join(t.TempDir(), "artifacts")
	command := exec.Command("sh", scriptPath, tag, image, artifacts)
	output, err := command.CombinedOutput()
	require.NoError(t, err, "real kustomize render: %s", output)
	release := filepath.Join(artifacts, "bootstrap-talos", tag)
	entries, err := os.ReadDir(release)
	require.NoError(t, err)
	require.Len(t, entries, 4)
	components, err := os.ReadFile(filepath.Join(release, "bootstrap-components.yaml"))
	require.NoError(t, err)
	metadata, err := os.ReadFile(filepath.Join(release, "metadata.yaml"))
	require.NoError(t, err)
	originalMetadata, err := os.ReadFile(filepath.Join(root, "config", "metadata", "metadata.yaml"))
	require.NoError(t, err)
	require.Equal(t, originalMetadata, metadata)
	for _, asset := range entries {
		content, err := os.ReadFile(filepath.Join(release, asset.Name()))
		require.NoError(t, err)
		require.NotContains(t, string(content), excluded)
	}
	var sha256Lines, sha512Lines strings.Builder
	for _, asset := range []struct {
		name string
		data []byte
	}{{"bootstrap-components.yaml", components}, {"metadata.yaml", metadata}} {
		fmt.Fprintf(&sha256Lines, "%x  %s\n", sha256.Sum256(asset.data), asset.name)
		fmt.Fprintf(&sha512Lines, "%x  %s\n", sha512.Sum512(asset.data), asset.name)
	}
	actual256, err := os.ReadFile(filepath.Join(release, "SHA256SUMS"))
	require.NoError(t, err)
	actual512, err := os.ReadFile(filepath.Join(release, "SHA512SUMS"))
	require.NoError(t, err)
	require.Equal(t, sha256Lines.String(), string(actual256))
	require.Equal(t, sha512Lines.String(), string(actual512))

	decoder := yaml.NewDecoder(bytes.NewReader(components))
	deployment, configCRD := false, false
	for {
		var resource map[string]any
		err := decoder.Decode(&resource)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.NotEqual(t, "Secret", resource["kind"], "release components must not embed credentials")
		if resource["kind"] == "Deployment" {
			deployment = true
			spec := resource["spec"].(map[string]any)
			template := spec["template"].(map[string]any)
			pod := template["spec"].(map[string]any)
			containers := pod["containers"].([]any)
			require.Len(t, containers, 1)
			require.Equal(t, image, containers[0].(map[string]any)["image"])
		}
		if resource["kind"] == "CustomResourceDefinition" && resource["metadata"].(map[string]any)["name"] == "talosconfigs.bootstrap.cluster.x-k8s.io" {
			configCRD = true
			versions := resource["spec"].(map[string]any)["versions"].([]any)
			for _, version := range versions {
				fields := version.(map[string]any)["schema"].(map[string]any)["openAPIV3Schema"].(map[string]any)["properties"].(map[string]any)["spec"].(map[string]any)["properties"].(map[string]any)
				for _, name := range []string{"configSecretRef", "strategicPatchSecretRefs", "machineConfig", "configRevision", "management"} {
					require.Contains(t, fields, name, "both served API schemas belong in the release")
				}
			}
		}
	}
	require.True(t, deployment, "actual controller deployment must be rendered")
	require.True(t, configCRD, "actual complete-input CRD must be rendered")
	originalManager, err := os.ReadFile(filepath.Join(repository(t), "config", "manager", "kustomization.yaml"))
	require.NoError(t, err)
	copiedManager, err := os.ReadFile(filepath.Join(root, "config", "manager", "kustomization.yaml"))
	require.NoError(t, err)
	require.Equal(t, originalManager, copiedManager, "rendering must not mutate public input sources")
}
