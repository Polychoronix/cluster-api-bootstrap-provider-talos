// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v1beta1

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	schemacel "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel"
	schemavalidation "k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/yaml"
)

// Exercise the checked-in schemas used by the API server, including their CEL
// rules, without installing CRDs or depending on admission webhook availability.
func TestConfigInputServedSchemas(t *testing.T) {
	t.Parallel()
	for _, filename := range []string{
		"bootstrap.cluster.x-k8s.io_talosconfigs.yaml",
		"bootstrap.cluster.x-k8s.io_talosconfigtemplates.yaml",
	} {
		t.Run(filename, func(t *testing.T) {
			data, err := os.ReadFile("../../config/crd/bases/" + filename)
			require.NoError(t, err)
			var crd apiextensionsv1.CustomResourceDefinition
			require.NoError(t, yaml.Unmarshal(data, &crd))
			require.Len(t, crd.Spec.Versions, 2)
			for _, version := range crd.Spec.Versions {
				t.Run(version.Name, func(t *testing.T) {
					require.True(t, version.Served)
					spec := version.Schema.OpenAPIV3Schema.Properties["spec"]
					if crd.Spec.Names.Kind == "TalosConfigTemplate" {
						spec = spec.Properties["template"].Properties["spec"]
					}
					for _, scope := range []apiextensionsv1.JSONSchemaProps{spec, spec.Properties["machineConfig"]} {
						for _, name := range []string{"configPatches", "strategicPatches", "strategicPatchSecretRefs"} {
							property, ok := scope.Properties[name]
							require.True(t, ok, name)
							require.NotNil(t, property.XListType, name)
							assert.Equal(t, "atomic", *property.XListType, name)
						}
					}
					var internal apiextensions.JSONSchemaProps
					require.NoError(t, apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(&spec, &internal, nil))
					validator, _, err := schemavalidation.NewSchemaValidator(&internal)
					require.NoError(t, err)
					structural, err := structuralschema.NewStructural(&internal)
					require.NoError(t, err)
					celValidator := schemacel.NewValidator(structural, false, 1000000)
					require.NotNil(t, celValidator)
					validate := func(input map[string]interface{}) field.ErrorList {
						errs := schemavalidation.ValidateCustomResource(field.NewPath("spec"), input, validator)
						celErrs, _ := celValidator.Validate(context.Background(), field.NewPath("spec"), structural, input, nil, 10000000)
						return append(errs, celErrs...)
					}
					ref := func(name, key string) map[string]interface{} {
						return map[string]interface{}{"name": name, "key": key}
					}
					for _, role := range []string{"none", "controlplane", "worker"} {
						require.Empty(t, validate(map[string]interface{}{
							"generateType":             role,
							"configSecretRef":          ref("shared-base", "config.yaml"),
							"configRevision":           "shared-2",
							"strategicPatchSecretRefs": []interface{}{ref("first", "patch"), ref("second", "patch")},
							"machineConfig": map[string]interface{}{
								"configSecretRef":          ref("machine-base", "Config_v1.yaml"),
								"configRevision":           "machine-3",
								"strategicPatches":         []interface{}{"first", "second"},
								"strategicPatchSecretRefs": []interface{}{ref("machine-first", "patch"), ref("machine-second", "patch")},
							},
							"talosEndpoint": map[string]interface{}{"address": "[2001:db8::1]:50000", "serverName": "talos.example.com", "node": "2001:db8::2"},
						}))
					}
					require.Empty(t, validate(map[string]interface{}{"generateType": "none"}))
					for _, badRef := range []map[string]interface{}{
						ref("", "key"), ref("BadName", "key"), ref("other/secret", "key"), ref("{{name}}", "key"),
						ref("secret", ""), ref("secret", "private/key"), ref("secret", "."), ref("secret", "..private"),
						{"name": "secret"}, {"key": "config"},
					} {
						for _, input := range []map[string]interface{}{
							{"generateType": "worker", "configSecretRef": badRef},
							{"generateType": "worker", "strategicPatchSecretRefs": []interface{}{badRef}},
							{"generateType": "worker", "machineConfig": map[string]interface{}{"configSecretRef": badRef}},
							{"generateType": "worker", "machineConfig": map[string]interface{}{"strategicPatchSecretRefs": []interface{}{badRef}}},
						} {
							require.NotEmpty(t, validate(input), "invalid reference admitted: %v", input)
						}
					}
					for _, machine := range []bool{false, true} {
						input := map[string]interface{}{"generateType": "worker", "data": "public baseline"}
						if machine {
							input["machineConfig"] = map[string]interface{}{"configSecretRef": ref("base", "config")}
						} else {
							input["configSecretRef"] = ref("base", "config")
						}
						require.NotEmpty(t, validate(input), "data and Secret baseline must be mutually exclusive")
						delete(input, "data")
						input["generateType"] = "init"
						require.NotEmpty(t, validate(input), "init must reject a Secret baseline")
					}
					for _, endpoint := range []map[string]interface{}{
						{"address": "host:50000"}, {"serverName": "host"},
						{"address": "", "serverName": "host"}, {"address": "host:50000", "serverName": ""},
					} {
						require.NotEmpty(t, validate(map[string]interface{}{"generateType": "worker", "talosEndpoint": endpoint}))
					}
				})
			}
		})
	}
}
