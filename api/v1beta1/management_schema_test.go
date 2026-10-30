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

func TestManagementServedSchemas(t *testing.T) {
	for _, kind := range []string{"talosconfigs", "talosconfigtemplates"} {
		data, err := os.ReadFile("../../config/crd/bases/bootstrap.cluster.x-k8s.io_" + kind + ".yaml")
		require.NoError(t, err)
		var crd apiextensionsv1.CustomResourceDefinition
		require.NoError(t, yaml.Unmarshal(data, &crd))
		for _, version := range crd.Spec.Versions {
			t.Run(kind+"/"+version.Name, func(t *testing.T) {
				require.True(t, version.Served)
				spec := version.Schema.OpenAPIV3Schema.Properties["spec"]
				management := spec.Properties["management"]
				if kind == "talosconfigtemplates" {
					management = spec.Properties["template"].Properties["spec"].Properties["management"]
				}
				require.NotNil(t, management.Properties["mode"].Default)
				assert.JSONEq(t, `"Observe"`, string(management.Properties["mode"].Default.Raw))
				if kind == "talosconfigs" {
					status := version.Schema.OpenAPIV3Schema.Properties["status"]
					assert.Contains(t, status.Properties, "management")
					assert.Contains(t, status.Properties, "lastHandledReconcileAt")
				}
				var internal apiextensions.JSONSchemaProps
				require.NoError(t, apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(&spec, &internal, nil))
				validator, _, err := schemavalidation.NewSchemaValidator(&internal)
				require.NoError(t, err)
				structural, err := structuralschema.NewStructural(&internal)
				require.NoError(t, err)
				celValidator := schemacel.NewValidator(structural, false, 1000000)
				validate := func(m map[string]interface{}) field.ErrorList {
					input := map[string]interface{}{"generateType": "none"}
					if m != nil {
						input["management"] = m
					}
					if kind == "talosconfigtemplates" {
						input = map[string]interface{}{"template": map[string]interface{}{"spec": input}}
					}
					errs := schemavalidation.ValidateCustomResource(field.NewPath("spec"), input, validator)
					celErrs, _ := celValidator.Validate(context.Background(), field.NewPath("spec"), structural, input, nil, 10000000)
					return append(errs, celErrs...)
				}
				require.Empty(t, validate(nil))
				fresh := func() map[string]interface{} {
					return map[string]interface{}{"mode": "Observe", "inventoryRevision": "inventory-1", "machines": []interface{}{map[string]interface{}{"name": "machine", "uid": "uid-1"}}}
				}
				if kind == "talosconfigtemplates" {
					require.NotEmpty(t, validate(fresh()))
					return
				}
				require.Empty(t, validate(fresh()))
				managed := fresh()
				managed["mode"] = "Managed"
				require.NotEmpty(t, validate(managed))
				managed["approval"] = map[string]interface{}{"revision": "inventory-2", "observationID": "observation-1"}
				require.NotEmpty(t, validate(managed))
				managed["approval"] = map[string]interface{}{"revision": "inventory-1", "observationID": "observation-1"}
				require.Empty(t, validate(managed))
				for _, mutate := range []func(map[string]interface{}){
					func(m map[string]interface{}) { m["inventoryRevision"] = "private/revision" },
					func(m map[string]interface{}) { m["machines"] = []interface{}{} },
					func(m map[string]interface{}) {
						m["machines"] = []interface{}{map[string]interface{}{"name": "other/machine", "uid": "uid-1"}}
					},
					func(m map[string]interface{}) {
						m["machines"] = []interface{}{map[string]interface{}{"name": "machine", "uid": ""}}
					},
					func(m map[string]interface{}) {
						m["machines"] = []interface{}{map[string]interface{}{"name": "machine", "uid": "uid-1"}, map[string]interface{}{"name": "other", "uid": "uid-2"}}
					},
				} {
					m := fresh()
					mutate(m)
					require.NotEmpty(t, validate(m))
				}
			})
		}
	}
}
