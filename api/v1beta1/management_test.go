// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v1beta1

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	admissionv1 "k8s.io/api/admission/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

func managementSpec(size int) *ManagementSpec {
	spec := &ManagementSpec{Mode: ManagementModeObserve, InventoryRevision: "inventory-1"}
	for i := range size {
		spec.Machines = append(spec.Machines, MachineIdentityReference{Name: fmt.Sprintf("machine-%d", i), UID: types.UID(fmt.Sprintf("uid-%d", i))})
	}
	return spec
}

func TestValidateManagementSpec(t *testing.T) {
	path := field.NewPath("spec", "management")
	require.Empty(t, ValidateManagementSpec(path, nil))
	for _, size := range []int{1, 5} {
		require.Empty(t, ValidateManagementSpec(path, managementSpec(size)))
	}
	for name, mutate := range map[string]func(*ManagementSpec){
		"mode":            func(s *ManagementSpec) { s.Mode = "Other" },
		"revision":        func(s *ManagementSpec) { s.InventoryRevision = "private/config\n" },
		"empty inventory": func(s *ManagementSpec) { s.Machines = nil },
		"name":            func(s *ManagementSpec) { s.Machines[0].Name = "other/machine" },
		"uid":             func(s *ManagementSpec) { s.Machines[0].UID = "" },
		"unsafe uid":      func(s *ManagementSpec) { s.Machines[0].UID = "private/uid" },
		"duplicate name": func(s *ManagementSpec) {
			s.Machines = append(s.Machines, MachineIdentityReference{Name: s.Machines[0].Name, UID: "uid-other"})
		},
		"duplicate uid": func(s *ManagementSpec) {
			s.Machines = append(s.Machines, MachineIdentityReference{Name: "other", UID: s.Machines[0].UID})
		},
		"missing approval": func(s *ManagementSpec) { s.Mode = ManagementModeManaged },
		"mismatched approval": func(s *ManagementSpec) {
			s.Mode = ManagementModeManaged
			s.Approval = &ObservationApproval{Revision: "inventory-2", ObservationID: "observation-1"}
		},
		"empty observation": func(s *ManagementSpec) { s.Approval = &ObservationApproval{Revision: s.InventoryRevision} },
		"unsafe approval": func(s *ManagementSpec) {
			s.Approval = &ObservationApproval{Revision: "private/revision", ObservationID: "private/observation"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			spec := managementSpec(1)
			mutate(spec)
			errs := ValidateManagementSpec(path, spec)
			require.NotEmpty(t, errs)
			assert.NotContains(t, errs.ToAggregate().Error(), "private/")
		})
	}
	managed := managementSpec(1)
	managed.Mode = ManagementModeManaged
	managed.Approval = &ObservationApproval{Revision: managed.InventoryRevision, ObservationID: "observation-1"}
	require.Empty(t, ValidateManagementSpec(path, managed))
	require.Empty(t, ValidateConfigInputs(field.NewPath("spec"), TalosConfigSpec{Management: managed}))
	require.NotEmpty(t, ValidateConfigInputs(field.NewPath("spec"), TalosConfigSpec{Management: managementSpec(5)}))
}

func TestManagementDefaultAndTemplateRejection(t *testing.T) {
	config := &TalosConfig{}
	require.NoError(t, config.Default(context.Background(), config))
	assert.Nil(t, config.Spec.Management)
	config.Spec.Management = managementSpec(1)
	config.Spec.Management.Mode = ""
	require.NoError(t, config.Default(context.Background(), config))
	assert.Equal(t, ManagementModeObserve, config.Spec.Management.Mode)
	config.Spec.Management.Mode = ManagementModeManaged
	require.NoError(t, config.Default(context.Background(), config))
	assert.Equal(t, ManagementModeManaged, config.Spec.Management.Mode)
	template := &TalosConfigTemplate{}
	template.Spec.Template.Spec.Management = managementSpec(1)
	_, err := template.ValidateCreate(context.Background(), template)
	require.Error(t, err)
}

func TestManagementDeepCopy(t *testing.T) {
	config := &TalosConfig{Spec: TalosConfigSpec{Management: managementSpec(1)}, Status: TalosConfigStatus{Management: &ManagementStatus{Observation: &ObservationStatus{Revision: "inventory-1", ID: "observation-1"}}}}
	config.Spec.Management.Approval = &ObservationApproval{Revision: "inventory-1", ObservationID: "observation-1"}
	copy := config.DeepCopy()
	copy.Spec.Management.Machines[0].UID = "changed"
	copy.Spec.Management.Approval.ObservationID = "changed"
	copy.Status.Management.Observation.ID = "changed"
	assert.Equal(t, types.UID("uid-0"), config.Spec.Management.Machines[0].UID)
	assert.Equal(t, "observation-1", config.Spec.Management.Approval.ObservationID)
	assert.Equal(t, "observation-1", config.Status.Management.Observation.ID)
}

func TestManagementApprovalUpdate(t *testing.T) {
	ctx := admission.NewContextWithRequest(context.Background(), admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{Operation: admissionv1.Update}})
	old := &TalosConfig{Spec: TalosConfigSpec{GenerateType: "none", Management: managementSpec(1)}}
	updated := old.DeepCopy()
	updated.Spec.Management.Mode = ManagementModeManaged
	updated.Spec.Management.Approval = &ObservationApproval{Revision: "inventory-1", ObservationID: "observation-1"}
	_, err := updated.ValidateUpdate(ctx, old, updated)
	require.NoError(t, err)
	updated.Spec.Data = "changed bootstrap input"
	_, err = updated.ValidateUpdate(ctx, old, updated)
	require.Error(t, err)
}
