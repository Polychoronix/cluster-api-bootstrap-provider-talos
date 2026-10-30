// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v1beta1

import (
	"regexp"

	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

var managementToken = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]*$`)

// ValidateManagementSpec validates public inventory identities for any owner cardinality.
// Errors intentionally omit submitted values, which must never carry private data.
func ValidateManagementSpec(path *field.Path, spec *ManagementSpec) field.ErrorList {
	if spec == nil {
		return nil
	}
	var errs field.ErrorList
	validateToken := func(p *field.Path, value string) {
		if len(value) > 253 || !managementToken.MatchString(value) {
			errs = append(errs, field.Invalid(p, "", "must be a nonempty public identifier of at most 253 letters, digits, dots, underscores or hyphens, starting with a letter or digit"))
		}
	}
	switch spec.Mode {
	case "", ManagementModeObserve, ManagementModeManaged:
	default:
		errs = append(errs, field.NotSupported(path.Child("mode"), "", []string{"Observe", "Managed"}))
	}
	validateToken(path.Child("inventoryRevision"), spec.InventoryRevision)
	if len(spec.Machines) == 0 || len(spec.Machines) > 1000 {
		errs = append(errs, field.Invalid(path.Child("machines"), "", "must contain between 1 and 1000 Machine identities"))
	}
	names := make(map[string]struct{}, len(spec.Machines))
	uids := make(map[string]struct{}, len(spec.Machines))
	for i, machine := range spec.Machines {
		p := path.Child("machines").Index(i)
		if len(validation.IsDNS1123Subdomain(machine.Name)) != 0 {
			errs = append(errs, field.Invalid(p.Child("name"), "", "must be a namespace-local Machine name"))
		}
		validateToken(p.Child("uid"), string(machine.UID))
		if _, exists := names[machine.Name]; exists {
			errs = append(errs, field.Invalid(p.Child("name"), "", "Machine names must be unique"))
		}
		if _, exists := uids[string(machine.UID)]; exists {
			errs = append(errs, field.Invalid(p.Child("uid"), "", "Machine UIDs must be unique"))
		}
		names[machine.Name] = struct{}{}
		uids[string(machine.UID)] = struct{}{}
	}
	if spec.Approval != nil {
		validateToken(path.Child("approval", "revision"), spec.Approval.Revision)
		validateToken(path.Child("approval", "observationID"), spec.Approval.ObservationID)
	}
	if spec.Mode == ManagementModeManaged {
		if spec.Approval == nil {
			errs = append(errs, field.Required(path.Child("approval"), "Managed requires explicit observation approval"))
		} else if spec.Approval.Revision != spec.InventoryRevision {
			errs = append(errs, field.Invalid(path.Child("approval", "revision"), "", "must match inventoryRevision"))
		}
	}
	return errs
}
