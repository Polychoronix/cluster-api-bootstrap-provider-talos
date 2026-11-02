// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v1beta1

import (
	"net"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// ValidateConfigInputs checks public references without reading or echoing private data.
func ValidateConfigInputs(path *field.Path, spec TalosConfigSpec) field.ErrorList {
	var errs field.ErrorList
	validateRef := func(p *field.Path, ref *SecretKeyReference) {
		if ref == nil {
			return
		}
		if len(validation.IsDNS1123Subdomain(ref.Name)) != 0 {
			errs = append(errs, field.Invalid(p.Child("name"), "", "must be a namespace-local Secret name"))
		}
		if ref.Key == "" || len(validation.IsConfigMapKey(ref.Key)) != 0 {
			errs = append(errs, field.Invalid(p.Child("key"), "", "must be a nonempty Secret data key"))
		}
	}
	validateRefs := func(p *field.Path, refs []SecretKeyReference) {
		for i := range refs {
			validateRef(p.Index(i), &refs[i])
		}
	}
	validateRef(path.Child("configSecretRef"), spec.ConfigSecretRef)
	validateRefs(path.Child("strategicPatchSecretRefs"), spec.StrategicPatchSecretRefs)
	baseline := spec.ConfigSecretRef
	if overlay := spec.MachineConfig; overlay != nil {
		validateRef(path.Child("machineConfig", "configSecretRef"), overlay.ConfigSecretRef)
		validateRefs(path.Child("machineConfig", "strategicPatchSecretRefs"), overlay.StrategicPatchSecretRefs)
		if overlay.ConfigSecretRef != nil {
			baseline = overlay.ConfigSecretRef
		}
	}
	if baseline != nil {
		if spec.Data != "" {
			errs = append(errs, field.Forbidden(path.Child("data"), "cannot be combined with a Secret baseline"))
		}
		switch spec.GenerateType {
		case "none", "controlplane", "worker":
		default:
			errs = append(errs, field.Invalid(path.Child("generateType"), "", "Secret baselines require none, controlplane or worker"))
		}
	}
	if endpoint := spec.TalosEndpoint; endpoint != nil {
		host, port, err := net.SplitHostPort(endpoint.Address)
		n, portErr := strconv.Atoi(port)
		if err != nil || host == "" || portErr != nil || n < 1 || n > 65535 || strings.ContainsAny(host, "/\\ \t\r\n") {
			errs = append(errs, field.Invalid(path.Child("talosEndpoint", "address"), "", "must be a host:port transport address"))
		}
		if endpoint.ServerName == "" || strings.ContainsAny(endpoint.ServerName, "/\\: \t\r\n") {
			// IPv6 certificate names are valid IP addresses, not host:port values.
			if net.ParseIP(endpoint.ServerName) == nil {
				errs = append(errs, field.Invalid(path.Child("talosEndpoint", "serverName"), "", "must be a TLS host name or IP without a port"))
			}
		}
		if strings.ContainsAny(endpoint.Node, ",/\\ \t\r\n") {
			errs = append(errs, field.Invalid(path.Child("talosEndpoint", "node"), "", "must identify a single target node"))
		}
	}
	return errs
}
