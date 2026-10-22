// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"encoding/json"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/talos/pkg/machinery/config/configloader"
	"sort"
	"strings"
)

// ConfigurationImage reads desired image intent, not a booted OS observation.
func ConfigurationImage(data []byte, spec bootstrapv1.TalosConfigSpec) (ImageIdentity, error) {
	p, e := configloader.NewFromBytes(data)
	if e != nil {
		return ImageIdentity{}, safe("image intent parse")
	}
	ref := ""
	if d := p.UnattendedInstallConfig(); d != nil {
		ref = d.InstallerImage()
	} else if p.Machine() != nil && p.Machine().Install() != nil {
		ref = p.Machine().Install().Image()
	}
	i := ImageIdentity{Reference: ref}
	if at := strings.IndexByte(ref, '@'); at >= 0 {
		i.Digest = ref[at+1:]
	}
	if n := strings.Index(ref, "/metal-installer/"); n >= 0 {
		part := strings.SplitN(ref[n+len("/metal-installer/"):], ":", 2)[0]
		i.SchematicID = strings.SplitN(part, "@", 2)[0]
	}
	if spec.ImageFactory != nil {
		i.Extensions = append([]string(nil), spec.ImageFactory.Extensions...)
		sort.Strings(i.Extensions)
	}
	return i, nil
}
func EncodeAcceptedIntent(intent AcceptedIntent) ([]byte, error) {
	if intent.MachineUID == "" || intent.TalosConfigUID == "" {
		return nil, safe("accepted intent identity")
	}
	return json.Marshal(intent)
}
func DecodeAcceptedIntent(data []byte) (*AcceptedIntent, error) {
	i := &AcceptedIntent{}
	if json.Unmarshal(data, i) != nil || i.MachineUID == "" || i.TalosConfigUID == "" {
		return nil, safe("accepted image intent unavailable")
	}
	return i, nil
}
