// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package configinput

import (
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	"time"
)

func generationBundle(secret *corev1.Secret) (*secrets.Bundle, error) {
	b := &secrets.Bundle{Clock: secrets.NewFixedClock(time.Now())}
	if data, ok := secret.Data["bundle"]; ok {
		if yaml.Unmarshal(data, b) != nil {
			return nil, failure("generation identity", "parse")
		}
	} else {
		if yaml.Unmarshal(secret.Data["certs"], &b.Certs) != nil || yaml.Unmarshal(secret.Data["kubeSecrets"], &b.Secrets) != nil || yaml.Unmarshal(secret.Data["trustdInfo"], &b.TrustdInfo) != nil {
			return nil, failure("generation identity", "parse")
		}
		b.Cluster = &secrets.Cluster{}
	}
	return b, nil
}
