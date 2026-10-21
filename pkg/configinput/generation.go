// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package configinput

import (
	"bytes"
	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	"time"
)

// DecodeGenerationSecret validates the standard bundle without exposing parser or credential bytes.
func DecodeGenerationSecret(secret *corev1.Secret, contract *config.VersionContract) (*secrets.Bundle, error) {
	if secret == nil {
		return nil, failure("generation identity", "missing")
	}
	ref := secret.Namespace + "/" + secret.Name
	b := &secrets.Bundle{Clock: secrets.NewFixedClock(time.Now())}
	decode := func(key string, target any) error {
		data, ok := secret.Data[key]
		if !ok || len(bytes.TrimSpace(data)) == 0 {
			return failure(ref+"/"+key, "missing or empty")
		}
		if yaml.Unmarshal(data, target) != nil {
			return failure(ref+"/"+key, "parse")
		}
		return nil
	}
	if _, ok := secret.Data["bundle"]; ok {
		if err := decode("bundle", b); err != nil {
			return nil, err
		}
	} else {
		if err := decode("certs", &b.Certs); err != nil {
			return nil, err
		}
		if err := decode("kubeSecrets", &b.Secrets); err != nil {
			return nil, err
		}
		if err := decode("trustdInfo", &b.TrustdInfo); err != nil {
			return nil, err
		}
		b.Cluster = &secrets.Cluster{}
	}
	if err := b.Validate(contract); err != nil {
		return nil, failure(ref+"/bundle", "validation")
	}
	return b, nil
}
