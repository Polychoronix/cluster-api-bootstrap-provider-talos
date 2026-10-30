// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/siderolabs/talos/pkg/machinery/config/configloader"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	endpointclient "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/talosclient"
)

// identityCreatedAnnotation is creation-only provenance, never inferred from an existing Secret.
const identityCreatedAnnotation = "bootstrap.cluster.x-k8s.io/identity-created-by"
const identityCreatedValue = "cabpt"

func identityError(stage string) error {
	return fmt.Errorf("configuration input cluster identity: %s failed", stage)
}

func identityCA(secret *corev1.Secret) ([]byte, error) {
	return endpointclient.ValidateIdentity(secret.Data["talosconfig"])
}

func providerCreatedIdentity(secret *corev1.Secret, scope *TalosConfigScope) bool {
	if secret.Annotations[identityCreatedAnnotation] != identityCreatedValue {
		return false
	}
	for _, owner := range secret.OwnerReferences {
		if owner.Kind == "Cluster" && owner.UID == scope.Cluster.UID && owner.Controller != nil && *owner.Controller {
			return true
		}
	}
	return false
}

func (r *TalosConfigReconciler) reconcileBaselineIdentity(ctx context.Context, scope *TalosConfigScope, data []byte) error {
	provider, err := configloader.NewFromBytes(data)
	if err != nil || provider.Machine() == nil {
		return identityError("baseline parse")
	}
	if ca := provider.K8sAPIServerCAConfig(); ca != nil && ca.IssuingCA() != nil {
		issuing := ca.IssuingCA()
		if len(issuing.Crt) > 0 && len(issuing.Key) > 0 {
			if _, err := tls.X509KeyPair(issuing.Crt, issuing.Key); err != nil {
				return identityError("Kubernetes CA")
			}
			existing, err := r.fetchSecret(ctx, scope.Config, scope.Cluster.Name+"-ca")
			if err == nil {
				if !bytes.Equal(existing.Data["tls.crt"], issuing.Crt) || !bytes.Equal(existing.Data["tls.key"], issuing.Key) {
					return identityError("Kubernetes CA mismatch")
				}
			} else if !apierrors.IsNotFound(err) {
				return identityError("Kubernetes CA lookup")
			} else if err := r.writeK8sCASecret(ctx, scope, issuing); err != nil {
				return identityError("Kubernetes CA extraction")
			}
		}
	}
	identity, err := r.fetchSecret(ctx, scope.Config, scope.Cluster.Name+"-talosconfig")
	if err == nil {
		ca, err := identityCA(identity)
		if err != nil {
			return err
		}
		issuing := provider.Machine().Security().IssuingCA()
		if issuing != nil && len(issuing.Crt) > 0 && !bytes.Equal(ca, issuing.Crt) {
			return identityError("Talos CA mismatch")
		}
		if issuing == nil || len(issuing.Crt) == 0 {
			matched := false
			for _, accepted := range provider.Machine().Security().AcceptedCAs() {
				if accepted != nil && bytes.Equal(ca, accepted.Crt) {
					matched = true
				}
			}
			if !matched {
				return identityError("Talos accepted CA mismatch")
			}
		}
		return nil // externally supplied credentials and endpoints remain byte-for-byte untouched
	}
	if !apierrors.IsNotFound(err) {
		return identityError("lookup")
	}
	issuing := provider.Machine().Security().IssuingCA()
	if issuing == nil || len(issuing.Crt) == 0 || len(issuing.Key) == 0 {
		return identityError("imported management Secret required")
	}
	bundle, err := secrets.NewBundleFromConfig(secrets.NewFixedClock(time.Now()), provider)
	if err != nil {
		return identityError("baseline extraction")
	}
	value, err := genTalosConfigFile(scope.Cluster.Name, bundle, nil)
	if err != nil {
		return identityError("client certificate generation")
	}
	identity = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Namespace: scope.Config.Namespace, Name: scope.Cluster.Name + "-talosconfig",
		Labels:          map[string]string{clusterv1.ClusterNameLabel: scope.Cluster.Name},
		Annotations:     map[string]string{identityCreatedAnnotation: identityCreatedValue},
		OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(scope.Cluster, clusterv1.GroupVersion.WithKind("Cluster"))},
	}, Data: map[string][]byte{"talosconfig": []byte(value)}}
	if err := r.Client.Create(ctx, identity); err != nil {
		return identityError("create")
	}
	return nil
}
