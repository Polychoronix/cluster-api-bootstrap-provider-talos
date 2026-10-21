// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/go-logr/logr"
	"github.com/siderolabs/crypto/x509"
	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	talosmachine "github.com/siderolabs/talos/pkg/machinery/config/machine"
	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	capiv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/collections"
	"sigs.k8s.io/controller-runtime/pkg/client"

	bootstrapv1beta1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
)

func (r *TalosConfigReconciler) fetchSecret(ctx context.Context, config *bootstrapv1beta1.TalosConfig, secretName string) (*corev1.Secret, error) {
	retSecret := &corev1.Secret{}
	err := r.Client.Get(ctx, client.ObjectKey{
		Namespace: config.GetNamespace(),
		Name:      secretName,
	}, retSecret)

	if err != nil {
		return nil, err
	}

	return retSecret, nil
}

// getSecretsBundle either generates or loads existing secret.
func (r *TalosConfigReconciler) getSecretsBundle(ctx context.Context, scope *TalosConfigScope, allowGenerate bool, versionContract *config.VersionContract) (*secrets.Bundle, error) {
	var secretsBundle *secrets.Bundle

	secretName := scope.Cluster.Name + "-talos"

retry:
	secret, err := r.fetchSecret(ctx, scope.Config, secretName)

	switch {
	case err != nil && k8serrors.IsNotFound(err):
		if !allowGenerate {
			return nil, fmt.Errorf("secrets bundle is missing")
		}

		// no cluster secret yet, generate new one
		secretsBundle, err = secrets.NewBundle(secrets.NewFixedClock(time.Now()), versionContract)
		if err != nil {
			return nil, identityError("bundle generation")
		}

		if err = r.writeSecretsBundleSecret(ctx, scope, secretName, secretsBundle); err != nil {
			if k8serrors.IsAlreadyExists(err) {
				// conflict on creation, retry loading
				goto retry
			}

			return nil, identityError("bundle write")
		}
	case err != nil:
		return nil, fmt.Errorf("error reading secrets bundle: %w", err)
	default:
		return configinput.DecodeGenerationSecret(secret, versionContract)
	}

	return secretsBundle, nil
}

func (r *TalosConfigReconciler) writeSecretsBundleSecret(ctx context.Context, scope *TalosConfigScope, secretName string, secretsBundle *secrets.Bundle) error {
	bundle, err := yaml.Marshal(secretsBundle)
	if err != nil {
		return fmt.Errorf("error marshaling secrets bundle: %w", err)
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: scope.Config.Namespace,
			Name:      secretName,
			Labels: map[string]string{
				capiv1.ClusterNameLabel: scope.Cluster.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(scope.Cluster, capiv1.GroupVersion.WithKind("Cluster")),
			},
		},
		Data: map[string][]byte{
			"bundle": bundle,
		},
	}

	return r.Client.Create(ctx, secret)
}

func (r *TalosConfigReconciler) writeK8sCASecret(ctx context.Context, scope *TalosConfigScope, certs *x509.PEMEncodedCertificateAndKey) error {
	// Create ca secret only if it doesn't already exist
	existing, err := r.fetchSecret(ctx, scope.Config, scope.Cluster.Name+"-ca")
	if err == nil {
		if !bytes.Equal(existing.Data["tls.crt"], certs.Crt) || !bytes.Equal(existing.Data["tls.key"], certs.Key) {
			return identityError("Kubernetes CA mismatch")
		}
		return nil
	}

	if !k8serrors.IsNotFound(err) {
		return err
	}

	certSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: scope.Config.Namespace,
			Name:      scope.Cluster.Name + "-ca",
			Labels: map[string]string{
				capiv1.ClusterNameLabel: scope.Cluster.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(scope.Cluster, capiv1.GroupVersion.WithKind("Cluster")),
			},
		},
		Data: map[string][]byte{
			"tls.crt": certs.Crt,
			"tls.key": certs.Key,
		},
	}

	err = r.Client.Create(ctx, certSecret)
	if k8serrors.IsAlreadyExists(err) {
		existing, err = r.fetchSecret(ctx, scope.Config, scope.Cluster.Name+"-ca")
		if err != nil {
			return identityError("Kubernetes CA winning-create lookup")
		}
		if !bytes.Equal(existing.Data["tls.crt"], certs.Crt) || !bytes.Equal(existing.Data["tls.key"], certs.Key) {
			return identityError("Kubernetes CA winning-create mismatch")
		}
	} else if err != nil {
		return identityError("Kubernetes CA create")
	}

	return nil
}

// rewritesBootstrapData reports whether an existing bootstrap data secret may be replaced with
// freshly rendered configuration.
//
// Two cases qualify. A Cluster API in-place update, where the owning controller has written a
// new desired spec and the node needs the regenerated configuration. And a MachinePool, whose
// single secret is re-read by the infrastructure provider every time it creates an instance, so
// freezing it would strand the pool on the configuration it was created with — Cluster API
// offers no in-place update flow for pools, so nothing else would ever rewrite it.
func (r *TalosConfigReconciler) rewritesBootstrapData(scope *TalosConfigScope) bool {
	if bootstrapv1beta1.IsInPlaceUpdate(scope.Config) {
		return true
	}

	return r.MachinePoolInPlaceUpdates && scope.ConfigOwner.IsMachinePool()
}

// writeBootstrapData creates a new secret with the data passed in as input
// writeBootstrapData persists the rendered machine configuration for a Machine.
//
// Bootstrap data is immutable for the life of a Machine under normal circumstances, so an
// existing secret is left untouched. The exceptions are listed on rewritesBootstrapData; there
// the secret is rewritten in place, keeping its name so the owner's bootstrap reference stays
// valid.
//
// configHash is recorded on the secret so the in-place update extension, and the MachinePool
// update loop, can distinguish a regenerated secret from one that still holds the pre-update
// configuration.
func (r *TalosConfigReconciler) writeBootstrapData(ctx context.Context, scope *TalosConfigScope, data []byte, configHash string, privateFreshness []byte) (string, error) {
	if len(privateFreshness) == 0 {
		return "", fmt.Errorf("configuration input generated Secret: freshness required")
	}
	ownerName := scope.ConfigOwner.GetName()
	dataSecretName := ownerName + "-bootstrap-data"
	if original := scope.ConfigOwner.DataSecretName(); original != nil && *original == dataSecretName {
		seed, err := r.fetchSecret(ctx, scope.Config, *original)
		if err != nil {
			return "", fmt.Errorf("configuration input bootstrap provenance: lookup failed")
		}
		providerOwned := false
		for _, ref := range seed.OwnerReferences {
			if ref.Kind == "TalosConfig" && ref.UID == scope.Config.UID {
				providerOwned = true
			}
		}
		if !providerOwned {
			dataSecretName = scope.Config.Name + "-update-data"
		}
	}

	r.Log.Info("handling bootstrap data for ", "owner", ownerName)

	existing, err := r.fetchSecret(ctx, scope.Config, dataSecretName)
	if err == nil {
		if !scope.ConfigOwner.IsMachinePool() {
			owned := false
			for _, ref := range existing.OwnerReferences {
				if ref.Kind == "TalosConfig" && ref.UID == scope.Config.UID {
					owned = true
				}
			}
			if !owned {
				return "", fmt.Errorf("configuration input generated Secret: ownership mismatch")
			}
		}
		if !r.rewritesBootstrapData(scope) {
			return dataSecretName, nil
		}

		if bytes.Equal(existing.Data[configinput.FreshnessKey], privateFreshness) {
			return dataSecretName, nil
		}

		r.Log.Info("regenerating bootstrap data", "owner", ownerName)

		patched := existing.DeepCopy()
		if patched.Annotations == nil {
			patched.Annotations = map[string]string{}
		}

		if scope.ConfigOwner.IsMachinePool() {
			patched.Annotations[bootstrapv1beta1.InPlaceConfigHashAnnotation] = configHash
		} else {
			delete(patched.Annotations, bootstrapv1beta1.InPlaceConfigHashAnnotation)
		}
		patched.Data = map[string][]byte{"value": data}
		patched.Data[configinput.FreshnessKey] = privateFreshness

		return dataSecretName, r.Client.Patch(ctx, patched, client.MergeFrom(existing))
	}

	if !k8serrors.IsNotFound(err) {
		return dataSecretName, err
	}

	certSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: scope.Config.Namespace,
			Name:      dataSecretName,
			Labels: map[string]string{
				capiv1.ClusterNameLabel: scope.Cluster.Name,
			},
			Annotations: map[string]string{},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(scope.Config, bootstrapv1beta1.GroupVersion.WithKind("TalosConfig")),
			},
		},
		Data: map[string][]byte{
			"value": data,
		},
	}
	certSecret.Data[configinput.FreshnessKey] = privateFreshness
	if scope.ConfigOwner.IsMachinePool() {
		certSecret.Annotations[bootstrapv1beta1.InPlaceConfigHashAnnotation] = configHash
	}

	err = r.Client.Create(ctx, certSecret)

	return dataSecretName, err
}

// reconcileClientConfig creates/updates a TalosConfig for the cluster.
func (r *TalosConfigReconciler) reconcileClientConfig(ctx context.Context, log logr.Logger, scope *TalosConfigScope) error {
	if !(scope.Config.Spec.GenerateType == talosmachine.TypeControlPlane.String() || scope.Config.Spec.GenerateType == talosmachine.TypeInit.String()) {
		// can only reconcile for control plane machines
		return nil
	}
	identity, identityErr := r.fetchSecret(ctx, scope.Config, scope.Cluster.Name+"-talosconfig")
	if identityErr == nil {
		ca, err := identityCA(identity)
		if err != nil {
			return err
		}
		if !providerCreatedIdentity(identity, scope) {
			return nil
		}
		if scope.Config.Spec.ConfigSecretRef != nil || scope.Config.Spec.GenerateType == "none" || scope.Config.Spec.MachineConfig != nil && scope.Config.Spec.MachineConfig.ConfigSecretRef != nil {
			return nil
		}
		bundle, err := r.getSecretsBundle(ctx, scope, false, defaultVersionContract)
		if err != nil {
			return identityError("generated bundle lookup")
		}
		if bundle.Certs == nil || bundle.Certs.OS == nil || !bytes.Equal(ca, bundle.Certs.OS.Crt) {
			return identityError("generated bundle mismatch")
		}
	} else if !k8serrors.IsNotFound(identityErr) {
		return identityError("lookup")
	}

	machines, err := collections.GetFilteredMachinesForCluster(ctx, r.Client, scope.Cluster, collections.ControlPlaneMachines(scope.Cluster.Name))
	if err != nil {
		return fmt.Errorf("failed getting control plane machines: %w", err)
	}

	var endpoints []string

	for _, machine := range machines {
		for _, addr := range machine.Status.Addresses {
			if addr.Type == capiv1.MachineExternalIP || addr.Type == capiv1.MachineInternalIP {
				endpoints = append(endpoints, addr.Address)
			}
		}
	}

	sort.Strings(endpoints)

	secretBundle, err := r.getSecretsBundle(ctx, scope, false, defaultVersionContract) // version contract doesn't matter, as we're getting the secrets
	if err != nil {
		return err
	}

	talosConfig, err := genTalosConfigFile(scope.Cluster.Name, secretBundle, endpoints)
	if err != nil {
		return err
	}

	// Create or update talosconfig secret
	dataSecretName := scope.Cluster.GetName() + "-talosconfig"

	log.Info("updating talosconfig", "endpoints", endpoints, "secret", dataSecretName)

	if k8serrors.IsNotFound(identityErr) {
		configSecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: scope.Cluster.Namespace,
				Name:      dataSecretName,
				Labels: map[string]string{
					capiv1.ClusterNameLabel: scope.Cluster.Name,
				},
				Annotations: map[string]string{identityCreatedAnnotation: identityCreatedValue},
				OwnerReferences: []metav1.OwnerReference{
					*metav1.NewControllerRef(scope.Cluster, capiv1.GroupVersion.WithKind("Cluster")),
				},
			},
			Data: map[string][]byte{
				"talosconfig": []byte(talosConfig),
			},
		}

		if err := r.Client.Create(ctx, configSecret); err != nil {
			return identityError("create conflict; revalidation required")
		}
		return nil
	}
	return patchIdentitySnapshot(ctx, r.Client, identity, []byte(talosConfig))
}
