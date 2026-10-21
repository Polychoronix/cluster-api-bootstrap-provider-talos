// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package talosclient opens namespace-local Talos identities without filesystem
// or environmental authentication and keeps transport and routing independent.
package talosclient

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"

	machineryclient "github.com/siderolabs/talos/pkg/machinery/client"
	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"go.yaml.in/yaml/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
)

const identityKey = "talosconfig"

// New opens one endpoint using the default named context from the standard
// <cluster>-talosconfig Secret in clusterKey.Namespace. Identity is read-only;
// only its CA and client key pair are used, never its endpoints or other auth.
// The returned client dials lazily, so transport failures are returned by RPCs.
// Every routed RPC must use WithNode and the caller must close the client.
func New(ctx context.Context, reader client.Reader, clusterKey types.NamespacedName, endpoint bootstrapv1.TalosEndpoint) (*machineryclient.Client, error) {
	secretKey := types.NamespacedName{Namespace: clusterKey.Namespace, Name: clusterKey.Name + "-talosconfig"}
	if reader == nil || clusterKey.Name == "" || len(validation.IsDNS1123Subdomain(secretKey.Name)) != 0 || len(validation.IsDNS1123Label(clusterKey.Namespace)) != 0 {
		return nil, errors.New("talos client: identity reference validation failed")
	}
	if errs := bootstrapv1.ValidateConfigInputs(field.NewPath("spec"), bootstrapv1.TalosConfigSpec{TalosEndpoint: &endpoint}); len(errs) != 0 {
		return nil, errors.New("talos client: endpoint validation failed")
	}

	secret := &corev1.Secret{}
	if err := reader.Get(ctx, secretKey, secret); err != nil {
		return nil, identityError(secretKey, "lookup")
	}
	identity, stage := validateIdentity(secret.Data[identityKey])
	if stage != "" {
		return nil, identityError(secretKey, stage)
	}

	talos, err := machineryclient.New(ctx,
		machineryclient.WithEndpoints(endpoint.Address),
		machineryclient.WithCluster(clusterKey.Name),
		machineryclient.WithConfigContext(&clientconfig.Context{}),
		machineryclient.WithTLSConfig(&tls.Config{
			MinVersion:   tls.VersionTLS12,
			RootCAs:      identity.roots,
			Certificates: []tls.Certificate{*identity.certificate},
			ServerName:   endpoint.ServerName,
		}),
		machineryclient.WithGRPCDialOptions(grpc.WithNoProxy()),
	)
	if err != nil {
		return nil, identityError(secretKey, "transport")
	}

	return talos, nil
}

// ValidateIdentity validates the named default context, CA and client key pair
// of an in-memory talosconfig, including the client certificate's trust chain,
// validity and client-auth usage. Returned CA bytes are decoded private input;
// callers must not log them. Errors contain only a safe stage, never a cause.
func ValidateIdentity(data []byte) (ca []byte, err error) {
	identity, stage := validateIdentity(data)
	if stage != "" {
		return nil, fmt.Errorf("talos client: %s failed", stage)
	}

	return identity.ca, nil
}

type validatedIdentity struct {
	ca          []byte
	roots       *x509.CertPool
	certificate *tls.Certificate
}

func validateIdentity(data []byte) (validatedIdentity, string) {
	if len(bytes.TrimSpace(data)) == 0 {
		return validatedIdentity{}, "empty"
	}
	// Use Talos' exact schema, but not ReadFrom's deprecated-target upgrade:
	// transport overrides those targets, and a null context must fail safely
	// rather than dereference nil in that upgrade.
	var config clientconfig.Config
	if err := yaml.NewDecoder(bytes.NewReader(data)).Decode(&config); err != nil {
		return validatedIdentity{}, "parse"
	}
	context := config.Contexts[config.Context]
	if config.Context == "" || context == nil || context.CA == "" || context.Crt == "" || context.Key == "" {
		return validatedIdentity{}, "identity validation"
	}

	ca, err := base64.StdEncoding.DecodeString(context.CA)
	if err != nil {
		return validatedIdentity{}, "CA validation"
	}
	roots := caPool(ca)
	if roots == nil {
		return validatedIdentity{}, "CA validation"
	}
	certificate, err := machineryclient.CertificateFromConfigContext(context)
	if err != nil || certificate == nil || len(certificate.Certificate) == 0 {
		return validatedIdentity{}, "client certificate validation"
	}
	leaf := certificate.Leaf
	if leaf == nil {
		leaf, err = x509.ParseCertificate(certificate.Certificate[0])
		if err != nil {
			return validatedIdentity{}, "client certificate validation"
		}
	}
	var intermediates *x509.CertPool
	if len(certificate.Certificate) > 1 {
		intermediates = x509.NewCertPool()
	}
	for _, der := range certificate.Certificate[1:] {
		intermediate, parseErr := x509.ParseCertificate(der)
		if parseErr != nil {
			return validatedIdentity{}, "client certificate validation"
		}
		intermediates.AddCert(intermediate)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{
		Roots:         roots,
		Intermediates: intermediates,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}); err != nil {
		return validatedIdentity{}, "client certificate validation"
	}

	return validatedIdentity{ca: ca, roots: roots, certificate: certificate}, ""
}

// WithNode targets exactly one node through the endpoint, or clears any stale
// routing metadata for direct mode. Unrelated outgoing metadata is preserved.
func WithNode(ctx context.Context, endpoint bootstrapv1.TalosEndpoint) context.Context {
	if endpoint.Node != "" {
		return machineryclient.WithNode(ctx, endpoint.Node)
	}

	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok || (len(md.Get("node")) == 0 && len(md.Get("nodes")) == 0) {
		return ctx
	}
	md = md.Copy()
	md.Delete("node")
	md.Delete("nodes")

	return metadata.NewOutgoingContext(ctx, md)
}

func caPool(data []byte) *x509.CertPool {
	pool := x509.NewCertPool()
	count := 0
	for len(bytes.TrimSpace(data)) != 0 {
		block, rest := pem.Decode(data)
		if block == nil || block.Type != "CERTIFICATE" {
			return nil
		}
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !certificate.IsCA {
			return nil
		}
		pool.AddCert(certificate)
		count++
		data = rest
	}
	if count == 0 {
		return nil
	}

	return pool
}

// Deliberately do not retain or unwrap causes: YAML, PEM, API and transport
// errors can carry private input. Only validated reference names and stages
// belong in controller conditions, events or logs.
func identityError(key types.NamespacedName, stage string) error {
	return fmt.Errorf("talos client: Secret %s/%s key %s: %s failed", key.Namespace, key.Name, identityKey, stage)
}
