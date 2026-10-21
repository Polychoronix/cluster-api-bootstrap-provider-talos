// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package updateoperation_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
)

func ownerOperationIntent() *updateoperation.OwnerPlanIntent {
	j := operationJournal()
	return &updateoperation.OwnerPlanIntent{Version: updateoperation.OwnerIntentVersion, TransactionID: j.TransactionID,
		Machine: j.Machine, TalosConfig: j.TalosConfig, Infrastructure: j.Infrastructure,
		Inputs:         []configinput.SourceIdentity{{Name: "base", Key: "config", UID: "source-uid", ResourceVersion: "3", Digest: sha256.Sum256([]byte(storePrivateSentinel))}},
		InputFreshness: append([]byte(nil), j.InputFreshness...), Phase: updateoperation.OwnerReleased,
		Owner:        updateoperation.ObjectIdentity{APIGroup: "controlplane.cluster.x-k8s.io", Kind: "TalosControlPlane", Name: "controlplane", UID: "controlplane-uid"},
		ControlPlane: []updateoperation.ObjectIdentity{j.Machine}}
}

func ownerOperationSecret(t *testing.T, intent *updateoperation.OwnerPlanIntent) *corev1.Secret {
	t.Helper()
	return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "owner-plan", UID: "owner-plan-uid", Annotations: map[string]string{updateoperation.ProvenanceAnnotation: updateoperation.OwnerIntentProvenanceValue}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "controlplane.cluster.x-k8s.io/v1beta1", Kind: intent.Owner.Kind, Name: intent.Owner.Name, UID: intent.Owner.UID}}},
		Data: map[string][]byte{updateoperation.OwnerIntentKey: encodedOperation(t, intent)}}
}

func TestOwnerIntentEnvelopeRequiresExactUIDVersionReleaseAndFreshness(t *testing.T) {
	t.Parallel()
	ref := updateoperation.OwnerPlanReference{Name: "owner-plan", UID: "owner-plan-uid"}
	intent := ownerOperationIntent()
	got, err := updateoperation.ReadOwnerIntent(context.Background(), operationClient(t, ownerOperationSecret(t, intent)), "tenant", ref)
	require.NoError(t, err)
	assert.Equal(t, intent, got)
	cases := map[string]func(*corev1.Secret, *updateoperation.OwnerPlanIntent, *updateoperation.OwnerPlanReference){
		"foreign namespace": func(s *corev1.Secret, _ *updateoperation.OwnerPlanIntent, _ *updateoperation.OwnerPlanReference) {
			s.Namespace = "foreign"
		},
		"recreated plan UID": func(s *corev1.Secret, _ *updateoperation.OwnerPlanIntent, _ *updateoperation.OwnerPlanReference) {
			s.UID = "replacement-plan"
		},
		"missing reference UID": func(_ *corev1.Secret, _ *updateoperation.OwnerPlanIntent, r *updateoperation.OwnerPlanReference) {
			r.UID = ""
		},
		"wrong version": func(_ *corev1.Secret, i *updateoperation.OwnerPlanIntent, _ *updateoperation.OwnerPlanReference) {
			i.Version = "v999"
		},
		"missing version": func(_ *corev1.Secret, i *updateoperation.OwnerPlanIntent, _ *updateoperation.OwnerPlanReference) {
			i.Version = ""
		},
		"prepared not released": func(_ *corev1.Secret, i *updateoperation.OwnerPlanIntent, _ *updateoperation.OwnerPlanReference) {
			i.Phase = updateoperation.OwnerPrepared
		},
		"consumed not released": func(_ *corev1.Secret, i *updateoperation.OwnerPlanIntent, _ *updateoperation.OwnerPlanReference) {
			i.Phase = updateoperation.OwnerConsumed
		},
		"unknown phase": func(_ *corev1.Secret, i *updateoperation.OwnerPlanIntent, _ *updateoperation.OwnerPlanReference) {
			i.Phase = "Unknown"
		},
		"missing transaction": func(_ *corev1.Secret, i *updateoperation.OwnerPlanIntent, _ *updateoperation.OwnerPlanReference) {
			i.TransactionID = ""
		},
		"missing freshness": func(_ *corev1.Secret, i *updateoperation.OwnerPlanIntent, _ *updateoperation.OwnerPlanReference) {
			i.InputFreshness = nil
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			i := ownerOperationIntent()
			s := ownerOperationSecret(t, i)
			r := ref
			mutate(s, i, &r)
			s.Data[updateoperation.OwnerIntentKey] = encodedOperation(t, i)
			_, err := updateoperation.ReadOwnerIntent(context.Background(), operationClient(t, s), "tenant", r)
			requireSafeOperationError(t, err)
		})
	}
	for _, data := range [][]byte{nil, []byte(`{"configuration":"` + storePrivateSentinel)} {
		s := ownerOperationSecret(t, ownerOperationIntent())
		s.Data[updateoperation.OwnerIntentKey] = data
		_, err := updateoperation.ReadOwnerIntent(context.Background(), operationClient(t, s), "tenant", ref)
		requireSafeOperationError(t, err)
	}
}

func TestPrivateJournalRoundTripPreservesExactSourcesAndUnknownOutcomes(t *testing.T) {
	t.Parallel()
	j := operationJournal()
	j.Inputs = ownerOperationIntent().Inputs
	j.OwnerPlan = &updateoperation.OwnerPlanReference{Name: "owner-plan", UID: "owner-plan-uid"}
	j.TargetImage = updateoperation.ImageIdentity{Reference: "factory.example/installer@sha256:abc", Digest: "sha256:abc", SchematicID: "schematic", Extensions: []string{"extension-one", "extension-two"}}
	j.PriorAcceptedImageIntent = updateoperation.ImageIdentity{Reference: "factory.example/installer:v1.14.0"}
	j.TargetKubernetesVersion = "v1.37.0"
	zero := int32(0)
	j.Steps = []updateoperation.OperationStep{
		{Kind: updateoperation.Apply, Phase: updateoperation.Issued, IssuedBootID: "before", ApplyOutcome: updateoperation.Unknown},
		{Kind: updateoperation.PullInstaller, Phase: updateoperation.Acknowledged, PulledImageName: j.TargetImage.Reference, PulledImageDigest: j.TargetImage.Digest},
		{Kind: updateoperation.Install, Phase: updateoperation.Acknowledged, InstallExitCode: &zero},
		{Kind: updateoperation.Reboot, Phase: updateoperation.StepObserved, IssuedBootID: "before", ObservedBootID: "after"},
	}
	encoded := encodedOperation(t, j)
	var decoded updateoperation.Journal
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.Equal(t, *j, decoded)
	assert.Nil(t, decoded.Steps[0].InstallExitCode, "unknown exit status must not become successful zero")
	require.NotNil(t, decoded.Steps[2].InstallExitCode)
	assert.Zero(t, *decoded.Steps[2].InstallExitCode)
	assert.Equal(t, updateoperation.Unknown, decoded.Steps[0].ApplyOutcome)
	assert.Equal(t, sha256.Sum256([]byte(storePrivateSentinel)), decoded.Inputs[0].Digest)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &fields))
	assert.Contains(t, fields, "transactionID")
	assert.Contains(t, fields, "inputFreshness")
	assert.Contains(t, fields, "priorAcceptedImageIntent")
	assert.NotContains(t, fields, "TransactionID")
}

func TestCompletionReceiptRoundTripDoesNotPublishConfigurationOrImage(t *testing.T) {
	t.Parallel()
	want := operationReceipt(operationJournal())
	encoded := encodedOperation(t, want)
	var got updateoperation.CompletionReceipt
	require.NoError(t, json.Unmarshal(encoded, &got))
	assert.Equal(t, *want, got)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &fields))
	assert.Len(t, fields, 7)
	for _, key := range []string{"transactionID", "machineUID", "talosConfigUID", "infrastructureUID", "inputFreshness", "phase", "completedAt"} {
		assert.Contains(t, fields, key)
	}
	assert.NotContains(t, fields, "configuration")
	assert.NotContains(t, fields, "targetImage")
	assert.NotContains(t, string(encoded), storePrivateSentinel)
}
