// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package updateoperation_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
)

const storePrivateSentinel = "private-configuration-and-parser-cause"

func operationJournal() *updateoperation.Journal {
	return &updateoperation.Journal{
		Version:        updateoperation.SchemaVersion,
		TransactionID:  "transaction-one",
		Machine:        updateoperation.ObjectIdentity{APIGroup: "cluster.x-k8s.io", Kind: "Machine", Name: "machine", UID: "machine-uid"},
		TalosConfig:    updateoperation.ObjectIdentity{APIGroup: "bootstrap.cluster.x-k8s.io", Kind: "TalosConfig", Name: "config", UID: "config-uid"},
		Infrastructure: updateoperation.ObjectIdentity{APIGroup: "infrastructure.cluster.x-k8s.io", Kind: "MetalMachine", Name: "infra", UID: "infra-uid"},
		InputFreshness: []byte("private-freshness"), Configuration: []byte(storePrivateSentinel),
		Phase: updateoperation.Planned, NodeName: "node", NodeUID: "node-uid", NodeProviderID: "metal://node",
		Steps: []updateoperation.OperationStep{{Kind: updateoperation.Apply, Phase: updateoperation.StepPlanned, ApplyOutcome: updateoperation.Unknown}},
	}
}

func operationReceipt(j *updateoperation.Journal) *updateoperation.CompletionReceipt {
	return &updateoperation.CompletionReceipt{TransactionID: j.TransactionID, MachineUID: j.Machine.UID,
		Version:        updateoperation.SchemaVersion,
		TalosConfigUID: j.TalosConfig.UID, InfrastructureUID: j.Infrastructure.UID,
		InputFreshness: append([]byte(nil), j.InputFreshness...), Phase: updateoperation.Observed,
		CompletedAt: metav1.NewTime(time.Date(2026, 10, 21, 12, 0, 0, 0, time.Local))}
}

func encodedOperation(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	require.NoError(t, err)
	return data
}

func operationSecret(t *testing.T, j *updateoperation.Journal) *corev1.Secret {
	t.Helper()
	controller := true
	return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: updateoperation.SecretName(j.TalosConfig), UID: "journal-secret-uid", ResourceVersion: "7",
		Annotations:     map[string]string{updateoperation.ProvenanceAnnotation: updateoperation.ProvenanceValue},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: j.TalosConfig.APIGroup + "/v1beta1", Kind: j.TalosConfig.Kind, Name: j.TalosConfig.Name, UID: j.TalosConfig.UID, Controller: &controller}}},
		Data: map[string][]byte{updateoperation.JournalKey: encodedOperation(t, j), "unrelated": []byte("preserve")}}
}

func operationClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

func requireSafeOperationError(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	assert.Nil(t, errors.Unwrap(err))
	assert.NotContains(t, fmt.Sprintf("%v %+v %#v", err, err, err), storePrivateSentinel)
}

type operationFailingReader struct{ client.Reader }

func (operationFailingReader) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return errors.New(storePrivateSentinel)
}

func TestReadOperationErrorsDoNotExposePrivateCauses(t *testing.T) {
	t.Parallel()
	j := operationJournal()
	reader := operationFailingReader{}
	_, err := updateoperation.ReadReceipt(context.Background(), reader, "tenant", j.TalosConfig, j.TransactionID)
	requireSafeOperationError(t, err)
	_, err = updateoperation.ReadOwnerIntent(context.Background(), reader, "tenant", updateoperation.OwnerPlanReference{Name: "plan", UID: "plan-uid"})
	requireSafeOperationError(t, err)
	_, _, err = updateoperation.ReadJournal(context.Background(), reader, "tenant", j.TalosConfig)
	requireSafeOperationError(t, err)
}

func TestJournalAndReceiptRejectWrongEnvelope(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*corev1.Secret){
		"foreign namespace":     func(s *corev1.Secret) { s.Namespace = "foreign" },
		"wrong provenance":      func(s *corev1.Secret) { s.Annotations[updateoperation.ProvenanceAnnotation] = "foreign-provider" },
		"missing provenance":    func(s *corev1.Secret) { s.Annotations = nil },
		"missing owner":         func(s *corev1.Secret) { s.OwnerReferences = nil },
		"replacement owner UID": func(s *corev1.Secret) { s.OwnerReferences[0].UID = "replacement-config" },
		"wrong owner name":      func(s *corev1.Secret) { s.OwnerReferences[0].Name = "other-config" },
		"wrong owner kind":      func(s *corev1.Secret) { s.OwnerReferences[0].Kind = "Secret" },
		"wrong owner version":   func(s *corev1.Secret) { s.OwnerReferences[0].APIVersion = "bootstrap.cluster.x-k8s.io/v999" },
		"not controller":        func(s *corev1.Secret) { controller := false; s.OwnerReferences[0].Controller = &controller },
		"controller absent":     func(s *corev1.Secret) { s.OwnerReferences[0].Controller = nil },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			j := operationJournal()
			s := operationSecret(t, j)
			s.Data[updateoperation.ReceiptKey] = encodedOperation(t, operationReceipt(j))
			mutate(s)
			c := operationClient(t, s)
			_, _, err := updateoperation.ReadJournal(context.Background(), c, "tenant", j.TalosConfig)
			requireSafeOperationError(t, err)
			_, err = updateoperation.ReadReceipt(context.Background(), c, "tenant", j.TalosConfig, j.TransactionID)
			requireSafeOperationError(t, err)
		})
	}
}

func TestReadReceiptRequiresExactObservedIdentity(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*updateoperation.CompletionReceipt){
		"transaction":             func(r *updateoperation.CompletionReceipt) { r.TransactionID = "other-transaction" },
		"config UID":              func(r *updateoperation.CompletionReceipt) { r.TalosConfigUID = "replacement-config" },
		"missing machine":         func(r *updateoperation.CompletionReceipt) { r.MachineUID = "" },
		"missing infrastructure":  func(r *updateoperation.CompletionReceipt) { r.InfrastructureUID = "" },
		"planned":                 func(r *updateoperation.CompletionReceipt) { r.Phase = updateoperation.Planned },
		"ambiguous":               func(r *updateoperation.CompletionReceipt) { r.Phase = updateoperation.Ambiguous },
		"missing freshness":       func(r *updateoperation.CompletionReceipt) { r.InputFreshness = nil },
		"missing completion time": func(r *updateoperation.CompletionReceipt) { r.CompletedAt = metav1.Time{} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			j := operationJournal()
			r := operationReceipt(j)
			mutate(r)
			s := operationSecret(t, j)
			s.Data[updateoperation.ReceiptKey] = encodedOperation(t, r)
			_, err := updateoperation.ReadReceipt(context.Background(), operationClient(t, s), "tenant", j.TalosConfig, j.TransactionID)
			requireSafeOperationError(t, err)
		})
	}
	j := operationJournal()
	s := operationSecret(t, j)
	want := operationReceipt(j)
	completeJournal(j, want.CompletedAt)
	s.Data[updateoperation.JournalKey] = encodedOperation(t, j)
	s.Data[updateoperation.ReceiptKey] = encodedOperation(t, want)
	got, err := updateoperation.ReadReceipt(context.Background(), operationClient(t, s), "tenant", j.TalosConfig, j.TransactionID)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	for _, key := range []string{updateoperation.JournalKey, updateoperation.ReceiptKey} {
		broken := s.DeepCopy()
		broken.Data[key] = []byte(`{"private":"` + storePrivateSentinel)
		if key == updateoperation.JournalKey {
			_, _, err = updateoperation.ReadJournal(context.Background(), operationClient(t, broken), "tenant", j.TalosConfig)
		} else {
			_, err = updateoperation.ReadReceipt(context.Background(), operationClient(t, broken), "tenant", j.TalosConfig, j.TransactionID)
		}
		requireSafeOperationError(t, err)
	}
}

func TestCreateJournalIsPrivateNamespaceLocalAndNeverOverwrites(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	j := operationJournal()
	c := operationClient(t)
	s, err := updateoperation.CreateJournal(ctx, c, "tenant", j)
	require.NoError(t, err)
	assert.Equal(t, "tenant", s.Namespace)
	assert.Equal(t, updateoperation.SecretName(j.TalosConfig), s.Name)
	assert.Equal(t, map[string]string{updateoperation.ProvenanceAnnotation: updateoperation.ProvenanceValue}, s.Annotations)
	assert.Empty(t, s.Labels)
	assert.NotContains(t, fmt.Sprint(s.ObjectMeta), storePrivateSentinel)
	_, got, err := updateoperation.ReadJournal(ctx, c, "tenant", j.TalosConfig)
	require.NoError(t, err)
	assert.Equal(t, j, got)
	j.TransactionID = "transaction-two"
	_, err = updateoperation.CreateJournal(ctx, c, "tenant", j)
	requireSafeOperationError(t, err)
	_, got, err = updateoperation.ReadJournal(ctx, c, "tenant", j.TalosConfig)
	require.NoError(t, err)
	assert.Equal(t, "transaction-one", got.TransactionID)
	_, _, err = updateoperation.ReadJournal(ctx, c, "foreign", j.TalosConfig)
	requireSafeOperationError(t, err)
}

type operationRecordingClient struct {
	client.Client
	patchType types.PatchType
	patchData []byte
	creates   int
	patches   int
}

func (c *operationRecordingClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	c.creates++
	return c.Client.Create(ctx, obj, opts...)
}

func (c *operationRecordingClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	c.patches++
	c.patchType = patch.Type()
	data, err := patch.Data(obj)
	if err != nil {
		return err
	}
	c.patchData = append([]byte(nil), data...)
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func TestWriteJournalUsesUIDAndResourceVersionJSONPatch(t *testing.T) {
	t.Parallel()
	j := operationJournal()
	s := operationSecret(t, j)
	c := &operationRecordingClient{Client: operationClient(t, s)}
	j.Phase = updateoperation.Observed
	r := operationReceipt(j)
	j.CompletedAt = &r.CompletedAt
	completeJournal(j, r.CompletedAt)
	before := s.DeepCopy()
	require.NoError(t, updateoperation.WriteJournal(context.Background(), c, s, j, r))
	assert.Equal(t, types.JSONPatchType, c.patchType)
	var ops []struct {
		Op    string          `json:"op"`
		Path  string          `json:"path"`
		Value json.RawMessage `json:"value"`
	}
	require.NoError(t, json.Unmarshal(c.patchData, &ops))
	require.Len(t, ops, 3)
	assert.Equal(t, "test", ops[0].Op)
	assert.Equal(t, "/metadata/uid", ops[0].Path)
	assert.JSONEq(t, `"journal-secret-uid"`, string(ops[0].Value))
	assert.Equal(t, "test", ops[1].Op)
	assert.Equal(t, "/metadata/resourceVersion", ops[1].Path)
	assert.JSONEq(t, `"7"`, string(ops[1].Value))
	assert.Equal(t, "replace", ops[2].Op)
	assert.Equal(t, "/data", ops[2].Path)
	assert.Zero(t, c.creates)
	assert.Equal(t, before, s, "CAS must not mutate the validated snapshot")
	live, got, err := updateoperation.ReadJournal(context.Background(), c, "tenant", j.TalosConfig)
	require.NoError(t, err)
	assert.Equal(t, j, got)
	assert.Equal(t, []byte("preserve"), live.Data["unrelated"])
	receipt, err := updateoperation.ReadReceipt(context.Background(), c, "tenant", j.TalosConfig, j.TransactionID)
	require.NoError(t, err)
	assert.Equal(t, r, receipt)
}

func TestWriteJournalNeverCreatesOrPatchesReplacement(t *testing.T) {
	t.Parallel()
	for _, change := range []string{"deleted", "recreated UID", "concurrent resourceVersion"} {
		t.Run(change, func(t *testing.T) {
			ctx := context.Background()
			j := operationJournal()
			s := operationSecret(t, j)
			c := &operationRecordingClient{Client: operationClient(t, s)}
			if change == "concurrent resourceVersion" {
				live := s.DeepCopy()
				live.Data["unrelated"] = []byte("concurrent-update")
				require.NoError(t, c.Update(ctx, live))
			} else {
				require.NoError(t, c.Delete(ctx, s))
				if change == "recreated UID" {
					replacement := s.DeepCopy()
					replacement.UID = "replacement-secret-uid"
					replacement.ResourceVersion = ""
					require.NoError(t, c.Client.Create(ctx, replacement))
				}
			}
			liveBefore := &corev1.Secret{}
			beforeErr := c.Get(ctx, client.ObjectKeyFromObject(s), liveBefore)
			j.Steps[0].Phase = updateoperation.Issued
			err := updateoperation.WriteJournal(ctx, c, s, j, nil)
			requireSafeOperationError(t, err)
			assert.Zero(t, c.creates)
			liveAfter := &corev1.Secret{}
			afterErr := c.Get(ctx, client.ObjectKeyFromObject(s), liveAfter)
			if beforeErr != nil {
				require.Error(t, afterErr)
			} else {
				require.NoError(t, afterErr)
				assert.Equal(t, liveBefore, liveAfter)
			}
		})
	}
}

func TestWriteJournalProtectsUnconsumedReceipt(t *testing.T) {
	t.Parallel()
	for _, change := range []string{"overwrite receipt", "new generation"} {
		t.Run(change, func(t *testing.T) {
			j := operationJournal()
			s := operationSecret(t, j)
			s.Data[updateoperation.ReceiptKey] = encodedOperation(t, operationReceipt(j))
			c := &operationRecordingClient{Client: operationClient(t, s)}
			var receipt *updateoperation.CompletionReceipt
			if change == "overwrite receipt" {
				receipt = operationReceipt(j)
			} else {
				j.TransactionID = "transaction-two"
			}
			err := updateoperation.WriteJournal(context.Background(), c, s, j, receipt)
			requireSafeOperationError(t, err)
			assert.Zero(t, c.patches, "reject before any Kubernetes mutation")
			live := &corev1.Secret{}
			require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(s), live))
			assert.Equal(t, s.Data, live.Data)
		})
	}
}

func TestWriteJournalRejectsMismatchedReceiptBeforePatch(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*updateoperation.CompletionReceipt){
		"transaction":    func(r *updateoperation.CompletionReceipt) { r.TransactionID = "other" },
		"machine":        func(r *updateoperation.CompletionReceipt) { r.MachineUID = "other" },
		"config":         func(r *updateoperation.CompletionReceipt) { r.TalosConfigUID = "other" },
		"infrastructure": func(r *updateoperation.CompletionReceipt) { r.InfrastructureUID = "other" },
		"freshness":      func(r *updateoperation.CompletionReceipt) { r.InputFreshness = []byte("other") },
		"phase":          func(r *updateoperation.CompletionReceipt) { r.Phase = updateoperation.Ambiguous },
		"time":           func(r *updateoperation.CompletionReceipt) { r.CompletedAt = metav1.Time{} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			j := operationJournal()
			s := operationSecret(t, j)
			j.Phase = updateoperation.Observed
			r := operationReceipt(j)
			j.CompletedAt = &r.CompletedAt
			j.Steps[0].Phase = updateoperation.StepObserved
			j.Steps[0].ApplyOutcome = updateoperation.NoReboot
			mutate(r)
			c := &operationRecordingClient{Client: operationClient(t, s)}
			requireSafeOperationError(t, updateoperation.WriteJournal(context.Background(), c, s, j, r))
			assert.Zero(t, c.patches)
		})
	}
}

func TestCreateJournalRejectsMissingIdentityBeforeCreate(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*updateoperation.Journal){
		"transaction":        func(j *updateoperation.Journal) { j.TransactionID = "" },
		"machine UID":        func(j *updateoperation.Journal) { j.Machine.UID = "" },
		"config UID":         func(j *updateoperation.Journal) { j.TalosConfig.UID = "" },
		"infrastructure UID": func(j *updateoperation.Journal) { j.Infrastructure.UID = "" },
		"node UID":           func(j *updateoperation.Journal) { j.NodeUID = "" },
		"provider ID":        func(j *updateoperation.Journal) { j.NodeProviderID = "" },
		"freshness":          func(j *updateoperation.Journal) { j.InputFreshness = nil },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			j := operationJournal()
			mutate(j)
			c := &operationRecordingClient{Client: operationClient(t)}
			_, err := updateoperation.CreateJournal(context.Background(), c, "tenant", j)
			requireSafeOperationError(t, err)
			assert.Zero(t, c.creates)
		})
	}
	c := &operationRecordingClient{Client: operationClient(t)}
	_, err := updateoperation.CreateJournal(context.Background(), c, "tenant", nil)
	requireSafeOperationError(t, err)
	assert.Zero(t, c.creates)
}

func TestWriteJournalRejectsMissingSnapshotIdentityBeforePatch(t *testing.T) {
	t.Parallel()
	for _, missing := range []string{"snapshot", "UID", "resourceVersion", "owner UID"} {
		t.Run(missing, func(t *testing.T) {
			j := operationJournal()
			s := operationSecret(t, j)
			c := &operationRecordingClient{Client: operationClient(t, s)}
			switch missing {
			case "snapshot":
				s = nil
			case "UID":
				s.UID = ""
			case "resourceVersion":
				s.ResourceVersion = ""
			case "owner UID":
				s.OwnerReferences[0].UID = "replacement"
			}
			requireSafeOperationError(t, updateoperation.WriteJournal(context.Background(), c, s, j, nil))
			assert.Zero(t, c.patches)
			assert.Zero(t, c.creates)
		})
	}
}

func TestReadJournalRejectsReplacedConfigIdentity(t *testing.T) {
	t.Parallel()
	j := operationJournal()
	s := operationSecret(t, j)
	j.TalosConfig.UID = "replaced-config-uid"
	s.Data[updateoperation.JournalKey] = encodedOperation(t, j)
	_, _, err := updateoperation.ReadJournal(context.Background(), operationClient(t, s), "tenant", operationJournal().TalosConfig)
	requireSafeOperationError(t, err)
}

func TestReadReceiptRejectsEmptyRequestedTransaction(t *testing.T) {
	t.Parallel()
	j := operationJournal()
	s := operationSecret(t, j)
	r := operationReceipt(j)
	r.TransactionID = ""
	s.Data[updateoperation.ReceiptKey] = encodedOperation(t, r)
	_, err := updateoperation.ReadReceipt(context.Background(), operationClient(t, s), "tenant", j.TalosConfig, "")
	requireSafeOperationError(t, err)
}

func completeJournal(j *updateoperation.Journal, at metav1.Time) {
	j.Phase = updateoperation.Observed
	j.CompletedAt = &at
	issued := metav1.NewTime(at.Add(-time.Minute))
	deadline := metav1.NewTime(at.Add(time.Minute))
	for i := range j.Steps {
		j.Steps[i].Phase = updateoperation.StepObserved
		j.Steps[i].IssuerID = "issuer"
		j.Steps[i].IssuedAt = &issued
		j.Steps[i].Deadline = &deadline
		j.Steps[i].IssuedBootID = "boot"
		if j.Steps[i].Kind == updateoperation.Apply {
			j.Steps[i].ApplyOutcome = updateoperation.NoReboot
		}
	}
}
