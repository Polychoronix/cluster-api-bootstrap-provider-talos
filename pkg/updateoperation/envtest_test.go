// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation_test

import (
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"testing"
)

func TestJournalCASDisposableAPIRejectsDeletedAndRecreatedSecrets(t *testing.T) {
	assets := os.Getenv("KUBEBUILDER_ASSETS")
	if assets == "" || testing.Short() {
		t.Skip("set explicit KUBEBUILDER_ASSETS for isolated journal API smoke")
	}
	environment := &envtest.Environment{UseExistingCluster: ptr.To(false), BinaryAssetsDirectory: assets}
	cfg, err := environment.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, environment.Stop()) })
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	require.NoError(t, c.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "journal-test"}}))
	j := operationJournal()
	created, err := op.CreateJournal(t.Context(), c, "journal-test", j)
	require.NoError(t, err)
	require.NotEmpty(t, created.UID)
	live, got, err := op.ReadJournal(t.Context(), c, "journal-test", j.TalosConfig)
	require.NoError(t, err)
	require.NoError(t, c.Delete(t.Context(), live))
	replacement := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: live.Namespace, Name: live.Name}, Data: map[string][]byte{"foreign": []byte("preserve-foreign")}}
	require.NoError(t, c.Create(t.Context(), replacement))
	require.NotEqual(t, live.UID, replacement.UID)
	require.Error(t, op.WriteJournal(t.Context(), c, live, got, nil))
	actual := &corev1.Secret{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(replacement), actual))
	require.Equal(t, replacement.Data, actual.Data)
	require.NoError(t, c.Delete(t.Context(), replacement))
	require.Error(t, op.WriteJournal(t.Context(), c, live, got, nil))
	require.Error(t, c.Get(t.Context(), client.ObjectKeyFromObject(replacement), actual), "conditional patch must never recreate deleted Secret")
}
