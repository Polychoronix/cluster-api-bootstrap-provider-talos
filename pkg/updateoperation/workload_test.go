// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestWorkloadFailureClassification(t *testing.T) {
	detail, notFound := WorkloadFailure(apierrors.NewNotFound(schema.GroupResource{Resource: "nodes"}, "n"))
	require.True(t, notFound)
	require.Equal(t, "not found", detail)
	detail, notFound = WorkloadFailure(fmt.Errorf("get: %w", context.DeadlineExceeded))
	require.False(t, notFound)
	require.Contains(t, detail, "timed out")
	detail, notFound = WorkloadFailure(apierrors.NewServiceUnavailable("secret server text"))
	require.False(t, notFound)
	require.Equal(t, "API status 503 ServiceUnavailable", detail)
	detail, _ = WorkloadFailure(errors.New("dial tcp: secret detail"))
	require.Equal(t, "transport error", detail)
}

// validateManagementNode separates an unreachable workload API from a changed Node: only
// the latter is "identity changed"; the former wraps ErrWorkloadUnavailable.
func TestActivatedNodeLookupFailureIsNotIdentityChange(t *testing.T) {
	previous := WorkloadAPITimeout
	WorkloadAPITimeout = 200 * time.Millisecond
	t.Cleanup(func() { WorkloadAPITimeout = previous })

	var mode atomic.Value
	mode.Store("")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch mode.Load().(string) {
		case "slow":
			time.Sleep(time.Second)
		case "down":
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, Status: metav1.StatusFailure, Reason: metav1.StatusReasonServiceUnavailable, Code: http.StatusServiceUnavailable})
			return
		case "gone":
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, Status: metav1.StatusFailure, Reason: metav1.StatusReasonNotFound, Code: http.StatusNotFound})
			return
		}
		uid := "node-uid"
		if mode.Load() == "replaced" {
			uid = "other-uid"
		}
		json.NewEncoder(w).Encode(&corev1.Node{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Node"}, ObjectMeta: metav1.ObjectMeta{Name: "tiikerinpentu", UID: types.UID(uid)}, Spec: corev1.NodeSpec{ProviderID: "hcloud://bm-1"}})
	}))
	t.Cleanup(server.Close)
	kube := fmt.Sprintf("apiVersion: v1\nkind: Config\nclusters:\n- name: c\n  cluster:\n    server: %s\ncontexts:\n- name: c\n  context:\n    cluster: c\n    user: u\ncurrent-context: c\nusers:\n- name: u\n  user: {}\n", server.URL)
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "cluster", Name: "kissanpentu-kubeconfig"}, Data: map[string][]byte{"value": []byte(kube)}}).Build()
	safety := &managementSafety{Authority: ManagementAuthority{Namespace: "cluster"}, ClusterName: "kissanpentu"}
	member := &managementMember{NodeName: "tiikerinpentu", NodeUID: "node-uid", ProviderID: "hcloud://bm-1"}

	mode.Store("")
	require.NoError(t, validateManagementNode(t.Context(), reader, safety, member))
	for _, unavailable := range []string{"slow", "down"} {
		mode.Store(unavailable)
		err := validateManagementNode(t.Context(), reader, safety, member)
		require.True(t, errors.Is(err, ErrWorkloadUnavailable), "%s: %v", unavailable, err)
		require.Contains(t, err.Error(), "activated workload Node lookup failed: tiikerinpentu: ")
		require.NotContains(t, err.Error(), "identity changed")
	}
	mode.Store("replaced")
	err := validateManagementNode(t.Context(), reader, safety, member)
	require.False(t, errors.Is(err, ErrWorkloadUnavailable))
	require.EqualError(t, err, "Talos management activated workload Node identity changed failed: tiikerinpentu: UID node-uid -> other-uid")
	mode.Store("gone")
	err = validateManagementNode(t.Context(), reader, safety, member)
	require.False(t, errors.Is(err, ErrWorkloadUnavailable))
	require.Contains(t, err.Error(), "identity changed failed: tiikerinpentu: not found")
}
