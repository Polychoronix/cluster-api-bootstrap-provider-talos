// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
)

func resnapshotTalosConfig(machineCA, clusterID string) []byte {
	b := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
	return []byte(fmt.Sprintf(`version: v1alpha1
machine:
  type: controlplane
  ca:
    crt: %s
    key: %s
cluster:
  id: %s
  secret: cluster-secret
  etcd:
    ca:
      crt: %s
      key: %s
`, b(machineCA), b("PRIVATE-KEY"), clusterID, b("etcd-ca"), b("PRIVATE-KEY")))
}

// activatedResnapshotFixture records and activates a two-member snapshot with an etcd
// view, returning the store, authority and an observation equal to the approved one.
func activatedResnapshotFixture(t *testing.T) (*assigningClient, ManagementAuthority, *ManagementObservation, *managementRecord) {
	t.Helper()
	c, a, o := managementStoreFixture(t)
	o.safety.Inputs = []configinput.SourceIdentity{{Name: "cluster-kubeconfig", Key: "value", UID: "kubeconfig-uid", ResourceVersion: "1", Digest: [32]byte{1}}}
	config := resnapshotTalosConfig("machine-ca", "cluster-id")
	o.safety.Members = []managementMember{
		{Machine: ObjectIdentity{Kind: "Machine", Name: "tiikerinpentu", UID: "m1"}, NodeName: "tiikerinpentu", NodeUID: "n1", ProviderID: "p1", Runtime: managementRuntime{Configuration: config, BootID: "boot-1", OSVersion: "v1.14.2"}},
		{Machine: ObjectIdentity{Kind: "Machine", Name: "leijonanpentu", UID: "m2"}, NodeName: "leijonanpentu", NodeUID: "n2", ProviderID: "p2", Runtime: managementRuntime{Configuration: config, BootID: "boot-2", OSVersion: "v1.14.2"}},
	}
	o.safety.Etcd = []managementEtcdMember{
		{MemberID: 0x9b92dcd3c97ac6f4, Hostname: "leijonanpentu", MachineUID: "m2", NodeUID: "n2"},
		{MemberID: 0xaaaa, Hostname: "tiikerinpentu", MachineUID: "m1", NodeUID: "n1"},
	}
	_, err := RecordManagementObservation(t.Context(), c, c, o)
	require.NoError(t, err)
	secret, record, _, err := readManagement(t.Context(), c, a)
	require.NoError(t, err)
	record.Activated = true
	record.AcceptedImages = map[types.UID]ImageIdentity{"m1": {Reference: "installer:v1.14.2"}, "m2": {Reference: "installer:v1.14.2"}}
	encoded, err := json.Marshal(record)
	require.NoError(t, err)
	require.NoError(t, PatchData(t.Context(), c, secret, map[string][]byte{managementRecordKey: encoded}))
	return c, a, o, record
}

func TestActivatedSnapshotFollowsNodeLifecycle(t *testing.T) {
	c, a, o, approved := activatedResnapshotFixture(t)
	// tiikerinpentu was reset: new boot, re-registered Node, new etcd member ID under
	// the same hostname; the kubeconfig Secret only saw metadata churn.
	o.safety.Members[0].Runtime.BootID = "boot-1b"
	o.safety.Members[0].NodeUID = "n1b"
	o.safety.Etcd[1].MemberID = 0x71c1e0b889b65175
	o.safety.Etcd[1].NodeUID = "n1b"
	o.safety.Inputs[0].ResourceVersion = "2"

	status, err := RecordManagementObservation(t.Context(), c, c, o)
	require.NoError(t, err)
	require.True(t, status.Activated)
	require.NotEqual(t, approved.ObservationID, status.ID)
	_, record, payload, err := readManagement(t.Context(), c, a)
	require.NoError(t, err)
	require.True(t, record.Activated)
	require.Equal(t, approved.AcceptedImages, record.AcceptedImages, "accepted images are never re-derived from the live configuration")
	current, err := json.Marshal(o.safety)
	require.NoError(t, err)
	require.Equal(t, current, payload)

	again, err := RecordManagementObservation(t.Context(), c, c, o)
	require.NoError(t, err)
	require.Equal(t, status.ID, again.ID)
}

func TestActivatedSnapshotIdentityChangesStayFailClosed(t *testing.T) {
	cases := []struct {
		name   string
		change func(o *ManagementObservation)
	}{
		{name: "Cluster identity", change: func(o *ManagementObservation) {
			o.safety.ClusterUID = "other-cluster"
		}},
		{name: "tiikerinpentu: Talos trust identity", change: func(o *ManagementObservation) {
			o.safety.Members[0].Runtime.Configuration = resnapshotTalosConfig("other-machine-ca", "cluster-id")
		}},
		{name: "leijonanpentu: Talos trust identity", change: func(o *ManagementObservation) {
			o.safety.Members[1].Runtime.Configuration = resnapshotTalosConfig("machine-ca", "other-cluster-id")
		}},
		{name: "tiikerinpentu: association", change: func(o *ManagementObservation) {
			o.safety.Members[0].ProviderID = "p-other"
		}},
		{name: "member inventory", change: func(o *ManagementObservation) {
			o.safety.Members = o.safety.Members[:1]
		}},
		{name: "credential cluster-kubeconfig", change: func(o *ManagementObservation) {
			o.safety.Inputs[0].Digest = [32]byte{2}
		}},
	}
	for _, tc := range cases {
		name, change := tc.name, tc.change
		t.Run(name, func(t *testing.T) {
			c, a, o, approved := activatedResnapshotFixture(t)
			o.safety.Members[0].Runtime.BootID = "boot-1b"
			change(o)
			_, err := RecordManagementObservation(t.Context(), c, c, o)
			require.Error(t, err)
			require.Contains(t, err.Error(), "activated safety snapshot changed failed: ")
			require.Contains(t, err.Error(), name)
			require.NotContains(t, err.Error(), "PRIVATE")
			_, record, _, err := readManagement(t.Context(), c, a)
			require.NoError(t, err)
			require.Equal(t, approved.ObservationID, record.ObservationID)
		})
	}
}

func TestManagementSnapshotDeltaNamesEtcdRejoin(t *testing.T) {
	_, _, o, _ := activatedResnapshotFixture(t)
	approved := o.safety
	current := o.safety
	current.Etcd = append([]managementEtcdMember(nil), o.safety.Etcd...)
	current.Etcd[1].MemberID = 0x71c1e0b889b65175
	identity, lifecycle := managementSnapshotDelta(approved, current)
	require.Empty(t, identity)
	require.Equal(t, []string{"etcd tiikerinpentu: member aaaa -> 71c1e0b889b65175"}, lifecycle)
}

// A TalosControlPlane spec edit while an in-place transaction is in flight (the owner
// writes the desired TalosConfig spec before the node converges) is re-verified intent:
// the activated snapshot is replaced, never fail-closed.
func TestActivatedSnapshotFollowsAuthorityPolicyChange(t *testing.T) {
	c, a, o, approved := activatedResnapshotFixture(t)
	approvedSafety := o.safety
	approvedSafety.Members = append([]managementMember(nil), o.safety.Members...)
	o.safety.Members = append([]managementMember(nil), o.safety.Members...)
	o.safety.AuthoritySpec = json.RawMessage(`{"machineTemplate":{"spec":{"deletion":{"nodeDeletionTimeoutSeconds":10}}}}`)
	o.safety.Members[0].ConfigSpec = json.RawMessage(`{"generateType":"controlplane","talosVersion":"v1.14.2"}`)
	o.safety.Members[0].EffectiveSpec = o.safety.Members[0].ConfigSpec
	o.safety.Members[0].Baseline = BaselineCurrent

	identity, lifecycle := managementSnapshotDelta(approvedSafety, o.safety)
	require.Empty(t, identity)
	require.Equal(t, []string{"authority policy", "tiikerinpentu: intent or baseline"}, lifecycle)

	status, err := RecordManagementObservation(t.Context(), c, c, o)
	require.NoError(t, err)
	require.True(t, status.Activated)
	_, record, _, err := readManagement(t.Context(), c, a)
	require.NoError(t, err)
	require.NotEqual(t, approved.ObservationID, record.ObservationID)
	require.Equal(t, approved.AcceptedImages, record.AcceptedImages)
}
