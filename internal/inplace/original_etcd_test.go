// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package inplace

import (
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestOriginalEtcdIdentityRejectsReplacementButAllowsLeaderProgress(t *testing.T) {
	original := []op.EtcdMemberIdentity{{MemberID: 1, Hostname: "one", PeerURLs: []string{"https://one:2380"}, ClientURLs: []string{"https://one:2379"}}, {MemberID: 2, Hostname: "two", PeerURLs: []string{"https://two:2380"}, ClientURLs: []string{"https://two:2379"}}, {MemberID: 3, Hostname: "three", PeerURLs: []string{"https://three:2380"}, ClientURLs: []string{"https://three:2379"}}}
	members := []*machineapi.EtcdMember{{Id: 1, Hostname: "one", PeerUrls: original[0].PeerURLs, ClientUrls: original[0].ClientURLs}, {Id: 2, Hostname: "two", PeerUrls: original[1].PeerURLs, ClientUrls: original[1].ClientURLs}, {Id: 3, Hostname: "three", PeerUrls: original[2].PeerURLs, ClientUrls: original[2].ClientURLs}}
	for _, leader := range []uint64{1, 2, 3} {
		view, err := checkedEtcdView(members, "one", &machineapi.EtcdMemberStatus{MemberId: 1, Leader: leader, RaftTerm: 100, RaftAppliedIndex: 200})
		require.NoError(t, err)
		require.True(t, sameEtcdView(original, view))
	}
	members[0].Id = 11
	members[1].Id = 12
	members[2].Id = 13
	view, err := checkedEtcdView(members, "one", &machineapi.EtcdMemberStatus{MemberId: 11, Leader: 12})
	require.NoError(t, err)
	require.False(t, sameEtcdView(original, view), "mutually consistent current peers cannot authorize replaced original IDs")
}
