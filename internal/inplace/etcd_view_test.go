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

func TestEtcdThreeMemberViewsBindQueriedMemberAndLeader(t *testing.T) {
	members := []*machineapi.EtcdMember{{Id: 1, Hostname: "node-one", PeerUrls: []string{"https://one:2380"}, ClientUrls: []string{"https://one:2379"}}, {Id: 2, Hostname: "node-two", PeerUrls: []string{"https://two:2380"}, ClientUrls: []string{"https://two:2379"}}, {Id: 3, Hostname: "node-three", PeerUrls: []string{"https://three:2380"}, ClientUrls: []string{"https://three:2379"}}}
	var initial []op.EtcdMemberIdentity
	for i, name := range []string{"node-one", "node-two", "node-three"} {
		view, err := checkedEtcdView(members, name, &machineapi.EtcdMemberStatus{MemberId: uint64(i + 1), Leader: 2})
		require.NoError(t, err)
		if initial == nil {
			initial = view
		} else {
			require.True(t, sameEtcdView(initial, view))
		}
	}
	_, err := checkedEtcdView(members, "node-one", &machineapi.EtcdMemberStatus{MemberId: 2, Leader: 2})
	require.Error(t, err, "misrouted healthy peer must not count as queried member")
	split := []*machineapi.EtcdMember{{Id: 11, Hostname: "node-one", PeerUrls: []string{"https://one:2380"}}, {Id: 12, Hostname: "node-two"}, {Id: 13, Hostname: "node-three"}}
	view, err := checkedEtcdView(split, "node-one", &machineapi.EtcdMemberStatus{MemberId: 11, Leader: 12})
	require.NoError(t, err)
	require.False(t, sameEtcdView(initial, view), "same hostnames from split ID space must fail")
	changedURL := []*machineapi.EtcdMember{{Id: 1, Hostname: "node-one", PeerUrls: []string{"https://wrong:2380"}}, {Id: 2, Hostname: "node-two"}, {Id: 3, Hostname: "node-three"}}
	view, err = checkedEtcdView(changedURL, "node-one", &machineapi.EtcdMemberStatus{MemberId: 1, Leader: 2})
	require.NoError(t, err)
	require.False(t, sameEtcdView(initial, view))
	_, err = checkedEtcdView(members, "node-one", &machineapi.EtcdMemberStatus{MemberId: 1, Leader: 99})
	require.Error(t, err)
}
