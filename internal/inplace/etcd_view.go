// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package inplace

import (
	"fmt"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	"reflect"
	"sort"
	"strings"
)

func checkedEtcdView(members []*machineapi.EtcdMember, expectedHostname string, status *machineapi.EtcdMemberStatus) ([]op.EtcdMemberIdentity, error) {
	if status == nil {
		return nil, fmt.Errorf("etcd member status absent")
	}
	views := make([]op.EtcdMemberIdentity, 0, len(members))
	ids := map[uint64]bool{}
	names := map[string]bool{}
	selfMatched := false
	leaderMatched := false
	for _, member := range members {
		if member == nil || member.Id == 0 || member.Hostname == "" || member.IsLearner {
			return nil, fmt.Errorf("etcd member view incomplete")
		}
		name := strings.ToLower(member.Hostname)
		if ids[member.Id] || names[name] {
			return nil, fmt.Errorf("etcd member view duplicate")
		}
		ids[member.Id] = true
		names[name] = true
		if member.Id == status.MemberId && name == strings.ToLower(expectedHostname) {
			selfMatched = true
		}
		if member.Id == status.Leader {
			leaderMatched = true
		}
		peers := append([]string(nil), member.PeerUrls...)
		clients := append([]string(nil), member.ClientUrls...)
		sort.Strings(peers)
		sort.Strings(clients)
		views = append(views, op.EtcdMemberIdentity{MemberID: member.Id, Hostname: name, PeerURLs: peers, ClientURLs: clients})
	}
	if !selfMatched || !leaderMatched {
		return nil, fmt.Errorf("etcd runtime member does not match queried node view")
	}
	sort.Slice(views, func(i, j int) bool { return views[i].MemberID < views[j].MemberID })
	return views, nil
}
func sameEtcdView(a, b []op.EtcdMemberIdentity) bool { return reflect.DeepEqual(a, b) }
