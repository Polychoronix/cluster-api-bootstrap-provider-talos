// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"reflect"
	"sort"

	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
	"github.com/siderolabs/talos/pkg/machinery/config/configloader"
	"k8s.io/apimachinery/pkg/types"
)

// managementSnapshotDelta compares an activated safety snapshot with a fresh, fully
// verified observation of the same authority. identity names identity-class changes,
// which keep the activated record fail-closed: the authority, the Cluster (UID, name,
// network and endpoint), the cluster credentials (kubeconfig, talosconfig trust
// identity), the member inventory, each member's Machine/TalosConfig/InfraMachine,
// Node name, provider ID and original seed, and the trust identity a member's live
// configuration carries (machine, etcd and Kubernetes CAs, discovery cluster ID and
// secret). lifecycle names the remaining differences: a member's node lifecycle (Node
// UID, boot ID, live configuration, OS and kubelet versions, extensions), etcd
// membership (member IDs after a node rejoined), and policy/intent the observation
// re-verified against the live objects. Every field not normalized away by
// managementSnapshotLifecycleFree is identity-class, so a field added later fails closed.
// Entries carry names and IDs only, never configuration bytes or Secret data.
func managementSnapshotDelta(previous, current managementSafety) (identity, lifecycle []string) {
	if previous.Authority != current.Authority {
		identity = append(identity, "authority")
	}
	if previous.ClusterUID != current.ClusterUID || previous.ClusterName != current.ClusterName {
		identity = append(identity, "Cluster identity")
	}
	if !bytes.Equal(previous.ClusterInputs, current.ClusterInputs) {
		identity = append(identity, "Cluster inputs")
	}
	if len(previous.Inputs) != len(current.Inputs) {
		identity = append(identity, "cluster credentials")
	} else {
		for i := range previous.Inputs {
			switch {
			case !sameRecordedCredential(previous.Inputs[i], current.Inputs[i]):
				identity = append(identity, "credential "+previous.Inputs[i].Name)
			case previous.Inputs[i] != current.Inputs[i]:
				lifecycle = append(lifecycle, "credential "+previous.Inputs[i].Name+" metadata")
			}
		}
	}
	if !bytes.Equal(previous.AuthoritySpec, current.AuthoritySpec) {
		lifecycle = append(lifecycle, "authority policy")
	}
	byUID := make(map[types.UID]managementMember, len(current.Members))
	for _, member := range current.Members {
		byUID[member.Machine.UID] = member
	}
	if len(previous.Members) != len(current.Members) {
		identity = append(identity, "member inventory")
	}
	for _, before := range previous.Members {
		name := before.Machine.Name
		after, ok := byUID[before.Machine.UID]
		if !ok {
			identity = append(identity, name+": left the inventory")
			continue
		}
		if !reflect.DeepEqual(before.Machine, after.Machine) || !reflect.DeepEqual(before.TalosConfig, after.TalosConfig) || !reflect.DeepEqual(before.Infrastructure, after.Infrastructure) || before.NodeName != after.NodeName || before.ProviderID != after.ProviderID || !sameManagementIdentity(before.Seed, after.Seed) {
			identity = append(identity, name+": association")
		}
		beforeTrust, okBefore := managementTalosIdentity(before.Runtime.Configuration)
		afterTrust, okAfter := managementTalosIdentity(after.Runtime.Configuration)
		if !okBefore || !okAfter || beforeTrust != afterTrust {
			identity = append(identity, name+": Talos trust identity")
		}
		if before.NodeUID != after.NodeUID {
			lifecycle = append(lifecycle, name+": Node UID "+string(before.NodeUID)+" -> "+string(after.NodeUID))
		}
		if before.Runtime.BootID != after.Runtime.BootID {
			lifecycle = append(lifecycle, name+": boot ID "+before.Runtime.BootID+" -> "+after.Runtime.BootID)
		}
		if !bytes.Equal(before.Runtime.Configuration, after.Runtime.Configuration) {
			lifecycle = append(lifecycle, name+": live configuration")
		}
		if before.Runtime.OSVersion != after.Runtime.OSVersion || before.Runtime.KubeletImage != after.Runtime.KubeletImage || before.Runtime.SchematicID != after.Runtime.SchematicID || !reflect.DeepEqual(before.Runtime.Extensions, after.Runtime.Extensions) {
			lifecycle = append(lifecycle, name+": OS, kubelet or extensions")
		}
		if !bytes.Equal(before.MachineSpec, after.MachineSpec) || !bytes.Equal(before.ConfigSpec, after.ConfigSpec) || !bytes.Equal(before.EffectiveSpec, after.EffectiveSpec) || !bytes.Equal(before.InfrastructureSpec, after.InfrastructureSpec) || !reflect.DeepEqual(before.Sources, after.Sources) || before.Baseline != after.Baseline || !reflect.DeepEqual(before.Drift, after.Drift) {
			lifecycle = append(lifecycle, name+": intent or baseline")
		}
	}
	if !reflect.DeepEqual(previous.Etcd, current.Etcd) {
		lifecycle = append(lifecycle, managementEtcdDelta(previous.Etcd, current.Etcd)...)
	}
	if len(identity) == 0 && !reflect.DeepEqual(managementSnapshotLifecycleFree(previous), managementSnapshotLifecycleFree(current)) {
		identity = append(identity, "unclassified safety field")
	}
	return identity, lifecycle
}

// sameRecordedCredential compares cluster credential sources the way activation
// validation does: by trust identity when recorded so, otherwise by whole-data digest.
// A source recorded one way and observed the other cannot be compared and differs.
func sameRecordedCredential(a, b configinput.SourceIdentity) bool {
	if a.Name != b.Name || a.Key != b.Key || a.UID != b.UID || (a.Identity == [32]byte{}) != (b.Identity == [32]byte{}) {
		return false
	}
	if a.Identity != [32]byte{} {
		return a.Identity == b.Identity
	}
	return a.Digest == b.Digest
}

// managementEtcdDelta names, per etcd hostname, how membership moved: a node that
// rejoined after its data was wiped keeps its hostname under a new member ID.
func managementEtcdDelta(previous, current []managementEtcdMember) []string {
	before := map[string]uint64{}
	for _, m := range previous {
		before[m.Hostname] = m.MemberID
	}
	after := map[string]uint64{}
	for _, m := range current {
		after[m.Hostname] = m.MemberID
	}
	names := map[string]bool{}
	for name := range before {
		names[name] = true
	}
	for name := range after {
		names[name] = true
	}
	sorted := make([]string, 0, len(names))
	for name := range names {
		sorted = append(sorted, name)
	}
	sort.Strings(sorted)
	var out []string
	for _, name := range sorted {
		if before[name] != after[name] {
			out = append(out, fmt.Sprintf("etcd %s: member %x -> %x", name, before[name], after[name]))
		}
	}
	if len(out) == 0 {
		out = append(out, "etcd membership details")
	}
	return out
}

// managementSnapshotLifecycleFree is the snapshot with every lifecycle field cleared and
// each member's live configuration reduced to its trust identity; what remains must be
// equal for an activated snapshot to be replaced.
func managementSnapshotLifecycleFree(s managementSafety) managementSafety {
	out := managementSafety{Authority: s.Authority, ClusterUID: s.ClusterUID, ClusterName: s.ClusterName, ClusterInputs: s.ClusterInputs}
	for _, input := range s.Inputs {
		input.ResourceVersion = ""
		if input.Identity != [32]byte{} {
			input.Digest = [32]byte{}
		}
		out.Inputs = append(out.Inputs, input)
	}
	members := append([]managementMember(nil), s.Members...)
	sort.Slice(members, func(i, j int) bool { return members[i].Machine.UID < members[j].Machine.UID })
	for _, m := range members {
		trust, _ := managementTalosIdentity(m.Runtime.Configuration)
		out.Members = append(out.Members, managementMember{Machine: m.Machine, TalosConfig: m.TalosConfig, Infrastructure: m.Infrastructure, NodeName: m.NodeName, ProviderID: m.ProviderID, Seed: managementSeedIdentity(m.Seed), Runtime: managementRuntime{Configuration: trust[:]}})
	}
	return out
}

func managementSeedIdentity(s configinput.SourceIdentity) configinput.SourceIdentity {
	s.ResourceVersion = ""
	return s
}

// managementTalosIdentity digests the trust identity a Talos machine configuration
// carries: machine (OS) CA, etcd CA, Kubernetes API server CA and the discovery cluster
// ID and secret. Certificates only, never keys; the digest never leaves this package.
func managementTalosIdentity(data []byte) ([32]byte, bool) {
	provider, err := configloader.NewFromBytes(data)
	if err != nil || provider.Machine() == nil || provider.Cluster() == nil {
		return [32]byte{}, false
	}
	h := sha256.New()
	field := func(b []byte) {
		b = bytes.TrimSpace(b)
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(b)))
		h.Write(n[:])
		h.Write(b)
	}
	if security := provider.Machine().Security(); security != nil {
		if ca := security.IssuingCA(); ca != nil {
			field(ca.Crt)
		}
		for _, ca := range security.AcceptedCAs() {
			if ca != nil {
				field(ca.Crt)
			}
		}
	}
	field([]byte("etcd"))
	if etcd := provider.Cluster().Etcd(); etcd != nil {
		if ca := etcd.CA(); ca != nil {
			field(ca.Crt)
		}
	}
	field([]byte("kubernetes"))
	if k8s := provider.K8sAPIServerCAConfig(); k8s != nil {
		if ca := k8s.IssuingCA(); ca != nil {
			field(ca.Crt)
		}
		for _, ca := range k8s.AcceptedCAs() {
			if ca != nil {
				field(ca.Crt)
			}
		}
	}
	field([]byte("discovery"))
	if discovery := provider.DiscoveryIdentityConfig(); discovery != nil {
		field([]byte(discovery.ClusterID()))
		field([]byte(discovery.ClusterSecret()))
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out, true
}
