// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"time"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
	endpointclient "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/talosclient"
	"github.com/siderolabs/talos/pkg/machinery/api/common"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	machineryclient "github.com/siderolabs/talos/pkg/machinery/client"
	"github.com/siderolabs/talos/pkg/machinery/config/configloader"
	"github.com/siderolabs/talos/pkg/machinery/config/encoder"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func validateManagementAuthority(ctx context.Context, reader client.Reader, a ManagementAuthority) error {
	if reader == nil || a.Namespace == "" || a.Name == "" || a.UID == "" || a.APIVersion == "" || (a.Kind != "TalosConfig" && a.Kind != "TalosControlPlane") {
		return managementError("authority identity")
	}
	if a.Kind == "TalosConfig" && managementGroup(a.APIVersion) != bootstrapv1.GroupVersion.Group || a.Kind == "TalosControlPlane" && managementGroup(a.APIVersion) != "controlplane.cluster.x-k8s.io" {
		return managementError("authority group")
	}
	o := &unstructured.Unstructured{}
	o.SetAPIVersion(a.APIVersion)
	o.SetKind(a.Kind)
	if reader.Get(ctx, client.ObjectKey{Namespace: a.Namespace, Name: a.Name}, o) != nil || o.GetUID() != a.UID || !o.GetDeletionTimestamp().IsZero() {
		return managementError("live authority")
	}
	if managementPaused(o) {
		return ErrManagementPaused
	}
	return nil
}

// ObserveManagement reads the explicit configured inventory independently of
// TCP ownership. reader must be the manager APIReader. It never creates an
// association or mutates the authority, member, bootstrap, identity or Talos.
func ObserveManagement(ctx context.Context, reader client.Reader, request ManagementObservationRequest) (*ManagementObservation, error) {
	if err := validateManagementAuthority(ctx, reader, request.Authority); err != nil {
		return nil, err
	}
	if request.Cluster == nil || request.Spec == nil || len(bootstrapv1.ValidateManagementSpec(field.NewPath("management"), request.Spec)) != 0 {
		return nil, managementError("request")
	}
	a := request.Authority
	cluster := &clusterv1.Cluster{}
	if request.Cluster.Namespace != a.Namespace || reader.Get(ctx, client.ObjectKeyFromObject(request.Cluster), cluster) != nil || cluster.UID != request.Cluster.UID || cluster.UID == "" || !cluster.DeletionTimestamp.IsZero() {
		return nil, managementError("Cluster identity")
	}
	if managementPaused(cluster) || (cluster.Spec.Paused != nil && *cluster.Spec.Paused) {
		return nil, ErrManagementPaused
	}
	authority := &unstructured.Unstructured{}
	authority.SetAPIVersion(a.APIVersion)
	authority.SetKind(a.Kind)
	if reader.Get(ctx, client.ObjectKey{Namespace: a.Namespace, Name: a.Name}, authority) != nil {
		return nil, managementError("authority read")
	}
	raw, found, e := unstructured.NestedMap(authority.Object, "spec", "management")
	live := &bootstrapv1.ManagementSpec{}
	if e != nil || !found || runtime.DefaultUnstructuredConverter.FromUnstructured(raw, live) != nil || !managementSame(live, request.Spec) {
		return nil, managementError("live management intent")
	}
	if len(request.Machines) != len(live.Machines) || len(request.TalosConfigs) != len(live.Machines) || (a.Kind == "TalosConfig" && len(live.Machines) != 1) {
		return nil, managementError("configured inventory")
	}
	// All declared members are preflighted before credentials or external RPCs.
	for _, pin := range live.Machines {
		m := &clusterv1.Machine{}
		if reader.Get(ctx, client.ObjectKey{Namespace: a.Namespace, Name: pin.Name}, m) != nil || m.UID != pin.UID {
			return nil, managementError("Machine preflight")
		}
		if managementPaused(m) {
			return nil, ErrManagementPaused
		}
		tc := &bootstrapv1.TalosConfig{}
		if !m.Spec.Bootstrap.ConfigRef.IsDefined() || reader.Get(ctx, client.ObjectKey{Namespace: a.Namespace, Name: m.Spec.Bootstrap.ConfigRef.Name}, tc) != nil {
			return nil, managementError("bootstrap preflight")
		}
		if managementPaused(tc) {
			return nil, ErrManagementPaused
		}
		infra, e := managementInfrastructure(ctx, reader, m)
		if e != nil {
			return nil, e
		}
		if managementPaused(infra) {
			return nil, ErrManagementPaused
		}
	}
	kubeSecret := &corev1.Secret{}
	if reader.Get(ctx, client.ObjectKey{Namespace: a.Namespace, Name: cluster.Name + "-kubeconfig"}, kubeSecret) != nil || kubeSecret.UID == "" {
		return nil, managementError("workload identity")
	}
	cfg, e := clientcmd.RESTConfigFromKubeConfig(kubeSecret.Data["value"])
	if e != nil {
		return nil, managementError("workload identity parse")
	}
	cfg.Timeout = 5 * time.Second
	workload, e := client.New(cfg, client.Options{})
	if e != nil {
		return nil, managementError("workload client")
	}
	identity := &corev1.Secret{}
	if reader.Get(ctx, client.ObjectKey{Namespace: a.Namespace, Name: cluster.Name + "-talosconfig"}, identity) != nil || identity.UID == "" {
		return nil, managementError("Talos identity")
	}
	if _, e := endpointclient.ValidateIdentity(identity.Data["talosconfig"]); e != nil {
		return nil, managementError("Talos identity validation")
	}
	clusterInputs, e := managementClusterInputs(cluster)
	if e != nil {
		return nil, managementError("Cluster input encoding")
	}
	authoritySpec, e := managementAuthoritySpec(authority)
	if e != nil {
		return nil, e
	}
	o := &ManagementObservation{safety: managementSafety{Authority: a, AuthoritySpec: authoritySpec, ClusterUID: cluster.UID, ClusterName: cluster.Name, ClusterInputs: clusterInputs, Revision: live.InventoryRevision, Inputs: []configinput.SourceIdentity{managementSource(kubeSecret, "value"), managementSource(identity, "talosconfig")}}, summary: ManagementObservationSummary{InventoryVerified: true, MachinesReady: true, EtcdHealthy: a.Kind != "TalosControlPlane"}}
	pinned := append([]bootstrapv1.MachineIdentityReference(nil), live.Machines...)
	sort.Slice(pinned, func(i, j int) bool { return pinned[i].Name < pinned[j].Name })
	nodeIDs := map[types.UID]bool{}
	etcdNodes := map[uint64]managementEtcdMember{}
	var observedLeader uint64
	for _, pin := range pinned {
		var supplied *clusterv1.Machine
		for _, m := range request.Machines {
			if m != nil && m.Name == pin.Name && m.UID == pin.UID {
				supplied = m
			}
		}
		if supplied == nil || supplied.Namespace != a.Namespace {
			return nil, managementError("Machine pin")
		}
		m := &clusterv1.Machine{}
		if reader.Get(ctx, client.ObjectKeyFromObject(supplied), m) != nil || m.UID != pin.UID || m.Spec.ClusterName != cluster.Name || !m.DeletionTimestamp.IsZero() {
			return nil, managementError("live Machine identity")
		}
		if managementPaused(m) {
			return nil, ErrManagementPaused
		}
		owner := metav1.GetControllerOf(m)
		if a.Kind == "TalosConfig" && owner != nil {
			return nil, managementError("standalone Machine controller")
		}
		if a.Kind == "TalosControlPlane" && owner != nil && (owner.Kind != a.Kind || owner.Name != a.Name || owner.UID != a.UID || owner.APIVersion != a.APIVersion) {
			return nil, managementError("foreign Machine controller")
		}
		if !m.Status.NodeRef.IsDefined() || m.Spec.ProviderID == "" || !m.Spec.Bootstrap.ConfigRef.IsDefined() || m.Spec.Bootstrap.DataSecretName == nil || *m.Spec.Bootstrap.DataSecretName == "" {
			return nil, managementError("Machine association incomplete")
		}
		node := &corev1.Node{}
		if err := workload.Get(ctx, client.ObjectKey{Name: m.Status.NodeRef.Name}, node); err != nil {
			return nil, managementError("workload Node lookup")
		}
		if node.UID == "" || nodeIDs[node.UID] || node.Spec.ProviderID != m.Spec.ProviderID || !node.DeletionTimestamp.IsZero() {
			return nil, managementError("workload Node identity")
		}
		nodeIDs[node.UID] = true
		ready := false
		for _, c := range node.Status.Conditions {
			if c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue {
				ready = true
			}
		}
		if !ready || node.Status.NodeInfo.KubeletVersion == "" {
			return nil, managementError("workload Node health")
		}
		if m.Spec.Version != node.Status.NodeInfo.KubeletVersion {
			return nil, managementError("Machine actual Kubernetes version differs")
		}
		tc := &bootstrapv1.TalosConfig{}
		if m.Spec.Bootstrap.ConfigRef.APIGroup != bootstrapv1.GroupVersion.Group || m.Spec.Bootstrap.ConfigRef.Kind != "TalosConfig" || reader.Get(ctx, client.ObjectKey{Namespace: a.Namespace, Name: m.Spec.Bootstrap.ConfigRef.Name}, tc) != nil || !managementController(tc, m) || !tc.DeletionTimestamp.IsZero() {
			return nil, managementError("settled bootstrap owner")
		}
		if managementPaused(tc) {
			return nil, ErrManagementPaused
		}
		expectedTC := false
		for _, t := range request.TalosConfigs {
			if t != nil && t.Namespace == a.Namespace && t.Name == tc.Name && t.UID == tc.UID {
				expectedTC = true
				if !reflect.DeepEqual(t.Spec, tc.Spec) {
					return nil, managementError("requested configuration differs from live")
				}
			}
		}
		if !expectedTC || tc.UID == "" || (a.Kind == "TalosConfig" && (tc.Name != a.Name || tc.UID != a.UID)) || (a.Kind == "TalosControlPlane" && tc.Spec.Management != nil) {
			return nil, managementError("bootstrap authority")
		}
		infra, e := managementInfrastructure(ctx, reader, m)
		if e != nil {
			return nil, e
		}
		if managementPaused(infra) {
			return nil, ErrManagementPaused
		}
		seed := &corev1.Secret{}
		if reader.Get(ctx, client.ObjectKey{Namespace: a.Namespace, Name: *m.Spec.Bootstrap.DataSecretName}, seed) != nil || seed.UID == "" || len(seed.Data["value"]) == 0 {
			return nil, managementError("original bootstrap seed")
		}
		normalized := tc.DeepCopy()
		normalized.Spec.Management = nil
		normalized.Generation = 0
		inputs, e := configinput.Resolve(ctx, reader, normalized)
		if e != nil || !inputs.HasBaseline() {
			return nil, managementError("private baseline resolution")
		}
		if normalized.Spec.TalosEndpoint == nil {
			return nil, managementError("explicit Talos endpoint")
		}
		installer := ""
		if normalized.Spec.ImageFactory != nil {
			if tc.Status.ImageFactory == nil {
				return nil, managementError("reviewed installer resolution missing")
			}
			installer = tc.Status.ImageFactory.InstallerImage
			if installer == "" {
				return nil, managementError("reviewed installer resolution missing")
			}
		}
		rendered, e := configinput.Render(inputs, configinput.RenderContext{Machine: m, Cluster: cluster, InstallerImage: installer})
		if e != nil {
			return nil, managementError("baseline rendering")
		}
		talos, e := endpointclient.New(ctx, reader, client.ObjectKey{Namespace: a.Namespace, Name: cluster.Name}, *normalized.Spec.TalosEndpoint)
		if e != nil {
			return nil, managementError("Talos transport")
		}
		observed, e := endpointclient.Observe(ctx, talos, *normalized.Spec.TalosEndpoint)
		if e != nil {
			talos.Close()
			return nil, managementError("actual Talos observation")
		}
		actual, e := managementConfigBytes(observed.Configuration)
		if e != nil {
			talos.Close()
			return nil, e
		}
		desired, e := managementConfigBytes(rendered.Data())
		if e != nil || !bytes.Equal(actual, desired) {
			talos.Close()
			return nil, managementError("active configuration differs from baseline")
		}
		provider, e := configloader.NewFromBytes(actual)
		if e != nil || provider.Machine() == nil {
			talos.Close()
			return nil, managementError("actual role")
		}
		_, cpRole := m.Labels[clusterv1.MachineControlPlaneLabel]
		if cpRole != (provider.Machine().Type().String() == "controlplane") || cpRole != (a.Kind == "TalosControlPlane") {
			talos.Close()
			return nil, managementError("role association")
		}
		ca, e := endpointclient.ValidateIdentity(identity.Data["talosconfig"])
		if e != nil {
			talos.Close()
			return nil, managementError("Talos identity")
		}
		trusted := false
		for _, accepted := range provider.Machine().Security().AcceptedCAs() {
			if accepted != nil && bytes.Equal(bytes.TrimSpace(accepted.Crt), bytes.TrimSpace(ca)) {
				trusted = true
			}
		}
		if issuing := provider.Machine().Security().IssuingCA(); issuing != nil && bytes.Equal(bytes.TrimSpace(issuing.Crt), bytes.TrimSpace(ca)) {
			trusted = true
		}
		if !trusted {
			talos.Close()
			return nil, managementError("active Talos trust identity")
		}
		if observed.KubeletImage != "" && managementImageTag(observed.KubeletImage) != strings.TrimPrefix(node.Status.NodeInfo.KubeletVersion, "v") {
			talos.Close()
			return nil, managementError("actual kubelet version")
		}
		if node.Status.NodeInfo.BootID != "" && node.Status.NodeInfo.BootID != observed.BootID {
			talos.Close()
			return nil, managementError("Node boot identity")
		}
		if a.Kind == "TalosControlPlane" {
			view, self, e := managementEtcd(ctx, talos, *normalized.Spec.TalosEndpoint, m, node)
			if e != nil {
				talos.Close()
				return nil, e
			}
			if o.safety.Etcd == nil {
				o.safety.Etcd = view
			} else if !managementSame(o.safety.Etcd, view) {
				talos.Close()
				return nil, managementError("inconsistent etcd membership")
			}
			if observedLeader != 0 && observedLeader != self.LeaderID {
				talos.Close()
				return nil, managementError("inconsistent etcd leadership")
			}
			observedLeader = self.LeaderID
			if _, exists := etcdNodes[self.MemberID]; exists {
				talos.Close()
				return nil, managementError("duplicate etcd association")
			}
			etcdNodes[self.MemberID] = self
		}
		talos.Close()
		ms, _ := json.Marshal(m.Spec)
		cs, _ := json.Marshal(normalized.Spec)
		is, _, e := unstructured.NestedMap(infra.Object, "spec")
		if e != nil {
			return nil, managementError("InfraMachine spec")
		}
		ib, e := json.Marshal(is)
		if e != nil {
			return nil, managementError("InfraMachine encoding")
		}
		o.safety.Members = append(o.safety.Members, managementMember{Machine: managementObject(clusterv1.GroupVersion.Group, "Machine", m), TalosConfig: managementObject(bootstrapv1.GroupVersion.Group, "TalosConfig", tc), Infrastructure: managementObject(m.Spec.InfrastructureRef.APIGroup, m.Spec.InfrastructureRef.Kind, infra), NodeName: node.Name, NodeUID: node.UID, ProviderID: node.Spec.ProviderID, MachineSpec: ms, ConfigSpec: cs, InfrastructureSpec: ib, Sources: inputs.Sources(), Seed: managementSource(seed, "value"), Runtime: managementRuntime{Configuration: actual, BootID: observed.BootID, OSVersion: observed.OSVersion, KubeletImage: observed.KubeletImage, SchematicID: observed.SchematicID, Extensions: observed.Extensions}})
	}
	if a.Kind == "TalosControlPlane" {
		for i := range o.safety.Etcd {
			v, ok := etcdNodes[o.safety.Etcd[i].MemberID]
			if !ok || o.safety.Etcd[i].IsLearner {
				return nil, managementError("unaccounted etcd member")
			}
			o.safety.Etcd[i].MachineUID = v.MachineUID
			o.safety.Etcd[i].NodeUID = v.NodeUID
		}
		if len(etcdNodes) != len(o.safety.Etcd) {
			return nil, managementError("unaccounted control-plane Node")
		}
		nodes := &corev1.NodeList{}
		if workload.List(ctx, nodes, client.MatchingLabels{"node-role.kubernetes.io/control-plane": ""}) != nil {
			return nil, managementError("complete control-plane Node inventory")
		}
		for _, n := range nodes.Items {
			if !nodeIDs[n.UID] {
				return nil, managementError("unconfigured control-plane Node")
			}
		}
		hc, e := rest.HTTPClientFor(cfg)
		if e != nil {
			return nil, managementError("API health client")
		}
		req, e := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(cfg.Host, "/")+"/readyz", nil)
		if e != nil {
			return nil, managementError("API health request")
		}
		response, e := hc.Do(req)
		if e != nil {
			return nil, managementError("API health")
		}
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return nil, managementError("API health")
		}
		o.summary.EtcdHealthy = true
	}
	if err := validateManagementAuthority(ctx, reader, a); err != nil {
		return nil, err
	}
	liveAuthority := &unstructured.Unstructured{}
	liveAuthority.SetAPIVersion(a.APIVersion)
	liveAuthority.SetKind(a.Kind)
	if reader.Get(ctx, client.ObjectKey{Namespace: a.Namespace, Name: a.Name}, liveAuthority) != nil {
		return nil, managementError("authority changed during observation")
	}
	if current, err := managementAuthoritySpec(liveAuthority); err != nil || !bytes.Equal(current, o.safety.AuthoritySpec) {
		return nil, managementError("authority policy changed during observation")
	}
	liveCluster := &clusterv1.Cluster{}
	if reader.Get(ctx, client.ObjectKeyFromObject(cluster), liveCluster) != nil || liveCluster.UID != cluster.UID {
		return nil, managementError("Cluster changed during observation")
	}
	if managementPaused(liveCluster) || liveCluster.Spec.Paused != nil && *liveCluster.Spec.Paused {
		return nil, ErrManagementPaused
	}
	if current, err := managementClusterInputs(liveCluster); err != nil || !bytes.Equal(current, o.safety.ClusterInputs) {
		return nil, managementError("Cluster inputs changed during observation")
	}
	for _, source := range o.safety.Inputs {
		secret := &corev1.Secret{}
		if reader.Get(ctx, client.ObjectKey{Namespace: a.Namespace, Name: source.Name}, secret) != nil || !managementSame(source, managementSource(secret, source.Key)) {
			return nil, managementError("source changed during observation")
		}
	}
	for _, member := range o.safety.Members {
		for _, source := range append(append([]configinput.SourceIdentity(nil), member.Sources...), member.Seed) {
			secret := &corev1.Secret{}
			if reader.Get(ctx, client.ObjectKey{Namespace: a.Namespace, Name: source.Name}, secret) != nil || !managementSame(source, managementSource(secret, source.Key)) {
				return nil, managementError("member source changed during observation")
			}
		}
		m := &clusterv1.Machine{}
		if reader.Get(ctx, client.ObjectKey{Namespace: a.Namespace, Name: member.Machine.Name}, m) != nil || m.UID != member.Machine.UID {
			return nil, managementError("Machine changed during observation")
		}
		if managementPaused(m) {
			return nil, ErrManagementPaused
		}
		currentMachine, _ := json.Marshal(m.Spec)
		if !bytes.Equal(currentMachine, member.MachineSpec) {
			return nil, managementError("Machine intent changed during observation")
		}
		infra, err := managementInfrastructure(ctx, reader, m)
		if err != nil || infra.GetUID() != member.Infrastructure.UID {
			return nil, managementError("infrastructure changed during observation")
		}
		if managementPaused(infra) {
			return nil, ErrManagementPaused
		}
		infraSpec, _, _ := unstructured.NestedMap(infra.Object, "spec")
		currentInfra, _ := json.Marshal(infraSpec)
		if !bytes.Equal(currentInfra, member.InfrastructureSpec) {
			return nil, managementError("infrastructure intent changed during observation")
		}
		tc := &bootstrapv1.TalosConfig{}
		if reader.Get(ctx, client.ObjectKey{Namespace: a.Namespace, Name: member.TalosConfig.Name}, tc) != nil || tc.UID != member.TalosConfig.UID || !managementController(tc, m) {
			return nil, managementError("bootstrap changed during observation")
		}
		if managementPaused(tc) {
			return nil, ErrManagementPaused
		}
		tc.Spec.Management = nil
		current, _ := json.Marshal(tc.Spec)
		if !bytes.Equal(current, member.ConfigSpec) {
			return nil, managementError("configuration changed during observation")
		}
	}
	return o, nil
}

func managementInfrastructure(ctx context.Context, reader client.Reader, m *clusterv1.Machine) (*unstructured.Unstructured, error) {
	crds := &unstructured.UnstructuredList{}
	crds.SetAPIVersion("apiextensions.k8s.io/v1")
	crds.SetKind("CustomResourceDefinitionList")
	if reader.List(ctx, crds) != nil {
		return nil, managementError("infrastructure contract discovery")
	}
	version := ""
	for _, crd := range crds.Items {
		group, _, _ := unstructured.NestedString(crd.Object, "spec", "group")
		kind, _, _ := unstructured.NestedString(crd.Object, "spec", "names", "kind")
		if group == m.Spec.InfrastructureRef.APIGroup && kind == m.Spec.InfrastructureRef.Kind {
			versions, _, _ := unstructured.NestedSlice(crd.Object, "spec", "versions")
			for _, v := range versions {
				x, ok := v.(map[string]any)
				if ok && x["served"] == true && x["storage"] == true {
					version, _ = x["name"].(string)
				}
			}
		}
	}
	if version == "" {
		return nil, managementError("infrastructure contract version")
	}
	infra := &unstructured.Unstructured{}
	infra.SetAPIVersion(m.Spec.InfrastructureRef.APIGroup + "/" + version)
	infra.SetKind(m.Spec.InfrastructureRef.Kind)
	if reader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: m.Spec.InfrastructureRef.Name}, infra) != nil || infra.GetUID() == "" || !managementController(infra, m) || !infra.GetDeletionTimestamp().IsZero() {
		return nil, managementError("settled infrastructure owner")
	}
	return infra, nil
}
func managementConfigBytes(data []byte) ([]byte, error) {
	c, e := configloader.NewFromBytes(data)
	if e != nil {
		return nil, managementError("active configuration parse")
	}
	b, e := c.EncodeBytes(encoder.WithComments(encoder.CommentsDisabled))
	if e != nil {
		return nil, managementError("active configuration encode")
	}
	return b, nil
}

func managementEtcd(ctx context.Context, c *machineryclient.Client, endpoint bootstrapv1.TalosEndpoint, m *clusterv1.Machine, n *corev1.Node) ([]managementEtcdMember, managementEtcdMember, error) {
	ctx = endpointclient.WithNode(ctx, endpoint)
	members, e := c.EtcdMemberList(ctx, &machineapi.EtcdMemberListRequest{})
	if e != nil || len(members.GetMessages()) != 1 || members.Messages[0] == nil || managementMetadataFailed(members.Messages[0].Metadata) {
		return nil, managementEtcdMember{}, managementError("etcd member observation")
	}
	status, e := c.EtcdStatus(ctx)
	if e != nil || len(status.GetMessages()) != 1 || status.Messages[0] == nil || managementMetadataFailed(status.Messages[0].Metadata) || status.Messages[0].MemberStatus == nil {
		return nil, managementEtcdMember{}, managementError("etcd status observation")
	}
	state := status.Messages[0].MemberStatus
	if state.MemberId == 0 || state.Leader == 0 || state.IsLearner || len(state.Errors) != 0 || state.RaftTerm == 0 || state.RaftAppliedIndex == 0 {
		return nil, managementEtcdMember{}, managementError("etcd health")
	}
	services, e := c.ServiceInfo(ctx, "etcd")
	if e != nil || len(services) != 1 || managementMetadataFailed(services[0].Metadata) || services[0].Service == nil || services[0].Service.Health == nil || !services[0].Service.Health.Healthy || services[0].Service.Health.Unknown {
		return nil, managementEtcdMember{}, managementError("etcd service health")
	}
	ids := map[uint64]bool{}
	names := map[string]bool{}
	view := []managementEtcdMember{}
	self := managementEtcdMember{}
	leader := false
	for _, member := range members.Messages[0].Members {
		if member == nil || member.Id == 0 || member.Hostname == "" || ids[member.Id] || names[member.Hostname] || len(member.PeerUrls) == 0 || len(member.ClientUrls) == 0 {
			return nil, managementEtcdMember{}, managementError("etcd member identity")
		}
		ids[member.Id] = true
		names[member.Hostname] = true
		v := managementEtcdMember{MemberID: member.Id, Hostname: member.Hostname, PeerURLs: append([]string(nil), member.PeerUrls...), ClientURLs: append([]string(nil), member.ClientUrls...), IsLearner: member.IsLearner}
		sort.Strings(v.PeerURLs)
		sort.Strings(v.ClientURLs)
		if member.Id == state.Leader {
			leader = true
		}
		if member.Id == state.MemberId {
			hostname := strings.SplitN(n.Name, ".", 2)[0]
			if member.Hostname != n.Name && member.Hostname != hostname {
				return nil, managementEtcdMember{}, managementError("etcd Node association")
			}
			self = v
			self.LeaderID = state.Leader
			self.MachineUID = m.UID
			self.NodeUID = n.UID
		}
		view = append(view, v)
	}
	if self.MemberID == 0 || !leader {
		return nil, managementEtcdMember{}, managementError("etcd complete association")
	}
	sort.Slice(view, func(i, j int) bool { return view[i].MemberID < view[j].MemberID })
	return view, self, nil
}

func managementMetadataFailed(metadata *common.Metadata) bool {
	return metadata != nil && (metadata.GetError() != "" || metadata.GetStatus().GetCode() != 0)
}

func managementImageTag(image string) string {
	image = strings.SplitN(image, "@", 2)[0]
	colon := strings.LastIndex(image, ":")
	if colon < 0 || strings.Contains(image[colon:], "/") {
		return ""
	}
	return strings.TrimPrefix(image[colon+1:], "v")
}

func managementClusterInputs(cluster *clusterv1.Cluster) ([]byte, error) {
	return json.Marshal(struct {
		Name     string
		Network  clusterv1.ClusterNetwork
		Endpoint clusterv1.APIEndpoint
	}{cluster.Name, cluster.Spec.ClusterNetwork, cluster.Spec.ControlPlaneEndpoint})
}

func managementAuthoritySpec(authority *unstructured.Unstructured) ([]byte, error) {
	spec, found, err := unstructured.NestedMap(authority.Object, "spec")
	if err != nil || !found {
		return nil, managementError("authority spec")
	}
	if management, ok := spec["management"].(map[string]any); ok {
		delete(management, "mode")
		delete(management, "approval")
	}
	encoded, err := json.Marshal(spec)
	if err != nil {
		return nil, managementError("authority spec encoding")
	}
	return encoded, nil
}
