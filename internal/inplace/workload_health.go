// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package inplace

import (
	"context"
	"fmt"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	endpointclient "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/talosclient"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"net/http"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sort"
	"strings"
	"time"
)

func (h *Handler) workloadClient(ctx context.Context, cluster *clusterv1.Cluster) (client.Client, *rest.Config, error) {
	s := &corev1.Secret{}
	if h.reader.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: cluster.Name + "-kubeconfig"}, s) != nil {
		return nil, nil, fmt.Errorf("operation workload credentials unavailable")
	}
	cfg, e := clientcmd.RESTConfigFromKubeConfig(s.Data["value"])
	if e != nil {
		return nil, nil, fmt.Errorf("operation workload credentials invalid")
	}
	cfg.Timeout = 5 * time.Second
	c, e := client.New(cfg, client.Options{})
	if e != nil {
		return nil, nil, fmt.Errorf("operation workload client failed")
	}
	return c, cfg, nil
}
func (h *Handler) workloadNode(ctx context.Context, cluster *clusterv1.Cluster, name string) (*corev1.Node, error) {
	c, _, e := h.workloadClient(ctx, cluster)
	if e != nil {
		return nil, e
	}
	n := &corev1.Node{}
	if c.Get(ctx, client.ObjectKey{Name: name}, n) != nil {
		return nil, fmt.Errorf("operation workload Node observation failed")
	}
	return n, nil
}
func (h *Handler) controlPlaneObservation(ctx context.Context, cluster *clusterv1.Cluster, j *op.Journal) ([]op.EtcdMemberIdentity, error) {
	if len(j.ControlPlane) == 0 {
		return nil, fmt.Errorf("operation expected controlplane inventory absent")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	list := &clusterv1.MachineList{}
	if h.reader.List(ctx, list, client.InNamespace(cluster.Namespace), client.MatchingLabels{clusterv1.ClusterNameLabel: cluster.Name, clusterv1.MachineControlPlaneLabel: ""}) != nil {
		return nil, fmt.Errorf("operation controlplane inventory lookup failed")
	}
	if len(list.Items) != len(j.ControlPlane) {
		return nil, fmt.Errorf("operation controlplane inventory incomplete")
	}
	expected := make([]string, 0, len(list.Items))
	var view []op.EtcdMemberIdentity
	var leader uint64
	ids := map[uint64]bool{}
	for _, m := range list.Items {
		found := false
		for _, pin := range j.ControlPlane {
			if pin.UID == m.UID && pin.Name == m.Name && pin.Kind == "Machine" && pin.APIGroup == clusterv1.GroupVersion.Group {
				found = true
			}
		}
		if !found || !m.DeletionTimestamp.IsZero() || !m.Status.NodeRef.IsDefined() {
			return nil, fmt.Errorf("operation controlplane member identity changed")
		}
		name := strings.SplitN(m.Status.NodeRef.Name, ".", 2)[0]
		expected = append(expected, strings.ToLower(name))
		cfg := &bootstrapv1.TalosConfig{}
		if h.reader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: m.Spec.Bootstrap.ConfigRef.Name}, cfg) != nil || cfg.Spec.TalosEndpoint == nil {
			return nil, fmt.Errorf("operation explicit controlplane endpoint unavailable")
		}
		c, err := endpointclient.New(ctx, h.reader, client.ObjectKey{Namespace: m.Namespace, Name: cluster.Name}, *cfg.Spec.TalosEndpoint)
		if err != nil {
			return nil, err
		}
		rpc := endpointclient.WithNode(ctx, *cfg.Spec.TalosEndpoint)
		services, serviceErr := c.ServiceInfo(rpc, "etcd")
		members, memberErr := c.EtcdMemberList(rpc, &machineapi.EtcdMemberListRequest{})
		status, statusErr := c.EtcdStatus(rpc)
		c.Close()
		if serviceErr != nil || len(services) != 1 || operationMetadataFailed(services[0].Metadata) || services[0].Service == nil || services[0].Service.Health == nil || !services[0].Service.Health.Healthy || services[0].Service.Health.Unknown {
			return nil, fmt.Errorf("operation etcd health failed")
		}
		if memberErr != nil || len(members.GetMessages()) != 1 || operationMetadataFailed(members.Messages[0].Metadata) || statusErr != nil || len(status.GetMessages()) != 1 || operationMetadataFailed(status.Messages[0].Metadata) || status.Messages[0].MemberStatus == nil {
			return nil, fmt.Errorf("operation etcd runtime observation failed")
		}
		state := status.Messages[0].MemberStatus
		if state.MemberId == 0 || state.Leader == 0 || state.RaftTerm == 0 || state.RaftAppliedIndex == 0 || state.IsLearner || len(state.Errors) > 0 || ids[state.MemberId] {
			return nil, fmt.Errorf("operation etcd member unhealthy or duplicated")
		}
		if leader != 0 && leader != state.Leader {
			return nil, fmt.Errorf("operation inconsistent etcd leadership")
		}
		leader = state.Leader
		ids[state.MemberId] = true
		current, err := checkedEtcdView(members.Messages[0].Members, name, state)
		if err != nil {
			return nil, err
		}
		if view == nil {
			view = current
		} else if !sameEtcdView(view, current) {
			return nil, fmt.Errorf("operation inconsistent etcd member IDs or URLs")
		}
	}
	if !ids[leader] || len(view) != len(expected) {
		return nil, fmt.Errorf("operation leader or member outside expected quorum")
	}
	sort.Strings(expected)
	actual := make([]string, len(view))
	for i, member := range view {
		actual[i] = member.Hostname
	}
	sort.Strings(actual)
	if strings.Join(expected, "\x00") != strings.Join(actual, "\x00") {
		return nil, fmt.Errorf("operation etcd membership differs from expected inventory")
	}
	_, cfg, err := h.workloadClient(ctx, cluster)
	if err != nil {
		return nil, err
	}
	httpClient, err := rest.HTTPClientFor(cfg)
	if err != nil {
		return nil, fmt.Errorf("operation API transport failed")
	}
	req, err := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(cfg.Host, "/")+"/readyz", nil)
	if err != nil {
		return nil, fmt.Errorf("operation API readiness request failed")
	}
	response, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("operation API not reachable")
	}
	response.Body.Close()
	if response.StatusCode != 200 {
		return nil, fmt.Errorf("operation Kubernetes API not ready")
	}
	return view, nil
}

func (h *Handler) controlPlaneHealth(ctx context.Context, cluster *clusterv1.Cluster, j *op.Journal) error {
	if len(j.OriginalEtcdMembers) == 0 {
		return fmt.Errorf("operation original etcd membership unavailable")
	}
	view, err := h.controlPlaneObservation(ctx, cluster, j)
	if err != nil {
		return err
	}
	if !sameEtcdView(j.OriginalEtcdMembers, view) {
		return fmt.Errorf("operation original etcd member identity changed")
	}
	return nil
}
