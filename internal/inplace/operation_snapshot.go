// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package inplace

import (
	"bytes"
	"context"
	"fmt"
	"github.com/Masterminds/semver/v3"
	"github.com/google/uuid"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"reflect"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strings"
)

type operationSnapshot struct {
	machine  *clusterv1.Machine
	config   *bootstrapv1.TalosConfig
	cluster  *clusterv1.Cluster
	infra    *unstructured.Unstructured
	output   *corev1.Secret
	resolved *configinput.Resolved
	endpoint bootstrapv1.TalosEndpoint
}

func objectIdentity(group, kind string, obj client.Object) op.ObjectIdentity {
	return op.ObjectIdentity{APIGroup: group, Kind: kind, Name: obj.GetName(), UID: obj.GetUID(), OwnerReferences: append([]metav1.OwnerReference(nil), obj.GetOwnerReferences()...)}
}
func (h *Handler) snapshot(ctx context.Context, request *clusterv1.Machine, raw runtimeRawExtension) (*operationSnapshot, bool, error) {
	_, fresh, err := h.desiredConfig(ctx, request, raw)
	if err != nil || !fresh {
		return nil, fresh, err
	}
	s := &operationSnapshot{machine: &clusterv1.Machine{}, config: &bootstrapv1.TalosConfig{}, cluster: &clusterv1.Cluster{}, output: &corev1.Secret{}, infra: &unstructured.Unstructured{}}
	if h.reader.Get(ctx, client.ObjectKeyFromObject(request), s.machine) != nil {
		return nil, false, fmt.Errorf("operation Machine lookup failed")
	}
	m := s.machine
	if h.reader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: m.Spec.Bootstrap.ConfigRef.Name}, s.config) != nil || h.reader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: m.Spec.ClusterName}, s.cluster) != nil {
		return nil, false, fmt.Errorf("operation owner lookup failed")
	}
	ref := m.Spec.InfrastructureRef
	version, err := contractVersion(ctx, h.reader, ref.APIGroup, ref.Kind)
	if err != nil {
		return nil, false, err
	}
	s.infra.SetAPIVersion(ref.APIGroup + "/" + version)
	s.infra.SetKind(ref.Kind)
	if h.reader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: ref.Name}, s.infra) != nil || s.infra.GetUID() == "" {
		return nil, false, fmt.Errorf("operation InfraMachine identity unavailable")
	}
	if h.reader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: s.config.Status.DataSecretName}, s.output) != nil {
		return nil, false, fmt.Errorf("operation rendered output lookup failed")
	}
	s.resolved, err = configinput.Resolve(ctx, h.reader, s.config)
	if err != nil {
		return nil, false, err
	}
	image := ""
	if s.config.Status.ImageFactory != nil {
		image = s.config.Status.ImageFactory.InstallerImage
	}
	expected, err := s.resolved.Freshness(configinput.RenderContext{Machine: m, Cluster: s.cluster, InstallerImage: image})
	if err != nil {
		return nil, false, err
	}
	if !bytes.Equal(expected, s.output.Data[configinput.FreshnessKey]) {
		return nil, false, nil
	}
	s.endpoint, err = h.machineEndpoints(ctx, m)
	return s, true, err
}
func contractVersion(ctx context.Context, reader client.Reader, group, kind string) (string, error) {
	list := &apiextensionsv1.CustomResourceDefinitionList{}
	if reader.List(ctx, list) != nil {
		return "", fmt.Errorf("operation infrastructure contract lookup failed")
	}
	for _, crd := range list.Items {
		if crd.Spec.Group == group && crd.Spec.Names.Kind == kind {
			for _, v := range crd.Spec.Versions {
				if v.Storage && v.Served {
					return v.Name, nil
				}
			}
		}
	}
	return "", fmt.Errorf("operation infrastructure contract unavailable")
}
func (h *Handler) plan(ctx context.Context, s *operationSnapshot, observed NodeObservation) (*op.Journal, error) {
	m := s.machine
	c := s.config
	accepted, err := op.DecodeAcceptedIntent(s.output.Data[op.AcceptedIntentKey])
	if err != nil || accepted.MachineUID != m.UID || accepted.TalosConfigUID != c.UID {
		return nil, fmt.Errorf("operation accepted image intent provenance unavailable")
	}
	image, err := op.ConfigurationImage(s.output.Data["value"], c.Spec)
	if err != nil {
		return nil, err
	}
	node, err := h.workloadNode(ctx, s.cluster, m.Status.NodeRef.Name)
	if err != nil {
		return nil, err
	}
	if m.Spec.ProviderID == "" || node.Spec.ProviderID != m.Spec.ProviderID || node.UID == "" {
		return nil, fmt.Errorf("operation workload Node identity mismatch")
	}
	j := &op.Journal{Version: op.SchemaVersion, TransactionID: uuid.NewString(), Machine: objectIdentity(clusterv1.GroupVersion.Group, "Machine", m), TalosConfig: objectIdentity(bootstrapv1.GroupVersion.Group, "TalosConfig", c), Infrastructure: objectIdentity(m.Spec.InfrastructureRef.APIGroup, m.Spec.InfrastructureRef.Kind, s.infra), Inputs: s.resolved.Sources(), InputFreshness: bytes.Clone(s.output.Data[configinput.FreshnessKey]), Configuration: bytes.Clone(s.output.Data["value"]), Phase: op.Planned, PriorAcceptedImageIntent: accepted.Image, TargetImage: image, TargetKubernetesVersion: m.Spec.Version, Endpoint: s.endpoint, NodeName: node.Name, NodeUID: node.UID, NodeProviderID: node.Spec.ProviderID, Steps: []op.OperationStep{{Kind: op.Apply, Phase: op.StepPlanned, ApplyOutcome: op.Unknown}}}
	if image.Reference != accepted.Image.Reference || !reflect.DeepEqual(image.Extensions, accepted.Image.Extensions) {
		if err := validateVersionTransition(observed.OSVersion, image.Reference, node.Status.NodeInfo.KubeletVersion, m.Spec.Version); err != nil {
			return nil, err
		}
		j.Steps = append(j.Steps, op.OperationStep{Kind: op.PullInstaller, Phase: op.StepPlanned}, op.OperationStep{Kind: op.Install, Phase: op.StepPlanned}, op.OperationStep{Kind: op.Reboot, Phase: op.StepPlanned})
	}
	if name := c.Annotations[op.OwnerPlanNameAnnotation]; name != "" {
		ref := op.OwnerPlanReference{Name: name, UID: types.UID(c.Annotations[op.OwnerPlanUIDAnnotation])}
		intent, err := op.ReadOwnerIntent(ctx, h.reader, m.Namespace, ref)
		if err != nil {
			return nil, err
		}
		if !reflect.DeepEqual(intent.Machine, j.Machine) || !reflect.DeepEqual(intent.TalosConfig, j.TalosConfig) || !reflect.DeepEqual(intent.Infrastructure, j.Infrastructure) || !reflect.DeepEqual(intent.Inputs, j.Inputs) || !bytes.Equal(intent.InputFreshness, j.InputFreshness) {
			return nil, fmt.Errorf("operation released owner plan identity mismatch")
		}
		j.TransactionID = intent.TransactionID
		j.OwnerPlan = &ref
		j.ControlPlane = intent.ControlPlane
	}
	if _, cp := m.Labels[clusterv1.MachineControlPlaneLabel]; cp {
		if len(j.ControlPlane) == 0 {
			return nil, fmt.Errorf("operation complete expected controlplane inventory required")
		}
		view, err := h.controlPlaneObservation(ctx, s.cluster, j)
		if err != nil {
			return nil, err
		}
		j.OriginalEtcdMembers = view
	}
	return j, nil
}
func (h *Handler) guard(ctx context.Context, s *operationSnapshot, j *op.Journal) error {
	if !reflect.DeepEqual(objectIdentity(clusterv1.GroupVersion.Group, "Machine", s.machine), j.Machine) || !reflect.DeepEqual(objectIdentity(bootstrapv1.GroupVersion.Group, "TalosConfig", s.config), j.TalosConfig) || !reflect.DeepEqual(objectIdentity(s.machine.Spec.InfrastructureRef.APIGroup, s.machine.Spec.InfrastructureRef.Kind, s.infra), j.Infrastructure) || !bytes.Equal(s.output.Data[configinput.FreshnessKey], j.InputFreshness) || !bytes.Equal(s.output.Data["value"], j.Configuration) || !reflect.DeepEqual(s.resolved.Sources(), j.Inputs) || s.endpoint != j.Endpoint {
		return fmt.Errorf("operation frozen identities or sources changed")
	}
	node, err := h.workloadNode(ctx, s.cluster, j.NodeName)
	if err != nil {
		return err
	}
	if node.UID != j.NodeUID || node.Spec.ProviderID != j.NodeProviderID {
		return fmt.Errorf("operation preserved Node replaced")
	}
	return nil
}
func validateVersionTransition(running, image, currentKube, targetKube string) error {
	runningV, e := semver.StrictNewVersion(strings.TrimPrefix(running, "v"))
	if e != nil {
		return fmt.Errorf("operation running OS version unknown")
	}
	if runningV.LessThan(semver.MustParse("1.13.0")) {
		return fmt.Errorf("operation legacy upgrade completion witness requires compatibility review")
	}
	desiredV, e := semver.StrictNewVersion(strings.TrimPrefix(imageTag(image), "v"))
	if e != nil {
		return fmt.Errorf("operation target installer version unknown")
	}
	if desiredV.LessThan(runningV) || desiredV.Major() != runningV.Major() || desiredV.Minor() > runningV.Minor()+1 {
		return fmt.Errorf("operation OS downgrade or skew not approved")
	}
	return validateKubeTransition(currentKube, targetKube)
}
func validateKubeTransition(current, target string) error {
	a, e := semver.StrictNewVersion(strings.TrimPrefix(current, "v"))
	if e != nil {
		return fmt.Errorf("operation current kubelet version unknown")
	}
	b, e := semver.StrictNewVersion(strings.TrimPrefix(target, "v"))
	if e != nil {
		return fmt.Errorf("operation target kubelet version unknown")
	}
	if b.LessThan(a) || a.Major() != b.Major() || b.Minor() > a.Minor()+1 {
		return fmt.Errorf("operation Kubernetes downgrade or skew not approved")
	}
	return nil
}
