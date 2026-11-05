// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// WorkloadAPITimeout bounds every request to a workload cluster API server made by
// management observation, activation validation and the in-place executor. It is set
// once at startup (CABPT --workload-api-timeout) before any controller runs. Workload
// APIs reached through a proxy (an external kubeconfig, a tailnet egress) routinely need
// more than a few seconds per request.
var WorkloadAPITimeout = 15 * time.Second

// ErrWorkloadUnavailable marks a workload API request that failed without telling
// anything about the object asked for (timeout, transport, credentials, API status other
// than NotFound). It never stands for a changed identity: callers wait and retry.
var ErrWorkloadUnavailable = errors.New("workload API unavailable")

// WorkloadRESTConfig parses a workload kubeconfig and applies WorkloadAPITimeout.
func WorkloadRESTConfig(kubeconfig []byte) (*rest.Config, error) {
	cfg, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return nil, err
	}
	cfg.Timeout = WorkloadAPITimeout
	return cfg, nil
}

// WorkloadNodes returns a typed Node client. Unlike a controller-runtime client it needs
// no API discovery, so a Node read is a single request through the workload proxy.
func WorkloadNodes(cfg *rest.Config) (typedcorev1.NodeInterface, error) {
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return clientset.CoreV1().Nodes(), nil
}

// WorkloadFailure classifies a failed workload API request. notFound reports that the
// API answered that the object does not exist; every other failure is unavailability.
// detail is client-side only (timeout, API status reason, transport), never server text.
func WorkloadFailure(err error) (detail string, notFound bool) {
	var netErr net.Error
	switch {
	case apierrors.IsNotFound(err):
		return "not found", true
	case errors.Is(err, context.DeadlineExceeded) || errors.As(err, &netErr) && netErr.Timeout() || apierrors.IsTimeout(err) || apierrors.IsServerTimeout(err):
		return fmt.Sprintf("timed out (limit %s)", WorkloadAPITimeout), false
	case errors.Is(err, context.Canceled):
		return "canceled", false
	}
	var status apierrors.APIStatus
	if errors.As(err, &status) {
		return fmt.Sprintf("API status %d %s", status.Status().Code, apierrors.ReasonForError(err)), false
	}
	return "transport error", false
}

// workloadUnavailable is a StageError carrying ErrWorkloadUnavailable.
func workloadUnavailable(stage, detail string) error {
	err := managementErrorDetail(stage, detail).(*StageError)
	err.cause = ErrWorkloadUnavailable
	return err
}
