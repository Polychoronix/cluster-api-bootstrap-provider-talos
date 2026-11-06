// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package inplace

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	common "github.com/siderolabs/talos/pkg/machinery/api/common"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/emptypb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"
	"time"
)

type cpHealthServer struct {
	machineapi.UnimplementedMachineServiceServer
	unhealthy bool
	badLeader bool
	members   []*machineapi.EtcdMember
}

func (s *cpHealthServer) ServiceList(context.Context, *emptypb.Empty) (*machineapi.ServiceListResponse, error) {
	return &machineapi.ServiceListResponse{Messages: []*machineapi.ServiceList{{Services: []*machineapi.ServiceInfo{{Id: "etcd", Health: &machineapi.ServiceHealth{Healthy: !s.unhealthy}}}}}}, nil
}
func (s *cpHealthServer) EtcdMemberList(context.Context, *machineapi.EtcdMemberListRequest) (*machineapi.EtcdMemberListResponse, error) {
	return &machineapi.EtcdMemberListResponse{Messages: []*machineapi.EtcdMembers{{Metadata: &common.Metadata{}, Members: s.members}}}, nil
}
func (s *cpHealthServer) EtcdStatus(context.Context, *emptypb.Empty) (*machineapi.EtcdStatusResponse, error) {
	leader := uint64(1)
	if s.badLeader {
		leader = 9
	}
	return &machineapi.EtcdStatusResponse{Messages: []*machineapi.EtcdStatus{{MemberStatus: &machineapi.EtcdMemberStatus{MemberId: 1, Leader: leader, RaftTerm: 2, RaftAppliedIndex: 8}}}}, nil
}
func cpCert(t *testing.T, template, parent *x509.Certificate, pub any, key any) ([]byte, *x509.Certificate) {
	t.Helper()
	der, err := x509.CreateCertificate(rand.Reader, template, parent, pub, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), cert
}
func TestControlPlaneReceiptGateRequiresRealEtcdAndAPIHealth(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	now := time.Now()
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caPEM, ca := cpCert(t, caTemplate, caTemplate, &key.PublicKey, key)
	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	serverPEM, _ := cpCert(t, &x509.Certificate{SerialNumber: big.NewInt(2), NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), DNSNames: []string{"talos.test"}, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}, ca, &serverKey.PublicKey, key)
	serverPair, err := tls.X509KeyPair(serverPEM, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(serverKey)}))
	require.NoError(t, err)
	clientKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	clientPEM, _ := cpCert(t, &x509.Certificate{SerialNumber: big.NewInt(3), NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}, ca, &clientKey.PublicKey, key)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(caPEM)
	backend := &cpHealthServer{members: []*machineapi.EtcdMember{{Id: 1, Hostname: "node"}}}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{serverPair}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert})))
	machineapi.RegisterMachineServiceServer(server, backend)
	go server.Serve(listener)
	t.Cleanup(func() { server.Stop(); listener.Close() })
	apiReady := true
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if apiReady {
			w.WriteHeader(200)
		} else {
			w.WriteHeader(503)
		}
	}))
	t.Cleanup(api.Close)
	kube := clientcmdapi.NewConfig()
	kube.Clusters["test"] = &clientcmdapi.Cluster{Server: api.URL}
	kube.AuthInfos["test"] = &clientcmdapi.AuthInfo{}
	kube.Contexts["test"] = &clientcmdapi.Context{Cluster: "test", AuthInfo: "test"}
	kube.CurrentContext = "test"
	kubeData, err := clientcmd.Write(*kube)
	require.NoError(t, err)
	identity := []byte("context: test\ncontexts:\n  test:\n    ca: " + base64.StdEncoding.EncodeToString(caPEM) + "\n    crt: " + base64.StdEncoding.EncodeToString(clientPEM) + "\n    key: " + base64.StdEncoding.EncodeToString(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(clientKey)})) + "\n")
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, clusterv1.AddToScheme(scheme))
	require.NoError(t, bootstrapv1.AddToScheme(scheme))
	cluster := &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: "cluster", UID: "cluster-uid"}}
	machine := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: "machine", UID: "machine-uid", Labels: map[string]string{clusterv1.ClusterNameLabel: "cluster", clusterv1.MachineControlPlaneLabel: ""}}, Spec: clusterv1.MachineSpec{Bootstrap: clusterv1.Bootstrap{ConfigRef: clusterv1.ContractVersionedObjectReference{Name: "config"}}}, Status: clusterv1.MachineStatus{NodeRef: clusterv1.MachineNodeReference{Name: "node"}}}
	configObject := &bootstrapv1.TalosConfig{ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: "config"}, Spec: bootstrapv1.TalosConfigSpec{TalosEndpoint: &bootstrapv1.TalosEndpoint{Address: listener.Addr().String(), ServerName: "talos.test", Node: "node"}}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(machine, configObject, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: "cluster-talosconfig"}, Data: map[string][]byte{"talosconfig": identity}}, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: "cluster-kubeconfig"}, Data: map[string][]byte{"value": kubeData}}).Build()
	handler := NewHandler(c, c, NewOperationNodeClient(c))
	journal := &op.Journal{ControlPlane: []op.ObjectIdentity{objectIdentity(clusterv1.GroupVersion.Group, "Machine", machine)}}
	original, err := handler.controlPlaneObservation(t.Context(), cluster, journal)
	require.NoError(t, err)
	journal.OriginalEtcdMembers = original
	require.NoError(t, handler.controlPlaneHealth(t.Context(), cluster, journal))
	backend.unhealthy = true
	require.Error(t, handler.controlPlaneHealth(t.Context(), cluster, journal))
	backend.unhealthy = false
	backend.badLeader = true
	require.Error(t, handler.controlPlaneHealth(t.Context(), cluster, journal))
	backend.badLeader = false
	apiReady = false
	require.Error(t, handler.controlPlaneHealth(t.Context(), cluster, journal))
	apiReady = true
	backend.members = []*machineapi.EtcdMember{{Id: 9, Hostname: "node"}}
	require.Error(t, handler.controlPlaneHealth(t.Context(), cluster, journal), "consistent current member replacement must not satisfy original receipt")
	backend.members = []*machineapi.EtcdMember{{Id: 1, Hostname: "node"}}
	journal.ControlPlane = append(journal.ControlPlane, op.ObjectIdentity{APIGroup: clusterv1.GroupVersion.Group, Kind: "Machine", Name: "missing", UID: "missing-uid"})
	require.Error(t, handler.controlPlaneHealth(t.Context(), cluster, journal))
	_ = client.ObjectKey{}
}
