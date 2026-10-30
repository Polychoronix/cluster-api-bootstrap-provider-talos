// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package inplace
import("context";"fmt";"errors";"sigs.k8s.io/controller-runtime/pkg/client";clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2";bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1";op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation")
func(h *Handler)rejectStalePending(ctx context.Context,request *clusterv1.Machine)error{
 machine:=&clusterv1.Machine{};if h.reader.Get(ctx,client.ObjectKeyFromObject(request),machine)!=nil{return fmt.Errorf("pending operation Machine lookup failed")};ref:=machine.Spec.Bootstrap.ConfigRef;if !ref.IsDefined(){return nil};config:=&bootstrapv1.TalosConfig{};if h.reader.Get(ctx,client.ObjectKey{Namespace:machine.Namespace,Name:ref.Name},config)!=nil{return fmt.Errorf("pending operation bootstrap lookup failed")}
 secret,j,err:=op.ReadJournal(ctx,h.reader,machine.Namespace,objectIdentity(bootstrapv1.GroupVersion.Group,"TalosConfig",config));if errors.Is(err,op.ErrNotFound){return nil};if err!=nil{return err};if j.Phase==op.Observed&&receiptConsumed(secret,j){return nil};if j.Phase==op.Observed{return fmt.Errorf("prior operation receipt not consumed")};return h.ambiguous(ctx,secret,j,fmt.Errorf("frozen operation inputs stale"))
}
