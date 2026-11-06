// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

// These objects are serialized only into Secret.data; formatted diagnostics must not expose payloads.
func (Journal) String() string               { return "<private Talos operation journal>" }
func (j Journal) GoString() string           { return j.String() }
func (CompletionReceipt) String() string     { return "<private Talos operation receipt>" }
func (r CompletionReceipt) GoString() string { return r.String() }
func (OwnerPlanIntent) String() string       { return "<private Talos owner intent>" }
func (i OwnerPlanIntent) GoString() string   { return i.String() }
