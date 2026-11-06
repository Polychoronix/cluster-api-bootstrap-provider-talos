// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v1beta1

// ActivationReadyCondition reports verified, consented lifecycle ownership.
const ActivationReadyCondition = "ActivationReady"

// ConfigDriftCondition reports, for an activated TalosConfig, whether the node's live
// machine configuration still matches what this TalosConfig renders. It is observed on a
// timer and never acted on: drift is reported, not repaired.
const ConfigDriftCondition = "ConfigDrift"

const (
	// ConfigDriftedReason: the live configuration differs from the render; the message
	// names the differing documents (apiVersion/kind[/name]), never their content.
	ConfigDriftedReason = "ConfigDrifted"
	// NoConfigDriftReason: the live configuration is the render, document for document.
	NoConfigDriftReason = "NoDrift"
	// DriftObservationPendingReason: the node could not be observed (transport, Talos API);
	// the previous verdict is unknown, not drifted.
	DriftObservationPendingReason = "ObservationPending"
)
