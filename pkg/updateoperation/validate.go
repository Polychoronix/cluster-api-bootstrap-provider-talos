// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package updateoperation

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"reflect"
)

func stepRank(p StepPhase) int {
	switch p {
	case StepPlanned:
		return 0
	case Issued:
		return 1
	case Acknowledged:
		return 2
	case StepObserved:
		return 3
	case StepAmbiguous:
		return 4
	}
	return -1
}
func validateJournal(j *Journal) error {
	if j == nil || j.Version != SchemaVersion || j.TransactionID == "" || !validIdentity(j.Machine) || !validIdentity(j.TalosConfig) || !validIdentity(j.Infrastructure) || j.NodeName == "" || j.NodeUID == "" || j.NodeProviderID == "" || len(j.Configuration) == 0 || len(j.InputFreshness) == 0 || len(j.Steps) == 0 {
		return safe("journal schema")
	}
	if j.Phase != Planned && j.Phase != Observed && j.Phase != Ambiguous {
		return safe("journal phase")
	}
	if len(j.ControlPlane) > 0 {
		if len(j.OriginalEtcdMembers) != len(j.ControlPlane) {
			return safe("journal original controlplane membership missing")
		}
		ids := map[uint64]bool{}
		names := map[string]bool{}
		for _, member := range j.OriginalEtcdMembers {
			if member.MemberID == 0 || member.Hostname == "" || ids[member.MemberID] || names[member.Hostname] {
				return safe("journal original etcd identity invalid")
			}
			ids[member.MemberID] = true
			names[member.Hostname] = true
		}
	}
	seen := map[OperationKind]bool{}
	for _, s := range j.Steps {
		if s.Kind != Apply && s.Kind != PullInstaller && s.Kind != Install && s.Kind != Reboot || seen[s.Kind] || stepRank(s.Phase) < 0 {
			return safe("journal step schema")
		}
		seen[s.Kind] = true
		if s.Kind == Apply && s.ApplyOutcome != Unknown && s.ApplyOutcome != NoReboot && s.ApplyOutcome != RebootRequired {
			return safe("journal apply outcome")
		}
		if stepRank(s.Phase) >= 1 {
			if s.IssuerID == "" || s.IssuedAt == nil || s.Deadline == nil || !s.Deadline.After(s.IssuedAt.Time) || s.IssuedBootID == "" {
				return safe("journal issued witness")
			}
		}
		if s.Phase == Acknowledged || s.Phase == StepObserved {
			switch s.Kind {
			case Apply:
				if s.ApplyOutcome == Unknown {
					return safe("journal apply unknown")
				}
			case PullInstaller:
				if s.PulledImageName == "" || s.PulledImageDigest == "" {
					return safe("journal image acknowledgment")
				}
			case Install:
				if s.InstallExitCode == nil || *s.InstallExitCode != 0 {
					return safe("journal install acknowledgment")
				}
			}
		}
		if j.Phase == Observed && s.Phase != StepObserved {
			return safe("journal incomplete receipt")
		}
	}
	if j.Phase == Observed && (j.CompletedAt == nil || j.CompletedAt.IsZero()) {
		return safe("journal completion time")
	}
	return nil
}
func frozenPlan(j *Journal) Journal {
	v := *j
	v.Phase = ""
	v.Steps = nil
	v.CompletedAt = nil
	return v
}
func validateTransition(old, next *Journal) error {
	if err := validateJournal(old); err != nil {
		return err
	}
	if err := validateJournal(next); err != nil {
		return err
	}
	if !reflect.DeepEqual(frozenPlan(old), frozenPlan(next)) {
		return safe("frozen operation intent changed")
	}
	if old.Phase == Observed || old.Phase == Ambiguous {
		if !reflect.DeepEqual(old, next) {
			return safe("terminal journal changed")
		}
		return nil
	}
	if len(old.Steps) != len(next.Steps) {
		return safe("journal steps changed")
	}
	for i, a := range old.Steps {
		b := next.Steps[i]
		if a.Kind != b.Kind || stepRank(b.Phase) < stepRank(a.Phase) || a.Phase == StepAmbiguous && !reflect.DeepEqual(a, b) {
			return safe("journal step regression")
		}
		if a.Phase != StepPlanned && (a.IssuerID != b.IssuerID || !sameTime(a.IssuedAt, b.IssuedAt) || !sameTime(a.Deadline, b.Deadline) || a.IssuedBootID != b.IssuedBootID) {
			return safe("journal issuer changed")
		}
		if a.ApplyOutcome != Unknown && a.ApplyOutcome != b.ApplyOutcome || a.PulledImageName != "" && a.PulledImageName != b.PulledImageName || a.PulledImageDigest != "" && a.PulledImageDigest != b.PulledImageDigest || a.InstallExitCode != nil && !reflect.DeepEqual(a.InstallExitCode, b.InstallExitCode) {
			return safe("journal acknowledgment changed")
		}
		if a.ObservedBootID != "" && a.ObservedBootID != b.ObservedBootID {
			return safe("journal observed boot witness changed")
		}
		if a.Phase == StepObserved && !reflect.DeepEqual(a, b) {
			return safe("journal observed step changed")
		}
	}
	return nil
}

func sameTime(a, b *metav1.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(b)
}
