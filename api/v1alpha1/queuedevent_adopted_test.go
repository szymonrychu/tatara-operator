package v1alpha1

import "testing"

func adoptedSpec() QueuedEventSpec {
	return QueuedEventSpec{
		Seq:           1,
		Class:         QueueClassNormal,
		Kind:          "upgrade",
		ProjectRef:    "proj",
		RepositoryRef: "charts",
		DedupKey:      "adopt-upgrade|charts|41",
		Payload: QueuedEventPayload{
			Kind:          "upgrade",
			RepositoryRef: "charts",
			AdoptedUpgrade: &AdoptedUpgradeRef{
				Number: 41, Title: "chore(deps): bump cilium", Author: "tatara-bot",
				HeadSHA: "abc", HeadBranch: "renovate/cilium",
				Repo: "szymonrychu/charts", HeadRepo: "szymonrychu/charts",
			},
		},
	}
}

// THE HAPPY SHAPE VALIDATES. An adoption event is a MINT with a typed PR
// snapshot: no agentKind, no taskRef, no newTask.
func TestValidateQueuedEventSpec_AdoptedUpgradeMintIsValid(t *testing.T) {
	if err := ValidateQueuedEventSpec(adoptedSpec()); err != nil {
		t.Fatalf("ValidateQueuedEventSpec = %v, want nil", err)
	}
}

// MUTUALLY EXCLUSIVE WITH THE ADMISSION-TICKET SHAPE. A ticket admits an
// EXISTING Task's pod and mints nothing; an adoption snapshot mints a Task that
// does not exist. A payload claiming both describes two different pieces of
// work and the dispatcher would have to guess which one it is.
func TestValidateQueuedEventSpec_AdoptedUpgradeRejectsTheTicketShape(t *testing.T) {
	spec := adoptedSpec()
	spec.Payload.AgentKind = "upgrade"
	spec.Payload.TaskRef = "some-task"
	if err := ValidateQueuedEventSpec(spec); err == nil {
		t.Fatal("ValidateQueuedEventSpec = nil, want an error for adoptedUpgrade + the ticket shape")
	}
}

// ...AND WITH THE B.7 BLUEPRINT, for the same reason.
func TestValidateQueuedEventSpec_AdoptedUpgradeRejectsTheBlueprintShape(t *testing.T) {
	spec := adoptedSpec()
	spec.Payload.AgentKind = "upgrade"
	spec.Payload.NewTask = &QueuedTaskBlueprint{Name: "t", Kind: "upgrade", Goal: "g", ProjectRef: "proj"}
	if err := ValidateQueuedEventSpec(spec); err == nil {
		t.Fatal("ValidateQueuedEventSpec = nil, want an error for adoptedUpgrade + newTask")
	}
}

// A MERGE REQUEST NUMBER IS THE WHOLE IDENTITY. Without it the mint has no
// deterministic Task name and no merge request to bind.
func TestValidateQueuedEventSpec_AdoptedUpgradeRequiresANumber(t *testing.T) {
	spec := adoptedSpec()
	spec.Payload.AdoptedUpgrade.Number = 0
	if err := ValidateQueuedEventSpec(spec); err == nil {
		t.Fatal("ValidateQueuedEventSpec = nil, want an error for adoptedUpgrade with number 0")
	}
}

// BACKWARD COMPATIBILITY. Every event already Queued when this ships has a nil
// AdoptedUpgrade and must keep validating byte-identically.
func TestValidateQueuedEventSpec_LegacyFlatMintUnchanged(t *testing.T) {
	spec := adoptedSpec()
	spec.Payload.AdoptedUpgrade = nil
	spec.Payload.Goal = "do a thing"
	if err := ValidateQueuedEventSpec(spec); err != nil {
		t.Fatalf("ValidateQueuedEventSpec = %v, want nil for a legacy flat mint", err)
	}
}
