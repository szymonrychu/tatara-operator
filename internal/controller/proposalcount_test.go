package controller

import (
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// testBotLogin is the fixture project's Scm.BotLogin: the author every genuine
// tatara proposal carries, and the authorship anchor the body-marker fallback
// requires.
const testBotLogin = "tatara-bot"

// iss builds an Issue CR fixture: provenance kind, forge state, platform status,
// owning repo, and mirrored forge labels. The author is the bot, which is what a
// genuine operator-filed proposal always looks like.
func iss(kind, state, status, repo string, labels ...string) tatarav1alpha1.Issue {
	return tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: "iss-" + repo + "-1", Namespace: "tatara"},
		Spec: tatarav1alpha1.IssueSpec{
			RepositoryRef: repo, Number: 1, ProjectRef: "demo", ProposalKind: kind,
		},
		Status: tatarav1alpha1.IssueStatus{
			State: state, Status: status, Labels: labels, Author: testBotLogin,
		},
	}
}

// issBody is iss plus a mirrored body, for the unstamped issues whose provenance
// can only come from the in-body marker. A marked body also gets the Spec anchor,
// because the operator has written the two together since f83e8f3 - that is what a
// genuine pre-Spec-field proposal actually looks like on disk.
func issBody(kind, state, status, repo, body string) tatarav1alpha1.Issue {
	out := iss(kind, state, status, repo)
	out.Status.Body = body
	if tatarav1alpha1.ProposalKindFromBody(body) != "" {
		out.Spec.ProposalBodyHash = tatarav1alpha1.ComputeProposalContentHash(body)
	}
	return out
}

// issNoAnchor is a marked body with NO Spec anchor: an issue the operator filed
// on behalf of an AGENT (issue_write action=create) whose body happens to contain
// the marker string. It is bot-authored, so the authorship gate passes; only the
// missing anchor separates it from a genuine proposal.
func issNoAnchor(kind, state, status, repo, body string) tatarav1alpha1.Issue {
	out := issBody(kind, state, status, repo, body)
	out.Spec.ProposalBodyHash = ""
	return out
}

// issAuthored is issBody with an explicit author: a human-filed issue whose body
// carries a PLANTED marker is the forgery this fallback must refuse.
func issAuthored(kind, state, status, repo, body, author string) tatarav1alpha1.Issue {
	out := issBody(kind, state, status, repo, body)
	out.Status.Author = author
	return out
}

// markedBody renders a mirrored body carrying the in-body provenance marker for
// kind - exactly what outcome.go stamps on every proposal it files.
func markedBody(kind string) string {
	return tatarav1alpha1.StampProposalMarker("do the thing", kind)
}

func TestProposalPending(t *testing.T) {
	tests := []struct {
		name string
		in   tatarav1alpha1.Issue
		want bool
	}{
		{"open brainstorm proposal awaiting a decision", iss("brainstorm", "open", "new", "r1"), true},
		{"open brainstorm proposal with an empty status", iss("brainstorm", "open", "", "r1"), true},
		{"approved frees its slot even while the forge issue stays open", iss("brainstorm", "open", "approved", "r1"), false},
		{"rejected does not count", iss("brainstorm", "open", "rejected", "r1"), false},
		{"done does not count", iss("brainstorm", "open", "done", "r1"), false},
		{"closed does not count", iss("brainstorm", "closed", "new", "r1"), false},
		{"incident provenance is not a brainstorm proposal", iss("incident", "open", "new", "r1"), false},
		{"unstamped with no body marker does not count", iss("", "open", "new", "r1"), false},

		// The body-marker fallback. Every proposal already open when this build
		// ships carries the in-body marker and NOT the Spec field, so without the
		// fallback the backlog reads zero on the first deploy and the controller
		// refills to the full target on top of it.
		{"unstamped with a brainstorm body marker counts",
			issBody("", "open", "new", "r1", markedBody(tatarav1alpha1.ProposalKindBrainstorm)), true},
		{"unstamped with an incident body marker does not count",
			issBody("", "open", "new", "r1", markedBody(tatarav1alpha1.ProposalKindIncident)), false},
		{"the body marker never revives a decided proposal",
			issBody("", "open", "approved", "r1", markedBody(tatarav1alpha1.ProposalKindBrainstorm)), false},

		// Precedence: Spec is the integrity-bearing field (the mirror never writes
		// Spec), so a forge-side body edit must not be able to override it either way.
		{"a stamped kind wins over a contradicting body marker",
			issBody("incident", "open", "new", "r1", markedBody(tatarav1alpha1.ProposalKindBrainstorm)), false},
		{"a stamped brainstorm survives a body whose marker was edited away",
			issBody("brainstorm", "open", "new", "r1", "the marker is gone"), true},

		// The AUTHORSHIP anchor on the fallback. The body is forge-editable, so
		// anyone with write access to any issue on a tracked repo could paste the
		// marker into it and inflate the backlog, suppressing legitimate refills.
		// The author is not editable, and a genuine proposal is always bot-filed.
		{"a marker planted in a human-authored issue does not count",
			issAuthored("", "open", "new", "r1", markedBody(tatarav1alpha1.ProposalKindBrainstorm), "mallory"), false},
		{"an empty author is never the bot",
			issAuthored("", "open", "new", "r1", markedBody(tatarav1alpha1.ProposalKindBrainstorm), ""), false},
		{"a stamped kind still counts regardless of author, because Spec is unforgeable",
			issAuthored("brainstorm", "open", "new", "r1", "", "mallory"), true},

		// The MINT anchor. mintIssueCR files agent-authored issues under the bot
		// account too, so authorship alone cannot tell an agent's issue that happens
		// to contain the marker string from a genuine proposal. The Spec anchor can:
		// the operator writes it only when IT declared the issue a proposal.
		{"a bot-filed issue whose body carries the marker but has no Spec anchor does not count",
			issNoAnchor("", "open", "new", "r1", markedBody(tatarav1alpha1.ProposalKindBrainstorm)), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := tc.in
			if got := proposalPending(&in, testBotLogin); got != tc.want {
				t.Fatalf("proposalPending = %v, want %v", got, tc.want)
			}
		})
	}
}

// An unconfigured bot login must never turn every issue into a proposal. BotLogin
// is a required, non-empty CRD field, so "" only happens on a malformed Project -
// and there the fallback must fail CLOSED, not open.
func TestProposalPendingWithNoBotLoginRefusesTheFallback(t *testing.T) {
	marked := issBody("", "open", "new", "r1", markedBody(tatarav1alpha1.ProposalKindBrainstorm))
	if proposalPending(&marked, "") {
		t.Fatal("an unstamped marker must not count when the project has no bot login")
	}
	stamped := iss("brainstorm", "open", "new", "r1")
	if !proposalPending(&stamped, "") {
		t.Fatal("a Spec-stamped proposal needs no authorship corroboration")
	}
}

func TestProposalDisplayStatus(t *testing.T) {
	tests := []struct {
		name string
		in   tatarav1alpha1.Issue
		want string
	}{
		{"approved wins regardless of state", iss("brainstorm", "closed", "approved", "r1"), "approved"},
		{"explicit rejection is declined", iss("brainstorm", "open", "rejected", "r1"), "declined"},
		{"a maintainer who just closes it has discarded it", iss("brainstorm", "closed", "new", "r1"), "declined"},
		{"still awaiting a decision", iss("brainstorm", "open", "new", "r1"), "open"},
		{"done is not a proposal verdict; it reads as open", iss("brainstorm", "open", "done", "r1"), "open"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := tc.in
			if got := proposalDisplayStatus(&in); got != tc.want {
				t.Fatalf("proposalDisplayStatus = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestPendingProposalCount(t *testing.T) {
	sysA := "tatara/systemic-abc"
	tests := []struct {
		name string
		in   []tatarav1alpha1.Issue
		want int
	}{
		{"empty", nil, 0},
		{"three standalone pending proposals", []tatarav1alpha1.Issue{
			iss("brainstorm", "open", "new", "r1"),
			iss("brainstorm", "open", "new", "r2"),
			iss("brainstorm", "open", "new", "r3"),
		}, 3},
		{"a systemic group across three repos is one slot", []tatarav1alpha1.Issue{
			iss("brainstorm", "open", "new", "r1", sysA),
			iss("brainstorm", "open", "new", "r2", sysA),
			iss("brainstorm", "open", "new", "r3", sysA),
		}, 1},
		{"one systemic group plus one standalone", []tatarav1alpha1.Issue{
			iss("brainstorm", "open", "new", "r1", sysA),
			iss("brainstorm", "open", "new", "r2", sysA),
			iss("brainstorm", "open", "new", "r3"),
		}, 2},
		{"excluded issues never reach the collapse", []tatarav1alpha1.Issue{
			iss("brainstorm", "open", "approved", "r1"),
			iss("brainstorm", "closed", "new", "r2"),
			iss("incident", "open", "new", "r3"),
		}, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := pendingProposalCount(tc.in, testBotLogin); got != tc.want {
				t.Fatalf("pendingProposalCount = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestPendingProposalCountCountsUnstampedBodyMarkerProposals is the rollout
// safety property, and the reason the body-marker fallback exists at all.
//
// On the first deploy of this build, EVERY open proposal in every project was
// minted before Spec.ProposalKind existed: it carries the in-body
// tatara-proposed-by:brainstorm marker and nothing in Spec. If the count read
// those as zero, the deficit would be the full target on the very first cron
// tick and the controller would file N more proposals on top of a backlog that
// was already full - the 2026-06-13 flooding-incident class. The count must be
// NONZERO here with no Spec.ProposalKind set anywhere.
func TestPendingProposalCountCountsUnstampedBodyMarkerProposals(t *testing.T) {
	body := markedBody(tatarav1alpha1.ProposalKindBrainstorm)
	issues := []tatarav1alpha1.Issue{
		issBody("", "open", "new", "r1", body),
		issBody("", "open", "", "r2", body),
		issBody("", "open", "new", "r3", body),
		// Not proposals, or already decided: these must not inflate the count.
		issBody("", "open", "new", "r4", markedBody(tatarav1alpha1.ProposalKindIncident)),
		issBody("", "open", "approved", "r5", body),
		issBody("", "closed", "new", "r6", body),
		issBody("", "open", "new", "r7", "a plain body with no marker"),
		// The planted marker: a human-authored issue on a tracked repo whose body
		// someone pasted the marker into. It must never buy a backlog slot.
		issAuthored("", "open", "new", "r8", body, "mallory"),
		// The agent-filed issue: bot-authored (mintIssueCR files everything under
		// the bot) with the marker string in its body but no operator-declared
		// provenance. Also never a backlog slot.
		issNoAnchor("", "open", "new", "r9", body),
	}
	for i := range issues {
		if issues[i].Spec.ProposalKind != "" {
			t.Fatalf("fixture %d must carry NO Spec.ProposalKind", i)
		}
	}
	if got := pendingProposalCount(issues, testBotLogin); got != 3 {
		t.Fatalf("pendingProposalCount = %d, want 3", got)
	}
	byRepo := pendingProposalCountByRepo(issues, testBotLogin)
	want := map[string]int{"r1": 1, "r2": 1, "r3": 1}
	if len(byRepo) != len(want) {
		t.Fatalf("pendingProposalCountByRepo = %v, want %v", byRepo, want)
	}
	for k, v := range want {
		if byRepo[k] != v {
			t.Fatalf("pendingProposalCountByRepo[%q] = %d, want %d", k, byRepo[k], v)
		}
	}
}

func TestPendingProposalCountByRepo(t *testing.T) {
	sysA := "tatara/systemic-abc"
	got := pendingProposalCountByRepo([]tatarav1alpha1.Issue{
		iss("brainstorm", "open", "new", "r1", sysA),
		iss("brainstorm", "open", "new", "r2", sysA),
		iss("brainstorm", "open", "new", "r1"),
		iss("brainstorm", "open", "approved", "r1"),
	}, testBotLogin)
	// Per-repo is deliberately UNCOLLAPSED: a systemic group spans repos, so
	// collapsing it would have to attribute the one slot to an arbitrary repo.
	want := map[string]int{"r1": 2, "r2": 1}
	if len(got) != len(want) {
		t.Fatalf("pendingProposalCountByRepo = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("pendingProposalCountByRepo[%q] = %d, want %d", k, got[k], v)
		}
	}
}

func TestBrainstormDeficit(t *testing.T) {
	tests := []struct {
		name                      string
		target, pending, inflight int
		want                      int
	}{
		{"empty backlog refills to target", 3, 0, 0, 3},
		{"partial backlog refills the gap", 3, 2, 0, 1},
		{"an in-flight session already covers the gap", 3, 2, 1, 0},
		{"at target", 3, 3, 0, 0},
		{"over target after lowering N clamps at zero", 3, 7, 0, 0},
		{"over target with a session in flight still clamps at zero", 3, 7, 1, 0},
		{"target zero disables refill", 0, 0, 0, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := brainstormDeficit(tc.target, tc.pending, tc.inflight); got != tc.want {
				t.Fatalf("brainstormDeficit(%d,%d,%d) = %d, want %d",
					tc.target, tc.pending, tc.inflight, got, tc.want)
			}
		})
	}
}

func TestBrainstormRefillDecision(t *testing.T) {
	target := func(n int) tatarav1alpha1.BrainstormActivity {
		v := n
		return tatarav1alpha1.BrainstormActivity{TargetOpenProposals: &v}
	}
	tests := []struct {
		name                     string
		act                      tatarav1alpha1.BrainstormActivity
		pending, inflight, skips int
		trigger                  string
		wantQuota                int
		wantRefill               bool
		wantReason               string
	}{
		{"empty backlog on an event", target(3), 0, 0, 0, "event", 3, true, ""},
		{"at target on an event", target(3), 3, 0, 0, "event", 0, false, "at-target"},
		{"in flight on an event", target(3), 0, 1, 0, "event", 2, true, ""},
		{"breaker tripped suppresses the event path", target(3), 0, 0, 3, "event", 0, false, "breaker-tripped"},
		{"breaker below threshold does not suppress", target(3), 0, 0, 2, "event", 3, true, ""},
		{"a cron tick ignores the breaker", target(3), 0, 0, 9, "cron", 3, true, ""},
		{"a large target is clamped to the submit_outcome ceiling", target(20), 0, 0, 0, "cron", 5, true, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			quota, refill, reason := brainstormRefillDecision(tc.act, tc.pending, tc.inflight, tc.skips, tc.trigger)
			if quota != tc.wantQuota || refill != tc.wantRefill || reason != tc.wantReason {
				t.Fatalf("brainstormRefillDecision = (%d, %v, %q), want (%d, %v, %q)",
					quota, refill, reason, tc.wantQuota, tc.wantRefill, tc.wantReason)
			}
		})
	}
}
