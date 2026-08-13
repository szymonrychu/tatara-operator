package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// sweepReader is the fake forge the sweep tests run against. Every method the
// sweep calls is served from these maps; everything else on scm.SCMReader is
// nil-embedded and panics if the sweep ever reaches for it (which it must not).
type sweepReader struct {
	scm.SCMReader
	issues   []scm.IssueRef
	prs      []scm.PRRef
	comments map[int][]scm.IssueComment
	content  map[int]scm.IssueContent

	// prComments backs ListPRComments (OP12's PRCommentLister capability),
	// keyed by PR number - a SEPARATE map from comments (issue comments), so a
	// test seeding one never accidentally feeds the other.
	prComments map[int][]scm.IssueComment

	// listCommentsErr, when set, fails EVERY comment read - the cheapest way to
	// drive a per-item sweep error (fail("list_comments")) that leaves firstErr
	// non-nil while the pass still structurally completes.
	listCommentsErr error

	issueCalls int
	prCalls    int
}

func (s *sweepReader) ListOpenIssues(context.Context, string, string) ([]scm.IssueRef, error) {
	s.issueCalls++
	return s.issues, nil
}

func (s *sweepReader) ListOpenPRs(context.Context, string, string) ([]scm.PRRef, error) {
	s.prCalls++
	return s.prs, nil
}

func (s *sweepReader) ListIssueComments(_ context.Context, _, _ string, number int) ([]scm.IssueComment, error) {
	if s.listCommentsErr != nil {
		return nil, s.listCommentsErr
	}
	return s.comments[number], nil
}

func (s *sweepReader) GetIssue(_ context.Context, _, _ string, number int) (scm.IssueContent, error) {
	if c, ok := s.content[number]; ok {
		return c, nil
	}
	return scm.IssueContent{}, nil
}

// ListPRComments implements scm.PRCommentLister (OP12): the sweep's
// listPRCommentsAfter type-asserts the reader for it, exactly like
// syncMergeRequestThread already does for the mirror's cadence sync.
func (s *sweepReader) ListPRComments(_ context.Context, _, _ string, number int) ([]scm.IssueComment, error) {
	return s.prComments[number], nil
}

func sweepProject(name string) *tatarav1alpha1.Project {
	return &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   testNS,
			Annotations: map[string]string{SweepAnnotation: SweepEnabledValue},
		},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef:        "scm-secret",
			MaxNewTasksPerSweep: 5,
			MaxOpenTasks:        6,
			Scm: &tatarav1alpha1.ScmSpec{
				Provider: "github",
				BotLogin: "tatara-bot",
			},
		},
	}
}

func sweepRepo(proj string) *tatarav1alpha1.Repository {
	return &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "tatara-operator", Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef: proj,
			URL:        "https://github.com/szymonrychu/tatara-operator.git",
		},
	}
}

func humanComment(id, author, body string, at time.Time) scm.IssueComment {
	return scm.IssueComment{ExternalID: id, Author: author, Body: body, CreatedAt: at}
}

// runSweep drives one full SweepProject pass against the fake forge.
func runSweep(t *testing.T, c client.Client, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, rd scm.SCMReader) {
	t.Helper()
	r := &ProjectReconciler{Client: c, Scheme: c.Scheme(), Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry())}
	if _, err := r.SweepProject(context.Background(), proj, rd, []tatarav1alpha1.Repository{*repo}, nil, SweepActivity); err != nil {
		t.Fatalf("SweepProject: %v", err)
	}
}

func sweepTasks(t *testing.T, c client.Client, proj string) []tatarav1alpha1.Task {
	t.Helper()
	var tl tatarav1alpha1.TaskList
	if err := c.List(context.Background(), &tl, client.InNamespace(testNS)); err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	out := make([]tatarav1alpha1.Task, 0, len(tl.Items))
	for i := range tl.Items {
		if tl.Items[i].Spec.ProjectRef == proj {
			out = append(out, tl.Items[i])
		}
	}
	return out
}

// TestOrphanIssuePredicate pins the THREE clauses of B.4's ONE orphan
// predicate, and the REASON each returns. Clause (c) is the reporter intake
// gate (issue #102): v3 deleted it by omission, and its entire purpose is that
// an INJECTED issue never becomes a Task.
//
// Clause (b) now takes a RESOLVED LIVE owner rather than reading owner refs
// itself (issue #521). The old form called own.ControllerOwner, which returns
// owned=true for any ref carrying controller=true and never checks the named
// Task exists - so an Issue CR whose owning Task was reaped kept a dangling
// ref forever and was skipped silently on every pass. Taking the live owner as
// a PARAMETER makes "resolve liveness first" structural instead of a
// convention a caller can forget. Minter.resolveLiveOwner is what fills it in;
// TestResolveLiveOwner* pins that half.
func TestOrphanIssuePredicate(t *testing.T) {
	proj := sweepProject("orphan-proj")
	repo := sweepRepo("orphan-proj")

	gated := sweepProject("orphan-proj")
	gated.Spec.Scm.ReporterLogins = []string{"alice"}

	tests := map[string]struct {
		proj       *tatarav1alpha1.Project
		iss        scm.Issue
		liveOwner  string
		want       bool
		wantReason string
	}{
		"open, no owner, open allowlist": {
			proj: proj, iss: scm.Issue{Number: 1, State: "open", Author: "carol"},
			want: true, wantReason: SweepSkipNone,
		},
		"clause a: closed on the forge": {
			proj: proj, iss: scm.Issue{Number: 1, State: "closed", Author: "carol"},
			want: false, wantReason: SweepSkipIssueNotOpen,
		},
		"clause b: a LIVE controller owner": {
			proj: proj, iss: scm.Issue{Number: 1, State: "open", Author: "carol"},
			liveOwner: "owner-task", want: false, wantReason: SweepSkipIssueOwned,
		},
		"clause b: no live owner IS an orphan (the #521 case: the ref named a reaped Task)": {
			proj: proj, iss: scm.Issue{Number: 1, State: "open", Author: "carol"},
			liveOwner: "", want: true, wantReason: SweepSkipNone,
		},
		"clause a beats clause b: a closed issue with a live owner still reports not-open": {
			proj: proj, iss: scm.Issue{Number: 1, State: "closed", Author: "carol"},
			liveOwner: "owner-task", want: false, wantReason: SweepSkipIssueNotOpen,
		},
		"clause c: a non-allowlisted author mints NOTHING": {
			proj: gated, iss: scm.Issue{Number: 1, State: "open", Author: "mallory"},
			want: false, wantReason: SweepSkipReporterNotAllowed,
		},
		"clause c: an allowlisted author passes": {
			proj: gated, iss: scm.Issue{Number: 1, State: "open", Author: "alice"},
			want: true, wantReason: SweepSkipNone,
		},
		"clause c: an empty author fails CLOSED under an active gate": {
			proj: gated, iss: scm.Issue{Number: 1, State: "open", Author: ""},
			want: false, wantReason: SweepSkipReporterNotAllowed,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got, reason := IsOrphanIssue(tc.proj, repo, tc.iss, tc.liveOwner)
			if got != tc.want {
				t.Fatalf("IsOrphanIssue orphan = %v, want %v", got, tc.want)
			}
			if reason != tc.wantReason {
				t.Fatalf("IsOrphanIssue reason = %q, want %q", reason, tc.wantReason)
			}
			if got && reason != SweepSkipNone {
				t.Fatalf("an orphan must carry no skip reason, got %q", reason)
			}
			if !got && reason == SweepSkipNone {
				t.Fatal("a non-orphan must NAME the clause that refused it")
			}
		})
	}
}

// TestMintStage pins the TWO mint stages and the ONE load-bearing ordering
// fact: tatara-parked beats every other clause. It does NOT pin an order among
// the remaining clauses (trusted-human-author, webhookOriginated,
// humanHasLastWord) - they all return the identical (StageTriaging, ""), so
// their relative order has zero observable effect and no case here depends on
// it.
//
// The tatara-parked LABEL READ is safe where fix 16's forbidden one is not: it
// decides COST (do we spend a pod on this issue now?), never AUTHORITY (may this
// issue be implemented?). Forging the label buys an attacker a Task that stays
// PARKED - it fails SAFE. Forging an approval label would buy them prod. Do not
// generalise one rule into the other.
//
// THE TRUSTED-AUTHOR CLAUSE's only pinned relationship to anything else is
// AFTER tatara-parked: an explicit human park still wins over the author's
// standing. It does not depend on webhookOriginated or humanHasLastWord at
// all - a maintainer's issue starts an agent even when the delivery was lost
// outright and no marker was ever stamped, regardless of where in the
// function this clause happens to sit relative to those two. That is what
// makes the sweep a genuine backstop rather than one that parks the work it
// was meant to rescue.
//
// The clause is NARROWED to a trusted HUMAN, not IsTrustedAuthor verbatim.
// IsTrustedAuthor documents the project's own bot login as a trusted insider
// (api/v1alpha1/logins.go:61), and taken literally the clause would mint every
// bot-authored orphan issue ACTIVE too - including an abandoned brainstorm
// proposal issue whose Task was reaped and whose Issue CR lost its controller
// owner in the process. ClassifyPR clause 2 (internal/controller/sweep.go,
// lines 513-516; ClassifyPR itself starts at line 507) exists because the
// identical property bit on the PR side: prInReactionScope returns true
// immediately for IsTrustedAuthor, and the bot is documented as trusted there
// too. That precedent is why this clause excludes the bot explicitly rather
// than repeating the mistake on the issue side.
func TestMintStage(t *testing.T) {
	base := sweepProject("mint-proj")
	repo := sweepRepo("mint-proj")

	trusted := sweepProject("mint-proj")
	trusted.Spec.Scm.MaintainerLogins = []string{"alice"}

	reporterOnly := sweepProject("mint-proj")
	reporterOnly.Spec.Scm.ReporterLogins = []string{"carol"}

	// A per-Repository override REPLACES the project list for that repo (a
	// non-nil pointer, including an explicit empty slice). Nothing about this
	// clause is project-global.
	noneHere := sweepRepo("mint-proj")
	noneHere.Spec.MaintainerLogins = &[]string{}

	t0 := time.Now().Add(-time.Hour)

	tests := map[string]struct {
		proj       *tatarav1alpha1.Project
		repo       *tatarav1alpha1.Repository
		iss        scm.Issue
		webhook    bool
		wantStage  string
		wantReason string
	}{
		"webhook-originated mints ACTIVE": {
			iss:       scm.Issue{Number: 1, State: "open", Author: "alice"},
			webhook:   true,
			wantStage: tatarav1alpha1.StateNew,
		},
		"a human has the last word: ACTIVE": {
			iss: scm.Issue{Number: 1, State: "open", Author: "alice", Comments: []scm.IssueComment{
				humanComment("1", "tatara-bot", "on it", t0),
				humanComment("2", "alice", "any update?", t0.Add(time.Minute)),
			}},
			wantStage: tatarav1alpha1.StateNew,
		},
		"the bot has the last word: PARKED": {
			iss: scm.Issue{Number: 1, State: "open", Author: "alice", Comments: []scm.IssueComment{
				humanComment("1", "alice", "please fix", t0),
				humanComment("2", "tatara-bot", "parked", t0.Add(time.Minute)),
			}},
			wantStage:  tatarav1alpha1.StateNew,
			wantReason: stage.ReasonBacklogSweep,
		},
		"an untouched backlog issue from an UNLISTED author: PARKED": {
			iss:        scm.Issue{Number: 1, State: "open", Author: "alice"},
			wantStage:  tatarav1alpha1.StateNew,
			wantReason: stage.ReasonBacklogSweep,
		},
		"tatara-parked beats a human last word: PARKED": {
			iss: scm.Issue{Number: 1, State: "open", Author: "alice", Labels: []string{TataraParkedLabel},
				Comments: []scm.IssueComment{humanComment("1", "alice", "ping", t0)}},
			wantStage:  tatarav1alpha1.StateNew,
			wantReason: stage.ReasonBacklogSweep,
		},
		// THE ORDERING. The label is checked BEFORE the marker, and that ordering IS
		// fix M25. The marker is DURABLE now (an annotation on the Issue CR), not an
		// in-process bool on the delivery being handled, so it can outlive the event
		// and meet a label the operator or a human stamped afterwards. If the marker
		// won, an uncleared one would re-open the M25 re-mint loop: mint ACTIVE ->
		// the pod re-triages -> it parks -> the reaper stamps tatara-parked -> the
		// sweep sees the marker again -> ACTIVE, forever. The label is the operator's
		// durable "this issue costs nothing" record and it is the OUTERMOST gate.
		"tatara-parked beats a WEBHOOK MARKER: PARKED": {
			iss:        scm.Issue{Number: 1, State: "open", Author: "alice", Labels: []string{TataraParkedLabel}},
			webhook:    true,
			wantStage:  tatarav1alpha1.StateNew,
			wantReason: stage.ReasonBacklogSweep,
		},
		"an empty comment author is never the bot": {
			iss: scm.Issue{Number: 1, State: "open", Author: "alice", Comments: []scm.IssueComment{
				humanComment("1", "", "deleted account", t0),
			}},
			wantStage: tatarav1alpha1.StateNew, wantReason: stage.ReasonBacklogSweep,
		},

		// --- the trusted-author clause ---
		"a MAINTAINER's brand-new issue mints ACTIVE with no marker and no comments": {
			proj:      trusted,
			iss:       scm.Issue{Number: 1, State: "open", Author: "alice"},
			wantStage: tatarav1alpha1.StateNew,
		},
		"a listed REPORTER's brand-new issue mints ACTIVE": {
			proj:      reporterOnly,
			iss:       scm.Issue{Number: 1, State: "open", Author: "carol"},
			wantStage: tatarav1alpha1.StateNew,
		},
		"PRECEDENCE: tatara-parked BEATS a trusted author": {
			proj:       trusted,
			iss:        scm.Issue{Number: 1, State: "open", Author: "alice", Labels: []string{TataraParkedLabel}},
			wantStage:  tatarav1alpha1.StateNew,
			wantReason: stage.ReasonBacklogSweep,
		},
		"PRECEDENCE: a trusted author needs NO webhook marker": {
			proj:      trusted,
			iss:       scm.Issue{Number: 1, State: "open", Author: "alice"},
			webhook:   false,
			wantStage: tatarav1alpha1.StateNew,
		},
		"an author outside the lists is NOT trusted: PARKED": {
			proj:       trusted,
			iss:        scm.Issue{Number: 1, State: "open", Author: "mallory"},
			wantStage:  tatarav1alpha1.StateNew,
			wantReason: stage.ReasonBacklogSweep,
		},
		"an empty author is never trusted: PARKED": {
			proj:       trusted,
			iss:        scm.Issue{Number: 1, State: "open", Author: ""},
			wantStage:  tatarav1alpha1.StateNew,
			wantReason: stage.ReasonBacklogSweep,
		},
		"a per-repo override that clears the maintainer list un-trusts the author": {
			proj:       trusted,
			repo:       noneHere,
			iss:        scm.Issue{Number: 1, State: "open", Author: "alice"},
			wantStage:  tatarav1alpha1.StateNew,
			wantReason: stage.ReasonBacklogSweep,
		},
		// COORDINATOR-NARROWED, DELIBERATELY PINNED (see the doc comment above). The
		// brief's clause was IsTrustedAuthor verbatim, which mints a bot-authored
		// orphan issue ACTIVE - an abandoned brainstorm proposal issue whose Task was
		// reaped, say, and whose Issue CR lost its controller owner along with it.
		// The narrowed clause excludes the project's own bot login, so this case
		// falls through to the webhook/last-word/backlog clauses exactly as it did
		// before this change: no webhook, no comments -> PARKED.
		"a BOT-authored orphan issue is NOT a trusted human and does not mint ACTIVE": {
			iss:        scm.Issue{Number: 1, State: "open", Author: "tatara-bot"},
			wantStage:  tatarav1alpha1.StateNew,
			wantReason: stage.ReasonBacklogSweep,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			proj := tc.proj
			if proj == nil {
				proj = base
			}
			r := tc.repo
			if r == nil {
				r = repo
			}
			gotStage, gotReason := MintStage(proj, r, tc.iss, tc.webhook)
			if gotStage != tc.wantStage || gotReason != tc.wantReason {
				t.Fatalf("MintStage = (%q, %q), want (%q, %q)", gotStage, gotReason, tc.wantStage, tc.wantReason)
			}
		})
	}
}

// TestSweepBacklogIssueMintsParkedTaskWithNoPod: a backlog issue mints a
// parked(backlog-sweep) Task that spawns NO pod and enqueues NO QueuedEvent.
func TestSweepBacklogIssueMintsParkedTaskWithNoPod(t *testing.T) {
	proj := sweepProject("backlog-proj")
	repo := sweepRepo("backlog-proj")
	c := newMirrorClient(t, proj, repo)
	rd := &sweepReader{
		issues:  []scm.IssueRef{{Repo: "szymonrychu/tatara-operator", Number: 7, Author: "alice", Title: "slow query", State: "open"}},
		content: map[int]scm.IssueContent{7: {Title: "slow query", Body: "it is slow"}},
	}

	runSweep(t, c, proj, repo, rd)

	tasks := sweepTasks(t, c, proj.Name)
	if len(tasks) != 1 {
		t.Fatalf("tasks = %d, want 1", len(tasks))
	}
	tk := tasks[0]
	// The mint sets the IMMUTABLE Spec.InitialState (fix C5); Status.Stage is
	// applied later by the TaskReconciler create-edge, which this test does not
	// run.
	if tk.Spec.InitialState != tatarav1alpha1.StateNew || tk.Spec.InitialParkReason != stage.ReasonBacklogSweep {
		t.Fatalf("initialStage = %q/%q, want parked/backlog-sweep", tk.Spec.InitialState, tk.Spec.InitialParkReason)
	}
	if tk.Status.PodName != "" {
		t.Fatalf("parked(backlog-sweep) spawned a pod: %q", tk.Status.PodName)
	}
	var qel tatarav1alpha1.QueuedEventList
	if err := c.List(context.Background(), &qel, client.InNamespace(testNS)); err != nil {
		t.Fatalf("list queuedevents: %v", err)
	}
	if len(qel.Items) != 0 {
		t.Fatalf("queuedevents = %d, want 0 (a parked(backlog-sweep) Task enqueues NOTHING)", len(qel.Items))
	}

	// The Task OWNS the Issue CR: on the next sweep the issue is no longer an
	// orphan, which is what breaks the re-mint loop (ownership, not a heuristic).
	var iss tatarav1alpha1.Issue
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: testNS, Name: tatarav1alpha1.IssueName(repo.Name, 7)}, &iss); err != nil {
		t.Fatalf("get issue CR: %v", err)
	}
	ownerName, ok := own.ControllerOwner(&iss)
	if !ok || ownerName != tk.Name {
		t.Fatalf("issue controller owner = %q/%v, want %q", ownerName, ok, tk.Name)
	}
	if len(tk.Status.IssueRefs) != 1 || tk.Status.IssueRefs[0] != iss.Name {
		t.Fatalf("task issueRefs = %v, want [%s]", tk.Status.IssueRefs, iss.Name)
	}
}

// Contract K.1: operator_orphan_adopted_total increments once per orphan work
// item the sweep mints a Task for, by kind - the issue mint (mintTaskForIssue)
// and the PR mint (mintReviewTaskForPR) each carry their own kind label.
func TestSweepMintsFireOrphanAdopted(t *testing.T) {
	t.Run("orphan issue mint", func(t *testing.T) {
		proj := sweepProject("oa-issue-proj")
		repo := sweepRepo("oa-issue-proj")
		c := newMirrorClient(t, proj, repo)
		rd := &sweepReader{
			issues:  []scm.IssueRef{{Repo: "szymonrychu/tatara-operator", Number: 9, Author: "alice", Title: "slow query", State: "open"}},
			content: map[int]scm.IssueContent{9: {Title: "slow query", Body: "it is slow"}},
		}
		reg := prometheus.NewRegistry()
		r := &ProjectReconciler{Client: c, Scheme: c.Scheme(), Metrics: obs.NewOperatorMetrics(reg)}
		if _, err := r.SweepProject(context.Background(), proj, rd, []tatarav1alpha1.Repository{*repo}, nil, SweepActivity); err != nil {
			t.Fatalf("SweepProject: %v", err)
		}
		if got := testutil.ToFloat64(r.Metrics.OrphanAdoptedCounter(SweepIssueKind)); got != 1 {
			t.Fatalf("operator_orphan_adopted_total{%s} = %v, want 1", SweepIssueKind, got)
		}
	})

	t.Run("orphan PR mint", func(t *testing.T) {
		base := "szymonrychu/tatara-operator"
		proj := sweepProject("oa-pr-proj")
		proj.Spec.TriggerLabel = "tatara"
		proj.Spec.Scm.PRReactionScope = "labeledOrMentioned"
		repo := sweepRepo("oa-pr-proj")
		c := newMirrorClient(t, proj, repo)
		rd := &sweepReader{prs: []scm.PRRef{{
			Repo: base, HeadRepo: base, Number: 31, Author: "contributor",
			HeadBranch: "feat/oa", Labels: []string{"tatara"},
		}}}
		reg := prometheus.NewRegistry()
		r := &ProjectReconciler{Client: c, Scheme: c.Scheme(), Metrics: obs.NewOperatorMetrics(reg)}
		if _, err := r.SweepProject(context.Background(), proj, rd, []tatarav1alpha1.Repository{*repo}, nil, SweepActivity); err != nil {
			t.Fatalf("SweepProject: %v", err)
		}
		if got := testutil.ToFloat64(r.Metrics.OrphanAdoptedCounter(SweepReviewKind)); got != 1 {
			t.Fatalf("operator_orphan_adopted_total{%s} = %v, want 1", SweepReviewKind, got)
		}
	})
}

// TestSweepHumanCommentMintsTriaging: a human comment on a backlog issue mints
// an ACTIVE Task.
func TestSweepHumanCommentMintsTriaging(t *testing.T) {
	proj := sweepProject("human-proj")
	repo := sweepRepo("human-proj")
	c := newMirrorClient(t, proj, repo)
	now := time.Now()
	rd := &sweepReader{
		issues:  []scm.IssueRef{{Repo: "szymonrychu/tatara-operator", Number: 7, Author: "alice", State: "open"}},
		content: map[int]scm.IssueContent{7: {Title: "slow query", Body: "it is slow"}},
		comments: map[int][]scm.IssueComment{7: {
			humanComment("1", "tatara-bot", "triaged", now.Add(-time.Hour)),
			humanComment("2", "alice", "still broken", now),
		}},
	}

	runSweep(t, c, proj, repo, rd)

	tasks := sweepTasks(t, c, proj.Name)
	if len(tasks) != 1 {
		t.Fatalf("tasks = %d, want 1", len(tasks))
	}
	if tasks[0].Spec.InitialState != tatarav1alpha1.StateNew {
		t.Fatalf("initialStage = %q, want triaging", tasks[0].Spec.InitialState)
	}
	if tasks[0].Spec.InitialParkReason != "" {
		t.Fatalf("initialStageReason = %q, want empty on an ACTIVE mint", tasks[0].Spec.InitialParkReason)
	}
}

// TestSweepUnauthorizedReporterMintsNoTask is clause (c) end to end: an open,
// unowned issue from a NON-allowlisted author mints NO Task; with an empty
// allowlist the same issue mints one.
func TestSweepUnauthorizedReporterMintsNoTask(t *testing.T) {
	repo := sweepRepo("gate-proj")
	injected := []scm.IssueRef{{Repo: "szymonrychu/tatara-operator", Number: 9, Author: "mallory", State: "open"}}

	gated := sweepProject("gate-proj")
	gated.Spec.Scm.ReporterLogins = []string{"alice"}
	c := newMirrorClient(t, gated, repo)
	runSweep(t, c, gated, repo, &sweepReader{issues: injected})
	if n := len(sweepTasks(t, c, gated.Name)); n != 0 {
		t.Fatalf("tasks = %d, want 0 (an injected issue never becomes a Task)", n)
	}

	open := sweepProject("open-proj")
	repo2 := sweepRepo("open-proj")
	c2 := newMirrorClient(t, open, repo2)
	runSweep(t, c2, open, repo2, &sweepReader{issues: injected})
	if n := len(sweepTasks(t, c2, open.Name)); n != 1 {
		t.Fatalf("tasks = %d, want 1 (an empty allowlist preserves the open default)", n)
	}
}

// TestSweepReapLoopNeverGoesActive is THE LOOP TEST.
//
// The reaper collected a parked(identity-unverified) Task and stamped the SCM
// issue with tatara-parked. Its bot park COMMENT is not the last word (the M25
// scenario: a 403 on a secondary limit). Keying "active vs parked" on "does the
// bot have the last word" would mint ACTIVE here, the pod would re-triage, it
// would park again - the exact loop this exists to kill. The predicate reads
// TASK HISTORY (the durable label), not a comment.
//
// Three passes against the same fake forge: the Task count does not grow.
func TestSweepReapLoopNeverGoesActive(t *testing.T) {
	proj := sweepProject("loop-proj")
	repo := sweepRepo("loop-proj")
	// The ownerless survivor of the reaped Task: fix H13 drops the ownerRef, and
	// per B.1 a zero-owner object is NEVER garbage collected. It is still there.
	survivor := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: tatarav1alpha1.IssueName(repo.Name, 42), Namespace: testNS},
		Spec: tatarav1alpha1.IssueSpec{
			RepositoryRef: repo.Name, Number: 42, ProjectRef: proj.Name,
			URL: "https://github.com/szymonrychu/tatara-operator/issues/42",
		},
	}
	c := newMirrorClient(t, proj, repo, survivor)
	now := time.Now()
	rd := &sweepReader{
		issues: []scm.IssueRef{{
			Repo: "szymonrychu/tatara-operator", Number: 42, Author: "alice", State: "open",
			Labels: []string{TataraParkedLabel},
		}},
		content: map[int]scm.IssueContent{42: {Title: "needs approval", Body: "please"}},
		comments: map[int][]scm.IssueComment{42: {
			humanComment("1", "tatara-bot", "I cannot verify the approver", now.Add(-2*time.Hour)),
			humanComment("2", "alice", "go ahead", now.Add(-time.Hour)),
		}},
	}

	for pass := 1; pass <= 3; pass++ {
		runSweep(t, c, proj, repo, rd)
		tasks := sweepTasks(t, c, proj.Name)
		if len(tasks) != 1 {
			t.Fatalf("pass %d: tasks = %d, want 1 (the sweep must not re-mint an owned issue)", pass, len(tasks))
		}
		tk := tasks[0]
		if tk.Spec.InitialState != tatarav1alpha1.StateNew || tk.Spec.InitialParkReason != stage.ReasonBacklogSweep {
			t.Fatalf("pass %d: initialStage = %q/%q, want parked/backlog-sweep (NEVER active)",
				pass, tk.Spec.InitialState, tk.Spec.InitialParkReason)
		}
	}

	// The mint ADOPTED the ownerless survivor: a blind Create would have
	// AlreadyExists'd on every re-mint of every previously-failed Task.
	var iss tatarav1alpha1.Issue
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: testNS, Name: tatarav1alpha1.IssueName(repo.Name, 42)}, &iss); err != nil {
		t.Fatalf("get issue CR: %v", err)
	}
	if _, ok := own.ControllerOwner(&iss); !ok {
		t.Fatal("the adopted Issue CR has no controller owner")
	}
	if iss.Status.Title != "needs approval" {
		t.Fatalf("issue mirror title = %q, want the synced forge title", iss.Status.Title)
	}
}

// TestMintAdoptsOwnerlessIssueCR: the mint is ADOPT-OR-CREATE on the Issue CR,
// never a blind Create. A failed Task RELEASES its controller-ownership and
// drops the ownerRef, leaving a zero-owner Issue CR that is never GC'd.
func TestMintAdoptsOwnerlessIssueCR(t *testing.T) {
	proj := sweepProject("adopt-proj")
	repo := sweepRepo("adopt-proj")
	survivor := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: tatarav1alpha1.IssueName(repo.Name, 3), Namespace: testNS},
		Spec: tatarav1alpha1.IssueSpec{
			RepositoryRef: repo.Name, Number: 3, ProjectRef: proj.Name,
			URL: "https://github.com/szymonrychu/tatara-operator/issues/3",
		},
	}
	survivor.Status.Comments = []tatarav1alpha1.Comment{{
		ExternalID: "old", Author: "alice", Body: "the original report",
		CreatedAt: metav1.NewTime(time.Now().Add(-48 * time.Hour)),
	}}
	c := newMirrorClient(t, proj, repo, survivor)
	rd := &sweepReader{
		issues:  []scm.IssueRef{{Repo: "szymonrychu/tatara-operator", Number: 3, Author: "alice", State: "open"}},
		content: map[int]scm.IssueContent{3: {Title: "t", Body: "b"}},
	}

	runSweep(t, c, proj, repo, rd)

	tasks := sweepTasks(t, c, proj.Name)
	if len(tasks) != 1 {
		t.Fatalf("tasks = %d, want 1", len(tasks))
	}
	var iss tatarav1alpha1.Issue
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: testNS, Name: survivor.Name}, &iss); err != nil {
		t.Fatalf("get issue CR: %v", err)
	}
	owner, ok := own.ControllerOwner(&iss)
	if !ok || owner != tasks[0].Name {
		t.Fatalf("controller owner = %q/%v, want %q", owner, ok, tasks[0].Name)
	}
	if len(iss.Status.Comments) != 1 || iss.Status.Comments[0].ExternalID != "old" {
		t.Fatalf("the adopted CR lost its mirrored thread: %+v", iss.Status.Comments)
	}
}

// TestAdoptPR pins the THREE adoption clauses. Clause (c) is what stops an
// outside contributor from injecting an MR into a trusted Task's merge stream: a
// fork PR may name its head branch anything, INCLUDING task/<a-real-task>.
func TestAdoptPR(t *testing.T) {
	proj := sweepProject("adopt-pr-proj")
	task := &tatarav1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "t-1", Namespace: testNS}}
	base := "szymonrychu/tatara-operator"
	wantBranch := agent.TaskBranch(task)

	tests := map[string]struct {
		pr   scm.PRRef
		task *tatarav1alpha1.Task
		want bool
	}{
		"bot, task branch, same repo": {
			pr:   scm.PRRef{Repo: base, HeadRepo: base, Author: "tatara-bot", HeadBranch: wantBranch},
			task: task, want: true,
		},
		"clause a: a human on the Task's own branch is NOT adopted": {
			pr:   scm.PRRef{Repo: base, HeadRepo: base, Author: "mallory", HeadBranch: wantBranch},
			task: task, want: false,
		},
		"clause b: the bot on some other branch is NOT adopted": {
			pr:   scm.PRRef{Repo: base, HeadRepo: base, Author: "tatara-bot", HeadBranch: "chore/bump"},
			task: task, want: false,
		},
		"clause c: a FORK PR on a task/* branch is NOT adopted": {
			pr:   scm.PRRef{Repo: base, HeadRepo: "mallory/tatara-operator", Author: "tatara-bot", HeadBranch: wantBranch},
			task: task, want: false,
		},
		"clause c: an UNKNOWN head repo fails CLOSED": {
			pr:   scm.PRRef{Repo: base, HeadRepo: "", Author: "tatara-bot", HeadBranch: wantBranch},
			task: task, want: false,
		},
		"no owning Task": {
			pr:   scm.PRRef{Repo: base, HeadRepo: base, Author: "tatara-bot", HeadBranch: wantBranch},
			task: nil, want: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := AdoptPR(proj, tc.task, tc.pr); got != tc.want {
				t.Fatalf("AdoptPR = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestTaskForBranch pins fix A3's rewrite: taskForBranch now resolves a
// branch by SCANNING the project's Tasks and matching agent.TaskBranch(t) ==
// branch (TaskBranchPrefix + CutPrefix is gone - the branch string no longer
// maps 1:1 to a Task's own metadata.name for a numbered or documentation
// Task). N tasks, an unknown branch, and a same-name Task in a DIFFERENT
// project must all still resolve correctly (issue #381 bug A).
func TestTaskForBranch(t *testing.T) {
	ctx := context.Background()
	proj := sweepProject("branch-proj")
	other := sweepProject("branch-proj-2")

	numbered := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "branch-proj-review-tatara-cli-87", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: proj.Name, Kind: "review",
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "https://github.com/acme/tatara-cli/pull/87", Number: 87, IsPR: true, Title: "fix the thing"},
		},
	}
	fallback := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "branch-proj-clarify-x", Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, Kind: "clarify"},
	}
	crossProject := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "branch-proj-2-clarify-y", Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: other.Name, Kind: "clarify"},
	}
	c := newMirrorClient(t, proj, other, numbered, fallback, crossProject)
	// No standalone reconciler constructor exists for this package's sweep
	// tests - runSweep (sweep_test.go:104) builds one inline the same way;
	// taskForBranch needs no SCMReader, so Metrics is the only field it touches.
	r := &ProjectReconciler{Client: c, Scheme: c.Scheme(), Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry())}

	got, err := r.taskForBranch(ctx, proj, agent.TaskBranch(numbered), "")
	if err != nil || got == nil || got.Name != numbered.Name {
		t.Fatalf("taskForBranch(numbered) = %v, %v, want %s", got, err, numbered.Name)
	}
	got, err = r.taskForBranch(ctx, proj, agent.TaskBranch(fallback), "")
	if err != nil || got == nil || got.Name != fallback.Name {
		t.Fatalf("taskForBranch(fallback) = %v, %v, want %s", got, err, fallback.Name)
	}
	got, err = r.taskForBranch(ctx, proj, "tatara/task-does-not-exist", "")
	if err != nil || got != nil {
		t.Fatalf("taskForBranch(unknown) = %v, %v, want nil, nil", got, err)
	}
	// crossProject's branch resolves to nil when queried against `proj`, not
	// `other`: a same-named branch in a sibling project must never leak in.
	got, err = r.taskForBranch(ctx, proj, agent.TaskBranch(crossProject), "")
	if err != nil || got != nil {
		t.Fatalf("taskForBranch(cross-project) = %v, %v, want nil, nil", got, err)
	}
}

// TestSweepAdoptsBotPRIntoOwningTask: a bot PR on the Task's own branch is
// adopted into that Task's mrRefs and the MergeRequest CR is owned by it.
func TestSweepAdoptsBotPRIntoOwningTask(t *testing.T) {
	proj := sweepProject("pradopt-proj")
	repo := sweepRepo("pradopt-proj")
	owner := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "pradopt-proj-clarify-x", Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, Kind: "clarify", Goal: "g"},
	}
	owner.Status.State = tatarav1alpha1.StateUnderImplementation
	c := newMirrorClient(t, proj, repo, owner)
	rd := &sweepReader{prs: []scm.PRRef{{
		Repo: "szymonrychu/tatara-operator", HeadRepo: "szymonrychu/tatara-operator",
		Number: 11, Author: "tatara-bot", HeadBranch: agent.TaskBranch(owner), HeadSHA: "deadbeef",
	}}}

	runSweep(t, c, proj, repo, rd)

	if n := len(sweepTasks(t, c, proj.Name)); n != 1 {
		t.Fatalf("tasks = %d, want 1 (adoption mints NOTHING)", n)
	}
	var mr tatarav1alpha1.MergeRequest
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: testNS, Name: tatarav1alpha1.MergeRequestName(repo.Name, 11)}, &mr); err != nil {
		t.Fatalf("get mergerequest CR: %v", err)
	}
	if got, ok := own.ControllerOwner(&mr); !ok || got != owner.Name {
		t.Fatalf("MR controller owner = %q/%v, want %q", got, ok, owner.Name)
	}
	var fresh tatarav1alpha1.Task
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: owner.Name}, &fresh); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if len(fresh.Status.MRRefs) != 1 || fresh.Status.MRRefs[0] != mr.Name {
		t.Fatalf("task mrRefs = %v, want [%s]", fresh.Status.MRRefs, mr.Name)
	}
}

// TestSweepForkPROnTaskBranchIsNotAdopted: end to end, a fork PR naming a real
// Task's branch is neither adopted nor turned into anything else.
func TestSweepForkPROnTaskBranchIsNotAdopted(t *testing.T) {
	proj := sweepProject("fork-proj")
	repo := sweepRepo("fork-proj")
	owner := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "fork-proj-clarify-y", Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, Kind: "clarify", Goal: "g"},
	}
	owner.Status.State = tatarav1alpha1.StateUnderImplementation
	c := newMirrorClient(t, proj, repo, owner)
	rd := &sweepReader{prs: []scm.PRRef{{
		Repo: "szymonrychu/tatara-operator", HeadRepo: "mallory/tatara-operator",
		Number: 12, Author: "tatara-bot", HeadBranch: agent.TaskBranch(owner),
	}}}

	runSweep(t, c, proj, repo, rd)

	var fresh tatarav1alpha1.Task
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: owner.Name}, &fresh); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if len(fresh.Status.MRRefs) != 0 {
		t.Fatalf("a FORK PR was injected into a trusted Task's merge stream: %v", fresh.Status.MRRefs)
	}
	if n := len(sweepTasks(t, c, proj.Name)); n != 1 {
		t.Fatalf("tasks = %d, want 1 (the fork PR minted something)", n)
	}
}

// TestSweepBotPRNotAdoptableIsIgnored is clause 2: BOT-AUTHORED and NOT
// ADOPTABLE -> IGNORE. FULL STOP. No Task, no pod, no tokens, NEVER a review
// Task. prInReactionScope does NOT close this hole: it returns true immediately
// for IsTrustedAuthor, and the bot IS a trusted author.
//
// Two real populations: (a) ORPHANED AGENT PRs, whose review Task the flaky
// review agent approves - and the author check PASSES, because the author IS the
// bot - shipping an abandoned, never-approved change through push-CD; and (b) CI
// PIN-BUMP PRs (tatara-helmfile's cd-release bump PR on every release, the
// wrapper's daily refresh-claude-code PR), each of which would eat the
// maxOpenTasks budget and RACE GitHub's own auto-merge.
func TestSweepBotPRNotAdoptableIsIgnored(t *testing.T) {
	tests := map[string]scm.PRRef{
		"an orphaned agent PR whose Task is gone": {
			Repo: "szymonrychu/tatara-operator", HeadRepo: "szymonrychu/tatara-operator",
			Number: 20, Author: "tatara-bot", HeadBranch: "tatara/task-a-task-that-no-longer-exists",
		},
		"a CI pin-bump PR": {
			Repo: "szymonrychu/tatara-operator", HeadRepo: "szymonrychu/tatara-operator",
			Number: 21, Author: "tatara-bot", HeadBranch: "chore/bump-tatara-cli-v1.2.3",
			Body: "Automated pin bump", Labels: []string{"semver:patch"},
		},
	}
	for name, pr := range tests {
		t.Run(name, func(t *testing.T) {
			proj := sweepProject("botpr-proj")
			repo := sweepRepo("botpr-proj")
			c := newMirrorClient(t, proj, repo)

			runSweep(t, c, proj, repo, &sweepReader{prs: []scm.PRRef{pr}})

			if n := len(sweepTasks(t, c, proj.Name)); n != 0 {
				t.Fatalf("tasks = %d, want 0 (a bot-authored non-adoptable PR is IGNORED, FULL STOP)", n)
			}
			// Not touched at all: no MergeRequest CR, so nothing owns it, nothing
			// reviews it, nothing merges it, and nothing races the forge's own
			// auto-merge on it.
			var mrl tatarav1alpha1.MergeRequestList
			if err := c.List(context.Background(), &mrl, client.InNamespace(testNS)); err != nil {
				t.Fatalf("list mergerequests: %v", err)
			}
			if len(mrl.Items) != 0 {
				t.Fatalf("mergerequest CRs = %d, want 0", len(mrl.Items))
			}
		})
	}
}

// TestSweepHumanPRReactionScope: a HUMAN-authored PR mints a review-kind Task
// iff prInReactionScope. The predicate's doc-comment names the incident it was
// written for - the !1090 token-burn loop, where the bot re-reviewed every
// unlabeled, un-mentioned MR on every scan cycle.
func TestSweepHumanPRReactionScope(t *testing.T) {
	base := "szymonrychu/tatara-operator"
	tests := map[string]struct {
		labels    []string
		body      string
		wantTasks int
	}{
		"outside the reaction scope: NO review Task": {wantTasks: 0},
		"carries the trigger label":                  {labels: []string{"tatara"}, wantTasks: 1},
		"@-mentions the bot":                         {body: "@tatara-bot please look", wantTasks: 1},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			proj := sweepProject("scope-proj")
			proj.Spec.TriggerLabel = "tatara"
			proj.Spec.Scm.PRReactionScope = "labeledOrMentioned"
			repo := sweepRepo("scope-proj")
			c := newMirrorClient(t, proj, repo)
			rd := &sweepReader{prs: []scm.PRRef{{
				Repo: base, HeadRepo: base, Number: 30, Author: "contributor",
				HeadBranch: "feat/x", Labels: tc.labels, Body: tc.body,
			}}}

			runSweep(t, c, proj, repo, rd)

			tasks := sweepTasks(t, c, proj.Name)
			if len(tasks) != tc.wantTasks {
				t.Fatalf("tasks = %d, want %d", len(tasks), tc.wantTasks)
			}
			if tc.wantTasks == 1 {
				if tasks[0].Spec.Kind != SweepReviewKind {
					t.Fatalf("kind = %q, want review", tasks[0].Spec.Kind)
				}
				if tasks[0].Spec.InitialState != tatarav1alpha1.StateNew {
					t.Fatalf("initialStage = %q, want triaging", tasks[0].Spec.InitialState)
				}
				if len(tasks[0].Status.MRRefs) != 1 {
					t.Fatalf("mrRefs = %v, want the reviewed MR", tasks[0].Status.MRRefs)
				}
			}
		})
	}
}

// ============================================================================
// The MERGEREQUEST orphan / re-mint gap. The reaper now leaves a human's
// still-open PR its mirror (OWNERLESS, so it is never GC'd and IS re-mintable);
// these are the sweep's half of that contract.
// ============================================================================

// TestMintReviewStage pins the re-mint disposition, and it turns on ONE
// question: HAS A REVIEW ALREADY BEEN POSTED ON THIS PR?
//
// status.status is OPERATOR-OWNED and written only when a review LANDED on the
// forge (C.5.3 clearPendingReview), so it is the durable record of "we already
// said our piece". "new" is the head-moved reset: a fresh review IS owed.
func TestMintReviewStage(t *testing.T) {
	withStatus := func(s string) *tatarav1alpha1.MergeRequest {
		mr := &tatarav1alpha1.MergeRequest{}
		mr.Status.Status = s
		return mr
	}
	tests := map[string]struct {
		cr         *tatarav1alpha1.MergeRequest
		wantStage  string
		wantReason string
	}{
		"no CR at all: a PR we have never seen mints ACTIVE": {
			cr: nil, wantStage: tatarav1alpha1.StateNew,
		},
		"a CR with no posted verdict: the review never landed, so RUN it": {
			cr: withStatus(""), wantStage: tatarav1alpha1.StateNew,
		},
		"status=new: the head MOVED and a FRESH review is owed": {
			cr: withStatus("new"), wantStage: tatarav1alpha1.StateNew,
		},
		"status=needs-changes: the review WAS posted -> PARKED, no re-review": {
			cr: withStatus("needs-changes"), wantStage: tatarav1alpha1.StateNew,
			wantReason: stage.ReasonAwaitingHuman,
		},
		"status=approved: the review WAS posted -> PARKED, no re-review": {
			cr: withStatus("approved"), wantStage: tatarav1alpha1.StateNew,
			wantReason: stage.ReasonAwaitingHuman,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			gotStage, gotReason := MintReviewStage(tc.cr)
			if gotStage != tc.wantStage || gotReason != tc.wantReason {
				t.Fatalf("MintReviewStage = (%q, %q), want (%q, %q)",
					gotStage, gotReason, tc.wantStage, tc.wantReason)
			}
		})
	}
}

// humanPRMirror is the ownerless MergeRequest CR the reaper leaves behind: the
// human's PR is still OPEN on the forge and a review has already been posted on
// it.
func humanPRMirror(proj, repo string, number int, rounds string) *tatarav1alpha1.MergeRequest {
	mr := &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name: tatarav1alpha1.MergeRequestName(repo, number), Namespace: testNS,
		},
		Spec: tatarav1alpha1.MergeRequestSpec{
			RepositoryRef: repo, Number: number, ProjectRef: proj,
			URL: "https://github.com/szymonrychu/tatara-operator/pull/50",
		},
	}
	if rounds != "" {
		mr.Annotations = map[string]string{AnnHumanReviewRounds: rounds}
	}
	mr.Status.Author = "contributor"
	mr.Status.HeadBranch = "fix/their-branch"
	mr.Status.State = "open"
	mr.Status.Status = "needs-changes"
	mr.Status.ReviewRounds = 2
	mr.Status.Comments = []tatarav1alpha1.Comment{{
		ExternalID: "old-review", Author: "tatara-bot", Body: "## Review: needs-changes",
		CreatedAt: metav1.NewTime(time.Now().Add(-8 * 24 * time.Hour)),
	}}
	return mr
}

func humanPR(number int) scm.PRRef {
	base := "szymonrychu/tatara-operator"
	return scm.PRRef{
		Repo: base, HeadRepo: "contributor/tatara-operator", Number: number,
		Author: "contributor", HeadBranch: "fix/their-branch", HeadSHA: "cafe",
	}
}

// TestSweepAdoptsOwnerlessMergeRequestCR is the MR analogue of
// TestMintAdoptsOwnerlessIssueCR: the re-mint ADOPTS the surviving mirror rather
// than creating a duplicate, and it comes back PARKED - a review was already
// posted on this PR, so re-running one would just re-post it.
func TestSweepAdoptsOwnerlessMergeRequestCR(t *testing.T) {
	proj := sweepProject("mradopt-proj")
	repo := sweepRepo("mradopt-proj")
	survivor := humanPRMirror(proj.Name, repo.Name, 50, "2")
	c := newMirrorClient(t, proj, repo, survivor)

	runSweep(t, c, proj, repo, &sweepReader{prs: []scm.PRRef{humanPR(50)}})

	tasks := sweepTasks(t, c, proj.Name)
	if len(tasks) != 1 {
		t.Fatalf("tasks = %d, want 1", len(tasks))
	}
	tk := tasks[0]
	if tk.Spec.Kind != SweepReviewKind {
		t.Fatalf("kind = %q, want review", tk.Spec.Kind)
	}
	if tk.Spec.InitialState != tatarav1alpha1.StateNew || tk.Spec.InitialParkReason != stage.ReasonAwaitingHuman {
		t.Fatalf("initialStage = %q/%q, want parked/awaiting-human (the review was ALREADY posted)",
			tk.Spec.InitialState, tk.Spec.InitialParkReason)
	}
	if tk.Status.PodName != "" {
		t.Fatalf("the re-minted review Task spawned a pod: %q", tk.Status.PodName)
	}
	// The V7-9 counter survived the reap: 3 rounds left, not 5.
	if tk.Status.HumanReviewRounds != 2 {
		t.Fatalf("humanReviewRounds = %d, want 2 carried from the surviving mirror", tk.Status.HumanReviewRounds)
	}

	// ADOPT, never a blind Create: ONE MergeRequest CR, and it is the SAME one -
	// the mirrored review thread is still on it.
	var mrl tatarav1alpha1.MergeRequestList
	if err := c.List(context.Background(), &mrl, client.InNamespace(testNS)); err != nil {
		t.Fatalf("list mergerequests: %v", err)
	}
	if len(mrl.Items) != 1 {
		t.Fatalf("mergerequest CRs = %d, want exactly 1 (the mint DOUBLE-MINTED)", len(mrl.Items))
	}
	got := mrl.Items[0]
	owner, ok := own.ControllerOwner(&got)
	if !ok || owner != tk.Name {
		t.Fatalf("the adopted MR CR's controller owner = %q/%v, want %q", owner, ok, tk.Name)
	}
	if len(got.Status.Comments) != 1 || got.Status.Comments[0].ExternalID != "old-review" {
		t.Fatalf("the adopted CR lost its mirrored review thread: %+v", got.Status.Comments)
	}
	if len(tk.Status.MRRefs) != 1 || tk.Status.MRRefs[0] != got.Name {
		t.Fatalf("task mrRefs = %v, want [%s]", tk.Status.MRRefs, got.Name)
	}
}

// TestSweepNeverReReviewsAHumanPR IS THE LOOP TEST, and it is the MergeRequest
// twin of TestSweepReapLoopNeverGoesActive.
//
// The cycle it must NOT create: a contributor opens a PR, the review agent
// requests changes, the Task parks at awaiting-human, seven days pass, B.6 reaps
// the Task, the sweep re-mints - and the new review pod RE-POSTS the same review
// on a PR nobody touched. Every seven days. Forever.
//
// Three full reap -> sweep cycles against the same fake forge. The Task count
// never grows, the MergeRequest CR is never duplicated, no minted Task is ever
// ACTIVE, and the forge is never written to (the reaper's writer panics on every
// method).
func TestSweepNeverReReviewsAHumanPR(t *testing.T) {
	ctx := context.Background()
	proj := sweepProject("noreloop")
	proj.Spec.Scm.BotLogin = "tatara-bot"
	repo := sweepRepo("noreloop")
	survivor := humanPRMirror(proj.Name, repo.Name, 50, "2")
	c := newMirrorClient(t, proj, repo, reapSecret(), survivor)

	w := &reapWriter{} // ZERO forge writes across every cycle
	r := &ProjectReconciler{
		Client: c, Scheme: c.Scheme(), Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		SCMFor: func(string) (scm.SCMWriter, error) { return w, nil },
	}
	rd := &sweepReader{prs: []scm.PRRef{humanPR(50)}}

	for cycle := 1; cycle <= 3; cycle++ {
		if _, err := r.SweepProject(ctx, proj, rd, []tatarav1alpha1.Repository{*repo}, nil, SweepActivity); err != nil {
			t.Fatalf("cycle %d: SweepProject: %v", cycle, err)
		}
		tasks := sweepTasks(t, c, proj.Name)
		if len(tasks) != 1 {
			t.Fatalf("cycle %d: tasks = %d, want 1 (the sweep re-minted on top of an owned PR)", cycle, len(tasks))
		}
		tk := tasks[0]
		if tk.Spec.InitialState != tatarav1alpha1.StateNew || tk.Spec.InitialParkReason != stage.ReasonAwaitingHuman {
			t.Fatalf("cycle %d: initialStage = %q/%q, want parked/awaiting-human: a re-minted review Task must NEVER be ACTIVE - it would re-post the review",
				cycle, tk.Spec.InitialState, tk.Spec.InitialParkReason)
		}
		// Project what the create-edge would apply from Spec.InitialState /
		// Spec.InitialParkReason (fix C5: the mint itself never stamps
		// Status.State/ParkReason) and check THAT is not active - the exact
		// question this assertion has always asked. #521 split the old single
		// stage value into two independent fields, so the projection must copy
		// BOTH: state alone no longer says whether a mint landed parked.
		projected := tk.DeepCopy()
		projected.Status.State = tk.Spec.InitialState
		projected.Status.ParkReason = tk.Spec.InitialParkReason
		if StageActive(projected) {
			t.Fatalf("cycle %d: the re-minted review Task is ACTIVE; it will spawn a review pod on a PR nobody touched", cycle)
		}
		if tk.Status.HumanReviewRounds != 2 {
			t.Fatalf("cycle %d: humanReviewRounds = %d, want 2: the V7-9 cap must survive the reap, or the PR gets 5 MORE review pods every 7 days",
				cycle, tk.Status.HumanReviewRounds)
		}

		// A second sweep pass inside the same cycle must be a total no-op: the PR is
		// now OWNED, so it is not an orphan and it mints NOTHING.
		if _, err := r.SweepProject(ctx, proj, rd, []tatarav1alpha1.Repository{*repo}, nil, SweepActivity); err != nil {
			t.Fatalf("cycle %d: SweepProject (second pass): %v", cycle, err)
		}
		if n := len(sweepTasks(t, c, proj.Name)); n != 1 {
			t.Fatalf("cycle %d: tasks after the second pass = %d, want 1 (an OWNED PR is not an orphan)", cycle, n)
		}

		// Drive the create-edge (fix C5) so the reaper - which reads Status.Stage,
		// not Spec.InitialState - sees this review Task as parked before it ages
		// out. In production the reconciler applies Spec.InitialState long before
		// seven days pass; this mirrors that sequencing.
		live := getSweepTask(t, c, tk.Name)
		tr := &TaskReconciler{Client: c, Metrics: r.Metrics}
		if _, err := tr.reconcileStage(ctx, proj, live, time.Now()); err != nil {
			t.Fatalf("cycle %d: drive create-edge: %v", cycle, err)
		}

		// Seven days pass. B.6 reaps the park. parkedAt() (reaper.go) prefers
		// status.parkedAt over stateEnteredAt once it is set - and the create-edge
		// drive above went through stage.Park, which stamps parkedAt to NOW - so
		// both timestamps must be rewound, or the reap gate reads the fresh
		// parkedAt and never ages out.
		aged := getSweepTask(t, c, tk.Name)
		rewound := metav1.NewTime(time.Now().Add(-8 * 24 * time.Hour))
		aged.Status.StateEnteredAt = &rewound
		aged.Status.ParkedAt = &rewound
		if err := c.Status().Update(ctx, aged); err != nil {
			t.Fatalf("cycle %d: rewind stateEnteredAt/parkedAt: %v", cycle, err)
		}
		if err := r.ReapTerminal(ctx, proj); err != nil {
			t.Fatalf("cycle %d: ReapTerminal: %v", cycle, err)
		}
		if n := len(sweepTasks(t, c, proj.Name)); n != 0 {
			t.Fatalf("cycle %d: tasks after the reap = %d, want 0", cycle, n)
		}
	}

	// ZERO forge writes, in every cycle. The human's PR was never closed, never
	// commented on, never re-reviewed, and their branch was never deleted.
	if len(w.closed) != 0 || len(w.deleted) != 0 || len(w.comments) != 0 || len(w.labels) != 0 {
		t.Fatalf("the forge was written to: closed=%v deleted=%v comments=%v labels=%v",
			w.closed, w.deleted, w.comments, w.labels)
	}
	// And EXACTLY ONE MergeRequest CR survived all three cycles.
	var mrl tatarav1alpha1.MergeRequestList
	if err := c.List(ctx, &mrl, client.InNamespace(testNS)); err != nil {
		t.Fatalf("list mergerequests: %v", err)
	}
	if len(mrl.Items) != 1 {
		t.Fatalf("mergerequest CRs = %d, want exactly 1 after three reap/sweep cycles", len(mrl.Items))
	}
}

// TestSweepDoesNotReMintOverAnOwnedHumanPR is the MR analogue of IsOrphanIssue's
// clause (b), and it closes a bug that fired on EVERY hourly pass: a human's PR
// never has a task/<name> head branch, so taskForBranch always returns nil for
// it, and ClassifyPR looked at nothing else. A PR under ACTIVE review therefore
// re-classified as PRReview on the very next pass - minting a second review Task
// whose ownMergeRequest then failed ("already has controller owner"), leaving a
// stage-less junk Task behind AND failing the whole sweep pass (which suppresses
// the heartbeat gauge the sweep-stalled alert reads).
func TestSweepDoesNotReMintOverAnOwnedHumanPR(t *testing.T) {
	proj := sweepProject("owned-pr-proj")
	repo := sweepRepo("owned-pr-proj")
	c := newMirrorClient(t, proj, repo)
	rd := &sweepReader{prs: []scm.PRRef{humanPR(60)}}

	// Pass 1 mints the review Task and takes ownership of the MR CR.
	runSweep(t, c, proj, repo, rd)
	tasks := sweepTasks(t, c, proj.Name)
	if len(tasks) != 1 || tasks[0].Spec.InitialState != tatarav1alpha1.StateNew {
		t.Fatalf("pass 1: tasks = %+v, want one triaging review Task", tasks)
	}

	// Passes 2 and 3 mint NOTHING and must not error: the MR CR now has a
	// controller owner, so the PR is not an orphan.
	for pass := 2; pass <= 3; pass++ {
		runSweep(t, c, proj, repo, rd)
		if n := len(sweepTasks(t, c, proj.Name)); n != 1 {
			t.Fatalf("pass %d: tasks = %d, want 1 (the sweep re-minted over a PR it is already reviewing)", pass, n)
		}
		var mrl tatarav1alpha1.MergeRequestList
		if err := c.List(context.Background(), &mrl, client.InNamespace(testNS)); err != nil {
			t.Fatalf("list mergerequests: %v", err)
		}
		if len(mrl.Items) != 1 {
			t.Fatalf("pass %d: mergerequest CRs = %d, want 1", pass, len(mrl.Items))
		}
	}
}

func getSweepTask(t *testing.T, c client.Client, name string) *tatarav1alpha1.Task {
	t.Helper()
	var tk tatarav1alpha1.Task
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, &tk); err != nil {
		t.Fatalf("get task %s: %v", name, err)
	}
	return &tk
}

// TestSweepMintCapsBind: both creation budgets bind. maxNewTasksPerSweep caps
// the pass; maxOpenTasks caps ACTIVE Tasks, and parked(backlog-sweep) Tasks are
// NOT active so they do not count against it. Remaining orphans are minted on
// the next pass - the predicate is stateless, nothing is lost.
func TestSweepMintCapsBind(t *testing.T) {
	t.Run("maxNewTasksPerSweep", func(t *testing.T) {
		proj := sweepProject("cap-new-proj")
		proj.Spec.MaxNewTasksPerSweep = 2
		repo := sweepRepo("cap-new-proj")
		c := newMirrorClient(t, proj, repo)
		rd := &sweepReader{}
		for n := 1; n <= 5; n++ {
			rd.issues = append(rd.issues, scm.IssueRef{
				Repo: "szymonrychu/tatara-operator", Number: n, Author: "alice", State: "open"})
		}
		before := testutil.ToFloat64(obs.SweepMintCapHitTotal.WithLabelValues(proj.Name, obs.SweepCapMaxNewTasksPerSweep))

		runSweep(t, c, proj, repo, rd)

		if n := len(sweepTasks(t, c, proj.Name)); n != 2 {
			t.Fatalf("tasks = %d, want 2 (maxNewTasksPerSweep)", n)
		}
		after := testutil.ToFloat64(obs.SweepMintCapHitTotal.WithLabelValues(proj.Name, obs.SweepCapMaxNewTasksPerSweep))
		if after <= before {
			t.Fatalf("operator_sweep_mint_cap_hit_total{cap=maxNewTasksPerSweep} did not increment (%v -> %v)", before, after)
		}

		// The remaining orphans are minted on the NEXT pass.
		runSweep(t, c, proj, repo, rd)
		if n := len(sweepTasks(t, c, proj.Name)); n != 4 {
			t.Fatalf("tasks after pass 2 = %d, want 4", n)
		}
	})

	t.Run("maxOpenTasks caps ACTIVE mints only", func(t *testing.T) {
		proj := sweepProject("cap-open-proj")
		proj.Spec.MaxOpenTasks = 1
		repo := sweepRepo("cap-open-proj")
		// One ACTIVE Task already: the budget is spent.
		live := &tatarav1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{Name: "cap-open-proj-live", Namespace: testNS},
			Spec:       tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, Kind: "clarify", Goal: "g"},
		}
		live.Status.State = tatarav1alpha1.StateUnderImplementation
		c := newMirrorClient(t, proj, repo, live)
		now := time.Now()
		rd := &sweepReader{
			issues: []scm.IssueRef{
				{Repo: "szymonrychu/tatara-operator", Number: 1, Author: "alice", State: "open"},
				{Repo: "szymonrychu/tatara-operator", Number: 2, Author: "alice", State: "open"},
			},
			comments: map[int][]scm.IssueComment{
				// #1 wants an ACTIVE mint (human last word) and must be REFUSED.
				1: {humanComment("c1", "alice", "still broken", now)},
			},
		}
		before := testutil.ToFloat64(obs.SweepMintCapHitTotal.WithLabelValues(proj.Name, obs.SweepCapMaxOpenTasks))

		runSweep(t, c, proj, repo, rd)

		tasks := sweepTasks(t, c, proj.Name)
		if len(tasks) != 2 {
			t.Fatalf("tasks = %d, want 2 (the live one + the parked backlog mint)", len(tasks))
		}
		for i := range tasks {
			if tasks[i].Name == live.Name {
				continue
			}
			if tasks[i].Spec.InitialState != tatarav1alpha1.StateNew {
				t.Fatalf("minted initialStage = %q, want parked (maxOpenTasks is spent)", tasks[i].Spec.InitialState)
			}
		}
		after := testutil.ToFloat64(obs.SweepMintCapHitTotal.WithLabelValues(proj.Name, obs.SweepCapMaxOpenTasks))
		if after <= before {
			t.Fatalf("operator_sweep_mint_cap_hit_total{cap=maxOpenTasks} did not increment (%v -> %v)", before, after)
		}
	})
}

// TestSweepHeartbeat: a clean pass stamps the heartbeat gauge. For a heartbeat,
// NoData IS the failure (the alert sets noDataState: Alerting), so the gauge is
// only ever stamped by a pass that actually completed.
func TestSweepHeartbeat(t *testing.T) {
	proj := sweepProject("hb-proj")
	repo := sweepRepo("hb-proj")
	c := newMirrorClient(t, proj, repo)

	obs.SweepLastSuccessTimestamp.WithLabelValues("hb-proj", SweepActivity).Set(0)
	runSweep(t, c, proj, repo, &sweepReader{})

	if got := testutil.ToFloat64(obs.SweepLastSuccessTimestamp.WithLabelValues("hb-proj", SweepActivity)); got <= 0 {
		t.Fatalf("operator_sweep_last_success_timestamp_seconds{activity=sweep} = %v, want a stamped timestamp", got)
	}
}

// TestSweepHeartbeatStampsDespitePerItemError: the heartbeat is a LIVENESS
// signal, not a zero-error one. A pass that hits a per-item error (one issue's
// comment read fails, or one stale MergeRequest CR read errors) records that
// error separately (SweepErrorsTotal) AND still stamps the heartbeat. Coupling
// the heartbeat to a fully-clean pass meant a single transient forge error - or
// one missing/stale CR - silenced the heartbeat for the WHOLE pass, and with the
// gauge reset on every restart the NoData(Alerting) alert then fired while the
// sweep was in fact running. The error is STILL returned for the reconciler's
// requeue; only the heartbeat is decoupled from it.
func TestSweepHeartbeatStampsDespitePerItemError(t *testing.T) {
	const activity = "sweep-hb-peritem-test"
	proj := sweepProject("hb-err-proj")
	repo := sweepRepo("hb-err-proj")
	c := newMirrorClient(t, proj, repo)

	rd := &sweepReader{
		issues:          []scm.IssueRef{{Repo: "szymonrychu/tatara-operator", Number: 1, Author: "alice", State: "open"}},
		listCommentsErr: errors.New("injected forge failure"),
	}

	obs.SweepLastSuccessTimestamp.WithLabelValues("hb-err-proj", activity).Set(0)
	r := &ProjectReconciler{Client: c, Scheme: c.Scheme(), Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry())}
	_, err := r.SweepProject(context.Background(), proj, rd, []tatarav1alpha1.Repository{*repo}, nil, activity)
	if err == nil {
		t.Fatal("SweepProject returned nil, want the per-item error propagated for the reconciler requeue")
	}
	if got := testutil.ToFloat64(obs.SweepLastSuccessTimestamp.WithLabelValues("hb-err-proj", activity)); got <= 0 {
		t.Fatalf("heartbeat = %v, want it stamped despite the per-item error (liveness is not zero-error)", got)
	}
}

// TestSweepHeartbeatSuppressedOnHardFailure: the OTHER edge. When the pass
// cannot even begin - activeTaskCount fails, so the sweep returns before the
// repos loop - the heartbeat is NOT stamped and SweepErrorsTotal records the
// list_tasks failure. This is the case the alert exists to catch: a sweep that
// genuinely is not running.
func TestSweepHeartbeatSuppressedOnHardFailure(t *testing.T) {
	const activity = "sweep-hb-hardfail-test"
	proj := sweepProject("hb-hard-proj")
	repo := sweepRepo("hb-hard-proj")

	c := fake.NewClientBuilder().
		WithScheme(mirrorScheme(t)).
		WithObjects(proj, repo).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, cli client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*tatarav1alpha1.TaskList); ok {
					return errors.New("injected task-list failure")
				}
				return cli.List(ctx, list, opts...)
			},
		}).
		Build()

	before := testutil.ToFloat64(obs.SweepErrorsTotal.WithLabelValues("hb-hard-proj", activity, "list_tasks"))
	obs.SweepLastSuccessTimestamp.WithLabelValues("hb-hard-proj", activity).Set(0)
	r := &ProjectReconciler{Client: c, Scheme: c.Scheme(), Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry())}
	_, err := r.SweepProject(context.Background(), proj, &sweepReader{}, []tatarav1alpha1.Repository{*repo}, nil, activity)
	if err == nil {
		t.Fatal("SweepProject returned nil, want the activeTaskCount failure")
	}
	if got := testutil.ToFloat64(obs.SweepLastSuccessTimestamp.WithLabelValues("hb-hard-proj", activity)); got != 0 {
		t.Fatalf("heartbeat = %v, want 0 (a sweep that cannot run must NOT stamp liveness)", got)
	}
	if after := testutil.ToFloat64(obs.SweepErrorsTotal.WithLabelValues("hb-hard-proj", activity, "list_tasks")); after <= before {
		t.Fatalf("operator_sweep_errors_total{reason=list_tasks} did not increment (%v -> %v)", before, after)
	}
}

// TestSweepEnabledByDefault: the cutover deleted issueScan/mrScan/backstop, so
// the sweep is the ONLY intake and an ABSENT annotation cannot mean "no intake".
// The annotation survives only as an explicit per-project break-glass OFF.
func TestSweepEnabledByDefault(t *testing.T) {
	proj := sweepProject("default-proj")
	proj.Annotations = nil
	if !SweepEnabled(proj) {
		t.Fatal("the sweep is OFF without the annotation; it must be ON by default")
	}
	on := sweepProject("on-proj")
	if !SweepEnabled(on) {
		t.Fatal("the sweep is OFF with the annotation explicitly enabled")
	}
	off := sweepProject("off-proj")
	off.Annotations = map[string]string{SweepAnnotation: SweepDisabledValue}
	if SweepEnabled(off) {
		t.Fatal("the sweep is ON with the break-glass annotation set to disabled")
	}
}

func boolp(b bool) *bool { return &b }

// ---------------------------------------------------------------------------
// THE WEBHOOK-ORIGINATED MARKER. THE PLATFORM'S FRONT DOOR.
//
// A human opens an issue and the platform DOES SOMETHING. Without the marker a
// freshly-opened human issue (open, human-authored, ZERO comments, no
// tatara-parked label) is byte-for-byte indistinguishable from a three-year-old
// untouched backlog issue, and the sweep parks BOTH: the loop in the README
// ("SCM webhook -> the operator turns a labelled issue into a Task -> it spawns
// an agent pod") is dead at step one, and the issue sits there until the human
// comments a SECOND time on their own issue.
//
// The distinguishing signal is LIVENESS, not thread shape, and it can only come
// from the webhook: a live, HMAC-verified, attributable delivery. It must NOT
// come from re-reading "zero comments" as "a human has the last word" - that
// would mint the whole cutover backlog ACTIVE, which is USER DECISION B2's
// 17-to-100-pod-hour re-triage storm.
// ---------------------------------------------------------------------------

// markedIssueCR is the ownerless mirror the webhook leaves behind: contract B.2
// permits an ownerless Issue CR, and the sweep's adopt-or-create path (fix
// M3-10) ADOPTS it rather than colliding with it.
func markedIssueCR(proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, number int) *tatarav1alpha1.Issue {
	return &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{
			Name: tatarav1alpha1.IssueName(repo.Name, number), Namespace: testNS,
			Annotations: map[string]string{AnnWebhookOriginated: time.Now().UTC().Format(time.RFC3339)},
		},
		Spec: tatarav1alpha1.IssueSpec{
			RepositoryRef: repo.Name, Number: number, ProjectRef: proj.Name,
			URL: "https://github.com/szymonrychu/tatara-operator/issues/7",
		},
	}
}

func sweptIssueCR(t *testing.T, c client.Client, repo *tatarav1alpha1.Repository, number int) *tatarav1alpha1.Issue {
	t.Helper()
	var iss tatarav1alpha1.Issue
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: testNS, Name: tatarav1alpha1.IssueName(repo.Name, number)}, &iss); err != nil {
		t.Fatalf("get issue CR %d: %v", number, err)
	}
	return &iss
}

// TestSweepWebhookOriginatedIssueMintsTriaging IS THE TEST THAT PROVES THE
// PLATFORM WORKS AT ALL: a human opens a brand-new issue (ZERO comments), a
// webhook lands and marks the mirror, and the very next sweep mints an ACTIVE
// (triaging) Task - not parked(backlog-sweep).
func TestSweepWebhookOriginatedIssueMintsTriaging(t *testing.T) {
	proj := sweepProject("webhook-proj")
	repo := sweepRepo("webhook-proj")
	c := newMirrorClient(t, proj, repo, markedIssueCR(proj, repo, 7))
	rd := &sweepReader{
		issues:  []scm.IssueRef{{Repo: "szymonrychu/tatara-operator", Number: 7, Author: "alice", State: "open"}},
		content: map[int]scm.IssueContent{7: {Title: "the login page 500s", Body: "steps to reproduce"}},
		// ZERO comments. A brand-new issue has none, and humanHasLastWord is false on
		// an empty thread. The marker is the ONLY thing that makes this ACTIVE.
	}

	runSweep(t, c, proj, repo, rd)

	tasks := sweepTasks(t, c, proj.Name)
	if len(tasks) != 1 {
		t.Fatalf("tasks = %d, want 1", len(tasks))
	}
	if tasks[0].Spec.InitialState != tatarav1alpha1.StateNew {
		t.Fatalf("initialStage = %q/%q, want triaging: a human OPENED this issue and the webhook said so",
			tasks[0].Spec.InitialState, tasks[0].Spec.InitialParkReason)
	}
	if tasks[0].Spec.InitialParkReason != "" {
		t.Fatalf("initialStageReason = %q, want empty on an ACTIVE mint", tasks[0].Spec.InitialParkReason)
	}

	// CONSUMED. The marker is a one-shot: it must not survive to re-activate a
	// LATER park.
	iss := sweptIssueCR(t, c, repo, 7)
	if _, still := iss.Annotations[AnnWebhookOriginated]; still {
		t.Fatal("the webhook marker survived the mint; it must be consumed exactly once")
	}
	// And the mint ADOPTED the ownerless CR the webhook created.
	owner, ok := own.ControllerOwner(iss)
	if !ok || owner != tasks[0].Name {
		t.Fatalf("issue controller owner = %q/%v, want %q", owner, ok, tasks[0].Name)
	}
}

// TestSweepCutoverBacklogStillParksWithZeroPods is the OTHER half, and it is the
// one the marker must not break. FORTY open, zero-comment, human-authored
// backlog issues with NO webhook and NO marker mint FORTY parked(backlog-sweep)
// Tasks: zero pods, zero QueuedEvents, zero tokens.
func TestSweepCutoverBacklogStillParksWithZeroPods(t *testing.T) {
	const backlog = 40
	proj := sweepProject("cutover-proj")
	proj.Spec.MaxNewTasksPerSweep = backlog // one pass, whole backlog
	repo := sweepRepo("cutover-proj")
	c := newMirrorClient(t, proj, repo, mdSecret())

	rd := &sweepReader{content: map[int]scm.IssueContent{}}
	for n := 1; n <= backlog; n++ {
		rd.issues = append(rd.issues, scm.IssueRef{
			Repo: "szymonrychu/tatara-operator", Number: n, Author: "alice", State: "open",
		})
		rd.content[n] = scm.IssueContent{Title: "old bug", Body: "from before the cutover"}
	}

	runSweep(t, c, proj, repo, rd)

	tasks := sweepTasks(t, c, proj.Name)
	if len(tasks) != backlog {
		t.Fatalf("tasks = %d, want %d", len(tasks), backlog)
	}
	for i := range tasks {
		if tasks[i].Spec.InitialState != tatarav1alpha1.StateNew ||
			tasks[i].Spec.InitialParkReason != stage.ReasonBacklogSweep {
			t.Fatalf("task %s initialStage = %q/%q, want parked/backlog-sweep: the cutover backlog must NEVER re-triage",
				tasks[i].Name, tasks[i].Spec.InitialState, tasks[i].Spec.InitialParkReason)
		}
	}

	var qel tatarav1alpha1.QueuedEventList
	if err := c.List(context.Background(), &qel, client.InNamespace(testNS)); err != nil {
		t.Fatalf("list queuedevents: %v", err)
	}
	if len(qel.Items) != 0 {
		t.Fatalf("queuedevents = %d, want 0", len(qel.Items))
	}

	// AND THE RECONCILER AGREES. Driving every one of the 40 Tasks through the
	// stage machine creates ZERO pods: parked is terminal, and the reconciler
	// returns before it can ever reach the pod path.
	r := tsReconciler(c)
	for i := range tasks {
		if _, err := r.reconcileStage(context.Background(), proj, &tasks[i], time.Now()); err != nil {
			t.Fatalf("reconcileStage %s: %v", tasks[i].Name, err)
		}
	}
	var pods corev1.PodList
	if err := c.List(context.Background(), &pods, client.InNamespace(testNS)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("pods = %d, want 0: a %d-issue cutover backlog must cost ZERO pod-hours",
			len(pods.Items), backlog)
	}
}

// TestSweepCutoverBacklogFromATrustedMaintainerMintsActiveInstead is the
// SIBLING the test above must not be read alone: it certifies the property the
// trusted-human-author clause deliberately VOIDS. TestSweepCutoverBacklogStill-
// ParksWithZeroPods only reads as "a backlog costs zero pod-hours" because
// sweepProject sets no maintainer or reporter logins; give the same 40-issue
// shape a maintainer allowlist that names the author and every one of those 40
// issues mints ACTIVE instead, because IsTrustedAuthor stops needing a webhook
// marker or a comment thread to trust "alice". The measured real-world cost of
// this (MintStage's doc comment, and MEMORY.md) is 6 orphan issues cluster-wide
// on 2026-07-28, ONE clarify pod per issue, ONCE - not 40 pods on every pass.
func TestSweepCutoverBacklogFromATrustedMaintainerMintsActiveInstead(t *testing.T) {
	const backlog = 40
	proj := sweepProject("cutover-trusted-proj")
	proj.Spec.MaxNewTasksPerSweep = backlog // one pass, whole backlog
	proj.Spec.MaxOpenTasks = backlog        // every mint here is ACTIVE, unlike the parked sibling: it competes for the same budget
	proj.Spec.Scm.MaintainerLogins = []string{"alice"}
	repo := sweepRepo("cutover-trusted-proj")
	c := newMirrorClient(t, proj, repo, mdSecret())

	rd := &sweepReader{content: map[int]scm.IssueContent{}}
	for n := 1; n <= backlog; n++ {
		rd.issues = append(rd.issues, scm.IssueRef{
			Repo: "szymonrychu/tatara-operator", Number: n, Author: "alice", State: "open",
		})
		rd.content[n] = scm.IssueContent{Title: "old bug", Body: "from before the cutover"}
	}

	runSweep(t, c, proj, repo, rd)

	tasks := sweepTasks(t, c, proj.Name)
	if len(tasks) != backlog {
		t.Fatalf("tasks = %d, want %d", len(tasks), backlog)
	}
	for i := range tasks {
		if tasks[i].Spec.InitialState != tatarav1alpha1.StateNew || tasks[i].Spec.InitialParkReason != "" {
			t.Fatalf("task %s initialStage = %q/%q, want triaging/\"\": a trusted maintainer's backlog issue mints ACTIVE",
				tasks[i].Name, tasks[i].Spec.InitialState, tasks[i].Spec.InitialParkReason)
		}
	}
}

// TestWebhookMarkerIsConsumedExactlyOnce: the marker is spent by the mint that
// reads it. If it survived, the reap/re-mint cycle would re-activate the issue
// forever - the M25 loop by another door.
//
// The reap here is the M25 scenario: the reaper's tatara-parked label write
// FAILED (a 403 on a secondary rate limit), so there is NO label on the forge
// issue to fall back on. The marker being GONE is the only thing keeping pass 2
// parked.
func TestWebhookMarkerIsConsumedExactlyOnce(t *testing.T) {
	proj := sweepProject("once-proj")
	repo := sweepRepo("once-proj")
	c := newMirrorClient(t, proj, repo, markedIssueCR(proj, repo, 7))
	rd := &sweepReader{
		issues:  []scm.IssueRef{{Repo: "szymonrychu/tatara-operator", Number: 7, Author: "alice", State: "open"}},
		content: map[int]scm.IssueContent{7: {Title: "t", Body: "b"}},
	}

	runSweep(t, c, proj, repo, rd)
	first := sweepTasks(t, c, proj.Name)
	if len(first) != 1 || first[0].Spec.InitialState != tatarav1alpha1.StateNew {
		t.Fatalf("pass 1: tasks = %d, initialStage = %q, want 1 triaging", len(first), first[0].Spec.InitialState)
	}

	// The Task parked and the reaper collected it: the Issue CR is RELEASED
	// (ownerless, never GC'd per B.1) and the Task is gone. No tatara-parked label
	// made it to the forge.
	iss := sweptIssueCR(t, c, repo, 7)
	iss.OwnerReferences = nil
	if err := c.Update(context.Background(), iss); err != nil {
		t.Fatalf("release issue ownership: %v", err)
	}
	if err := c.Delete(context.Background(), &first[0]); err != nil {
		t.Fatalf("delete reaped task: %v", err)
	}

	runSweep(t, c, proj, repo, rd)

	second := sweepTasks(t, c, proj.Name)
	if len(second) != 1 {
		t.Fatalf("pass 2: tasks = %d, want 1", len(second))
	}
	if second[0].Spec.InitialState != tatarav1alpha1.StateNew ||
		second[0].Spec.InitialParkReason != stage.ReasonBacklogSweep {
		t.Fatalf("pass 2: initialStage = %q/%q, want parked/backlog-sweep: a SPENT marker must never re-activate a Task that has since parked",
			second[0].Spec.InitialState, second[0].Spec.InitialParkReason)
	}
}

// TestSweepBudgetsBindOnWebhookOriginatedMints: the marker buys an ACTIVE stage,
// not a bypass. maxNewTasksPerSweep and maxOpenTasks BOTH still bind (fix B1),
// and an issue the budget deferred KEEPS its marker so the next pass still mints
// it ACTIVE - the deferral must not silently downgrade it to the backlog.
func TestSweepBudgetsBindOnWebhookOriginatedMints(t *testing.T) {
	t.Run("maxNewTasksPerSweep binds and the deferred marker survives", func(t *testing.T) {
		proj := sweepProject("cap-new-proj")
		proj.Spec.MaxNewTasksPerSweep = 2
		repo := sweepRepo("cap-new-proj")
		c := newMirrorClient(t, proj, repo,
			markedIssueCR(proj, repo, 1), markedIssueCR(proj, repo, 2), markedIssueCR(proj, repo, 3))
		rd := &sweepReader{content: map[int]scm.IssueContent{}}
		for n := 1; n <= 3; n++ {
			rd.issues = append(rd.issues, scm.IssueRef{
				Repo: "szymonrychu/tatara-operator", Number: n, Author: "alice", State: "open"})
			rd.content[n] = scm.IssueContent{Title: "t", Body: "b"}
		}

		runSweep(t, c, proj, repo, rd)

		tasks := sweepTasks(t, c, proj.Name)
		if len(tasks) != 2 {
			t.Fatalf("tasks = %d, want 2 (maxNewTasksPerSweep binds on marked issues too)", len(tasks))
		}
		if v := sweptIssueCR(t, c, repo, 3).Annotations[AnnWebhookOriginated]; v == "" {
			t.Fatal("the budget-deferred issue LOST its marker; the next pass would park a live human issue")
		}
	})

	t.Run("maxOpenTasks binds: a marked mint is ACTIVE and counts", func(t *testing.T) {
		proj := sweepProject("cap-open-proj")
		proj.Spec.MaxOpenTasks = 1
		repo := sweepRepo("cap-open-proj")
		c := newMirrorClient(t, proj, repo, markedIssueCR(proj, repo, 1), markedIssueCR(proj, repo, 2))
		rd := &sweepReader{content: map[int]scm.IssueContent{}}
		for n := 1; n <= 2; n++ {
			rd.issues = append(rd.issues, scm.IssueRef{
				Repo: "szymonrychu/tatara-operator", Number: n, Author: "alice", State: "open"})
			rd.content[n] = scm.IssueContent{Title: "t", Body: "b"}
		}

		runSweep(t, c, proj, repo, rd)

		tasks := sweepTasks(t, c, proj.Name)
		if len(tasks) != 1 {
			t.Fatalf("tasks = %d, want 1 (maxOpenTasks=1 binds: an ACTIVE mint counts against it)", len(tasks))
		}
		if tasks[0].Spec.InitialState != tatarav1alpha1.StateNew {
			t.Fatalf("initialStage = %q, want triaging", tasks[0].Spec.InitialState)
		}
	})
}

// TestWebhookOpenedIssueTriagesAndSpawnsAPod IS THE END-TO-END PROOF, from the
// webhook's marker to a running agent pod:
//
//	marker -> sweep mints triaging -> reconcile: triaging mints the Issue CR and
//	          routes to clarifying -> reconcile: the clarify POD is created.
//
// Nothing about the platform works if this does not.
func TestWebhookOpenedIssueTriagesAndSpawnsAPod(t *testing.T) {
	proj := sweepProject("e2e-proj")
	proj.Spec.MaxConcurrentAgents = 3
	readySince := metav1.NewTime(time.Now().Add(-time.Hour))
	proj.Status.Memory = &tatarav1alpha1.MemoryStatus{
		Phase: "Ready", Endpoint: "http://mem", ReadySince: &readySince,
	}
	repo := sweepRepo("e2e-proj")
	c := newMirrorClient(t, proj, repo, mdSecret(), markedIssueCR(proj, repo, 7))
	rd := &sweepReader{
		issues:  []scm.IssueRef{{Repo: "szymonrychu/tatara-operator", Number: 7, Author: "alice", State: "open"}},
		content: map[int]scm.IssueContent{7: {Title: "the login page 500s", Body: "steps to reproduce"}},
	}

	runSweep(t, c, proj, repo, rd)

	tasks := sweepTasks(t, c, proj.Name)
	if len(tasks) != 1 || tasks[0].Spec.InitialState != tatarav1alpha1.StateNew {
		t.Fatalf("sweep minted %d tasks at initialStage %q, want 1 at triaging", len(tasks), tasks[0].Spec.InitialState)
	}

	r := tsReconciler(c)
	r.PodConfig = agent.PodConfig{
		Namespace: testNS, AnthropicSecretName: "anthropic", CLIOIDCSecretName: "tatara-cli-oidc",
	}
	task := &tasks[0]
	now := time.Now()

	// PASS 0: the create-edge (fix C5). The mint only set Spec.InitialState; this
	// pass applies it to Status.Stage and requeues - it does not yet drive
	// triaging's own routing.
	task = tsReconcile(t, r, proj, task, now)
	if task.Status.State != tatarav1alpha1.StateNew {
		t.Fatalf("stage after the create-edge = %q, want triaging", task.Status.State)
	}

	// PASS 1: triaging is POD-LESS. It mints the Issue CRs (F.2) and routes on
	// spec.kind - clarify -> clarifying, where the C.6 approval gate lives.
	task = tsReconcile(t, r, proj, task, now)
	if task.Status.State != tatarav1alpha1.StateRefined {
		t.Fatalf("stage after triage = %q, want clarifying", task.Status.State)
	}

	// PASS 2: clarifying is a POD stage. The pod is created.
	task = tsReconcile(t, r, proj, task, now)
	var pod corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: testNS, Name: agent.PodName(task)}, &pod); err != nil {
		t.Fatalf("THE AGENT POD WAS NEVER CREATED. A human opened an issue and the platform did nothing: %v", err)
	}
	if pod.Annotations[annPodStage] != tatarav1alpha1.StateRefined {
		t.Fatalf("pod stage annotation = %q, want clarifying", pod.Annotations[annPodStage])
	}
}

// TestSweepOrphanExternalMRCommentConvergesInOnePass is OP12's headline
// convergence test: an orphan external MR (never seen before - no MergeRequest
// CR yet) that ALSO already carries a pending human comment. ONE sweep pass
// must both (a) mint its review Task on the PRReview classification (the
// existing ClassifyPR->MintReviewTask rule) AND (b) redeliver that comment to
// the JUST-MINTED owner - not wait a further sweep cycle for the mirror to
// catch up. This is the "Convergence with OP6" amendment: sweepPRs re-reads
// the MR CR after the classify/mint switch specifically so this lands in one
// pass, and redeliverMRComments' belt-and-suspenders EnsureTaskForMRComment
// call is provably not needed here (the switch's own mint already bound the
// owner) - proving the two paths agree without duplicating the mint.
func TestSweepOrphanExternalMRCommentConvergesInOnePass(t *testing.T) {
	ctx := context.Background()
	base := "szymonrychu/tatara-operator"
	proj := sweepProject("op12-orphan-proj")
	repo := sweepRepo("op12-orphan-proj")
	c := newMirrorClient(t, proj, repo) // no MergeRequest CR seeded: a true orphan

	rd := &sweepReader{
		prs: []scm.PRRef{{
			Repo: base, HeadRepo: base, Number: 60, Author: "octocat",
			HeadBranch: "octocat/feature-x", HeadSHA: "sha-60",
		}},
		prComments: map[int][]scm.IssueComment{
			60: {{ExternalID: "500", Author: "octocat", Body: "please take a look", CreatedAt: fixedTime(1)}},
		},
	}

	runSweep(t, c, proj, repo, rd)

	tasks := sweepTasks(t, c, proj.Name)
	if len(tasks) != 1 {
		t.Fatalf("tasks = %d, want 1 (the orphan's review task)", len(tasks))
	}
	if tasks[0].Spec.Kind != SweepReviewKind {
		t.Fatalf("kind = %q, want review", tasks[0].Spec.Kind)
	}

	var mr tatarav1alpha1.MergeRequest
	mrKey := types.NamespacedName{Namespace: testNS, Name: tatarav1alpha1.MergeRequestName(repo.Name, 60)}
	if err := c.Get(ctx, mrKey, &mr); err != nil {
		t.Fatalf("get mergerequest: %v", err)
	}
	if ctrl, ok := ownerControllerName(&mr); !ok || ctrl != tasks[0].Name {
		t.Fatalf("MR controller owner = %q (ok=%v), want the minted review task %q", ctrl, ok, tasks[0].Name)
	}
	if mr.Status.Ownership != tatarav1alpha1.OwnershipExternal {
		t.Fatalf("ownership = %q, want external (backfilled from the non-bot author)", mr.Status.Ownership)
	}
	if mr.Status.LastMirroredCommentID != "500" {
		t.Fatalf("cursor = %q, want 500 - the pending comment must redeliver in THIS SAME pass", mr.Status.LastMirroredCommentID)
	}
	if !mirrorHasComment(&mr, "500") {
		t.Fatalf("comment 500 not mirrored onto the MR CR")
	}

	var tk tatarav1alpha1.Task
	if err := c.Get(ctx, types.NamespacedName{Namespace: testNS, Name: tasks[0].Name}, &tk); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if len(tk.Status.PendingEvents) != 1 {
		t.Fatalf("pendingEvents = %d, want 1 (the redelivered mr_comment)", len(tk.Status.PendingEvents))
	}
	if tk.Status.PendingEvents[0].Kind != "mr_comment" || tk.Status.PendingEvents[0].Author != "octocat" {
		t.Fatalf("delivered event = %+v, want kind=mr_comment author=octocat", tk.Status.PendingEvents[0])
	}
}

// sweepErrorsTotalSeries reports whether operator_sweep_errors_total currently
// exposes a series for the given (activity, reason) pair, gathered straight
// from the controller-runtime metrics registry. A *prometheus.CounterVec child
// only exists in Gather() output once WithLabelValues has been called for that
// exact label combination somewhere (init-time seeding or a real increment) -
// calling WithLabelValues ourselves here would lazily create it and defeat the
// point of the check, so this walks the gathered families instead.
func sweepErrorsTotalSeries(t *testing.T, project, activity, reason string) bool {
	t.Helper()
	families, err := ctrlmetrics.Registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != "operator_sweep_errors_total" {
			continue
		}
		for _, m := range fam.GetMetric() {
			var gotProject, gotActivity, gotReason string
			for _, lp := range m.GetLabel() {
				switch lp.GetName() {
				case "project":
					gotProject = lp.GetValue()
				case "activity":
					gotActivity = lp.GetValue()
				case "reason":
					gotReason = lp.GetValue()
				}
			}
			if gotProject == project && gotActivity == activity && gotReason == reason {
				return true
			}
		}
	}
	return false
}

// TestSweepErrorsSeededForProject: the seeding that used to run at init() moved
// to the Project reconcile path when `project` joined the label set (issue
// #441) - project names are not known at process start. A CounterVec with no
// WithLabelValues call has NO series at all, so without this the first real
// evaluation of the sweep-error alert would undercount an error storm that
// started before any of these reasons had fired once.
func TestSweepErrorsSeededForProject(t *testing.T) {
	const project = "seeded-scan-proj"
	obs.SeedSweepErrorsForProject(project)

	// Task 3 (refine re-homing): brainstorm has no cron of its own any more
	// (demand-driven, event path only, so only stamp_failed can fire on it);
	// documentation/issueScan share the plain cron reason set; refine is now
	// its own activity with its own inflight-dedup reason. The barrier-era
	// refine_barrier_held/refine_barrier_timeout/refine_check_failed reasons
	// have no producer left anywhere and are gone from the seeded set too.
	perActivityReasons := map[string][]string{
		"brainstorm":    {"stamp_failed"},
		"documentation": {"invalid_cron", "stamp_failed"},
		"issueScan":     {"invalid_cron", "stamp_failed"},
		"refine":        {"invalid_cron", "stamp_failed", "refine_inflight_check_failed"},
		"upgrade":       {"invalid_cron", "stamp_failed", "upgrade_count_failed"},
	}
	for activity, reasons := range perActivityReasons {
		for _, reason := range reasons {
			if !sweepErrorsTotalSeries(t, project, activity, reason) {
				t.Errorf("SweepErrorsTotal{project=%s,activity=%s,reason=%s} has no series; want it seeded per project",
					project, activity, reason)
			}
		}
	}
	if !sweepErrorsTotalSeries(t, project, SweepActivity, "list_tasks") {
		t.Errorf("SweepErrorsTotal{project=%s,activity=%s,reason=list_tasks} has no series; want the sweep reason set seeded too",
			project, SweepActivity)
	}
	if sweepErrorsTotalSeries(t, "never-seeded-proj", "brainstorm", "stamp_failed") {
		t.Errorf("SweepErrorsTotal has a series for a project that was never seeded; seeding must be per-project")
	}
}

// TestSweepHeartbeatIsPerProject is the issue #441 regression test. Before
// `project` joined the label set, the three Projects on this cluster
// (tatara, infrastructure, mtg) all wrote ONE heartbeat series: it moved
// backward and forward last-write-wins, the alert read whichever value was
// current, and a Project whose cron was genuinely dead was masked by a healthy
// sibling's write. Two Projects stamping the SAME activity must now produce two
// independent series.
func TestSweepHeartbeatIsPerProject(t *testing.T) {
	const activity = "sweep-hb-perproject-test"
	const stale = 1000.0

	projA := sweepProject("hb-multi-a")
	projB := sweepProject("hb-multi-b")
	repoB := sweepRepo("hb-multi-b")
	c := newMirrorClient(t, projB, repoB)

	// Project A last swept long ago and is NOT swept in this test.
	obs.SweepLastSuccessTimestamp.WithLabelValues(projA.Name, activity).Set(stale)

	r := &ProjectReconciler{Client: c, Scheme: c.Scheme(), Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry())}
	if _, err := r.SweepProject(context.Background(), projB, &sweepReader{},
		[]tatarav1alpha1.Repository{*repoB}, nil, activity); err != nil {
		t.Fatalf("SweepProject(hb-multi-b): %v", err)
	}

	if got := testutil.ToFloat64(obs.SweepLastSuccessTimestamp.WithLabelValues(projA.Name, activity)); got != stale {
		t.Fatalf("project hb-multi-a heartbeat = %v, want %v unchanged - project hb-multi-b's sweep clobbered it (issue #441)", got, stale)
	}
	if got := testutil.ToFloat64(obs.SweepLastSuccessTimestamp.WithLabelValues(projB.Name, activity)); got <= stale {
		t.Fatalf("project hb-multi-b heartbeat = %v, want a fresh stamp above %v", got, stale)
	}
}

// ---------------------------------------------------------------------------
// THE READ-AFTER-WRITE RACE (mtg-decks#9, 2026-07-28).
//
// MarkWebhookOriginated used to Create the mirror CR and then immediately
// re-Get it through the CACHED client under retry.RetryOnConflict, which
// retries ONLY IsConflict. An informer cache that had not yet observed the
// Create answered NotFound, the retry returned it verbatim, the webhook
// handler 500'd, and MintForItem never ran - so a brand-new human issue
// started nothing at all. It is a RACE: issue #7 won it on 2026-07-24, issue
// #9 lost it by 437ms. laggingIssueGets reproduces the losing side
// deterministically.
// ---------------------------------------------------------------------------

// laggingIssueGets returns an interceptor that answers NotFound to every Get of
// an Issue while live (the default), mimicking an informer cache that has not
// observed a Create yet, plus a caughtUp func the caller uses to switch the
// cache back on before it verifies what was actually stored. Unlike a fixed
// budget, this never runs out from under a test's OWN verification read: a
// budget large enough to survive an implementation regression back to a
// second internal read is, by the same arithmetic, large enough to also
// intercept the test's post-call verification Get, since both draw from the
// SAME counter. A caller that never calls caughtUp gets every Issue Get
// lagging for the whole test, n irrelevant (kept for the one caller - the
// already-exists test below - that wants a SPECIFIC number of real reads to
// lag and then stop on its own, matching its own internal call count).
func laggingIssueGets(n int) (interceptor.Funcs, func()) {
	remaining, live := n, true
	return interceptor.Funcs{
		Get: func(ctx context.Context, cli client.WithWatch, key client.ObjectKey,
			obj client.Object, opts ...client.GetOption) error {
			if _, isIssue := obj.(*tatarav1alpha1.Issue); isIssue && live && remaining > 0 {
				remaining--
				return apierrors.NewNotFound(
					tatarav1alpha1.GroupVersion.WithResource("issues").GroupResource(), key.Name)
			}
			return cli.Get(ctx, key, obj, opts...)
		},
	}, func() { live = false }
}

// TestMarkWebhookOriginatedBrandNewIssueNeedsNoSecondRead: the COMMON path. No
// mirror CR exists, every cached Issue read lags, and the mark still succeeds -
// because the marker is stamped on the object being CREATED, so there is no
// second read to race. The lag stays live for the WHOLE call (a fixed budget
// here would silently stop protecting the assertion the moment an
// implementation regression added back a second internal read, because that
// extra read would consume the budget meant for this test's own verification
// Get instead) - caughtUp() only switches it off afterward, once the call has
// returned.
func TestMarkWebhookOriginatedBrandNewIssueNeedsNoSecondRead(t *testing.T) {
	proj := sweepProject("race-new-proj")
	repo := sweepRepo("race-new-proj")
	lag, caughtUp := laggingIssueGets(8)
	c := fake.NewClientBuilder().
		WithScheme(mirrorScheme(t)).
		WithObjects(proj, repo).
		WithInterceptorFuncs(lag).
		Build()

	marked, err := MarkWebhookOriginated(context.Background(), c, nil, proj, repo, 9,
		"https://github.com/szymonrychu/mtg-decks/issues/9", time.Now())
	if err != nil {
		t.Fatalf("MarkWebhookOriginated = %v, want nil: a brand-new issue must not depend on a cached re-read", err)
	}
	if !marked {
		t.Fatal("marked = false, want true: the create-time stamp IS the mark")
	}

	// The cache has now "caught up": switch the lag off before reading back what
	// was actually stored, so this verification read is not itself intercepted.
	caughtUp()
	var iss tatarav1alpha1.Issue
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: testNS, Name: tatarav1alpha1.IssueName(repo.Name, 9)}, &iss); err != nil {
		t.Fatalf("get issue CR: %v", err)
	}
	if iss.Annotations[AnnWebhookOriginated] == "" {
		t.Fatal("the created Issue CR carries no webhook-originated marker")
	}
}

// TestMarkWebhookOriginatedRetriesNotFoundFromTheCache: the OTHER half. The CR
// already exists (a concurrent writer won the Create), so the Get/Update branch
// still runs - and it must RETRY a NotFound from the lagging cache instead of
// returning it, which is what 500'd the delivery.
func TestMarkWebhookOriginatedRetriesNotFoundFromTheCache(t *testing.T) {
	proj := sweepProject("race-exists-proj")
	repo := sweepRepo("race-exists-proj")
	existing := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{
			Name: tatarav1alpha1.IssueName(repo.Name, 9), Namespace: testNS,
		},
		Spec: tatarav1alpha1.IssueSpec{
			RepositoryRef: repo.Name, Number: 9, ProjectRef: proj.Name,
			URL: "https://github.com/szymonrychu/mtg-decks/issues/9",
		},
	}
	// TWO lagging Gets: the first is ensureIssueCR's existence probe (which then
	// Creates and loses on AlreadyExists), the second is the Get/Update branch's
	// own read. The third Get sees the object and the Update stamps it - this
	// test's own budget naturally runs out from its own internal call count, so
	// it does not need caughtUp (discarded here).
	lag, _ := laggingIssueGets(2)
	c := fake.NewClientBuilder().
		WithScheme(mirrorScheme(t)).
		WithObjects(proj, repo, existing).
		WithInterceptorFuncs(lag).
		Build()

	marked, err := MarkWebhookOriginated(context.Background(), c, nil, proj, repo, 9,
		"https://github.com/szymonrychu/mtg-decks/issues/9", time.Now())
	if err != nil {
		t.Fatalf("MarkWebhookOriginated = %v, want nil: a NotFound from a lagging cache must be RETRIED, not returned", err)
	}
	if !marked {
		t.Fatal("marked = false, want true: the existing CR must end up carrying the marker")
	}

	var iss tatarav1alpha1.Issue
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: testNS, Name: tatarav1alpha1.IssueName(repo.Name, 9)}, &iss); err != nil {
		t.Fatalf("get issue CR: %v", err)
	}
	if iss.Annotations[AnnWebhookOriginated] == "" {
		t.Fatal("the existing Issue CR was never marked")
	}
}

// TestClearWebhookOriginatedRetriesNotFoundFromTheCache is FINDING 2's
// regression test (2026-07-28 review round 1): ClearWebhookOriginated used to
// treat a cached NotFound as "no marker to clear" and return nil - correct for
// a genuinely absent CR, wrong for a CR this same webhook request may have
// just CREATED (mint's own Task.status.issueRefs write races the same
// informer watch MarkWebhookOriginated used to lose against). Silently no-op'ing
// that left the marker stamped forever: nothing else in the platform ever
// clears it, so the issue re-activates on every later reap cycle. It must now
// RETRY a NotFound instead, exactly like MarkWebhookOriginated.
func TestClearWebhookOriginatedRetriesNotFoundFromTheCache(t *testing.T) {
	existing := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{
			Name: tatarav1alpha1.IssueName("clear-repo", 9), Namespace: testNS,
			Annotations: map[string]string{AnnWebhookOriginated: "2026-07-28T06:08:07Z"},
		},
		Spec: tatarav1alpha1.IssueSpec{RepositoryRef: "clear-repo", Number: 9, ProjectRef: "clear-proj"},
	}
	// ONE lagging Get: ClearWebhookOriginated's own read. The second sees the
	// object and the Update spends the marker.
	lag, _ := laggingIssueGets(1)
	c := fake.NewClientBuilder().
		WithScheme(mirrorScheme(t)).
		WithObjects(existing).
		WithInterceptorFuncs(lag).
		Build()

	if err := ClearWebhookOriginated(context.Background(), c, nil, testNS, existing.Name); err != nil {
		t.Fatalf("ClearWebhookOriginated = %v, want nil: a NotFound from a lagging cache must be RETRIED, not swallowed as a no-op", err)
	}

	var iss tatarav1alpha1.Issue
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: existing.Name}, &iss); err != nil {
		t.Fatalf("get issue CR: %v", err)
	}
	if _, ok := iss.Annotations[AnnWebhookOriginated]; ok {
		t.Fatal("the marker is still set: ClearWebhookOriginated must have spent it despite the lagging cache")
	}
}

// collidingSource is one source issue shared by two Tasks. It is the whole
// point of the collision tests below: agent.TaskBranch keys on
// (branchKind, source.number, title slug), NOT on the Task's own name.
func collidingSource() *tatarav1alpha1.TaskSource {
	return &tatarav1alpha1.TaskSource{
		Provider: "github",
		IssueRef: "https://github.com/szymonrychu/tatara-operator/issues/26",
		Number:   26,
	}
}

// claimedMR builds a MergeRequest CR already controller-owned by task, the way
// a prior adoption leaves it.
func claimedMR(t *testing.T, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository,
	number int, task *tatarav1alpha1.Task) *tatarav1alpha1.MergeRequest {

	t.Helper()
	mr := &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{Name: tatarav1alpha1.MergeRequestName(repo.Name, number), Namespace: testNS},
		Spec: tatarav1alpha1.MergeRequestSpec{
			ProjectRef: proj.Name, RepositoryRef: repo.Name, Number: number,
			URL: "https://github.com/szymonrychu/tatara-operator/pull/1358",
		},
	}
	own.AddPlainOwner(mr, task)
	if err := own.HandOverController(mr, nil, task); err != nil {
		t.Fatalf("test setup: hand over controller to %s: %v", task.Name, err)
	}
	return mr
}

// liveOwnerOf reads a mirror's controller owner for a ClassifyPR call whose
// subject is the PREDICATE, not the liveness resolution - the tests that pin
// liveness resolve it through Minter.resolveLiveOwner instead.
func liveOwnerOf(t *testing.T, mr *tatarav1alpha1.MergeRequest) string {
	t.Helper()
	name, _ := own.ControllerOwner(mr)
	return name
}

// TestSweepBranchCollisionAdoptsIntoTheClaimingTask is issue #477's ACTUAL
// mechanism, reproduced.
//
// agent.TaskBranch is NOT unique per Task. For a Task with source.number > 0 it
// is tatara/<branchKind>-<number>[-<slug>], and branchKind COLLAPSES review,
// brainstorm, clarify and refine into "chore" - so two clarify Tasks on the SAME
// source issue carry the SAME head branch. That is live in prod:
// infrastructure-clarify-2026-07-26-t9n9n and infrastructure-clarify-2026-07-27-gtwgp
// both resolve to tatara/chore-26, and mr-helmfile-1358 is controller-owned by
// the latter.
//
// taskForBranch returned the FIRST match in list order, so the sweep picked the
// Task that does NOT own the MR, ClassifyPR said PRAdopt, and ownMergeRequest
// refused with "already has controller owner <other>". That is a hard per-item
// sweep error, re-attempted byte-identically on every 4h pass for as long as
// both Tasks exist. The MR's controller owner is the disambiguator.
func TestSweepBranchCollisionAdoptsIntoTheClaimingTask(t *testing.T) {
	proj := sweepProject("collide-proj")
	repo := sweepRepo("collide-proj")

	// decoy is inserted FIRST (the fake client's tracker preserves insertion
	// order) and is parked: exactly the arbitrary pick the scan-and-return-first
	// lookup made in prod.
	decoy := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "collide-proj-clarify-2026-07-26-aaaaa", Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, Kind: "clarify", Goal: "g", Source: collidingSource()},
	}
	decoy.Status.State = tatarav1alpha1.StateRefined
	decoy.Status.ParkReason = stage.ReasonAwaitingHuman
	claimant := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "collide-proj-clarify-2026-07-27-zzzzz", Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, Kind: "clarify", Goal: "g", Source: collidingSource()},
	}
	claimant.Status.State = tatarav1alpha1.StateUnderImplementation
	if agent.TaskBranch(decoy) != agent.TaskBranch(claimant) {
		t.Fatalf("setup: branches must COLLIDE, got %q and %q", agent.TaskBranch(decoy), agent.TaskBranch(claimant))
	}

	mr := claimedMR(t, proj, repo, 1358, claimant)
	c := newMirrorClient(t, proj, repo, decoy, claimant, mr)
	rd := &sweepReader{prs: []scm.PRRef{{
		Repo: "szymonrychu/tatara-operator", HeadRepo: "szymonrychu/tatara-operator",
		Number: 1358, Author: "tatara-bot", HeadBranch: agent.TaskBranch(claimant), HeadSHA: "deadbeef",
	}}}

	// Pre-fix this returns "intake: own mergerequest mr-tatara-operator-1358:
	// ... already has controller owner collide-proj-clarify-2026-07-27-zzzzz".
	runSweep(t, c, proj, repo, rd)

	var fresh tatarav1alpha1.MergeRequest
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: mr.Name}, &fresh); err != nil {
		t.Fatalf("get mergerequest CR: %v", err)
	}
	if got, ok := own.ControllerOwner(&fresh); !ok || got != claimant.Name {
		t.Fatalf("MR controller owner = %q/%v, want %q (the sweep must never steal)", got, ok, claimant.Name)
	}
	var freshClaimant, freshDecoy tatarav1alpha1.Task
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: claimant.Name}, &freshClaimant); err != nil {
		t.Fatalf("get claimant: %v", err)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: decoy.Name}, &freshDecoy); err != nil {
		t.Fatalf("get decoy: %v", err)
	}
	if len(freshClaimant.Status.MRRefs) != 1 || freshClaimant.Status.MRRefs[0] != mr.Name {
		t.Fatalf("claimant mrRefs = %v, want [%s]", freshClaimant.Status.MRRefs, mr.Name)
	}
	if len(freshDecoy.Status.MRRefs) != 0 {
		t.Fatalf("decoy mrRefs = %v, want none: the MR belongs to the Task that owns it", freshDecoy.Status.MRRefs)
	}
}

// TestTaskForBranchPrefersTheClaimingTask pins the collision tie-break directly
// (issue #477). Two Tasks on one branch: the MR's controller owner wins when it
// is named, and with no claim the answer is deterministic - still-pushing beats
// finished, newest beats oldest - so it can never depend on list order.
func TestTaskForBranchPrefersTheClaimingTask(t *testing.T) {
	ctx := context.Background()
	proj := sweepProject("prefer-proj")

	older := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name: "prefer-proj-clarify-a", Namespace: testNS,
			CreationTimestamp: metav1.NewTime(time.Date(2026, 7, 26, 0, 0, 0, 0, time.UTC)),
		},
		Spec: tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, Kind: "clarify", Goal: "g", Source: collidingSource()},
	}
	older.Status.State = tatarav1alpha1.StateRefined
	older.Status.ParkReason = stage.ReasonAwaitingHuman
	newer := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name: "prefer-proj-clarify-b", Namespace: testNS,
			CreationTimestamp: metav1.NewTime(time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC)),
		},
		Spec: tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, Kind: "clarify", Goal: "g", Source: collidingSource()},
	}
	newer.Status.State = tatarav1alpha1.StateUnderImplementation
	// Finished, and the NEWEST of the three: it must still lose to a Task that
	// can still push, or the sweep would adopt a live PR into a dead Task.
	dead := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name: "prefer-proj-clarify-c", Namespace: testNS,
			CreationTimestamp: metav1.NewTime(time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC)),
		},
		Spec: tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, Kind: "clarify", Goal: "g", Source: collidingSource()},
	}
	dead.Status.State = tatarav1alpha1.StateDone

	c := newMirrorClient(t, proj, older, newer, dead)
	r := &ProjectReconciler{Client: c, Scheme: c.Scheme(), Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry())}
	branch := agent.TaskBranch(newer)

	// The claim wins outright, even against a newer sibling.
	got, err := r.taskForBranch(ctx, proj, branch, older.Name)
	if err != nil || got == nil || got.Name != older.Name {
		t.Fatalf("taskForBranch(claimed by %s) = %v, %v, want %s", older.Name, got, err, older.Name)
	}
	// A claim naming a Task that does NOT share this branch falls back to the
	// deterministic pick rather than resolving to nothing.
	got, err = r.taskForBranch(ctx, proj, branch, "prefer-proj-takeover-elsewhere")
	if err != nil || got == nil || got.Name != newer.Name {
		t.Fatalf("taskForBranch(foreign claim) = %v, %v, want %s", got, err, newer.Name)
	}
	// No claim at all: newest still-pushing, NOT the newest overall (dead).
	got, err = r.taskForBranch(ctx, proj, branch, "")
	if err != nil || got == nil || got.Name != newer.Name {
		t.Fatalf("taskForBranch(unclaimed) = %v, %v, want %s", got, err, newer.Name)
	}
}

// TestClassifyPRClaimedByAnotherTask pins clause 1b: adoptable BY SHAPE but the
// mirror is already controller-owned elsewhere, so the adoption could only ever
// fail (ownMergeRequest never steals). PRClaimed, not PRAdopt and not an error.
func TestClassifyPRClaimedByAnotherTask(t *testing.T) {
	proj := sweepProject("claim-proj")
	repo := sweepRepo("claim-proj")
	owner := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "claim-proj-clarify-owner", Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, Kind: "clarify", Goal: "g"},
	}
	other := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "claim-proj-clarify-other", Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, Kind: "clarify", Goal: "g"},
	}
	pr := scm.PRRef{
		Repo: "szymonrychu/tatara-operator", HeadRepo: "szymonrychu/tatara-operator",
		Number: 7, Author: "tatara-bot", HeadBranch: agent.TaskBranch(owner),
	}

	// liveOwner is what the mirror's controller ref RESOLVES TO, not what it
	// says: since #521 a ref naming a Task the API server does not have is a
	// dangling string, and resolveLiveOwner has already dropped it by here.
	if got := ClassifyPR(proj, repo, pr, owner, ""); got != PRAdopt {
		t.Fatalf("no mirror yet: ClassifyPR = %q, want %q", got, PRAdopt)
	}
	if got := ClassifyPR(proj, repo, pr, owner, liveOwnerOf(t, claimedMR(t, proj, repo, 7, owner))); got != PRAdopt {
		t.Fatalf("owner already owns it: ClassifyPR = %q, want %q (adoption stays idempotent)", got, PRAdopt)
	}
	if got := ClassifyPR(proj, repo, pr, owner, liveOwnerOf(t, claimedMR(t, proj, repo, 7, other))); got != PRClaimed {
		t.Fatalf("claimed by %s: ClassifyPR = %q, want %q", other.Name, got, PRClaimed)
	}
}

// TestSweepClaimedPRIsSkippedNotErrored is issue #477's ERROR-trickle half, end
// to end. The claiming Task is on a DIFFERENT branch (a mid-flight takeover
// hand-over), so taskForBranch cannot disambiguate and clause 1b is the net
// that catches it. The pass must come back CLEAN - counted on
// operator_sweep_skipped_total, absent from operator_sweep_errors_total - and
// it must not steal the MR.
func TestSweepClaimedPRIsSkippedNotErrored(t *testing.T) {
	proj := sweepProject("skip-proj")
	repo := sweepRepo("skip-proj")
	branchTask := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "skip-proj-clarify-branch", Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, Kind: "clarify", Goal: "g"},
	}
	branchTask.Status.State = tatarav1alpha1.StateRefined
	branchTask.Status.ParkReason = stage.ReasonAwaitingHuman
	claimant := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "skip-proj-takeover-claimant", Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, Kind: "takeover", Goal: "g"},
	}
	claimant.Status.State = tatarav1alpha1.StateMerged
	if agent.TaskBranch(branchTask) == agent.TaskBranch(claimant) {
		t.Fatal("setup: the claimant must be on a DIFFERENT branch for clause 1b to be the net")
	}

	mr := claimedMR(t, proj, repo, 209, claimant)
	c := newMirrorClient(t, proj, repo, branchTask, claimant, mr)
	rd := &sweepReader{prs: []scm.PRRef{{
		Repo: "szymonrychu/tatara-operator", HeadRepo: "szymonrychu/tatara-operator",
		Number: 209, Author: "tatara-bot", HeadBranch: agent.TaskBranch(branchTask), HeadSHA: "cafe",
	}}}

	errsBefore := testutil.ToFloat64(obs.SweepErrorsTotal.WithLabelValues(proj.Name, SweepActivity, "adopt_pr"))
	skipsBefore := testutil.ToFloat64(obs.SweepSkippedTotal.WithLabelValues(proj.Name, SweepActivity, SweepSkipMRClaimed))

	runSweep(t, c, proj, repo, rd) // pre-fix: a hard "already has controller owner" error

	if got := testutil.ToFloat64(obs.SweepErrorsTotal.WithLabelValues(proj.Name, SweepActivity, "adopt_pr")); got != errsBefore {
		t.Fatalf("adopt_pr errors = %v, want %v: an already-claimed MR is a skip, not a fault", got, errsBefore)
	}
	if got := testutil.ToFloat64(obs.SweepSkippedTotal.WithLabelValues(proj.Name, SweepActivity, SweepSkipMRClaimed)); got != skipsBefore+1 {
		t.Fatalf("skips = %v, want %v: the skip must still be COUNTED", got, skipsBefore+1)
	}
	var fresh tatarav1alpha1.MergeRequest
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: mr.Name}, &fresh); err != nil {
		t.Fatalf("get mergerequest CR: %v", err)
	}
	if got, ok := own.ControllerOwner(&fresh); !ok || got != claimant.Name {
		t.Fatalf("MR controller owner = %q/%v, want %q", got, ok, claimant.Name)
	}
	var freshBranchTask tatarav1alpha1.Task
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: branchTask.Name}, &freshBranchTask); err != nil {
		t.Fatalf("get branch task: %v", err)
	}
	if len(freshBranchTask.Status.MRRefs) != 0 {
		t.Fatalf("branch task mrRefs = %v, want none", freshBranchTask.Status.MRRefs)
	}
}
