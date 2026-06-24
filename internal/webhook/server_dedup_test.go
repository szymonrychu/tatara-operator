package webhook_test

// Phase 2, Task 8: webhook dedup via taskMatchesItem + Spec.Source.IsPR slot.
//
// These tests verify that:
// (a) Two deliveries for the same work-item produce ONE Task (deterministic
//     name collision is idempotent).
// (b) A PR-slot event is not blocked by an issue-slot task and vice versa
//     (slot disambiguation via Spec.Source.IsPR, not LabelIsPR).
// (c) No tatara.io/source-* labels are written on new Tasks.
// (d) Spec.Source.DedupNumber is set on a bot-PR task that links an issue.

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// TestP2Dedup_TwoDeliveriesSameIssue_OneTask verifies that two webhook
// deliveries for the same labeled issue produce a single Task (first create
// wins; second sees the non-terminal Task and returns duplicate).
func TestP2Dedup_TwoDeliveriesSameIssue_OneTask(t *testing.T) {
	const secretVal = "p2dedup-issue" //gitleaks:allow
	proj := projectWithBot("p2di-proj", "p2di-scm", "tatara", "tatara-bot")
	repo := repository("p2di-repo", "p2di-proj", "https://github.com/o/r.git", "main")

	c := seedClient(t, proj, secret("p2di-scm", secretVal), repo)
	h, reg := newServer(t, c)

	body := []byte(`{"action":"labeled","sender":{"login":"alice"},"label":{"name":"tatara"},"issue":{"number":3,"title":"bug","body":"fix","labels":[{"name":"tatara"}],"html_url":"https://github.com/o/r/issues/3"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issues")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	require.Equal(t, http.StatusAccepted, post(t, h, "p2di-proj", hdr, body).Code)
	require.Equal(t, http.StatusAccepted, post(t, h, "p2di-proj", hdr, body).Code)

	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	require.Len(t, qel.Items, 1, "two deliveries for the same issue must produce exactly one QueuedEvent")

	dupCount := counterValue(t, reg, "operator_webhook_events_total",
		map[string]string{"provider": "github", "kind": "issue", "action": "labeled", "result": "duplicate"})
	require.Equal(t, 1.0, dupCount, "second delivery must be counted as duplicate")
}

// TestP2Dedup_PRSlotNotBlockedByIssueSlot verifies that a PR-slot event is
// NOT blocked by an existing issue-slot Task for the same number.
// Slot is disambiguated by Spec.Source.IsPR, not LabelIsPR.
func TestP2Dedup_PRSlotNotBlockedByIssueSlot(t *testing.T) {
	const secretVal = "p2dedup-prissue" //gitleaks:allow
	proj := projectWithBot("p2pri-proj", "p2pri-scm", "tatara", "tatara-bot")
	repo := repository("p2pri-repo", "p2pri-proj", "https://github.com/o/r.git", "main")

	// Existing issue-slot task for issue #10 (IsPR=false).
	issueTask := &tatarav1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p2pri-issue-task",
			Namespace: ns,
			Labels:    map[string]string{tatarav1.LabelSourceKind: "issueLifecycle"},
		},
		Spec: tatarav1.TaskSpec{
			ProjectRef:    "p2pri-proj",
			RepositoryRef: "p2pri-repo",
			Kind:          "issueLifecycle",
			Source: &tatarav1.TaskSource{
				Provider: "github",
				IssueRef: "o/r#10",
				Number:   10,
				IsPR:     false,
			},
		},
		Status: tatarav1.TaskStatus{LifecycleState: "Implement"},
	}

	c := seedClient(t, proj, secret("p2pri-scm", secretVal), repo)
	require.NoError(t, c.Create(context.Background(), issueTask))
	require.NoError(t, c.Status().Update(context.Background(), issueTask))

	h, reg := newServer(t, c)

	// Bot PR #10 without "Closes #N" -> PR-slot event (IsPR=true, dedupIsPR=true).
	prBody := []byte(`{"action":"opened","sender":{"login":"tatara-bot"},"pull_request":{"number":10,"title":"fix","body":"just a PR no linked issue","user":{"login":"tatara-bot"},"labels":[{"name":"tatara"}],"html_url":"https://github.com/o/r/pull/10","head":{"sha":"sha1","ref":"fix"}},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
	prHdr := http.Header{}
	prHdr.Set("X-GitHub-Event", "pull_request")
	prHdr.Set("X-Hub-Signature-256", ghSign(secretVal, prBody))

	w := post(t, h, "p2pri-proj", prHdr, prBody)
	require.Equal(t, http.StatusAccepted, w.Code)

	// The PR-slot event must NOT be blocked by the issue-slot task.
	// GitHub pull_request events are mapped to Kind="mr" by the parser.
	dupCount := counterValue(t, reg, "operator_webhook_events_total",
		map[string]string{"provider": "github", "kind": "mr", "action": "opened", "result": "duplicate"})
	require.Equal(t, 0.0, dupCount, "PR-slot event must not be blocked by an issue-slot task")

	createdCount := counterValue(t, reg, "operator_webhook_events_total",
		map[string]string{"provider": "github", "kind": "mr", "action": "opened", "result": "task_created"})
	require.Equal(t, 1.0, createdCount, "PR-slot event must create a new task when only an issue-slot task exists")
}

// TestP2Dedup_NoSourceDedupeLabelsWritten verifies that the created Task
// carries NO tatara.io/source-repo, source-number, or head-sha labels.
func TestP2Dedup_NoSourceDedupeLabelsWritten(t *testing.T) {
	const secretVal = "p2dedup-nolabels" //gitleaks:allow
	proj := projectWithBot("p2nl-proj", "p2nl-scm", "tatara", "tatara-bot")
	repo := repository("p2nl-repo", "p2nl-proj", "https://github.com/o/r.git", "main")

	c := seedClient(t, proj, secret("p2nl-scm", secretVal), repo)
	h, _ := newServer(t, c)

	body := []byte(`{"action":"labeled","sender":{"login":"alice"},"label":{"name":"tatara"},"issue":{"number":11,"title":"test","body":"body","labels":[{"name":"tatara"}],"html_url":"https://github.com/o/r/issues/11"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issues")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	require.Equal(t, http.StatusAccepted, post(t, h, "p2nl-proj", hdr, body).Code)

	// The QueuedEvent's Payload.Labels must not contain the 3 dedup labels.
	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	require.Len(t, qel.Items, 1)

	labels := qel.Items[0].Spec.Payload.Labels
	// Use string literals; the LabelSource* consts are deleted in Phase 2 Task 9.
	for _, forbidden := range []string{
		"tatara.io/source-repo",
		"tatara.io/source-number",
		"tatara.io/head-sha",
	} {
		if _, ok := labels[forbidden]; ok {
			t.Errorf("created task must not carry label %q; labels = %v", forbidden, labels)
		}
	}
}

// TestP2Dedup_SlotViaSrcIsPR_NotLabel verifies that slot disambiguation uses
// Spec.Source.IsPR (not LabelIsPR) for tasks that have a Spec.Source set.
// An issue-slot task with Spec.Source.IsPR=false must NOT block a PR-slot
// event even when LabelIsPR is absent (no label -> default "false" would
// INCORRECTLY block in the old label-based check if we had a PR task with
// no label, but actually here we test the inverse: issue task IsPR=false
// correctly lets PR event through).
func TestP2Dedup_SlotViaSrcIsPR_NotLabel(t *testing.T) {
	const secretVal = "p2dedup-slot" //gitleaks:allow
	proj := projectWithBot("p2sl-proj", "p2sl-scm", "tatara", "tatara-bot")
	repo := repository("p2sl-repo", "p2sl-proj", "https://github.com/o/r.git", "main")

	// Issue-slot task with Spec.Source.IsPR=false AND LabelIsPR NOT SET.
	issueTask := &tatarav1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p2sl-issue-task",
			Namespace: ns,
			// Deliberately omit LabelIsPR to ensure Spec.Source.IsPR is used.
			Labels: map[string]string{tatarav1.LabelSourceKind: "issueLifecycle"},
		},
		Spec: tatarav1.TaskSpec{
			ProjectRef:    "p2sl-proj",
			RepositoryRef: "p2sl-repo",
			Kind:          "issueLifecycle",
			Source: &tatarav1.TaskSource{
				Provider: "github",
				IssueRef: "o/r#15",
				Number:   15,
				IsPR:     false,
			},
		},
		Status: tatarav1.TaskStatus{LifecycleState: "Implement"},
	}

	c := seedClient(t, proj, secret("p2sl-scm", secretVal), repo)
	require.NoError(t, c.Create(context.Background(), issueTask))
	require.NoError(t, c.Status().Update(context.Background(), issueTask))

	h, reg := newServer(t, c)

	// PR-slot event for PR #15 (no "Closes" link -> it IS a PR slot).
	prBody := []byte(`{"action":"opened","sender":{"login":"tatara-bot"},"pull_request":{"number":15,"title":"fix","body":"no closes link","user":{"login":"tatara-bot"},"labels":[{"name":"tatara"}],"html_url":"https://github.com/o/r/pull/15","head":{"sha":"sha15","ref":"fix-15"}},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
	prHdr := http.Header{}
	prHdr.Set("X-GitHub-Event", "pull_request")
	prHdr.Set("X-Hub-Signature-256", ghSign(secretVal, prBody))

	w := post(t, h, "p2sl-proj", prHdr, prBody)
	require.Equal(t, http.StatusAccepted, w.Code)

	dupCount := counterValue(t, reg, "operator_webhook_events_total",
		map[string]string{"provider": "github", "kind": "mr", "action": "opened", "result": "duplicate"})
	require.Equal(t, 0.0, dupCount, "PR-slot event must not be blocked by issue-slot task (Spec.Source.IsPR=false)")
}

// TestP2Dedup_BotPRClosesIssue_DedupsAgainstScanTask verifies the cross-kind
// dedup the migration relies on: a bot PR "Closes #7" must dedup against an
// existing scan-created Task for issue #7 that carries LabelSourceKind=issueScan
// (NOT issueLifecycle) and NO legacy source-* labels. The webhook list must not
// pre-filter on LabelSourceKind, and the match must hit via TaskMatchesItem on
// (dedupRepo, dedupNumber=7) + Spec.Source identity, not the legacy-label shortcut.
func TestP2Dedup_BotPRClosesIssue_DedupsAgainstScanTask(t *testing.T) {
	const secretVal = "p2dedup-crosskind" //gitleaks:allow
	proj := projectWithBot("p2ck-proj", "p2ck-scm", "tatara", "tatara-bot")
	repo := repository("p2ck-repo", "p2ck-proj", "https://github.com/o/r.git", "main")

	// Scan-created issue task for issue #7: kind=issueScan, NO source-* labels,
	// identity only via Spec.Source (the Phase-2 path the migration depends on).
	scanTask := &tatarav1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p2ck-scan-issue7",
			Namespace: ns,
			Labels:    map[string]string{tatarav1.LabelSourceKind: "issueScan"},
		},
		Spec: tatarav1.TaskSpec{
			ProjectRef:    "p2ck-proj",
			RepositoryRef: "p2ck-repo",
			Kind:          "issueLifecycle",
			Source: &tatarav1.TaskSource{
				Provider: "github",
				IssueRef: "o/r#7",
				Number:   7,
				IsPR:     false,
			},
		},
		Status: tatarav1.TaskStatus{LifecycleState: "Implement"},
	}

	c := seedClient(t, proj, secret("p2ck-scm", secretVal), repo)
	require.NoError(t, c.Create(context.Background(), scanTask))
	require.NoError(t, c.Status().Update(context.Background(), scanTask))

	h, reg := newServer(t, c)

	// Bot PR #21 closing issue #7 -> dedup slot is issue #7 (dedupIsPR=false).
	prBody := []byte(`{"action":"opened","sender":{"login":"tatara-bot"},"pull_request":{"number":21,"title":"fix","body":"Closes #7","user":{"login":"tatara-bot"},"labels":[{"name":"tatara"}],"html_url":"https://github.com/o/r/pull/21","head":{"sha":"sha21","ref":"fix-7"}},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
	prHdr := http.Header{}
	prHdr.Set("X-GitHub-Event", "pull_request")
	prHdr.Set("X-Hub-Signature-256", ghSign(secretVal, prBody))

	require.Equal(t, http.StatusAccepted, post(t, h, "p2ck-proj", prHdr, prBody).Code)

	// No new QueuedEvent: the scan task already owns issue #7's slot.
	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	require.Len(t, qel.Items, 0, "bot PR 'Closes #7' must dedup against the existing issueScan task, not create a QueuedEvent")

	dupCount := counterValue(t, reg, "operator_webhook_events_total",
		map[string]string{"provider": "github", "kind": "mr", "action": "opened", "result": "duplicate"})
	require.Equal(t, 1.0, dupCount, "cross-kind dedup must produce result=duplicate")
}

// TestP2Dedup_IssueSlotNotBlockedByPRSlot is the reverse of
// TestP2Dedup_PRSlotNotBlockedByIssueSlot: an issues.labeled event for issue #N
// must NOT be deduped by an existing PR-slot Task for the same number (slot
// disambiguation via Spec.Source.IsPR lets the issue through).
func TestP2Dedup_IssueSlotNotBlockedByPRSlot(t *testing.T) {
	const secretVal = "p2dedup-issnotpr" //gitleaks:allow
	proj := projectWithBot("p2ip-proj", "p2ip-scm", "tatara", "tatara-bot")
	repo := repository("p2ip-repo", "p2ip-proj", "https://github.com/o/r.git", "main")

	// Existing PR-slot task for PR #12 (IsPR=true).
	prTask := &tatarav1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p2ip-pr-task",
			Namespace: ns,
			Labels:    map[string]string{tatarav1.LabelSourceKind: "issueLifecycle"},
		},
		Spec: tatarav1.TaskSpec{
			ProjectRef:    "p2ip-proj",
			RepositoryRef: "p2ip-repo",
			Kind:          "issueLifecycle",
			Source: &tatarav1.TaskSource{
				Provider: "github",
				IssueRef: "o/r#12",
				Number:   12,
				IsPR:     true,
			},
		},
		Status: tatarav1.TaskStatus{LifecycleState: "MRCI"},
	}

	c := seedClient(t, proj, secret("p2ip-scm", secretVal), repo)
	require.NoError(t, c.Create(context.Background(), prTask))
	require.NoError(t, c.Status().Update(context.Background(), prTask))

	h, reg := newServer(t, c)

	// issues.labeled for issue #12 -> issue slot (dedupIsPR=false).
	body := []byte(`{"action":"labeled","sender":{"login":"alice"},"label":{"name":"tatara"},"issue":{"number":12,"title":"bug","body":"fix","labels":[{"name":"tatara"}],"html_url":"https://github.com/o/r/issues/12"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issues")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	require.Equal(t, http.StatusAccepted, post(t, h, "p2ip-proj", hdr, body).Code)

	dupCount := counterValue(t, reg, "operator_webhook_events_total",
		map[string]string{"provider": "github", "kind": "issue", "action": "labeled", "result": "duplicate"})
	require.Equal(t, 0.0, dupCount, "issue-slot event must not be blocked by a PR-slot task")

	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	require.Len(t, qel.Items, 1, "issue-slot event must create a new QueuedEvent when only a PR-slot task exists")
}

// TestP2Dedup_BotPRLinkedIssue_DedupNumber verifies that when a bot PR with
// "Closes #N" is processed, the created Task's Source.DedupNumber is set to N
// (the linked issue number), not the PR number.
func TestP2Dedup_BotPRLinkedIssue_DedupNumber(t *testing.T) {
	const secretVal = "p2dedup-botpr" //gitleaks:allow
	proj := projectWithBot("p2bp-proj", "p2bp-scm", "tatara", "tatara-bot")
	repo := repository("p2bp-repo", "p2bp-proj", "https://github.com/o/r.git", "main")

	c := seedClient(t, proj, secret("p2bp-scm", secretVal), repo)
	h, _ := newServer(t, c)

	// Bot PR #20 with "Closes #5" in the body.
	prBody := []byte(`{"action":"opened","sender":{"login":"tatara-bot"},"pull_request":{"number":20,"title":"fix issue 5","body":"Closes #5","user":{"login":"tatara-bot"},"labels":[{"name":"tatara"}],"html_url":"https://github.com/o/r/pull/20","head":{"sha":"sha-bot","ref":"fix-5"}},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
	prHdr := http.Header{}
	prHdr.Set("X-GitHub-Event", "pull_request")
	prHdr.Set("X-Hub-Signature-256", ghSign(secretVal, prBody))

	require.Equal(t, http.StatusAccepted, post(t, h, "p2bp-proj", prHdr, prBody).Code)

	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	require.Len(t, qel.Items, 1)

	src := qel.Items[0].Spec.Payload.Source
	require.NotNil(t, src)
	require.Equal(t, 20, src.Number, "Source.Number must be the PR number")
	require.Equal(t, 5, src.DedupNumber, "Source.DedupNumber must be the linked issue number (5)")
}
