// Copyright 2026 tatara authors.

package controller

// BUG FIX: parkWithComment GitLab MR sigil ref
//
// When a bot-PR-entry task has IssueRef="" and IsPR=true, the fallback
// previously assigned a bare web URL (https://host/g/p/-/merge_requests/5)
// as commentRef.  GitLab's Comment() routes via '!' (MR) or '#' (issue)
// sigil; a bare URL has neither, so glBangRef / glHashRef return
// "gitlab: malformed issue ref" and the park comment is silently lost.
//
// The fix builds a proper sigil ref:  group/proj!iid  for GitLab MRs
// (matching every other place in the controller that constructs MR refs).

import (
	"context"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// refCapturingWriter records the issueRef argument from every Comment call.
type refCapturingWriter struct {
	lifecycleFakeSCMWriter
	capturedRefs []string
}

func (w *refCapturingWriter) Comment(_ context.Context, _, issueRef, _ string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.capturedRefs = append(w.capturedRefs, issueRef)
	return nil
}

// seedGitLabPRTask creates the project+repo+secret+task combo needed by the
// GitLab MR park test.  The repo URL is a GitLab HTTP clone URL so that
// repoSlugFromURL can derive "g/p" from it.
func seedGitLabPRTask(t *testing.T, name, proj, repo, sec string) *tatarav1alpha1.Task {
	t.Helper()
	ctx := context.Background()

	mkSecret(t, sec, map[string][]byte{"token": []byte("tok"), "webhookSecret": []byte("wh")})

	project := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: proj, Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: sec,
			Scm: &tatarav1alpha1.ScmSpec{
				Provider: "gitlab", Owner: "g", BotLogin: "tatara-bot",
			},
			Agent: tatarav1alpha1.AgentSpec{
				Model: "claude-x", Image: "wrapper:1", PermissionMode: "bypassPermissions",
				MaxTurnsPerTask: 50, TurnTimeoutSeconds: 1800,
			},
		},
	}
	if err := k8sClient.Create(ctx, project); err != nil {
		t.Fatalf("create project: %v", err)
	}
	project.Status.Memory = stableMemStatus("http://mem.svc:8080")
	if err := k8sClient.Status().Update(ctx, project); err != nil {
		t.Fatalf("set memory ready: %v", err)
	}

	r := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: repo, Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef:       proj,
			URL:              "https://gitlab.example.com/g/p.git",
			DefaultBranch:    "main",
			ReingestSchedule: "0 6 * * *",
		},
	}
	if err := k8sClient.Create(ctx, r); err != nil {
		t.Fatalf("create repo: %v", err)
	}

	// Bot-PR-entry task: IssueRef is empty, IsPR=true, Number is the MR iid.
	src := &tatarav1alpha1.TaskSource{
		Provider: "gitlab",
		IssueRef: "",
		IsPR:     true,
		URL:      "https://gitlab.example.com/g/p/-/merge_requests/5",
		Number:   5,
	}
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    proj,
			RepositoryRef: repo,
			Goal:          "MR !5: refactor auth",
			Kind:          "issueLifecycle",
			Source:        src,
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task: %v", err)
	}
	task.Status.PRNumber = 5
	task.Status.PrURL = "https://gitlab.example.com/g/p/-/merge_requests/5"
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("status update: %v", err)
	}
	return fetchTask(t, name)
}

// TestParkWithComment_GitLabMR_UsesSignilRef is a table-driven test that
// verifies parkWithComment builds a proper "group/proj!iid" sigil ref for
// GitLab MR tasks whose IssueRef is empty.  A bare web URL must NOT be
// passed to Comment(); the GitLab driver requires a sigil ref to route
// to the merge-request notes endpoint.
func TestParkWithComment_GitLabMR_UsesSignilRef(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	tests := []struct {
		name         string
		taskName     string
		projName     string
		repoName     string
		secName      string
		wantRefHas   string // substring the captured ref must contain
		wantRefNoURL string // substring the captured ref must NOT contain (bare URL)
	}{
		{
			name:         "gitlab MR entry: sigil ref not bare URL",
			taskName:     "park-gl-mr-sigil",
			projName:     "park-gl-mr-proj",
			repoName:     "park-gl-mr-repo",
			secName:      "park-gl-mr-sec",
			wantRefHas:   "!",   // MR sigil separator
			wantRefNoURL: "/-/", // fragment only present in web URLs
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := seedGitLabPRTask(t, tc.taskName, tc.projName, tc.repoName, tc.secName)

			// Set an expired deadline so the MRCI deadline path fires;
			// we call parkWithComment directly to isolate the unit.
			past := metav1.NewTime(time.Now().Add(-time.Minute))
			task.Status.DeadlineAt = &past

			fw := &refCapturingWriter{}
			r := newLifecycleReconciler(t, &fw.lifecycleFakeSCMWriter)
			r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

			if err := r.parkWithComment(ctx, task, fw, "tok", "deadline", "MR park test"); err != nil {
				t.Fatalf("parkWithComment: %v", err)
			}

			fw.mu.Lock()
			refs := make([]string, len(fw.capturedRefs))
			copy(refs, fw.capturedRefs)
			fw.mu.Unlock()

			if len(refs) == 0 {
				t.Fatal("parkWithComment did not call Comment; must post a park comment for GitLab MR tasks with empty IssueRef")
			}

			ref := refs[0]
			if !strings.Contains(ref, tc.wantRefHas) {
				t.Errorf("Comment ref = %q; want sigil %q (e.g. g/p!5)", ref, tc.wantRefHas)
			}
			if strings.Contains(ref, tc.wantRefNoURL) {
				t.Errorf("Comment ref = %q; must NOT be a bare web URL (contains %q)", ref, tc.wantRefNoURL)
			}
		})
	}
}
