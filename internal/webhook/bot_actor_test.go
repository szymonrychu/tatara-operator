package webhook

// Tests for item 3 (Phase 1 Task 5): every inbound comment->Task branch must
// suppress a bot-authored actor before it can spawn/reactivate a Task - an
// incident agent's own evidence comment on its tracker issue must never spawn
// a competing clarify/issue Task. isBotActor is the shared helper; these tests
// exercise it both as a pure function and at createClarifyTask's entry as a
// belt-and-suspenders guard for a caller that reaches it directly, without
// going through handleIssueComment's own guard first.

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestIsBotActor(t *testing.T) {
	proj := &tatarav1.Project{Spec: tatarav1.ProjectSpec{Scm: &tatarav1.ScmSpec{BotLogin: "tatara-bot"}}}
	cases := []struct {
		name  string
		proj  *tatarav1.Project
		login string
		want  bool
	}{
		{"matches bot login", proj, "tatara-bot", true},
		{"human login", proj, "human", false},
		{"empty login never matches", proj, "", false},
		{"nil scm fails open (false)", &tatarav1.Project{}, "tatara-bot", false},
		{"empty bot login fails open (false)", &tatarav1.Project{Spec: tatarav1.ProjectSpec{Scm: &tatarav1.ScmSpec{}}}, "tatara-bot", false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if got := isBotActor(tt.proj, tt.login); got != tt.want {
				t.Fatalf("isBotActor() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestCreateClarifyTask_BotAuthoredComment_NoTaskSpawned calls createClarifyTask
// directly (bypassing handleIssueComment's own guard) with a bot-authored
// ActorLogin, verifying the defensive guard at its entry stops it from spawning
// a clarify Task.
func TestCreateClarifyTask_BotAuthoredComment_NoTaskSpawned(t *testing.T) {
	sch := runtime.NewScheme()
	_ = tatarav1.AddToScheme(sch)
	fc := fake.NewClientBuilder().WithScheme(sch).
		WithStatusSubresource(&tatarav1.Project{}, &tatarav1.Task{}, &tatarav1.QueuedEvent{}).Build()
	seq := &queue.SeqSource{Client: fc, Namespace: "tatara"}
	s := NewServer(Config{Client: fc, Namespace: "tatara", Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()), Seq: seq})

	proj := tatarav1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "botclarify-proj", Namespace: "tatara"},
		Spec:       tatarav1.ProjectSpec{Scm: &tatarav1.ScmSpec{Provider: "github", BotLogin: "tatara-bot"}},
	}
	repo := &tatarav1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "botclarify-repo", Namespace: "tatara"},
		Spec:       tatarav1.RepositorySpec{ProjectRef: "botclarify-proj", URL: "https://github.com/o/r.git", DefaultBranch: "main"},
	}
	if err := fc.Create(context.Background(), repo); err != nil {
		t.Fatalf("create repo: %v", err)
	}

	ev := scm.WebhookEvent{
		Kind: "issue", IsComment: true, Repo: "https://github.com/o/r.git",
		IssueRef: "o/r#9", Number: 9, ActorLogin: "tatara-bot", CommentBody: "investigating",
	}
	w := httptest.NewRecorder()
	s.createClarifyTask(context.Background(), w, "github", proj, ev)

	if w.Code != 202 {
		t.Fatalf("want 202 (accepted-and-ignored), got %d", w.Code)
	}
	var tasks tatarav1.TaskList
	if err := fc.List(context.Background(), &tasks, client.InNamespace("tatara")); err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(tasks.Items) != 0 {
		t.Fatalf("bot-authored comment must not spawn a clarify Task, got %d", len(tasks.Items))
	}
	var qes tatarav1.QueuedEventList
	if err := fc.List(context.Background(), &qes, client.InNamespace("tatara")); err != nil {
		t.Fatalf("list queued events: %v", err)
	}
	if len(qes.Items) != 0 {
		t.Fatalf("bot-authored comment must not spawn a QueuedEvent, got %d", len(qes.Items))
	}
}
