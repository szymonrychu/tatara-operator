package controller

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

type recordingGateSCM struct {
	*gateFakeSCM
	posted []string
}

func (f *recordingGateSCM) Comment(_ context.Context, _, ref, body string) error {
	f.posted = append(f.posted, ref+"|"+body)
	return nil
}

func newGateTaskReconciler(fake *recordingGateSCM) *TaskReconciler {
	return &TaskReconciler{
		SCMFor:    func(string) (scm.SCMWriter, error) { return fake, nil },
		ReaderFor: func(string, string) (scm.SCMReader, error) { return fake, nil },
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
	}
}

func gateProjRepo(bot string) (*tatarav1alpha1.Project, *tatarav1alpha1.Repository) {
	proj := &tatarav1alpha1.Project{}
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{Provider: "github", BotLogin: bot}
	repo := &tatarav1alpha1.Repository{}
	repo.Spec.URL = "https://github.com/o/n"
	return proj, repo
}

func TestGatedComment_SuppressesLastWord(t *testing.T) {
	const bot = "tatara-bot"
	fake := &recordingGateSCM{gateFakeSCM: &gateFakeSCM{comments: []scm.IssueComment{tc(bot, 2)}}}
	proj, repo := gateProjRepo(bot)
	r := newGateTaskReconciler(fake)
	posted, err := r.gatedComment(context.Background(), proj, repo, fake, "tok", "github", 7, false, "", "o/n#7", "hello")
	if err != nil || posted {
		t.Fatalf("want (false,nil), got (%v,%v)", posted, err)
	}
	if len(fake.posted) != 0 {
		t.Fatalf("suppressed post must not call Comment, got %v", fake.posted)
	}
}

func TestGatedComment_PostsWhenOpen(t *testing.T) {
	const bot = "tatara-bot"
	fake := &recordingGateSCM{gateFakeSCM: &gateFakeSCM{comments: []scm.IssueComment{tc(bot, 1), tc("human", 2)}}}
	proj, repo := gateProjRepo(bot)
	proj.Spec.Scm.MaintainerLogins = []string{"human"}
	r := newGateTaskReconciler(fake)
	posted, err := r.gatedComment(context.Background(), proj, repo, fake, "tok", "github", 7, false, "", "o/n#7", "hello")
	if err != nil || !posted {
		t.Fatalf("want (true,nil), got (%v,%v)", posted, err)
	}
	if len(fake.posted) != 1 {
		t.Fatalf("open gate must post exactly once, got %v", fake.posted)
	}
}

func TestGatedComment_SuppressesBotMR(t *testing.T) {
	const bot = "tatara-bot"
	fake := &recordingGateSCM{gateFakeSCM: &gateFakeSCM{prAuthor: bot}}
	proj, repo := gateProjRepo(bot)
	r := newGateTaskReconciler(fake)
	posted, _ := r.gatedComment(context.Background(), proj, repo, fake, "tok", "github", 7, true, "", "o/n#7", "park note")
	if posted || len(fake.posted) != 0 {
		t.Fatalf("bot MR must be silent, got posted=%v calls=%v", posted, fake.posted)
	}
}

func TestGatedCommentCore_SuppressesDuplicateContent(t *testing.T) {
	const bot = "tatara-bot"
	fake := &recordingGateSCM{gateFakeSCM: &gateFakeSCM{comments: []scm.IssueComment{tc(bot, 2)}}}
	fake.comments[0].Body = "hello"
	proj, repo := gateProjRepo(bot)
	posted, err := gatedCommentCore(context.Background(), fake, fake, nil, proj, repo, "tok", "github", 7, false, bot, "", "o/n#7", "hello")
	if err != nil || posted {
		t.Fatalf("want (false,nil) for duplicate content, got (%v,%v)", posted, err)
	}
	if len(fake.posted) != 0 {
		t.Fatalf("suppressed post must not call Comment, got %v", fake.posted)
	}
}
