package controller

// Author-tiered autoapprove gate tests: an "implement" triage outcome is gated
// by the issue author's tier (bot / maintainer / third-party) and, for third
// parties, by ScmSpec.AutoApproveThirdParty. The bot self-approve guard is
// covered in lifecycle_label_test.go (marker-driven); these cover the
// maintainer and third-party tiers plus the implementation-tier opt-in.

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// seedAutoApproveTask seeds a project (Owner "o", bot "tatara-bot",
// autoApproveThirdParty=tier) + repo + issueLifecycle task authored by author,
// already Succeeded with an "implement" outcome. The reader reports a plain
// (non-tatara-authored) issue body and no comments.
func seedAutoApproveTask(t *testing.T, suffix, author, tier string) (*TaskReconciler, *tatarav1alpha1.Task, *labelWriter) {
	t.Helper()
	ctx := context.Background()
	sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "aa-scm-" + suffix, Namespace: testNS}, Data: map[string][]byte{"token": []byte("tok")}}
	require.NoError(t, k8sClient.Create(ctx, sec))
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "aa-proj-" + suffix, Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{ScmSecretRef: "aa-scm-" + suffix, Scm: &tatarav1alpha1.ScmSpec{
			Provider: "github", Owner: "o", BotLogin: "tatara-bot", AutoApproveThirdParty: tier,
		}},
	}
	require.NoError(t, k8sClient.Create(ctx, proj))
	repo := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "aa-repo-" + suffix, Namespace: testNS},
		Spec:       tatarav1alpha1.RepositorySpec{ProjectRef: proj.Name, URL: "https://github.com/o/r.git", DefaultBranch: "main", ReingestSchedule: "0 6 * * *"},
	}
	require.NoError(t, k8sClient.Create(ctx, repo))
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "aa-task-" + suffix, Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, RepositoryRef: repo.Name, Kind: "issueLifecycle",
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#7", Number: 7, AuthorLogin: author}},
	}
	require.NoError(t, k8sClient.Create(ctx, task))
	markSucceededWithOutcome(t, task.Name, "implement")

	w := &labelWriter{}
	r := &TaskReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		SCMFor:    func(string) (scm.SCMWriter, error) { return w, nil },
		ReaderFor: func(_, _ string) (scm.SCMReader, error) { return &commentReader{}, nil }}
	return r, getTaskByName(t, task.Name), w
}

// Maintainer-authored implement proceeds straight to Implement (trusted human).
func TestFinishTriage_MaintainerImplement_Approved(t *testing.T) {
	r, task, w := seedAutoApproveTask(t, "maint", "o", "brainstorming")
	proj := projOf(t, task)
	_, err := r.finishTriage(context.Background(), proj, getTaskByName(t, task.Name))
	require.NoError(t, err)
	require.Equal(t, []string{"tatara-approved"}, w.added)
	require.Equal(t, "Implement", getTaskByName(t, task.Name).Status.LifecycleState)
}

// Third-party implement at the default (brainstorming) tier parks awaiting the
// maintainer: implementation is not auto-approved.
func TestFinishTriage_ThirdPartyImplement_Brainstorming_Parks(t *testing.T) {
	r, task, w := seedAutoApproveTask(t, "tp-bs", "contributor", "brainstorming")
	proj := projOf(t, task)
	_, err := r.finishTriage(context.Background(), proj, getTaskByName(t, task.Name))
	require.NoError(t, err)
	require.Equal(t, []string{"tatara-brainstorming"}, w.added)
	require.Equal(t, "Conversation", getTaskByName(t, task.Name).Status.LifecycleState)
}

// Third-party implement at the "off" tier also parks (most conservative).
func TestFinishTriage_ThirdPartyImplement_Off_Parks(t *testing.T) {
	r, task, w := seedAutoApproveTask(t, "tp-off", "contributor", "off")
	proj := projOf(t, task)
	_, err := r.finishTriage(context.Background(), proj, getTaskByName(t, task.Name))
	require.NoError(t, err)
	require.Equal(t, []string{"tatara-brainstorming"}, w.added)
	require.Equal(t, "Conversation", getTaskByName(t, task.Name).Status.LifecycleState)
}

// Third-party implement at the "implementation" tier is auto-approved and skips
// the await-approval park (the opt-in).
func TestFinishTriage_ThirdPartyImplement_ImplementationTier_Approved(t *testing.T) {
	r, task, w := seedAutoApproveTask(t, "tp-impl", "contributor", "implementation")
	proj := projOf(t, task)
	_, err := r.finishTriage(context.Background(), proj, getTaskByName(t, task.Name))
	require.NoError(t, err)
	require.Equal(t, []string{"tatara-approved"}, w.added)
	require.Equal(t, "Implement", getTaskByName(t, task.Name).Status.LifecycleState)
}

// A bot-login author (no marker) is the bot tier and parks with no human
// engagement, even at the implementation tier (the guard is inviolate).
func TestFinishTriage_BotLoginImplement_NoHuman_Parks(t *testing.T) {
	r, task, w := seedAutoApproveTask(t, "botlogin", "tatara-bot", "implementation")
	proj := projOf(t, task)
	_, err := r.finishTriage(context.Background(), proj, getTaskByName(t, task.Name))
	require.NoError(t, err)
	require.Equal(t, []string{"tatara-brainstorming"}, w.added)
	require.Equal(t, "Conversation", getTaskByName(t, task.Name).Status.LifecycleState)
}

// classifyAuthor unit coverage for the three tiers and the unknown-author
// fallback (treated as maintainer so behavior is unchanged when the author is
// not captured).
func TestClassifyAuthor_Tiers(t *testing.T) {
	r, task, _ := seedAutoApproveTask(t, "classify", "contributor", "brainstorming")
	proj := projOf(t, task)
	rdr := &commentReader{} // no marker
	r.ReaderFor = func(_, _ string) (scm.SCMReader, error) { return rdr, nil }

	cases := []struct {
		login string
		want  authorTier
	}{
		{"tatara-bot", tierBot},
		{"o", tierMaintainer},
		{"", tierMaintainer},
		{"contributor", tierThirdParty},
	}
	for _, c := range cases {
		fresh := getTaskByName(t, task.Name)
		fresh.Spec.Source.AuthorLogin = c.login
		got, err := r.classifyAuthor(context.Background(), proj, fresh)
		require.NoError(t, err)
		require.Equal(t, c.want, got, "author %q", c.login)
	}
}

// autoApproveThirdParty defaults to brainstorming when unset.
func TestAutoApproveThirdParty_Default(t *testing.T) {
	require.Equal(t, "brainstorming", autoApproveThirdParty(nil))
	require.Equal(t, "brainstorming", autoApproveThirdParty(&tatarav1alpha1.ScmSpec{}))
	require.Equal(t, "implementation", autoApproveThirdParty(&tatarav1alpha1.ScmSpec{AutoApproveThirdParty: "implementation"}))
}
