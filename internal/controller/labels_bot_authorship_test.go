package controller

import (
	"context"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// authorStateWriter is a minimal scm.SCMWriter stub exposing only
// GetIssueState, for isBotAuthoredProposal's live GitLab author-verification
// path (FIX-4).
type authorStateWriter struct {
	scm.SCMWriter
	author string
	err    error
}

func (w *authorStateWriter) GetIssueState(_ context.Context, _, _ string, _ int) (scm.IssueState, error) {
	return scm.IssueState{Author: w.author}, w.err
}

// TestIsBotAuthoredProposal_GitHub_TrustsHint: on GitHub, Source.AuthorLogin
// IS the real resource author, so the hint alone decides - no live read.
func TestIsBotAuthoredProposal_GitHub_TrustsHint(t *testing.T) {
	proj := &tatarav1alpha1.Project{Spec: tatarav1alpha1.ProjectSpec{Scm: &tatarav1alpha1.ScmSpec{BotLogin: "tatara-bot"}}}
	task := &tatarav1alpha1.Task{Spec: tatarav1alpha1.TaskSpec{Source: &tatarav1alpha1.TaskSource{IssueRef: "o/r#5", AuthorLogin: "tatara-bot"}}}
	if !isBotAuthoredProposal(context.Background(), nil, "https://github.com/o/r.git", "tok", "github", proj, task) {
		t.Fatal("github hint must be trusted without a live read (nil writer must not matter)")
	}
}

func TestIsBotAuthoredProposal_GitHub_HumanHint(t *testing.T) {
	proj := &tatarav1alpha1.Project{Spec: tatarav1alpha1.ProjectSpec{Scm: &tatarav1alpha1.ScmSpec{BotLogin: "tatara-bot"}}}
	task := &tatarav1alpha1.Task{Spec: tatarav1alpha1.TaskSpec{Source: &tatarav1alpha1.TaskSource{IssueRef: "o/r#5", AuthorLogin: "human"}}}
	if isBotAuthoredProposal(context.Background(), nil, "https://github.com/o/r.git", "tok", "github", proj, task) {
		t.Fatal("human hint must not be treated as bot-authored")
	}
}

// TestIsBotAuthoredProposal_GitLab_HintIgnored_LiveAuthorWins: on GitLab,
// Source.AuthorLogin carries the webhook ACTOR (e.g. a human re-triggering
// triage), not the real issue author - the hint alone must never decide.
func TestIsBotAuthoredProposal_GitLab_HintIgnored_LiveAuthorWins(t *testing.T) {
	proj := &tatarav1alpha1.Project{Spec: tatarav1alpha1.ProjectSpec{Scm: &tatarav1alpha1.ScmSpec{BotLogin: "tatara-bot"}}}
	task := &tatarav1alpha1.Task{Spec: tatarav1alpha1.TaskSpec{Source: &tatarav1alpha1.TaskSource{IssueRef: "g/p#5", AuthorLogin: "human-actor"}}}
	w := &authorStateWriter{author: "tatara-bot"}
	if !isBotAuthoredProposal(context.Background(), w, "https://gitlab.com/g/p.git", "tok", "gitlab", proj, task) {
		t.Fatal("gitlab must verify live author, ignoring the actor hint")
	}
}

// TestIsBotAuthoredProposal_GitLab_SpoofedHint_LiveAuthorWins is the adversarial
// case: a human-authored issue whose Source.AuthorLogin (webhook actor) happens
// to equal the bot login must NOT be treated as bot-authored on GitLab.
func TestIsBotAuthoredProposal_GitLab_SpoofedHint_LiveAuthorWins(t *testing.T) {
	proj := &tatarav1alpha1.Project{Spec: tatarav1alpha1.ProjectSpec{Scm: &tatarav1alpha1.ScmSpec{BotLogin: "tatara-bot"}}}
	task := &tatarav1alpha1.Task{Spec: tatarav1alpha1.TaskSpec{Source: &tatarav1alpha1.TaskSource{IssueRef: "g/p#5", AuthorLogin: "tatara-bot"}}}
	w := &authorStateWriter{author: "human"}
	if isBotAuthoredProposal(context.Background(), w, "https://gitlab.com/g/p.git", "tok", "gitlab", proj, task) {
		t.Fatal("gitlab live author must win over a spoofed-looking actor hint")
	}
}

func TestIsBotAuthoredProposal_GitLab_ReadError_FailsClosed(t *testing.T) {
	proj := &tatarav1alpha1.Project{Spec: tatarav1alpha1.ProjectSpec{Scm: &tatarav1alpha1.ScmSpec{BotLogin: "tatara-bot"}}}
	task := &tatarav1alpha1.Task{Spec: tatarav1alpha1.TaskSpec{Source: &tatarav1alpha1.TaskSource{IssueRef: "g/p#5", AuthorLogin: "tatara-bot"}}}
	w := &authorStateWriter{err: context.DeadlineExceeded}
	if isBotAuthoredProposal(context.Background(), w, "https://gitlab.com/g/p.git", "tok", "gitlab", proj, task) {
		t.Fatal("an SCM read error on gitlab must fail closed (not bot-authored)")
	}
}

func TestIsBotAuthoredProposal_GitLab_NilWriter_FailsClosed(t *testing.T) {
	proj := &tatarav1alpha1.Project{Spec: tatarav1alpha1.ProjectSpec{Scm: &tatarav1alpha1.ScmSpec{BotLogin: "tatara-bot"}}}
	task := &tatarav1alpha1.Task{Spec: tatarav1alpha1.TaskSpec{Source: &tatarav1alpha1.TaskSource{IssueRef: "g/p#5", AuthorLogin: "tatara-bot"}}}
	if isBotAuthoredProposal(context.Background(), nil, "https://gitlab.com/g/p.git", "tok", "gitlab", proj, task) {
		t.Fatal("a nil writer on gitlab must fail closed")
	}
}
