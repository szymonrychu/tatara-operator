package webhook

// Tests for item 3 (Phase 1 Task 5): every inbound comment->Task branch must
// suppress a bot-authored actor before it can spawn/reactivate a Task - an
// incident agent's own evidence comment on its tracker issue must never spawn
// a competing clarify/issue Task. isBotActor is the shared helper exercised
// here as a pure function.

import (
	"testing"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
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
