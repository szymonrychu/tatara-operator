package agent

import (
	"encoding/json"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBuildPod_ExtraSkillSources_WhenSet(t *testing.T) {
	p := &tatarav1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "mtg"}}
	p.Spec.Agent.SkillSources = []tatarav1alpha1.AgentSkillSource{
		{Name: "example-skills", URL: "https://github.com/example/example-skills", Ref: "main", Subdir: ".claude/skills"},
	}
	m := podEnvMap(t, p)
	raw, ok := m["TATARA_EXTRA_SKILL_SOURCES"]
	if !ok {
		t.Fatal("TATARA_EXTRA_SKILL_SOURCES absent when sources set")
	}
	var got []map[string]string
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("env is not JSON: %v (%q)", err, raw)
	}
	if len(got) != 1 || got[0]["name"] != "example-skills" ||
		got[0]["url"] != "https://github.com/example/example-skills" ||
		got[0]["ref"] != "main" || got[0]["subdir"] != ".claude/skills" {
		t.Fatalf("bad payload: %v", got)
	}
}

func TestBuildPod_NoExtraSkillSources_WhenEmpty(t *testing.T) {
	p := &tatarav1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "mtg"}}
	m := podEnvMap(t, p)
	if _, ok := m["TATARA_EXTRA_SKILL_SOURCES"]; ok {
		t.Fatal("TATARA_EXTRA_SKILL_SOURCES must be absent when no sources set")
	}
}
