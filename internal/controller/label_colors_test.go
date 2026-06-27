package controller

import (
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func TestManagedLabelColors_DefaultsAndCustom(t *testing.T) {
	m := managedLabelColors(nil)
	for _, name := range []string{
		"tatara-brainstorming", "tatara-approved", "tatara-implementation",
		"tatara-declined", "tatara-incident", "tatara-idea", "tatara-rejected",
	} {
		c, ok := m[name]
		if !ok || len(c) != 6 {
			t.Fatalf("label %q: color=%q ok=%v (want a 6-hex color)", name, c, ok)
		}
	}

	m2 := managedLabelColors(&tatarav1alpha1.ScmSpec{IncidentLabel: "oncall", BrainstormingLabel: "discuss"})
	if _, ok := m2["oncall"]; !ok {
		t.Fatal("custom incident label name must be colored")
	}
	if _, ok := m2["discuss"]; !ok {
		t.Fatal("custom brainstorming label name must be colored")
	}
}
