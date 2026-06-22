package controller

import (
	"strings"
	"testing"
)

func TestBrainstormGoalAppendsGuidance(t *testing.T) {
	base := brainstormGoalProject([]string{"o/a"}, "STATE", "")
	if strings.Contains(base, "PROJECT CHARTER") {
		t.Fatal("empty guidance must not add a charter block")
	}
	g := brainstormGoalProject([]string{"o/a"}, "STATE", "self-hosting infra")
	if !strings.Contains(g, "PROJECT CHARTER: self-hosting infra") {
		t.Fatalf("guidance not appended: %s", g)
	}
}

func TestHealthCheckGoalAppendsGuidance(t *testing.T) {
	base := healthCheckGoalProject([]string{"o/a"}, "STATE", "")
	if strings.Contains(base, "PROJECT CHARTER") {
		t.Fatal("empty guidance must not add a charter block")
	}
	g := healthCheckGoalProject([]string{"o/a"}, "STATE", "self-hosting infra")
	if !strings.Contains(g, "PROJECT CHARTER: self-hosting infra") {
		t.Fatalf("guidance not appended: %s", g)
	}
}
