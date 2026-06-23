package controller

import (
	"testing"
)

func TestElectSystemicLeads(t *testing.T) {
	cands := []candidate{
		{repo: "o/r1", number: 15, labels: []string{"tatara/systemic-abc"}, title: "C"},
		{repo: "o/r1", number: 12, labels: []string{"tatara/systemic-abc"}, title: "A"},
		{repo: "o/r2", number: 9, labels: []string{"tatara/systemic-abc"}, title: "B"},
		{repo: "o/r1", number: 7, labels: []string{"bug"}, title: "standalone"},
	}
	got := electSystemicLeads(cands)
	if _, ok := got["o/r1#7"]; ok {
		t.Fatal("standalone (no systemic label) must not be in the map")
	}
	lead := got["o/r1#12"]
	if !lead.isLead || lead.leadNumber != 12 {
		t.Fatalf("o/r1#12 should be lead: %+v", lead)
	}
	if len(lead.sameRepoSiblings) != 1 || lead.sameRepoSiblings[0] != 15 {
		t.Fatalf("lead sameRepoSiblings want [15]: %+v", lead.sameRepoSiblings)
	}
	if len(lead.crossRepo) != 1 || lead.crossRepo[0] != "o/r2#9 - B" {
		t.Fatalf("lead crossRepo want [o/r2#9 - B]: %+v", lead.crossRepo)
	}
	sib := got["o/r1#15"]
	if sib.isLead || sib.leadNumber != 12 {
		t.Fatalf("o/r1#15 should be non-lead pointing at 12: %+v", sib)
	}
	r2lead := got["o/r2#9"]
	if !r2lead.isLead || r2lead.leadNumber != 9 {
		t.Fatalf("o/r2#9 should be its repo's lead: %+v", r2lead)
	}
}
