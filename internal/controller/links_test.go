package controller

import (
	"strings"
	"testing"
)

func TestRenderLinksBlock_TwoURLs(t *testing.T) {
	got := RenderLinksBlock([]string{"o/a#1", "o/b#2"})
	want := "<!-- tatara-links:start -->\nRelated issues (same task): o/a#1, o/b#2\n<!-- tatara-links:end -->"
	if got != want {
		t.Fatalf("RenderLinksBlock() = %q, want %q", got, want)
	}
}

func TestSpliceLinksBlock_AppendsWhenAbsent(t *testing.T) {
	body := "original body text"
	block := RenderLinksBlock([]string{"o/a#1"})
	got := SpliceLinksBlock(body, block)
	if !strings.Contains(got, "original body text") || !strings.Contains(got, block) {
		t.Fatalf("SpliceLinksBlock() = %q, want original body + appended block", got)
	}
}

func TestSpliceLinksBlock_IdempotentRewrite(t *testing.T) {
	body := "original body text\n\n" + RenderLinksBlock([]string{"o/a#1"})
	newBlock := RenderLinksBlock([]string{"o/a#1", "o/b#2"})
	got := SpliceLinksBlock(body, newBlock)
	if strings.Count(got, "tatara-links:start") != 1 {
		t.Fatalf("SpliceLinksBlock() must rewrite in place, not duplicate the block: %q", got)
	}
	if !strings.Contains(got, "o/b#2") || !strings.Contains(got, "original body text") {
		t.Fatalf("SpliceLinksBlock() = %q, want rewritten block + preserved surrounding body", got)
	}
}
