package main

import (
	"testing"

	"github.com/szymonrychu/tatara-operator/internal/scm"
)

func TestReaderForWiring(t *testing.T) {
	rd, err := scm.ReaderByProvider("github", "tok")
	if err != nil || rd == nil {
		t.Fatalf("ReaderByProvider github: %v", err)
	}
}
