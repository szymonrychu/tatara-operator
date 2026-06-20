package controller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestParseLightragStatusCounts(t *testing.T) {
	body := []byte(`{"status_counts":{"PROCESSED":130,"PENDING":10,"PROCESSING":5,"FAILED":2}}`)
	got, err := parseLightragStatusCounts(body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	want := map[string]int{"PROCESSED": 130, "PENDING": 10, "PROCESSING": 5, "FAILED": 2}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("status %q = %d, want %d", k, got[k], v)
		}
	}
}

func TestParseLightragStatusCountsEmpty(t *testing.T) {
	got, err := parseLightragStatusCounts([]byte(`{"status_counts":{}}`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	// An absent status reads 0 from the nil/empty map - the caller zero-fills.
	if got["PROCESSED"] != 0 {
		t.Errorf("PROCESSED = %d, want 0", got["PROCESSED"])
	}
}

func TestFetchLightragDocCounts(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/documents/status_counts" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status_counts":{"PROCESSED":42,"FAILED":1}}`))
	}))
	defer srv.Close()

	r := &ProjectReconciler{LightragBaseURL: func(string) string { return srv.URL }}
	counts, err := r.fetchLightragDocCounts(context.Background(), srv.Client(), "tatara")
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if counts["PROCESSED"] != 42 {
		t.Errorf("PROCESSED = %d, want 42", counts["PROCESSED"])
	}
	if counts["FAILED"] != 1 {
		t.Errorf("FAILED = %d, want 1", counts["FAILED"])
	}
}

func TestFetchLightragDocCountsHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	r := &ProjectReconciler{LightragBaseURL: func(string) string { return srv.URL }}
	if _, err := r.fetchLightragDocCounts(context.Background(), srv.Client(), "tatara"); err == nil {
		t.Fatal("expected error on HTTP 500, got nil")
	}
}
