package controller

import (
	"context"
	"sync"

	"github.com/szymonrychu/tatara-operator/internal/agent"
)

// Compile-time check: fakeSession satisfies agent.Session.
var _ agent.Session = (*fakeSession)(nil)

type submittedTurn struct { //nolint:unused // used by Task-6 reconciler tests
	BaseURL, Text, CallbackURL, TurnID string
}

// fakeSession records SubmitTurn/GetTurn/DeleteSession calls and returns
// scripted turn ids. It is safe for concurrent use by the reconciler.
type fakeSession struct { //nolint:unused // used by Task-6 reconciler tests
	mu        sync.Mutex
	submits   []submittedTurn
	nextID    int
	getResult map[string]agent.TurnResult
	deleted   []string
	submitErr error
}

func newFakeSession() *fakeSession { //nolint:unused // used by Task-6 reconciler tests
	return &fakeSession{getResult: map[string]agent.TurnResult{}}
}

func (f *fakeSession) SubmitTurn(_ context.Context, baseURL, text, callbackURL string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.submitErr != nil {
		return "", f.submitErr
	}
	f.nextID++
	id := "turn-" + itoa(f.nextID)
	f.submits = append(f.submits, submittedTurn{BaseURL: baseURL, Text: text, CallbackURL: callbackURL, TurnID: id})
	return id, nil
}

func (f *fakeSession) GetTurn(_ context.Context, _ string, turnID string) (agent.TurnResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.getResult[turnID], nil
}

func (f *fakeSession) DeleteSession(_ context.Context, baseURL string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, baseURL)
	return nil
}

func (f *fakeSession) lastSubmit() (submittedTurn, bool) { //nolint:unused // used by Task-6 reconciler tests
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.submits) == 0 {
		return submittedTurn{}, false
	}
	return f.submits[len(f.submits)-1], true
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
