package controller

import (
	"context"
	"strconv"
	"sync"

	"github.com/szymonrychu/tatara-operator/internal/agent"
)

// Compile-time check: fakeSession satisfies agent.Session.
var _ agent.Session = (*fakeSession)(nil)

type submittedTurn struct {
	BaseURL, Text, CallbackURL, TurnID string
}

// fakeSession records SubmitTurn/GetTurn/DeleteSession calls and returns
// scripted turn ids. It is safe for concurrent use by the reconciler.
type interjection struct {
	BaseURL, Text string
}

type fakeSession struct {
	mu           sync.Mutex
	submits      []submittedTurn
	handoffs     []submittedTurn
	nextID       int
	getResult    map[string]agent.TurnResult
	deleted      []string
	submitErr    error
	interjects   []interjection
	interjectErr error
	// sessionInfo is what GetSession returns; sessionErr overrides it.
	sessionInfo agent.SessionInfo
	sessionErr  error
}

func newFakeSession() *fakeSession {
	v := agent.ContractVersion
	return &fakeSession{
		getResult:   map[string]agent.TurnResult{},
		sessionInfo: agent.SessionInfo{State: agent.SessionStateReady, ContractVersion: &v},
	}
}

func (f *fakeSession) SubmitTurn(_ context.Context, baseURL, text, callbackURL string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.submitErr != nil {
		return "", f.submitErr
	}
	f.nextID++
	id := "turn-" + strconv.Itoa(f.nextID)
	f.submits = append(f.submits, submittedTurn{BaseURL: baseURL, Text: text, CallbackURL: callbackURL, TurnID: id})
	return id, nil
}

func (f *fakeSession) SubmitHandoffTurn(_ context.Context, baseURL, text, callbackURL string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.submitErr != nil {
		return "", f.submitErr
	}
	f.nextID++
	id := "turn-" + strconv.Itoa(f.nextID)
	f.handoffs = append(f.handoffs, submittedTurn{BaseURL: baseURL, Text: text, CallbackURL: callbackURL, TurnID: id})
	return id, nil
}

func (f *fakeSession) GetSession(_ context.Context, _ string) (agent.SessionInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sessionErr != nil {
		return agent.SessionInfo{}, f.sessionErr
	}
	return f.sessionInfo, nil
}

func (f *fakeSession) Interject(_ context.Context, baseURL, text string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.interjectErr != nil {
		return f.interjectErr
	}
	f.interjects = append(f.interjects, interjection{BaseURL: baseURL, Text: text})
	return nil
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

func (f *fakeSession) lastSubmit() (submittedTurn, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.submits) == 0 {
		return submittedTurn{}, false
	}
	return f.submits[len(f.submits)-1], true
}
