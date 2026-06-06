package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// TokenFunc mints a bearer token for the wrapper audience.
type TokenFunc func(ctx context.Context) (string, error)

// httpSession is the production Session, talking to a wrapper pod's REST API.
type httpSession struct {
	token TokenFunc
	hc    *http.Client
}

// NewHTTPSession returns a Session that authenticates wrapper calls with a
// bearer minted by token (audience tatara-claude-code-wrapper).
func NewHTTPSession(token TokenFunc) Session {
	return &httpSession{token: token, hc: &http.Client{Timeout: 30 * time.Second}}
}

func (s *httpSession) do(ctx context.Context, method, url string, body any, out any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("agent: marshal body: %w", err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rdr)
	if err != nil {
		return fmt.Errorf("agent: new request: %w", err)
	}
	tok, err := s.token(ctx)
	if err != nil {
		return fmt.Errorf("agent: mint token: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := s.hc.Do(req)
	if err != nil {
		return fmt.Errorf("agent: do request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return &HTTPError{Status: resp.StatusCode, Body: string(b)}
	}
	if out != nil && resp.StatusCode != http.StatusNoContent {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return fmt.Errorf("agent: decode response: %w", err)
		}
	}
	return nil
}

func (s *httpSession) SubmitTurn(ctx context.Context, baseURL, text, callbackURL string) (string, error) {
	body := map[string]string{"text": text, "callbackUrl": callbackURL}
	var out struct {
		TurnID string `json:"turnId"`
	}
	if err := s.do(ctx, http.MethodPost, baseURL+"/v1/messages", body, &out); err != nil {
		return "", err
	}
	return out.TurnID, nil
}

func (s *httpSession) GetTurn(ctx context.Context, baseURL, turnID string) (TurnResult, error) {
	var out struct {
		State      string `json:"state"`
		FinalText  string `json:"finalText"`
		StopReason string `json:"stopReason"`
		Error      string `json:"error"`
	}
	if err := s.do(ctx, http.MethodGet, baseURL+"/v1/messages/"+turnID, nil, &out); err != nil {
		return TurnResult{}, err
	}
	return TurnResult{State: out.State, FinalText: out.FinalText, StopReason: out.StopReason, Err: out.Error}, nil
}

func (s *httpSession) DeleteSession(ctx context.Context, baseURL string) error {
	return s.do(ctx, http.MethodDelete, baseURL+"/v1/session", nil, nil)
}

var _ Session = (*httpSession)(nil)
