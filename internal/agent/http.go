package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// TokenFunc mints a bearer token for the wrapper audience.
type TokenFunc func(ctx context.Context) (string, error)

// AgentHTTPRecorder is implemented by obs.OperatorMetrics; it records the
// method, outcome ("ok", "http_error", "unreachable", "timeout"), and latency
// of every agent wrapper HTTP call.
type AgentHTTPRecorder interface {
	AgentHTTP(method, outcome string, seconds float64)
}

// httpSession is the production Session, talking to a wrapper pod's REST API.
type httpSession struct {
	token   TokenFunc
	hc      *http.Client
	metrics AgentHTTPRecorder // nil when no instrumentation is configured
}

// NewHTTPSession returns a Session that authenticates wrapper calls with a
// bearer minted by token (audience tatara-claude-code-wrapper).
func NewHTTPSession(token TokenFunc) Session {
	return &httpSession{token: token, hc: &http.Client{Timeout: 30 * time.Second}}
}

// NewHTTPSessionWithMetrics is like NewHTTPSession but instruments every do()
// call via rec. Use this in production; NewHTTPSession is a zero-metrics
// convenience for tests that do not need the counter/histogram overhead.
func NewHTTPSessionWithMetrics(token TokenFunc, rec AgentHTTPRecorder) Session {
	return &httpSession{token: token, hc: &http.Client{Timeout: 30 * time.Second}, metrics: rec}
}

// do executes one HTTP call and records metrics when s.metrics is set.
// logicalMethod is the high-level operation name used as the "method" label
// (e.g. "submit_turn"); it is derived by the caller shims below.
func (s *httpSession) do(ctx context.Context, logicalMethod, httpMethod, url string, body any, out any) error {
	start := time.Now()
	outcome, err := s.doOnce(ctx, httpMethod, url, body, out)
	if s.metrics != nil {
		s.metrics.AgentHTTP(logicalMethod, outcome, time.Since(start).Seconds())
	}
	return err
}

func (s *httpSession) doOnce(ctx context.Context, method, url string, body any, out any) (outcome string, err error) {
	var rdr io.Reader
	if body != nil {
		b, merr := json.Marshal(body)
		if merr != nil {
			return "client_error", fmt.Errorf("agent: marshal body: %w", merr)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rdr)
	if err != nil {
		return "client_error", fmt.Errorf("agent: new request: %w", err)
	}
	tok, err := s.token(ctx)
	if err != nil {
		return "token_error", fmt.Errorf("agent: mint token: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := s.hc.Do(req)
	if err != nil {
		if isUnreachable(err) {
			return "unreachable", &UnreachableError{Err: err}
		}
		// Distinguish context timeout/deadline from other transport errors.
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return "timeout", fmt.Errorf("agent: do request: %w", err)
		}
		return "transport_error", fmt.Errorf("agent: do request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return "http_error", &HTTPError{Status: resp.StatusCode, Body: string(b)}
	}
	if out != nil && resp.StatusCode != http.StatusNoContent {
		if derr := json.NewDecoder(resp.Body).Decode(out); derr != nil {
			return "decode_error", fmt.Errorf("agent: decode response: %w", derr)
		}
	}
	return "ok", nil
}

func (s *httpSession) SubmitTurn(ctx context.Context, baseURL, text, callbackURL string) (string, error) {
	body := map[string]string{"text": text, "callbackUrl": callbackURL}
	var out struct {
		TurnID string `json:"turnId"`
	}
	if err := s.do(ctx, "submit_turn", http.MethodPost, baseURL+"/v1/messages", body, &out); err != nil {
		return "", err
	}
	return out.TurnID, nil
}

func (s *httpSession) Interject(ctx context.Context, baseURL, text string) error {
	return s.do(ctx, "interject", http.MethodPost, baseURL+"/v1/interject", map[string]string{"text": text}, nil)
}

func (s *httpSession) GetTurn(ctx context.Context, baseURL, turnID string) (TurnResult, error) {
	var out struct {
		State      string `json:"state"`
		FinalText  string `json:"finalText"`
		StopReason string `json:"stopReason"`
		Error      string `json:"error"`
	}
	if err := s.do(ctx, "get_turn", http.MethodGet, baseURL+"/v1/messages/"+turnID, nil, &out); err != nil {
		return TurnResult{}, err
	}
	return TurnResult{State: out.State, FinalText: out.FinalText, StopReason: out.StopReason, Err: out.Error}, nil
}

func (s *httpSession) DeleteSession(ctx context.Context, baseURL string) error {
	return s.do(ctx, "delete_session", http.MethodDelete, baseURL+"/v1/session", nil, nil)
}

var _ Session = (*httpSession)(nil)
