package scm

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

type ghLabel struct {
	Name string `json:"name"`
}

type ghWorkItem struct {
	Number  int       `json:"number"`
	Title   string    `json:"title"`
	Body    string    `json:"body"`
	Labels  []ghLabel `json:"labels"`
	HTMLURL string    `json:"html_url"`
}

type ghPayload struct {
	Ref        string `json:"ref"`
	After      string `json:"after"`
	Repository struct {
		CloneURL string `json:"clone_url"`
		FullName string `json:"full_name"`
	} `json:"repository"`
	Issue       *ghWorkItem `json:"issue"`
	PullRequest *ghWorkItem `json:"pull_request"`
}

// DetectAndVerify verifies the X-Hub-Signature-256 HMAC and parses the payload.
func (*GitHub) DetectAndVerify(h http.Header, payload []byte, secret string) (WebhookEvent, error) {
	if err := verifyGitHubSig(h.Get("X-Hub-Signature-256"), payload, secret); err != nil {
		return WebhookEvent{}, err
	}
	event := h.Get("X-GitHub-Event")
	var p ghPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return WebhookEvent{}, fmt.Errorf("github: parse payload: %w", err)
	}
	switch event {
	case "push":
		return WebhookEvent{Kind: "push", Repo: p.Repository.CloneURL, Branch: strings.TrimPrefix(p.Ref, "refs/heads/")}, nil
	case "issues":
		return ghWorkItemEvent("issue", p.Repository.FullName, p.Repository.CloneURL, p.Issue), nil
	case "pull_request":
		return ghWorkItemEvent("mr", p.Repository.FullName, p.Repository.CloneURL, p.PullRequest), nil
	default:
		return WebhookEvent{Kind: "other"}, nil
	}
}

func ghWorkItemEvent(kind, fullName, cloneURL string, wi *ghWorkItem) WebhookEvent {
	if wi == nil {
		return WebhookEvent{Kind: "other"}
	}
	labels := make([]string, 0, len(wi.Labels))
	for _, l := range wi.Labels {
		labels = append(labels, l.Name)
	}
	return WebhookEvent{
		Kind:     kind,
		Repo:     cloneURL,
		Labels:   labels,
		Title:    wi.Title,
		Body:     wi.Body,
		IssueRef: fmt.Sprintf("%s#%d", fullName, wi.Number),
		URL:      wi.HTMLURL,
	}
}

func verifyGitHubSig(header string, payload []byte, secret string) error {
	if header == "" {
		return errors.New("github: missing X-Hub-Signature-256")
	}
	want, ok := strings.CutPrefix(header, "sha256=")
	if !ok {
		return errors.New("github: malformed signature")
	}
	m := hmac.New(sha256.New, []byte(secret))
	m.Write(payload)
	got := hex.EncodeToString(m.Sum(nil))
	if !hmac.Equal([]byte(got), []byte(want)) {
		return errors.New("github: signature mismatch")
	}
	return nil
}
