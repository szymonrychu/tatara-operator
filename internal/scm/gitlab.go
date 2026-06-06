package scm

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

type glLabel struct {
	Title string `json:"title"`
}

type glPayload struct {
	ObjectKind string `json:"object_kind"`
	Ref        string `json:"ref"`
	Project    struct {
		GitHTTPURL        string `json:"git_http_url"`
		PathWithNamespace string `json:"path_with_namespace"`
	} `json:"project"`
	ObjectAttributes struct {
		IID         int    `json:"iid"`
		Title       string `json:"title"`
		Description string `json:"description"`
		URL         string `json:"url"`
	} `json:"object_attributes"`
	Labels []glLabel `json:"labels"`
}

// DetectAndVerify verifies the X-Gitlab-Token and parses the payload.
func (*GitLab) DetectAndVerify(h http.Header, payload []byte, secret string) (WebhookEvent, error) {
	token := h.Get("X-Gitlab-Token")
	if token == "" {
		return WebhookEvent{}, errors.New("gitlab: missing X-Gitlab-Token")
	}
	if subtle.ConstantTimeCompare([]byte(token), []byte(secret)) != 1 {
		return WebhookEvent{}, errors.New("gitlab: token mismatch")
	}
	var p glPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return WebhookEvent{}, fmt.Errorf("gitlab: parse payload: %w", err)
	}
	switch h.Get("X-Gitlab-Event") {
	case "Push Hook":
		return WebhookEvent{Kind: "push", Repo: p.Project.GitHTTPURL, Branch: trimGitLabRef(p.Ref)}, nil
	case "Issue Hook":
		return glWorkItemEvent("issue", p), nil
	case "Merge Request Hook":
		return glWorkItemEvent("mr", p), nil
	default:
		return WebhookEvent{Kind: "other"}, nil
	}
}

func trimGitLabRef(ref string) string {
	const prefix = "refs/heads/"
	if len(ref) > len(prefix) && ref[:len(prefix)] == prefix {
		return ref[len(prefix):]
	}
	return ref
}

func glWorkItemEvent(kind string, p glPayload) WebhookEvent {
	labels := make([]string, 0, len(p.Labels))
	for _, l := range p.Labels {
		labels = append(labels, l.Title)
	}
	return WebhookEvent{
		Kind:     kind,
		Repo:     p.Project.GitHTTPURL,
		Labels:   labels,
		Title:    p.ObjectAttributes.Title,
		Body:     p.ObjectAttributes.Description,
		IssueRef: fmt.Sprintf("%s!%d", p.Project.PathWithNamespace, p.ObjectAttributes.IID),
		URL:      p.ObjectAttributes.URL,
	}
}
