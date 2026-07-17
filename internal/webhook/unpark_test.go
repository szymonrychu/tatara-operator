package webhook

// W-cachelag: driveCommentUnpark is the acutely broken call site of the
// unpark cache-lag defect. AppendTaskEvent's Status().Update, microseconds
// earlier in the SAME request, is a write straight to the API server; the
// cached informer has not observed it yet. driveCommentUnpark's ApplyUnpark
// call must read through the manager's uncached APIReader for its retry-loop
// Get, and must never decline silently.

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// upStaleGetClient is unpark_test's twin of controller's staleGetClient:
// Get always returns a captured stale snapshot for the watched Task,
// regardless of what the live embedded client holds; every other call
// (Status().Update included) passes straight through.
type upStaleGetClient struct {
	client.Client
	stale *tatarav1.Task
}

func (s *upStaleGetClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if task, ok := obj.(*tatarav1.Task); ok && key.Name == s.stale.Name {
		s.stale.DeepCopyInto(task)
		return nil
	}
	return s.Client.Get(ctx, key, obj, opts...)
}

func upServer(t *testing.T, cachedClient client.Client, apiReader client.Reader, logBuf *bytes.Buffer) *Server {
	t.Helper()
	seq := &queue.SeqSource{Client: cachedClient, Namespace: peNS}
	logger := slog.New(slog.NewJSONHandler(logBuf, nil))
	return NewServer(Config{
		Client:    cachedClient,
		APIReader: apiReader,
		Namespace: peNS,
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Seq:       seq,
		Logger:    logger,
	})
}

func upTask(name, kind, stageReason string) *tatarav1.Task {
	return &tatarav1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: peNS},
		Spec:       tatarav1.TaskSpec{Kind: kind, ProjectRef: "pe-proj", Goal: "g"},
		Status: tatarav1.TaskStatus{
			Stage:       tatarav1.StageParked,
			StageReason: stageReason,
		},
	}
}

// upLogLines parses each JSON line in buf into a msg->fields map, for a
// straightforward "did this log line fire with these fields" assertion.
func upLogLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("unmarshal log line %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

// The core regression, reproduced through the ACTUAL production call site:
// driveCommentUnpark must un-park on the live-fresh non-bot pendingEvent even
// though the cached client's Get would have shown it missing.
func TestDriveCommentUnpark_UsesLiveReadNotCachedGet(t *testing.T) {
	proj := peProject("tatara-bot")
	stale := upTask("t-cachelag", "review", stage.ReasonAwaitingHuman)
	live := stale.DeepCopy()
	live.Status.PendingEvents = []tatarav1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Author: "human", Body: "go ahead",
	}}

	liveClient := peClient(t, proj, live)
	cached := &upStaleGetClient{Client: liveClient, stale: stale.DeepCopy()}
	var logBuf bytes.Buffer
	s := upServer(t, cached, liveClient, &logBuf)

	s.driveCommentUnpark(context.Background(), proj, stale)

	got := getPETask(t, liveClient, stale.Name)
	if got.Status.Stage != tatarav1.StageReviewing {
		t.Fatalf("driveCommentUnpark declined despite a fresh non-bot pendingEvent on the live read; "+
			"stage=%s(%s), want reviewing", got.Status.Stage, got.Status.StageReason)
	}
}

// The decline path (target=="", err==nil) must log at INFO with the task
// name and stageReason - a silent decline here is what hid the cache-lag
// race for a full day with zero errors.
func TestDriveCommentUnpark_DeclineLogsInfo(t *testing.T) {
	proj := peProject("tatara-bot")
	// A bot-only pendingEvent never un-parks: a clean, deterministic decline
	// with no cache-lag divergence needed.
	task := upTask("t-decline", "review", stage.ReasonAwaitingHuman)
	task.Status.PendingEvents = []tatarav1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Author: "tatara-bot", Body: "parked",
	}}
	c := peClient(t, proj, task)
	var logBuf bytes.Buffer
	s := upServer(t, c, c, &logBuf)

	s.driveCommentUnpark(context.Background(), proj, task)

	got := getPETask(t, c, task.Name)
	if got.Status.Stage != tatarav1.StageParked {
		t.Fatalf("bot-only event must not un-park; stage=%s", got.Status.Stage)
	}

	var found map[string]any
	for _, line := range upLogLines(t, &logBuf) {
		if line["msg"] == "pendingEvents: comment-driven unpark declined" {
			found = line
			break
		}
	}
	if found == nil {
		t.Fatalf("no INFO log for the declined un-park; log lines: %s", logBuf.String())
	}
	if found["task"] != task.Name {
		t.Fatalf("decline log missing task=%q: %+v", task.Name, found)
	}
	if found["stage_reason"] != stage.ReasonAwaitingHuman {
		t.Fatalf("decline log missing stage_reason=%q: %+v", stage.ReasonAwaitingHuman, found)
	}
}
