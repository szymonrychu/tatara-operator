package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

// CallbackServer handles the in-cluster /internal/turn-complete endpoint the
// wrapper POSTs to on each turn, and runs the poll backstop for missed
// callbacks. It has no OIDC: INTERNAL_ADDR is not exposed via ingress.
type CallbackServer struct {
	Client    client.Client
	Metrics   *obs.OperatorMetrics
	Session   agent.Session
	Namespace string
}

type turnCompletePayload struct {
	TurnID          string  `json:"turnId"`
	State           string  `json:"state"`
	FinalText       string  `json:"finalText"`
	StopReason      string  `json:"stopReason"`
	Error           string  `json:"error"`
	DurationSeconds float64 `json:"durationSeconds"`
}

// Handler returns the http.Handler for POST /internal/turn-complete.
func (s *CallbackServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/turn-complete", s.handleTurnComplete)
	return mux
}

func (s *CallbackServer) handleTurnComplete(w http.ResponseWriter, r *http.Request) {
	l := log.FromContext(r.Context())
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var p turnCompletePayload
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	if p.TurnID == "" {
		http.Error(w, "turnId is required", http.StatusBadRequest)
		return
	}
	s.Metrics.ObserveTurnDuration(p.DurationSeconds)

	if err := s.recordResult(r.Context(), agent.TurnResult{
		State: p.State, FinalText: p.FinalText, StopReason: p.StopReason, Err: p.Error,
	}, p.TurnID); err != nil {
		if errors.Is(err, errTurnNotFound) {
			http.Error(w, "unknown turn", http.StatusNotFound)
			return
		}
		l.Error(err, "record turn result", "turn_id", p.TurnID)
		http.Error(w, "record failed", http.StatusInternalServerError)
		return
	}
	l.Info("recorded turn result", "action", "turn_complete", "turn_id", p.TurnID, "state", p.State)
	w.WriteHeader(http.StatusNoContent)
}

var errTurnNotFound = errors.New("no task with that current turn")

// recordResult writes finalText onto the executing Subtask (if any) and bumps
// the Task's turn-complete annotation to requeue its reconcile.
// Both the Subtask status write and the Task annotation update are wrapped in
// RetryOnConflict to handle concurrent reconcile updates.
func (s *CallbackServer) recordResult(ctx context.Context, tr agent.TurnResult, turnID string) error {
	task, err := s.resolveTaskByTurn(ctx, turnID)
	if err != nil {
		return err
	}

	// Write subtask result on the status subresource; retry on conflict.
	if sub := task.Annotations[annCurrentSubtask]; sub != "" {
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			st := &tatarav1alpha1.Subtask{}
			if err := s.Client.Get(ctx, types.NamespacedName{Namespace: s.Namespace, Name: sub}, st); err != nil {
				if apierrors.IsNotFound(err) {
					return nil
				}
				return fmt.Errorf("get executing subtask: %w", err)
			}
			st.Status.Result = tr.FinalText
			return s.Client.Status().Update(ctx, st)
		}); err != nil {
			return fmt.Errorf("write subtask result: %w", err)
		}
	}

	// Stamp turn-complete on the Task annotation; retry on conflict with a fresh Get.
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := s.Client.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, fresh); err != nil {
			return fmt.Errorf("reload task: %w", err)
		}
		if fresh.Annotations == nil {
			fresh.Annotations = map[string]string{}
		}
		fresh.Annotations[annTurnComplete] = time.Now().UTC().Format(time.RFC3339)
		if err := s.Client.Update(ctx, fresh); err != nil {
			return err
		}
		return nil
	})
}

// resolveTaskByTurn finds the Task whose current-turn annotation matches turnID.
// Tasks with an empty annCurrentTurn are skipped to prevent empty-to-empty matches.
func (s *CallbackServer) resolveTaskByTurn(ctx context.Context, turnID string) (*tatarav1alpha1.Task, error) {
	var list tatarav1alpha1.TaskList
	if err := s.Client.List(ctx, &list, client.InNamespace(s.Namespace)); err != nil {
		return nil, fmt.Errorf("list tasks: %w", err)
	}
	for i := range list.Items {
		ann := list.Items[i].Annotations[annCurrentTurn]
		if ann == "" {
			continue
		}
		if ann == turnID {
			return &list.Items[i], nil
		}
	}
	return nil, errTurnNotFound
}

// PollOnce polls in-flight turns for delivered results that missed a callback.
// It is the backstop body; the ticker loop calls it. It also expires turns that
// have exceeded their deadline so a wedged turn does not requeue forever.
func (s *CallbackServer) PollOnce(ctx context.Context) {
	l := log.FromContext(ctx)
	var list tatarav1alpha1.TaskList
	if err := s.Client.List(ctx, &list, client.InNamespace(s.Namespace)); err != nil {
		return
	}
	for i := range list.Items {
		task := &list.Items[i]
		turn := task.Annotations[annCurrentTurn]
		if turn == "" || isTerminal(task.Status.Phase) || task.Annotations[annTurnComplete] != "" {
			continue
		}

		// Check for turn timeout before hitting the wrapper.
		if s.isTurnTimedOut(ctx, task) {
			l.Info("turn timed out in poll backstop", "action", "turn_timeout",
				"task", task.Name, "turn_id", turn)
			_ = s.expireTimedOutTurn(ctx, task, turn)
			continue
		}

		if s.Session == nil {
			continue
		}
		tr, err := s.Session.GetTurn(ctx, agent.BaseURL(task, s.Namespace), turn)
		if err != nil {
			continue
		}
		if tr.State == "completed" || tr.State == "failed" {
			_ = s.recordResult(ctx, tr, turn)
		}
	}
}

// isTurnTimedOut checks the turn-started-at annotation against the project
// turnTimeoutSeconds + grace. Returns false when any lookup fails (safe default).
func (s *CallbackServer) isTurnTimedOut(ctx context.Context, task *tatarav1alpha1.Task) bool {
	raw := task.Annotations[annTurnStartedAt]
	if raw == "" {
		return false
	}
	startedAt, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return false
	}
	var project tatarav1alpha1.Project
	if err := s.Client.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Spec.ProjectRef}, &project); err != nil {
		return false
	}
	timeout := project.Spec.Agent.TurnTimeoutSeconds
	if timeout <= 0 {
		timeout = 1800
	}
	deadline := startedAt.Add(time.Duration(timeout)*time.Second + turnTimeoutGrace)
	return time.Now().After(deadline)
}

// expireTimedOutTurn performs the terminal cleanup for a timed-out turn:
// deletes the session + Pod/Service and sets Task phase=Failed/TurnTimeout.
func (s *CallbackServer) expireTimedOutTurn(ctx context.Context, task *tatarav1alpha1.Task, turn string) error {
	if s.Session != nil {
		_ = s.Session.DeleteSession(ctx, agent.BaseURL(task, s.Namespace))
	}
	// Delete Pod and Service best-effort (owner-references ensure GC too).
	p := &corev1.Pod{}
	p.Name = agent.PodName(task)
	p.Namespace = task.Namespace
	_ = s.Client.Delete(ctx, p)
	svc := &corev1.Service{}
	svc.Name = agent.PodName(task)
	svc.Namespace = task.Namespace
	_ = s.Client.Delete(ctx, svc)

	fresh := &tatarav1alpha1.Task{}
	if err := s.Client.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, fresh); err != nil {
		return err
	}
	fresh.Status.Phase = "Failed"
	apimeta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		Reason:             "TurnTimeout",
		Message:            fmt.Sprintf("turn %s exceeded timeout", turn),
		ObservedGeneration: fresh.Generation,
	})
	return s.Client.Status().Update(ctx, fresh)
}

// Start runs the callback HTTP server and the poll backstop until ctx is done.
// It implements sigs.k8s.io/controller-runtime/pkg/manager.Runnable.
func (s *CallbackServer) Start(ctx context.Context, addr string) error {
	srv := &http.Server{Addr: addr, Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		t := time.NewTicker(pollRequeue)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if s.Session != nil {
					s.PollOnce(ctx)
				}
			}
		}
	}()
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("callback server: %w", err)
	}
	return nil
}
