package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
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
func (s *CallbackServer) recordResult(ctx context.Context, tr agent.TurnResult, turnID string) error {
	task, err := s.resolveTaskByTurn(ctx, turnID)
	if err != nil {
		return err
	}
	if sub := task.Annotations[annCurrentSubtask]; sub != "" {
		st := &tatarav1alpha1.Subtask{}
		if err := s.Client.Get(ctx, types.NamespacedName{Namespace: s.Namespace, Name: sub}, st); err == nil {
			st.Status.Result = tr.FinalText
			if err := s.Client.Status().Update(ctx, st); err != nil {
				return fmt.Errorf("write subtask result: %w", err)
			}
		} else if !apierrors.IsNotFound(err) {
			return fmt.Errorf("get executing subtask: %w", err)
		}
	}
	if task.Annotations == nil {
		task.Annotations = map[string]string{}
	}
	task.Annotations[annTurnComplete] = time.Now().UTC().Format(time.RFC3339)
	if err := s.Client.Update(ctx, task); err != nil {
		return fmt.Errorf("requeue task: %w", err)
	}
	return nil
}

// resolveTaskByTurn finds the Task whose current-turn annotation matches turnID.
func (s *CallbackServer) resolveTaskByTurn(ctx context.Context, turnID string) (*tatarav1alpha1.Task, error) {
	var list tatarav1alpha1.TaskList
	if err := s.Client.List(ctx, &list, client.InNamespace(s.Namespace)); err != nil {
		return nil, fmt.Errorf("list tasks: %w", err)
	}
	for i := range list.Items {
		if list.Items[i].Annotations[annCurrentTurn] == turnID {
			return &list.Items[i], nil
		}
	}
	return nil, errTurnNotFound
}

// PollOnce polls in-flight turns for delivered results that missed a callback.
// It is the backstop body; the ticker loop calls it.
func (s *CallbackServer) PollOnce(ctx context.Context) {
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
		tr, err := s.Session.GetTurn(ctx, agent.BaseURL(task, s.Namespace), turn)
		if err != nil {
			continue
		}
		if tr.State == "completed" || tr.State == "failed" {
			_ = s.recordResult(ctx, tr, turn)
		}
	}
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
