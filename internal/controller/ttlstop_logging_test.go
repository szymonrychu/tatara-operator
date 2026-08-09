package controller

import (
	"context"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/szymonrychu/tatara-operator/internal/agent"
)

// A STOP THAT CAPTURED NOTHING IS NOT AN INFO EVENT.
//
// Every G.7 stop logged "agent pod TTL-stopped; handed off" at INFO, on every
// path, including the one where nothing whatsoever was handed off. The failure
// mode therefore had no log signature at all: the #527 investigation grepped
// 299395 lines of operator stdout and found exactly two agent_pod_ttl_stop
// entries, both of them claiming success while their Tasks' entire notes journal
// was the placeholder note. An operator reading Loki had no way to see the loss.
func TestLogTTLStop_LevelAndWordingFollowTheHandoffDimension(t *testing.T) {
	capture := func(res agent.TTLStopResult) (line string, errored bool) {
		sink := funcr.NewJSON(func(obj string) { line = obj }, funcr.Options{})
		l := logr.New(&levelSpySink{LogSink: sink.GetSink(), onError: func() { errored = true }})
		ctx := ctrllog.IntoContext(context.Background(), l)
		logTTLStop(ctx, "handed off", "NO continuation state captured", "agent_pod_ttl_stop", res,
			"resource_id", "task-1")
		return line, errored
	}

	for _, tc := range []struct {
		name    string
		res     agent.TTLStopResult
		wantErr bool
		wantMsg string
	}{
		{
			name:    "agent handoff",
			res:     agent.TTLStopResult{Outcome: agent.TTLOutcomeGraceful, Handoff: agent.TTLHandoffAgent},
			wantMsg: "handed off",
		},
		{
			name:    "synthetic handoff",
			res:     agent.TTLStopResult{Outcome: agent.TTLOutcomeGraceful, Handoff: agent.TTLHandoffSynthetic},
			wantMsg: "handed off",
		},
		{
			// The bucket the alert fires on. A force-delete is NOT what makes this an
			// error - a graceful stop that captured nothing is the same loss.
			name:    "nothing captured",
			res:     agent.TTLStopResult{Outcome: agent.TTLOutcomeGraceful, Handoff: agent.TTLHandoffNone},
			wantErr: true,
			wantMsg: "NO continuation state captured",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			line, errored := capture(tc.res)
			if errored != tc.wantErr {
				t.Errorf("logged as error = %v, want %v: %s", errored, tc.wantErr, line)
			}
			if !strings.Contains(line, tc.wantMsg) {
				t.Errorf("message = %s, want it to contain %q", line, tc.wantMsg)
			}
			for _, want := range []string{`"handoff":"` + tc.res.Handoff, `"outcome":"` + tc.res.Outcome} {
				if !strings.Contains(line, want) {
					t.Errorf("line = %s, want it to carry %s", line, want)
				}
			}
		})
	}
}

// levelSpySink records whether Error was used, which is the only thing a funcr
// JSON sink does not put in the rendered line.
type levelSpySink struct {
	logr.LogSink
	onError func()
}

func (s *levelSpySink) WithValues(kv ...any) logr.LogSink {
	return &levelSpySink{LogSink: s.LogSink.WithValues(kv...), onError: s.onError}
}

func (s *levelSpySink) WithName(name string) logr.LogSink {
	return &levelSpySink{LogSink: s.LogSink.WithName(name), onError: s.onError}
}

func (s *levelSpySink) Error(err error, msg string, kv ...any) {
	s.onError()
	s.LogSink.Error(err, msg, kv...)
}
