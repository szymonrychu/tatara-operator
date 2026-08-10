package obs

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
)

// ShutdownGate answers one question at log-emission time: has this process's
// stop sequence been engaged?
//
// It holds the signal context rather than a bool a goroutine flips, because the
// lines this gate exists to classify land INSIDE the stop sequence - the
// client-go body-read ERROR in issue #579 was written 545us after "Stopping and
// waiting for leader election runnables". A watcher goroutine that has to be
// scheduled before it can set a flag can lose that race; reading ctx.Err() at
// emission time cannot.
type ShutdownGate struct {
	ctx atomic.Pointer[gateContext]
}

// gateContext exists only because atomic.Pointer needs a concrete type to point
// at and context.Context is an interface.
type gateContext struct{ ctx context.Context }

func NewShutdownGate() *ShutdownGate { return &ShutdownGate{} }

// Arm binds the gate to the process signal context (ctrl.SetupSignalHandler).
func (g *ShutdownGate) Arm(ctx context.Context) {
	if g == nil {
		return
	}
	g.ctx.Store(&gateContext{ctx: ctx})
}

// Engaged reports whether the signal context has been cancelled, i.e. SIGTERM
// has arrived and the manager is stopping. An unarmed or nil gate is never
// engaged, so nothing is ever downgraded by accident.
func (g *ShutdownGate) Engaged() bool {
	if g == nil {
		return false
	}
	c := g.ctx.Load()
	if c == nil || c.ctx == nil {
		return false
	}
	return errors.Is(c.ctx.Err(), context.Canceled)
}

// Log messages owned by dependencies, restated here because matching them is the
// only lever those dependencies offer. Each is locked by a test.
const (
	// sigs.k8s.io/controller-runtime pkg/manager/internal.go:533
	msgStopSequenceEngaged = "error received after stop sequence was engaged"
	// sigs.k8s.io/controller-runtime pkg/manager/internal.go:637
	errLeaderElectionLost = "leader election lost"
)

// downgradeRule decides whether one ERROR record is a known, expected
// consequence of this process shutting down. Rules are evaluated only when the
// gate is engaged, so each may be as narrow as its emission site allows.
type downgradeRule struct {
	name  string
	match func(msg string, err error) bool
}

// shutdownDowngradeRules is the whole policy. Adding a rule means naming the
// upstream emission site it covers; a rule broad enough to catch a genuine
// failure would blind the "Tatara operator error recurring" alert to it, which
// is precisely why #579 rejected the rule-side msg exclusion.
var shutdownDowngradeRules = []downgradeRule{
	{
		// k8s.io/client-go rest/request.go:1196. io.ReadAll(resp.Body) sees the
		// cancelled request context and lands in the default: branch alongside
		// genuine faults; upstream gives the analogous benign case
		// (http2.StreamError) a V(2).Info and caller cancellation nothing.
		// Deliberately not pinned to that one msg: any dependency logging a
		// cancelled in-flight request during our own shutdown is the same class.
		//
		// context.DeadlineExceeded is NOT matched, for the same reason
		// controller.isShutdownCancellation does not match it: a timeout is a
		// real failure whoever's clock it was.
		name:  "cancelled_request",
		match: func(_ string, err error) bool { return errors.Is(err, context.Canceled) },
	},
	{
		// controller-runtime pkg/manager/internal.go:533. OnStoppedLeading pushes
		// a plain errors.New("leader election lost") onto errChan, which
		// LeaderElectionReleaseOnCancel (main.go) makes certain on every graceful
		// shutdown of the leaseholder. It carries no cancellation to match on and
		// the call site already filters context.Canceled, so the pair
		// (msg, err text) is the only available discriminator - pinned to BOTH so
		// any other runnable failure arriving on the same channel, e.g. a webhook
		// Shutdown deadline, keeps its ERROR.
		name: "leader_election_lost",
		match: func(msg string, err error) bool {
			return msg == msgStopSequenceEngaged && err != nil && err.Error() == errLeaderElectionLost
		},
	},
}

// shutdownDowngradeHandler re-levels ERROR to INFO for records matched by
// shutdownDowngradeRules while the gate is engaged.
//
// This sits at the handler, not at the Reconcile boundary, because the records
// in question are written by dependencies from inside a reconcile - the boundary
// classifier in internal/controller/labels.go is structurally incapable of
// seeing them (#579). Note that logr's slog sink calls Handle with
// context.Background() (logr@v1.4.3 slogsink.go:94), so the reconcile context is
// NOT available here; the gate supplies the shutdown half of the predicate that
// isShutdownCancellation reads off ctx.Err(), and the record supplies the error
// half.
type shutdownDowngradeHandler struct {
	inner slog.Handler
	gate  *ShutdownGate
}

func (h *shutdownDowngradeHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.inner.Enabled(ctx, l)
}

func (h *shutdownDowngradeHandler) Handle(ctx context.Context, r slog.Record) error {
	if r.Level < slog.LevelError || !h.gate.Engaged() {
		return h.inner.Handle(ctx, r)
	}
	rule := matchShutdownRule(r)
	if rule == "" {
		return h.inner.Handle(ctx, r)
	}
	// The record was admitted at ERROR; at LOG_LEVEL=warn or above the INFO
	// replacement is below threshold, and slog handlers do not re-check level in
	// Handle. Dropping it is the consistent reading: the line is being reclassified
	// as a lifecycle event, and an operator who asked for warn-and-above did not
	// ask for lifecycle events. The chart default is info, where nothing is lost.
	if !h.inner.Enabled(ctx, slog.LevelInfo) {
		return nil
	}
	out := slog.NewRecord(r.Time, slog.LevelInfo, r.Message, r.PC)
	r.Attrs(func(a slog.Attr) bool {
		out.AddAttrs(a)
		return true
	})
	// The class stays countable at the configured level rather than vanishing: a
	// rollout that silently drops these lines is indistinguishable from one that
	// never had them, and the next person debugging a real drain needs to see
	// them.
	//
	// The marker keys are namespaced ON PURPOSE. slog's JSON handler does not
	// dedupe keys and Loki's `| json` is last-wins, so writing the marker to
	// "action" - which hard rule 12 puts on nearly every line this operator emits
	// - would make a downgraded line unfindable by its own action. Nothing else
	// in this repo writes "shutdown_downgrade" or "original_level".
	out.AddAttrs(
		slog.String("shutdown_downgrade", rule),
		slog.String("original_level", r.Level.String()),
	)
	return h.inner.Handle(ctx, out)
}

func (h *shutdownDowngradeHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &shutdownDowngradeHandler{inner: h.inner.WithAttrs(attrs), gate: h.gate}
}

// WithGroup would nest the marker attrs inside the open group, so a downgraded
// line would carry <group>_shutdown_downgrade in Loki rather than
// shutdown_downgrade. No caller reaches this: logr's slog sink only ever calls
// WithAttrs/WithName (logr@v1.4.3 slogsink.go), and no operator code groups its
// own attrs. Left as a straight delegation rather than a guard, but named here
// so the first caller to add a group knows what it changes.
func (h *shutdownDowngradeHandler) WithGroup(name string) slog.Handler {
	return &shutdownDowngradeHandler{inner: h.inner.WithGroup(name), gate: h.gate}
}

// matchShutdownRule returns the name of the first rule matching the record, or
// "" if none does.
func matchShutdownRule(r slog.Record) string {
	err := recordError(r)
	for _, rule := range shutdownDowngradeRules {
		if rule.match(r.Message, err) {
			return rule.name
		}
	}
	return ""
}

// recordError returns the first attribute value that is an error. logr's slog
// sink puts the live error object on the record as slog.Any("err", err), so the
// error is still an error here and errors.Is works on it - the JSON text does
// not exist yet. Only top-level attrs are scanned: no emitter on this path nests
// its error in a slog.Group.
func recordError(r slog.Record) error {
	var found error
	r.Attrs(func(a slog.Attr) bool {
		if err, ok := a.Value.Resolve().Any().(error); ok {
			found = err
			return false
		}
		return true
	})
	return found
}
