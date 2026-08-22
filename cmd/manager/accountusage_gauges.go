package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/accountusage"
	"github.com/szymonrychu/tatara-operator/internal/budget"
	"github.com/szymonrychu/tatara-operator/internal/controller"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

// accountUsageGaugeInterval is the gauge cadence. The store is read in memory,
// so this costs nothing and keeps the series alive between Task events.
const accountUsageGaugeInterval = time.Minute

// accountUsageGaugeRunnable is the SOLE producer of the tatara_account_usage_*
// gauges for the wrapper feed, and it is leader-only for exactly that reason.
// The poller already documents itself as the sole producer for its own path
// (internal/accountusage/poller.go); this widens "the producer" to "the
// leader's resolved fleet state" without ever adding a second one.
//
// It must NOT be folded into the per-replica turn-complete handler: three
// processes racing to Set the same gauge with individually-fresh values give an
// arbitrary, flapping series depending on which pod Prometheus happens to
// scrape. The same argument is why the store write itself is leader-only.
//
// It also SEEDS the store once, before its first tick. A leader restart or
// re-election otherwise leaves the in-process store empty until the next turn
// completes - potentially minutes during which the gate reads 0% and fully
// opens. The seed is a single namespace-wide List through the CACHED client, so
// it is an in-memory read rather than an API call, and it runs once.
type accountUsageGaugeRunnable struct {
	reader    client.Reader
	store     *accountusage.Store
	metrics   *obs.OperatorMetrics
	namespace string
	interval  time.Duration
	// defaults is the operator-wide budget config. Only two things are read
	// from it: whether the claudeSubscription gate is on at all, and
	// MaxSnapshotAge. Per-Project overrides are deliberately NOT consulted -
	// the account is shared fleet-wide, so gate readiness is one fleet-wide
	// fact, not a per-Project one.
	defaults budget.Config
}

// NeedLeaderElection implements manager.LeaderElectionRunnable. TRUE: see the
// single-producer discipline in the type comment.
func (a accountUsageGaugeRunnable) NeedLeaderElection() bool { return true }

func (a accountUsageGaugeRunnable) Start(ctx context.Context) error {
	// A failed seed is not worth tearing the manager down for: the next
	// turn-complete feeds the store anyway, and returning an error from Start
	// stops the whole control plane.
	if err := seedStoreFromTasks(ctx, a.reader, a.store, a.namespace); err != nil {
		slog.Warn("account usage seed failed", "action", "account_usage_seed", "error", err)
	}
	interval := a.interval
	if interval <= 0 {
		interval = accountUsageGaugeInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	a.emit()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			a.emit()
		}
	}
}

// emit publishes the resolved fleet state. It reads the in-process store only -
// no List, no API call - so it is safe at a 60s cadence and the series stay
// alive even when no new Task event arrives.
//
// An EMPTY store publishes NO USAGE series. A 0 percent is indistinguishable
// from a genuinely idle account, and publishing it would hide an inert gate
// behind a plausible-looking series, which is exactly how the gate sat at 0%
// for weeks unnoticed.
//
// tatara_account_usage_gate_ready is the exception and is emitted BEFORE that
// early return: an enabled gate holding no snapshot is precisely the outage
// condition, so it has to render 0 rather than nothing. It is emitted only when
// the claudeSubscription gate is actually on, so an operator that never enabled
// it creates no series and TataraAccountUsageFeedDead cannot fire on it.
func (a accountUsageGaugeRunnable) emit() {
	if a.metrics == nil || a.store == nil {
		return
	}
	snap := a.store.Get()
	now := time.Now()
	gateOn := a.defaults.Enabled && a.defaults.Mode == budget.ModeClaudeSubscription
	if gateOn {
		// SnapshotFresh treats a ZERO observedAt as fresh (unknown means the
		// poller's path, which must not be disabled by a staleness bound), so
		// the empty store is excluded explicitly here rather than relying on it.
		a.metrics.SetAccountUsageGateReady(
			!snap.UpdatedAt.IsZero() && budget.SnapshotFresh(a.defaults, snap.UpdatedAt, now))
	}
	if snap.UpdatedAt.IsZero() {
		return
	}
	a.metrics.SetAccountUsageSnapshotAge(snap.Source, now.Sub(snap.UpdatedAt).Seconds())
	a.metrics.SetAccountUsage("five_hour", snap.FiveHour.Percent)
	a.metrics.SetAccountUsage("seven_day", snap.Weekly.Percent)
	if !snap.FiveHour.Reset.IsZero() {
		a.metrics.SetAccountUsageReset("five_hour", float64(snap.FiveHour.Reset.Unix()))
	}
	if !snap.Weekly.Reset.IsZero() {
		a.metrics.SetAccountUsageReset("seven_day", float64(snap.Weekly.Reset.Unix()))
	}
}

// seedStoreFromTasks folds the newest status.accountUsage across every Task in
// the namespace into the store. One List, once, at leader start.
//
// It is FLEET-WIDE, not per-Project, and deliberately so: the Claude
// subscription is one account shared by every agent pod, so the correct value
// is the newest observation anywhere, not the newest within some Project. The
// retired #225 design keyed it per-Project (dd40ee4) and went stale for any
// Project that fell quiet while its neighbours burned the same shared windows.
func seedStoreFromTasks(ctx context.Context, reader client.Reader, store *accountusage.Store, namespace string) error {
	if store == nil {
		return nil
	}
	var tl tatarav1alpha1.TaskList
	if err := reader.List(ctx, &tl, client.InNamespace(namespace)); err != nil {
		return fmt.Errorf("accountusage: seed list tasks: %w", err)
	}
	for i := range tl.Items {
		au := tl.Items[i].Status.AccountUsage
		if au == nil || au.ObservedAt.IsZero() {
			continue
		}
		store.SetIfNewer(controller.SnapshotFromTaskAccountUsage(au))
	}
	return nil
}
