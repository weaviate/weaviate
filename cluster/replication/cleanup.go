//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package replication

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sirupsen/logrus"

	clusterTypes "github.com/weaviate/weaviate/cluster/types"
)

const (
	cleanupBatchSize         = 1000 // op ids per RAFT command
	cleanupMaxBatchesPerTick = 10   // ⇒ at most 10k ops removed per tick
	cleanupLogAction         = "replication_cleanup"

	// Re-check period used when the interval reads 0, the disable sentinel, so a
	// sweep re-enabled at runtime is picked up without a restart.
	defaultCleanupInterval = time.Hour
)

// StaleOpRemover is the RAFT surface the cleaner needs. *cluster.Raft satisfies it.
type StaleOpRemover interface {
	ForceDeleteReplicationsByIds(ctx context.Context, ids []uint64) error
}

// OpCleanerParams are the dependencies of an OpCleaner. Every field is
// required; NewOpCleaner rejects a nil one.
type OpCleanerParams struct {
	Logger     *logrus.Logger
	NodeID     string
	FSM        *ShardReplicationFSM
	Remover    StaleOpRemover
	Clock      clockwork.Clock
	Registerer prometheus.Registerer

	// ReadyToSweep is the whole-tick gate: IsLeader() && Ready() && FSMHasCaughtUp().
	ReadyToSweep func() bool
	// IsLeader is re-checked before every chunk, so a leader demoted mid-tick
	// stops at the next chunk boundary.
	IsLeader func() bool

	Enabled          func() bool
	MaxAge           func() time.Duration
	Interval         func() time.Duration
	IncludeCancelled func() bool
	Jitter           func(time.Duration) time.Duration
}

// OpCleaner periodically removes old terminal replication ops from the FSM via
// a deterministic RAFT command. Its loop runs on every node; a tick on a
// follower returns immediately, so leadership needs no lifecycle choreography.
type OpCleaner struct {
	logger *logrus.Entry
	p      OpCleanerParams

	deleted    *prometheus.CounterVec
	failures   prometheus.Counter
	ineligible *prometheus.GaugeVec
}

// NewOpCleaner returns an error if any dependency or config getter is nil. A
// nil getter would make Get() return the zero value and silently disable the
// sweep, so callers must substitute an explicit fallback instead.
func NewOpCleaner(p OpCleanerParams) (*OpCleaner, error) {
	switch {
	case p.Logger == nil:
		return nil, errors.New("replication cleanup: Logger is required")
	case p.FSM == nil:
		return nil, errors.New("replication cleanup: FSM is required")
	case p.Remover == nil:
		return nil, errors.New("replication cleanup: Remover is required")
	case p.Clock == nil:
		return nil, errors.New("replication cleanup: Clock is required")
	case p.Registerer == nil:
		return nil, errors.New("replication cleanup: Registerer is required")
	case p.ReadyToSweep == nil:
		return nil, errors.New("replication cleanup: ReadyToSweep is required")
	case p.IsLeader == nil:
		return nil, errors.New("replication cleanup: IsLeader is required")
	case p.Enabled == nil:
		return nil, errors.New("replication cleanup: Enabled getter is required")
	case p.MaxAge == nil:
		return nil, errors.New("replication cleanup: MaxAge getter is required")
	case p.Interval == nil:
		return nil, errors.New("replication cleanup: Interval getter is required")
	case p.IncludeCancelled == nil:
		return nil, errors.New("replication cleanup: IncludeCancelled getter is required")
	case p.Jitter == nil:
		return nil, errors.New("replication cleanup: Jitter is required")
	}

	auto := promauto.With(p.Registerer)
	return &OpCleaner{
		logger: p.Logger.WithFields(logrus.Fields{
			"component": cleanupLogAction,
			"action":    cleanupLogAction,
			"node":      p.NodeID,
		}),
		p: p,
		deleted: auto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "weaviate",
			Name:      "replication_operation_cleanup_deleted_total",
			Help:      "Total number of stale replication operations removed by the cleanup sweep",
		}, []string{"state"}),
		failures: auto.NewCounter(prometheus.CounterOpts{
			Namespace: "weaviate",
			Name:      "replication_operation_cleanup_failures_total",
			Help:      "Total number of failed replication cleanup batches. A lost election is not a failure",
		}),
		ineligible: auto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "weaviate",
			Name:      "replication_operation_cleanup_ineligible",
			Help:      "Replication operations that are age-eligible for cleanup but excluded. Only refreshed by the current leader",
		}, []string{"reason"}),
	}, nil
}

// Run loops until ctx is cancelled. It runs on every node: leadership is
// enforced inside Tick, so nothing starts or stops on election or demotion.
func (c *OpCleaner) Run(ctx context.Context) error {
	// Jittered first wait so a cluster restarted together does not sweep in lockstep.
	wait := c.p.Jitter(c.currentInterval())
	for {
		// A stoppable timer rather than Clock.After: an After timer stays armed
		// until it fires, so a cancelled loop would leave one behind per cycle.
		timer := c.p.Clock.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.Chan():
		}

		interval := c.p.Interval()
		if interval <= 0 {
			// Say so rather than silently re-checking every hour. A disabled
			// sweep is not misconfigured, so it stays quiet.
			if c.p.Enabled() {
				c.logger.Warnf("replication cleanup is enabled but REPLICA_MOVEMENT_CLEANUP_INTERVAL is %s; "+
					"skipping the sweep and re-checking in %s", interval, defaultCleanupInterval)
			}
			wait = defaultCleanupInterval
			continue
		}
		wait = interval

		// Tick logs its own failures, and the sweep is idempotent, so a failed
		// tick changes nothing about the next one.
		_, _ = c.Tick(ctx)
	}
}

// currentInterval applies the built-in fallback for the first wait only. Run
// re-reads Interval() on every cycle, which is the hot-reload mechanism.
func (c *OpCleaner) currentInterval() time.Duration {
	if interval := c.p.Interval(); interval > 0 {
		return interval
	}
	return defaultCleanupInterval
}

// Tick performs one sweep pass and returns the number of ops removed. On a
// follower, or when the sweep is disabled, it does nothing and returns (0, nil).
func (c *OpCleaner) Tick(ctx context.Context) (int, error) {
	if !c.p.Enabled() || !c.p.ReadyToSweep() {
		return 0, nil
	}

	maxAge := c.p.MaxAge()
	if maxAge <= 0 {
		// A zero max-age must never be read as "delete every terminal op". The
		// test is <= 0 because the cutoff arithmetic below must never run on a
		// negative age, whatever its source.
		c.logger.Warnf("replication cleanup is enabled but REPLICA_MOVEMENT_CLEANUP_MAX_AGE is %s; skipping the sweep", maxAge)
		return 0, nil
	}

	cutoff := c.p.Clock.Now().Add(-maxAge).UnixMilli()

	// One selection scan per tick. The budget is per tick, not per state:
	// include-cancelled widens the predicate but must not raise the number of
	// ops removed or RAFT commands issued.
	stale, flaggedSkipped := c.p.FSM.SelectStaleOps(cutoff, c.p.IncludeCancelled(), cleanupBatchSize*cleanupMaxBatchesPerTick)
	c.ineligible.WithLabelValues("flagged").Set(float64(flaggedSkipped))

	removed := 0
	var tickErr error
	// Never re-select between chunks. Execute returns once the leader has
	// applied, but this node's own applied index can still lag, so a re-select
	// could hand back ids already removed.
	for start := 0; start < len(stale); start += cleanupBatchSize {
		if ctx.Err() != nil {
			break
		}
		if !c.p.IsLeader() {
			// Demoted mid-tick. The new leader picks the remaining batches up on
			// its own tick, so do not forward them.
			c.logger.Debug("stopping replication cleanup tick, no longer leader")
			break
		}

		chunk := stale[start:min(start+cleanupBatchSize, len(stale))]
		ids := make([]uint64, 0, len(chunk))
		for _, op := range chunk {
			ids = append(ids, op.ID)
		}

		if err := c.p.Remover.ForceDeleteReplicationsByIds(ctx, ids); err != nil {
			switch {
			case ctx.Err() != nil:
				c.logger.Debugf("replication cleanup batch aborted during shutdown: %v", err)
			case clusterTypes.IsNoLeader(err):
				// A lost election is a deferral, not a failure: the new leader
				// sweeps on its next tick.
				c.logger.Debugf("replication cleanup batch deferred, leadership moved: %v", err)
			default:
				c.failures.Inc()
				c.logger.Errorf("could not remove %d stale replication operations: %v", len(ids), err)
				tickErr = fmt.Errorf("remove stale replication operations: %w", err)
			}
			break
		}

		for _, op := range chunk {
			c.deleted.WithLabelValues(op.State.String()).Inc()
		}
		removed += len(chunk)
	}

	if removed > 0 || flaggedSkipped > 0 {
		c.logger.Infof("removed %d stale replication operations older than %s (%d age-eligible but flagged)",
			removed, maxAge, flaggedSkipped)
	} else {
		c.logger.Debugf("removed no stale replication operations older than %s", maxAge)
	}

	return removed, tickErr
}
