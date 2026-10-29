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

package suspendsweep

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/proto/api"
	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// DB is the part of *db.DB that Sweep calls.
type DB interface {
	LocalIndexClassNames() ([]string, error)
	NamespaceStateForClass(className string) (api.NamespaceState, error)
	OpenLocalShardNames(collection string) ([]string, error)
	UnloadShard(ctx context.Context, className, shardName string) (db.ShardUnloadOutcome, error)
}

// outcomeUnloading says an unload is still running. Sweep records it for a
// progress report, and DB.UnloadShard never returns it.
const outcomeUnloading db.ShardUnloadOutcome = "unloading"

type shardKey struct{ className, shardName string }

// shardOutcomes holds one class's shard names by their latest unload outcome.
type shardOutcomes map[db.ShardUnloadOutcome]map[string]struct{}

// progressInterval is how often a batch of unloads reports while it runs.
const progressInterval = 30 * time.Second

// Sweep unloads the local shards of suspended namespaces. Its state outlives a
// tick, so slots caps the running unloads across classes and ticks.
type Sweep struct {
	logger *logrus.Entry
	db     DB
	slots  *semaphore.Weighted // one slot per running unload

	// mu is never held across a DB call or slots.Acquire.
	mu          sync.Mutex
	inFlight    map[shardKey]struct{}    // guarded by mu
	outcomes    map[string]shardOutcomes // guarded by mu, keyed by class name
	dispatching bool                     // guarded by mu, true while Run dispatches
	// stopProgress is guarded by mu. It is non-nil while a batch runs, and
	// closing it stops the batch's progress reports.
	stopProgress chan struct{}
}

// New returns a sweep over repo that runs at most limit unloads at once.
func New(logger *logrus.Entry, repo DB, limit int) *Sweep {
	return &Sweep{
		logger:   logger,
		db:       repo,
		slots:    semaphore.NewWeighted(int64(limit)),
		inFlight: map[shardKey]struct{}{},
		outcomes: map[string]shardOutcomes{},
	}
}

// Run is one tick. It waits for a slot as long as it takes, so it returns once
// every listed shard is dispatched or ctx ends. The unloads it started keep
// running on ctx. Calls must not overlap, and the suspend reconcile cron job
// never overlaps them.
func (s *Sweep) Run(ctx context.Context) {
	s.startDispatch(ctx)
	defer s.endDispatch(ctx)

	classNames, err := s.db.LocalIndexClassNames()
	if err != nil {
		s.logger.Logf(accessorErrLevel(err), "cannot list local classes, ending the sweep: %v", err)
		return
	}
	for _, className := range classNames {
		if ctx.Err() != nil {
			return
		}
		s.reconcileClass(ctx, className)
	}
}

// startDispatch starts a batch and its progress reports, or joins the running
// batch of an earlier tick. A batch ends once its last unload returns with no
// tick dispatching.
func (s *Sweep) startDispatch(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dispatching = true
	if s.stopProgress != nil {
		return
	}
	stop := make(chan struct{})
	s.stopProgress = stop
	enterrors.GoWrapper(func() { s.reportProgressUntil(ctx, stop) }, s.logger)
}

// endDispatch ends the batch if no unload is running.
func (s *Sweep) endDispatch(ctx context.Context) {
	s.reportBatch(ctx, s.stopDispatching())
}

// stopDispatching clears dispatching and returns what endBatchLocked returns.
func (s *Sweep) stopDispatching() map[string]shardOutcomes {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dispatching = false
	return s.endBatchLocked()
}

// reconcileClass dispatches an unload for each local shard of a suspended class
// that is not in flight.
func (s *Sweep) reconcileClass(ctx context.Context, className string) {
	logger := s.logger.WithField("class", className)
	stillSuspended := func() bool {
		_, suspended, err := s.classSuspended(className)
		if err != nil {
			logger.Logf(accessorErrLevel(err),
				"cannot read the namespace state, leaving the class's shards for the next sweep: %v", err)
		}
		return suspended
	}
	if !stillSuspended() {
		return
	}

	names, err := s.db.OpenLocalShardNames(className)
	if err != nil {
		logger.Logf(accessorErrLevel(err), "cannot list local shards, leaving them for the next sweep: %v", err)
		return
	}
	for _, shardName := range s.withoutInFlight(className, names) {
		if err := s.slots.Acquire(ctx, 1); err != nil {
			return
		}
		// A cancel can free a slot before Acquire sees it.
		if ctx.Err() != nil || !stillSuspended() {
			s.slots.Release(1)
			return
		}
		s.markInFlight(className, shardName)
		enterrors.GoWrapper(func() { s.unload(ctx, className, shardName) }, s.logger)
	}
}

// withoutInFlight drops from names the shards whose unload is in flight.
func (s *Sweep) withoutInFlight(className string, names []string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.DeleteFunc(names, func(shardName string) bool {
		_, inFlight := s.inFlight[shardKey{className, shardName}]
		return inFlight
	})
}

func (s *Sweep) markInFlight(className, shardName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inFlight[shardKey{className, shardName}] = struct{}{}
}

func (s *Sweep) reportProgressUntil(ctx context.Context, stop chan struct{}) {
	ticker := time.NewTicker(progressInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.reportProgress(ctx, stop)
		case <-stop:
			return
		case <-ctx.Done():
			return
		}
	}
}

// endBatchLocked ends the batch once no tick dispatches and no unload runs. It
// stops the progress reports and returns the outcomes for the last report, or
// nil while the batch runs. The caller holds mu.
func (s *Sweep) endBatchLocked() map[string]shardOutcomes {
	if s.dispatching || len(s.inFlight) > 0 {
		return nil
	}
	close(s.stopProgress)
	s.stopProgress = nil
	return s.takeOutcomesLocked()
}

// unload closes the shard unless its class no longer reads suspended. It reads
// the state after reconcileClass has added the shard to inFlight, so once a
// resume has applied, a false UnloadInFlight means this unload closes nothing.
func (s *Sweep) unload(ctx context.Context, className, shardName string) {
	logger := s.logger.WithFields(logrus.Fields{"class": className, "shard": shardName})
	var record db.ShardUnloadOutcome // stays empty on the exits that record nothing
	defer func() {
		outcomes := s.finishUnload(className, shardName, record)
		// Report before the release, so every outcome is logged once every slot is free.
		s.reportBatch(ctx, outcomes)
		s.slots.Release(1)
	}()

	state, suspended, err := s.classSuspended(className)
	if err != nil {
		logger.Logf(accessorErrLevel(err),
			"cannot read the namespace state, leaving the shard for the next sweep: %v", err)
		return
	}
	if !suspended {
		logger.WithField("state", state).Debug("namespace no longer suspended, leaving the shard loaded")
		return
	}
	outcome := db.ShardUnloadOutcomeFailed // kept when UnloadShard panics
	err = enterrors.RunRecovered(logger, func() error {
		var err error
		outcome, err = s.db.UnloadShard(ctx, className, shardName)
		return err
	})
	if errors.Is(err, clusterSchema.ErrClassNotFound) {
		logger.Debugf("class dropped, nothing left to unload: %v", err)
		return
	}
	record = outcome
}

// finishUnload takes the shard out of inFlight, records outcome unless it is
// empty, and returns what endBatchLocked returns.
func (s *Sweep) finishUnload(className, shardName string, outcome db.ShardUnloadOutcome) map[string]shardOutcomes {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inFlight, shardKey{className, shardName})
	if outcome != "" {
		s.recordOutcomeLocked(className, shardName, outcome)
	}
	return s.endBatchLocked()
}

// recordOutcomeLocked makes outcome the shard's only recorded outcome. The
// caller holds mu.
func (s *Sweep) recordOutcomeLocked(className, shardName string, outcome db.ShardUnloadOutcome) {
	outcomes, ok := s.outcomes[className]
	if !ok {
		outcomes = shardOutcomes{}
		s.outcomes[className] = outcomes
	}
	for _, names := range outcomes {
		delete(names, shardName)
	}
	names, ok := outcomes[outcome]
	if !ok {
		names = map[string]struct{}{}
		outcomes[outcome] = names
	}
	names[shardName] = struct{}{}
}

// reportBatch logs the outcomes endBatchLocked returned, unless ctx has ended.
// A stopping node cancels its running unloads, which then read as
// refused_in_use.
func (s *Sweep) reportBatch(ctx context.Context, outcomes map[string]shardOutcomes) {
	if ctx.Err() == nil {
		s.logOutcomes(outcomes)
	}
}

// reportProgress logs every outcome recorded since the last report, each
// running unload as unloading, unless ctx has ended.
func (s *Sweep) reportProgress(ctx context.Context, stop chan struct{}) {
	outcomes := s.takeProgress(stop)
	if ctx.Err() == nil {
		s.logOutcomes(outcomes)
	}
}

// takeProgress records every running unload as unloading and takes the record,
// under one hold of mu so no unloading entry outlives the report. It returns nil
// once stop's batch has ended.
func (s *Sweep) takeProgress(stop chan struct{}) map[string]shardOutcomes {
	s.mu.Lock()
	defer s.mu.Unlock()
	// A report that fires as its batch ends must not report the next batch.
	if s.stopProgress != stop {
		return nil
	}
	for key := range s.inFlight {
		s.recordOutcomeLocked(key.className, key.shardName, outcomeUnloading)
	}
	return s.takeOutcomesLocked()
}

// takeOutcomesLocked clears the record and returns what it held, so each
// outcome is logged once. The caller holds mu.
func (s *Sweep) takeOutcomesLocked() map[string]shardOutcomes {
	outcomes := s.outcomes
	s.outcomes = map[string]shardOutcomes{}
	return outcomes
}

func (s *Sweep) logOutcomes(outcomes map[string]shardOutcomes) {
	for _, className := range slices.Sorted(maps.Keys(outcomes)) {
		s.reportClass(className, outcomes[className])
	}
}

// reportClass logs at Info the unloads still running or refused as in use, at
// Error those that need a restart, and each other outcome at Debug, a recovering
// shard included.
func (s *Sweep) reportClass(className string, outcomes shardOutcomes) {
	logger := s.logger.WithField("class", className)
	pending, broken := logrus.Fields{}, logrus.Fields{}
	var pendingMsgs []string
	for _, outcome := range slices.Sorted(maps.Keys(outcomes)) {
		if len(outcomes[outcome]) == 0 {
			continue
		}
		key, names := string(outcome), db.ReportedShardNames(outcomes[outcome])
		switch outcome {
		case outcomeUnloading:
			pending[key] = names
			pendingMsgs = append(pendingMsgs, "shard unloads still running")
		case db.ShardUnloadOutcomeRefusedInUse:
			pending[key] = names
			pendingMsgs = append(pendingMsgs,
				"shards in use, a later sweep retries them while the namespace stays suspended")
		case db.ShardUnloadOutcomeTorn, db.ShardUnloadOutcomeFailed:
			broken[key] = names
		case db.ShardUnloadOutcomeIndexClosing, db.ShardUnloadOutcomeRecovering, db.ShardUnloadOutcomeUnloaded:
			logger.WithField(key, names).Debug("shard unload outcome")
		}
	}
	if len(pending) > 0 {
		logger.WithFields(pending).Info(strings.Join(pendingMsgs, "; "))
	}
	if len(broken) > 0 {
		logger.WithFields(broken).
			Error("shard unloads failed, restart the node to clear these shards before resuming the namespace")
	}
}

// UnloadInFlight reports whether a sweep dispatched the shard's unload and the
// unload has not returned. A shard still waiting for a slot is not in flight.
func (s *Sweep) UnloadInFlight(className, shardName string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.inFlight[shardKey{className, shardName}]
	return ok
}

// classSuspended reports whether the class's namespace reads suspended.
func (s *Sweep) classSuspended(className string) (api.NamespaceState, bool, error) {
	state, err := s.db.NamespaceStateForClass(className)
	if err != nil {
		return "", false, err
	}
	return state, state == api.NamespaceStateSuspended, nil
}

// accessorErrLevel is the level Sweep logs a DB accessor error at. A class drop
// or node stop returns ErrClassNotFound or ErrIndexClosing. A diverged namespace
// map or a namespace delete returns ErrNamespaceUnknownLocally.
func accessorErrLevel(err error) logrus.Level {
	switch {
	case errors.Is(err, clusterSchema.ErrClassNotFound), errors.Is(err, db.ErrIndexClosing):
		return logrus.DebugLevel
	case errors.Is(err, db.ErrNamespaceUnknownLocally):
		return logrus.WarnLevel
	default:
		return logrus.ErrorLevel
	}
}
