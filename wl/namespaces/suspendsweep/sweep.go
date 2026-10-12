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
	"slices"
	"sync"

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

type shardKey struct{ className, shardName string }

// Sweep unloads the local shards of suspended namespaces. Its state outlives a
// tick, so slots caps the running unloads across classes and ticks.
type Sweep struct {
	logger *logrus.Entry
	db     DB
	slots  *semaphore.Weighted // one slot per running unload

	// mu is never held across a DB call or slots.Acquire.
	mu       sync.Mutex
	inFlight map[shardKey]struct{} // guarded by mu
}

// New returns a sweep over repo that runs at most limit unloads at once.
func New(logger *logrus.Entry, repo DB, limit int) *Sweep {
	return &Sweep{
		logger:   logger,
		db:       repo,
		slots:    semaphore.NewWeighted(int64(limit)),
		inFlight: map[shardKey]struct{}{},
	}
}

// Run is one tick. It waits for a slot as long as it takes, so it returns once
// every listed shard is dispatched or ctx ends. The unloads it started keep
// running on ctx. Calls must not overlap, and the suspend reconcile cron job
// never overlaps them.
func (s *Sweep) Run(ctx context.Context) {
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
	s.mu.Lock()
	toUnload := slices.DeleteFunc(names, func(shardName string) bool {
		_, inFlight := s.inFlight[shardKey{className, shardName}]
		return inFlight
	})
	s.mu.Unlock()

	for _, shardName := range toUnload {
		if err := s.slots.Acquire(ctx, 1); err != nil {
			return
		}
		if !stillSuspended() {
			s.slots.Release(1)
			return
		}
		s.mu.Lock()
		s.inFlight[shardKey{className, shardName}] = struct{}{}
		s.mu.Unlock()
		enterrors.GoWrapper(func() { s.unload(ctx, className, shardName) }, s.logger)
	}
}

// unload closes the shard unless its class no longer reads suspended. It reads
// the state after reconcileClass has added the shard to inFlight, so once a
// resume has applied, a false UnloadInFlight means this unload closes nothing.
func (s *Sweep) unload(ctx context.Context, className, shardName string) {
	logger := s.logger.WithFields(logrus.Fields{"class": className, "shard": shardName})
	defer func() {
		s.mu.Lock()
		delete(s.inFlight, shardKey{className, shardName})
		s.mu.Unlock()
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
	// shutdownOrRestoreShard logs a shutdown that fails, and RunRecovered logs a panic.
	_ = enterrors.RunRecovered(logger, func() error {
		_, err := s.db.UnloadShard(ctx, className, shardName)
		return err
	})
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
