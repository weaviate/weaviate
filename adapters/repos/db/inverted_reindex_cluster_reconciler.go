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

package db

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/logrusext"
)

const migrationClusterQueryTimeout = 5 * time.Second

const migrationClusterReconcileInterval = time.Minute

// MigrationLocalTaskSource reads this node's own applied view of the reindex
// task namespace. ok=false means "not installed", which licenses no discard.
type MigrationLocalTaskSource func() ([]*distributedtask.Task, bool)

// MigrationClusterTaskSource reads the reindex task namespace from the leader.
type MigrationClusterTaskSource func(context.Context) ([]*distributedtask.Task, error)

// migrationClusterReconciler decides this node's undecided migration records
// against the leader's reindex task list.
type migrationClusterReconciler struct {
	db *DB

	mu      sync.RWMutex
	local   MigrationLocalTaskSource
	cluster MigrationClusterTaskSource

	samplersOnce sync.Once
	unresolved   *logrusext.Sampler
	shuttingDown *logrusext.Sampler

	periodicOnce sync.Once
}

// A node drain fails preventShutdown on every loaded shard at once, and a
// shard count is a tenant count, so one pass can reach these two lines once
// per tenant. The pass repeats every interval, so the sampling windows have to
// span passes: the samplers live on the reconciler, not on a call.
func (r *migrationClusterReconciler) samplers() (unresolved, shuttingDown *logrusext.Sampler) {
	r.samplersOnce.Do(func() {
		r.unresolved = logrusext.NewSampler(r.db.logger, maxReportedErrors, migrationClusterReconcileInterval)
		r.shuttingDown = logrusext.NewSampler(r.db.logger, maxReportedErrors, migrationClusterReconcileInterval)
	})
	return r.unresolved, r.shuttingDown
}

// SetTaskSources and the periodic pass run on different goroutines, so the
// sources are guarded; periodicOnce starts the pass on the first call only.
func (r *migrationClusterReconciler) SetTaskSources(ctx context.Context, source MigrationLocalTaskSource,
	cluster MigrationClusterTaskSource,
) {
	func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.local = source
		r.cluster = cluster
	}()

	r.ReconcileLoaded(ctx)
	r.periodicOnce.Do(func() {
		enterrors.GoWrapper(func() { r.reconcilePeriodically(ctx) }, r.db.logger)
	})
}

func (r *migrationClusterReconciler) reconcilePeriodically(ctx context.Context) {
	ticker := time.NewTicker(migrationClusterReconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.db.shutdown:
			return
		case <-ticker.C:
			r.ReconcileLoaded(ctx)
		}
	}
}

func (r *migrationClusterReconciler) ReconcileLoaded(ctx context.Context) {
	undecided := r.shardsWithUndecidedRecords()
	if len(undecided) == 0 {
		return
	}
	tasks, err := r.clusterTasksBounded(ctx)
	if err != nil {
		r.db.logger.WithField("action", "reindex_migration_reconcile").Warnf(
			"the leader's reindex task list is unreachable; deciding nothing this pass: %v", err)
		return
	}

	unresolved, shuttingDown := r.samplers()

	for _, found := range undecided {
		for _, name := range found.names {
			r.reconcileShard(ctx, found.idx, name, tasks, unresolved, shuttingDown)
		}
	}
}

// Names, not shards: resolving a shard the walk already handed over would
// call Load, which has no shutdown flag to refuse and would rebuild a
// deactivated tenant outside the shard map, where nothing can ever shut it
// down again. getLoadedShard takes the same locks every teardown does, so a
// shard torn down since its name was collected reads as absent instead.
func (r *migrationClusterReconciler) reconcileShard(ctx context.Context, idx *Index, name string,
	tasks []*distributedtask.Task, unresolved, shuttingDown *logrusext.Sampler,
) {
	shard, release, err := idx.getLoadedShard(name)
	if err != nil {
		shuttingDown.WithSampling(func(l logrus.FieldLogger) {
			l.WithField("shard", name).Infof(
				"skipping migration reconciliation on a shard that is shutting down: %v", err)
		})
		return
	}
	defer release()
	if shard == nil {
		return
	}

	concrete, err := unwrapShard(ctx, shard)
	if err != nil {
		unresolved.WithSampling(func(l logrus.FieldLogger) {
			l.WithField("shard", name).Errorf(
				"skipping migration reconciliation on a shard that could not be resolved: %v", err)
		})
		return
	}
	concrete.migrations().ReconcileWithClusterTasks(ctx, tasks)
}

func loadedShardNames(idx *Index) []string {
	var names []string
	idx.ForEachLoadedShard(func(name string, _ ShardLike) error {
		names = append(names, name)
		return nil
	})
	return names
}

func (r *migrationClusterReconciler) snapshotIndices() []*Index {
	r.db.indexLock.RLock()
	defer r.db.indexLock.RUnlock()
	indices := make([]*Index, 0, len(r.db.indices))
	for _, idx := range r.db.indices {
		indices = append(indices, idx)
	}
	return indices
}

// Shard names, not a boolean: the walk that finds the shards with something to
// decide is the walk that reconciles them, so a node where one tenant in a
// hundred thousand holds a record does not pay for the other 99,999 twice.
type migrationUndecidedShards struct {
	idx   *Index
	names []string
}

func (r *migrationClusterReconciler) shardsWithUndecidedRecords() []migrationUndecidedShards {
	var found []migrationUndecidedShards
	for _, idx := range r.snapshotIndices() {
		var names []string
		idx.ForEachLoadedShard(func(name string, shard ShardLike) error {
			store := shard.migrationRecordStore()
			if store == nil || len(store.Unreadable()) > 0 {
				return nil
			}
			if store.HasUndecided() {
				names = append(names, name)
			}
			return nil
		})
		if len(names) > 0 {
			found = append(found, migrationUndecidedShards{idx: idx, names: names})
		}
	}
	return found
}

func (r *migrationClusterReconciler) LocalTasks() ([]*distributedtask.Task, bool) {
	source := func() MigrationLocalTaskSource {
		r.mu.RLock()
		defer r.mu.RUnlock()
		return r.local
	}()

	if source == nil {
		return nil, false
	}
	return source()
}

// clusterTasksBounded puts a timeout on the leader query alone, so an
// unreachable leader cannot hold a pass open. The shard walk after it is
// not bounded.
func (r *migrationClusterReconciler) clusterTasksBounded(ctx context.Context) ([]*distributedtask.Task, error) {
	source := func() MigrationClusterTaskSource {
		r.mu.RLock()
		defer r.mu.RUnlock()
		return r.cluster
	}()

	if source == nil {
		return nil, fmt.Errorf("no cluster-wide reindex task source is installed on this node")
	}
	ctx, cancel := context.WithTimeout(ctx, migrationClusterQueryTimeout)
	defer cancel()
	return source(ctx)
}

// SetMigrationTaskSources installs the two task sources reconciliation reads.
// The first pass runs before this returns; the repeat runs until ctx ends.
func (db *DB) SetMigrationTaskSources(ctx context.Context, source MigrationLocalTaskSource,
	cluster MigrationClusterTaskSource,
) {
	db.migrationCluster.SetTaskSources(ctx, source, cluster)
}
