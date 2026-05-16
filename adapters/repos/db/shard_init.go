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
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/indexcheckpoint"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	shardusage "github.com/weaviate/weaviate/adapters/repos/db/shard_usage"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	entsentry "github.com/weaviate/weaviate/entities/sentry"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func NewShard(ctx context.Context, promMetrics *monitoring.PrometheusMetrics,
	shardName string, index *Index, class *models.Class, jobQueueCh chan job,
	scheduler *queue.Scheduler, indexCheckpoints *indexcheckpoint.Checkpoints,
	reindexer ShardReindexerV3, lazyLoadSegments bool, bitmapBufPool roaringset.BitmapBufPool,
) (_ *Shard, err error) {
	start := time.Now()
	index.logger.WithFields(logrus.Fields{
		"action": "init_shard",
		"shard":  shardName,
		"index":  index.ID(),
	}).Debugf("initializing shard %q", shardName)

	if shardusage.RemoveComputedUsageDataForUnloadedShard(index.path(), shardName); err != nil {
		return nil, fmt.Errorf("shard %q: remove computed usage file for unloaded shard: %w", shardName, err)
	}

	if err := newPropertyDeleteIndexHelper().ensureBucketsAreRemovedForNonExistentPropertyIndexes(index.path(), shardName, class); err != nil {
		return nil, fmt.Errorf("shard %q: remove nonexistent property index buckets: %w", shardName, err)
	}

	if err := newVectorDropIndexHelper().ensureFilesAreRemovedForDroppedVectorIndexes(index.path(), shardName, class); err != nil {
		return nil, fmt.Errorf("shard %q: remove dropped vector index files: %w", shardName, err)
	}

	metrics, err := NewMetrics(index.logger, promMetrics, string(index.Config.ClassName), shardName)
	if err != nil {
		return nil, fmt.Errorf("init shard %q metrics: %w", shardName, err)
	}
	if index.Config.LazySegmentsDisabled {
		lazyLoadSegments = false // disabled globally
	}

	shutCtx, shutCtxCancel := context.WithCancelCause(context.Background())

	s := &Shard{
		index:       index,
		class:       class,
		name:        shardName,
		promMetrics: promMetrics,
		metrics:     metrics,
		slowQueryReporter: helpers.NewSlowQueryReporter(index.Config.QuerySlowLogEnabled,
			index.Config.QuerySlowLogThreshold, index.logger),
		replicationMap:   pendingReplicaTasks{Tasks: make(map[string]replicaTask, 32)},
		centralJobQueue:  jobQueueCh,
		scheduler:        scheduler,
		indexCheckpoints: indexCheckpoints,

		shutdownLock:  new(sync.RWMutex),
		shutCtx:       shutCtx,
		shutCtxCancel: shutCtxCancel,

		status:                          ShardStatus{Status: storagestate.StatusLoading},
		searchableBlockmaxPropNamesLock: new(sync.Mutex),
		reindexer:                       reindexer,
		usingBlockMaxWAND:               index.invertedIndexConfig.UsingBlockMaxWAND,
		bitmapBufPool:                   bitmapBufPool,
		HFreshEnabled:                   index.HFreshEnabled,
		lazySegmentLoadingEnabled:       lazyLoadSegments,
	}

	index.metrics.UpdateShardStatus("", storagestate.StatusLoading.String())

	defer func() {
		p := recover()
		if p != nil {
			err = fmt.Errorf("unexpected error initializing shard %q of index %q: %v", shardName, index.ID(), p)
			index.logger.WithError(err).WithFields(logrus.Fields{
				"index": index.ID(),
				"shard": shardName,
			}).Error("panic during shard initialization")
			enterrors.PrintStack(index.logger)
		}

		if err != nil {
			// Initializing a shard should normally not fail. If it does, this could
			// mean that this setup requires further attention, e.g. to manually fix
			// a data corruption. This makes it a prime use case for sentry:
			entsentry.CaptureException(err)
			// spawn a new context as we cannot guarantee that the init context is
			// still valid, but we want to make sure that we have enough time to clean
			// up the partial init
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()
			s.index.logger.WithFields(logrus.Fields{
				"action":   "new_shard",
				"duration": 120 * time.Second,
			}).Debug("context.WithTimeout")

			s.cleanupPartialInit(ctx)
		}
	}()

	defer func() {
		index.metrics.ObserveUpdateShardStatus(s.status.Status.String(), time.Since(start))
	}()

	s.activityTrackerRead.Store(1)  // initial state
	s.activityTrackerWrite.Store(1) // initial state
	s.initCycleCallbacks()

	s.docIdLock = make([]sync.Mutex, IdLockPoolSize)

	defer index.metrics.ShardStartup(start)

	_, err = os.Stat(s.path())
	exists := err == nil

	if err := os.MkdirAll(s.path(), os.ModePerm); err != nil {
		return nil, err
	}

	// init the store itself synchronously
	if err := s.initLSMStore(); err != nil {
		return nil, fmt.Errorf("init shard's %q store: %w", s.ID(), err)
	}

	// Finalize any completed migrations whose directory renames were deferred
	// from a runtime swap. This must run before bucket loading (initNonVector)
	// so that buckets are found at their canonical directory names.
	FinalizeCompletedMigrations(s.pathLSM(), s.index.logger)

	// Pessimistically mark any in-flight enable-rangeable / repair-rangeable
	// migration's target property as "not locally ready" on this shard.
	// Without this, a post-restart shard whose recovery hasn't finished
	// the local swap yet would serve range queries from an empty
	// PreReindexHook'd bucket as soon as the cluster-wide schema flag
	// flips on another node. See [Shard.rangeableLocalReady] and GH
	// 0-weaviate-issues#212 Issue C for the full rationale. Props not
	// found in this scan default to "ready" (no migration ever ran, or
	// every prior migration already tidied — FinalizeCompletedMigrations
	// above promoted them to canonical above).
	markInFlightRangeableMigrationsNotReady(s)

	_ = s.reindexer.RunBeforeLsmInit(ctx, s)

	if err := s.initNonVector(ctx, class); err != nil {
		return nil, errors.Wrapf(err, "init shard %q", s.ID())
	}

	if err = s.initShardVectors(ctx); err != nil {
		return nil, fmt.Errorf("init shard vectors: %w", err)
	}

	if s.index.AsyncIndexingEnabled {
		f := func() {
			_ = s.ForEachVectorQueue(func(targetVector string, _ *VectorIndexQueue) error {
				if err := s.ConvertQueue(targetVector); err != nil {
					index.logger.WithError(err).Errorf("preload shard for target vector: %s", targetVector)
				}
				return nil
			})
		}
		enterrors.GoWrapper(f, s.index.logger)
	}
	s.NotifyReady()

	if exists {
		s.index.logger.Printf("Completed loading shard %s in %s", s.ID(), time.Since(start))
	} else {
		s.index.logger.Printf("Created shard %s in %s", s.ID(), time.Since(start))
	}

	_ = s.reindexer.RunAfterLsmInit(ctx, s)
	_ = s.reindexer.RunAfterLsmInitAsync(ctx, s)
	return s, nil
}

// cleanupPartialInit is called when the shard was only partially initialized.
// Internally it just uses [Shutdown], but also adds some logging.
func (s *Shard) cleanupPartialInit(ctx context.Context) {
	log := s.index.logger.WithField("action", "cleanup_partial_initialization")
	if err := s.Shutdown(ctx); err != nil {
		log.WithError(err).Error("failed to shutdown store")
	}

	log.Debug("successfully cleaned up partially initialized shard")
}

func (s *Shard) NotifyReady() {
	s.UpdateStatus(storagestate.StatusReady.String(), statusReasonNotifyReady)
	s.index.logger.
		WithField("action", "startup").
		Debugf("shard=%s is ready", s.name)
}

// markInFlightRangeableMigrationsNotReady scans this shard's
// .migrations/ directory for rangeable-related tracker dirs whose
// `tidied.mig` sentinel is not present, and flips the corresponding
// per-prop entry in Shard.rangeableLocalReady to false. See GH
// 0-weaviate-issues#212 Issue C and [Shard.rangeableLocalReady] for
// rationale. Idempotent and safe to call on shards with no rangeable
// migrations on disk.
//
// Rangeable migrations under [FilterableToRangeableStrategy] use a
// tracker dir named `filterable_to_rangeable[_<p1>_<p2>...]_<gen>`.
// We extract the prop-name component(s) by stripping the prefix +
// trailing `_<gen>` segment, then split on `_` to handle the
// multi-property dir-name shape produced by [migrationDirWithProps].
//
// Properties that don't have a tracker dir, or whose dir has
// `tidied.mig` (FinalizeCompletedMigrations promoted them to canonical
// in this same startup), are left untouched — the default-true policy
// in [Shard.IsRangeableLocallyReady] applies to them.
func markInFlightRangeableMigrationsNotReady(s *Shard) {
	migrationsDir := filepath.Join(s.pathLSM(), ".migrations")
	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		// No .migrations dir is the common case: nothing to do.
		return
	}
	const prefix = MigrationDirPrefixFilterableToRangeable + "_"
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		base, _, ok := parseMigrationDirName(name)
		if !ok {
			continue
		}
		// `base` is the strategy + props portion (no _<gen>), e.g.
		// "filterable_to_rangeable_price" or "filterable_to_rangeable_a_b".
		if !strings.HasPrefix(base, prefix) {
			continue
		}
		propsPart := strings.TrimPrefix(base, prefix)
		if propsPart == "" {
			continue
		}
		// tidied.mig present means FinalizeCompletedMigrations either
		// promoted the migration or will at the next call site; the
		// query-side fallback isn't needed for these.
		dirPath := filepath.Join(migrationsDir, name)
		if fileExistsInDir(dirPath, "tidied.mig") {
			continue
		}
		// Multi-prop migrations use `_` as the separator (see
		// migrationDirWithProps). Single-prop is just one segment.
		for _, propName := range strings.Split(propsPart, "_") {
			if propName == "" {
				continue
			}
			s.setRangeableLocallyReady(propName, false)
		}
	}
}
