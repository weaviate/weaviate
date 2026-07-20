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
	"encoding/json"
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
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
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

	if err := shardusage.RemoveComputedUsageDataForUnloadedShard(index.path(), shardName); err != nil {
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

	if err := s.sweepChangelogDir(); err != nil {
		return nil, fmt.Errorf("sweep changelog dir for shard %q: %w", s.ID(), err)
	}

	// init the store itself synchronously
	if err := s.initLSMStore(); err != nil {
		return nil, fmt.Errorf("init shard's %q store: %w", s.ID(), err)
	}

	// Must run before FinalizeCompletedMigrations deletes the on-disk
	// evidence; see seedRangeableLocalReadyFromMigrationHistory godoc.
	seedRangeableLocalReadyFromMigrationHistory(s)

	// Finalize any completed migrations whose directory renames were deferred
	// from a runtime swap. This must run before bucket loading (initNonVector)
	// so that buckets are found at their canonical directory names.
	FinalizeCompletedMigrations(s.pathLSM(), s.index.logger)

	// Pessimistically mark any in-flight enable-rangeable / repair-rangeable
	// migration's target property as "not locally ready" on this shard.
	// Without this, a post-restart shard whose recovery hasn't finished
	// the local swap yet would serve range queries from an empty
	// PreReindexHook'd bucket as soon as the cluster-wide schema flag
	// flips on another node. See [Shard.rangeableLocalReady] for the
	// full rationale. Props not found in this scan default to "ready"
	// (no migration ever ran, or every prior migration already tidied  -
	// FinalizeCompletedMigrations above promoted them to canonical).
	markInFlightRangeableMigrationsNotReady(s)

	// Must run exactly here; see
	// loadRangeableBucketsForCompletedLocalMigrations godoc.
	if err := loadRangeableBucketsForCompletedLocalMigrations(ctx, s); err != nil {
		return nil, fmt.Errorf("load rangeable buckets for shard %q: %w", s.ID(), err)
	}

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

// rangeableMigrationTrackerDirs returns the absolute paths of every
// rangeable migration tracker directory under this shard's
// .migrations/ dir, in any lifecycle state. Shared by
// [seedRangeableLocalReadyFromMigrationHistory] (runs before
// FinalizeCompletedMigrations, sees tidied dirs too) and
// [markInFlightRangeableMigrationsNotReady] (runs after, so only
// genuinely in-flight dirs remain).
func rangeableMigrationTrackerDirs(s *Shard) []string {
	migrationsDir := filepath.Join(s.pathLSM(), ".migrations")
	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		return nil
	}
	const prefix = MigrationDirPrefixFilterableToRangeable + "_"
	var dirs []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		base, _, ok := parseMigrationDirName(name)
		if !ok {
			continue
		}
		if !strings.HasPrefix(base, prefix) {
			continue
		}
		dirs = append(dirs, filepath.Join(migrationsDir, name))
	}
	return dirs
}

// seedRangeableLocalReadyFromMigrationHistory scans this shard's
// .migrations/ directory for every rangeable migration tracker, tidied
// or not, and optimistically marks each tracker's properties "locally
// ready" (true) in Shard.rangeableLocalReady.
//
// MUST run before FinalizeCompletedMigrations, which deletes a tidied
// tracker's directory - the only on-disk evidence a shard ever
// completed a rangeable migration. Without this seed, a shard that
// restarts after its local swap tidied but before the cluster-wide
// schema flip lands loses that fact: bucket loading is schema-driven
// off the still-pre-flip live schema, so IsRangeableLocallyReady's
// bucket-existence fallback can't self-heal it either
// (weaviate/weaviate#12189).
//
// markInFlightRangeableMigrationsNotReady runs AFTER
// FinalizeCompletedMigrations and is the authoritative correction pass
// for still-in-flight properties; the ordering (seed, finalize,
// correct) happens entirely within NewShard before NotifyReady.
//
// An unparseable payload.mig disables the fast exit for the whole
// shard rather than guessing at property names.
func seedRangeableLocalReadyFromMigrationHistory(s *Shard) {
	for _, dirPath := range rangeableMigrationTrackerDirs(s) {
		propNames, ok := readRecoveryPropertyNames(dirPath)
		if !ok {
			s.rangeableLocalReadyHistoryUnknown.Store(true)
			continue
		}
		for _, propName := range propNames {
			s.setRangeableLocallyReady(propName, true)
		}
	}
}

// markInFlightRangeableMigrationsNotReady scans this shard's
// .migrations/ directory for rangeable-related tracker dirs whose
// `tidied.mig` sentinel is not present, and flips the corresponding
// per-prop entry in Shard.rangeableLocalReady to false. See
// [Shard.rangeableLocalReady] for rationale. Idempotent and safe to
// call on shards with no rangeable migrations on disk.
//
// Property names are read from the on-disk recovery payload (payload.mig
// inside each tracker dir). Parsing them out of the dir name would be
// fragile for props whose names themselves contain `_` (e.g.
// `price_cents`), because [migrationDirWithProps] joins multiple props
// with `_` - the dir-name decoder can't tell `price_cents` (one prop)
// from `[price, cents]` (two props).
//
// Tracker dirs whose payload.mig is unreadable or missing are skipped
// - they are either stale (operator surgery, partial-init crash) or
// from an old build before payload persistence. We accept the
// default-true policy in [Shard.IsRangeableLocallyReady] for those
// edge cases; the bucket-existence fallback inside
// IsRangeableLocallyReady still protects queries when the
// PreReindexHook hasn't fired yet on this replica.
//
// Properties that don't have a tracker dir, or whose dir has
// `tidied.mig` (FinalizeCompletedMigrations promoted them to canonical
// in this same startup), are left untouched - the default-true policy
// in [Shard.IsRangeableLocallyReady] applies to them. Runs AFTER
// FinalizeCompletedMigrations, so only genuinely in-flight trackers
// remain here - [seedRangeableLocalReadyFromMigrationHistory] already
// captured the tidied ones.
func markInFlightRangeableMigrationsNotReady(s *Shard) {
	for _, dirPath := range rangeableMigrationTrackerDirs(s) {
		// tidied.mig present means FinalizeCompletedMigrations either
		// promoted the migration or will at the next call site; the
		// query-side fallback isn't needed for these.
		if fileExistsInDir(dirPath, "tidied.mig") {
			continue
		}
		propNames, ok := readRecoveryPropertyNames(dirPath)
		if !ok {
			continue
		}
		for _, propName := range propNames {
			s.setRangeableLocallyReady(propName, false)
		}
	}
}

// readRecoveryPropertyNames extracts the `Properties` slice from a
// migration tracker dir's payload.mig sentinel file (see
// ShardReindexTaskGeneric.SaveRecoveryPayload). Returns (nil, false)
// when the file is missing, unreadable, or doesn't parse as a
// ReindexTaskPayload-shaped JSON - those edge cases are tolerated by
// the caller, which falls back to the default-true readiness policy.
func readRecoveryPropertyNames(migDir string) ([]string, bool) {
	data, err := os.ReadFile(filepath.Join(migDir, reindexRecoveryPayloadFile))
	if err != nil {
		return nil, false
	}
	// Anonymous shape: only the field we need. Avoids depending on
	// ReindexTaskPayload here (no import cycle risk, but keeping shard
	// init lean).
	var rec struct {
		Payload struct {
			Properties []string `json:"properties"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, false
	}
	return rec.Payload.Properties, true
}

// loadRangeableBucketsForCompletedLocalMigrations loads the rangeable
// bucket for every property marked "locally ready" in
// Shard.rangeableLocalReady but that the LIVE (pre-flip) class schema
// wouldn't load on its own: createPropertyValueIndex only loads a
// rangeable bucket when HasRangeableIndex(prop) is true against the
// live schema, which by construction is still pre-flip in exactly this
// window. Without this load, every consumer that trusts the
// explicit-true entry (write overlay, addToPropertyValueIndex,
// deleteFromInvertedIndicesLSM, the read gate once the flip lands) hits
// a nil s.store.Bucket(...) and hard-errors, even though the bucket
// physically exists on disk.
//
// Two ordering constraints on the NewShard call site:
//  1. MUST run AFTER FinalizeCompletedMigrations: the canonical bucket
//     directory doesn't exist under its un-suffixed name until that
//     rename runs. Loading here first would create it empty, and
//     finalizeMigrationDir's stale-dir removal would then os.RemoveAll
//     it out from under an already-registered bucket - store
//     corruption, not just a stale read.
//  2. Driven off rangeableLocalReady, not class.Properties: enable-
//     rangeable supports IndexFilterable=false targets that
//     initPropertyBuckets' HasAnyInvertedIndex gate skips until the
//     live schema flip lands, so iterating class.Properties would
//     silently miss them.
//
// Idempotent: CreateOrLoadBucket early-returns on an already-registered
// bucket name.
func loadRangeableBucketsForCompletedLocalMigrations(ctx context.Context, s *Shard) error {
	s.rangeableLocalReadyMu.RLock()
	propNames := make([]string, 0, len(s.rangeableLocalReady))
	for propName, ready := range s.rangeableLocalReady {
		if ready {
			propNames = append(propNames, propName)
		}
	}
	s.rangeableLocalReadyMu.RUnlock()

	for _, propName := range propNames {
		bucketName := helpers.BucketRangeableFromPropNameLSM(propName)
		if s.store.Bucket(bucketName) != nil {
			continue
		}
		if err := s.store.CreateOrLoadBucket(
			ctx, bucketName,
			s.makeDefaultBucketOptions(lsmkv.StrategyRoaringSetRange)...,
		); err != nil {
			return fmt.Errorf("load rangeable bucket for prop %q: %w", propName, err)
		}
	}
	return nil
}
