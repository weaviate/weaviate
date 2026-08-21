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

	// Ahead of initNonVector because reconciliation renames directories: a
	// bucket opened at a name it is about to move would serve the wrong data.
	s.reconcileMigrationRecords(ctx, class)

	// Pessimistically mark any in-flight enable-rangeable / repair-rangeable
	// migration's target property as "not locally ready" on this shard.
	// Without this, a post-restart shard whose recovery hasn't finished
	// the local swap yet would serve range queries from an empty
	// PreReindexHook'd bucket as soon as the cluster-wide schema flag
	// flips on another node. See [Shard.rangeableLocalReady] for the
	// full rationale. Props no record names default to "ready": no migration
	// ever ran, or reconciliation above already promoted it.
	markInFlightRangeableMigrationsNotReady(s)

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

// markInFlightRangeableMigrationsNotReady flips the per-prop entry in
// Shard.rangeableLocalReady to false for every rangeable migration on this
// shard whose flip decision is not yet durable. See
// [Shard.rangeableLocalReady] for the rationale. Idempotent and safe to call
// on shards with no rangeable migration.
//
// Property names come from the record rather than from the tracker dir's
// name: that name joins multiple properties with "_", so its decoder cannot
// tell "price_cents" (one property) from ["price", "cents"] (two).
//
// A migration whose flip is decided is left untouched — reconciliation has
// promoted it, or will at the load that can rename its directory safely — and
// so is a property no record names. Both fall back to the default-true policy
// in [Shard.IsRangeableLocallyReady].
//
// A record that does not decode is the third case, and it cannot be answered
// per property: the property list is exactly what could not be read. It marks
// the shard undecidable instead, which the same policy reads as not ready.
func markInFlightRangeableMigrationsNotReady(s *Shard) {
	if s.migrationRecords == nil {
		return
	}
	if len(s.migrationRecords.Unreadable()) > 0 {
		s.rangeableUndecidable.Store(true)
	}
	for _, rec := range s.migrationRecords.Records() {
		if rec.Subject().Key.StrategyCode != StrategyCodeFilterableToRangeable || rec.PointerSwapped() {
			continue
		}
		for _, propName := range rec.Subject().Properties {
			s.setRangeableLocallyReady(propName, false)
		}
	}
}

// maxRecoveryPayloadBytes bounds what [readTaskProps] parses. A payload names
// every targeted tenant, so a large multi-tenant migration reaches megabytes,
// and the cleanup probes that want one field from it run inside the RAFT
// apply of a property DELETE, holding the FSM loop cluster-wide.
//
// A payload over the bound is refused, not parsed, and reads as
// [errRecoveryPayloadTooLarge] — see [readTaskProps] for what callers conclude.
const maxRecoveryPayloadBytes = 1 << 20 // 1 MiB

// unboundedRecoveryPayload parses a payload of any size.
const unboundedRecoveryPayload = 0

// errRecoveryPayloadTooLarge marks a payload.mig [maxRecoveryPayloadBytes]
// refused. Distinguishable from a payload that was opened and could not be
// parsed, so a refusal is not counted as a read: it cost a stat.
var errRecoveryPayloadTooLarge = errors.New("recovery payload exceeds the parse bound")

// readRecoveryPayloadFacts extracts the property list and migration type from
// a migration tracker dir's payload.mig file (see
// ShardReindexTaskGeneric.SaveRecoveryPayload). The error keeps a missing
// payload (os.IsNotExist) distinguishable from an unreadable or unparseable
// one: [migrationDirScope.inScopeFailingOpen] treats only the former as "the
// task recorded nothing", while the latter makes the unloaded-shard gate fail
// open.
//
// maxBytes refuses a larger payload before opening it;
// [unboundedRecoveryPayload] reads any size.
func readRecoveryPayloadFacts(migDir string, maxBytes int64) ([]string, ReindexMigrationType, error) {
	path := filepath.Join(migDir, reindexRecoveryPayloadFile)
	if maxBytes > unboundedRecoveryPayload {
		info, err := os.Stat(path)
		if err != nil {
			return nil, "", err
		}
		if info.Size() > maxBytes {
			return nil, "", fmt.Errorf("%w: %s holds %d bytes, bound is %d",
				errRecoveryPayloadTooLarge, reindexRecoveryPayloadFile, info.Size(), maxBytes)
		}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}
	// Anonymous shape: only the fields we need. Avoids depending on
	// ReindexTaskPayload here (no import cycle risk, but keeping shard
	// init lean).
	var rec struct {
		Payload struct {
			Properties    []string             `json:"properties"`
			MigrationType ReindexMigrationType `json:"migrationType"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, "", fmt.Errorf("parse %s: %w", reindexRecoveryPayloadFile, err)
	}
	return rec.Payload.Properties, rec.Payload.MigrationType, nil
}
