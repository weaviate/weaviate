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
	registration monitoring.ShardRegistration,
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
		registration:                    registration,
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
	s.inheritResourcePressureReadOnly()

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

// inheritResourcePressureReadOnly marks a freshly built shard READONLY while
// the resource scan holds the DB read-only, so a shard that did not exist when
// the scan swept does not come up READY and take the writes the scan is trying
// to stop.
//
// The flag is raised before the sweep, so a lazily loaded shard is caught by
// exactly one of the two: this runs under the load lock the sweep needs to see
// the shard as loaded. An eagerly built shard is not in the shard map yet here,
// and reconciles against the flag when it is published instead.
func (s *Shard) inheritResourcePressureReadOnly() {
	if !s.index.db.resourcePressureReadOnly() {
		return
	}
	if err := s.SetStatusReadonly(statusReasonResourcePressure); err != nil {
		s.index.logger.WithField("action", "set_shard_read_only").
			Errorf("failed to set to READONLY on init: shard %q: %v", s.name, err)
	}
}

func (s *Shard) NotifyReady() {
	s.UpdateStatus(storagestate.StatusReady.String(), statusReasonNotifyReady)
	s.index.logger.
		WithField("action", "startup").
		Debugf("shard=%s is ready", s.name)
}

// markInFlightRangeableMigrationsNotReady flips the per-prop entry in
// Shard.rangeableLocalReady to false for every rangeable migration on this
// shard that has not been promoted onto its canonical directory. See
// [Shard.rangeableLocalReady] for the rationale. Idempotent and safe on shards
// with no rangeable migration.
//
// Property names come from the record rather than from the tracker dir's name:
// that name joins multiple properties with "_", so its decoder cannot tell
// "price_cents" (one property) from ["price", "cents"] (two).
//
// Only a promoted migration is left untouched, along with a property no record
// names; both fall back to the default-true policy in
// [Shard.IsRangeableLocallyReady]. Promoted covers a superseded property too,
// whose canonical directory holds a successor's data — safe here because
// property_<p>_rangeable is built by exactly one strategy code, so the
// superseding record is another rangeable record this same scan answers.
//
// A decided flip is not enough: the flip decision is recorded before the first
// pointer moves, and it lives only in the process that made it, so a
// decided-but-unpromoted record at load means the canonical rangeable
// directory is the empty one initNonVector just recreated.
// Reconciliation runs immediately above and promotes what it can, so a record
// still short of Promoted here is one it declined. A record that does not decode
// cannot be answered per property, since the property list is exactly what could
// not be read: it marks the whole shard undecidable, which the same policy reads
// as not ready.
//
// The mark can be permanent. Only OnMigrationComplete clears it, and
// [migrationReconciler.promoteProperty] has terminal arms that leave a record
// at Swapped for good, so such a shard falls back to the filterable walk on
// every load until an operator resubmits the migration.
func markInFlightRangeableMigrationsNotReady(s *Shard) {
	if s.migrationRecords == nil {
		return
	}
	if len(s.migrationRecords.Unreadable()) > 0 {
		s.rangeableUndecidable.Store(true)
	}
	for _, rec := range s.migrationRecords.Records() {
		if rec.Subject().Key.StrategyCode != StrategyCodeFilterableToRangeable ||
			rec.State() == MigrationStatePromoted {
			continue
		}
		for _, propName := range rec.Subject().Properties() {
			s.setRangeableLocallyReady(propName, false)
		}
	}
}

// maxRecoveryPayloadBytes bounds the probes that want one field of payload.mig
// and run inside the RAFT apply of a property DELETE, holding the FSM loop
// cluster-wide. A payload names every targeted tenant and unit, so a large
// multi-tenant migration reaches megabytes.
//
// It is a latency bound, so it holds only where refusing is fail-open: over it
// the payload is refused rather than parsed and reads as
// [errRecoveryPayloadTooLarge] — see [readTaskProps] for what callers
// conclude.
const maxRecoveryPayloadBytes = 1 << 20 // 1 MiB

// maxRecoveryWalkPayloadBytes is what [loadReindexRecoveryRecord] reads under
// instead. Refusing there arms no double-write mirror, so the flip that
// follows takes the canonical directory away with every write since the
// restart — which no legitimate payload may be allowed to cause, and an
// ordinary multi-tenant migration clears a megabyte on tenant names alone.
//
// It is a memory bound, not a latency one: the walk runs once at startup, off
// any RAFT apply, but a corrupt or hostile file would otherwise be one
// unbounded read during boot, and an OOM there is a crash loop. Roughly 300
// bytes of the payload go per unit, so this clears a cluster of some 800k
// units — orders of magnitude past anything Weaviate runs.
const maxRecoveryWalkPayloadBytes = 256 << 20

// errRecoveryPayloadTooLarge marks a payload.mig [maxRecoveryPayloadBytes]
// refused. Distinguishable from a payload that was opened and could not be
// parsed, so a refusal is not counted as a read: it cost a stat.
var errRecoveryPayloadTooLarge = errors.New("recovery payload exceeds the parse bound")

func refuseOversizedRecoveryPayload(path string, bound int64) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Size() > bound {
		return fmt.Errorf("%w: %s holds %d bytes, bound is %d",
			errRecoveryPayloadTooLarge, reindexRecoveryPayloadFile, info.Size(), bound)
	}
	return nil
}

// recoveryPayloadFacts is what a tracker's payload.mig says about the
// migration that wrote it: what it touches, and which task and unit own it.
// The identity is what lets a reader ask whether that task is still live
// before it reclaims the directory.
type recoveryPayloadFacts struct {
	properties    []string
	migrationType ReindexMigrationType
	taskID        string
	taskVersion   uint64
	unitID        string
}

// readRecoveryPayloadFacts reads them from a migration tracker dir (see
// ShardReindexTaskGeneric.SaveRecoveryPayload). The error keeps a missing
// payload (os.IsNotExist) distinguishable from an unreadable one: only the
// former reads as "recorded nothing" ([migrationDirScope.inScopeFailingOpen]);
// the latter fails the unloaded-shard gate open.
func readRecoveryPayloadFacts(migDir string) (recoveryPayloadFacts, error) {
	path := filepath.Join(migDir, reindexRecoveryPayloadFile)
	if err := refuseOversizedRecoveryPayload(path, maxRecoveryPayloadBytes); err != nil {
		return recoveryPayloadFacts{}, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return recoveryPayloadFacts{}, err
	}
	// Anonymous shape: only the fields we need. Avoids depending on
	// ReindexTaskPayload here (no import cycle risk, but keeping shard
	// init lean).
	var rec struct {
		TaskID      string `json:"taskID"`
		TaskVersion uint64 `json:"taskVersion"`
		UnitID      string `json:"unitID"`
		Payload     struct {
			Properties    []string             `json:"properties"`
			MigrationType ReindexMigrationType `json:"migrationType"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(data, &rec); err != nil {
		return recoveryPayloadFacts{}, fmt.Errorf("parse %s: %w", reindexRecoveryPayloadFile, err)
	}
	return recoveryPayloadFacts{
		properties:    rec.Payload.Properties,
		migrationType: rec.Payload.MigrationType,
		taskID:        rec.TaskID,
		taskVersion:   rec.TaskVersion,
		unitID:        rec.UnitID,
	}, nil
}
