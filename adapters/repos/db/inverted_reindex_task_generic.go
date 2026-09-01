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

// Runtime reindex — phase contract
// ================================
//
// The runtime swap path (semantic migrations through OnGroupCompleted,
// and non-semantic migrations through OnAfterLsmInitAsync) is partitioned
// into THREE phases. Maintainers MUST preserve the boundary between them
// — drift between phases causes the per-shard "FINALIZING window"
// misalignment between bucket content and the query analyzer at
// production scale, surfaced as
// https://github.com/weaviate/0-weaviate-issues/issues/216 and fixed by
// the prep/atomic/defer split this file implements.
//
// Phase 1 — PREP (background, NOT inside the overlay window)
// ----------------------------------------------------------
// Implemented by [ShardReindexTaskGeneric.runtimePrepare].
//
// Allowed work: heavy disk I/O. FlushAndSwitch on every per-property
// reindex bucket, ShutdownBucket(reindex) to make its segments
// immutable, PrependSegmentsFromBucket(reindex → ingest) per property,
// removeReindexBucketsDirs, and the record write that commits the
// staged data.
//
// Constraints: this phase runs BEFORE the per-shard tokenization
// overlay is set. Queries during this phase see the pre-migration
// bucket content with the pre-migration analyzer — correct.
//
// Phase 2 — ATOMIC SWAP (inside the overlay window; per-shard
// "mixed-state" subwindow MUST stay microseconds)
// ------------------------------------------------------------
// Implemented by [ShardReindexTaskGeneric.runtimeSwap].
//
// 2a — tight loop, MUST stay microseconds: a query hitting a
// not-yet-swapped prop tokenizes new-analyzer input against the old
// bucket. The [onPropSwapped] overlay hook fires per-flip (not once up
// front) so the overlay≠bucket exposure stays one in-memory map write.
//
// 2b — post-atomic inline retirement (slow but correctness-safe):
// oldMainBucket.Shutdown(ctx) + removal of its directory at the handle the
// record names, per property. Runs AFTER every prop has flipped in 2a, so
// it's outside the mixed-state subwindow. Shutdown MUST be inline (not
// deferred): it's the only call that frees the bucket's path from
// GlobalBucketRegistry, and deferring it leaks the path, failing the next
// in-process shard init at this name with ErrBucketAlreadyRegistered.
//
// 2c — post-atomic inline finalize: OnMigrationComplete +
// trimOlderGenerationsLocked. These run OUTSIDE the mixed-state
// subwindow.
//
//   - OnMigrationComplete is a per-strategy hook with significant
//     drift between implementations. Some are no-ops (semantic
//     change-tokenization, enable-filterable, enable-searchable —
//     their cluster-wide schema flip is in OnTaskCompleted).
//     Others mutate in-memory local state that the query path
//     consults (e.g. FilterableToRangeableStrategy.OnMigrationComplete
//     calls Shard.setRangeableLocallyReady so this shard's queries
//     match the new schema before the RAFT flip propagates).
//     Others issue RAFT calls inline
//     (FilterableToRangeableStrategy.applyPerPropertySchemaUpdate,
//     MapToBlockmaxStrategy.updateToBlockMaxInvertedIndexConfig).
//     RAFT calls in this position are slow (100s of ms) but
//     correctness-safe — the overlay covers the entire RunSwapOnShard
//     for change-tokenization, and BlockMax has no analyzer overlay
//     because the format change is internal. See the godoc on
//     [MigrationStrategy.OnMigrationComplete] for the per-strategy
//     contract.
//
// Phase 3 — DEFERRED LIVE-BUCKET RENAME (next process startup, BEFORE
// LSM init reloads any buckets)
// ---------------------------------------------------------------------
// Implemented by [migrationReconciler.Reconcile], which promotes every
// record whose flip decision is durable by renaming its staged
// directory onto the canonical name it records.
//
// Why deferred: the ingest bucket is the LIVE post-swap main bucket.
// Its mmaps are open, its segment registry holds the ingest_<N> path
// as its dir. Renaming that dir while the bucket is in-memory would
// corrupt the segment registry and any subsequent write that
// resolves paths from the bucket's stored dir. At next startup,
// before LSM init touches the canonical name, no bucket is mmapping
// anything — the rename is safe.
//
// Crash safety: the flip decision is fsynced ahead of the first pointer
// flip, so a record short of it proves no flip was ever decided.
// [ShardReindexTaskGeneric.RunSwapOnShard] dispatches on the record to
// resume; a crash anywhere in or after 2b resolves at the next shard load,
// when reconciliation finishes the same directory work before any bucket
// opens.
//
// Atomic-phase regression guard: a unit test must fail if
// SwapBucketPointer is preceded by any disk-I/O or compaction-wait
// op inside Phase 2 — the "atomic" subwindow has to stay
// microseconds for queries at production scale, since any inline
// disk work bloats the window where a query can read post-swap
// bucket content with the pre-swap analyzer.

package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/diskio"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// ShardReindexTaskGeneric is a strategy-parameterized reindex task. All
// lifecycle logic (state machine, merge/swap, object iteration, progress
// tracking) lives here, with strategy-specific behavior delegated to a
// MigrationStrategy.
//
// See the file-level phase-contract godoc above for the prep / atomic
// swap / deferred-rename invariants that every code path in this file
// must preserve.
type ShardReindexTaskGeneric struct {
	name     string
	logger   logrus.FieldLogger
	strategy MigrationStrategy

	taskID               string
	taskVersion          uint64
	unitID               string
	migrationType        ReindexMigrationType
	targetTokenization   string
	originalTokenization string

	keyParser            indexKeyParser
	objectsIteratorAsync objectsIteratorAsync
	config               reindexTaskConfig

	// skipSwapOnFinish, when true, causes the reindex iteration to stop once
	// the rebuild is recorded complete, without proceeding to runtimeSwap().
	// Used by RunReindexOnlyOnShard for barrier semantics: all shards must
	// finish reindexing before any shard swaps.
	//
	// Atomic because the field is written by runShardLifecycle (from the
	// StartTask worker goroutine) and read by OnAfterLsmInitAsync's loop
	// body (which can run on a separate goroutine when the cached task
	// instance is later invoked from OnGroupCompleted's swap phase).
	skipSwapOnFinish atomic.Bool

	// progressCallback, when set, is called from the iteration loop with the
	// current fraction-complete (clamped 0..1). It lets the DTM-side recorder
	// surface live unit progress to the GET /indexes and GET /tasks endpoints.
	// Set via SetProgressCallback before RunOnShard / RunReindexOnlyOnShard;
	// concurrent reads from the iteration goroutine happen-after the set
	// because RunOnShard is called after the setter on the same goroutine.
	progressCallback func(float32)

	// processOneSwapPropFn is the dispatch function for runtimeSwap's
	// Phase 2a per-prop body. Defaults to the [processOneSwapProp]
	// method in [NewShardReindexTaskGeneric]; tests substitute a
	// wrapper for fault injection or observation. No test-only branch
	// runs in production — the field is always set.
	processOneSwapPropFn func(ctx context.Context, store *lsmkv.Store, propIdx int, propName string) (*lsmkv.Bucket, error)

	// registerDoubleWriteCallbacksFn dispatches OnAfterLsmInit's ingest-window
	// double-write registration. Defaults to [registerDoubleWriteCallbacks] in
	// [NewShardReindexTaskGeneric]; tests wrap it to drive a write into the
	// arm→record window the production ordering must not lose
	// (weaviate/weaviate#11688). Always set — no test-only branch runs in
	// production.
	registerDoubleWriteCallbacksFn func(shard *Shard, props []string,
		bucketNamer func(string) string) (func(), error)

	// onPropSwapped runs inside the Phase 2a tight loop right after each
	// bucket-pointer flip, so a query never observes overlay≠bucket for
	// longer than one in-memory map write. Runs on the swap goroutine, so
	// SetTokenizationOverlay's own lock is enough. Wired only for
	// tokenization-changing migrations.
	//
	// Only the recovery/resume path still uses this; the live Phase-2a loop
	// routes through swapPropAtomic when wired.
	onPropSwapped func(propName string)

	// swapPropAtomic, when non-nil, runs the Phase-2a per-prop flip AND the
	// overlay set as ONE critical section (Shard.SwapBucketAndSetOverlay).
	// nil = legacy two-step flip + onPropSwapped. Returns the displaced
	// bucket for Phase-2b, or (nil, nil) on an already-swapped prop.
	swapPropAtomic func(ctx context.Context, store *lsmkv.Store, propIdx int, propName string) (*lsmkv.Bucket, error)

	// closingGuard reports whether the shard's index is still open. Supplied
	// at construction and never reassigned; production wires
	// [defaultIndexClosingGuard] everywhere. Never nil.
	closingGuard indexClosingGuard

	// rebuildRangeableRepFn dispatches [rebuildRangeableInMemoryReps]'s
	// per-prop bucket rebuild; defaults to
	// [lsmkv.Bucket.RebuildRangeableSegmentInMemory], tests substitute a
	// failure-injecting wrapper. Always set - no test-only branch runs in
	// production.
	rebuildRangeableRepFn func(ctx context.Context, b *lsmkv.Bucket) error
}

// indexClosingGuard reports context.Canceled once the shard's index is
// closing or closed, and nil while it is still open.
type indexClosingGuard func(shard ShardLike) error

// defaultIndexClosingGuard is the guard every production task runs: it takes
// the index's close read-lock, so a drop that starts after the check cannot
// complete until the caller returns.
func defaultIndexClosingGuard(shard ShardLike) error {
	return shard.Index().withCloseRLockGuard(func() error { return nil })
}

// NewShardReindexTaskGeneric creates a new generic reindex task. closingGuard
// must not be nil; production callers pass [defaultIndexClosingGuard].
func NewShardReindexTaskGeneric(name string, logger logrus.FieldLogger,
	strategy MigrationStrategy, config reindexTaskConfig,
	keyParser indexKeyParser, objectsIteratorAsync objectsIteratorAsync,
	closingGuard indexClosingGuard,
) *ShardReindexTaskGeneric {
	logger = logger.WithField("task", name)

	logger.WithField("config", fmt.Sprintf("%+v", config)).Debug("task created")

	t := &ShardReindexTaskGeneric{
		name:                 name,
		logger:               logger,
		strategy:             strategy,
		keyParser:            keyParser,
		objectsIteratorAsync: objectsIteratorAsync,
		config:               config,
		closingGuard:         closingGuard,
	}
	t.processOneSwapPropFn = t.processOneSwapProp
	t.registerDoubleWriteCallbacksFn = t.registerDoubleWriteCallbacks
	t.rebuildRangeableRepFn = func(ctx context.Context, b *lsmkv.Bucket) error {
		return b.RebuildRangeableSegmentInMemory(ctx)
	}
	return t
}

func (t *ShardReindexTaskGeneric) processOneSwapProp(ctx context.Context, store *lsmkv.Store, _ int, propName string) (*lsmkv.Bucket, error) {
	ingestName := t.ingestBucketName(propName)
	mainName := t.strategy.SourceBucketName(propName)

	if main := store.Bucket(mainName); main != nil && filepath.Base(main.GetDir()) == ingestName {
		return nil, nil
	}

	oldMainBucket, err := store.SwapBucketPointer(ctx, mainName, ingestName)
	if err != nil {
		return nil, fmt.Errorf("swapping bucket pointer %q <- %q: %w", mainName, ingestName, err)
	}
	return oldMainBucket, nil
}

func (t *ShardReindexTaskGeneric) Name() string {
	return t.name
}

// SetProgressCallback installs a fn the iteration loop will call with the
// current fraction-complete (0..1) every checkProcessingEveryNoObjects
// iterations. Must be called before RunOnShard / RunReindexOnlyOnShard.
// The throttled-recorder wrapper layered above is responsible for keeping
// the wire traffic bounded.
func (t *ShardReindexTaskGeneric) SetProgressCallback(fn func(float32)) {
	t.progressCallback = fn
}

// migrationPath returns the absolute path to the migration directory for
// this task on the given shard LSM path.
func (t *ShardReindexTaskGeneric) migrationPath(lsmPath string) string {
	return filepath.Join(lsmPath, ".migrations", t.strategy.MigrationDirName())
}

// reindexRecoveryPayloadFile is the filename of the on-disk JSON record
// describing the in-flight reindex task. Written by [ReindexProvider]
// before the reindex iteration starts; read at startup by
// [DiscoverInFlightReindexTasks] to rebuild task instances for shards that
// had a reindex in progress when the node went down. It lives in the
// migration's own sub-directory so it is removed alongside it on
// reset/cleanup.
const reindexRecoveryPayloadFile = "payload.mig"

// SaveRecoveryPayload writes the given JSON-encoded recovery record to
// payload.mig inside the migration directory of this task on the given
// shard LSM path. It is idempotent: if the file already exists with the
// same content, and is small enough to compare, the call is a no-op;
// otherwise it is overwritten.
// Callers are expected to ensure the migration directory exists; this
// function will [os.MkdirAll] it just in case to keep startup recovery
// robust against partial state.
func (t *ShardReindexTaskGeneric) SaveRecoveryPayload(lsmPath string, payload []byte) error {
	migDir := t.migrationPath(lsmPath)
	if err := os.MkdirAll(migDir, 0o777); err != nil {
		return fmt.Errorf("mkdir migration dir %q: %w", migDir, err)
	}
	target := filepath.Join(migDir, reindexRecoveryPayloadFile)
	if err := refuseOversizedRecoveryPayload(target, maxRecoveryPayloadBytes); err == nil {
		if existing, err := os.ReadFile(target); err == nil && bytes.Equal(existing, payload) {
			return nil
		}
	}
	return os.WriteFile(target, payload, 0o600)
}

// RunOnShard runs the full reindex lifecycle on a live shard: OnAfterLsmInit
// followed by repeated OnAfterLsmInitAsync calls until the task is complete.
// This is intended for debug/runtime-triggered migrations on an already-running shard.
// The shard may be a *Shard or *LazyLoadShard.
func (t *ShardReindexTaskGeneric) RunOnShard(ctx context.Context, shard ShardLike) error {
	return t.runShardLifecycle(ctx, shard, false)
}

// RunReindexOnlyOnShard runs the reindex iteration only — no merge, no swap.
// After this returns, the shard has:
//   - ingest bucket with double-written data
//   - reindex bucket with reindexed data
//   - main bucket unchanged (still serving queries)
//   - the record reports the rebuild complete
//
// This is used for barrier semantics: all shards must finish reindexing
// before any shard swaps. Call RunSwapOnShard after all shards are done.
//
// The task instance registers double-write callbacks that MUST remain active
// until RunSwapOnShard completes — callers must use the same task instance for
// both calls.
func (t *ShardReindexTaskGeneric) RunReindexOnlyOnShard(ctx context.Context, shard ShardLike) error {
	return t.runShardLifecycle(ctx, shard, true)
}

func (t *ShardReindexTaskGeneric) runShardLifecycle(ctx context.Context, shard ShardLike, skipSwap bool) error {
	if skipSwap {
		t.skipSwapOnFinish.Store(true)
		defer t.skipSwapOnFinish.Store(false)
	}

	concreteShard, err := unwrapShard(ctx, shard)
	if err != nil {
		return fmt.Errorf("unwrapping shard %q: %w", shard.Name(), err)
	}

	// context.Canceled means the index is closing — propagate unwrapped so
	// callers treat it as a clean stop, consistent with OnAfterLsmInitAsync.
	if err := t.onAfterLsmInitGuarded(ctx, concreteShard); err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}
		return fmt.Errorf("after LSM init: %w", err)
	}

	for {
		rerunAt, _, err := t.OnAfterLsmInitAsync(ctx, shard)
		if err != nil {
			return fmt.Errorf("after async LSM init: %w", err)
		}
		if rerunAt.IsZero() {
			return nil
		}
	}
}

// RunPrepareOnShard runs the disk-I/O-heavy prep phase between
// RunReindexOnlyOnShard and RunSwapOnShard.
//
// Preconditions:
//   - MUST have completed RunReindexOnlyOnShard, or be re-entering on a
//     record that already commits the staged data, in which case this is an
//     idempotent no-op.
//
// Performs, per property:
//   - reindexBucket.FlushAndSwitch()  // memtable → immutable segments
//   - store.ShutdownBucket(reindexName)  // waits for compaction to
//     drain — this is the load-bearing slow step that #216 (the
//     "atomic phase actually atomic" design contract) requires
//     to live outside the per-shard atomic window
//   - ingestBucket.PrependSegmentsFromBucket(...)  // segment copy
//
// Then commits the staged data in the record and removes the reindex
// bucket dirs.
//
// Idempotent: a record that already commits the staged data returns
// nil. Safe to call repeatedly from rehydrate flows.
//
// MUST be called BEFORE the per-shard tokenization overlay is set
// by [reindex_provider.OnGroupCompleted]. Setting the overlay
// before prep completes would expose the very gap the overlay was
// supposed to close — query input would tokenize as NEW against the
// still-OLD bucket while prep is doing seconds of disk I/O.
//
// Double-write callbacks registered during reindex MUST remain
// active across this call (they fire on writes to MAIN to mirror
// into INGEST; MAIN is still serving queries with OLD data while
// prep runs, and the mirror keeps the new ingest segments
// consistent with ongoing writes). Callbacks are disabled only at
// the end of [runtimeSwap] after the atomic pointer flip.
func (t *ShardReindexTaskGeneric) RunPrepareOnShard(ctx context.Context, shard ShardLike) error {
	entry, err := t.enterDTMPhase(ctx, shard, "RunPrepareOnShard")
	if err != nil {
		return err
	}
	concreteShard, logger := entry.shard, entry.logger

	if entry.rec.StagedDataComplete() {
		logger.Debug("RunPrepareOnShard: already merged on disk; no-op")
		return nil
	}

	props := entry.rec.Subject().Properties()
	if len(props) == 0 {
		return fmt.Errorf("no props found for prep on shard %q", concreteShard.Name())
	}

	if err := t.ensureReindexBucketsLoadedForSwap(ctx, logger, concreteShard, props); err != nil {
		return fmt.Errorf("ensure buckets loaded: %w", err)
	}

	return t.runtimePrepare(ctx, logger, shard, props)
}

// dtmPhaseEntry is the shared entry state of the DTM-scheduler-driven phase
// callbacks (RunPrepareOnShard / RunSwapOnShard).
type dtmPhaseEntry struct {
	shard  *Shard
	logger logrus.FieldLogger
	rec    MigrationRecord
}

func (t *ShardReindexTaskGeneric) enterDTMPhase(ctx context.Context, shard ShardLike, method string) (*dtmPhaseEntry, error) {
	concreteShard, err := unwrapShard(ctx, shard)
	if err != nil {
		return nil, fmt.Errorf("unwrapping shard %q: %w", shard.Name(), err)
	}

	logger := t.logger.WithFields(map[string]any{
		"collection": concreteShard.Index().Config.ClassName.String(),
		"shard":      concreteShard.Name(),
		"method":     method,
	})

	rec, ok := t.migrationRecord(concreteShard)
	if !ok {
		// Shouldn't happen via OnGroupCompleted (units are node-assigned).
		return nil, fmt.Errorf("shard %q has no migration record — no in-flight migration on disk", concreteShard.Name())
	}

	// Must precede the iteration-resume ladder below: committed implies the
	// iteration already completed.
	if rec.StagedDataComplete() {
		return &dtmPhaseEntry{shard: concreteShard, logger: logger, rec: rec}, nil
	}

	if !rec.IterationComplete() {
		logger.Info(method + ": rebuild not yet complete; resuming iteration")
		if err := t.RunReindexOnlyOnShard(ctx, shard); err != nil {
			return nil, fmt.Errorf("resume iteration: %w", err)
		}
		if rec, ok = t.migrationRecord(concreteShard); !ok || !rec.IterationComplete() {
			return nil, fmt.Errorf("shard %q: iteration resume returned with the rebuild still incomplete", concreteShard.Name())
		}
	}

	return &dtmPhaseEntry{shard: concreteShard, logger: logger, rec: rec}, nil
}

// RunSwapOnShard runs the swap + OnMigrationComplete phase, dispatching on the
// record so a node that restarted inside the FINALIZING window resumes from
// whatever state its record last reached instead of re-running the whole path.
func (t *ShardReindexTaskGeneric) RunSwapOnShard(ctx context.Context, shard ShardLike) error {
	entry, err := t.enterDTMPhase(ctx, shard, "RunSwapOnShard")
	if err != nil {
		return err
	}
	concreteShard, logger := entry.shard, entry.logger

	props := entry.rec.Subject().Properties()
	if len(props) == 0 {
		return fmt.Errorf("no props found for swap on shard %q", concreteShard.Name())
	}

	switch {
	case entry.rec.FlipDecided():
		logger.WithField("props", props).Info("RunSwapOnShard: flip already decided; running OnMigrationComplete only")
		if err := t.requireCanonicalHoldsMigratedData(shard, entry.rec); err != nil {
			return err
		}
		return t.finalizeMigrationAfterRecovery(ctx, logger, shard, props)

	case entry.rec.StagedDataComplete():
		logger.WithField("props", props).Info("RunSwapOnShard: resuming from merged state, in-memory atomic swap")
		if err := t.ensureReindexBucketsLoadedForSwap(ctx, logger, concreteShard, props); err != nil {
			return fmt.Errorf("ensure buckets loaded: %w", err)
		}
		return t.runtimeSwap(ctx, logger, shard, props)
	}

	logger.WithField("props", props).Info("starting prep+swap phase (caller did not invoke RunPrepareOnShard separately)")

	if err := t.ensureReindexBucketsLoadedForSwap(ctx, logger, concreteShard, props); err != nil {
		return fmt.Errorf("ensure buckets loaded: %w", err)
	}

	if err := t.runtimePrepare(ctx, logger, shard, props); err != nil {
		return fmt.Errorf("runtime prepare: %w", err)
	}

	if err := t.runtimeSwap(ctx, logger, shard, props); err != nil {
		return fmt.Errorf("runtime swap: %w", err)
	}

	return nil
}

// ensureReindexBucketsLoadedForSwap defensively loads any reindex or
// ingest buckets that are missing from the in-memory store but whose
// directories still exist on disk. This protects the pre-prepend
// runtimeSwap path on the rehydrate flow from a class of state-divergence
// races between in-memory bucket state and on-disk reindex state:
//
//   - A previous in-process runtimePrepare was interrupted mid-flight
//     by ctx.Canceled (graceful shutdown). The interrupted
//     ShutdownBucket may have removed the reindex bucket from the
//     store's bucket map without advancing the record, and the
//     cancellation can leave compaction callbacks unregistered
//     partway through the unhook sequence.
//   - On restart, the shard-registered recovery task's OnAfterLsmInit
//     (see [shardReindexerV3RecoveryOnly]) is the only re-load hook.
//     If for any reason the bucket name lookup in
//     [runtimePrepare]'s first iteration misses (lsm store re-init,
//     concurrent bucket shutdown, cached-task vs fresh-task pointer
//     differences after the rehydrate path's [createReindexTasks]),
//     the prepare fails with "reindex bucket not found" before any
//     side effect, and the post-completion ack records success=false
//     for the whole task — flipping the cluster to FAILED while
//     other replicas have already completed the swap.
//
// CreateOrLoadBucket is idempotent, so calling it when the bucket is
// already loaded is harmless. We narrow the call to props whose dirs
// exist on disk to avoid creating empty buckets in a degenerate state
// where the dir is genuinely gone (which would mask a real bug).
func (t *ShardReindexTaskGeneric) ensureReindexBucketsLoadedForSwap(
	ctx context.Context, logger logrus.FieldLogger, shard *Shard, props []string,
) error {
	store := shard.Store()
	lsmPath := shard.pathLSM()

	var missingReindex, missingIngest []string
	for _, propName := range props {
		reindexName := t.reindexBucketName(propName)
		if store.Bucket(reindexName) == nil {
			there, err := diskio.DirExists(filepath.Join(lsmPath, reindexName))
			if err != nil {
				return fmt.Errorf("probe reindex bucket dir for %q: %w", propName, err)
			}
			if there {
				missingReindex = append(missingReindex, propName)
			}
		}
		ingestName := t.ingestBucketName(propName)
		if store.Bucket(ingestName) == nil {
			there, err := diskio.DirExists(filepath.Join(lsmPath, ingestName))
			if err != nil {
				return fmt.Errorf("probe ingest bucket dir for %q: %w", propName, err)
			}
			if there {
				missingIngest = append(missingIngest, propName)
			}
		}
	}

	if len(missingReindex) > 0 {
		logger.WithField("props", missingReindex).
			Warn("reindex buckets not in store but dirs exist; defensively loading before runtime swap")
		if err := t.loadReindexBuckets(ctx, logger, shard, missingReindex); err != nil {
			return fmt.Errorf("load reindex buckets: %w", err)
		}
	}
	if len(missingIngest) > 0 {
		logger.WithField("props", missingIngest).
			Warn("ingest buckets not in store but dirs exist; defensively loading before runtime swap")
		// keepLevelCompaction=false, keepTombstones=false: at this
		// point (pre-prepend, mid-runtimeSwap) the standard
		// post-merge ingest options apply.
		if err := t.loadIngestBuckets(ctx, logger, shard, missingIngest, false, false); err != nil {
			return fmt.Errorf("load ingest buckets: %w", err)
		}
	}
	return nil
}

func (t *ShardReindexTaskGeneric) stagedPropsStillOnDisk(logger logrus.FieldLogger,
	shard *Shard, subject MigrationSubject, props []string,
) ([]string, error) {
	kept := make([]string, 0, len(props))
	var promoted, misnamed []string
	for _, propName := range props {
		staged := subject.Props[propName].Staged
		if staged == "" || staged != t.ingestBucketName(propName) {
			misnamed = append(misnamed, propName)
			continue
		}
		there, err := diskio.DirExists(filepath.Join(shard.pathLSM(), staged))
		if err != nil {
			return nil, fmt.Errorf("probe staged dir for %q: %w", propName, err)
		}
		if there {
			kept = append(kept, propName)
			continue
		}
		promoted = append(promoted, propName)
	}
	if len(promoted) > 0 {
		logger.WithField("property_count", len(promoted)).
			WithField("props", migrationReportedNames(promoted)).Info(
			"staged directories are gone, so the canonical name holds these properties already; not re-creating them")
	}
	if len(misnamed) > 0 {
		logger.WithField("property_count", len(misnamed)).
			WithField("props", migrationReportedNames(misnamed)).Warn(
			"the record names a staged directory this task would not open, so it belongs to another generation; opening neither name")
	}
	return kept, nil
}

// requireCanonicalHoldsMigratedData refuses to commit a migration's schema
// effect unless the canonical name really holds this migration's data.
//
// The flip decision is written before the first pointer moves (see
// [migrationRecordQuestions.FlipDecided]), so it proves the flip was DECIDED,
// never that it ran. Two things put the data under the canonical name, and
// they answer in different places:
//
//   - an in-process flip points the canonical name at the staged directory,
//     which the open bucket's own directory says;
//   - a promotion renamed that directory onto the canonical name, which only
//     disk says. An enable-* migration commits the very schema flag that
//     decides whether shard init opens that bucket, so requiring an open one
//     here would make the retry uncommittable exactly when it is needed.
//
// Promoted means every property is either promoted or superseded, and both
// leave the same disk shape: the canonical directory present, the staged one
// gone. A missing canonical directory is one a later load deleted, and the
// schema effect must stay off an index nothing serves.
//
// A superseded property therefore commits its schema effect over a successor's
// data. That is only safe because [typesConflictReason] refuses a new task
// overlapping an in-flight one's properties, so a successor can only exist
// once this migration's task is terminal — and a terminal task never re-enters
// here. Without that, an enable-searchable successor targeting a different
// tokenization would have its predecessor commit the wrong one.
func (t *ShardReindexTaskGeneric) requireCanonicalHoldsMigratedData(shard ShardLike, rec MigrationRecord) error {
	subject := rec.Subject()
	for _, propName := range subject.Properties() {
		if bucket := shard.Store().Bucket(t.strategy.SourceBucketName(propName)); bucket != nil {
			serving := filepath.Base(bucket.GetDir())
			if serving == subject.Props[propName].Staged {
				continue
			}
			if serving != subject.Props[propName].Canonical {
				return fmt.Errorf("stale migration state on shard %q: the record claims property %q is complete, but its bucket serves %q, which is neither this migration's staged directory %q nor its canonical one %q; refusing to report success",
					shard.Name(), propName, serving, subject.Props[propName].Staged, subject.Props[propName].Canonical)
			}
		}
		if rec.State() != MigrationStatePromoted {
			return fmt.Errorf("stale migration state on shard %q: the record claims property %q is complete, but the canonical name does not serve this migration's staged directory and the migration is only %q, not promoted; refusing to report success",
				shard.Name(), propName, rec.State())
		}
		canonicalThere, err := diskio.DirExists(filepath.Join(shard.pathLSM(), subject.Props[propName].Canonical))
		if err != nil {
			return fmt.Errorf("probe canonical dir for %q: %w", propName, err)
		}
		if !canonicalThere {
			return fmt.Errorf("stale migration state on shard %q: the record claims property %q is promoted, but its canonical directory %q is gone; refusing to report success",
				shard.Name(), propName, subject.Props[propName].Canonical)
		}
		stagedThere, err := diskio.DirExists(filepath.Join(shard.pathLSM(), subject.Props[propName].Staged))
		if err != nil {
			return fmt.Errorf("probe staged dir for %q: %w", propName, err)
		}
		if stagedThere {
			return fmt.Errorf("stale migration state on shard %q: the record claims property %q is promoted, but its staged directory %q is still there, so the rename that promotion is made of never ran; refusing to report success",
				shard.Name(), propName, subject.Props[propName].Staged)
		}
	}
	return nil
}

func (t *ShardReindexTaskGeneric) ensureCanonicalBucketsOpen(ctx context.Context,
	shard ShardLike, props []string,
) error {
	concrete, err := unwrapShard(ctx, shard)
	if err != nil {
		return fmt.Errorf("open the canonical buckets this completion advertises: %w", err)
	}
	closed := make([]string, 0, len(props))
	for _, propName := range props {
		if concrete.store.Bucket(t.strategy.SourceBucketName(propName)) == nil {
			closed = append(closed, propName)
		}
	}
	if len(closed) == 0 {
		return nil
	}
	t.strategy.PreReindexHook(concrete, closed)
	for _, propName := range closed {
		name := t.strategy.SourceBucketName(propName)
		if concrete.store.Bucket(name) == nil {
			return fmt.Errorf("refusing to report migration complete for property %q: its canonical bucket %q "+
				"is not open, so the schema effect would advertise an index nothing serves", propName, name)
		}
	}
	return nil
}

// finalizeMigrationAfterRecovery runs the strategy's OnMigrationComplete
// hook and trims older on-disk generations. This is the rehydrate-path
// equivalent of runtimeSwap's final two steps,
// invoked by the recovery branches in [RunSwapOnShard] which don't go
// through runtimeSwap.
//
// Best-effort on trim — failures are logged, not returned, matching
// the trim policy at the end of runtimeSwap.
func (t *ShardReindexTaskGeneric) finalizeMigrationAfterRecovery(
	ctx context.Context, logger logrus.FieldLogger, shard ShardLike, props []string,
) error {
	if err := t.ensureCanonicalBucketsOpen(ctx, shard, props); err != nil {
		return err
	}
	if err := t.rebuildRangeableInMemoryReps(ctx, logger, shard, props); err != nil {
		return err
	}
	if err := t.strategy.OnMigrationComplete(ctx, shard); err != nil {
		return fmt.Errorf("on migration complete: %w", err)
	}
	if concrete, err := unwrapShard(ctx, shard); err == nil {
		concrete.migrations().RetireSuperseded(ctx)
	} else {
		logger.Warnf("RunSwapOnShard: cannot retire superseded migrations: %v", err)
	}
	t.trimOlderGenerationsLocked(logger, shard, props)
	logger.Info("RunSwapOnShard: recovery path complete")
	return nil
}

// rebuildRangeableInMemoryReps restores the INDEX_RANGEABLE_IN_MEMORY
// contract for buckets promoted by a reindex swap: ingest buckets open
// without an in-memory rep, so without this they'd serve range reads from
// disk until the next open. Idempotent.
//
// A rebuild failure degrades to disk serving (WARN-and-continue) instead of
// failing the migration: data work (prepend, swap) has already
// committed, disk serving is always correct, and only the in-memory
// acceleration is deferred to next restart. Every degrade still logs at
// ERROR and increments a metric so it stays visible.
//
// context.Canceled is the one error this function still returns, so
// [runPerUnitPhase]'s errors.Is(context.Canceled) check keeps routing to
// the transient ack path instead of a permanent FAILED or false FINISHED.
func (t *ShardReindexTaskGeneric) rebuildRangeableInMemoryReps(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, props []string,
) error {
	if t.strategy.TargetStrategy() != lsmkv.StrategyRoaringSetRange ||
		!shard.Index().Config.IndexRangeableInMemory {
		return nil
	}

	store := shard.Store()
	className := shard.Index().Config.ClassName.String()
	shardName := shard.Name()
	for _, propName := range props {
		bucketName := t.strategy.SourceBucketName(propName)

		bucket := store.Bucket(bucketName)
		if bucket == nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				// Missing buckets have legitimate transient causes (shutdown
				// draining, a property dropped mid-migration); only treat this
				// as a hard failure once we know the caller isn't shutting down.
				return fmt.Errorf("rangeable in-memory rebuild aborted for property %q: %w", propName, ctxErr)
			}
			err := fmt.Errorf(
				"rangeable index for property %q could not be activated for in-memory "+
					"serving: bucket %q not found post-swap, rebuild the index to repair it",
				propName, bucketName,
			)
			logger.WithField("bucket", bucketName).Errorf("rangeable in-memory rebuild: %v", err)
			monitoring.GetMetrics().IncRangeableInMemoryRebuildDegraded(className, shardName, propName)
			continue
		}

		started := time.Now()
		if err := t.rebuildRangeableRepFn(ctx, bucket); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				// Wrap ctxErr too: it guarantees errors.Is(context.Canceled)
				// works even if the underlying err doesn't itself wrap
				// ctx.Err(); err is kept for diagnostics.
				return fmt.Errorf("rangeable in-memory rebuild aborted for property %q: %w: %w", propName, ctxErr, err)
			}
			wrapped := fmt.Errorf(
				"rangeable index for property %q built and data intact, but could not be "+
					"activated for in-memory serving: %w, rebuild the index to repair it",
				propName, err,
			)
			logger.WithField("bucket", bucketName).Errorf("rangeable in-memory rebuild: %v", wrapped)
			monitoring.GetMetrics().IncRangeableInMemoryRebuildDegraded(className, shardName, propName)
			continue
		}
		if bucket.RangeableServesFromMemory() {
			logger.WithFields(logrus.Fields{
				"bucket": bucketName,
				"took":   time.Since(started).String(),
			}).Info("rangeable in-memory index built at migration finalize; serving range queries from memory")
		}
	}
	return nil
}

// unwrapShard extracts the concrete *Shard from a ShardLike,
// handling both *Shard and *LazyLoadShard.
func unwrapShard(ctx context.Context, shard ShardLike) (*Shard, error) {
	switch s := shard.(type) {
	case *Shard:
		return s, nil
	case *LazyLoadShard:
		return s.Unwrap(ctx)
	default:
		return nil, fmt.Errorf("unsupported shard type %T", shard)
	}
}

// logPhase builds the per-invocation logger and the finished-lifecycle
// callback shared by the LSM-init hooks. context.Canceled logs at Debug —
// the scheduler already logs the cancelled unit.
func (t *ShardReindexTaskGeneric) logPhase(collectionName, shardName, method string,
) (logrus.FieldLogger, func(started time.Time, err error)) {
	logger := t.logger.WithFields(map[string]any{
		"collection": collectionName,
		"shard":      shardName,
		"method":     method,
	})
	logger.Info("starting")
	done := func(started time.Time, err error) {
		logger := logger.WithField("took", time.Since(started))
		switch {
		case err == nil:
			logger.Info("finished")
		case errors.Is(err, context.Canceled):
			logger.Debugf("finished after cancellation: %v", err)
		default:
			logger.Errorf("finished with error: %v", err)
		}
	}
	return logger, done
}

func (t *ShardReindexTaskGeneric) onAfterLsmInitGuarded(ctx context.Context, shard *Shard) error {
	if err := t.closingGuard(shard); err != nil {
		t.logger.Debug("index is closing, stopping after-LSM-init hook")
		return err
	}
	return t.OnAfterLsmInit(ctx, shard)
}

// OnAfterLsmInit is the shard-init entry into the after-LSM-init hook. It runs
// no closing check: some NewShard routes already hold closeLock.RLock and
// sync.RWMutex is not reentrant. DTM callers MUST use onAfterLsmInitGuarded.
func (t *ShardReindexTaskGeneric) OnAfterLsmInit(ctx context.Context, shard *Shard) (err error) {
	collectionName := shard.Index().Config.ClassName.String()
	shardName := shard.Name()
	logger, done := t.logPhase(collectionName, shardName, "OnAfterLsmInit")
	defer func(started time.Time) { done(started, err) }(time.Now())

	// skip shard only if not started, otherwise double writes have to be
	// enabled if migration was already started
	isShardSelected := t.isShardSelected(collectionName, shardName)

	rec, hasRecord := t.migrationRecord(shard)
	if !hasRecord && !isShardSelected {
		logger.Debug("different collection/shard selected. nothing to do")
		return nil
	}

	props := t.findPropsToReindex(shard)
	if hasRecord {
		props = rec.Subject().Properties()
	}
	logger.WithField("props", props).Debug("props found")
	if len(props) == 0 {
		logger.Debug("no props found. nothing to do")
		return nil
	}

	if hasRecord && rec.State() == MigrationStatePromoted {
		logger.Debug("migration already promoted. nothing to open")
		return nil
	}

	committed := hasRecord && rec.StagedDataComplete()
	if committed {
		if props, err = t.stagedPropsStillOnDisk(logger, shard, rec.Subject(), props); err != nil {
			return err
		}
		if len(props) == 0 {
			logger.Debug("no staged directory left to open: each is either promoted or named by another generation")
			return nil
		}
		logger.Debug("merged, not swapped. starting ingest buckets")
	} else {
		if hasRecord {
			logger.Debug("resuming reindex buckets from disk")
		} else {
			logger.Debug("not reindexed. starting reindex buckets")
		}
		if err = t.loadReindexBuckets(ctx, logger, shard, props); err != nil {
			err = fmt.Errorf("starting reindex buckets: %w", err)
			return err
		}
	}

	t.strategy.PreReindexHook(shard, props)

	// since reindex bucket will be merged into ingest bucket with reindex segments being before ingest,
	// ingest segments should not be compacted and tombstones should be kept
	if err = t.loadIngestBuckets(ctx, logger, shard, props, !committed, !committed); err != nil {
		err = fmt.Errorf("starting ingest buckets:%w", err)
		return err
	}
	disableJustRegistered, err := t.registerDoubleWriteCallbacksFn(shard, props, t.ingestBucketName)
	if err != nil {
		return err
	}

	if hasRecord {
		return nil
	}

	// Ceiled up 1ms: LastUpdateTimeUnix has ms resolution and the skip predicate
	// is `<`. The horizon is captured after the callbacks are live.
	cutoff := time.Now().Truncate(time.Millisecond).Add(time.Millisecond)
	subject := t.migrationSubject(shard, props, cutoff)
	if err = t.putMigrationRecord(shard, NewMigrationRecordIterating(subject, MigrationCheckpoint{})); err != nil {
		disableJustRegistered()
		err = fmt.Errorf("recording the migration as iterating: %w", err)
		return err
	}

	return nil
}

func (t *ShardReindexTaskGeneric) OnAfterLsmInitAsync(ctx context.Context, shard ShardLike,
) (rerunAt time.Time, reloadShard bool, err error) {
	collectionName := shard.Index().Config.ClassName.String()
	shardName := shard.Name()
	logger, done := t.logPhase(collectionName, shardName, "OnAfterLsmInitAsync")
	defer func(started time.Time) { done(started, err) }(time.Now())

	zerotime := time.Time{}

	if !t.isShardSelected(collectionName, shardName) {
		logger.Debug("different collection/shard selected. nothing to do")
		return zerotime, false, nil
	}

	if err = t.closingGuard(shard); err != nil {
		logger.Debug("index is closing, stopping reindex drain")
		return zerotime, false, err
	}

	rec, hasRecord := t.migrationRecord(shard)
	var props []string
	if hasRecord {
		props = rec.Subject().Properties()
	}

	if hasRecord && rec.FlipDecided() {
		if err = t.requireCanonicalHoldsMigratedData(shard, rec); err != nil {
			return zerotime, false, err
		}
		if err = t.ensureCanonicalBucketsOpen(ctx, shard, props); err != nil {
			return zerotime, false, err
		}
		// Same ordering contract as runtimeSwap (see there for reasoning):
		// this re-entry branch must recheck the rebuild too, or a retry
		// could flip the schema without it ever succeeding.
		if err = t.rebuildRangeableInMemoryReps(ctx, logger, shard, props); err != nil {
			return zerotime, false, err
		}
		err = t.strategy.OnMigrationComplete(ctx, shard)
		if err != nil {
			err = fmt.Errorf("updating inverted index config: %w", err)
		}
		return zerotime, false, err
	}

	if len(props) == 0 {
		logger.Debug("no props read. nothing to do")
		return zerotime, false, nil
	}

	iterating, ok := rec.(MigrationRecordIterating)
	if !ok {
		logger.Debug("rebuild already complete. nothing to do")
		return zerotime, false, nil
	}

	subject := iterating.Subject()
	reindexStarted := subject.IterationCutoff
	lastStoredKey := t.keyParser.FromBytes(iterating.Checkpoint().LastProcessedKey)

	logger.WithFields(map[string]any{
		"last_stored_key": lastStoredKey,
		"reindex_started": reindexStarted,
	}).Debug("reindexing")

	if err = ctx.Err(); err != nil {
		err = fmt.Errorf("context check (1): %w / %w", err, context.Cause(ctx))
		return zerotime, false, err
	}

	processedCount := 0
	indexedCount := 0
	lastProcessedKey := lastStoredKey.Clone()

	// Total-object estimate for live progress reporting. ObjectCountAsync is
	// the cheap, eventually-consistent count; an exact count would require a
	// full bucket scan, which would itself dominate the reindex runtime on
	// large shards. A nil/error result here just disables progress emission —
	// the reindex still completes correctly, just without UI feedback. The
	// estimate can drift if writes land during the iteration (the double-
	// write callbacks add to ingest, not objects bucket, but inserts of new
	// objects DO update objects bucket); we clamp progress at 0.99 below to
	// avoid a "100% — still working" UX glitch when drift makes processed
	// briefly exceed total.
	var totalObjects int64
	if t.progressCallback != nil {
		if n, countErr := shard.ObjectCountAsync(ctx); countErr == nil && n > 0 {
			totalObjects = n
		}
	}

	store := shard.Store()
	propExtraction := storobj.NewPropExtraction()
	bucketsByPropName := map[string]*lsmkv.Bucket{}
	for _, prop := range props {
		propExtraction.Add(prop)
		bucketName := t.reindexBucketName(prop)
		bucketsByPropName[prop] = store.Bucket(bucketName)
	}

	defer func() {
		if err != nil && !bytes.Equal(lastStoredKey.Bytes(), lastProcessedKey.Bytes()) {
			logger.WithField("last_processed_key", lastProcessedKey).Debug("recording progress on error")
			if cerr := t.recordCheckpoint(shard, subject, lastProcessedKey); cerr != nil {
				logger.Warnf("recording reindex progress on error: %v", cerr)
			}
		}
	}()

	breakCh := make(chan bool, 1)
	breakCh <- false
	finished := false

	// Flush the objects bucket so all data prior to this point is in
	// segments before the CursorOnDisk scan in [uuidObjectsIteratorAsync].
	// [Bucket.FlushAndSwitch] is serialized via [Bucket.flushAndSwitchMu]
	// so concurrent reindex tasks on the same shard wait for one another's
	// flush to land; on return, `sg.segments` is guaranteed to include
	// every pre-call write and the segment cursor will see it. Without
	// the FlushAndSwitch, ingest writes that committed to the in-memory
	// objects memtable just before this call would not be visible to
	// the segment-only Cursor() the iteration uses, producing a
	// per-replica `path = N/M` divergence on rows that were in flight
	// at iteration start.
	objectsBucket := store.Bucket(helpers.ObjectsBucketLSM)
	if objectsBucket != nil {
		if err = objectsBucket.FlushAndSwitch(); err != nil {
			err = fmt.Errorf("flushing objects bucket before reindex: %w", err)
			return zerotime, false, err
		}
	}

	err = store.PauseObjectBucketCompaction(ctx)
	if err != nil {
		return zerotime, false, err
	}
	defer store.ResumeObjectBucketCompaction(ctx)

	// Build the analyzer overlay from the strategy. For from-scratch
	// strategies (enable-filterable / enable-searchable) this forces the
	// target inverted-index flag on for the backfill scan, so the analyzer
	// produces values for the property we are populating. The live RAFT
	// schema is unchanged; only the analyzer's per-call view is overlaid.
	schemaOverlay := t.strategy.AnalyzerOverlay(props)

	processingStarted, mdCh := t.objectsIteratorAsync(logger, shard, lastStoredKey, t.keyParser.FromBytes,
		propExtraction, reindexStarted, breakCh, schemaOverlay)

	for md := range mdCh {
		if md == nil {
			finished = true
		} else if md.err != nil {
			err = md.err
			return zerotime, false, err
		} else if err = ctx.Err(); err != nil {
			breakCh <- true
			err = fmt.Errorf("context check (loop): %w / %w", err, context.Cause(ctx))
			return zerotime, false, err
		} else {
			if len(md.props) > 0 {
				for _, invprop := range md.props {
					if bucket, ok := bucketsByPropName[invprop.Name]; ok {
						if err := t.strategy.WriteToReindexBucket(shard, bucket, md.docID, invprop); err != nil {
							breakCh <- true
							err = fmt.Errorf("adding object '%s' prop '%s': %w", md.key.String(), invprop.Name, err)
							return zerotime, false, err
						}
					}
				}
				indexedCount++
			}
			processedCount++
			lastProcessedKey = md.key

			// Emit live progress every checkProcessingEveryNoObjects iterations.
			// The denominator is the ObjectCountAsync estimate from above; the
			// throttled recorder above this layer caps wire frequency. Clamp at
			// 0.99 so we never appear "complete" until the loop actually ends —
			// the final 1.0 is emitted by RecordDistributedTaskUnitCompletion on
			// success or carried by the FAILED status on error.
			if processedCount%t.config.checkProcessingEveryNoObjects == 0 {
				if t.progressCallback != nil && totalObjects > 0 {
					p := float32(processedCount) / float32(totalObjects)
					if p > 0.99 {
						p = 0.99
					}
					t.progressCallback(p)
				}
			}

			breakCh <- processedCount%t.config.checkProcessingEveryNoObjects == 0 &&
				time.Since(processingStarted) > t.config.processingDuration
		}
	}
	if !bytes.Equal(lastStoredKey.Bytes(), lastProcessedKey.Bytes()) {
		if err := t.recordCheckpoint(shard, subject, lastProcessedKey); err != nil {
			err = fmt.Errorf("recording reindex progress: %w", err)
			return zerotime, false, err
		}
		lastStoredKey = lastProcessedKey.Clone()
	}
	if finished {
		// Flush before recording the rebuild complete, or a SIGKILL leaves a record
		// claiming rows that never left the memtable.
		if err = t.flushReindexBuckets(shard, props, "recording the rebuild complete"); err != nil {
			return zerotime, false, err
		}
		if err = t.putMigrationRecord(shard, NewMigrationRecordIterated(subject)); err != nil {
			err = fmt.Errorf("recording the rebuild as complete: %w", err)
			return zerotime, false, err
		}
		if t.skipSwapOnFinish.Load() {
			logger.WithFields(map[string]any{
				"processed_count": processedCount,
				"indexed_count":   indexedCount,
			}).Info("reindex complete (swap deferred for barrier)")
			return zerotime, false, nil
		}
		// Inline runtime swap path (non-semantic migrations: MapToBlockmax,
		// RoaringSetRefresh, EnableRangeable / Repair-*). Semantic
		// migrations have skipSwapOnFinish=true and go through
		// OnGroupCompleted's three-phase flow (prep → overlay → atomic
		// swap). Here we run prep + atomic-swap inline: no overlay
		// needed — these migration types don't change the analyzer's
		// tokenization view of the bucket, so there is no FINALIZING
		// window where the analyzer and bucket content can disagree.
		if err = t.runtimePrepare(ctx, logger, shard, props); err != nil {
			err = fmt.Errorf("runtime prepare: %w", err)
			return zerotime, false, err
		}
		if err = t.runtimeSwap(ctx, logger, shard, props); err != nil {
			err = fmt.Errorf("runtime swap: %w", err)
			return zerotime, false, err
		}
		return zerotime, false, nil
	}
	return time.Now().Add(t.config.pauseDuration), false, nil
}

func (t *ShardReindexTaskGeneric) runtimePrepare(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, props []string,
) error {
	store := shard.Store()

	for _, propName := range props {
		reindexName := t.reindexBucketName(propName)
		ingestName := t.ingestBucketName(propName)

		reindexBucket := store.Bucket(reindexName)
		if reindexBucket == nil {
			return fmt.Errorf("reindex bucket %q not found", reindexName)
		}
		ingestBucket := store.Bucket(ingestName)
		if ingestBucket == nil {
			return fmt.Errorf("ingest bucket %q not found", ingestName)
		}

		// FlushAndSwitch makes the reindex memtable immutable so its
		// segments are safe to copy.
		if err := reindexBucket.FlushAndSwitch(); err != nil {
			return fmt.Errorf("flushing reindex bucket %q: %w", reindexName, err)
		}
		reindexDir := reindexBucket.GetDir()
		// FOLLOW-UP: store.ShutdownBucket / bucket.Shutdown does not abort
		// an in-flight long-running compaction when ctx is cancelled —
		// it waits for the compaction to finish naturally and only then
		// observes the cancellation, returning "long-running compaction
		// in progress: context canceled". During a graceful shutdown
		// (rolling restart) this means the prep can be interrupted mid-
		// flight even though there's a clean exit path that doesn't
		// touch the compaction's output. Tracked separately.
		if err := store.ShutdownBucket(ctx, reindexName); err != nil {
			return fmt.Errorf("shutting down reindex bucket %q: %w", reindexName, err)
		}

		// Prepend reindex segments into the ingest bucket. After this,
		// ingest contains all reindexed + double-written data.
		if err := ingestBucket.PrependSegmentsFromBucket(ctx, reindexDir); err != nil {
			return fmt.Errorf("prepending segments from %q to %q: %w", reindexName, ingestName, err)
		}
	}

	subject, err := t.migrationSubjectNow(shard)
	if err != nil {
		return err
	}
	if err := t.putMigrationRecord(shard, NewMigrationRecordMerged(subject)); err != nil {
		return fmt.Errorf("recording the staged data as complete: %w", err)
	}

	if err := t.removeReindexBucketsDirs(ctx, logger, shard, props); err != nil {
		return fmt.Errorf("removing reindex bucket dirs: %w", err)
	}

	logger.Debug("runtime prepare: all props merged")
	return nil
}

func (t *ShardReindexTaskGeneric) runtimeSwap(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, props []string,
) error {
	swapCompleted := false
	defer func() {
		if !swapCompleted {
			return
		}
		if registry := shard.migrationMirrorRegistry(); registry != nil {
			registry.DisarmMigrationMirrors(t.migrationRecordKey())
		}
	}()

	store := shard.Store()

	displaced := make(map[string]string, len(props))
	for _, propName := range props {
		if bucket := store.Bucket(t.strategy.SourceBucketName(propName)); bucket != nil {
			displaced[propName] = filepath.Base(bucket.GetDir())
		}
	}
	subject, err := t.migrationSubjectNow(shard)
	if err != nil {
		return err
	}
	if err := t.putMigrationRecord(shard, NewMigrationRecordSwapped(subject, props, displaced)); err != nil {
		return fmt.Errorf("recording the flip decision: %w", err)
	}

	if concrete, err := unwrapShard(ctx, shard); err == nil {
		concrete.migrations().RetireSuperseded(ctx)
	} else {
		logger.Warnf("runtime swap: cannot retire superseded migrations: %v", err)
	}

	oldMainBuckets := make(map[string]*lsmkv.Bucket, len(props))
	for propIdx, propName := range props {
		var (
			oldMainBucket *lsmkv.Bucket
			err           error
		)
		if t.swapPropAtomic != nil {
			oldMainBucket, err = t.swapPropAtomic(ctx, store, propIdx, propName)
			if err != nil {
				return err
			}
		} else {
			oldMainBucket, err = t.processOneSwapPropFn(ctx, store, propIdx, propName)
			if err != nil {
				return err
			}
			// Fire even when processOneSwapPropFn no-ops an already-swapped
			// prop, so a resumed swap re-establishes the overlay.
			if t.onPropSwapped != nil {
				t.onPropSwapped(propName)
			}
		}
		if oldMainBucket != nil {
			oldMainBuckets[propName] = oldMainBucket
		}
	}
	logger.Debug("runtime swap: all props in-memory swapped")

	// Phase 2b: only a shut-down bucket may be removed, and Bucket.Shutdown is what
	// releases its path from lsmkv.GlobalBucketRegistry — skip it and the next
	// in-process shard init at this name fails with ErrBucketAlreadyRegistered.
	for _, propName := range props {
		oldMainBucket, ok := oldMainBuckets[propName]
		if !ok {
			continue
		}
		if err := oldMainBucket.Shutdown(ctx); err != nil {
			return fmt.Errorf("shutting down old main bucket for %q: %w", propName, err)
		}
		dir := filepath.Base(oldMainBucket.GetDir())
		if key, role, held := migrationDirHeldByAnotherRecord(
			migrationRecordsOf(shard), subject, dir, migrationLiveDataRoles); held {
			logger.WithField("dir", dir).Warnf(
				"leaving the displaced directory of %q in place: record %s names it as its %s, "+
					"and its retirement has not run yet", propName, key, role)
			continue
		}
		if err := os.RemoveAll(oldMainBucket.GetDir()); err != nil {
			return fmt.Errorf("removing displaced dir for %q: %w", propName, err)
		}
	}

	logger.Debug("runtime swap: displaced dirs removed (staged→canonical rename deferred to next load)")
	swapCompleted = true

	// Ordering contract: rebuild must be checked before OnMigrationComplete.
	//
	// Unlike the semantic-migration family ([IsSemanticMigration]),
	// FilterableToRangeableStrategy.OnMigrationComplete is not gated by
	// task-terminal status - it RAFT-commits IndexRangeFilters=true
	// unconditionally the first time any shard's swap reaches this line.
	// Skipping the check would advertise range-query support while this
	// shard still falls back to disk (or a corrupt segment parsed as empty
	// - see [rebuildRangeableInMemoryReps]).
	if err := t.rebuildRangeableInMemoryReps(ctx, logger, shard, props); err != nil {
		return err
	}

	// OnMigrationComplete: no-op for semantic migrations (the cluster-
	// wide schema flip lives in OnTaskCompleted.flipSemanticMigrationSchema).
	// Per-shard schema-flag flip for blockmax / repair-* strategies.
	// Either way, runs OUTSIDE the per-shard atomic window because it
	// doesn't touch bucket pointers.
	if err := t.strategy.OnMigrationComplete(ctx, shard); err != nil {
		return fmt.Errorf("on migration complete: %w", err)
	}

	t.trimOlderGenerationsLocked(logger, shard, props)

	logger.Info("runtime swap: migration complete (ingest→main rename deferred to next restart)")

	return nil
}

func (t *ShardReindexTaskGeneric) trimOlderGenerationsLocked(
	logger logrus.FieldLogger, shard ShardLike, props []string,
) {
	concrete, err := unwrapShard(context.Background(), shard)
	if err != nil {
		logger.Warnf("runtime swap: trim: failed to unwrap shard; skipping cleanup: %v", err)
		return
	}
	preserve, ok := trimPreserveSetOf(concrete)
	if !ok {
		logger.Warn("runtime swap: trim: this shard's migration records could not be read in full; " +
			"trimming nothing")
		return
	}
	lsmPath := concrete.pathLSM()
	for _, path := range t.obsoleteSidecarDirs(logger, lsmPath, props, preserve) {
		t.removeAllSafe(logger, path)
	}
	for _, path := range t.obsoleteTrackerDirs(logger, lsmPath, preserve) {
		t.removeAllSafe(logger, path)
	}
}

type migrationTrimPreserve struct {
	records []MigrationRecord
}

func trimPreserveSetOf(shard *Shard) (migrationTrimPreserve, bool) {
	if shard.migrationRecords == nil || len(shard.migrationRecords.Unreadable()) > 0 {
		return migrationTrimPreserve{}, false
	}
	return migrationTrimPreserve{records: shard.migrationRecords.Records()}, true
}

func (p migrationTrimPreserve) bucketDir(dir string) bool {
	for _, rec := range p.records {
		if rec.OwnsBucket(dir) {
			return true
		}
	}
	return false
}

func (p migrationTrimPreserve) trackerDir(dir string) bool {
	for _, rec := range p.records {
		if rec.Subject().TrackerDir == dir {
			return true
		}
	}
	return false
}

func (t *ShardReindexTaskGeneric) obsoleteSidecarDirs(logger logrus.FieldLogger,
	lsmPath string, props []string, preserve migrationTrimPreserve,
) []string {
	entries, err := os.ReadDir(lsmPath)
	if err != nil {
		logger.Warnf("runtime swap: trim: failed to read LSM dir; skipping cleanup: %v", err)
		return nil
	}
	currentReindexBase, _, _ := parseMigrationDirName(t.strategy.ReindexSuffix())
	currentIngestBase, _, _ := parseMigrationDirName(t.strategy.IngestSuffix())
	_, currentGenN, _ := parseMigrationDirName(t.strategy.MigrationDirName())

	var out []string
	for _, propName := range props {
		mainBucket := t.strategy.SourceBucketName(propName)
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			name := entry.Name()
			if !strings.HasPrefix(name, mainBucket) {
				continue
			}
			// Strip the mainBucket prefix to inspect the suffix.
			rest := name[len(mainBucket):]
			if len(rest) == 0 {
				continue // the live main bucket itself
			}
			suffixBase, suffixGen, ok := parseMigrationDirName(rest)
			if !ok || preserve.bucketDir(name) {
				continue
			}
			switch suffixBase {
			case currentReindexBase:
				out = append(out, filepath.Join(lsmPath, name))
			case currentIngestBase:
				if suffixGen < currentGenN {
					out = append(out, filepath.Join(lsmPath, name))
				}
			}
		}
	}
	return out
}

func (t *ShardReindexTaskGeneric) obsoleteTrackerDirs(logger logrus.FieldLogger,
	lsmPath string, preserve migrationTrimPreserve,
) []string {
	migsDir := filepath.Join(lsmPath, ".migrations")
	entries, err := os.ReadDir(migsDir)
	if err != nil {
		if !os.IsNotExist(err) {
			logger.Warnf("runtime swap: trim: failed to read .migrations dir; skipping cleanup: %v", err)
		}
		return nil
	}
	currentMigBase, currentGenN, _ := parseMigrationDirName(t.strategy.MigrationDirName())

	var out []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		base, gen, ok := parseMigrationDirName(entry.Name())
		if !ok || preserve.trackerDir(entry.Name()) {
			continue
		}
		if base != currentMigBase || gen >= currentGenN {
			continue
		}
		out = append(out, filepath.Join(migsDir, entry.Name()))
	}
	return out
}

func (t *ShardReindexTaskGeneric) removeAllSafe(logger logrus.FieldLogger, path string) {
	if err := os.RemoveAll(path); err != nil {
		logger.WithField("path", path).
			Warnf("runtime swap: trim: failed to remove obsolete dir; the orphan audit reclaims it: %v", err)
	}
}

// -----------------------------------------------------------------------------
// Bucket operations
// -----------------------------------------------------------------------------

func (t *ShardReindexTaskGeneric) setMigrationIdentity(desc distributedtask.TaskDescriptor,
	unitID string, payload *ReindexTaskPayload,
) {
	t.taskID = desc.ID
	t.taskVersion = desc.Version
	t.unitID = unitID
	t.migrationType = payload.MigrationType
	t.targetTokenization = payload.TargetTokenization
	t.originalTokenization = payload.OriginalTokenization
}

func (t *ShardReindexTaskGeneric) migrationRecordKey() MigrationRecordKey {
	return MigrationRecordKey{
		TaskVersion:  t.taskVersion,
		StrategyCode: t.strategy.StrategyCode(),
		UnitID:       t.unitID,
	}
}

// The record key deliberately omits the generation, so a retried unit that
// minted a new one finds the abandoned generation's record under its own key.
// Adopting it would resume the rebuild at a checkpoint for directories this
// task no longer writes, and flip onto directories the record does not name.
func (t *ShardReindexTaskGeneric) migrationRecord(shard ShardLike) (MigrationRecord, bool) {
	store := shard.migrationRecordStore()
	if store == nil {
		return nil, false
	}
	rec, ok := store.Get(t.migrationRecordKey())
	if !ok {
		return nil, false
	}
	if !t.recordIsThisGeneration(rec) {
		return nil, false
	}
	return rec, true
}

// An empty tracker dir names no generation at all, so it cannot be told apart
// from this one and is left to the caller that wrote it.
func (t *ShardReindexTaskGeneric) recordIsThisGeneration(rec MigrationRecord) bool {
	tracker := rec.Subject().TrackerDir
	return tracker == "" || tracker == t.strategy.MigrationDirName()
}

func migrationRecordsOf(shard ShardLike) []MigrationRecord {
	store := shard.migrationRecordStore()
	if store == nil {
		return nil
	}
	return store.Records()
}

func (t *ShardReindexTaskGeneric) putMigrationRecord(shard ShardLike, rec MigrationRecord) error {
	store := shard.migrationRecordStore()
	if store == nil {
		return fmt.Errorf("shard %q holds no migration record store", shard.Name())
	}
	if key := t.migrationRecordKey(); !key.valid() {
		return fmt.Errorf("migration identity %s is incomplete", key)
	}
	return store.Put(rec)
}

func (t *ShardReindexTaskGeneric) flushReindexBuckets(shard ShardLike, props []string, what string) error {
	store := shard.Store()
	if store == nil {
		return nil
	}
	for _, prop := range props {
		bucket := store.Bucket(t.reindexBucketName(prop))
		if bucket == nil {
			continue
		}
		if err := bucket.FlushAndSwitch(); err != nil {
			return fmt.Errorf("flushing the reindex bucket for property %q before %s: %w", prop, what, err)
		}
	}
	return nil
}

func (t *ShardReindexTaskGeneric) recordCheckpoint(shard ShardLike, subject MigrationSubject,
	lastProcessedKey indexKey,
) error {
	// Flush before the checkpoint: the resume seeks strictly past the checkpoint
	// key, so a posting the crash dropped from the buffer is never rebuilt.
	if err := t.flushReindexBuckets(shard, subject.Properties(), "recording iteration progress"); err != nil {
		return err
	}
	return t.putMigrationRecord(shard, NewMigrationRecordIterating(subject, MigrationCheckpoint{
		LastProcessedKey: lastProcessedKey.Clone().Bytes(),
		UpdatedAt:        time.Now(),
	}))
}

func (t *ShardReindexTaskGeneric) migrationSubjectNow(shard ShardLike) (MigrationSubject, error) {
	rec, ok := t.migrationRecord(shard)
	if !ok {
		return MigrationSubject{}, fmt.Errorf("migration %s has no record on shard %q",
			t.migrationRecordKey(), shard.Name())
	}
	return rec.Subject(), nil
}

func (t *ShardReindexTaskGeneric) migrationSubject(shard ShardLike, props []string, cutoff time.Time) MigrationSubject {
	subject := MigrationSubject{
		Key:                  t.migrationRecordKey(),
		TaskID:               t.taskID,
		MigrationType:        t.migrationType,
		TargetTokenization:   t.targetTokenization,
		OriginalTokenization: t.originalTokenization,
		IterationCutoff:      cutoff,
		TrackerDir:           t.strategy.MigrationDirName(),
		Props:                make(map[string]MigrationPropertyDirs, len(props)),
	}
	for _, propName := range props {
		subject.Props[propName] = MigrationPropertyDirs{
			Staged:    t.ingestBucketName(propName),
			Canonical: t.strategy.SourceBucketName(propName),
			Sidecar:   t.reindexBucketName(propName),
		}
	}
	return subject
}

func (t *ShardReindexTaskGeneric) reindexBucketName(propName string) string {
	return t.strategy.SourceBucketName(propName) + t.strategy.ReindexSuffix()
}

func (t *ShardReindexTaskGeneric) ingestBucketName(propName string) string {
	return t.strategy.SourceBucketName(propName) + t.strategy.IngestSuffix()
}

func (t *ShardReindexTaskGeneric) loadReindexBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard *Shard, props []string,
) error {
	bucketOpts := t.bucketOptions(shard, t.strategy.TargetStrategy(), false, false, t.config.memtableOptFactor)
	return t.loadBuckets(ctx, logger, shard, props, t.reindexBucketName, bucketOpts)
}

func (t *ShardReindexTaskGeneric) loadIngestBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard *Shard, props []string,
	keepLevelCompaction, keepTombstones bool,
) error {
	strategy := t.strategy.TargetStrategy()
	bucketOpts := t.bucketOptions(shard, strategy, keepLevelCompaction, keepTombstones, t.config.memtableOptFactor)

	// Only the ingest bucket becomes the main bucket post-swap; the reindex
	// buckets are torn down and never serve reads.
	if strategy == lsmkv.StrategyRoaringSetRange && shard.Index().Config.IndexRangeableInMemory {
		bucketOpts = append(bucketOpts, lsmkv.WithRangeableInMemoryDeferred(true))
		logger.WithField("props", props).Info(
			"rangeable properties are serving from disk during reindex ingest; " +
				"in-memory acceleration is restored automatically when the migration " +
				"finalizes. A node restart, shard reload, or tenant reactivation only " +
				"repairs this if that automatic rebuild fails.",
		)
	}

	return t.loadBuckets(ctx, logger, shard, props, t.ingestBucketName, bucketOpts)
}

func (t *ShardReindexTaskGeneric) loadBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, props []string, bucketNamer func(string) string,
	bucketOpts []lsmkv.BucketOption,
) error {
	store := shard.Store()

	eg, gctx := enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
	eg.SetLimit(t.config.concurrency)
	for i := range props {
		propName := props[i]

		eg.Go(func() error {
			bucketName := bucketNamer(propName)
			logger.WithField("bucket", bucketName).Debug("loading bucket")
			if err := store.CreateOrLoadBucket(gctx, bucketName, bucketOpts...); err != nil {
				return err
			}
			logger.WithField("bucket", bucketName).Debug("bucket loaded")
			return nil
		})
	}

	return eg.Wait()
}

func (t *ShardReindexTaskGeneric) removeReindexBucketsDirs(ctx context.Context, logger logrus.FieldLogger,
	shard ShardLike, props []string,
) error {
	return t.removeBucketsDirs(ctx, logger, shard, props, t.reindexBucketName)
}

func (t *ShardReindexTaskGeneric) removeBucketsDirs(ctx context.Context, logger logrus.FieldLogger,
	shard ShardLike, props []string, bucketNamer func(string) string,
) error {
	lsmPath := shard.pathLSM()
	eg, _ := enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
	eg.SetLimit(t.config.concurrency)
	for i := range props {
		propName := props[i]

		eg.Go(func() error {
			bucketName := bucketNamer(propName)
			bucketPath := filepath.Join(lsmPath, bucketName)

			logger.WithField("bucket", bucketName).Debug("removing bucket")

			return os.RemoveAll(bucketPath)
		})
	}
	return eg.Wait()
}

func (t *ShardReindexTaskGeneric) registerDoubleWriteCallbacks(shard *Shard, props []string,
	bucketNamer func(string) string,
) (func(), error) {
	buckets := make(map[string]*lsmkv.Bucket, len(props))
	for _, propName := range props {
		bucketName := bucketNamer(propName)
		bucket := shard.store.Bucket(bucketName)
		if bucket == nil {
			return nil, fmt.Errorf("arming the double-write mirror on shard %q: staged bucket %q for property %q is not open",
				shard.Name(), bucketName, propName)
		}
		buckets[propName] = bucket
	}

	disarm := shard.registerDoubleWriteWithScope(props, t.strategy.AnalyzerOverlay(props),
		func(scope map[string]struct{}) (onAddToPropertyValueIndex, onDeleteFromPropertyValueIndex) {
			armed := armedMirror{props: scope, buckets: buckets}
			return t.strategy.MakeAddCallback(bucketNamer, armed),
				t.strategy.MakeDeleteCallback(bucketNamer, armed)
		})

	registry := shard.migrationMirrorRegistry()
	key := t.migrationRecordKey()
	for _, propName := range props {
		registry.ArmMigrationMirror(key, propName, func() { disarm(propName) })
	}

	var spent sync.Once
	return func() {
		spent.Do(func() {
			for _, propName := range props {
				registry.DisarmMigrationMirror(key, propName)
			}
		})
	}, nil
}

func (t *ShardReindexTaskGeneric) bucketOptions(shard *Shard, strategy string,
	keepLevelCompaction, keepTombstones bool, memtableOptFactor int,
) []lsmkv.BucketOption {
	cfg := shard.Index().Config

	opts := shard.makeDefaultBucketOptions(
		strategy,
		lsmkv.WithKeepLevelCompaction(keepLevelCompaction),
		lsmkv.WithKeepTombstones(keepTombstones),
		// overwrite DynamicMemtableSizing
		lsmkv.WithDynamicMemtableSizing(
			memtableOptFactor*cfg.MemtablesInitialSizeMB,
			memtableOptFactor*cfg.MemtablesMaxSizeMB,
			memtableOptFactor*cfg.MemtablesMinActiveSeconds,
			memtableOptFactor*cfg.MemtablesMaxActiveSeconds,
		),
	)

	// Override: RoaringSetRange ingest buckets never keep an in-memory rep.
	// PrependSegmentsFromBucket can't rebuild it, so a kept rep would serve
	// stale/empty range results until the next clean bucket open.
	if strategy == lsmkv.StrategyRoaringSetRange {
		opts = append(opts, lsmkv.WithKeepSegmentsInMemory(false))
	}

	return opts
}

// -----------------------------------------------------------------------------
// Property discovery and selection
// -----------------------------------------------------------------------------

func (t *ShardReindexTaskGeneric) findPropsToReindex(shard ShardLike) []string {
	collectionName := shard.Index().Config.ClassName.String()
	shardName := shard.Name()
	propNames := []string{}

	if !t.isShardSelected(collectionName, shardName) {
		return propNames
	}

	// When selection is enabled and an explicit list of properties is given,
	// the selected list IS the authoritative reindex target. Existing
	// strategies (e.g. repair-searchable, change-tokenization) target
	// properties whose source bucket already exists; new from-scratch
	// strategies (enable-filterable, enable-searchable) target properties
	// whose source bucket does not exist yet and will be created in
	// PreReindexHook. Both cases reduce to "use the selected list".
	if selected, ok := t.selectedProps(collectionName); ok {
		return selected
	}

	// Fallback: discover props by scanning existing buckets that have the
	// expected source strategy + index type. Used when selection is not
	// enabled (whole-collection migrations).
	for name, bucket := range shard.Store().GetBucketsByName() {
		if bucket.Strategy() == t.strategy.SourceStrategy() {
			propName, indexType := GetPropNameAndIndexTypeFromBucketName(name)

			if indexType == t.strategy.SourceIndexType() {
				propNames = append(propNames, propName)
			}
		}
	}
	return propNames
}

// selectedProps is the sorted property list the task already carries in its own
// config for collectionName. ok=false means it carries none, so the properties
// have to be discovered from the shard's buckets instead.
func (t *ShardReindexTaskGeneric) selectedProps(collectionName string) ([]string, bool) {
	if !t.config.selectionEnabled {
		return nil, false
	}
	selected := t.config.selectedPropsByCollection[collectionName]
	if len(selected) == 0 {
		return nil, false
	}
	propNames := make([]string, 0, len(selected))
	for propName := range selected {
		propNames = append(propNames, propName)
	}
	// Sort for determinism — map iteration order is randomized and the record's
	// property list is compared and logged.
	sort.Strings(propNames)
	return propNames, true
}

func (t *ShardReindexTaskGeneric) isShardSelected(collectionName, shardName string) bool {
	if t.config.selectionEnabled {
		selectedShards, isCollectionSelected := t.config.selectedShardsByCollection[collectionName]
		if !isCollectionSelected {
			return false
		}

		if len(selectedShards) > 0 {
			if _, isShardSelected := selectedShards[shardName]; !isShardSelected {
				return false
			}
		}
	}
	return true
}

// -----------------------------------------------------------------------------
// Migration data and object iterator
// -----------------------------------------------------------------------------

type migrationData struct {
	key   indexKey
	docID uint64
	props []inverted.Property
	err   error
}

type objectsIteratorAsync func(logger logrus.FieldLogger, shard ShardLike, lastKey indexKey, keyParse func([]byte) indexKey, propExtraction *storobj.PropertyExtraction, reindexStarted time.Time, breakCh <-chan bool, schemaOverlay map[string]inverted.PropertyOverlay,
) (time.Time, <-chan *migrationData)

func uuidObjectsIteratorAsync(logger logrus.FieldLogger, shard ShardLike, lastKey indexKey, keyParse func([]byte) indexKey,
	propExtraction *storobj.PropertyExtraction, reindexStarted time.Time, breakCh <-chan bool,
	schemaOverlay map[string]inverted.PropertyOverlay,
) (time.Time, <-chan *migrationData) {
	startedCh := make(chan time.Time)
	mdCh := make(chan *migrationData)

	enterrors.GoWrapper(func() {
		// CursorOnDisk is safe here because [Bucket.FlushAndSwitch] is
		// serialized via [Bucket.flushAndSwitchMu] — every caller
		// (including the preceding FlushAndSwitch on this iterator's
		// path) waits for any concurrent flush to complete before
		// returning. So `sg.segments` is guaranteed to include all
		// data written before this call, with no transient window
		// where data is parked in `b.flushing` invisible to the
		// segment cursor — the original race that the
		// flushAndSwitchMu lock was added to close.
		cursor := shard.Store().Bucket(helpers.ObjectsBucketLSM).CursorOnDisk()
		defer cursor.Close()

		startedCh <- time.Now() // after cursor created (necessary locks acquired)
		addProps := additional.Properties{}
		className := shard.Index().Config.ClassName.String()

		var k, v []byte
		if lastKey == nil {
			k, v = cursor.First()
		} else {
			key := lastKey.Bytes()
			k, v = cursor.Seek(key)
			if bytes.Equal(k, key) {
				k, v = cursor.Next()
			}
		}

		for ; k != nil; k, v = cursor.Next() {
			ik := keyParse(k)
			obj, err := storobj.FromBinaryOptionalDisk(v, className, addProps, propExtraction)
			if err != nil {
				mdCh <- &migrationData{err: fmt.Errorf("unmarshalling object '%s': %w", ik.String(), err)}
				break
			}

			if obj.LastUpdateTimeUnix() < reindexStarted.UnixMilli() {
				// The overlay is required by from-scratch strategies whose
				// target inverted-index flag is still false on the live
				// schema during backfill. It is nil for retokenize / refresh
				// strategies. See MigrationStrategy.AnalyzerOverlay.
				props, _, err := shard.AnalyzeObjectForMigrationWithOverlay(obj, schemaOverlay)
				if err != nil {
					mdCh <- &migrationData{err: fmt.Errorf("analyzing object '%s': %w", ik.String(), err)}
					break
				}

				if <-breakCh {
					break
				}
				mdCh <- &migrationData{key: ik.Clone(), props: props, docID: obj.DocID}
			} else {
				if <-breakCh {
					break
				}
				mdCh <- &migrationData{key: ik.Clone()}
			}
		}
		if k == nil {
			<-breakCh
			mdCh <- nil
		}
		close(mdCh)
	}, logger)

	return <-startedCh, mdCh
}

// -----------------------------------------------------------------------------
// Index key types
// -----------------------------------------------------------------------------

type indexKey interface {
	String() string
	Bytes() []byte
	Clone() indexKey
}

type uuidBytes []byte

func (b uuidBytes) String() string {
	if b == nil {
		return "nil"
	}
	uid, err := uuid.FromBytes(b)
	if err != nil {
		return err.Error()
	}
	return uid.String()
}

func (b uuidBytes) Bytes() []byte {
	return b
}

func (b uuidBytes) Clone() indexKey {
	buf := make([]byte, len(b))
	copy(buf, b)
	return uuidBytes(buf)
}

type indexKeyParser interface {
	FromString(key string) (indexKey, error)
	FromBytes(key []byte) indexKey
}

// UuidKeyParser parses index keys as UUIDs.
type UuidKeyParser struct{}

func (p *UuidKeyParser) FromString(key string) (indexKey, error) {
	uid, err := uuid.Parse(key)
	if err != nil {
		return nil, err
	}
	buf, err := uid.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return uuidBytes(buf), nil
}

func (p *UuidKeyParser) FromBytes(key []byte) indexKey {
	return uuidBytes(key)
}
