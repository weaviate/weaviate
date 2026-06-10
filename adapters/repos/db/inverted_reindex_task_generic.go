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
// removeReindexBucketsDirs, sentinel writes (markPrepended,
// markMerged).
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
// 2b — post-atomic inline tidy (slow but correctness-safe):
// oldMainBucket.Shutdown(ctx) + os.Rename(oldMainDir, backupDir) per
// property, then rt.markSwapped + rt.markTidied. This runs AFTER
// every prop has flipped in 2a, so the mixed-state subwindow is
// closed. Queries during 2b see all-new buckets with the overlay
// active — correct. The oldMain.Shutdown is REQUIRED inline (not
// deferred) because Bucket.Shutdown is the only call that removes
// the bucket's path from GlobalBucketRegistry; deferring it leaks
// the path entry process-wide, which makes any subsequent in-process
// shard init at the same canonical name fail with
// ErrBucketAlreadyRegistered. The oldMain → backup rename is also
// fine inline because the bucket has just been shut down — the
// load-bearing rule is: rename only buckets that have been shut down;
// never rename a live bucket that is serving queries.
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
// Implemented by [FinalizeCompletedMigrations] (which scans
// .migrations/ for tracker dirs with merged.mig — recovery promotes
// merged-but-not-tidied gens — and tidied.mig, then renames each
// gen's __ingest_<N>/ dir to its canonical name).
//
// Why deferred: the ingest bucket is the LIVE post-swap main bucket.
// Its mmaps are open, its segment registry holds the ingest_<N> path
// as its dir. Renaming that dir while the bucket is in-memory would
// corrupt the segment registry and any subsequent write that
// resolves paths from the bucket's stored dir. At next startup,
// before LSM init touches the canonical name, no bucket is mmapping
// anything — the rename is safe.
//
// Crash safety: every phase transition is preceded by a tracker
// sentinel fsync (markPrepended, markMerged, markSwappedProp,
// markSwapped, markTidied). The dispatch in
// [ShardReindexTaskGeneric.RunSwapOnShard] inspects sentinels to
// pick the right resume path on rehydrate. A crash within Phase 2b
// (after markSwappedProp on some props but before markSwapped on
// the aggregate) lands in the IsMerged dispatch branch on the
// next-process restart: if ingest buckets are loaded (rare),
// runtimeSwap is re-invoked; if not (normal startup), the
// recoverRuntimeSwapBuckets path does the disk-rename equivalent
// of 2b before LSM init loads any bucket.
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
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"
)

// ShardReindexTaskGeneric is a strategy-parameterized implementation of
// ShardReindexTaskV3. All lifecycle logic (state machine, merge/swap/tidy,
// object iteration, progress tracking) lives here, with strategy-specific
// behavior delegated to a MigrationStrategy.
//
// See the file-level phase-contract godoc above for the prep / atomic
// swap / deferred-rename invariants that every code path in this file
// must preserve.
type ShardReindexTaskGeneric struct {
	name                 string
	logger               logrus.FieldLogger
	strategy             MigrationStrategy
	newReindexTracker    func(lsmPath string) (reindexTracker, error)
	keyParser            indexKeyParser
	objectsIteratorAsync objectsIteratorAsync
	config               reindexTaskConfig

	// skipSwapOnFinish, when true, causes the reindex iteration to stop after
	// markReindexed() without proceeding to runtimeSwap(). This is used by
	// RunReindexOnlyOnShard for barrier semantics: all shards must finish
	// reindexing before any shard swaps.
	//
	// Atomic because the field is written by runShardLifecycle (from the
	// StartTask worker goroutine) and read by OnAfterLsmInitAsync's loop
	// body (which can run on a separate goroutine when the cached task
	// instance is later invoked from OnGroupCompleted's swap phase).
	skipSwapOnFinish atomic.Bool

	// callbackDisableFuncs collects the disable functions returned by
	// registerAddToPropertyValueIndex / registerDeleteFromPropertyValueIndex.
	// They are called after a runtime swap to stop the double-write callbacks.
	callbackDisableFuncsMu sync.Mutex
	callbackDisableFuncs   []func()

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
	processOneSwapPropFn func(ctx context.Context, store *lsmkv.Store, rt reindexTracker, propIdx int, propName string) (*lsmkv.Bucket, error)

	// processOneTidyPropFn is the dispatch function for
	// tidyBackupBuckets' per-prop body. Same shape as
	// [processOneSwapPropFn] — defaults to the [processOneTidyProp]
	// method; tests substitute a wrapper.
	processOneTidyPropFn func(propIdx int, propName, lsmPath string) error

	// onPropSwapped runs inside the Phase 2a tight loop right after each
	// bucket-pointer flip, so a query never observes overlay≠bucket for
	// longer than one in-memory map write. Runs on the swap goroutine, so
	// SetTokenizationOverlay's own lock is enough. Wired only for
	// tokenization-changing migrations.
	onPropSwapped func(propName string)

	// onBeforeDoubleWriteRegistration is a test-only synchronization hook
	// invoked by [OnAfterLsmInit] right before the ingest double-write
	// callbacks are registered (and therefore before the reindexStarted
	// timestamp is captured). Tests use it to inject concurrent writes
	// deterministically at the exact point where the pre-fix ordering
	// (markStarted before callback registration) permanently lost them.
	// Nil in production.
	onBeforeDoubleWriteRegistration func()

	// onAfterMigrationComplete is a test-only synchronization hook
	// invoked by [runtimeSwap] right after strategy.OnMigrationComplete
	// returns (schema flag flipped) and BEFORE the write-quiescence
	// barrier and the deferred disableCallbacks run. Tests use it to
	// inject in-flight writes that analyzed under the pre-flip schema
	// (HasColumnarIndex=false) deterministically into the window where
	// the double-write mirror is their only route into the target
	// bucket. Nil in production.
	onAfterMigrationComplete func()
}

// NewShardReindexTaskGeneric creates a new generic reindex task.
func NewShardReindexTaskGeneric(name string, logger logrus.FieldLogger,
	strategy MigrationStrategy, config reindexTaskConfig,
	keyParser indexKeyParser, objectsIteratorAsync objectsIteratorAsync,
) *ShardReindexTaskGeneric {
	logger = logger.WithField("task", name)
	newReindexTracker := func(lsmPath string) (reindexTracker, error) {
		rt := NewFileReindexTracker(lsmPath, strategy.MigrationDirName(), keyParser)
		if err := rt.init(); err != nil {
			return nil, err
		}
		return rt, nil
	}

	logger.WithField("config", fmt.Sprintf("%+v", config)).Debug("task created")

	t := &ShardReindexTaskGeneric{
		name:                 name,
		logger:               logger,
		strategy:             strategy,
		newReindexTracker:    newReindexTracker,
		keyParser:            keyParser,
		objectsIteratorAsync: objectsIteratorAsync,
		config:               config,
	}
	t.processOneSwapPropFn = t.processOneSwapProp
	t.processOneTidyPropFn = t.processOneTidyProp
	return t
}

// processOneSwapProp is the production body of runtimeSwap's Phase 2a
// per-prop loop: in-memory pointer flip + per-prop sentinel write.
// Returns the displaced old main bucket for the caller's Phase 2b
// (Shutdown + dir rename). Skips props whose per-prop sentinel is
// already set (recovery idempotency).
func (t *ShardReindexTaskGeneric) processOneSwapProp(ctx context.Context, store *lsmkv.Store, rt reindexTracker, _ int, propName string) (*lsmkv.Bucket, error) {
	if rt.IsSwappedProp(propName) {
		return nil, nil
	}
	ingestName := t.ingestBucketName(propName)
	mainName := t.strategy.SourceBucketName(propName)
	oldMainBucket, err := store.SwapBucketPointer(ctx, mainName, ingestName)
	if err != nil {
		return nil, fmt.Errorf("swapping bucket pointer %q <- %q: %w", mainName, ingestName, err)
	}
	if err := rt.markSwappedProp(propName); err != nil {
		return nil, fmt.Errorf("marking swapped prop %q: %w", propName, err)
	}
	return oldMainBucket, nil
}

// processOneTidyProp is the production body of tidyBackupBuckets'
// per-prop loop: remove the per-prop backup-bucket dir. Idempotent
// (RemoveAll returns nil on already-removed dirs).
func (t *ShardReindexTaskGeneric) processOneTidyProp(_ int, propName, lsmPath string) error {
	bucketName := t.backupBucketName(propName)
	bucketPath := filepath.Join(lsmPath, bucketName)
	return os.RemoveAll(bucketPath)
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

// MigrationDirName returns the strategy-specific sub-directory under each
// shard's <lsm>/.migrations/ that this task owns. Exposed so callers can
// drop or read recovery sentinels (e.g. payload.mig) without depending on
// the unexported reindexTracker.
func (t *ShardReindexTaskGeneric) MigrationDirName() string {
	return t.strategy.MigrationDirName()
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
// had a reindex in progress when the node went down. The sentinel lives
// in the same migration sub-directory as the other *.mig sentinels so it
// is removed alongside them on reset/cleanup.
const reindexRecoveryPayloadFile = "payload.mig"

// SaveRecoveryPayload writes the given JSON-encoded recovery record to
// payload.mig inside the migration directory of this task on the given
// shard LSM path. It is idempotent: if the file already exists with the
// same content, the call is a no-op; otherwise it is overwritten.
// Callers are expected to ensure the migration directory exists; this
// function will [os.MkdirAll] it just in case to keep startup recovery
// robust against partial state.
func (t *ShardReindexTaskGeneric) SaveRecoveryPayload(lsmPath string, payload []byte) error {
	migDir := t.migrationPath(lsmPath)
	if err := os.MkdirAll(migDir, 0o777); err != nil {
		return fmt.Errorf("mkdir migration dir %q: %w", migDir, err)
	}
	target := filepath.Join(migDir, reindexRecoveryPayloadFile)
	if existing, err := os.ReadFile(target); err == nil && bytes.Equal(existing, payload) {
		return nil
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

// RunReindexOnlyOnShard runs the reindex iteration WITHOUT swap/tidy.
// After this returns, the shard has:
//   - ingest bucket with double-written data
//   - reindex bucket with reindexed data
//   - main bucket unchanged (still serving queries)
//   - sentinel state: IsReindexed()
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

// runShardLifecycle is the shared body of RunOnShard / RunReindexOnlyOnShard.
// skipSwap=true sets the same-named flag for the duration of the call so the
// iteration loop stops after IsReindexed() — used for the barrier semantics
// of semantic migrations. skipSwap=false runs all the way through swap+tidy.
func (t *ShardReindexTaskGeneric) runShardLifecycle(ctx context.Context, shard ShardLike, skipSwap bool) error {
	if skipSwap {
		t.skipSwapOnFinish.Store(true)
		defer t.skipSwapOnFinish.Store(false)
	}

	concreteShard, err := unwrapShard(ctx, shard)
	if err != nil {
		return fmt.Errorf("unwrapping shard %q: %w", shard.Name(), err)
	}

	if err := t.OnAfterLsmInit(ctx, concreteShard); err != nil {
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
//   - MUST have completed RunReindexOnlyOnShard (IsReindexed() == true)
//     OR be re-entering a recovery state (IsPrepended() set but
//     IsMerged() not, or IsMerged() already set in which case this is
//     an idempotent no-op).
//
// Performs, per property:
//   - reindexBucket.FlushAndSwitch()  // memtable → immutable segments
//   - store.ShutdownBucket(reindexName)  // waits for compaction to
//     drain — this is the load-bearing slow step that #216 (the
//     "atomic phase actually atomic" design contract) requires
//     to live outside the per-shard atomic window
//   - ingestBucket.PrependSegmentsFromBucket(...)  // segment copy
//
// Then advances sentinels: markPrepended + removeReindexBucketsDirs
// + markMerged.
//
// Idempotent / sentinel-aware: if rt.IsMerged() already, returns nil.
// If rt.IsPrepended() && !rt.IsMerged() (mid-prep crash recovery),
// finishes the cleanup and returns. Safe to call repeatedly from
// rehydrate flows.
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
	concreteShard, err := unwrapShard(ctx, shard)
	if err != nil {
		return fmt.Errorf("unwrapping shard %q: %w", shard.Name(), err)
	}

	logger := t.logger.WithFields(map[string]any{
		"collection": concreteShard.Index().Config.ClassName.String(),
		"shard":      concreteShard.Name(),
		"method":     "RunPrepareOnShard",
	})

	rt, err := t.newReindexTracker(concreteShard.pathLSM())
	if err != nil {
		return fmt.Errorf("creating reindex tracker: %w", err)
	}

	// Idempotent fast-path: already merged means prep was already
	// completed (either by an earlier call in this process or by a
	// previous boot's runtimePrepare → markMerged).
	if rt.IsMerged() {
		logger.Debug("RunPrepareOnShard: already merged on disk; no-op")
		return nil
	}

	// Re-entry path: state-on-disk vs state-in-RAFT race (mirrors the
	// same check in RunSwapOnShard). If we land here with state
	// "started but not reindexed", resume the iteration before
	// preparing.
	if !rt.IsReindexed() {
		if !rt.IsStarted() {
			return fmt.Errorf("shard %q is not in reindexed state and has no started sentinel — no in-flight migration on disk", concreteShard.Name())
		}
		logger.Info("RunPrepareOnShard: state not yet reindexed on disk; resuming iteration before prep")
		if err := t.RunReindexOnlyOnShard(ctx, shard); err != nil {
			return fmt.Errorf("resume iteration before prep: %w", err)
		}
		rt, err = t.newReindexTracker(concreteShard.pathLSM())
		if err != nil {
			return fmt.Errorf("creating reindex tracker after iteration resume: %w", err)
		}
		if !rt.IsReindexed() {
			return fmt.Errorf("shard %q: iteration resume returned but IsReindexed still false", concreteShard.Name())
		}
	}

	props, err := t.readPropsToReindex(rt)
	if err != nil {
		return fmt.Errorf("reading props: %w", err)
	}
	if len(props) == 0 {
		return fmt.Errorf("no props found for prep on shard %q", concreteShard.Name())
	}

	if err := t.ensureReindexBucketsLoadedForSwap(ctx, logger, concreteShard, props); err != nil {
		return fmt.Errorf("ensure buckets loaded: %w", err)
	}

	return t.runtimePrepare(ctx, logger, shard, rt, props)
}

// RunSwapOnShard runs the swap+tidy+OnMigrationComplete phase.
//
// Preconditions:
//   - MUST have completed RunReindexOnlyOnShard (IsReindexed() == true).
//   - SHOULD use the same task instance that ran RunReindexOnlyOnShard
//     (preserves double-write callbacks registered during reindex).
//     The rehydrate path after a node restart violates this — see below.
//
// Recovery semantics (post-restart rehydrate path):
//
// This function is the cluster's authoritative completion path for
// semantic migrations, invoked by [ReindexProvider.OnGroupCompleted]
// once all units in the group are terminal. On a node that restarted
// inside the FINALIZING window (graceful rolling restart, OOM kill,
// k8s pod hardware failure, etc.), the previous in-process runtimeSwap
// may have crashed AFTER advancing one of the on-disk sentinels but
// BEFORE writing the next one:
//
//   - mid-Step-1 (ShutdownBucket failed under ctx.Canceled):
//     reindexed.mig set, no later sentinel. Reindex bucket dir still
//     on disk. On restart, OnAfterLsmInit re-loads the reindex bucket
//     and the original runtimeSwap path is valid.
//   - between markPrepended() and markMerged():
//     reindex bucket dirs partially or fully removed; segments are in
//     ingest_<gen>. runtimeSwap's "reindex bucket not found" error
//     fires here and the recovery path in this function takes over.
//   - between markMerged() and per-prop markSwappedProp():
//     reindex dirs gone; segments fully in ingest_<gen>. Dispatch to
//     [recoverRuntimeSwapBuckets] (dir-rename-based swap).
//   - between markSwapped() and markTidied():
//     in-memory swap done; backup_<gen> dirs still on disk. Dispatch
//     to [tidyBackupBuckets].
//   - IsTidied:
//     fully done. The previous run already wrote tidied.mig and
//     [FinalizeCompletedMigrations] at startup may have already
//     promoted the canonical dir and removed the tracker — but if it
//     didn't (race with task lifecycle), the per-shard
//     [strategy.OnMigrationComplete] still needs to run so the
//     in-process state aligns with the on-disk completion. Idempotent.
//
// Without this dispatch, a node killed past markPrepended() would
// re-fire OnGroupCompleted's rehydrate branch on restart, runtimeSwap
// would fail with "reindex bucket not found", the ack barrier would
// emit success=false, and the task would flip to FAILED cluster-wide
// while the OTHER replicas (whose acks already landed) have already
// swapped their buckets — producing the cluster-wide schema↔bucket
// inversion this dispatch is here to prevent.
func (t *ShardReindexTaskGeneric) RunSwapOnShard(ctx context.Context, shard ShardLike) error {
	concreteShard, err := unwrapShard(ctx, shard)
	if err != nil {
		return fmt.Errorf("unwrapping shard %q: %w", shard.Name(), err)
	}

	logger := t.logger.WithFields(map[string]any{
		"collection": concreteShard.Index().Config.ClassName.String(),
		"shard":      concreteShard.Name(),
		"method":     "RunSwapOnShard",
	})

	rt, err := t.newReindexTracker(concreteShard.pathLSM())
	if err != nil {
		return fmt.Errorf("creating reindex tracker: %w", err)
	}

	// State-on-disk vs state-in-RAFT race: a rolling restart that
	// landed inside the FINALIZING window can leave a node with the
	// unit's terminal state replicated in RAFT (the unit-completion
	// record landed before the crash) but the on-disk markReindexed
	// sentinel lost (file write didn't persist before SIGTERM, or the
	// race went the other way and the iteration was still running when
	// RAFT replicated some OTHER node's vote that flipped the group to
	// FINALIZING). The recovered task's [OnAfterLsmInit] (fired at
	// shard init by [shardReindexerV3RecoveryOnly]) only loads buckets
	// — it does NOT run the iteration. The iteration lives in
	// [OnAfterLsmInitAsync], which the recovery-only reindexer
	// intentionally no-ops.
	//
	// If we land here with state "started but not reindexed", we need
	// to resume the iteration before swapping. Calling
	// [RunReindexOnlyOnShard] is the right entry point: it runs
	// OnAfterLsmInit (idempotent for already-loaded buckets) and then
	// loops OnAfterLsmInitAsync until the iteration terminates and
	// markReindexed fires. After the call returns, we re-read the
	// tracker and continue with the sentinel-aware swap dispatch.
	//
	// This is the local repro path where the iteration didn't reach
	// markReindexed but the scheduler still scheduled the swap (e.g.
	// rolling restart caught the unit mid-iteration).
	if !rt.IsReindexed() {
		if !rt.IsStarted() {
			// Migration never started on this shard. This shouldn't
			// happen via OnGroupCompleted (the scheduler only invokes
			// this for units that were assigned to this node), but
			// surface it cleanly rather than silently completing.
			return fmt.Errorf("shard %q is not in reindexed state and has no started sentinel — no in-flight migration on disk", concreteShard.Name())
		}
		logger.Info("RunSwapOnShard: state not yet reindexed on disk; resuming iteration before swap")
		if err := t.RunReindexOnlyOnShard(ctx, shard); err != nil {
			return fmt.Errorf("resume iteration before swap: %w", err)
		}
		// Re-read tracker. The iteration should have set IsReindexed
		// (and possibly more — runtimeSwap MAY have run inline if
		// skipSwapOnFinish was somehow not honored, though it's set in
		// runShardLifecycle).
		rt, err = t.newReindexTracker(concreteShard.pathLSM())
		if err != nil {
			return fmt.Errorf("creating reindex tracker after iteration resume: %w", err)
		}
		if !rt.IsReindexed() {
			return fmt.Errorf("shard %q: iteration resume returned but IsReindexed still false", concreteShard.Name())
		}
	}

	props, err := t.readPropsToReindex(rt)
	if err != nil {
		return fmt.Errorf("reading props: %w", err)
	}
	if len(props) == 0 {
		return fmt.Errorf("no props found for swap on shard %q", concreteShard.Name())
	}

	// Sentinel-aware dispatch. The original runtimeSwap path is only
	// valid in the pre-prepend window — past markPrepended() the
	// reindex bucket dirs are gone and runtimeSwap's first lookup
	// fails with "reindex bucket not found". Each branch below picks
	// the appropriate recovery path so the rehydrate flow converges
	// to a fully tidied + migration-complete state regardless of which
	// sentinel the previous attempt last persisted.
	switch {
	case rt.IsTidied():
		// Fully done on disk. The in-process strategy state may still
		// need OnMigrationComplete (e.g. analyzer overlay clears) —
		// OnMigrationComplete is idempotent for the strategies we
		// support. Trim is best-effort and also idempotent.
		logger.WithField("props", props).Info("RunSwapOnShard: already tidied on disk; running OnMigrationComplete only")
		return t.finalizeMigrationAfterRecovery(ctx, logger, shard, rt, props)

	case rt.IsSwapped():
		// In-memory swap completed (per-prop dirs renamed); just tidy
		// backups and finalize.
		logger.WithField("props", props).Info("RunSwapOnShard: resuming from swapped state, tidying backups")
		if err := t.tidyBackupBuckets(ctx, logger, shard, rt, props); err != nil {
			return fmt.Errorf("recovery tidy: %w", err)
		}
		return t.finalizeMigrationAfterRecovery(ctx, logger, shard, rt, props)

	case rt.IsMerged():
		// Two sub-cases distinguish in-process happy path from post-
		// restart recovery under the prep/atomic/defer phase model:
		//
		//  - **In-process happy path** (OnGroupCompleted called
		//    RunPrepareOnShard first): ingest buckets are loaded in
		//    the LSM store. Do the in-memory atomic SwapBucketPointer
		//    via [runtimeSwap]. Disk rename is deferred to next
		//    startup via OnBeforeLsmInit's recoverRuntimeSwapBuckets.
		//  - **Recovery fallback**: ingest buckets are NOT loaded.
		//    In practice this is rare — post-restart, OnBeforeLsmInit's
		//    IsMerged && !IsSwapped branch typically does the disk
		//    rename before LSM init touches the main bucket name,
		//    advancing the state to IsSwapped. We only land here in
		//    the merged state with ingest buckets unloaded if that
		//    branch was skipped (e.g. swapBuckets config disabled).
		//    Use the dir-rename swap to converge.
		if t.ingestBucketsLoaded(shard, props) {
			logger.WithField("props", props).Info("RunSwapOnShard: resuming from merged state, in-memory atomic swap")
			return t.runtimeSwap(ctx, logger, shard, rt, props)
		}
		logger.WithField("props", props).Info("RunSwapOnShard: resuming from merged state without loaded ingest buckets, recovering swap via dir renames")
		if err := t.recoverRuntimeSwapBuckets(ctx, logger, shard, rt, props); err != nil {
			return fmt.Errorf("recovery swap: %w", err)
		}
		if err := t.tidyBackupBuckets(ctx, logger, shard, rt, props); err != nil {
			return fmt.Errorf("recovery tidy after swap: %w", err)
		}
		return t.finalizeMigrationAfterRecovery(ctx, logger, shard, rt, props)

	case rt.IsPrepended():
		// Mid-prepend crash: segments are in ingest_<gen> (the
		// PrependSegmentsFromBucket loop completed because markPrepended
		// is set), but reindex dirs may still be partially on disk and
		// markMerged() did not run. Finish the removal, advance the
		// sentinel, then dispatch to the dir-rename-based recovery.
		logger.WithField("props", props).Info("RunSwapOnShard: resuming from prepended state, completing merge then recovering swap")
		if err := t.removeReindexBucketsDirs(ctx, logger, shard, props); err != nil {
			return fmt.Errorf("recovery remove reindex dirs: %w", err)
		}
		if err := rt.markMerged(); err != nil {
			return fmt.Errorf("recovery markMerged: %w", err)
		}
		if err := t.recoverRuntimeSwapBuckets(ctx, logger, shard, rt, props); err != nil {
			return fmt.Errorf("recovery swap from prepended: %w", err)
		}
		if err := t.tidyBackupBuckets(ctx, logger, shard, rt, props); err != nil {
			return fmt.Errorf("recovery tidy from prepended: %w", err)
		}
		return t.finalizeMigrationAfterRecovery(ctx, logger, shard, rt, props)
	}

	// Default: pre-prepend state (only reindexed.mig set). Under the
	// prep/atomic/defer phase model, the happy-path caller is
	// [reindex_provider.OnGroupCompleted], which invokes
	// RunPrepareOnShard BEFORE RunSwapOnShard so the prep work runs
	// OUTSIDE the per-shard tokenization-overlay window. Reaching this
	// branch via OnGroupCompleted's flow means rehydrate happened but
	// RunPrepareOnShard hasn't — call it defensively, but note that
	// the atomic-window contract is no longer met (prep runs inside
	// the overlay window). Acceptable for tests and edge cases where
	// FINALIZING-window query correctness isn't being asserted.
	logger.WithField("props", props).Info("starting prep+swap phase (caller did not invoke RunPrepareOnShard separately)")

	if err := t.ensureReindexBucketsLoadedForSwap(ctx, logger, concreteShard, props); err != nil {
		return fmt.Errorf("ensure buckets loaded: %w", err)
	}

	if err := t.runtimePrepare(ctx, logger, shard, rt, props); err != nil {
		return fmt.Errorf("runtime prepare: %w", err)
	}

	if err := t.runtimeSwap(ctx, logger, shard, rt, props); err != nil {
		return fmt.Errorf("runtime swap: %w", err)
	}

	return nil
}

// ingestBucketsLoaded reports whether the ingest buckets for every
// prop are currently loaded in the shard's LSM store. Used by the
// IsMerged dispatch in [RunSwapOnShard] to distinguish the in-process
// happy path (RunPrepareOnShard just ran, ingest buckets are warm,
// do in-memory SwapBucketPointer) from the recovery fallback (ingest
// buckets aren't loaded, dir-rename swap via recoverRuntimeSwapBuckets).
func (t *ShardReindexTaskGeneric) ingestBucketsLoaded(shard ShardLike, props []string) bool {
	store := shard.Store()
	for _, propName := range props {
		if store.Bucket(t.ingestBucketName(propName)) == nil {
			return false
		}
	}
	return true
}

// ensureReindexBucketsLoadedForSwap defensively loads any reindex or
// ingest buckets that are missing from the in-memory store but whose
// directories still exist on disk. This protects the pre-prepend
// runtimeSwap path on the rehydrate flow from a class of state-divergence
// races between in-memory bucket state and on-disk reindex state:
//
//   - A previous in-process runtimeSwap was interrupted mid-flight
//     by ctx.Canceled (graceful shutdown) at Step 1 or Step 2. The
//     interrupted ShutdownBucket may have removed the reindex bucket
//     from the store's bucket map without persisting a sentinel
//     advance, and the cancellation can leave compaction callbacks
//     unregistered partway through the unhook sequence.
//   - On restart, the recovery-only shard reindexer's RunBeforeLsmInit
//     is intentionally a no-op (see [shardReindexerV3RecoveryOnly])
//     and the shard-registered recovery task's OnAfterLsmInit is the
//     only re-load hook. If for any reason the bucket name lookup in
//     [runtimeSwap]'s first iteration misses (lsm store re-init,
//     concurrent bucket shutdown, cached-task vs fresh-task pointer
//     differences after the rehydrate path's [createReindexTasks]),
//     runtimeSwap fails with "reindex bucket not found" before any
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
		if store.Bucket(reindexName) == nil &&
			dirExists(filepath.Join(lsmPath, reindexName)) {
			missingReindex = append(missingReindex, propName)
		}
		ingestName := t.ingestBucketName(propName)
		if store.Bucket(ingestName) == nil &&
			dirExists(filepath.Join(lsmPath, ingestName)) {
			missingIngest = append(missingIngest, propName)
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

// finalizeMigrationAfterRecovery runs the strategy's OnMigrationComplete
// hook and trims older on-disk generations. This is the rehydrate-path
// equivalent of runtimeSwap's final two steps (lines 1103/1124),
// invoked by the recovery branches in [RunSwapOnShard] which don't go
// through runtimeSwap.
//
// Best-effort on trim — failures are logged, not returned, matching
// the trim policy at the end of runtimeSwap.
func (t *ShardReindexTaskGeneric) finalizeMigrationAfterRecovery(
	ctx context.Context, logger logrus.FieldLogger, shard ShardLike,
	rt reindexTracker, props []string,
) error {
	if err := t.strategy.OnMigrationComplete(ctx, shard); err != nil {
		return fmt.Errorf("on migration complete: %w", err)
	}
	t.trimOlderGenerationsLocked(logger, shard, rt, props)
	logger.Info("RunSwapOnShard: recovery path complete")
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

func (t *ShardReindexTaskGeneric) OnBeforeLsmInit(ctx context.Context, shard *Shard) (err error) {
	collectionName := shard.Index().Config.ClassName.String()
	shardName := shard.Name()
	logger := t.logger.WithFields(map[string]any{
		"collection": collectionName,
		"shard":      shardName,
		"method":     "OnBeforeLsmInit",
	})
	logger.Info("starting")
	defer func(started time.Time) {
		logger = logger.WithField("took", time.Since(started))
		if err != nil {
			logger.Errorf("finished with error: %v", err)
		} else {
			logger.Info("finished")
		}
	}(time.Now())

	if !t.isShardSelected(collectionName, shardName) {
		logger.Debug("different collection/shard selected. nothing to do")
		return nil
	}

	rt, err := t.newReindexTracker(shard.pathLSM())
	if err != nil {
		err = fmt.Errorf("creating reindex tracker: %w", err)
		return err
	}

	rt.checkOverrides(logger, &t.config)

	if rt.IsRollback() {
		// make it so it "survives" the rt.reset()
		t.config.rollback = true
	}

	if rt.IsReset() && rt.IsTidied() {
		rt.reset()
		err = fmt.Errorf("reset was manually triggered")
		return err
	}

	if t.config.conditionalStart && !rt.HasStartCondition() {
		err = fmt.Errorf("conditional start is set, but file trigger is not found")
		return err
	}

	props, err := t.readPropsToReindex(rt)
	if err != nil {
		err = fmt.Errorf("reading reindexable props: %w", err)
		return err
	}

	if t.config.rollback {
		logger.Debugf("rollback started: config=%v, runtime=%v", t.config.rollback, rt.IsRollback())

		if rt.IsTidied() {
			err = fmt.Errorf("rollback: backup buckets are deleted, can not restore")
			return err
		}
		if rt.IsSwapped() {
			if err = t.unswapIngestAndBackupBuckets(ctx, logger, shard, rt, props); err != nil {
				err = fmt.Errorf("rollback: unswapping buckets: %w", err)
				return err
			}
		}
		if err = t.removeReindexBucketsDirs(ctx, logger, shard, props); err != nil {
			err = fmt.Errorf("rollback: removing reindex buckets: %w", err)
			return err
		}
		if err = t.removeIngestBucketsDirs(ctx, logger, shard, props); err != nil {
			err = fmt.Errorf("rollback: removing ingest buckets: %w", err)
			return err
		}
		if err = rt.reset(); err != nil {
			err = fmt.Errorf("rollback: removing migration files: %w", err)
			return err
		}

		logger.Debug("rollback completed")
		return nil
	}

	if len(props) == 0 {
		logger.Debug("no props read. nothing to do")
		return nil
	}

	if err = ctx.Err(); err != nil {
		err = fmt.Errorf("context check (1): %w / %w", err, context.Cause(ctx))
		return err
	}

	// Torn-state recovery (same shape as [OnAfterLsmInit]): if rt.IsReindexed()
	// claims the iteration completed but the reindex bucket dirs that
	// markReindexed() must have populated are missing on disk, the sentinel
	// is forged or the prior run died before any data reached disk. Without
	// this, mergeReindexAndIngestBuckets below would either fail loudly with
	// "bucket not found" (best case) or quietly merge nothing (worst case),
	// leading to a swap of an empty bucket and a silent data-loss outcome
	// once OnMigrationComplete flips the schema flag.
	//
	// Only safe to gate on missing dirs in the pre-prepend window — once
	// markPrepended() has run, the dirs are intentionally removed by
	// [runtimeSwap]; a missing dir then is correct.
	if rt.IsReindexed() && !rt.IsPrepended() && !rt.IsMerged() && !rt.IsSwapped() && !rt.IsTidied() {
		if missing := t.firstMissingReindexBucketDir(shard.pathLSM(), props); missing != "" {
			logger.WithField("missing_bucket_dir", missing).
				Errorf("torn migration state at OnBeforeLsmInit: reindexed.mig sentinel exists but reindex bucket dir %q is missing on disk; resetting reindexed sentinel so iteration runs again from scratch", missing)
			if uerr := rt.unmarkReindexed(); uerr != nil {
				err = fmt.Errorf("torn-state recovery: removing stale reindexed.mig: %w", uerr)
				return err
			}
		}
	}

	isMerged := rt.IsMerged()
	if !isMerged && rt.IsReindexed() {
		if rt.IsPrepended() {
			// Runtime swap already prepended reindex segments into ingest.
			// Just clean up the reindex dirs (may already be gone) and mark merged.
			logger.Debug("reindexed and prepended, not merged. removing reindex dirs")

			if err = t.removeReindexBucketsDirs(ctx, logger, shard, props); err != nil {
				err = fmt.Errorf("removing reindex bucket dirs: %w", err)
				return err
			}
			if err = rt.markMerged(); err != nil {
				err = fmt.Errorf("marking merged after prepend recovery: %w", err)
				return err
			}
		} else {
			logger.Debug("reindexed, not merged. merging buckets")

			if err = t.mergeReindexAndIngestBuckets(ctx, logger, shard, rt, props); err != nil {
				err = fmt.Errorf("merging reindex and ingest buckets:%w", err)
				return err
			}
		}
		isMerged = true
	}

	if err = ctx.Err(); err != nil {
		err = fmt.Errorf("context check (2): %w / %w", err, context.Cause(ctx))
		return err
	}

	isSwapped := rt.IsSwapped()
	isTidied := rt.IsTidied()
	isPrepended := rt.IsPrepended()
	if isMerged {
		if isSwapped {
			if t.config.unswapBuckets && !isPrepended {
				if isTidied {
					logger.Debug("swapped and tidied. can not be unswapped")
				} else {
					logger.Debug("swapped, not tidied. unswapping buckets")

					if err = t.unswapIngestAndBackupBuckets(ctx, logger, shard, rt, props); err != nil {
						err = fmt.Errorf("unswapping ingest and backup buckets:%w", err)
						return err
					}
					isSwapped = false
				}
			}
		} else {
			if isPrepended {
				// Crash recovery for runtime swap: the merge was done via
				// PrependSegmentsFromBucket so the on-disk layout may differ
				// from the restart-based swap (old main may already be renamed
				// to backup for some props). Use a recovery-aware swap.
				logger.Debug("prepended, merged, not swapped. recovering runtime swap")

				if err = t.recoverRuntimeSwapBuckets(ctx, logger, shard, rt, props); err != nil {
					err = fmt.Errorf("recovering runtime swap buckets: %w", err)
					return err
				}
				isSwapped = true
			} else if t.config.swapBuckets {
				logger.Debug("merged, not swapped. swapping buckets")

				if err = t.swapIngestAndBackupBuckets(ctx, logger, shard, rt, props); err != nil {
					err = fmt.Errorf("swapping ingest and backup buckets:%w", err)
					return err
				}
				isSwapped = true
			}
		}
	}

	if err = ctx.Err(); err != nil {
		err = fmt.Errorf("context check (3): %w / %w", err, context.Cause(ctx))
		return err
	}

	if isSwapped {
		if isTidied {
			// Pre-existing IsTidied on entry: either this run just
			// finished tidyBackupBuckets, OR a previous run crashed
			// between markTidied and the PreReindexHook fire below.
			// In the latter case the target bucket would otherwise
			// stay unloaded and OnAfterLsmInitAsync's safety check
			// refuses OnMigrationComplete — replica stuck. The hook
			// is idempotent; firing it unconditionally closes the
			// narrow markTidied-to-hook crash window.
			// weaviate/0-weaviate-issues#246.
			t.strategy.PreReindexHook(shard, props)
			logger.Debug("tidied. nothing to do")
			return nil
		}

		if t.config.tidyBuckets {
			logger.Debug("swapped, not tidied. tidying buckets")

			if err = t.tidyBackupBuckets(ctx, logger, shard, rt, props); err != nil {
				err = fmt.Errorf("tidying backup buckets:%w", err)
				return err
			}

			// Recovery just transitioned us into IsTidied. Same
			// reasoning as the IsTidied-on-entry branch above —
			// load the target bucket before returning.
			t.strategy.PreReindexHook(shard, props)
		}
	}

	return nil
}

func (t *ShardReindexTaskGeneric) OnAfterLsmInit(ctx context.Context, shard *Shard) (err error) {
	collectionName := shard.Index().Config.ClassName.String()
	shardName := shard.Name()
	logger := t.logger.WithFields(map[string]any{
		"collection": collectionName,
		"shard":      shardName,
		"method":     "OnAfterLsmInit",
	})
	logger.Info("starting")
	defer func(started time.Time) {
		logger = logger.WithField("took", time.Since(started))
		if err != nil {
			logger.Errorf("finished with error: %v", err)
		} else {
			logger.Info("finished")
		}
	}(time.Now())

	// skip shard only if not started or rollback requested
	// otherwise double writes have to be enabled if migration was already started
	isShardSelected := t.isShardSelected(collectionName, shardName)

	if t.config.rollback && isShardSelected {
		logger.Debug("rollback. nothing to do")
		return nil
	}

	rt, err := t.newReindexTracker(shard.pathLSM())
	if err != nil {
		err = fmt.Errorf("creating reindex tracker: %w", err)
		return err
	}

	rt.checkOverrides(logger, &t.config)

	if rt.IsRollback() {
		logger.Debug("rollback. nothing to do")
		return err
	}

	if t.config.conditionalStart && !rt.HasStartCondition() {
		err = fmt.Errorf("conditional start is set, but file trigger is not found")
		return err
	}

	isStarted := rt.IsStarted()
	if !isStarted && !isShardSelected {
		logger.Debug("different collection/shard selected. nothing to do")
		return nil
	}

	props, err := t.getPropsToReindex(shard, rt)
	if err != nil {
		err = fmt.Errorf("getting reindexable props: %w", err)
		return err
	}
	logger.WithField("props", props).Debug("props found")
	if len(props) == 0 {
		logger.Debug("no props found. nothing to do")
		return nil
	}

	// NOTE: markStarted is intentionally NOT called here. The reindexStarted
	// timestamp is the iterator-skip predicate (the backfill iterator skips
	// objects with LastUpdateTimeUnix >= reindexStarted, assuming the
	// double-write callbacks mirror them into the ingest bucket). It is
	// therefore only safe to capture AFTER registerDoubleWriteCallbacks has
	// run — see the markStarted call at the end of this function. Capturing
	// it here (the pre-fix ordering) opened a seconds-wide window between
	// the timestamp snapshot and callback registration (bucket loading does
	// real disk I/O in between) during which a live write was BOTH skipped
	// by the iterator AND not double-written — permanently lost from the
	// target bucket (weaviate/weaviate#11688).

	// Torn-state recovery: if rt.IsReindexed() is true but the reindex
	// bucket dirs that markReindexed() must have populated are missing on
	// disk, the sentinel lies. This happens when:
	//
	//   - a previous reindex submission was crash-killed between
	//     markReindexed() and the first runtimeSwap step (so no later
	//     sentinel was written and no swap-side effects landed), AND
	//     the reindex bucket dirs were never created — e.g. an OnAfterLsmInit
	//     that ran with the buckets evicted, or a manual/forged sentinel;
	//
	//   - operator surgery (rare) wrote a reindexed.mig without running the
	//     actual iteration.
	//
	// Without this check, the next OnAfterLsmInitAsync hits the
	// `if rt.IsReindexed() { ... "nothing to do" ... }` short-circuit at
	// the top of the loop, the iteration is skipped, the runtime swap
	// runs against an EMPTY reindex bucket, and the migration reports
	// FINISHED while the target bucket is silently empty. Schema flag
	// flips on top of no data — Sev 1 silent failure.
	//
	// The reindex bucket dirs are only safe to expect on disk during the
	// pre-prepend window: IsReindexed && !IsPrepended && !IsMerged &&
	// !IsSwapped. Once markPrepended() has run, the dirs are intentionally
	// removed by [runtimeSwap] (the segments live in the ingest bucket from
	// then on), so a missing dir past that point is correct, not torn.
	if rt.IsReindexed() && !rt.IsPrepended() && !rt.IsMerged() && !rt.IsSwapped() && !rt.IsTidied() {
		if missing := t.firstMissingReindexBucketDir(shard.pathLSM(), props); missing != "" {
			logger.WithField("missing_bucket_dir", missing).
				Errorf("torn migration state: reindexed.mig sentinel exists but reindex bucket dir %q is missing on disk; assuming the prior reindex never wrote any data and resetting sentinel so iteration runs again", missing)
			if uerr := rt.unmarkReindexed(); uerr != nil {
				err = fmt.Errorf("torn-state recovery: removing stale reindexed.mig: %w", uerr)
				return err
			}
		}
	}

	if rt.IsSwapped() {
		if !rt.IsTidied() {
			logger.Debug("swapped, not tidied. starting backup buckets")

			if err = t.loadBackupBuckets(ctx, logger, shard, props); err != nil {
				err = fmt.Errorf("starting backup buckets:%w", err)
				return err
			}
			if _, err = t.registerDoubleWriteCallbacks(shard, props, t.backupBucketName, false); err != nil {
				err = fmt.Errorf("registering backup double-write callbacks: %w", err)
				return err
			}
		}
	} else {
		isMerged := rt.IsMerged()
		if isMerged {
			logger.Debug("merged, not swapped. starting ingest buckets")
		} else {
			// Load reindex buckets whenever they are expected to still be
			// on disk. They are removed during runtimeSwap (between
			// markPrepended and markMerged); once IsPrepended is set the
			// dirs may be gone. We must load them on a resume from
			// "IsReindexed && !IsPrepended" just as on a fresh start
			// (!IsReindexed). The previous condition only handled the
			// fresh-start case, so after a crash between markReindexed and
			// runtimeSwap a follow-up RunSwapOnShard would fail with
			// "reindex bucket not found" even though the dirs were still
			// on disk.
			if !rt.IsPrepended() {
				if !rt.IsReindexed() {
					logger.Debug("not reindexed. starting reindex buckets")
				} else {
					logger.Debug("reindexed, not prepended. resuming reindex buckets from disk")
				}

				if err = t.loadReindexBuckets(ctx, logger, shard, props); err != nil {
					err = fmt.Errorf("starting reindex buckets: %w", err)
					return err
				}
			}

			logger.Debug("not merged. starting ingest buckets")
		}

		t.strategy.PreReindexHook(shard, props)

		// since reindex bucket will be merged into ingest bucket with reindex segments being before ingest,
		// ingest segments should not be compacted and tombstones should be kept
		if err = t.loadIngestBuckets(ctx, logger, shard, props, !isMerged, !isMerged); err != nil {
			err = fmt.Errorf("starting ingest buckets:%w", err)
			return err
		}
		if t.onBeforeDoubleWriteRegistration != nil {
			t.onBeforeDoubleWriteRegistration()
		}
		disableJustRegistered, err := t.registerDoubleWriteCallbacks(shard, props, t.ingestBucketName, true)
		if err != nil {
			err = fmt.Errorf("registering ingest double-write callbacks: %w", err)
			return err
		}

		// Capture the reindexStarted timestamp ONLY NOW, strictly after the
		// double-write callbacks are active. The iteration loop skips every
		// object with LastUpdateTimeUnix >= reindexStarted on the assumption
		// that such writes were double-written into the ingest bucket; with
		// the timestamp captured after registration, that assumption holds
		// for every write — there is no gap in which a write is both skipped
		// by the iterator and missed by the callbacks.
		//
		// The timestamp is rounded UP to the next millisecond boundary:
		// LastUpdateTimeUnix has millisecond resolution and the iterator's
		// predicate is `tsMillis < reindexStarted.UnixMilli()`. Without the
		// ceil, a write landing just before registration could share the
		// truncated millisecond of the timestamp, get skipped, and not be
		// double-written. Rounding up guarantees every skipped write
		// happened at/after the millisecond boundary, which is at/after
		// callback registration.
		//
		// Writes in the overlap window (after registration, before the
		// timestamp) are BOTH double-written into ingest AND visited by the
		// backfill iterator (their LastUpdateTimeUnix is below the
		// threshold). This converges for every migration strategy because
		// the reindex segments are prepended BEFORE the ingest segments
		// (PrependSegmentsFromBucket), so the double-written ingest entries
		// are strictly newer in LSM merge order, and each strategy's writes
		// are idempotent per key:
		//   - columnar: ColumnarPut* is replace-per-docID; newer ingest
		//     entry wins (both carry the same post-write value anyway)
		//   - rangeable: RoaringSetRangeAdd of (value, docID) is set-
		//     idempotent; ingest-layer deletes shadow older backfill adds
		//   - searchable/filterable (map/set/inverted): identical
		//     (term, docID) entries merge to one; ingest-layer tombstones
		//     shadow older backfill entries
		if !isStarted {
			startedAt := time.Now().Truncate(time.Millisecond).Add(time.Millisecond)
			if err = rt.markStarted(startedAt); err != nil {
				// Disable only the pair registered above — the task instance
				// may hold active registrations for other shards.
				disableJustRegistered()
				err = fmt.Errorf("marking reindex started: %w", err)
				return err
			}
		}
	}

	return nil
}

func (t *ShardReindexTaskGeneric) OnAfterLsmInitAsync(ctx context.Context, shard ShardLike,
) (rerunAt time.Time, reloadShard bool, err error) {
	collectionName := shard.Index().Config.ClassName.String()
	shardName := shard.Name()
	logger := t.logger.WithFields(map[string]any{
		"collection": collectionName,
		"shard":      shardName,
		"method":     "OnAfterLsmInitAsync",
	})
	logger.Info("starting")
	defer func(started time.Time) {
		logger = logger.WithField("took", time.Since(started))
		if err != nil {
			logger.Errorf("finished with error: %v", err)
		} else {
			logger.Info("finished")
		}
	}(time.Now())

	zerotime := time.Time{}

	if !t.isShardSelected(collectionName, shardName) {
		logger.Debug("different collection/shard selected. nothing to do")
		return zerotime, false, nil
	}

	rt, err := t.newReindexTracker(shard.pathLSM())
	if err != nil {
		err = fmt.Errorf("creating reindex tracker: %w", err)
		return zerotime, false, err
	}

	rt.checkOverrides(logger, &t.config)

	// rollback initiated by the user after restart, stop double writes
	if rt.IsRollback() {
		logger.Debug("rollback started")
		props, err2 := t.readPropsToReindex(rt)
		if err2 != nil {
			err = fmt.Errorf("reading reindexable props for rollback: %w", err2)
			return zerotime, false, err
		}
		err = nil

		if !rt.IsSwapped() {
			err = t.unloadReindexBuckets(ctx, logger, shard, props)
			if err != nil {
				err = fmt.Errorf("unloading reindex buckets: %w", err)
				return zerotime, false, err
			}
			logger.Info("reindex buckets unloaded")
			err = t.unloadIngestBuckets(ctx, logger, shard, props)
			if err != nil {
				err = fmt.Errorf("unloading ingest buckets: %w", err)
				return zerotime, false, err
			}
			logger.Info("ingest buckets unloaded")
		} else {
			logger.Warnf("inverted bucket is being used for search, will not be unloaded: %s. Rollback will proceed on restart", shard.Name())
		}
		// return early to stop ingestion
		return zerotime, false, nil
	}

	if t.config.rollback {
		logger.Debug("rollback. nothing to do")
		return zerotime, false, nil
	}

	if t.config.conditionalStart && !rt.HasStartCondition() {
		err = fmt.Errorf("conditional start is set, but file trigger is not found")
		return zerotime, false, err
	}

	props, err := t.readPropsToReindex(rt)
	if err != nil {
		err = fmt.Errorf("reading reindexable props: %w", err)
		return zerotime, false, err
	}

	if rt.IsPaused() {
		logger.Debug("paused. waiting for resuming")
		return time.Now().Add(t.config.pauseDuration), false, nil
	}

	if rt.IsTidied() {
		// Defense in depth: rt.IsTidied()=true means a previous run reported
		// a successful migration on this shard. The expected post-migration
		// state is the strategy's target bucket existing and populated. If
		// the bucket is missing now (e.g. a DELETE removed the index between
		// that previous run and this re-trigger), the on-disk sentinel lies
		// — calling OnMigrationComplete here would re-flip the schema flag
		// to true and report success, while the customer's index is in fact
		// empty. Fail loudly instead.
		for _, propName := range props {
			bucketName := t.strategy.SourceBucketName(propName)
			if shard.Store().Bucket(bucketName) == nil {
				err = fmt.Errorf(
					"stale migration state on shard %q: tidied sentinel claims property %q is complete, but target bucket %q is missing — usually caused by a DELETE between the previous successful reindex and this one; refusing to silently report success",
					shard.Name(), propName, bucketName)
				return zerotime, false, err
			}
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

	if rt.IsReindexed() {
		logger.Debug("reindexed. nothing to do")
		return zerotime, false, nil
	}

	var reindexStarted time.Time
	if !rt.IsStarted() {
		err = fmt.Errorf("missing reindex started")
		return zerotime, false, err
	} else if reindexStarted, err = rt.getStarted(); err != nil {
		err = fmt.Errorf("getting reindex started: %w", err)
		return zerotime, false, err
	}

	var lastStoredKey indexKey
	if lastStoredKey, _, err = rt.GetProgress(); err != nil {
		err = fmt.Errorf("getting reindex progress: %w", err)
		return zerotime, false, err
	}

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

	defer func() {
		if err != nil && !bytes.Equal(lastStoredKey.Bytes(), lastProcessedKey.Bytes()) {
			logger.WithField("last_processed_key", lastProcessedKey).Debug("marking progress on error")
			rt.markProgress(lastProcessedKey, processedCount, indexedCount)
		}
	}()

	store := shard.Store()
	propExtraction := storobj.NewPropExtraction()
	bucketsByPropName := map[string]*lsmkv.Bucket{}
	for _, prop := range props {
		propExtraction.Add(prop)
		bucketName := t.reindexBucketName(prop)
		bucketsByPropName[prop] = store.Bucket(bucketName)
	}

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

			breakCh <- processedCount%t.config.checkProcessingEveryNoObjects == 0 && (time.Since(processingStarted) > t.config.processingDuration || rt.IsPaused())
			time.Sleep(t.config.perObjectDelay)
		}
	}
	if !bytes.Equal(lastStoredKey.Bytes(), lastProcessedKey.Bytes()) {
		if err := rt.markProgress(lastProcessedKey, processedCount, indexedCount); err != nil {
			err = fmt.Errorf("marking reindex progress: %w", err)
			return zerotime, false, err
		}
		lastStoredKey = lastProcessedKey.Clone()
	}
	if finished {
		// Durability barrier: flush every per-property reindex bucket's
		// memtable to a segment BEFORE writing markReindexed. Without
		// this, a SIGKILL / pod hardware failure / OOM kill between
		// markReindexed and the eventual [runtimeSwap] Step 1
		// (FlushAndSwitch) loses any in-memtable writes — the
		// markReindexed sentinel falsely claims iteration is complete
		// while the underlying re-tokenized rows are still in volatile
		// memory. On restart, the recovery path sees IsReindexed=true,
		// skips re-iterating, and runtimeSwap prepends a truncated
		// reindex bucket into ingest. The cluster schema then flips to
		// the new tokenization, the canonical bucket on this replica
		// is missing the lost rows, and queries return per-replica
		// divergent counts.
		//
		// FlushAndSwitch's contract is durability: every write that
		// returned before the call is in a segment file (fsynced) by
		// the time FlushAndSwitch returns. Doing it here closes the
		// "markReindexed happens-before durability" gap unique to the
		// FINALIZING-barrier path, where the inline runtimeSwap is
		// deferred and a crash window opens between markReindexed and
		// the eventual flush. The inline path (skipSwapOnFinish=false)
		// also benefits — markReindexed now strictly happens-after
		// durable persistence regardless of which path runs next.
		//
		// Failure mode the durability barrier prevents: a SIGKILL
		// between markReindexed and the eventual flush leaves rows
		// that iterated successfully but never made it to segments,
		// surfacing as per-replica `path = N/M` divergence on the
		// killed node.
		for propName, bucket := range bucketsByPropName {
			if bucket == nil {
				continue
			}
			if err = bucket.FlushAndSwitch(); err != nil {
				err = fmt.Errorf("flushing reindex bucket for prop %q before markReindexed: %w", propName, err)
				return zerotime, false, err
			}
		}
		if err = rt.markReindexed(); err != nil {
			err = fmt.Errorf("marking reindexed: %w", err)
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
		if err = t.runtimePrepare(ctx, logger, shard, rt, props); err != nil {
			err = fmt.Errorf("runtime prepare: %w", err)
			return zerotime, false, err
		}
		if err = t.runtimeSwap(ctx, logger, shard, rt, props); err != nil {
			err = fmt.Errorf("runtime swap: %w", err)
			return zerotime, false, err
		}
		return zerotime, false, nil
	}
	return time.Now().Add(t.config.pauseDuration), false, nil
}

// runtimeSwap implements Phase 2 of the runtime swap path. See the
// file-level phase-contract godoc for the full prep/atomic/defer
// design. The implementation is partitioned into 2a / 2b / 2c
// sub-phases with HARD boundaries — do not move work between them
// without re-reading the contract.
//
// Phase 2a — atomic per-prop SwapBucketPointer loop. MUST stay
// microseconds total. This bounds the per-shard "mixed-state"
// subwindow (some props swapped, others not) during which queries
// to not-yet-swapped props would tokenize input with the new value
// against an old-tokenized bucket. Only allowed work: in-memory
// pointer flip + single-fsync sentinel mark.
//
//   - store.SwapBucketPointer(mainName, ingestName) per prop
//   - rt.markSwappedProp(propName) per prop
//
// Forbidden in 2a: anything that can block (disk I/O, lock
// contention, RAFT calls, compaction waits). A guard test must
// catch regressions where someone adds a yield point between two
// SwapBucketPointer calls.
//
// Phase 2b — post-atomic inline tidy. Slow but correctness-safe:
// every prop is already in-memory-swapped, so the mixed-state
// subwindow is closed and queries see all-new buckets with the
// overlay still active.
//
//   - oldMainBucket.Shutdown(ctx) per prop (REQUIRED INLINE — see
//     below)
//   - os.Rename(oldMainDir, backupDir) per prop (safe inline — the
//     load-bearing rule is: only rename a shut-down bucket, never
//     rename a live bucket that is serving queries)
//   - rt.markSwapped
//   - rt.markTidied
//
// Why oldMain.Shutdown MUST be inline (not deferred to next-startup
// like the live-ingest rename): Bucket.Shutdown is the only call
// that removes the bucket's path from GlobalBucketRegistry (see
// lsmkv/bucket.go Shutdown defer of Remove(b.GetDir())). After
// SwapBucketPointer the old bucket is no longer in the store's
// bucketsByName map, so Store.Shutdown's iteration will not call
// its Shutdown. Without an inline Shutdown the old bucket's path
// remains in the process-wide registry indefinitely, and any
// subsequent in-process shard init that tries to register a bucket
// at the same canonical name (shard reload, lazy-load unwrap,
// second migration on the same shard) fails with
// ErrBucketAlreadyRegistered. The unit test
// TestMapToBlockmaxMigration_RuntimeSwap_ThenRestart reproduces
// this if the inline Shutdown is removed.
//
// Phase 2c — post-atomic inline finalize.
//
//   - OnMigrationComplete (per-strategy hook; see
//     [MigrationStrategy.OnMigrationComplete] godoc for the
//     per-strategy contract)
//   - trimOlderGenerationsLocked (removes the current gen's backup
//     dir + every older gen's sidecars)
//
// Live-bucket rename (Phase 3): the ingest bucket whose pointer was
// flipped into the canonical slot is STILL at __ingest_<gen>/ on
// disk. That rename to the canonical name is deferred to next
// startup via [FinalizeCompletedMigrations], because renaming a
// dir whose mmaps are open would corrupt the segment registry.
//
// Disable double-write callbacks via a defer at the top of the
// function so callbacks stop on every exit path. Same-process
// retry of runtimeSwap is not supported (the in-memory bucket
// state is partially mutated); recovery after a mid-swap crash
// happens on the next node restart via OnBeforeLsmInit.
// runtimePrepare runs the Phase 1 (background-safe) preparation work
// that used to be inlined into runtimeSwap.
//
// Performs, per property:
//   - reindexBucket.FlushAndSwitch()            // memtable → segments
//   - store.ShutdownBucket(reindexName)         // drains compaction
//   - ingestBucket.PrependSegmentsFromBucket(...) // segment copy
//
// Then advances sentinels: markPrepended + removeReindexBucketsDirs
// + markMerged.
//
// Bucket=OLD and schema=OLD throughout — queries on the live main
// bucket continue correctly. The per-shard tokenization overlay
// MUST NOT yet be set: setting it before this call would expose the
// very gap the overlay was supposed to close (query input
// tokenized as NEW against the still-OLD bucket while prep does
// disk I/O for seconds).
//
// Sentinel-aware: if rt.IsPrepended() is true (crash mid-prep) we
// skip the per-prop loop and finish the merge-cleanup steps only.
// The caller checks rt.IsMerged() before calling.
//
// Crash safety: markPrepended is set after the per-prop loop but
// BEFORE removeReindexBucketsDirs. A crash in that window leaves
// IsPrepended=true with reindex dirs partially removed; on restart,
// either the recovery path here or OnBeforeLsmInit's prepended-but-
// not-merged branch finishes the cleanup.
func (t *ShardReindexTaskGeneric) runtimePrepare(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, rt reindexTracker, props []string,
) error {
	store := shard.Store()

	if rt.IsPrepended() {
		logger.Debug("runtime prepare: already prepended on disk; finishing merge cleanup only")
	} else {
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

		// Mark prepended before removing the reindex dirs. On crash
		// recovery, OnBeforeLsmInit sees IsPrepended() and skips the
		// file-move merge path.
		if err := rt.markPrepended(); err != nil {
			return fmt.Errorf("marking prepended: %w", err)
		}
	}

	// Remove reindex bucket directories — their segments have been
	// copied into ingest, so the originals are no longer needed.
	// Idempotent: removeReindexBucketsDirs is safe to call when the
	// dirs are already gone (the post-IsPrepended recovery case).
	if err := t.removeReindexBucketsDirs(ctx, logger, shard, props); err != nil {
		return fmt.Errorf("removing reindex bucket dirs: %w", err)
	}

	if err := rt.markMerged(); err != nil {
		return fmt.Errorf("marking merged: %w", err)
	}
	logger.Debug("runtime prepare: all props merged")
	return nil
}

func (t *ShardReindexTaskGeneric) runtimeSwap(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, rt reindexTracker, props []string,
) error {
	// Always disable the double-write callbacks registered by this task
	// instance, regardless of whether the swap completes successfully.
	//
	// On the happy path this defer is a no-op: the explicit
	// disableCallbacks call below (after the post-flip write-quiescence
	// barrier) has already run, so the deferred second call finds an
	// empty list. NOTE for callback invocations between SwapBucketPointer
	// and the explicit disable: SwapBucketPointer removes the ingest NAME
	// from the store's map while the bucket itself stays live under the
	// canonical name. Strategy callbacks that resolve their target by
	// ingest name per invocation must fall back to the canonical name to
	// stay functional in that window (the columnar strategy does — see
	// EnableColumnarStrategy.resolveDoubleWriteBucket — where such an
	// invocation is a harmless redundant write into the live swapped-in
	// bucket).
	//
	// On an error path this is the load-bearing case: without it,
	// callbacks would keep firing against buckets that may be mid-swap,
	// shut down, or otherwise in an inconsistent state. We want
	// subsequent writes to stop touching the ingest/backup buckets
	// entirely; they should land only in whatever the main pointer
	// resolves to.
	//
	// Recovery after a mid-swap failure happens on the next node
	// restart: OnBeforeLsmInit reads the sentinel files and rebuilds
	// disk layout (via recoverRuntimeSwapBuckets), then OnAfterLsmInit
	// re-registers fresh callbacks based on the new on-disk state.
	// Same-process retry of runtimeSwap is not supported (the in-memory
	// bucket state is partially mutated).
	defer t.disableCallbacks()

	store := shard.Store()
	lsmPath := shard.pathLSM()

	// Phase 2a (atomic, tight loop): in-memory pointer swap per property.
	// This is the ONLY work that runs inside the per-shard tokenization
	// overlay's "mixed-state" window (between first prop swapped and last
	// prop swapped). SwapBucketPointer is a single map-write under
	// bucketsLock (microseconds); markSwappedProp is a single fsync
	// (single-digit ms). The per-prop loop completes in a few ms total
	// even for 4-property migrations.
	//
	// The slow disk work (old-bucket Shutdown, oldMainDir→backupDir
	// rename) is pulled OUT of this loop so it can't extend the
	// mixed-state window. It runs in Phase 2b below (after all
	// in-memory swaps are done) and only touches the OLD (already
	// shut-down) bucket — never the LIVE ingest bucket whose dir stays
	// at ingest_<gen> until next-restart recovery does the ingest→main
	// rename safely (no in-memory state at that point).
	// processOneSwapPropFn dispatches to processOneSwapProp by default;
	// the per-prop body returns (nil, nil) for props whose per-prop
	// sentinel is already on disk — recovery idempotency.
	oldMainBuckets := make(map[string]*lsmkv.Bucket, len(props))
	for propIdx, propName := range props {
		oldMainBucket, err := t.processOneSwapPropFn(ctx, store, rt, propIdx, propName)
		if err != nil {
			return err
		}
		if oldMainBucket != nil {
			oldMainBuckets[propName] = oldMainBucket
		}
		// Fire even when processOneSwapPropFn no-ops an already-swapped prop
		// (sentinel on disk), so a resumed swap re-establishes the overlay.
		if t.onPropSwapped != nil {
			t.onPropSwapped(propName)
		}
	}
	logger.Debug("runtime swap: all props in-memory swapped")

	// Phase 2b (post-atomic, slow but inline): shutdown + rename of the
	// OLD (now-dead) main buckets. The load-bearing rule is: rename
	// only shut-down buckets; never rename a live bucket that is
	// serving queries. The OLD bucket is no longer in the store's
	// bucketsByName map (SwapBucketPointer deleted it), so it's not
	// serving queries; Shutdown drains any in-flight compaction and
	// closes mmaps cleanly. The rename moves its dir off the canonical
	// name so the LIVE bucket (still at ingest_<gen> on disk) is the
	// only candidate for the canonical name on next restart.
	//
	// This work is OUTSIDE the mixed-state window — every prop has
	// already had its in-memory pointer swapped. Queries during this
	// phase see new buckets for all props (overlay matches), so
	// per-prop slow ops here don't extend the correctness-sensitive
	// window.
	for _, propName := range props {
		oldMainBucket, ok := oldMainBuckets[propName]
		if !ok {
			// IsSwappedProp(propName) was true on entry — the previous
			// attempt's runtimeSwap call shut down + renamed already.
			continue
		}
		if err := oldMainBucket.Shutdown(ctx); err != nil {
			return fmt.Errorf("shutting down old main bucket for %q: %w", propName, err)
		}
		oldMainDir := oldMainBucket.GetDir()
		backupDir := filepath.Join(lsmPath, t.backupBucketName(propName))
		if err := os.Rename(oldMainDir, backupDir); err != nil {
			return fmt.Errorf("renaming old main dir %q -> %q: %w", oldMainDir, backupDir, err)
		}
	}

	if err := rt.markSwapped(); err != nil {
		return fmt.Errorf("marking swapped: %w", err)
	}

	// markTidied signals that all on-disk cleanup that can be done inline
	// has been done. The LIVE bucket's dir (ingest_<gen>) is still at its
	// pre-swap name — that rename to the canonical name is deferred to
	// next-restart recovery (OnBeforeLsmInit → recoverRuntimeSwapBuckets)
	// because renaming a dir whose buckets are mmap'd by the in-memory
	// store would corrupt the segment registry. At restart time, nothing
	// has loaded that dir yet, so the rename is safe.
	if err := rt.markTidied(); err != nil {
		return fmt.Errorf("marking tidied: %w", err)
	}
	logger.Debug("runtime swap: tidy complete (ingest→main rename deferred to next restart)")

	// OnMigrationComplete: no-op for semantic migrations (the cluster-
	// wide schema flip lives in OnTaskCompleted.flipSemanticMigrationSchema).
	// Per-shard schema-flag flip for blockmax / repair-* strategies.
	// Either way, runs OUTSIDE the per-shard atomic window because it
	// doesn't touch bucket pointers.
	if err := t.strategy.OnMigrationComplete(ctx, shard); err != nil {
		return fmt.Errorf("on migration complete: %w", err)
	}

	if t.onAfterMigrationComplete != nil {
		t.onAfterMigrationComplete()
	}

	// Write-quiescence barrier + mirror disable. INVARIANT: the
	// double-write mirror callbacks may only be disabled once no write
	// that analyzed under the PRE-flip schema can still be in flight.
	// Such a write carries the old Has*Index flags (e.g.
	// HasColumnarIndex=false), so the direct write path skips the new
	// bucket and the mirror is its only route in; disabling the mirror
	// under it loses the write silently (the disabled wrapper returns
	// nil). OnMigrationComplete above has flipped the schema flag, so:
	//   - writes that analyze AFTER the barrier see the new flag and are
	//     covered by the direct write path;
	//   - writes that analyzed before hold the shard's inverted-write
	//     gate until their index updates (including the mirror) have
	//     been applied — draining the gate waits them out while the
	//     mirror is still active.
	// Only then is it safe to disable. The deferred disableCallbacks at
	// the top of this function then no-ops (the list is cleared).
	concreteShard, err := unwrapShard(ctx, shard)
	if err != nil {
		return fmt.Errorf("unwrapping shard for write-quiescence barrier: %w", err)
	}
	concreteShard.drainInvertedIndexWrites()
	t.disableCallbacks()

	// Trim older generations on disk (best-effort cleanup of sidecar
	// dirs from completed-and-tidied prior migrations on this prop).
	// Independent of the atomic window — operates on _bak / .migrations
	// dirs whose owning gen is strictly older than this gen.
	t.trimOlderGenerationsLocked(logger, shard, rt, props)

	logger.Info("runtime swap: migration complete (ingest→main rename deferred to next restart)")

	return nil
}

// trimOlderGenerationsLocked removes on-disk leftovers from generations
// older than `currentGen` for the strategy's (prefix, propNamesSuffix).
// Called after `markTidied()` at the end of [runtimeSwap].
//
// Removes, per shard:
//   - all `…_<reindexSuffix-base>_<M>/`, `…_<ingestSuffix-base>_<M>/`,
//     `…_<backupSuffix-base>_<M>/` dirs with M < currentGen, plus the
//     `…_<backupSuffix-base>_<currentGen>/` dir produced by this swap
//     (the pre-T_N data we no longer need).
//   - all `.migrations/<migrationDirPrefix><propSuffix>_<M>/` for
//     M < currentGen.
//
// Keeps:
//   - The current gen's ingest dir (the live main's physical dir, still
//     referenced by the in-memory bucket pointer until next-restart
//     finalize renames it to canonical).
//   - The current gen's migration tracker dir (its tidied.mig is the
//     signal next-restart finalize uses to promote the ingest dir to
//     canonical).
func (t *ShardReindexTaskGeneric) trimOlderGenerationsLocked(
	logger logrus.FieldLogger, shard ShardLike, _ reindexTracker, props []string,
) {
	concrete, err := unwrapShard(context.Background(), shard)
	if err != nil {
		logger.Warnf("runtime swap: trim: failed to unwrap shard; skipping cleanup: %v", err)
		return
	}
	lsmPath := concrete.pathLSM()
	currentGen := t.strategy.MigrationDirName()
	currentReindex := t.strategy.ReindexSuffix()
	currentIngest := t.strategy.IngestSuffix()
	currentBackup := t.strategy.BackupSuffix()

	// Reverse the gen suffix off each current suffix to get the
	// suffix-without-gen base for prefix matching against older
	// generations on disk. genSuffix = "_<N>"; everything before the last
	// "_<digits>" is the base. parseMigrationDirName does this for the
	// migration dir name; for the bucket suffixes we extract the same way.
	currentReindexBase, _, _ := parseMigrationDirName(currentReindex)
	currentIngestBase, _, _ := parseMigrationDirName(currentIngest)
	currentBackupBase, _, _ := parseMigrationDirName(currentBackup)
	currentMigBase, currentGenN, _ := parseMigrationDirName(currentGen)

	// Bucket sidecar dirs live at the top of the LSM dir. Match by
	// "_<base>_<M>" suffix on names that start with the prop's main
	// bucket name. The current gen's ingest dir is intentionally kept.
	entries, err := os.ReadDir(lsmPath)
	if err != nil {
		logger.Warnf("runtime swap: trim: failed to read LSM dir; skipping cleanup: %v", err)
	} else {
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
				if !ok {
					continue
				}
				switch suffixBase {
				case currentReindexBase, currentBackupBase:
					// Always obsolete after tidied (reindex is already
					// removed during runtimeSwap step 2; current-gen
					// backup is the pre-T_N data we no longer need).
					t.removeAllSafe(logger, filepath.Join(lsmPath, name))
				case currentIngestBase:
					if suffixGen < currentGenN {
						t.removeAllSafe(logger, filepath.Join(lsmPath, name))
					}
				}
			}
		}
	}

	// Migration tracker dirs: remove all for older gens of THIS strategy
	// + prop tuple.
	migsDir := filepath.Join(lsmPath, ".migrations")
	migEntries, err := os.ReadDir(migsDir)
	if err != nil {
		if !os.IsNotExist(err) {
			logger.Warnf("runtime swap: trim: failed to read .migrations dir; skipping cleanup: %v", err)
		}
		return
	}
	for _, entry := range migEntries {
		if !entry.IsDir() {
			continue
		}
		base, gen, ok := parseMigrationDirName(entry.Name())
		if !ok {
			continue
		}
		if base != currentMigBase {
			continue
		}
		if gen >= currentGenN {
			continue
		}
		t.removeAllSafe(logger, filepath.Join(migsDir, entry.Name()))
	}
}

func (t *ShardReindexTaskGeneric) removeAllSafe(logger logrus.FieldLogger, path string) {
	if err := os.RemoveAll(path); err != nil {
		logger.WithField("path", path).
			Warnf("runtime swap: trim: failed to remove obsolete dir; next-restart finalize will sweep: %v", err)
	}
}

// -----------------------------------------------------------------------------
// Bucket operations
// -----------------------------------------------------------------------------

func (t *ShardReindexTaskGeneric) reindexBucketName(propName string) string {
	return t.strategy.SourceBucketName(propName) + t.strategy.ReindexSuffix()
}

func (t *ShardReindexTaskGeneric) ingestBucketName(propName string) string {
	return t.strategy.SourceBucketName(propName) + t.strategy.IngestSuffix()
}

func (t *ShardReindexTaskGeneric) backupBucketName(propName string) string {
	return t.strategy.SourceBucketName(propName) + t.strategy.BackupSuffix()
}

// firstMissingReindexBucketDir returns the path of the first reindex
// bucket directory that is expected to be on disk but is not. Used by the
// torn-state recovery in [OnAfterLsmInit]: a real iteration that reached
// markReindexed() must have populated at least one segment in every
// per-property reindex bucket, so the bucket dir must exist. If we ever
// see IsReindexed && a missing reindex bucket dir (and no later
// runtime-swap sentinel that legitimately removed the dir), the sentinel
// is forged or the prior run died mid-flight before any iteration data
// reached disk. The caller resets reindexed.mig in that case so the next
// pass re-iterates instead of swapping in an empty bucket.
//
// Returns "" if every prop's reindex bucket dir is present. Returns the
// MISSING path (not the prop name) so the operator-facing log message
// names the exact dir to inspect.
func (t *ShardReindexTaskGeneric) firstMissingReindexBucketDir(lsmPath string, props []string) string {
	for _, propName := range props {
		dir := filepath.Join(lsmPath, t.reindexBucketName(propName))
		if !dirExists(dir) {
			return dir
		}
	}
	return ""
}

func (t *ShardReindexTaskGeneric) mergeReindexAndIngestBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard *Shard, rt reindexTracker, props []string,
) error {
	lsmPath := shard.pathLSM()
	segmentPathsToMove := [][2]string{}
	bucketPathsToRemove := make([]string, 0, len(props))
	lock := new(sync.Mutex)

	eg, gctx := enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
	eg.SetLimit(t.config.concurrency)
	for i := range props {
		propName := props[i]

		eg.Go(func() error {
			reindexBucketName := t.reindexBucketName(propName)
			reindexBucketPath := filepath.Join(lsmPath, reindexBucketName)
			ingestBucketName := t.ingestBucketName(propName)
			ingestBucketPath := filepath.Join(lsmPath, ingestBucketName)

			for {
				propSegmentPathsToMove, needsRecover, err := t.getSegmentPathsToMove(reindexBucketPath, ingestBucketPath)
				if err != nil {
					return fmt.Errorf("buckets '%s' & '%s': %w", reindexBucketName, ingestBucketName, err)
				}

				if needsRecover {
					if err := t.recoverReindexBucket(gctx, logger, shard, propName, reindexBucketName); err != nil {
						return err
					}
				} else {
					lock.Lock()
					bucketPathsToRemove = append(bucketPathsToRemove, reindexBucketPath)
					segmentPathsToMove = append(segmentPathsToMove, propSegmentPathsToMove...)
					lock.Unlock()
					return nil
				}
			}
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}

	logger.WithField("segments_to_move", segmentPathsToMove).WithField("buckets_to_remove", bucketPathsToRemove).
		Debug("merging reindex and ingest buckets")

	eg, _ = enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
	eg.SetLimit(t.config.concurrency)
	for i := range segmentPathsToMove {
		i := i
		eg.Go(func() error {
			return os.Rename(segmentPathsToMove[i][0], segmentPathsToMove[i][1])
		})
	}
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("moving segment: %w", err)
	}

	eg, _ = enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
	eg.SetLimit(t.config.concurrency)
	for i := range bucketPathsToRemove {
		i := i
		eg.Go(func() error {
			return os.RemoveAll(bucketPathsToRemove[i])
		})
	}
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("removing bucket: %w", err)
	}

	if err := rt.markMerged(); err != nil {
		return fmt.Errorf("marking reindex merged: %w", err)
	}
	return nil
}

func (t *ShardReindexTaskGeneric) getSegmentPathsToMove(bucketPathSrc, bucketPathDst string,
) ([][2]string, bool, error) {
	segmentPaths := [][2]string{}
	needsRecover := false

	err := filepath.WalkDir(bucketPathSrc, func(path string, d os.DirEntry, err error) error {
		if d.IsDir() {
			return nil
		}
		if isSegmentDb(d.Name()) || isSegmentBloom(d.Name()) {
			ext := filepath.Ext(d.Name())
			idAndData := strings.Split(strings.TrimSuffix(strings.TrimPrefix(d.Name(), "segment-"), ext), ".")
			timestamp, err := strconv.ParseInt(idAndData[0], 10, 64)
			if err != nil {
				return err
			}
			timestampPast := time.Unix(0, timestamp).AddDate(-23, 0, 0).UnixNano()
			idAndData[0] = strconv.FormatInt(timestampPast, 10)
			segmentPaths = append(segmentPaths, [2]string{
				path, filepath.Join(bucketPathDst, fmt.Sprintf("segment-%s%s", strings.Join(idAndData, "."), ext)),
			})
		} else if isSegmentWal(d.Name()) {
			if info, err := d.Info(); err != nil {
				return err
			} else if info.Size() > 0 {
				needsRecover = true
				return filepath.SkipAll
			}
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if needsRecover {
		return nil, true, nil
	}
	return segmentPaths, false, nil
}

// recoverRuntimeSwapBuckets handles the disk-rename half of a runtime
// swap. It serves two callers with identical on-disk semantics:
//
//  1. **Crash recovery** for a runtime swap that was interrupted
//     between SwapBucketPointer and the post-atomic Phase 2b
//     rename pair.
//
//  2. **Deferred disk rename** on next startup after a successful
//     runtime swap. Under the prep/atomic/defer phase model (see
//     [runtimeSwap] godoc), runtimeSwap performs ONLY the in-memory
//     bucket-pointer flip and leaves the disk dirs (main_<gen> and
//     ingest_<gen>) untouched. OnBeforeLsmInit's IsMerged && !IsSwapped
//     branch picks this up at next startup, BEFORE LSM init loads any
//     bucket from main_<gen>, and routes through this function.
//
// For each property the disk state is one of:
//
//   - mainExists && ingestExists && !backupExists → atomic swap was
//     in-memory only (post-refactor happy path) or never started →
//     do the full rename pair (main → backup, ingest → main).
//   - !mainExists && backupExists → halfway through the pair: main
//     was renamed but ingest wasn't yet → rename ingest → main.
//   - mainExists && backupExists → ingest was already renamed to
//     main on a prior recovery attempt; nothing to do this round.
//
// The IsSwappedProp sentinel is intentionally NOT used to skip props
// here. Pre-refactor it was set together with the disk rename, so
// IsSwappedProp=true implied the dirs were already swapped on disk.
// Post-refactor, runtimeSwap sets IsSwappedProp after the in-memory
// pointer flip — the disk dirs may still need renaming. Trusting the
// on-disk state instead of the sentinel makes this function correct
// for both paths.
func (t *ShardReindexTaskGeneric) recoverRuntimeSwapBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, rt reindexTracker, props []string,
) error {
	lsmPath := shard.pathLSM()

	for _, propName := range props {
		mainName := t.strategy.SourceBucketName(propName)
		mainDir := filepath.Join(lsmPath, mainName)
		ingestDir := filepath.Join(lsmPath, t.ingestBucketName(propName))
		backupDir := filepath.Join(lsmPath, t.backupBucketName(propName))

		mainExists := dirExists(mainDir)
		backupExists := dirExists(backupDir)
		ingestExists := dirExists(ingestDir)

		switch {
		case mainExists && !backupExists:
			// Pre-rename state (either swap not started, or post-refactor
			// happy-path deferred-rename). Do the full rename pair.
			if !ingestExists {
				return fmt.Errorf("recovery rename for %q: main exists, no backup, but ingest dir missing — unrecoverable", propName)
			}
			if err := os.Rename(mainDir, backupDir); err != nil {
				return fmt.Errorf("recovery rename main->backup for %q: %w", propName, err)
			}
			if err := os.Rename(ingestDir, mainDir); err != nil {
				return fmt.Errorf("recovery rename ingest->main for %q: %w", propName, err)
			}
		case !mainExists && backupExists:
			// Halfway: main was renamed to backup but ingest not yet to main.
			if !ingestExists {
				return fmt.Errorf("recovery rename for %q: main missing, backup exists, but ingest dir missing — unrecoverable", propName)
			}
			if err := os.Rename(ingestDir, mainDir); err != nil {
				return fmt.Errorf("recovery rename ingest->main for %q: %w", propName, err)
			}
		case mainExists && backupExists:
			// Both exist — ingest was already renamed to main on a prior
			// recovery pass. Idempotent no-op.
		default:
			return fmt.Errorf("unexpected disk state for prop %q: main=%v backup=%v ingest=%v",
				propName, mainExists, backupExists, ingestExists)
		}

		if err := rt.markSwappedProp(propName); err != nil {
			return fmt.Errorf("marking swapped prop %q: %w", propName, err)
		}
		// Recovery re-establishes the overlay for the same reason as the happy path.
		if t.onPropSwapped != nil {
			t.onPropSwapped(propName)
		}
	}

	if err := rt.markSwapped(); err != nil {
		return fmt.Errorf("marking swapped: %w", err)
	}
	return nil
}

func (t *ShardReindexTaskGeneric) swapIngestAndBackupBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, rt reindexTracker, props []string,
) error {
	lsmPath := shard.pathLSM()

	eg, _ := enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
	eg.SetLimit(t.config.concurrency)
	for i := range props {
		propName := props[i]

		if !rt.IsSwappedProp(props[i]) {
			eg.Go(func() error {
				bucketName := t.strategy.SourceBucketName(propName)
				bucketPath := filepath.Join(lsmPath, bucketName)
				ingestBucketName := t.ingestBucketName(propName)
				ingestBucketPath := filepath.Join(lsmPath, ingestBucketName)
				backupBucketName := t.backupBucketName(propName)
				backupBucketPath := filepath.Join(lsmPath, backupBucketName)

				logger.WithFields(map[string]any{
					"bucket":        bucketName,
					"ingest_bucket": ingestBucketName,
					"backup_bucket": backupBucketName,
				}).Debug("swapping buckets")

				if err := os.Rename(bucketPath, backupBucketPath); err != nil {
					return err
				}
				if err := os.Rename(ingestBucketPath, bucketPath); err != nil {
					return err
				}
				if err := rt.markSwappedProp(propName); err != nil {
					return fmt.Errorf("marking reindex swapped for '%s': %w", propName, err)
				}
				return nil
			})
		}
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	if err := rt.markSwapped(); err != nil {
		return fmt.Errorf("marking reindex swapped: %w", err)
	}

	logger.Debug("swapped buckets")

	return nil
}

func (t *ShardReindexTaskGeneric) unswapIngestAndBackupBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, rt reindexTracker, props []string,
) error {
	lsmPath := shard.pathLSM()

	eg, _ := enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
	eg.SetLimit(t.config.concurrency)
	for i := range props {
		propName := props[i]

		if rt.IsSwappedProp(props[i]) {
			eg.Go(func() error {
				bucketName := t.strategy.SourceBucketName(propName)
				bucketPath := filepath.Join(lsmPath, bucketName)
				ingestBucketName := t.ingestBucketName(propName)
				ingestBucketPath := filepath.Join(lsmPath, ingestBucketName)
				backupBucketName := t.backupBucketName(propName)
				backupBucketPath := filepath.Join(lsmPath, backupBucketName)

				logger.WithFields(map[string]any{
					"bucket":        bucketName,
					"ingest_bucket": ingestBucketName,
					"backup_bucket": backupBucketName,
				}).Debug("unswapping buckets")

				if err := os.Rename(bucketPath, ingestBucketPath); err != nil {
					return err
				}
				if err := os.Rename(backupBucketPath, bucketPath); err != nil {
					return err
				}
				if err := rt.unmarkSwappedProp(propName); err != nil {
					return fmt.Errorf("unmarking reindex swapped for '%s': %w", propName, err)
				}
				return nil
			})
		}
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	if err := rt.unmarkSwapped(); err != nil {
		return fmt.Errorf("unmarking reindex swapped: %w", err)
	}

	logger.Debug("unswapped buckets")

	return nil
}

func (t *ShardReindexTaskGeneric) tidyBackupBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, rt reindexTracker, props []string,
) error {
	lsmPath := shard.pathLSM()

	eg, _ := enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
	eg.SetLimit(t.config.concurrency)
	for i := range props {
		propIdx := i
		propName := props[i]

		eg.Go(func() error {
			return t.processOneTidyPropFn(propIdx, propName, lsmPath)
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}
	if err := rt.markTidied(); err != nil {
		return fmt.Errorf("marking reindex tidied: %w", err)
	}
	return nil
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
	bucketOpts := t.bucketOptions(shard, t.strategy.TargetStrategy(), keepLevelCompaction, keepTombstones, t.config.memtableOptFactor)
	return t.loadBuckets(ctx, logger, shard, props, t.ingestBucketName, bucketOpts)
}

func (t *ShardReindexTaskGeneric) loadBackupBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard *Shard, props []string,
) error {
	bucketOpts := t.bucketOptions(shard, t.strategy.BackupStrategy(), false, false, t.config.backupMemtableOptFactor)
	return t.loadBuckets(ctx, logger, shard, props, t.backupBucketName, bucketOpts)
}

// perPropertyBucketOptioner is an optional [MigrationStrategy] extension
// for strategies whose target buckets require per-property bucket options
// on top of the shared per-strategy defaults. The canonical case is
// [EnableColumnarStrategy]: columnar buckets refuse every write until a
// per-property *columnar.Schema is set via lsmkv.WithColumnarSchema (the
// schema's single column is named after the property and typed from its
// dataType), so a shared option slice cannot serve all props. The
// returned options are appended AFTER the shared ones, so per-prop
// options win on conflicting settings.
type perPropertyBucketOptioner interface {
	PerPropertyBucketOptions(shard *Shard, propName string) []lsmkv.BucketOption
}

// perPropBucketOpts combines the shared per-strategy bucket options with
// the strategy's per-property extras (if the strategy implements
// [perPropertyBucketOptioner]). The shared slice is never mutated — a
// fresh slice is built when extras exist, because loadBuckets calls this
// concurrently for multiple props off the same shared slice.
func (t *ShardReindexTaskGeneric) perPropBucketOpts(shard *Shard, propName string,
	shared []lsmkv.BucketOption,
) []lsmkv.BucketOption {
	pp, ok := t.strategy.(perPropertyBucketOptioner)
	if !ok {
		return shared
	}
	extra := pp.PerPropertyBucketOptions(shard, propName)
	if len(extra) == 0 {
		return shared
	}
	out := make([]lsmkv.BucketOption, 0, len(shared)+len(extra))
	out = append(out, shared...)
	return append(out, extra...)
}

func (t *ShardReindexTaskGeneric) loadBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard *Shard, props []string, bucketNamer func(string) string,
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
			opts := t.perPropBucketOpts(shard, propName, bucketOpts)
			if err := store.CreateOrLoadBucket(gctx, bucketName, opts...); err != nil {
				return err
			}
			logger.WithField("bucket", bucketName).Debug("bucket loaded")
			return nil
		})
	}

	return eg.Wait()
}

func (t *ShardReindexTaskGeneric) unloadIngestBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, props []string,
) error {
	return t.unloadBuckets(ctx, logger, shard, props, t.ingestBucketName)
}

func (t *ShardReindexTaskGeneric) unloadReindexBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, props []string,
) error {
	return t.unloadBuckets(ctx, logger, shard, props, t.reindexBucketName)
}

func (t *ShardReindexTaskGeneric) unloadBuckets(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, props []string, bucketNamer func(string) string,
) error {
	store := shard.Store()

	eg, gctx := enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
	eg.SetLimit(t.config.concurrency)
	for i := range props {
		propName := props[i]

		eg.Go(func() error {
			bucketName := bucketNamer(propName)
			logger.WithField("bucket", bucketName).Debug("unloading bucket")
			if err := store.ShutdownBucket(gctx, bucketName); err != nil {
				return err
			}
			logger.WithField("bucket", bucketName).Debug("bucket unloaded")
			return nil
		})
	}

	return eg.Wait()
}

func (t *ShardReindexTaskGeneric) recoverReindexBucket(ctx context.Context,
	logger logrus.FieldLogger, shard *Shard, propName, bucketName string,
) error {
	store := shard.Store()
	// Per-prop options (e.g. the columnar schema) are required here too:
	// WAL replay of a columnar bucket without its schema would drop the
	// recovered rows on the floor.
	bucketOpts := t.perPropBucketOpts(shard, propName,
		t.bucketOptions(shard, t.strategy.TargetStrategy(), true, false, t.config.memtableOptFactor))

	logger.WithField("bucket", bucketName).Debug("recover wals, loading bucket")
	if err := store.CreateOrLoadBucket(ctx, bucketName, bucketOpts...); err != nil {
		return fmt.Errorf("bucket '%s': %w", bucketName, err)
	}
	logger.WithField("bucket", bucketName).Debug("recover wals, shutting down bucket")
	if err := store.ShutdownBucket(ctx, bucketName); err != nil {
		return fmt.Errorf("bucket '%s': %w", bucketName, err)
	}
	logger.WithField("bucket", bucketName).Debug("recover wals, shut down bucket")

	return nil
}

func (t *ShardReindexTaskGeneric) removeReindexBucketsDirs(ctx context.Context, logger logrus.FieldLogger,
	shard ShardLike, props []string,
) error {
	return t.removeBucketsDirs(ctx, logger, shard, props, t.reindexBucketName)
}

func (t *ShardReindexTaskGeneric) removeIngestBucketsDirs(ctx context.Context, logger logrus.FieldLogger,
	shard ShardLike, props []string,
) error {
	return t.removeBucketsDirs(ctx, logger, shard, props, t.ingestBucketName)
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

func dirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

// registerDoubleWriteCallbacks registers the strategy's add/delete mirror
// callbacks on the shard's property-value-index write path. The disable
// funcs are collected on the task for the eventual [disableCallbacks] at
// the end of [runtimeSwap]; the returned func disables ONLY the pair
// registered by this call (idempotent — each disable is an atomic flag),
// for failure paths that must not touch registrations made for other
// shards by the same task instance.
//
// INVARIANT (enforced here, fail-loud): registration MUST NOT happen
// before every bucket the callbacks resolve is resolvable via the exact
// per-invocation resolution path the callbacks use — the primary lookup
// is shard.Store().Bucket(bucketNamer(prop)) in every strategy's
// MakeAddCallback/MakeDeleteCallback. Activating mirrors whose target is
// unresolvable puts the shard in the worst failure shape this migration
// family has: every property-value write 5xxs with "double-write: bucket
// not found" AFTER the object-store half already committed (non-atomic
// from the client's view), AND the row is permanently lost from the
// target bucket because the backfill iterator skips it
// (LastUpdateTimeUnix >= reindexStarted) on the assumption the mirror
// covered it. This is the exact client-visible signature reported on
// weaviate/weaviate#11678 against the pre-mirror-lifetime-fix build.
// The lifecycle guarantees the invariant ([OnAfterLsmInit] loads the
// ingest/backup buckets strictly before registering), so a failure here
// always means an ordering regression — surface it at migration start
// instead of as write-path 500s.
func (t *ShardReindexTaskGeneric) registerDoubleWriteCallbacks(shard *Shard, props []string,
	bucketNamer func(string) string, forTargetStrategy bool,
) (func(), error) {
	store := shard.Store()
	for _, propName := range props {
		if bucketName := bucketNamer(propName); store.Bucket(bucketName) == nil {
			return nil, fmt.Errorf(
				"registering double-write callbacks: target bucket %q for prop %q is not resolvable in the shard store; "+
					"refusing to activate mirror callbacks that would fail every concurrent write (the bucket must be loaded before registration)",
				bucketName, propName)
		}
	}

	propsByName := map[string]struct{}{}
	for i := range props {
		propsByName[props[i]] = struct{}{}
	}

	disableAdd := shard.registerAddToPropertyValueIndex(
		t.strategy.MakeAddCallback(bucketNamer, propsByName, forTargetStrategy))
	disableDelete := shard.registerDeleteFromPropertyValueIndex(
		t.strategy.MakeDeleteCallback(bucketNamer, propsByName, forTargetStrategy))

	t.callbackDisableFuncsMu.Lock()
	t.callbackDisableFuncs = append(t.callbackDisableFuncs, disableAdd, disableDelete)
	t.callbackDisableFuncsMu.Unlock()

	return func() {
		disableAdd()
		disableDelete()
	}, nil
}

// disableCallbacks calls all stored callback disable functions collected
// during registerDoubleWriteCallbacks, then clears the list.
func (t *ShardReindexTaskGeneric) disableCallbacks() {
	t.callbackDisableFuncsMu.Lock()
	defer t.callbackDisableFuncsMu.Unlock()

	for _, fn := range t.callbackDisableFuncs {
		fn()
	}
	t.callbackDisableFuncs = nil
}

func (t *ShardReindexTaskGeneric) bucketOptions(shard *Shard, strategy string,
	keepLevelCompaction, keepTombstones bool, memtableOptFactor int,
) []lsmkv.BucketOption {
	cfg := shard.Index().Config

	return shard.makeDefaultBucketOptions(strategy,
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
}

// -----------------------------------------------------------------------------
// Property discovery and selection
// -----------------------------------------------------------------------------

func (t *ShardReindexTaskGeneric) findPropsToReindex(shard ShardLike) (props []string, save bool) {
	collectionName := shard.Index().Config.ClassName.String()
	shardName := shard.Name()
	propNames := []string{}

	if !t.isShardSelected(collectionName, shardName) {
		return propNames, false
	}

	// When selection is enabled and an explicit list of properties is given,
	// the selected list IS the authoritative reindex target. Existing
	// strategies (e.g. repair-searchable, change-tokenization) target
	// properties whose source bucket already exists; new from-scratch
	// strategies (enable-filterable, enable-searchable) target properties
	// whose source bucket does not exist yet and will be created in
	// PreReindexHook. Both cases reduce to "use the selected list".
	if t.config.selectionEnabled {
		if selectedProps := t.config.selectedPropsByCollection[collectionName]; len(selectedProps) > 0 {
			for propName := range selectedProps {
				propNames = append(propNames, propName)
			}
			// Sort for determinism — map iteration order is randomized
			// and downstream sentinel/tracker state hashes the list.
			sort.Strings(propNames)
			return propNames, true
		}
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
	return propNames, true
}

func (t *ShardReindexTaskGeneric) getPropsToReindex(shard ShardLike, rt reindexTracker) ([]string, error) {
	if rt.HasProps() {
		props, err := rt.GetProps()
		if err != nil {
			return nil, err
		}
		return props, nil
	}
	props, save := t.findPropsToReindex(shard)
	if save {
		if err := rt.saveProps(props); err != nil {
			return nil, err
		}
	}
	return props, nil
}

func (t *ShardReindexTaskGeneric) readPropsToReindex(rt reindexTracker) ([]string, error) {
	if rt.HasProps() {
		props, err := rt.GetProps()
		if err != nil {
			return nil, err
		}
		return props, nil
	}
	return []string{}, nil
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
// Segment helpers
// -----------------------------------------------------------------------------

func isSegmentDb(filename string) bool {
	return strings.HasPrefix(filename, "segment-") && strings.HasSuffix(filename, ".db")
}

func isSegmentBloom(filename string) bool {
	return strings.HasPrefix(filename, "segment-") && strings.HasSuffix(filename, ".bloom")
}

func isSegmentWal(filename string) bool {
	return strings.HasPrefix(filename, "segment-") && strings.HasSuffix(filename, ".wal")
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
