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
// oldMainBucket.Shutdown(ctx) + removal of its directory at the handle
// the record names, per property. This runs AFTER every prop has
// flipped in 2a, so the mixed-state subwindow is closed. Queries during
// 2b see all-new buckets with the overlay active — correct. The
// oldMain.Shutdown is REQUIRED inline (not deferred) because
// Bucket.Shutdown is the only call that removes the bucket's path from
// GlobalBucketRegistry; deferring it leaks the path entry
// process-wide, which makes any subsequent in-process shard init at the
// same canonical name fail with ErrBucketAlreadyRegistered.
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
// Crash safety: every phase transition is a durable record write, and
// the flip decision is written and fsynced ahead of the first pointer
// flip so that a record short of it proves no flip was ever decided.
// The dispatch in [ShardReindexTaskGeneric.RunSwapOnShard] reads the
// record to pick the right resume point. A crash anywhere in or after
// 2b resolves at the next shard load: reconciliation probes the
// handles the record carries and finishes the same directory work,
// before any bucket is opened.
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
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/additional"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// ShardReindexTaskGeneric is a strategy-parameterized reindex task. All
// lifecycle logic (state machine, merge/swap/tidy, object iteration,
// progress tracking) lives here, with strategy-specific behavior
// delegated to a MigrationStrategy.
//
// See the file-level phase-contract godoc above for the prep / atomic
// swap / deferred-rename invariants that every code path in this file
// must preserve.
type ShardReindexTaskGeneric struct {
	name     string
	logger   logrus.FieldLogger
	strategy MigrationStrategy

	// The migration's durable identity. taskVersion is the RAFT log index of
	// the task's creation, so it is also the generation supersession compares.
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
		bucketNamer func(string) string) func()

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

	// indexClosingGuard reports a closing index as context.Canceled.
	// Defaults to Index.withCloseRLockGuard in NewShardReindexTaskGeneric;
	// the drain-rematerialize test wraps it to park a worker at the exact
	// point a concurrent Index.drop has to land. Always set — no test-only
	// branch runs in production.
	indexClosingGuard func(shard ShardLike) error

	// rebuildRangeableRepFn dispatches [rebuildRangeableInMemoryReps]'s
	// per-prop bucket rebuild; defaults to
	// [lsmkv.Bucket.RebuildRangeableSegmentInMemory], tests substitute a
	// failure-injecting wrapper. Always set - no test-only branch runs in
	// production.
	rebuildRangeableRepFn func(ctx context.Context, b *lsmkv.Bucket) error
}

// NewShardReindexTaskGeneric creates a new generic reindex task.
func NewShardReindexTaskGeneric(name string, logger logrus.FieldLogger,
	strategy MigrationStrategy, config reindexTaskConfig,
	keyParser indexKeyParser, objectsIteratorAsync objectsIteratorAsync,
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
	}
	t.processOneSwapPropFn = t.processOneSwapProp
	t.indexClosingGuard = func(shard ShardLike) error {
		return shard.Index().withCloseRLockGuard(func() error { return nil })
	}
	t.registerDoubleWriteCallbacksFn = t.registerDoubleWriteCallbacks
	t.rebuildRangeableRepFn = func(ctx context.Context, b *lsmkv.Bucket) error {
		return b.RebuildRangeableSegmentInMemory(ctx)
	}
	return t
}

// processOneSwapProp is the production body of runtimeSwap's Phase 2a
// per-prop loop: the in-memory pointer flip. Returns the displaced old main
// bucket for the caller's Phase 2b, or nil for a property this process has
// already flipped.
func (t *ShardReindexTaskGeneric) processOneSwapProp(ctx context.Context, store *lsmkv.Store, _ int, propName string) (*lsmkv.Bucket, error) {
	ingestName := t.ingestBucketName(propName)
	mainName := t.strategy.SourceBucketName(propName)

	// A property already flipped in this process has no ingest-name entry
	// left. Reading the bucket map keeps the loop free of I/O, which the
	// per-property marker file this replaces could not.
	if store.Bucket(ingestName) == nil {
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

// runShardLifecycle is the shared body of RunOnShard / RunReindexOnlyOnShard.
// skipSwap=true sets the same-named flag for the duration of the call so the
// iteration loop stops once the rebuild is recorded complete — used for the
// barrier semantics of semantic migrations. skipSwap=false runs all the way
// through swap and retirement.
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

	// Idempotent fast-path: committed means prep already completed, either
	// by an earlier call in this process or by a previous boot's
	// runtimePrepare.
	if entry.rec.StagedDataComplete() {
		logger.Debug("RunPrepareOnShard: already merged on disk; no-op")
		return nil
	}

	props := entry.rec.Subject().Properties
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

// enterDTMPhase unwraps the shard and — if the recorded state trails RAFT
// (still iterating after a rolling restart) — resumes the iteration before
// returning.
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

	// MUST stay ahead of the iteration-resume ladder below: committed implies
	// the iteration completed, so resuming would re-run it against a
	// migration whose data is already staged. Both callers handle it
	// downstream.
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

// RunSwapOnShard runs the swap + OnMigrationComplete phase.
//
// Preconditions:
//   - the migration's rebuild is complete (the record is Iterated or beyond);
//   - SHOULD use the same task instance that ran RunReindexOnlyOnShard, which
//     preserves the double-write callbacks registered during the rebuild. The
//     rehydrate path after a node restart cannot, and re-arms them instead.
//
// This function is the cluster's authoritative completion path for semantic
// migrations, invoked by [ReindexProvider.OnGroupCompleted] once all units in
// the group are terminal. A node that restarted inside the FINALIZING window
// re-enters here at whatever state its record last reached, and the dispatch
// below picks up from exactly there. Without it, such a node would re-run the
// pre-prepend path, fail with "reindex bucket not found", and flip the task to
// FAILED cluster-wide while the other replicas have already swapped — the
// schema-versus-bucket inversion this dispatch exists to prevent.
func (t *ShardReindexTaskGeneric) RunSwapOnShard(ctx context.Context, shard ShardLike) error {
	entry, err := t.enterDTMPhase(ctx, shard, "RunSwapOnShard")
	if err != nil {
		return err
	}
	concreteShard, logger := entry.shard, entry.logger

	props := entry.rec.Subject().Properties
	if len(props) == 0 {
		return fmt.Errorf("no props found for swap on shard %q", concreteShard.Name())
	}

	switch {
	case entry.rec.PointerSwapped():
		// The flip decision is durable, so every directory step left is
		// reconciliation's to finish at a load that can rename safely. What
		// remains here is in-process state the strategy still owes: the
		// analyzer overlay, the schema flag, the in-memory range reps.
		logger.WithField("props", props).Info("RunSwapOnShard: flip already decided; running OnMigrationComplete only")
		return t.finalizeMigrationAfterRecovery(ctx, logger, shard, props)

	case entry.rec.StagedDataComplete():
		// The staged data is complete; the flip is what is left. The ingest
		// buckets are open by this point on every route — the load hook opens
		// them for a committed record, and the guard below covers a bucket
		// map that a cancelled shutdown left short.
		logger.WithField("props", props).Info("RunSwapOnShard: resuming from merged state, in-memory atomic swap")
		if err := t.ensureReindexBucketsLoadedForSwap(ctx, logger, concreteShard, props); err != nil {
			return fmt.Errorf("ensure buckets loaded: %w", err)
		}
		return t.runtimeSwap(ctx, logger, shard, props)
	}

	// The rebuild is complete but nothing is staged yet. Under the
	// prep/atomic/defer phase model the happy-path caller is
	// [reindex_provider.OnGroupCompleted], which invokes RunPrepareOnShard
	// BEFORE RunSwapOnShard so the prep work runs OUTSIDE the per-shard
	// tokenization-overlay window. Reaching here through that flow means
	// rehydrate happened but RunPrepareOnShard did not — run it defensively,
	// noting that the atomic-window contract is no longer met.
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
//   - A previous in-process runtimeSwap was interrupted mid-flight
//     by ctx.Canceled (graceful shutdown) at Step 1 or Step 2. The
//     interrupted ShutdownBucket may have removed the reindex bucket
//     from the store's bucket map without advancing the record, and
//     the cancellation can leave compaction callbacks
//     unregistered partway through the unhook sequence.
//   - On restart, the shard-registered recovery task's OnAfterLsmInit
//     (see [shardReindexerV3RecoveryOnly]) is the only re-load hook.
//     If for any reason the bucket name lookup in
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
		if store.Bucket(reindexName) == nil {
			// A stat that fails for any reason other than ENOENT must not read
			// as "dir gone": skipping the load here is what produces the
			// cluster-wide FAILED this function exists to prevent.
			there, err := migrationDirExists(filepath.Join(lsmPath, reindexName))
			if err != nil {
				return fmt.Errorf("probe reindex bucket dir for %q: %w", propName, err)
			}
			if there {
				missingReindex = append(missingReindex, propName)
			}
		}
		ingestName := t.ingestBucketName(propName)
		if store.Bucket(ingestName) == nil {
			there, err := migrationDirExists(filepath.Join(lsmPath, ingestName))
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

// finalizeMigrationAfterRecovery runs the strategy's OnMigrationComplete
// hook and trims older on-disk generations. This is the rehydrate-path
// equivalent of runtimeSwap's final two steps (lines 1103/1124),
// invoked by the recovery branches in [RunSwapOnShard] which don't go
// through runtimeSwap.
//
// Best-effort on trim — failures are logged, not returned, matching
// the trim policy at the end of runtimeSwap.
func (t *ShardReindexTaskGeneric) finalizeMigrationAfterRecovery(
	ctx context.Context, logger logrus.FieldLogger, shard ShardLike, props []string,
) error {
	// Ordering contract: rebuild must run and be checked before
	// OnMigrationComplete (see runtimeSwap for the full reasoning).
	if err := t.rebuildRangeableInMemoryReps(ctx, logger, shard, props); err != nil {
		return err
	}
	if err := t.strategy.OnMigrationComplete(ctx, shard); err != nil {
		return fmt.Errorf("on migration complete: %w", err)
	}
	// This path reaches a flipped record without going through runtimeSwap, so
	// it owes the same relation pass: a predecessor superseded by this record
	// is retired here or waits for a restart.
	if concrete, err := unwrapShard(ctx, shard); err == nil {
		concrete.retireSupersededMigrations(ctx)
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
// failing the migration: data work (prepend, swap, tidy) has already
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

// onAfterLsmInitGuarded is the DTM-route entry into the after-LSM-init hook:
// scheduler/worker goroutines hold no closeLock, so a drain that outlived a
// collection DELETE must stop before it opens a single bucket. Returns
// context.Canceled unwrapped when the index is closing (clean stop).
func (t *ShardReindexTaskGeneric) onAfterLsmInitGuarded(ctx context.Context, shard *Shard) error {
	if err := t.indexClosingGuard(shard); err != nil {
		t.logger.Debug("index is closing, stopping after-LSM-init hook")
		return err
	}
	return t.OnAfterLsmInit(ctx, shard)
}

// OnAfterLsmInit is the shard-init entry into the after-LSM-init hook, and the
// shared body of both routes. It runs no closing check of its own: some
// NewShard routes already hold closeLock.RLock, and sync.RWMutex is not
// reentrant, so taking it again deadlocks against a queued drop() writer.
// DTM-driven callers MUST use onAfterLsmInitGuarded instead.
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

	// An extant record fixes the property set for the rest of the migration:
	// re-discovering it here would let a schema that moved on since the claim
	// stage data for one set and flip pointers for another.
	props := t.findPropsToReindex(shard)
	if hasRecord {
		props = rec.Subject().Properties
	}
	logger.WithField("props", props).Debug("props found")
	if len(props) == 0 {
		logger.Debug("no props found. nothing to do")
		return nil
	}

	// Promotion has renamed the staged directory onto the canonical name, so
	// the copy this hook would open and keep fresh is the live one already.
	//
	// A recorded flip that has NOT been promoted is deliberately not stopped
	// here. The pointer flip lives only in the process that made it, so this
	// load serves the property from the canonical directory again — the one
	// promotion removes before renaming the staged directory over it. Without
	// the mirror below, every write taken until then is deleted by the
	// promotion that follows.
	if hasRecord && rec.State() == MigrationStatePromoted {
		logger.Debug("migration already promoted. nothing to open")
		return nil
	}

	// Committed means the prepend loop finished, so the rebuild's own buckets
	// have served their purpose and their directories may already be gone.
	committed := hasRecord && rec.StagedDataComplete()
	if committed {
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
	disableJustRegistered := t.registerDoubleWriteCallbacksFn(shard, props, t.ingestBucketName)

	if hasRecord {
		return nil
	}

	// The first durable write. The horizon it fixes is captured only after
	// the callbacks are live, so every write the iterator skips
	// (LastUpdateTimeUnix >= the horizon) is already mirrored into ingest.
	// Ceiled up one ms: LastUpdateTimeUnix has ms resolution and the skip
	// predicate is `<`, so a write sharing the truncated ms could otherwise
	// land before registration. Overlap writes converge because reindex
	// segments precede ingest in merge order and writes are per-key
	// idempotent.
	cutoff := time.Now().Truncate(time.Millisecond).Add(time.Millisecond)
	subject := t.migrationSubject(shard, props, cutoff)
	if err = t.putMigrationRecord(shard, NewMigrationRecordIterating(subject, MigrationCheckpoint{})); err != nil {
		// Disable only the pair registered above; the task may hold live
		// registrations for other shards.
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

	// A cancelled worker re-enters here with no ctx check of its own, and
	// everything below this point creates directories.
	if err = t.indexClosingGuard(shard); err != nil {
		logger.Debug("index is closing, stopping reindex drain")
		return zerotime, false, err
	}

	rec, hasRecord := t.migrationRecord(shard)
	var props []string
	if hasRecord {
		props = rec.Subject().Properties
	}

	if hasRecord && rec.PointerSwapped() {
		// Defense in depth: a durable flip decision means a previous run
		// reported a successful migration on this shard. The expected state
		// is the strategy's target bucket existing and populated. If the
		// bucket is missing now (e.g. a DELETE removed the index between that
		// previous run and this re-trigger), calling OnMigrationComplete here
		// would re-flip the schema flag and report success while the
		// customer's index is in fact empty. Fail loudly instead.
		for _, propName := range props {
			bucketName := t.strategy.SourceBucketName(propName)
			if shard.Store().Bucket(bucketName) == nil {
				err = fmt.Errorf(
					"stale migration state on shard %q: the record claims property %q is complete, but target bucket %q is missing — usually caused by a DELETE between the previous successful reindex and this one; refusing to silently report success",
					shard.Name(), propName, bucketName)
				return zerotime, false, err
			}
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

	if !hasRecord {
		err = fmt.Errorf("missing migration record")
		return zerotime, false, err
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

	defer func() {
		if err != nil && !bytes.Equal(lastStoredKey.Bytes(), lastProcessedKey.Bytes()) {
			logger.WithField("last_processed_key", lastProcessedKey).Debug("recording progress on error")
			if cerr := t.recordCheckpoint(shard, subject, lastProcessedKey, processedCount, indexedCount); cerr != nil {
				logger.Warnf("recording reindex progress on error: %v", cerr)
			}
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

			breakCh <- processedCount%t.config.checkProcessingEveryNoObjects == 0 &&
				time.Since(processingStarted) > t.config.processingDuration
		}
	}
	if !bytes.Equal(lastStoredKey.Bytes(), lastProcessedKey.Bytes()) {
		if err := t.recordCheckpoint(shard, subject, lastProcessedKey, processedCount, indexedCount); err != nil {
			err = fmt.Errorf("recording reindex progress: %w", err)
			return zerotime, false, err
		}
		lastStoredKey = lastProcessedKey.Clone()
	}
	if finished {
		// Durability barrier: flush every per-property reindex bucket's
		// memtable to a segment BEFORE recording the rebuild complete.
		// Without this, a SIGKILL between that record write and the
		// eventual [runtimeSwap] Step 1 (FlushAndSwitch) loses any
		// in-memtable writes — the record would claim a complete rebuild
		// while the re-tokenized rows are still in volatile memory. On
		// restart the resume skips re-iterating, the swap prepends a
		// truncated reindex bucket into ingest, the cluster schema flips
		// to the new tokenization, and this replica's canonical bucket is
		// missing the lost rows: queries return per-replica divergent
		// counts.
		//
		// Doing it here makes the record write strictly happen-after durable
		// persistence, on both the barrier path (where the swap is deferred
		// and the crash window is wide) and the inline one.
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
// against an old-tokenized bucket. Only allowed work: the in-memory
// pointer flip. The loop performs no I/O at all.
//
//   - store.SwapBucketPointer(mainName, ingestName) per prop
//   - the flip decision, written ahead of the loop
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
//   - os.RemoveAll(oldMainDir) per prop (safe inline — the load-bearing
//     rule is: only remove a shut-down bucket, never one that is still
//     serving queries)
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
//   - trimOlderGenerationsLocked (removes the current gen's reindex
//     dir + every older gen's sidecars)
//
// Live-bucket rename (Phase 3): the ingest bucket whose pointer was
// flipped into the canonical slot is STILL at __ingest_<gen>/ on
// disk. That rename to the canonical name is deferred to next
// load via reconciliation, because renaming a
// dir whose mmaps are open would corrupt the segment registry.
//
// Disable double-write callbacks via a defer at the top of the
// function so callbacks stop on every exit path. Same-process
// retry of runtimeSwap is not supported (the in-memory bucket
// state is partially mutated); recovery after a mid-swap crash
// happens after the next node restart, through reconciliation at shard
// init and RunSwapOnShard's record dispatch.
// runtimePrepare runs the Phase 1 (background-safe) preparation work
// that used to be inlined into runtimeSwap.
//
// Performs, per property:
//   - reindexBucket.FlushAndSwitch()            // memtable → segments
//   - store.ShutdownBucket(reindexName)         // drains compaction
//   - ingestBucket.PrependSegmentsFromBucket(...) // segment copy
//
// Then commits the staged data in the record and removes the reindex
// bucket dirs.
//
// Bucket=OLD and schema=OLD throughout — queries on the live main
// bucket continue correctly. The per-shard tokenization overlay
// MUST NOT yet be set: setting it before this call would expose the
// very gap the overlay was supposed to close (query input
// tokenized as NEW against the still-OLD bucket while prep does
// disk I/O for seconds).
//
// The caller checks that the record is not yet committed before calling.
//
// Crash safety: the record advances to Merged after the per-prop loop and
// BEFORE removeReindexBucketsDirs, so a crash in that window leaves a
// committed record with reindex dirs partially removed. The removal re-runs
// harmlessly, but no path reloads the live bucket, so the shard can report the
// migration complete while still serving pre-migration data. Tracked as
// weaviate/etienne-claude-issues#390.
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

	// The record commits when the loop finishes: from here the staged data is
	// complete and every sweep must preserve it. Removing the source dirs is
	// janitorial and idempotent by recorded handle, so it follows the write
	// rather than gating it.
	subject, err := t.migrationSubjectNow(shard)
	if err != nil {
		return err
	}
	if err := t.putMigrationRecord(shard, NewMigrationRecordMerged(subject)); err != nil {
		return fmt.Errorf("recording the staged data as complete: %w", err)
	}

	// Their segments have been copied into ingest, so the originals are no
	// longer needed. Idempotent: safe to call when the dirs are already gone.
	if err := t.removeReindexBucketsDirs(ctx, logger, shard, props); err != nil {
		return fmt.Errorf("removing reindex bucket dirs: %w", err)
	}

	logger.Debug("runtime prepare: all props merged")
	return nil
}

func (t *ShardReindexTaskGeneric) runtimeSwap(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, props []string,
) error {
	// The mirror survives every error exit. Restart promotion already promotes a
	// merged generation unconditionally and removes the old canonical directory,
	// so tearing the mirror down mid-loop would route every write to a
	// not-yet-flipped property into the one directory the restart then deletes:
	// the teardown was itself the loss mechanism. Disarming on completion is
	// hygiene — past the flip the mirror resolves by name to the surviving
	// bucket and copies nothing new.
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

	// Write-ahead: the flip decision is durable before the first pointer moves,
	// which is what lets a Merged record prove no flip was ever decided. The
	// displaced handles resolve only now — they are the directories currently
	// serving these properties.
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

	// Before the flip, not after it. Retirement shuts the predecessor's staged
	// bucket down, and a write that already read the pre-disarm callback
	// snapshot can still fire afterwards. The identity check in the callbacks'
	// shared prologue already stops that write from reaching the successor's
	// live data; this ordering keeps it landing somewhere useful instead of
	// nowhere, by leaving the predecessor's bucket resolvable while stragglers
	// drain. It is outside the no-I/O rule, which spans the first pointer flip
	// to the last.
	// The one directory that must survive until Phase 2b — the predecessor's
	// live data, which this flip displaces — is held back by this record's own
	// displaced claim.
	if concrete, err := unwrapShard(ctx, shard); err == nil {
		concrete.retireSupersededMigrations(ctx)
	} else {
		logger.Warnf("runtime swap: cannot retire superseded migrations: %v", err)
	}

	// Phase 2a (atomic, tight loop): in-memory pointer swap per property.
	// This is the ONLY work that runs inside the per-shard tokenization
	// overlay's "mixed-state" window (between first prop swapped and last
	// prop swapped). The loop performs no I/O at all: SwapBucketPointer is a
	// single map-write under bucketsLock, and the flip decision was already
	// made durable before the loop began.
	//
	// The slow disk work — shutting the displaced bucket down and removing it
	// — is pulled OUT into Phase 2b so it cannot extend the mixed-state
	// window. It touches only the OLD, already shut-down bucket, never the
	// live one, whose directory keeps its staged name until reconciliation
	// promotes it at a later load (renaming a dir whose buckets are mmap'd
	// would corrupt the segment registry).
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

	// Phase 2b (post-atomic, slow but inline): shutdown + removal of the
	// OLD (now-dead) main buckets. The load-bearing rule is: remove
	// only shut-down buckets; never remove a live bucket that is
	// serving queries. The OLD bucket is no longer in the store's
	// bucketsByName map (SwapBucketPointer deleted it), so it's not
	// serving queries; Shutdown drains any in-flight compaction and
	// closes mmaps cleanly. Removing its dir leaves the LIVE bucket
	// (still at ingest_<gen> on disk) as the only candidate for the
	// canonical name on next restart.
	//
	// This work is OUTSIDE the mixed-state window — every prop has
	// already had its in-memory pointer swapped. Queries during this
	// phase see new buckets for all props (overlay matches), so
	// per-prop slow ops here don't extend the correctness-sensitive
	// window.
	for _, propName := range props {
		oldMainBucket, ok := oldMainBuckets[propName]
		if !ok {
			// The property had no ingest-name entry left, so an earlier
			// attempt in this process already flipped and retired it.
			continue
		}
		if err := oldMainBucket.Shutdown(ctx); err != nil {
			return fmt.Errorf("shutting down old main bucket for %q: %w", propName, err)
		}
		// Removed at the recorded handle rather than renamed aside. A derived
		// backup name parks a crash's leftovers where no record points, and
		// nothing attributes a directory no record names.
		if err := os.RemoveAll(oldMainBucket.GetDir()); err != nil {
			return fmt.Errorf("removing displaced dir for %q: %w", propName, err)
		}
	}

	// The live bucket's dir keeps its staged name until reconciliation
	// promotes it at a later load: renaming a dir whose buckets are mmap'd by
	// the in-memory store would corrupt the segment registry, and at load
	// nothing has opened it yet.
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

	// Trim older generations on disk (best-effort cleanup of sidecar
	// dirs from prior migrations on this prop). Independent of the atomic
	// window: operates on sidecar and .migrations dirs whose owning gen is
	// strictly older than this gen.
	t.trimOlderGenerationsLocked(logger, shard, props)

	logger.Info("runtime swap: migration complete (ingest→main rename deferred to next restart)")

	return nil
}

// trimOlderGenerationsLocked removes on-disk leftovers from generations
// older than `currentGen` for the strategy's (prefix, propNamesSuffix).
// Called at the end of [runtimeSwap].
//
// Removes, per shard:
//   - all `…_<reindexSuffix-base>_<M>/` and `…_<ingestSuffix-base>_<M>/`
//     dirs with M < currentGen.
//   - all `.migrations/<migrationDirPrefix><propSuffix>_<M>/` for
//     M < currentGen.
//
// Keeps every directory a record still names. Those belong to the relation,
// which disarms the mirror pointed at them and shuts their buckets down before
// removing anything; removing one here would delete an open bucket's directory
// out from under a mirror that is still copying into it, and with both-or-
// neither semantics the next user write fails. What is left for this sweep is
// what no record can attribute: leftovers of a cluster that predates the
// records, and of runs whose record has already gone.
func (t *ShardReindexTaskGeneric) trimOlderGenerationsLocked(
	logger logrus.FieldLogger, shard ShardLike, props []string,
) {
	// Not a cold-tenant hydration despite the shape: this runs on a shard
	// whose buckets this node just swapped, so it is loaded by construction
	// and the unwrap is a type assertion.
	concrete, err := unwrapShard(context.Background(), shard)
	if err != nil {
		logger.Warnf("runtime swap: trim: failed to unwrap shard; skipping cleanup: %v", err)
		return
	}
	preserve, ok := trimPreserveSetOf(concrete)
	if !ok {
		logger.Warn("runtime swap: trim: migration records could not all be read; trimming nothing")
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

// migrationTrimPreserve answers what this trim may not remove: anything a
// readable record names, and anything the release before the records marked
// as completed. Everything else on the shard is what no record can attribute.
type migrationTrimPreserve struct {
	records []MigrationRecord
	legacy  map[string]struct{}
}

// trimPreserveSetOf builds it, or reports that it cannot. The records are the
// whole protection set for the removals, and a record this build cannot read
// names directories nothing else will vouch for — so an incomplete set is not
// a smaller one, it is no answer at all.
func trimPreserveSetOf(shard *Shard) (migrationTrimPreserve, bool) {
	if shard.migrationRecords == nil || len(shard.migrationRecords.Unreadable()) > 0 {
		return migrationTrimPreserve{}, false
	}
	records := shard.migrationRecords.Records()
	return migrationTrimPreserve{
		records: records,
		legacy:  migrationLegacyMarkerDirsAt(shard.pathLSM(), records),
	}, true
}

func (p migrationTrimPreserve) bucketDir(dir string) bool {
	if _, ok := p.legacy[dir]; ok {
		return true
	}
	for _, rec := range p.records {
		if rec.OwnsBucket(dir) {
			return true
		}
	}
	return false
}

func (p migrationTrimPreserve) trackerDir(dir string) bool {
	if _, ok := p.legacy[dir]; ok {
		return true
	}
	for _, rec := range p.records {
		if rec.Subject().TrackerDir == dir {
			return true
		}
	}
	return false
}

// obsoleteSidecarDirs names the sidecar bucket directories this trim owns.
// They live at the top of the LSM directory and are matched by an
// "_<base>_<M>" suffix on a name starting with the property's main bucket.
// The current generation's ingest directory is deliberately kept: it is the
// data the flip just made live.
func (t *ShardReindexTaskGeneric) obsoleteSidecarDirs(logger logrus.FieldLogger,
	lsmPath string, props []string, preserve migrationTrimPreserve,
) []string {
	entries, err := os.ReadDir(lsmPath)
	if err != nil {
		logger.Warnf("runtime swap: trim: failed to read LSM dir; skipping cleanup: %v", err)
		return nil
	}
	// Reverse the gen suffix off each current suffix to get the
	// suffix-without-gen base for prefix matching against older generations
	// on disk. genSuffix = "_<N>"; everything before the last "_<digits>" is
	// the base.
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
				// Already removed during runtimeSwap step 2; this is the
				// leftover of a run that did not get that far.
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

// obsoleteTrackerDirs names the tracker directories this trim owns: older
// generations of this strategy and property tuple.
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

// setMigrationIdentity stamps the identity the record key is built from. It is
// applied after construction because the same constructor serves the
// rehydrate path, which reaches its identity from a different place.
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

// migrationRecord is this migration's durable state: what reconciliation left
// on the shard at load, advanced by whatever the engine has done since.
func (t *ShardReindexTaskGeneric) migrationRecord(shard ShardLike) (MigrationRecord, bool) {
	store := shard.migrationRecordStore()
	if store == nil {
		return nil, false
	}
	return store.Get(t.migrationRecordKey())
}

// putMigrationRecord makes one transition durable. It refuses an incomplete
// identity rather than writing under a key the loader would reject: an
// unreadable record withholds every destructive and promoting action on the
// shard, so writing one would freeze reconciliation.
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

// flushReindexBuckets is the durability barrier every progress record has to
// clear. FlushAndSwitch's contract is that every write which returned before
// the call is in an fsynced segment by the time it returns.
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

// recordCheckpoint advances the iteration resume point. The whole record is
// rewritten because a checkpoint only means anything alongside the horizon and
// the directories the same record names.
func (t *ShardReindexTaskGeneric) recordCheckpoint(shard ShardLike, subject MigrationSubject,
	lastProcessedKey indexKey, processedCount, indexedCount int,
) error {
	// The checkpoint is fsynced and the postings it vouches for are not, so
	// without this the two disagree after a crash: the resume seeks strictly
	// past the checkpoint key, and nothing ever rebuilds a posting the crash
	// dropped from the buffer. The flip then promotes a bucket permanently
	// missing it.
	if err := t.flushReindexBuckets(shard, subject.Properties, "recording iteration progress"); err != nil {
		return err
	}
	return t.putMigrationRecord(shard, NewMigrationRecordIterating(subject, MigrationCheckpoint{
		LastProcessedKey: lastProcessedKey.Clone().Bytes(),
		ProcessedCount:   processedCount,
		IndexedCount:     indexedCount,
		UpdatedAt:        time.Now(),
	}))
}

// migrationSubjectNow is the subject every transition after the first has to
// carry. Advancing the recorded one rather than rebuilding it is what keeps
// the facts fixed when the migration armed — the iteration horizon, the
// displacement links — from being re-derived out of state that has moved on.
func (t *ShardReindexTaskGeneric) migrationSubjectNow(shard ShardLike) (MigrationSubject, error) {
	rec, ok := t.migrationRecord(shard)
	if !ok {
		return MigrationSubject{}, fmt.Errorf("migration %s has no record on shard %q",
			t.migrationRecordKey(), shard.Name())
	}
	return rec.Subject(), nil
}

// migrationSubject names every directory the migration touches. Bucket names
// are directory names, so the record holds handles rather than a recipe for
// re-deriving them.
func (t *ShardReindexTaskGeneric) migrationSubject(shard ShardLike, props []string, cutoff time.Time) MigrationSubject {
	subject := MigrationSubject{
		Key:                  t.migrationRecordKey(),
		TaskID:               t.taskID,
		MigrationType:        t.migrationType,
		Properties:           props,
		TargetTokenization:   t.targetTokenization,
		OriginalTokenization: t.originalTokenization,
		IterationCutoff:      cutoff,
		TrackerDir:           t.strategy.MigrationDirName(),
		StagedDirs:           make(map[string]string, len(props)),
		CanonicalDirs:        make(map[string]string, len(props)),
		SidecarDirs:          make([]string, 0, len(props)),
	}
	for _, propName := range props {
		subject.StagedDirs[propName] = t.ingestBucketName(propName)
		subject.CanonicalDirs[propName] = t.strategy.SourceBucketName(propName)
		subject.SidecarDirs = append(subject.SidecarDirs, t.reindexBucketName(propName))
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

// registerDoubleWriteCallbacks arms the strategy's add/delete mirror callbacks
// and publishes one disarm handle per (record, property) on the shard. The
// returned func disarms the whole record, for the failure paths that must not
// touch other shards' registrations.
//
// The handles go on the shard rather than on this task instance because the
// actor that disarms is never the actor that armed: a successor's retirement,
// reconciliation's cancel edge, terminal cleanup. The provider also clears a
// terminal task's instance cache outright.
//
// Per property because the relation that disarms is per property: a
// successor's property set can partially overlap a committed predecessor's,
// and one shared handle would either keep mirroring a property the successor
// took over — writing predecessor-form rows into the successor's live bucket
// once its staged bucket is shut down — or stop mirroring the properties the
// successor never touched.
func (t *ShardReindexTaskGeneric) registerDoubleWriteCallbacks(shard *Shard, props []string,
	bucketNamer func(string) string,
) func() {
	// The staged buckets are open by now (loadIngestBuckets precedes this
	// call) and each one is this mirror's for the record's whole life, so the
	// pointers can be captured once. Without them the callbacks cannot tell
	// their own flip from someone shutting their bucket down.
	buckets := make(map[string]*lsmkv.Bucket, len(props))
	for _, propName := range props {
		if bucket := shard.store.Bucket(bucketNamer(propName)); bucket != nil {
			buckets[propName] = bucket
		}
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

	// Through the registry, so the published handles never outlive the
	// callbacks they disarm.
	return func() { registry.DisarmMigrationMirrors(key) }
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
	// Sort for determinism — map iteration order is randomized and downstream
	// downstream state hashes the list.
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
