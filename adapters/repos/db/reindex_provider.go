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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	api "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	entschema "github.com/weaviate/weaviate/entities/schema"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// ReindexProvider implements distributedtask.UnitAwareProvider for reindex tasks.
// It uses the existing ShardReindexTaskGeneric machinery to execute the actual
// migration work, with the DTM providing cluster coordination, progress tracking,
// and lifecycle management.
//
// Migration family classification (see [IsSemanticMigration] for the
// authoritative predicate):
//
//   - "Semantic" migrations are the ones that change query
//     semantics for the migrated property — change-tokenization,
//     change-tokenization-filterable, enable-filterable, enable-searchable.
//     These get the full barrier dance: every shard reindexes first
//     (RunReindexOnlyOnShard), and only after every unit is terminal does
//     OnGroupCompleted fire to run the swap phase (RunSwapOnShard) on each
//     local shard, followed by OnTaskCompleted's cluster-wide schema flip.
//     No shard serves new data until ALL shards are ready. This is where
//     the SWAPPING-window tokenization overlay lives.
//
//   - "Format-only" migrations don't change query semantics — they only
//     change the on-disk bucket format. enable-rangeable, repair-rangeable,
//     repair-filterable, repair-searchable (Map→Blockmax), and the
//     RoaringSetRefresh strategy fall in this bucket. Each shard runs the
//     full lifecycle independently via RunOnShard; there is no cluster-wide
//     schema flip to coordinate.
//
// Note on enable-rangeable: it is intentionally NOT classified as
// semantic. Range queries' correctness during the migration is gated
// by the per-shard rangeableLocalReady flag (see [Shard.rangeableLocalReady]),
// not by the barrier dance — falling back to the filterable bucket walk
// on shards that haven't completed locally is slow but correct.
type ReindexProvider struct {
	mu       sync.Mutex
	recorder distributedtask.TaskCompletionRecorder

	db            *DB
	schemaManager *schema.Manager
	logger        logrus.FieldLogger
	localNode     string
	concurrency   func() int

	// serverCtx is cancelled when the server is shutting down. OnGroupCompleted
	// fires after StartTask's per-task goroutine has already returned (its ctx
	// is gone by then), so we cannot use the per-task ctx for the swap phase —
	// we derive from the server ctx instead so a graceful shutdown can abort
	// long-running swaps.
	serverCtx context.Context

	runningHandles map[distributedtask.TaskDescriptor]*reindexTaskHandle

	// payloads caches deserialized task payloads for use in OnGroupCompleted,
	// which receives the raw *Task but needs the typed payload.
	payloads map[distributedtask.TaskDescriptor]*ReindexTaskPayload

	// reindexTasks caches the ShardReindexTaskGeneric instances created during
	// processOneUnit, keyed by task descriptor and unit ID. For semantic
	// migrations, OnGroupCompleted must call RunSwapOnShard on the SAME task
	// instances that ran RunReindexOnlyOnShard, because those instances have
	// the double-write callbacks registered via OnAfterLsmInit. Creating new
	// task instances in OnGroupCompleted would lose those callbacks.
	reindexTasks map[distributedtask.TaskDescriptor]map[string][]*ShardReindexTaskGeneric

	// activeWorkers tracks units that currently have a per-unit goroutine
	// inside processOneUnit's iteration body. The re-entry guard reads
	// this (not the reindexTasks cache) so post-restart recovery — which
	// seeds reindexTasks via [SeedReindexTaskCache] for OnGroupCompleted's
	// callback preservation — does NOT short-circuit the resumed unit.
	// weaviate/0-weaviate-issues#239 Mode 2.
	//
	// Guarded by [mu]. Set after the guard, cleared from a defer so any
	// return path (failure, context.Canceled, panic) releases the slot.
	activeWorkers map[distributedtask.TaskDescriptor]map[string]bool

	// cleanupInProgressMu guards [cleanupInProgress]. Held only for the
	// register/unregister/lookup increment/decrement; never held across
	// the actual sidecar-teardown call (that runs unlocked, the registry
	// only records "a cleanup is mid-flight on this tuple").
	cleanupInProgressMu sync.RWMutex
	// cleanupInProgress is the per-(collection, shard) refcount of
	// in-flight terminal-task cleanups. The backup gate consults this
	// alongside the DTM activity lookup so a backup landing in the
	// "task is terminal in DTM but [autoCleanupAfterTerminal] is still
	// tearing __reindex / __ingest sidecars" gap sees the shard as
	// busy and refuses. Refcount (not bool) so re-entrant cleanups —
	// two terminal-state transitions on different (property,
	// indexType) tuples sharing the same shard — don't lose each
	// other's registration.
	cleanupInProgress map[reindexCleanupKey]int
}

// reindexCleanupKey identifies a per-(collection, shard) slot in the
// [ReindexProvider.cleanupInProgress] registry. Used as the map key so
// the backup gate's "is cleanup mid-flight on this shard?" lookup is
// a single map probe.
type reindexCleanupKey struct {
	collection string
	shard      string
}

// phaseUnitResolution holds the per-unit setup work that every per-shard
// phase callback needs before running. Skip=true → caller silently moves
// on; non-empty Errs → setup failed, caller MUST NOT proceed; Rehydrate=true
// → tasks were just instantiated from disk and need RunReindexOnlyOnShard
// before any phase work.
type phaseUnitResolution struct {
	Shard              ShardLike
	UnitTasks          []*ShardReindexTaskGeneric
	Rehydrate          bool
	Errs               []string
	SawContextCanceled bool
	Skip               bool
}

// phaseResult is the aggregated outcome of a per-unit phase callback:
// per-task error strings + the shutdown-cancellation signal the scheduler
// needs for transient-vs-permanent ack routing.
type phaseResult struct {
	Errs               []string
	SawContextCanceled bool
}

// NewReindexProvider creates a new ReindexProvider. The concurrency function
// is called at task start time to determine how many shards to reindex in
// parallel (typically backed by a runtime.DynamicValue). serverCtx should
// be a process-shutdown context so the OnGroupCompleted swap phase can
// abort cleanly on graceful shutdown.
// composeProgressEnvelope maps a single sub-task's 0-1 progress into
// the unit-wide envelope: each of N sub-tasks owns 1/N of [0, 1].
// Capped at 0.99 to leave headroom for the final 1.0 written by
// RecordDistributedTaskUnitCompletion. With N=1 this is a no-op clamp.
func composeProgressEnvelope(taskIdx, totalTasks int, progress float32) float32 {
	if totalTasks <= 0 {
		return 0
	}
	envelope := (float32(taskIdx) + progress) / float32(totalTasks)
	if envelope > 0.99 {
		envelope = 0.99
	}
	if envelope < 0 {
		envelope = 0
	}
	return envelope
}

func NewReindexProvider(
	db *DB,
	schemaManager *schema.Manager,
	logger logrus.FieldLogger,
	localNode string,
	concurrency func() int,
	serverCtx context.Context,
) *ReindexProvider {
	if serverCtx == nil {
		serverCtx = context.Background()
	}
	return &ReindexProvider{
		db:                db,
		schemaManager:     schemaManager,
		logger:            logger,
		localNode:         localNode,
		concurrency:       concurrency,
		serverCtx:         serverCtx,
		runningHandles:    make(map[distributedtask.TaskDescriptor]*reindexTaskHandle),
		payloads:          make(map[distributedtask.TaskDescriptor]*ReindexTaskPayload),
		reindexTasks:      make(map[distributedtask.TaskDescriptor]map[string][]*ShardReindexTaskGeneric),
		activeWorkers:     make(map[distributedtask.TaskDescriptor]map[string]bool),
		cleanupInProgress: make(map[reindexCleanupKey]int),
	}
}

func (p *ReindexProvider) SetCompletionRecorder(recorder distributedtask.TaskCompletionRecorder) {
	p.recorder = recorder
}

// SeedReindexTaskCache pre-populates the per-descriptor task cache with
// instances reconstructed during startup recovery (see
// [DiscoverInFlightReindexTasks] and [RegisterRecoveredReindexes]). The
// purpose is to make OnGroupCompleted reuse the recovered instances —
// whose double-write callbacks were re-registered during shard init —
// rather than fall through to the rehydrate branch and call
// OnAfterLsmInit a second time (which would attempt to load already
// loaded ingest buckets).
//
// Safe to call concurrently with StartTask: StartTask only writes
// entries for tasks it is starting, while seeding fills entries that
// would otherwise be missing because the scheduler isn't (re)starting
// the task post-restart.
func (p *ReindexProvider) SeedReindexTaskCache(
	cache map[distributedtask.TaskDescriptor]map[string][]*ShardReindexTaskGeneric,
) {
	if len(cache) == 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for desc, byUnit := range cache {
		if p.reindexTasks[desc] == nil {
			p.reindexTasks[desc] = map[string][]*ShardReindexTaskGeneric{}
		}
		for unitID, tasks := range byUnit {
			if len(p.reindexTasks[desc][unitID]) == 0 {
				p.reindexTasks[desc][unitID] = tasks
			}
		}
	}
}

func (p *ReindexProvider) GetLocalTasks() []distributedtask.TaskDescriptor {
	return nil
}

// Lock-coupled getters/setters for the in-memory caches keyed by
// TaskDescriptor. Every state mutation goes through one of these so the
// lock is always released via defer.

func (p *ReindexProvider) registerStartingTask(desc distributedtask.TaskDescriptor, handle *reindexTaskHandle, payload *ReindexTaskPayload) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.runningHandles[desc] = handle
	p.payloads[desc] = payload
}

func (p *ReindexProvider) deleteRunningHandle(desc distributedtask.TaskDescriptor) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.runningHandles, desc)
}

func (p *ReindexProvider) runningHandle(desc distributedtask.TaskDescriptor) (*reindexTaskHandle, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	handle, ok := p.runningHandles[desc]
	return handle, ok
}

func (p *ReindexProvider) cachedPayload(desc distributedtask.TaskDescriptor) *ReindexTaskPayload {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.payloads[desc]
}

func (p *ReindexProvider) cachedReindexTasks(desc distributedtask.TaskDescriptor, unitID string) []*ShardReindexTaskGeneric {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.reindexTasks[desc][unitID]
}

func (p *ReindexProvider) cacheReindexTasks(desc distributedtask.TaskDescriptor, unitID string, tasks []*ShardReindexTaskGeneric) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.reindexTasks[desc] == nil {
		p.reindexTasks[desc] = make(map[string][]*ShardReindexTaskGeneric)
	}
	p.reindexTasks[desc][unitID] = tasks
}

func (p *ReindexProvider) clearTaskCaches(desc distributedtask.TaskDescriptor) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.payloads, desc)
	delete(p.reindexTasks, desc)
}

// claimActiveWorker reserves the (desc, unitID) slot in activeWorkers.
// Returns false if another worker already holds it.
func (p *ReindexProvider) claimActiveWorker(desc distributedtask.TaskDescriptor, unitID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.activeWorkers[desc][unitID] {
		return false
	}
	if p.activeWorkers[desc] == nil {
		p.activeWorkers[desc] = make(map[string]bool)
	}
	p.activeWorkers[desc][unitID] = true
	return true
}

func (p *ReindexProvider) releaseActiveWorker(desc distributedtask.TaskDescriptor, unitID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.activeWorkers[desc], unitID)
	if len(p.activeWorkers[desc]) == 0 {
		delete(p.activeWorkers, desc)
	}
}

func (p *ReindexProvider) CleanupTask(_ distributedtask.TaskDescriptor) error {
	return nil
}

func (p *ReindexProvider) StartTask(task *distributedtask.Task) (distributedtask.TaskHandle, error) {
	var payload ReindexTaskPayload
	if err := json.Unmarshal(task.Payload, &payload); err != nil {
		return nil, fmt.Errorf("unmarshal reindex payload: %w", err)
	}

	className := entschema.ClassName(payload.Collection)
	idx := p.db.GetIndex(className)
	if idx == nil {
		return nil, fmt.Errorf("collection %q not found", payload.Collection)
	}

	// Determine which units belong to this node.
	var localUnits []string
	for unitID, nodeName := range payload.UnitToNode {
		if nodeName == p.localNode {
			localUnits = append(localUnits, unitID)
		}
	}

	if len(localUnits) == 0 {
		p.logger.WithField("taskID", task.ID).WithField("node", p.localNode).
			Info("reindex provider: no local units, skipping")
	}

	ctx, cancel := context.WithCancel(context.Background())
	handle := &reindexTaskHandle{
		cancel: cancel,
		doneCh: make(chan struct{}),
	}

	p.registerStartingTask(task.TaskDescriptor, handle, &payload)

	// Progress is emitted from the inverted-index reindex iteration every
	// checkProcessingEveryNoObjects iterations (default 1000). p.recorder
	// is the scheduler-provided recorder, already wrapped in a global
	// ThrottledRecorder (see Scheduler.Start) that caps per-unit writes
	// at 3s — sufficient for the GET /indexes poller without flooding
	// Raft. No additional throttle is needed here.
	enterrors.GoWrapper(func() {
		defer func() {
			p.deleteRunningHandle(task.TaskDescriptor)
			close(handle.doneCh)
		}()

		p.processUnits(ctx, task, &payload, idx, localUnits, p.recorder)
	}, p.logger)

	return handle, nil
}

func (p *ReindexProvider) processUnits(
	ctx context.Context,
	task *distributedtask.Task,
	payload *ReindexTaskPayload,
	idx *Index,
	localUnits []string,
	recorder distributedtask.TaskCompletionRecorder,
) {
	limiter := distributedtask.NewConcurrencyLimiter(p.concurrency())

	// defer Wait so an early return on Acquire ctx-cancel still drains
	// spawned per-unit goroutines before OnTaskCompleted's cleanup runs.
	var wg sync.WaitGroup
	defer wg.Wait()
	for _, unitID := range localUnits {
		unit := task.Units[unitID]
		if unit != nil && (unit.Status == distributedtask.UnitStatusCompleted || unit.Status == distributedtask.UnitStatusFailed) {
			continue
		}

		if err := limiter.Acquire(ctx); err != nil {
			return
		}

		wg.Add(1)
		unitID := unitID
		enterrors.GoWrapper(func() {
			defer wg.Done()
			defer limiter.Release()

			p.processOneUnit(ctx, task, payload, idx, unitID, recorder)
		}, p.logger)
	}
}

// processOneUnit executes reindex on a single unit (shard replica).
// For semantic migrations (e.g. change-tokenization), the task is cached so that
// OnGroupCompleted can reuse the same instance to run the swap phase — this
// preserves double-write callbacks registered during reindex.
func (p *ReindexProvider) processOneUnit(
	ctx context.Context,
	task *distributedtask.Task,
	payload *ReindexTaskPayload,
	idx *Index,
	unitID string,
	recorder distributedtask.TaskCompletionRecorder,
) {
	shardName := payload.UnitToShard[unitID]
	logger := p.logger.WithField("taskID", task.ID).
		WithField("unit", unitID).WithField("shard", shardName)

	logger.Info("reindex provider: starting unit")

	// Report initial progress to claim the unit.
	if err := recorder.UpdateDistributedTaskUnitProgress(
		ctx, task.Namespace, task.ID, task.Version, p.localNode, unitID, 0.0,
	); err != nil {
		logger.Errorf("reindex provider: failed to report initial progress: %v", err)
		return
	}

	// Find the shard.
	shard, err := lookupShardByName(idx, shardName)
	if err != nil {
		p.failUnit(ctx, task, unitID, recorder, err.Error())
		return
	}

	// Unwrap up front: the lsmPath is needed by createReindexTasks to
	// pick the per-shard generation suffix for this migration's
	// sidecar dirs. We also need the concrete shard for persistRecoveryRecord
	// below.
	concreteShard, unwrapErr := unwrapShard(ctx, shard)
	if unwrapErr != nil {
		p.failUnit(ctx, task, unitID, recorder,
			fmt.Sprintf("unwrap shard: %v", unwrapErr))
		return
	}

	// For semantic migrations (change-tokenization, enable-rangeable), use
	// two-phase execution: reindex only, then swap after all units complete.
	// For format-only migrations, run the full lifecycle per shard.
	semantic := IsSemanticMigration(payload.MigrationType)

	// Re-entry guard. The DTM scheduler can relaunch our task handle a
	// few tens of ms after the previous handle's per-unit goroutines
	// finished — wg.Wait() returns once recordUnitCompletion is called
	// from the worker, but the RAFT apply that flips the unit to
	// Completed (and lets the scheduler skip the relaunch) races the
	// next scheduler tick. We've observed the two "starting unit" logs
	// ~70ms apart on the same unit in CI (acceptance large
	// reindex-multinode-aj, MultiRoundRobin round 1).
	//
	// Without this guard, the relaunched processOneUnit calls
	// createReindexTasks(rehydrate=false), which picks
	// nextMigrationGeneration = max(existing)+1 = N+1 — a different
	// generation than the previous, in-flight run at gen N. The new
	// tasks (gen N+1) get written into p.reindexTasks[desc][unitID],
	// clobbering the gen-N task instances that OnGroupCompleted relies
	// on. When OnGroupCompleted then calls RunSwapOnShard on the
	// cached gen N+1 task, its tracker points at the (just-mkdir'd)
	// .migrations/<dir>_N+1/ which has no reindexed.mig → "shard is
	// not in reindexed state" → swap fails → migration is half-applied
	// on this replica → #10675-shape per-replica data divergence.
	//
	// Guard signal is `activeWorkers` (per-unit "a goroutine is inside
	// the iteration body right now"), NOT the `reindexTasks` cache.
	// weaviate/0-weaviate-issues#239 Mode 2: post-restart recovery
	// seeds `reindexTasks` so OnGroupCompleted can reuse the in-flight
	// task instances with their registered double-write callbacks —
	// using the cache as the guard signal trapped the resumed unit
	// forever after a leader restart (the QA c01-leader-postfix repro
	// + the local TestMultiNode_GracefulLeaderRestartDuringReindex
	// failure both surfaced this).
	if semantic {
		if !p.claimActiveWorker(task.TaskDescriptor, unitID) {
			logger.Info("reindex provider: skipping re-entered unit (concurrent worker)")
			return
		}
		defer p.releaseActiveWorker(task.TaskDescriptor, unitID)
	}

	// Use cached task instances when present. Two populating paths land
	// here: (a) post-restart [SeedReindexTaskCache] for callback-preserving
	// resume; (b) the FSM-lag re-entry case where the previous worker
	// cached gen-N tasks before exiting — reusing them avoids the gen-N+1
	// clobber the old guard existed to prevent.
	var (
		tasks  []*ShardReindexTaskGeneric
		cached bool
	)
	if semantic {
		tasks = p.cachedReindexTasks(task.TaskDescriptor, unitID)
		cached = len(tasks) > 0
	}
	if !cached {
		var createErr error
		tasks, createErr = p.createReindexTasks(payload, concreteShard.pathLSM(), false)
		if createErr != nil {
			p.failUnit(ctx, task, unitID, recorder, fmt.Sprintf("creating reindex tasks: %v", createErr))
			return
		}
	}

	// Compose per-task progress into a single per-unit envelope so the
	// operator sees a monotonic 0→1 climb across N tasks instead of N
	// independent 0→0.99 ramps that look like regressions on the same
	// /v1/tasks field. weaviate/0-weaviate-issues#232 Finding 1.
	totalTasks := len(tasks)
	for idx, reindexTask := range tasks {
		// Capture per-iteration; the closure may outlive this stack frame
		// because the callback fires from inside the reindex loop.
		taskRef := reindexTask
		taskIdx := idx
		taskRef.SetProgressCallback(func(progress float32) {
			envelope := composeProgressEnvelope(taskIdx, totalTasks, progress)
			if err := recorder.UpdateDistributedTaskUnitProgress(
				ctx, task.Namespace, task.ID, task.Version, p.localNode, unitID, envelope,
			); err != nil {
				logger.WithField("progress", envelope).
					Debugf("reindex provider: failed to report progress (will retry on next tick): %v", err)
			}
		})
	}

	// Cache task instances for semantic migrations so OnGroupCompleted can
	// call RunSwapOnShard on the same instances (with callbacks registered).
	// On the cached-tasks path (post-restart recovery or FSM-lag re-entry)
	// we already have these instances in the map; only write on the
	// fresh-tasks path.
	if semantic && !cached {
		p.cacheReindexTasks(task.TaskDescriptor, unitID, tasks)
	}

	// Persist a recovery record so that a restart mid-flight can rebuild
	// these same task instances during shard init. Without this, writes
	// arriving between shard init and OnGroupCompleted's swap go only to
	// the old main bucket (no ingest double-write) and are lost on swap.
	// See [ReindexProvider.persistRecoveryRecord] for the on-disk shape.
	//
	// Guarded against Index.drop: SaveRecoveryPayload MkdirAll's the
	// migration dir — same re-materialization race the tracker paths guard
	// via newReindexTrackerGuarded. This goroutine never holds closeLock.
	if err := concreteShard.Index().withCloseRLockGuard(func() error {
		return p.persistRecoveryRecord(task, payload, unitID, concreteShard.pathLSM(), tasks)
	}); err != nil {
		if errors.Is(err, context.Canceled) {
			// Index is closing (concurrent DELETE): the cascade-cancel will
			// terminate this task; stop quietly instead of failing the unit.
			p.logger.WithField("unit", unitID).
				Debug("index closing during recovery-record persist; stopping unit")
			return
		}
		// A failure to persist the recovery record means a restart in the
		// next few seconds would lose the in-flight reindex's double-write
		// callbacks. That is bad enough to fail the unit explicitly rather
		// than silently degrade.
		p.failUnit(ctx, task, unitID, recorder,
			fmt.Sprintf("persist reindex recovery record: %v", err))
		return
	}

	for _, reindexTask := range tasks {
		var runErr error
		if semantic {
			runErr = reindexTask.RunReindexOnlyOnShard(ctx, shard)
		} else {
			runErr = reindexTask.RunOnShard(ctx, shard)
		}
		if runErr != nil {
			// weaviate/0-weaviate-issues#239 Mode 1: don't FSM-flip
			// FAILED on a shutdown signal.
			if errors.Is(runErr, context.Canceled) {
				logger.Infof("reindex provider: unit interrupted by shutdown; will resume after restart: %v", runErr)
				return
			}
			p.failUnit(ctx, task, unitID, recorder,
				fmt.Sprintf("reindex (%s): %v", reindexTask.Name(), runErr))
			return
		}
	}

	logger.Info("reindex provider: unit completed")

	if err := recorder.RecordDistributedTaskUnitCompletion(
		ctx, task.Namespace, task.ID, task.Version, p.localNode, unitID,
	); err != nil {
		logger.Errorf("reindex provider: failed to record completion: %v", err)
		return
	}
}

// maxReindexPropertiesPerTask caps the number of properties in a single
// reindex task's payload. The REST handler today always submits one
// property per task, so this is defense-in-depth against future internal
// callers or a corrupt RAFT replay carrying a pathological array length.
const maxReindexPropertiesPerTask = 1024

// createReindexTasks constructs the strategy/task instances for a payload.
// Each per-strategy bucket-sidecar dir and the migration tracker dir carry
// a per-node generation suffix `_<N>` so back-to-back in-process
// migrations on the same property don't collide on dir paths.
//
// lsmPath is required because the generation is computed per-shard from
// the shard's local on-disk state. When rehydrate is true (called from
// [OnGroupCompleted]'s rehydrate path after a process restart lost the
// in-memory task cache), the generation is the highest existing
// in-flight one on disk — we want to reconstruct the SAME strategy
// instance the original processOneUnit constructed. When rehydrate is
// false (the fresh-task path from processOneUnit), the generation is
// `max(existing) + 1`.
//
// See `docs/runtime-reindex.md` for the deferred-finalize + per-migration-
// generation design rationale.
func (p *ReindexProvider) createReindexTasks(payload *ReindexTaskPayload, lsmPath string, rehydrate bool) ([]*ShardReindexTaskGeneric, error) {
	// Every migration type requires at least one property — repair-* / enable-*
	// because they're per-property migrations, change-tokenization because it
	// needs exactly one property. Check up front so each arm only deals with
	// its unique constraints.
	if len(payload.Properties) == 0 {
		return nil, fmt.Errorf("%s requires at least one property", payload.MigrationType)
	}
	if len(payload.Properties) > maxReindexPropertiesPerTask {
		return nil, fmt.Errorf("%s payload has %d properties; max is %d",
			payload.MigrationType, len(payload.Properties), maxReindexPropertiesPerTask)
	}

	// genFor returns the generation suffix N to use for this migration on
	// this shard, given the strategy's dir prefix and its props suffix
	// (e.g. "_text" or sorted-joined "_p1_p2", or "" for class-level
	// strategies). The ok return is always true on the normal path
	// (rehydrate=false). On rehydrate=true, ok=false means there is no
	// in-flight migration for this strategy on disk — every prior
	// generation's tracker dir was already cleaned up by either
	// `FinalizeCompletedMigrations` (at startup) or the end-of-swap trim
	// (in-process). The caller MUST skip task instantiation in that case;
	// instantiating with a fabricated gen would later try to swap from
	// reindex bucket dirs that no longer exist.
	genFor := func(prefix, propSuffix string) (int, bool) {
		if rehydrate {
			if gen := maxMigrationGeneration(lsmPath, prefix, propSuffix); gen > 0 {
				return gen, true
			}
			return 0, false
		}
		return nextMigrationGeneration(lsmPath, prefix, propSuffix), true
	}

	// On the normal path (rehydrate=false) genFor always returns ok=true.
	// On rehydrate (post-restart) ok=false means the strategy has no
	// in-flight on-disk state — `FinalizeCompletedMigrations` at startup
	// or the end-of-swap trim already cleaned up. Re-instantiating with
	// a fabricated gen would fail at runtimeSwap with "reindex bucket
	// not found", so callers skip task instantiation in that case.
	switch payload.MigrationType {
	case ReindexTypeChangeAlgorithm:
		gen, ok := genFor(MigrationDirSearchableMapToBlockmax, "")
		if !ok {
			return nil, nil
		}
		return []*ShardReindexTaskGeneric{
			NewRuntimeMapToBlockmaxTask(p.logger, p.schemaManager, payload.Properties, payload.Collection, gen),
		}, nil

	case ReindexTypeRebuildSearchable:
		gen, ok := genFor(MigrationDirPrefixRebuildSearchable, propsSuffix(payload.Properties))
		if !ok {
			return nil, nil
		}
		return []*ShardReindexTaskGeneric{
			NewRuntimeRebuildSearchableTask(p.logger, payload.Properties, payload.Collection, gen),
		}, nil

	case ReindexTypeRepairFilterable:
		gen, ok := genFor(MigrationDirFilterableRoaringsetRefresh, "")
		if !ok {
			return nil, nil
		}
		return []*ShardReindexTaskGeneric{
			NewRuntimeRoaringSetRefreshTask(p.logger, payload.Properties, payload.Collection, gen),
		}, nil

	case ReindexTypeEnableRangeable, ReindexTypeRepairRangeable:
		// Repair-rangeable uses the same strategy as enable-rangeable —
		// rangeable is rebuilt from the existing filterable bucket either
		// way. The validator at submit time gates which one is allowed
		// based on the property's current IndexRangeFilters state.
		gen, ok := genFor(MigrationDirPrefixFilterableToRangeable, propsSuffix(payload.Properties))
		if !ok {
			return nil, nil
		}
		return []*ShardReindexTaskGeneric{
			NewRuntimeFilterableToRangeableTask(p.logger, p.schemaManager, payload.Properties, payload.Collection, gen),
		}, nil

	case ReindexTypeEnableFilterable:
		gen, ok := genFor(MigrationDirPrefixEnableFilterable, propsSuffix(payload.Properties))
		if !ok {
			return nil, nil
		}
		return []*ShardReindexTaskGeneric{
			NewRuntimeEnableFilterableTask(p.logger, payload.Properties, payload.Collection, gen),
		}, nil

	case ReindexTypeEnableSearchable:
		if payload.TargetTokenization == "" {
			return nil, fmt.Errorf("enable-searchable requires targetTokenization")
		}
		gen, ok := genFor(MigrationDirPrefixEnableSearchable, propsSuffix(payload.Properties))
		if !ok {
			return nil, nil
		}
		return []*ShardReindexTaskGeneric{
			NewRuntimeEnableSearchableTask(p.logger, payload.Properties, payload.Collection, payload.TargetTokenization, gen),
		}, nil

	case ReindexTypeChangeTokenization:
		if len(payload.Properties) != 1 {
			return nil, fmt.Errorf("change-tokenization requires exactly one property")
		}
		propName := payload.Properties[0]
		if payload.TargetTokenization == "" {
			return nil, fmt.Errorf("change-tokenization requires targetTokenization")
		}
		if payload.BucketStrategy == "" {
			return nil, fmt.Errorf("change-tokenization requires bucketStrategy")
		}

		// ChangeTokenization spawns one sub-task per inverted index that
		// COULD be re-tokenized (searchable + filterable). For a property
		// that has IndexFilterable=false but IndexSearchable=true, the
		// filterable sub-task has no source bucket to swap into and
		// runtimeSwap would fail with "target bucket property_X not
		// found". The handler-side validator does not (yet) cover this
		// case, so the defense lives here: only dispatch the filterable
		// retokenize sub-task when the property actually has a filterable
		// index.
		var tasks []*ShardReindexTaskGeneric
		if searchableGen, ok := genFor(MigrationDirPrefixSearchableRetokenize, "_"+propName); ok {
			tasks = append(tasks, NewRuntimeSearchableRetokenizeTask(
				p.logger, propName, payload.TargetTokenization,
				payload.Collection, payload.BucketStrategy, payload.Collection,
				searchableGen,
			))
		}
		if p.propertyHasFilterableBucket(payload.Collection, propName) {
			if filterableGen, ok := genFor(MigrationDirPrefixFilterableRetokenize, "_"+propName); ok {
				tasks = append(tasks, NewRuntimeFilterableRetokenizeTask(
					p.logger,
					propName, payload.TargetTokenization,
					payload.Collection, payload.Collection,
					filterableGen,
				))
			}
		}
		return tasks, nil

	case ReindexTypeChangeTokenizationFilterable:
		if len(payload.Properties) != 1 {
			return nil, fmt.Errorf("change-tokenization-filterable requires exactly one property")
		}
		propName := payload.Properties[0]
		if payload.TargetTokenization == "" {
			return nil, fmt.Errorf("change-tokenization-filterable requires targetTokenization")
		}
		filterableGen, ok := genFor(MigrationDirPrefixFilterableRetokenize, "_"+propName)
		if !ok {
			return nil, nil
		}
		filterableTask := NewRuntimeFilterableRetokenizeTask(
			p.logger,
			propName, payload.TargetTokenization,
			payload.Collection, payload.Collection,
			filterableGen,
		)
		return []*ShardReindexTaskGeneric{filterableTask}, nil

	default:
		return nil, fmt.Errorf("unknown migration type %q", payload.MigrationType)
	}
}

// propsSuffix returns the "_p1_p2..." prop-names suffix that
// migrationDirWithProps appends after a strategy prefix. Returns "" for
// empty prop slices. Kept in sync with [migrationDirWithProps] — must
// produce the same suffix string the strategy's MigrationDirName() will
// emit, so [nextMigrationGeneration] / [maxMigrationGeneration] scan
// against the same target.
func propsSuffix(propNames []string) string {
	if len(propNames) == 0 {
		return ""
	}
	// migrationDirWithProps sorts the names; replicate that here so the
	// resulting suffix matches.
	sorted := make([]string, len(propNames))
	copy(sorted, propNames)
	sort.Strings(sorted)
	return "_" + strings.Join(sorted, "_")
}

// propertyHasFilterableBucket reports whether the named property carries
// a filterable inverted index according to the live schema. Used by
// [createReindexTasks] to decide whether ChangeTokenization's filterable
// sub-task should be created — submitting it for a filterable=false
// property would spawn a retokenize on a non-existent source bucket and
// fail the swap.
//
// A missing class or property is treated as "no filterable bucket"; the
// task creator returns the (possibly empty) set of remaining sub-tasks
// and the upstream call paths surface a clean error if the resulting
// list is empty.
func (p *ReindexProvider) propertyHasFilterableBucket(className, propName string) bool {
	if p.schemaManager == nil {
		// Defensive default: behave as the pre-change code would have —
		// dispatch both sub-tasks. The downstream swap failure mode is
		// the same as before the precheck was added.
		return true
	}
	cls := p.schemaManager.ReadOnlyClass(className)
	if cls == nil {
		return false
	}
	for _, prop := range cls.Properties {
		if prop.Name != propName {
			continue
		}
		// IndexFilterable nil → defaults to true (filterable index is on).
		// IndexFilterable *true → on.
		// IndexFilterable *false → off (no source bucket).
		return prop.IndexFilterable == nil || *prop.IndexFilterable
	}
	return false
}

// loadPayload returns the cached payload for a task descriptor, or
// unmarshals it from task.Payload if the cache is empty. The cache is
// populated by StartTask; it can be empty for OnGroupCompleted / etc.
// after a node restart that happened between reindex finishing and the
// group callback firing.
func (p *ReindexProvider) loadPayload(task *distributedtask.Task) (*ReindexTaskPayload, error) {
	if cached := p.cachedPayload(task.TaskDescriptor); cached != nil {
		return cached, nil
	}

	var pl ReindexTaskPayload
	if err := json.Unmarshal(task.Payload, &pl); err != nil {
		return nil, fmt.Errorf("unmarshal payload: %w", err)
	}
	return &pl, nil
}

// failUnit records that the given unit has failed. The recorder call
// goes through RAFT (RecordDistributedTaskUnitFailure → applyDistributedTaskCommand),
// so transient errors are possible: leadership loss, network blip, RAFT
// timeout. If the FSM never learns the unit failed, the task stays in
// "started" forever and the scheduler will not retry it on this node
// (it only re-schedules units that have a terminal status). The
// scheduler's task-level retry only fires when ALL local units are
// terminal — a single un-recorded failure can therefore wedge the task.
//
// Retry the recorder call a few times with backoff to ride out transient
// RAFT issues. If the retries also fail, log the recorder error with the
// full context and the original failure reason so operators can replay
// it manually (recording the failure is idempotent because the FSM keys
// by (taskID, version, nodeID, unitID)).
//
// Permanent FSM rejections — "task does not exist", "task is no longer
// running", "unit ... is already terminal", "unit ... belongs to node X
// not Y" — are NOT retried: the FSM is in a stable state where another
// retry will return the same error. We log them at warning level (not
// the loud "manual operator action required" alarm) because the FSM is
// internally consistent; the local node just lost a race.
func (p *ReindexProvider) failUnit(
	ctx context.Context,
	task *distributedtask.Task,
	unitID string,
	recorder distributedtask.TaskCompletionRecorder,
	errMsg string,
) {
	logger := p.logger.WithField("taskID", task.ID).WithField("unit", unitID)
	logger.Errorf("reindex provider: unit failed: %s", errMsg)

	const maxAttempts = 3
	backoff := 200 * time.Millisecond
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		recErr := recorder.RecordDistributedTaskUnitFailure(
			ctx, task.Namespace, task.ID, task.Version, p.localNode, unitID, errMsg,
		)
		if recErr == nil {
			return
		}
		lastErr = recErr

		// Permanent FSM rejection: retry won't help. Log at warning level
		// and exit — no operator action required, the FSM is in a stable
		// state.
		if isPermanentRecorderRejection(logger, recErr) {
			logger.WithField("originalFailure", errMsg).WithField("recorderError", recErr.Error()).
				Warn("reindex provider: FSM rejected unit-failure record as permanent (task already terminal, " +
					"version mismatch, or unit owned by another node); not retrying")
			return
		}

		if attempt < maxAttempts {
			select {
			case <-ctx.Done():
				logger.WithField("attempt", attempt).WithField("recorderError", recErr.Error()).
					Error("reindex provider: context cancelled while recording unit failure; FSM may not see the failure")
				return
			case <-time.After(backoff):
			}
			backoff *= 2
		}
	}

	// All retries exhausted. The FSM does not know this unit failed; the
	// scheduler will not advance the task. Operators need to inspect the
	// task (and likely abort it manually) using the recorded reason.
	logger.WithField("originalFailure", errMsg).
		WithField("recorderError", lastErr.Error()).
		Error("reindex provider: failed to record unit failure after retries; " +
			"FSM may not advance the task and manual operator action may be required")
}

// isPermanentRecorderRejection reports whether the recorder error
// describes a stable FSM state that retrying cannot fix.
//
// The primary check is errors.Is against
// [distributedtask.ErrPermanentRejection], which works for both local
// FSM errors (sentinel wrapped via fmt.Errorf("...: %w", ...)) and
// errors that have round-tripped through gRPC and been re-hydrated by
// [distributedtask.RehydratePermanentRejection] in
// applyDistributedTaskCommand.
//
// As a safety net during rolling upgrades, we keep a substring-matching
// fallback for the case where a pre-sentinel peer (running an older
// build that returns the plain-text error rather than the typed
// sentinel) is the leader and a newer follower receives its rejection
// over RPC. Whenever the fallback fires, we log at Warn level so
// operators can confirm in prod whether the sentinel path has fully
// rolled out.
//
// TODO(v1.40): remove the substring fallback below. The sentinel
// path ships in v1.38; once the supported rolling-upgrade window
// can no longer include a pre-sentinel binary (i.e. the v1.40
// cycle), the substring markers and the operator-Warn become dead
// code. The errors.Is check above is the steady-state.
//
// The unmarshal-error path in Manager.RecordUnitCompletion (a malformed
// SubCommand) is technically permanent too but is deliberately NOT
// matched: applyDistributedTaskCommand marshals the request itself, so
// a malformed SubCommand reaching the Manager is a same-binary bug
// rather than a recoverable FSM state — it should surface as the loud
// alarm, not the quiet warning.
func isPermanentRecorderRejection(logger logrus.FieldLogger, err error) bool {
	if err == nil {
		return false
	}

	// Preferred path: typed sentinel.
	if errors.Is(err, distributedtask.ErrPermanentRejection) {
		return true
	}

	// Pre-sentinel fallback for mixed-version clusters: substring match
	// against the wire phrasings used by older peers. If this fires, we
	// want a Warn log so an operator can see that the sentinel path
	// isn't (yet) fully rolled out in their cluster.
	msg := err.Error()
	// All four substrings come from cluster/distributedtask/manager.go:
	//   "task %s/%s/%d is no longer running"          → ErrTaskNotRunning
	//   "task %s/%s/%d does not exist"                → ErrTaskDoesNotExist
	//   "unit %s in task %s/%s/%d is already terminal" → ErrUnitAlreadyTerminal
	//   "unit %s in task %s/%s/%d belongs to node ..." → ErrUnitWrongNode
	for _, marker := range []string{
		"is no longer running",
		"does not exist",
		"is already terminal",
		"belongs to node",
	} {
		if strings.Contains(msg, marker) {
			if logger != nil {
				logger.WithField("matchedMarker", marker).WithField("recorderError", msg).
					Warn("reindex provider: permanent-rejection detected via legacy substring fallback; " +
						"a peer is likely running a pre-sentinel build")
			}
			return true
		}
	}
	return false
}

// reindexRecoveryRecord is the on-disk payload describing an in-flight
// reindex task. It lives in <shard>/lsm/.migrations/<dir>/payload.mig and
// is written by [ReindexProvider.persistRecoveryRecord] before the
// reindex iteration starts. At startup, [DiscoverInFlightReindexTasks]
// scans every shard's .migrations/ directory and decodes these records
// to reconstruct ShardReindexTaskGeneric instances that have the right
// strategy + tokenization + bucket-strategy, so [OnAfterLsmInit] can
// fire during shard load and re-register the double-write callbacks
// BEFORE any post-restart write reaches the shard.
//
// TaskID + TaskVersion are kept so OnGroupCompleted's cache
// (keyed by [distributedtask.TaskDescriptor]) can be pre-populated with
// the recovered instances, avoiding a second OnAfterLsmInit pass via the
// rehydrate path.
type reindexRecoveryRecord struct {
	TaskID      string             `json:"taskID"`
	TaskVersion uint64             `json:"taskVersion"`
	UnitID      string             `json:"unitID"`
	Payload     ReindexTaskPayload `json:"payload"`
}

// persistRecoveryRecord writes one recovery record per generated task
// into each task's migration directory. For semantic migrations
// (change-tokenization) there are two tasks per unit (searchable +
// filterable) and therefore two migration directories per shard; the
// same record is written into each.
//
// lsmPath must be the concrete shard's LSM directory
// (<data>/<index>/<shard>/lsm) — the migration sub-directory under
// <lsmPath>/.migrations/<dir>/ is what holds the per-strategy sentinels
// and the new payload.mig file.
func (p *ReindexProvider) persistRecoveryRecord(
	task *distributedtask.Task,
	payload *ReindexTaskPayload,
	unitID string,
	lsmPath string,
	tasks []*ShardReindexTaskGeneric,
) error {
	if lsmPath == "" {
		return fmt.Errorf("empty lsm path")
	}
	rec := reindexRecoveryRecord{
		TaskID:      task.ID,
		TaskVersion: task.Version,
		UnitID:      unitID,
		Payload:     *payload,
	}
	encoded, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal recovery record: %w", err)
	}
	for _, t := range tasks {
		if err := t.SaveRecoveryPayload(lsmPath, encoded); err != nil {
			return fmt.Errorf("save recovery payload for task %q: %w", t.Name(), err)
		}
	}
	return nil
}

// resolveUnitForPhase prepares the per-unit (shard, unitTasks) every phase
// callback needs, rehydrating from disk on cache miss. Returns a
// [phaseUnitResolution] capturing both the success path (Shard + UnitTasks)
// and the three not-proceeding paths (Skip, setup-Errs, ContextCanceled).
func (p *ReindexProvider) resolveUnitForPhase(
	ctx context.Context,
	task *distributedtask.Task,
	payload *ReindexTaskPayload,
	unitID string,
	idx *Index,
	logger logrus.FieldLogger,
) phaseUnitResolution {
	unit := task.Units[unitID]
	if unit != nil && unit.Status == distributedtask.UnitStatusFailed {
		logger.WithField("unit", unitID).Warn("reindex provider: skipping failed unit")
		return phaseUnitResolution{Skip: true}
	}

	shardName := payload.UnitToShard[unitID]
	resolvedShard, err := lookupShardByName(idx, shardName)
	if err != nil {
		logger.WithField("unit", unitID).WithField("shard", shardName).
			Errorf("reindex provider: resolveUnitForPhase: shard lookup failed: %v", err)
		return phaseUnitResolution{
			Errs: []string{fmt.Sprintf("unit %s shard %s lookup: %v", unitID, shardName, err)},
		}
	}

	if cached := p.cachedReindexTasks(task.TaskDescriptor, unitID); len(cached) > 0 {
		return phaseUnitResolution{Shard: resolvedShard, UnitTasks: cached}
	}

	// Cache miss — instantiate from disk.
	concreteShard, unwrapErr := unwrapShard(ctx, resolvedShard)
	if unwrapErr != nil {
		logger.WithField("unit", unitID).
			Errorf("reindex provider: resolveUnitForPhase: unwrap shard for rehydrate: %v", unwrapErr)
		return phaseUnitResolution{
			Errs:               []string{fmt.Sprintf("unit %s unwrap shard: %v", unitID, unwrapErr)},
			SawContextCanceled: errors.Is(unwrapErr, context.Canceled),
		}
	}
	fresh, err := p.createReindexTasks(payload, concreteShard.pathLSM(), true)
	if err != nil {
		logger.WithField("unit", unitID).
			Errorf("reindex provider: resolveUnitForPhase: creating reindex tasks: %v", err)
		return phaseUnitResolution{
			Errs:               []string{fmt.Sprintf("unit %s create tasks: %v", unitID, err)},
			SawContextCanceled: errors.Is(err, context.Canceled),
		}
	}
	if len(fresh) == 0 {
		// Nothing on disk: prior FinalizeCompletedMigrations or end-of-swap
		// trim already cleaned this unit up. Phase callbacks have no work.
		logger.WithField("unit", unitID).
			Info("reindex provider: resolveUnitForPhase: no in-flight state on disk for this unit (post-restart of already-finalized migration); skipping")
		return phaseUnitResolution{Skip: true}
	}

	// Cache the rehydrated tasks so the SWAP callback's lookup hits after
	// the PreparationCompleteAck barrier (RAFT propagation can take minutes).
	p.cacheReindexTasks(task.TaskDescriptor, unitID, fresh)

	logger.WithField("unit", unitID).
		Info("reindex provider: resolveUnitForPhase: rebuilding tasks from disk (node likely restarted); cached for subsequent phase callbacks")
	return phaseUnitResolution{Shard: resolvedShard, UnitTasks: fresh, Rehydrate: true}
}

// runShardPrepPhase runs the disk-I/O PREP phase for one unit on one shard.
// Best-effort across tasks: one task failing doesn't abort the rest. The
// returned ok=true iff every task on this shard reached merged.mig; on
// false the caller MUST skip OVERLAY+SWAP.
func (p *ReindexProvider) runShardPrepPhase(
	ctx context.Context,
	unitID string,
	shard ShardLike,
	unitTasks []*ShardReindexTaskGeneric,
	rehydrate bool,
	logger logrus.FieldLogger,
) (ok bool, out phaseResult) {
	ok = true
	for _, reindexTask := range unitTasks {
		if rehydrate {
			if err := reindexTask.RunReindexOnlyOnShard(ctx, shard); err != nil {
				logger.WithField("unit", unitID).WithField("task", reindexTask.Name()).
					Errorf("reindex provider: shard prep — rehydrate failed; prep will not run for this task: %v", err)
				out.Errs = append(out.Errs, fmt.Sprintf("unit %s task %s rehydrate: %v", unitID, reindexTask.Name(), err))
				if errors.Is(err, context.Canceled) {
					out.SawContextCanceled = true
				}
				ok = false
				continue
			}
		}
		if err := reindexTask.RunPrepareOnShard(ctx, shard); err != nil {
			logger.WithField("unit", unitID).WithField("task", reindexTask.Name()).
				Errorf("reindex provider: shard prep — prep failed; swap will not run for this task: %v", err)
			out.Errs = append(out.Errs, fmt.Sprintf("unit %s task %s prepare: %v", unitID, reindexTask.Name(), err))
			if errors.Is(err, context.Canceled) {
				out.SawContextCanceled = true
			}
			ok = false
		}
	}
	return ok, out
}

// runShardSwapPhase. Partial success leaves the overlay set for the
// flipped props only; un-swapped buckets keep the old tokenization and
// need operator rebuild.
func (p *ReindexProvider) runShardSwapPhase(
	ctx context.Context,
	payload *ReindexTaskPayload,
	unitID string,
	shardName string,
	shard ShardLike,
	unitTasks []*ShardReindexTaskGeneric,
	logger logrus.FieldLogger,
) (out phaseResult) {
	allSwapped := true
	anySwapped := false

	// Wire a per-prop hook rather than setting the overlay once up front;
	// see [maybeWirePerPropOverlaySet] for why the latter is a correctness bug.
	setShard, setUnwrapErr := unwrapShard(ctx, shard)
	if setUnwrapErr != nil && IsTokenizationChangingMigration(payload.MigrationType) {
		logger.WithField("unit", unitID).WithField("shard", shardName).
			Warnf("reindex provider: cannot wire tokenization overlay — shard unwrap failed; queries during SWAPPING window may observe stale-tokenization results: %v", setUnwrapErr)
	}
	overlayWasSet := maybeWirePerPropOverlaySet(setShard, payload, unitTasks)

	for _, reindexTask := range unitTasks {
		if err := reindexTask.RunSwapOnShard(ctx, shard); err != nil {
			logger.WithField("unit", unitID).WithField("task", reindexTask.Name()).
				Errorf("reindex provider: shard swap — swap failed; migration is half-applied on this shard: %v", err)
			out.Errs = append(out.Errs, fmt.Sprintf("unit %s task %s swap: %v", unitID, reindexTask.Name(), err))
			if errors.Is(err, context.Canceled) {
				out.SawContextCanceled = true
			}
			allSwapped = false
		} else {
			anySwapped = true
		}
	}

	// All swaps failed: tear the overlay back down so the analyzer stops
	// claiming the new tokenization while buckets still hold old data.
	if maybeClearTokenizationOverlayOnAllFailed(setShard, payload, overlayWasSet, anySwapped) {
		logger.WithField("unit", unitID).WithField("shard", shardName).
			Debug("reindex provider: cleared tokenization overlay — every swap sub-task failed; no bucket pointer was flipped on this shard")
	}

	if allSwapped {
		logger.WithField("unit", unitID).WithField("shard", shardName).
			Info("reindex provider: swap complete")
	} else {
		logger.WithField("unit", unitID).WithField("shard", shardName).
			Error("reindex provider: swap INCOMPLETE for this shard — at least one task's RunSwapOnShard returned an error; the cluster-wide post-completion ack will report this as a failure")
	}
	return out
}

// runPerUnitPhase is the shared outer loop for OnGroupCompleted and
// OnSwapRequested. Iterates this node's units in the group, runs
// runPhase on each, and aggregates errors with context.Canceled wrap
// semantics that the scheduler's transient-vs-permanent ack routing
// depends on.
//
// When parallel=true the runPhase callbacks fire concurrently across
// units. Used by the SWAP path to keep the per-replica user-observable
// cutover window bounded by max(per-shard runtimeSwap) rather than
// Σ(per-shard runtimeSwap) — without parallelism, each shard's inline
// post-pointer-flip work (oldBucket.Shutdown drain, dir rename, trim)
// serializes the NEXT shard's pointer flip, which makes the partial-
// results window grow linearly in shard count and is the regression
// surfaced by TestLiveQueriesDuringChangeTokenization on container
// disk. Per-shard state in runPhase is structurally disjoint:
//   - separate per-shard LSM store / bucket pointers (Shard.store).
//   - separate per-shard sentinel tracker (.migrations/<dir>/*.mig).
//   - separate per-shard tokenization overlay (Shard.TokenizationFor).
//   - separate ShardReindexTaskGeneric instance per (task, unit) with
//     its own callbackDisableFuncs guarded by callbackDisableFuncsMu.
//
// Provider-level shared state (p.payloads, p.runningHandles,
// p.reindexTasks) is already mutex-protected via p.mu in
// resolveUnitForPhase and its peers, so concurrent calls are safe.
//
// When parallel=false the loop is sequential (legacy behavior). Used
// by the PREP path where heavy IO (FlushAndSwitch, ShutdownBucket,
// PrependSegmentsFromBucket) per shard would compound under
// parallelism — and where the latency doesn't affect the user-
// observable query-consistency window because queries during PREP
// still see the OLD tokenization.
func (p *ReindexProvider) runPerUnitPhase(
	task *distributedtask.Task,
	payload *ReindexTaskPayload,
	localGroupUnitIDs []string,
	idx *Index,
	logger logrus.FieldLogger,
	callbackName string,
	parallel bool,
	runPhase func(unitID string, shard ShardLike, unitTasks []*ShardReindexTaskGeneric, rehydrate bool) phaseResult,
) error {
	ctx := p.serverCtx
	var agg phaseResult
	var aggMu sync.Mutex

	runOne := func(unitID string) {
		res := p.resolveUnitForPhase(ctx, task, payload, unitID, idx, logger)
		if res.Skip {
			return
		}
		if len(res.Errs) > 0 {
			func() {
				aggMu.Lock()
				defer aggMu.Unlock()
				agg.Errs = append(agg.Errs, res.Errs...)
				if res.SawContextCanceled {
					agg.SawContextCanceled = true
				}
			}()
			return
		}

		phase := runPhase(unitID, res.Shard, res.UnitTasks, res.Rehydrate)
		func() {
			aggMu.Lock()
			defer aggMu.Unlock()
			if len(phase.Errs) > 0 {
				agg.Errs = append(agg.Errs, phase.Errs...)
			}
			if phase.SawContextCanceled {
				agg.SawContextCanceled = true
			}
		}()
	}

	if parallel {
		var wg sync.WaitGroup
		for _, unitID := range localGroupUnitIDs {
			unitID := unitID
			wg.Add(1)
			enterrors.GoWrapper(func() {
				defer wg.Done()
				runOne(unitID)
			}, logger)
		}
		wg.Wait()
	} else {
		for _, unitID := range localGroupUnitIDs {
			runOne(unitID)
		}
	}

	if len(agg.Errs) == 0 {
		return nil
	}
	// %w-wrap context.Canceled so the scheduler's errors.Is check routes
	// shutdown-induced failures to the transient (recovery) branch instead
	// of acking a permanent failure that would flip the task to FAILED.
	if agg.SawContextCanceled {
		return fmt.Errorf("%s: %d unit(s) failed: %s: %w",
			callbackName, len(agg.Errs), strings.Join(agg.Errs, "; "), context.Canceled)
	}
	return fmt.Errorf("%s: %d unit(s) failed: %s",
		callbackName, len(agg.Errs), strings.Join(agg.Errs, "; "))
}

// OnGroupCompleted fires after all units in a group reach terminal state.
// For NeedsPreparationBarrier=true tasks it runs PREP only; OVERLAY+SWAP happen in
// OnSwapRequested once the cluster-wide PreparationCompleteAck barrier clears.
// For NeedsPreparationBarrier=false it runs PREP+OVERLAY+SWAP inline.
//
// Any per-node failure here flips the task to FAILED via the appropriate
// ack, which guarantees no cluster-wide schema flip commits while buckets
// remain un-swapped.
func (p *ReindexProvider) OnGroupCompleted(task *distributedtask.Task, groupID string, localGroupUnitIDs []string) error {
	logger := p.logger.WithField("taskID", task.ID).WithField("groupID", groupID).
		WithField("localGroupUnitIDs", localGroupUnitIDs)

	// Recovery-replay short-circuit. The scheduler can invoke
	// OnGroupCompleted for a task whose terminal state was reached in a
	// prior process lifetime — FINISHED, FAILED, or CANCELLED tasks
	// rehydrated during startup recovery, or replayed when a node rejoins
	// the cluster with a stale RAFT log. For semantic migrations the swap
	// dirs are long gone by then (markTidied + the per-shard
	// trimOlderGenerations call removed them), so any attempt to re-run
	// runtimeSwap would error with "reindex bucket %q not found" — noise
	// only since the ack barrier in [Manager.RecordPostCompletionAck]
	// drops acks on terminal tasks (correctness unaffected), but every
	// operator restart spams an ERROR log entry per local unit that
	// looks like a real problem.
	//
	// Returning nil here mirrors the format-only-migration short-circuit
	// below: no-op for a request the system is not in a position to act
	// on.
	if task.Status.IsTerminal() {
		logger.WithField("status", task.Status).
			Debug("reindex provider: group-completion: skipping replay on past-terminal task")
		return nil
	}

	payload, err := p.loadPayload(task)
	if err != nil {
		logger.Errorf("reindex provider: group-completion: failed to load payload: %v", err)
		return fmt.Errorf("load payload: %w", err)
	}

	if !IsSemanticMigration(payload.MigrationType) {
		logger.Info("reindex provider: group-completion (format-only, no-op)")
		return nil
	}

	if task.NeedsPreparationBarrier {
		logger.Info("reindex provider: group-completion → starting PREP phase (barrier mode)")
	} else {
		logger.Info("reindex provider: group-completion → starting swap phase (no-barrier path)")
	}

	className := entschema.ClassName(payload.Collection)
	idx := p.db.GetIndex(className)
	if idx == nil {
		logger.Error("reindex provider: group-completion — collection not found")
		return fmt.Errorf("collection %q not found on this node", payload.Collection)
	}

	// Atomic-phase contract (full picture, see file-level godoc on
	// inverted_reindex_task_generic.go for the per-strategy detail):
	//
	//   1. PREP (RunPrepareOnShard, per task) — disk-I/O-heavy work:
	//      FlushAndSwitch reindex bucket, ShutdownBucket, Prepend.
	//   2. OVERLAY WIRING — maybeWirePerPropOverlaySet. Installs the
	//      per-prop onPropSwapped hook on each task so the overlay is
	//      SET atomically with that prop's bucket-pointer flip in
	//      phase 3.
	//   3. ATOMIC SWAP (RunSwapOnShard, per task) — in-memory
	//      bucket-pointer flip + per-prop sentinel fsync + per-prop
	//      overlay set, all in the Phase 2a tight loop. The disk
	//      dirs aren't renamed here; that's deferred to next startup
	//      via OnBeforeLsmInit's recoverRuntimeSwapBuckets path.
	//
	// Under barrier=false, all three phases run inside this single
	// OnGroupCompleted callback on each node. Under barrier=true,
	// phase 1 runs here; phases 2-3 run in OnSwapRequested after the
	// cluster-wide PreparationCompleteAck barrier transitions PREPARING to
	// SWAPPING. The split bounds the cross-replica stagger window at
	// billion-scale to RAFT propagation latency rather than per-node
	// PREP duration.
	ctx := p.serverCtx
	// PREP path runs heavy IO per shard (FlushAndSwitch, ShutdownBucket,
	// PrependSegmentsFromBucket). Sequential to avoid compounding IO
	// contention; query consistency is not at stake here because queries
	// during PREP still see OLD tokenization.
	return p.runPerUnitPhase(task, payload, localGroupUnitIDs, idx, logger,
		"group-completion", false,
		func(unitID string, shard ShardLike, unitTasks []*ShardReindexTaskGeneric, rehydrate bool) phaseResult {
			return p.onGroupCompletedRunPhaseForUnit(ctx, task, payload, unitID, shard, unitTasks, rehydrate, logger)
		})
}

// onGroupCompletedRunPhaseForUnit is the per-unit callback driven by
// runPerUnitPhase for OnGroupCompleted. Encapsulates the
// barrier-vs-non-barrier dispatch. PREP always runs (idempotent at merged.mig);
// OVERLAY+SWAP run inline only when NeedsPreparationBarrier=false and PREP succeeded.
func (p *ReindexProvider) onGroupCompletedRunPhaseForUnit(
	ctx context.Context,
	task *distributedtask.Task,
	payload *ReindexTaskPayload,
	unitID string,
	shard ShardLike,
	unitTasks []*ShardReindexTaskGeneric,
	rehydrate bool,
	logger logrus.FieldLogger,
) (out phaseResult) {
	prepOK, prep := p.runShardPrepPhase(ctx, unitID, shard, unitTasks, rehydrate, logger)
	out.Errs = append(out.Errs, prep.Errs...)
	if prep.SawContextCanceled {
		out.SawContextCanceled = true
	}

	if task.NeedsPreparationBarrier {
		return out
	}

	if !prepOK {
		shardName := payload.UnitToShard[unitID]
		logger.WithField("unit", unitID).WithField("shard", shardName).
			Warn("reindex provider: prep phase incomplete; skipping overlay set + atomic swap for this shard")
		logger.WithField("unit", unitID).WithField("shard", shardName).
			Error("reindex provider: swap INCOMPLETE for this shard — prep phase failed; the cluster-wide post-completion ack will report this as a failure")
		return out
	}

	shardName := payload.UnitToShard[unitID]
	swap := p.runShardSwapPhase(ctx, payload, unitID, shardName, shard, unitTasks, logger)
	out.Errs = append(out.Errs, swap.Errs...)
	if swap.SawContextCanceled {
		out.SawContextCanceled = true
	}
	return out
}

// OnSwapRequested runs OVERLAY+SWAP for NeedsPreparationBarrier=true tasks. The
// scheduler only fires this after the cluster-wide PREPARING → SWAPPING
// transition. Returns non-nil on any per-shard swap failure; the resulting
// PostCompletionAck flip-to-FAILED prevents the cluster-wide schema flip.
func (p *ReindexProvider) OnSwapRequested(task *distributedtask.Task, groupID string, localGroupUnitIDs []string) error {
	logger := p.logger.WithField("taskID", task.ID).WithField("groupID", groupID).
		WithField("localGroupUnitIDs", localGroupUnitIDs)

	if task.Status.IsTerminal() {
		logger.WithField("status", task.Status).
			Debug("reindex provider: swap-requested: skipping replay on past-terminal task")
		return nil
	}

	payload, err := p.loadPayload(task)
	if err != nil {
		logger.Errorf("reindex provider: swap-requested — failed to load payload: %v", err)
		return fmt.Errorf("load payload: %w", err)
	}

	if !IsSemanticMigration(payload.MigrationType) {
		// Defensive: format-only migrations shouldn't carry NeedsPreparationBarrier.
		logger.Warn("reindex provider: swap-requested for non-semantic migration (NeedsPreparationBarrier inconsistency); no-op")
		return nil
	}

	logger.Info("reindex provider: swap-requested → starting OVERLAY+SWAP after cluster-wide PREP barrier")

	className := entschema.ClassName(payload.Collection)
	idx := p.db.GetIndex(className)
	if idx == nil {
		logger.Error("reindex provider: swap-requested — collection not found")
		return fmt.Errorf("collection %q not found on this node", payload.Collection)
	}

	ctx := p.serverCtx
	// SWAP path runs the in-memory pointer flip first (the user-observable
	// event) and then per-shard post-flip work (Shutdown drain, dir
	// rename, sentinel writes, trim). Parallel across this node's units
	// so the post-flip work on shard A does NOT serialize the pointer
	// flip on shard B — without this the per-replica cutover window
	// grows linearly in shard count. Per-shard state is structurally
	// disjoint (see runPerUnitPhase godoc).
	return p.runPerUnitPhase(task, payload, localGroupUnitIDs, idx, logger,
		"swap-requested", true,
		func(unitID string, shard ShardLike, unitTasks []*ShardReindexTaskGeneric, rehydrate bool) phaseResult {
			return p.onSwapRequestedRunPhaseForUnit(ctx, payload, unitID, shard, unitTasks, rehydrate, logger)
		})
}

// onSwapRequestedRunPhaseForUnit runs OVERLAY+SWAP. On rehydrate (cache
// miss after restart), it first re-runs PREP — idempotent at merged.mig —
// so OnAfterLsmInit registers double-write callbacks before SWAP.
func (p *ReindexProvider) onSwapRequestedRunPhaseForUnit(
	ctx context.Context,
	payload *ReindexTaskPayload,
	unitID string,
	shard ShardLike,
	unitTasks []*ShardReindexTaskGeneric,
	rehydrate bool,
	logger logrus.FieldLogger,
) (out phaseResult) {
	if rehydrate {
		prepOK, prep := p.runShardPrepPhase(ctx, unitID, shard, unitTasks, rehydrate, logger)
		out.Errs = append(out.Errs, prep.Errs...)
		if prep.SawContextCanceled {
			out.SawContextCanceled = true
		}
		if !prepOK {
			shardName := payload.UnitToShard[unitID]
			logger.WithField("unit", unitID).WithField("shard", shardName).
				Warn("reindex provider: swap-requested: rehydrate prep-sentinel check failed; skipping overlay+swap for this shard")
			return out
		}
	}

	shardName := payload.UnitToShard[unitID]
	swap := p.runShardSwapPhase(ctx, payload, unitID, shardName, shard, unitTasks, logger)
	out.Errs = append(out.Errs, swap.Errs...)
	if swap.SawContextCanceled {
		out.SawContextCanceled = true
	}
	return out
}

// OnTaskCompleted is the cluster-wide cutover for semantic migrations:
// every node's local SWAP has already committed, so the RAFT-idempotent
// schema flip propagates the new tokenization within apply latency.
// Skips the flip on non-SWAPPING terminal states (FAILED / CANCELLED) so
// the schema remains pre-migration when the cluster-wide migration didn't
// succeed.
func (p *ReindexProvider) OnTaskCompleted(task *distributedtask.Task) {
	// Clear caches up-front so a failed-task early return doesn't leak.
	payload, payloadErr := p.loadPayload(task)
	p.clearTaskCaches(task.TaskDescriptor)

	logger := p.logger.WithField("taskID", task.ID).WithField("status", task.Status)
	logger.Info("reindex provider: task-completion")

	if task.Status != distributedtask.TaskStatusSwapping {
		// Non-SWAPPING terminal/in-flight: no cluster-wide schema flip.
		// FAILED/CANCELLED auto-clean partial sidecar state on every node;
		// FAILED additionally logs operator repair guidance.
		if payloadErr == nil {
			switch task.Status {
			case distributedtask.TaskStatusFailed:
				logOperatorRepairGuidanceOnFailedSemanticMigration(logger, payload)
				p.autoCleanupAfterTerminal(task, payload, logger)
			case distributedtask.TaskStatusCancelled:
				p.autoCleanupAfterTerminal(task, payload, logger)
			case distributedtask.TaskStatusStarted,
				distributedtask.TaskStatusPreparing,
				distributedtask.TaskStatusSwapping,
				distributedtask.TaskStatusFinished:
				// SWAPPING handled below; STARTED/PREPARING never reach
				// OnTaskCompleted; FINISHED tidies via the swap pipeline.
			}
		}
		return
	}
	if payloadErr != nil {
		logger.Errorf("reindex provider: task-completion: failed to load payload; schema flip will not run: %v", payloadErr)
		return
	}
	if !IsSemanticMigration(payload.MigrationType) {
		// Format-only migrations flip their metadata inside RunSwapOnShard.
		return
	}

	// p.serverCtx outlives the per-task ctx (which is gone by the time the
	// scheduler tick fires OnTaskCompleted).
	ctx := p.serverCtx
	if err := p.flipSemanticMigrationSchema(ctx, payload, logger); err != nil {
		logger.Errorf("reindex provider: task-completion: schema flip failed; migration result is half-applied (bucket swapped on every node, schema still reflects pre-migration state): %v", err)
		// Leave the overlay in place: buckets are NEW-tokenized but the
		// schema is still pre-flip on this node — the overlay keeps queries
		// aligned until either a retry lands or TokenizationFor self-clears.
		return
	}

	if IsTokenizationChangingMigration(payload.MigrationType) {
		className := entschema.ClassName(payload.Collection)
		if idx := p.db.GetIndex(className); idx != nil {
			idx.ForEachShard(func(shardName string, sh ShardLike) error {
				// Unwrap so the clear reaches the concrete shard whose
				// overlay the set hook populated. On unwrap failure,
				// TokenizationFor self-clears on the next query.
				concreteShard, err := unwrapShard(ctx, sh)
				if err != nil {
					logger.WithField("shard", shardName).
						Warnf("reindex provider: tokenization overlay clear skipped (unwrap failed); relying on TokenizationFor self-clear: %v", err)
					return nil
				}
				for _, propName := range payload.Properties {
					concreteShard.ClearTokenizationOverlay(propName)
				}
				return nil
			})
		}
	}
}

// autoCleanupAfterTerminal runs on every node when a semantic migration
// reaches FAILED or CANCELLED. Drains any still-running local
// goroutine, then wipes partial sidecar state per (property, indexType).
// Errors are logged and swallowed; the next-restart audit catches anything
// missed.
//
// Backup-gate race avoidance: a backup landing AFTER the FSM has flipped
// to FAILED/CANCELLED but BEFORE this routine finishes its sidecar
// teardown sees [IsLiveReindexTaskStatus]==false but the on-disk
// __reindex / __ingest sidecars are still being torn out. Registering
// every shard the task touched in [cleanupInProgress] before
// CleanStalePartialReindexState fires (and unregistering after) makes
// "cleanup is still happening on this shard" an explicit state the
// gate consults — closing the cleanup-vs-status-visibility gap the
// DTM-only lookup leaves open.
func (p *ReindexProvider) autoCleanupAfterTerminal(task *distributedtask.Task, payload *ReindexTaskPayload, logger logrus.FieldLogger) {
	drainCtx, drainCancel := context.WithTimeout(p.serverCtx, reindexTerminalCleanupDrainTimeout)
	defer drainCancel()
	if err := p.WaitForLocalTaskDrain(drainCtx, task.TaskDescriptor); err != nil {
		logger.Warnf("auto-cleanup after terminal status: drain did not finish in %s; skipping cleanup: %v", reindexTerminalCleanupDrainTimeout, err)
		return
	}
	indexTypes := semanticMigrationIndexTypesForAudit(payload.MigrationType)
	if len(indexTypes) == 0 || len(payload.Properties) == 0 {
		return
	}
	// Register every shard the task touched as "cleanup in progress"
	// for the duration of the per-(property, indexType) teardown loop.
	// The unregister fires from the defer so any return path — including
	// a panic inside CleanStalePartialReindexState — releases the slot.
	shards := uniqueShardsFromPayload(payload)
	for _, shardName := range shards {
		p.registerCleanup(payload.Collection, shardName)
	}
	defer func() {
		for _, shardName := range shards {
			p.unregisterCleanup(payload.Collection, shardName)
		}
	}()
	cleanupCtx, cancel := context.WithTimeout(p.serverCtx, reindexTerminalCleanupTimeout)
	defer cancel()
	for _, propName := range payload.Properties {
		for _, indexType := range indexTypes {
			if err := p.db.CleanStalePartialReindexState(cleanupCtx, payload.Collection, propName, indexType); err != nil {
				logger.WithField("property", propName).WithField("index_type", indexType).
					Warnf("auto-cleanup after terminal status failed: %v", err)
			}
		}
	}
	logger.Info("auto-cleanup after terminal status: partial sidecar state cleared on this node")
}

// uniqueShardsFromPayload returns the distinct shard names referenced
// in payload.UnitToShard. Used by [autoCleanupAfterTerminal] to register
// each shard exactly once in [cleanupInProgress] — multiple units can
// map to the same shard for multi-property migrations.
func uniqueShardsFromPayload(payload *ReindexTaskPayload) []string {
	if len(payload.UnitToShard) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(payload.UnitToShard))
	out := make([]string, 0, len(payload.UnitToShard))
	for _, shardName := range payload.UnitToShard {
		if shardName == "" {
			continue
		}
		if _, ok := seen[shardName]; ok {
			continue
		}
		seen[shardName] = struct{}{}
		out = append(out, shardName)
	}
	return out
}

// registerCleanup marks (collection, shard) as having an in-flight
// terminal-task cleanup. Refcounted: paired calls to
// [unregisterCleanup] release the slot, with the entry dropping out of
// the map once the count returns to zero. Safe to call concurrently
// from multiple terminal-state transitions (different tasks, different
// (property, indexType) tuples) that share a shard.
func (p *ReindexProvider) registerCleanup(collection, shard string) {
	p.cleanupInProgressMu.Lock()
	defer p.cleanupInProgressMu.Unlock()
	p.cleanupInProgress[reindexCleanupKey{collection: collection, shard: shard}]++
}

// unregisterCleanup releases one outstanding "cleanup-in-progress"
// registration on (collection, shard). When the refcount returns to
// zero the map entry is removed so [IsCleanupInProgress] reports false
// for that tuple and the registry doesn't grow unbounded across
// task lifetimes.
//
// Calling unregisterCleanup without a matching registerCleanup is a
// programming error and would underflow the count; the [autoCleanup
// AfterTerminal] defer pairs every register with one unregister via
// the same shard slice so this cannot happen in practice.
func (p *ReindexProvider) unregisterCleanup(collection, shard string) {
	p.cleanupInProgressMu.Lock()
	defer p.cleanupInProgressMu.Unlock()
	k := reindexCleanupKey{collection: collection, shard: shard}
	p.cleanupInProgress[k]--
	if p.cleanupInProgress[k] <= 0 {
		delete(p.cleanupInProgress, k)
	}
}

// IsCleanupInProgress reports whether [autoCleanupAfterTerminal] is
// currently tearing partial sidecar state on (collection, shard).
//
// Backup gate consumer: the cluster-wide [DB.AnyLiveReindexForShard]
// answer must include this signal — the DTM activity lookup it wraps
// flips a task to terminal as soon as the FSM lands, but the
// node-local sidecar buckets are still being shut down for tens of
// seconds after that. A backup that snapshots the shard in that gap
// would capture half-removed __reindex / __ingest dirs.
//
// Wiring: install [CleanupInProgressLookupBuilder] (returns a closure
// over this method) on the DB alongside [DB.SetShardReindexActivity
// Lookup] so [DB.AnyLiveReindexForShard] consults both. Returns false
// if the registry is nil (test fixtures that construct the provider
// without going through [NewReindexProvider]).
func (p *ReindexProvider) IsCleanupInProgress(collection, shard string) bool {
	p.cleanupInProgressMu.RLock()
	defer p.cleanupInProgressMu.RUnlock()
	if p.cleanupInProgress == nil {
		return false
	}
	return p.cleanupInProgress[reindexCleanupKey{collection: collection, shard: shard}] > 0
}

// CleanupInProgressLookup is the per-(collection, shard) "is the
// terminal-task cleanup goroutine still inside its
// CleanStalePartialReindexState loop?" probe. Sibling type to
// [ShardReindexActivityLookup] (which is the cluster-wide DTM-backed
// "is there a LIVE reindex task on this shard?" probe). The backup
// gate OR-s them: a shard is busy if EITHER a DTM task is live OR a
// terminal-cleanup is still running.
type CleanupInProgressLookup func(collection, shard string) bool

// CleanupInProgressLookupBuilder returns a fresh snapshot. Mirrors the
// builder pattern used by [ShardReindexActivityLookupBuilder] so the
// wiring in configure_api.go can install both lookups identically.
type CleanupInProgressLookupBuilder func() CleanupInProgressLookup

// CleanupInProgressLookupBuilder returns a builder whose closures
// re-read the live [cleanupInProgress] registry on every invocation.
// Use to wire the backup gate into the provider without coupling the
// DB struct to the concrete *ReindexProvider type.
//
// Returning the closure (rather than a direct method handle) keeps
// the contract symmetric with [ShardReindexActivityLookupBuilder] and
// lets the gate take a snapshot per probe rather than caching the
// underlying state.
func (p *ReindexProvider) CleanupInProgressLookupBuilder() CleanupInProgressLookupBuilder {
	return func() CleanupInProgressLookup {
		return p.IsCleanupInProgress
	}
}

// reindexTerminalCleanupDrainTimeout matches reindexCancelDrainTimeout
// in the REST handlers so both cancel paths converge on identical
// stuck-task behavior.
const reindexTerminalCleanupDrainTimeout = 10 * time.Second

// reindexTerminalCleanupTimeout bounds cleanup per shard across all
// (property, indexType) pairs.
const reindexTerminalCleanupTimeout = 60 * time.Second

// IsLiveReindexTaskStatus reports whether a task in the given DTM status
// still owns its on-disk tracker dirs.
func IsLiveReindexTaskStatus(status distributedtask.TaskStatus) bool {
	switch status {
	case distributedtask.TaskStatusStarted,
		distributedtask.TaskStatusPreparing,
		distributedtask.TaskStatusSwapping:
		return true
	case distributedtask.TaskStatusFinished,
		distributedtask.TaskStatusCancelled,
		distributedtask.TaskStatusFailed:
		return false
	}
	return false
}

// logOperatorRepairGuidanceOnFailedSemanticMigration logs the exact REST
// command an operator should issue to recover from a FAILED semantic
// migration. The failure mode it targets: sub-tasks that swapped BEFORE
// the failed sibling left their bucket NEW-tokenized while the cluster-
// wide schema flip was correctly skipped — every query against the
// affected inverted index returns 0 until the index is rebuilt against
// the current schema.
func logOperatorRepairGuidanceOnFailedSemanticMigration(logger logrus.FieldLogger, payload *ReindexTaskPayload) {
	if !IsSemanticMigration(payload.MigrationType) {
		return
	}
	if len(payload.Properties) == 0 {
		// Reserved for a future whole-collection rebuild. No targeted
		// guidance possible; the generic operator runbook applies.
		logger.Errorf(
			"reindex provider: %s on %s FAILED with empty Properties; manual repair guidance not available — inspect /v1/tasks and consider rebuild on every affected inverted index",
			payload.MigrationType, payload.Collection)
		return
	}
	for _, propName := range payload.Properties {
		// The repair body rebuilds every index the migration could have
		// torn — we can't tell from here which sub-task failed, and
		// rebuild is idempotent on a healthy index.
		var repairBody string
		switch payload.MigrationType {
		case ReindexTypeChangeTokenization,
			ReindexTypeEnableSearchable,
			ReindexTypeChangeAlgorithm,
			ReindexTypeRebuildSearchable:
			repairBody = `{"filterable":{"rebuild":true},"searchable":{"rebuild":true}}`
		case ReindexTypeChangeTokenizationFilterable,
			ReindexTypeEnableFilterable,
			ReindexTypeRepairFilterable:
			repairBody = `{"filterable":{"rebuild":true}}`
		case ReindexTypeEnableRangeable, ReindexTypeRepairRangeable:
			repairBody = `{"rangeable":{"rebuild":true}}`
		default:
			// Fallback for any future migration type: rebuild everything.
			repairBody = `{"filterable":{"rebuild":true},"searchable":{"rebuild":true},"rangeable":{"rebuild":true}}`
		}
		logger.WithFields(map[string]any{
			"property":       propName,
			"migration_type": payload.MigrationType,
			"repair_command": fmt.Sprintf(
				"PUT /v1/schema/%s/indexes/%s %s",
				payload.Collection, propName, repairBody),
		}).Errorf(
			"reindex provider: %s on %s.%s FAILED; per-shard sub-tasks "+
				"that committed their swap BEFORE the failure left the "+
				"canonical inverted bucket holding new-tokenization "+
				"data while the schema reverted to pre-migration state "+
				"— issue the repair_command above to rebuild the "+
				"affected inverted index(es) from raw objects against "+
				"the current schema",
			payload.MigrationType, payload.Collection, propName)
	}
}

// LocalCallbacksDone implements [distributedtask.RecoveryAwareProvider].
// Returns false iff at least one tracker dir on this node is started but
// neither tidied nor merged — the signature of a swap interrupted mid-flight.
// Returning false makes the scheduler bootstrap re-fire OnGroupCompleted so
// the rehydrate path completes the swap; without it, a half-applied local
// swap could leave this node at OLD tokenization after a cluster-wide
// schema flip already committed (#10675 family).
func (p *ReindexProvider) LocalCallbacksDone(task *distributedtask.Task, localNode string) bool {
	var payload ReindexTaskPayload
	if err := json.Unmarshal(task.Payload, &payload); err != nil {
		return true
	}
	if !IsSemanticMigration(payload.MigrationType) {
		return true
	}
	if p.db == nil {
		return true
	}
	idx := p.db.GetIndex(entschema.ClassName(payload.Collection))
	if idx == nil {
		return true
	}
	indexTypes := semanticMigrationIndexTypes(payload.MigrationType)
	if len(indexTypes) == 0 {
		return true
	}

	for unitID, nodeName := range payload.UnitToNode {
		if nodeName != localNode {
			continue
		}
		shardName := payload.UnitToShard[unitID]
		shard, err := lookupShardByName(idx, shardName)
		if err != nil {
			continue
		}
		concrete, err := unwrapShard(context.Background(), shard)
		if err != nil {
			continue
		}
		lsmPath := concrete.pathLSM()
		// ChangeAlgorithm uses a class-level tracker dir; per-property
		// migrationDirsForPropertyIndex deliberately omits it.
		if payload.MigrationType == ReindexTypeChangeAlgorithm &&
			hasUntidiedTracker(lsmPath, []string{MigrationDirSearchableMapToBlockmax}) {
			return false
		}
		for _, indexType := range indexTypes {
			for _, propName := range payload.Properties {
				prefixes := migrationDirsForPropertyIndex(propName, indexType)
				if hasUntidiedTracker(lsmPath, prefixes) {
					return false
				}
			}
		}
	}
	return true
}

// semanticMigrationIndexTypes returns the inverted-index discriminators
// each semantic migration type writes per-property tracker dirs for.
// Format-only migrations don't appear here because LocalCallbacksDone
// short-circuits on !IsSemanticMigration before calling this.
func semanticMigrationIndexTypes(mt ReindexMigrationType) []string {
	switch mt {
	case ReindexTypeChangeTokenization:
		return []string{"searchable", "filterable"}
	case ReindexTypeChangeTokenizationFilterable:
		return []string{"filterable"}
	case ReindexTypeEnableSearchable:
		return []string{"searchable"}
	case ReindexTypeEnableFilterable:
		return []string{"filterable"}
	case ReindexTypeChangeAlgorithm:
		return []string{"searchable"}
	case ReindexTypeRebuildSearchable,
		ReindexTypeRepairFilterable,
		ReindexTypeEnableRangeable, ReindexTypeRepairRangeable:
		// Format-only migrations. Returning nil short-circuits
		// LocalCallbacksDone's recovery check — they don't go through
		// the swap barrier so there's nothing to recover at this layer.
		return nil
	}
	return nil
}

// hasUntidiedTracker returns true iff at least one of the named tracker
// prefixes has a generation directory on disk that has started.mig but
// neither tidied.mig nor merged.mig — the signature of a swap that
// began but did not commit. Trackers that have tidied/merged are NOT a
// recovery signal (they are completed migrations waiting for the next
// restart's FinalizeCompletedMigrations to promote them to canonical).
// A completely missing tracker dir is also NOT a recovery signal: a
// prior FinalizeCompletedMigrations already promoted-and-removed it.
func hasUntidiedTracker(lsmPath string, prefixes []string) bool {
	migsDir := filepath.Join(lsmPath, ".migrations")
	entries, err := os.ReadDir(migsDir)
	if err != nil {
		return false
	}
	prefixSet := map[string]bool{}
	for _, p := range prefixes {
		prefixSet[p] = true
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		base, _, ok := parseMigrationDirName(entry.Name())
		if !ok || !prefixSet[base] {
			continue
		}
		dirPath := filepath.Join(migsDir, entry.Name())
		if fileExistsInDir(dirPath, "tidied.mig") || fileExistsInDir(dirPath, "merged.mig") {
			continue
		}
		// Tracker dir exists for this strategy but neither tidied.mig
		// nor merged.mig is present — local swap was interrupted.
		return true
	}
	return false
}

// flipSemanticMigrationSchema issues the cluster-wide RAFT update that
// completes a semantic migration. For change-tokenization the schema's
// Tokenization is set to the target; for enable-filterable the per-property
// IndexFilterable flag is set to true; for enable-searchable the
// IndexSearchable flag is set to true and Tokenization to the target.
//
// applyPerPropertySchemaUpdate is idempotent at the mutator level (returns
// apply=false when the value already matches) so multiple nodes firing
// OnTaskCompleted produce at most one RAFT commit.
func (p *ReindexProvider) flipSemanticMigrationSchema(
	ctx context.Context, payload *ReindexTaskPayload, logger logrus.FieldLogger,
) error {
	if p.schemaManager == nil {
		// Defensive: tests can construct the provider without a schema
		// manager; treat that as a no-op rather than a panic so those
		// tests keep working.
		return nil
	}

	switch payload.MigrationType {
	case ReindexTypeChangeTokenization, ReindexTypeChangeTokenizationFilterable:
		if payload.TargetTokenization == "" {
			return fmt.Errorf("change-tokenization without targetTokenization in payload")
		}
		missing, err := applyPerPropertySchemaUpdate(ctx, p.schemaManager, payload.Collection, payload.Properties,
			[]string{api.PropertyFieldTokenization},
			func(prop *models.Property) bool {
				if prop.Tokenization == payload.TargetTokenization {
					return false
				}
				prop.Tokenization = payload.TargetTokenization
				return true
			})
		if err != nil {
			return fmt.Errorf("flip tokenization: %w", err)
		}
		if len(missing) > 0 {
			// Single-property migration; a missing property between submit
			// and finalize is a hard error.
			return fmt.Errorf("property %v not found in class %q at finalize", missing, payload.Collection)
		}
		logger.WithField("tokenization", payload.TargetTokenization).
			Info("reindex provider: change-tokenization cutover committed")
		return nil

	case ReindexTypeEnableFilterable:
		trueVal := true
		_, err := applyPerPropertySchemaUpdate(ctx, p.schemaManager, payload.Collection, payload.Properties,
			[]string{api.PropertyFieldIndexFilterable},
			func(prop *models.Property) bool {
				if prop.IndexFilterable != nil && *prop.IndexFilterable {
					return false
				}
				prop.IndexFilterable = &trueVal
				return true
			})
		if err != nil {
			return fmt.Errorf("flip indexFilterable: %w", err)
		}
		// Missing properties are tolerated for multi-property enable-*:
		// a dropped property is the same outcome we'd want.
		logger.Info("reindex provider: enable-filterable cutover committed")
		return nil

	case ReindexTypeEnableSearchable:
		if payload.TargetTokenization == "" {
			return fmt.Errorf("enable-searchable without targetTokenization in payload")
		}
		trueVal := true
		_, err := applyPerPropertySchemaUpdate(ctx, p.schemaManager, payload.Collection, payload.Properties,
			[]string{api.PropertyFieldIndexSearchable, api.PropertyFieldTokenization},
			func(prop *models.Property) bool {
				if prop.IndexSearchable != nil && *prop.IndexSearchable && prop.Tokenization == payload.TargetTokenization {
					return false
				}
				prop.IndexSearchable = &trueVal
				prop.Tokenization = payload.TargetTokenization
				return true
			})
		if err != nil {
			return fmt.Errorf("flip indexSearchable+tokenization: %w", err)
		}
		logger.WithField("tokenization", payload.TargetTokenization).
			Info("reindex provider: enable-searchable cutover committed")
		return nil

	case ReindexTypeChangeAlgorithm:
		// Defer until every local searchable bucket is blockmax — submit is
		// per-property, so the class may still have map buckets.
		if defer_, err := p.shouldDeferBlockmaxFlip(ctx, payload, logger); err != nil {
			return err
		} else if defer_ {
			return nil
		}
		if err := updateToBlockMaxInvertedIndexConfig(ctx, p.schemaManager, payload.Collection); err != nil {
			return fmt.Errorf("flip UsingBlockMaxWAND: %w", err)
		}
		logger.Info("reindex provider: change-algorithm cutover committed")
		return nil

	default:
		// IsSemanticMigration above gates this; reaching here is a programming error.
		return fmt.Errorf("unexpected semantic migration type %q in task-completion", payload.MigrationType)
	}
}

// shouldDeferBlockmaxFlip is true while any local searchable bucket is still
// on the source (map) strategy — defers the cluster-wide flip until every
// per-property ChangeAlgorithm has drained (weaviate/0-weaviate-issues#254).
func (p *ReindexProvider) shouldDeferBlockmaxFlip(
	ctx context.Context, payload *ReindexTaskPayload, logger logrus.FieldLogger,
) (bool, error) {
	if p.db == nil {
		return false, nil
	}
	idx := p.db.GetIndex(entschema.ClassName(payload.Collection))
	if idx == nil {
		return false, fmt.Errorf("collection %q not found on this node", payload.Collection)
	}
	var stillMap bool
	idx.ForEachLoadedShard(func(_ string, sh ShardLike) error {
		concrete, err := unwrapShard(ctx, sh)
		if err != nil {
			return nil
		}
		for name, bucket := range concrete.Store().GetBucketsByName() {
			_, indexType := GetPropNameAndIndexTypeFromBucketName(name)
			if indexType != IndexTypePropSearchableValue {
				continue
			}
			if bucket.Strategy() == lsmkv.StrategyMapCollection {
				stillMap = true
				return nil
			}
		}
		return nil
	})
	if stillMap {
		logger.Info("reindex provider: change-algorithm cutover deferred — some searchable buckets still on map (subsequent per-property migration will complete the flip)")
		return true, nil
	}
	return false, nil
}

// IsSemanticMigration returns true for migration types that change query
// behavior and therefore require the cross-replica swap barrier + cluster-
// wide schema flip after every node has acknowledged. enable-rangeable is
// intentionally NOT semantic — predates the barrier family.
func IsSemanticMigration(mt ReindexMigrationType) bool {
	return mt == ReindexTypeChangeTokenization ||
		mt == ReindexTypeChangeTokenizationFilterable ||
		mt == ReindexTypeEnableFilterable ||
		mt == ReindexTypeEnableSearchable ||
		mt == ReindexTypeChangeAlgorithm
}

// IsTokenizationChangingMigration is true for migrations that flip a
// property's Tokenization at finalize, opening a SWAPPING-window
// misalignment between per-shard bucket flips and the cluster-wide
// schema flip — the per-shard tokenization overlay closes that gap.
func IsTokenizationChangingMigration(mt ReindexMigrationType) bool {
	return mt == ReindexTypeChangeTokenization ||
		mt == ReindexTypeChangeTokenizationFilterable
}

// maybeWirePerPropOverlaySet installs the per-prop onPropSwapped hook
// on every task of a tokenization-changing migration so the per-shard
// tokenization overlay is SET atomically with each property's
// bucket-pointer flip, inside the swap's Phase 2a tight loop. Returns
// true iff the hook was wired (i.e. this is a tokenization-changing
// migration with a non-empty target), so the caller can match
// [maybeClearTokenizationOverlayOnAllFailed]'s clear decision.
//
// Why per-prop, not once up front: RunSwapOnShard's disk-I/O preamble
// (MkdirAll, sentinel stats, prop read) runs between the loop start and
// the flip. Setting the overlay before the loop exposes overlay=NEW /
// bucket=OLD for that whole window, so a BM25 query returns a wrong
// count (0 for reverse field→word). Per-flip wiring collapses it to one
// map write; a swap that fails before any flip never sets it, keeping
// the all-failed path clean.
func maybeWirePerPropOverlaySet(shard *Shard, payload *ReindexTaskPayload, tasks []*ShardReindexTaskGeneric) bool {
	if shard == nil || payload == nil {
		return false
	}
	if !IsTokenizationChangingMigration(payload.MigrationType) {
		return false
	}
	if payload.TargetTokenization == "" {
		return false
	}
	target := payload.TargetTokenization
	for _, task := range tasks {
		if task == nil {
			continue
		}
		task := task
		// onPropSwapped covers the recovery/resume path; the live Phase-2a
		// loop uses swapPropAtomic (see the field docs on both).
		task.onPropSwapped = func(propName string) {
			shard.SetTokenizationOverlay(propName, target)
		}
		task.swapPropAtomic = func(ctx context.Context, store *lsmkv.Store,
			rt reindexTracker, propIdx int, propName string,
		) (*lsmkv.Bucket, error) {
			return shard.SwapBucketAndSetOverlay(propName, target,
				func() (*lsmkv.Bucket, error) {
					oldMainBucket, err := task.processOneSwapPropFn(ctx, store, rt, propIdx, propName)
					if err != nil {
						return nil, err
					}
					if task.afterFlipBeforeOverlayHook != nil {
						task.afterFlipBeforeOverlayHook()
					}
					return oldMainBucket, nil
				})
		}
	}
	return true
}

// maybeClearTokenizationOverlayOnAllFailed is the defensive CLEAR
// hook — called by [OnGroupCompleted] AFTER the per-task swap loop
// on a shard. It clears the per-shard tokenization overlay iff (a)
// the per-prop overlay hook was wired by
// [maybeWirePerPropOverlaySet] (the `wasSet` argument) AND
// (b) every per-task swap failed before flipping its bucket pointer
// (the `anySwapped` argument is false).
//
// Idempotent backstop: with per-prop wiring a fully-failed swap never
// sets the overlay. It still matters if a flip succeeded but the
// migration then went FAILED, since the skipped cluster-wide schema flip
// means nothing else would ever clear the overlay.
//
// Partial success (≥ 1 per-task swap returned nil → ≥ 1 bucket
// pointer flipped) is intentionally left intact: the overlay aligns
// with the swapped index type's content, which is strictly better
// than letting partially-flipped buckets misroute against the OLD
// schema tokenization. The partial-success case surfaces through the
// FAILED-task repair_command log line in
// [logOperatorRepairGuidanceOnFailedSemanticMigration].
//
// Returns true iff the clear was actually applied (for tests +
// observability).
func maybeClearTokenizationOverlayOnAllFailed(
	shard *Shard, payload *ReindexTaskPayload, wasSet, anySwapped bool,
) bool {
	if shard == nil || payload == nil {
		return false
	}
	if !wasSet || anySwapped {
		return false
	}
	for _, propName := range payload.Properties {
		shard.ClearTokenizationOverlay(propName)
	}
	return true
}

// WaitForLocalTaskDrain blocks until the local goroutine processing the
// given task descriptor has exited, or the provided ctx is cancelled,
// whichever comes first. Returns nil when the goroutine has drained,
// ctx.Err() if the wait timed out.
//
// Intended for the cancel→cleanup sequence: a caller that issued
// [distributedtask.Manager.CancelDistributedTask] cannot safely tear
// down the __reindex / __ingest sidecar buckets while the worker
// goroutine is still writing to them. Calling WaitForLocalTaskDrain
// between CancelDistributedTask and [DB.CleanStalePartialReindexState]
// closes that race window.
//
// Returns nil immediately if no goroutine is running for this descriptor
// (e.g. the task already terminated, or never ran on this node).
func (p *ReindexProvider) WaitForLocalTaskDrain(
	ctx context.Context,
	desc distributedtask.TaskDescriptor,
) error {
	handle, ok := p.runningHandle(desc)
	if !ok {
		return nil
	}
	select {
	case <-handle.Done():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// reindexTaskHandle implements distributedtask.TaskHandle.
type reindexTaskHandle struct {
	cancel context.CancelFunc
	doneCh chan struct{}
}

func (h *reindexTaskHandle) Terminate() {
	h.cancel()
}

func (h *reindexTaskHandle) Done() <-chan struct{} {
	return h.doneCh
}

// ShardReplicaOwnershipForMT returns shard ownership filtered for multi-tenant
// collections. It filters by the given tenant names (or all tenants if empty)
// and skips tenants whose activity status indicates no local data (OFFLOADED,
// OFFLOADING, FROZEN, FREEZING, UNFREEZING, ONLOADING).
func (db *DB) ShardReplicaOwnershipForMT(ctx context.Context, className string, tenantNames []string) (map[string][]string, error) {
	result := make(map[string][]string)
	tenantSet := make(map[string]struct{}, len(tenantNames))
	for _, tn := range tenantNames {
		tenantSet[tn] = struct{}{}
	}

	err := db.schemaReader.Read(className, true, func(_ *models.Class, state *sharding.State) error {
		if state == nil {
			return fmt.Errorf("unable to retrieve sharding state for class %s", className)
		}

		for shardName, shard := range state.Physical {
			// Filter by tenant names if specified.
			if len(tenantSet) > 0 {
				if _, ok := tenantSet[shardName]; !ok {
					continue
				}
			}

			// Skip tenants without local data.
			status := entschema.ActivityStatus(shard.Status)
			switch status {
			case models.TenantActivityStatusHOT,
				models.TenantActivityStatusACTIVE,
				models.TenantActivityStatusCOLD,
				models.TenantActivityStatusINACTIVE:
				// These have local data — include them.
			default:
				// OFFLOADED, OFFLOADING, FROZEN, FREEZING, UNFREEZING, ONLOADING — skip.
				continue
			}

			for _, node := range shard.BelongsToNodes {
				if node != "" {
					result[node] = append(result[node], shardName)
				}
			}
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read sharding state for class %s: %w", className, err)
	}

	// Sort shard names per node for determinism.
	for _, shards := range result {
		sort.Strings(shards)
	}

	return result, nil
}

// ShardReplicaOwnership returns a map of node name to shard names, with one
// entry per replica. Unlike ShardOwnership (which assigns each shard to one
// node for export load balancing), this returns ALL replica nodes for each
// shard. This is needed for reindex tasks where every replica must process
// its own local copy of the data.
//
// WARNING: Do NOT use ShardOwnership for reindex — it only returns one node per
// shard and would leave replicas on other nodes un-reindexed.
func (db *DB) ShardReplicaOwnership(ctx context.Context, className string) (map[string][]string, error) {
	result := make(map[string][]string)

	err := db.schemaReader.Read(className, true, func(_ *models.Class, state *sharding.State) error {
		if state == nil {
			return fmt.Errorf("unable to retrieve sharding state for class %s", className)
		}

		for shardName, shard := range state.Physical {
			for _, node := range shard.BelongsToNodes {
				if node != "" {
					result[node] = append(result[node], shardName)
				}
			}
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read sharding state for class %s: %w", className, err)
	}

	// Sort shard names per node for determinism.
	for _, shards := range result {
		sort.Strings(shards)
	}

	return result, nil
}

// lookupShardByName returns the named shard from the index, or an error
// describing why the lookup failed. There is no Index.GetShardByName, so
// callers walk ForEachShard; centralise that walk here so the two call
// sites (processOneUnit and OnGroupCompleted) report the same shape of
// error.
func lookupShardByName(idx *Index, shardName string) (ShardLike, error) {
	var found ShardLike
	if err := idx.ForEachShard(func(name string, s ShardLike) error {
		if name == shardName {
			found = s
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("iterating shards: %w", err)
	}
	if found == nil {
		return nil, fmt.Errorf("shard %q not found", shardName)
	}
	return found, nil
}
