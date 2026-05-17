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
//     the FINALIZING-window tokenization overlay lives.
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
}

// NewReindexProvider creates a new ReindexProvider. The concurrency function
// is called at task start time to determine how many shards to reindex in
// parallel (typically backed by a runtime.DynamicValue). serverCtx should
// be a process-shutdown context so the OnGroupCompleted swap phase can
// abort cleanly on graceful shutdown.
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
		db:             db,
		schemaManager:  schemaManager,
		logger:         logger,
		localNode:      localNode,
		concurrency:    concurrency,
		serverCtx:      serverCtx,
		runningHandles: make(map[distributedtask.TaskDescriptor]*reindexTaskHandle),
		payloads:       make(map[distributedtask.TaskDescriptor]*ReindexTaskPayload),
		reindexTasks:   make(map[distributedtask.TaskDescriptor]map[string][]*ShardReindexTaskGeneric),
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

	p.mu.Lock()
	p.runningHandles[task.TaskDescriptor] = handle
	p.payloads[task.TaskDescriptor] = &payload
	p.mu.Unlock()

	// Progress is emitted from the inverted-index reindex iteration every
	// checkProcessingEveryNoObjects iterations (default 1000). p.recorder
	// is the scheduler-provided recorder, already wrapped in a global
	// ThrottledRecorder (see Scheduler.Start) that caps per-unit writes
	// at 3s — sufficient for the GET /indexes poller without flooding
	// Raft. No additional throttle is needed here.
	enterrors.GoWrapper(func() {
		defer func() {
			p.mu.Lock()
			delete(p.runningHandles, task.TaskDescriptor)
			p.mu.Unlock()
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

	var wg sync.WaitGroup
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

	wg.Wait()
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
		logger.WithError(err).Error("reindex provider: failed to report initial progress")
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
	// Skip the work outright instead of re-running the cached task: the
	// first run's RunReindexOnlyOnShard either is in flight or has
	// already called RecordDistributedTaskUnitCompletion, and rerunning
	// OnAfterLsmInit on the same task instance would APPEND to the
	// task's callbackDisableFuncs list and double every subsequent
	// double-write callback fire. The FSM-side recorder calls are
	// idempotent against terminal units (manager.UpdateUnitProgress
	// silently ignores updates to terminal units), so the second run
	// has nothing useful to do.
	if semantic {
		p.mu.Lock()
		cached := p.reindexTasks[task.TaskDescriptor][unitID]
		p.mu.Unlock()
		if len(cached) > 0 {
			logger.WithField("nTasks", len(cached)).
				Info("reindex provider: skipping re-entered unit (scheduler relaunched handle before FSM saw prior completion)")
			return
		}
	}

	// Create the reindex task(s) for this migration type.
	tasks, err := p.createReindexTasks(payload, concreteShard.pathLSM(), false)
	if err != nil {
		p.failUnit(ctx, task, unitID, recorder, fmt.Sprintf("creating reindex tasks: %v", err))
		return
	}

	// Hook up live progress reporting. The recorder above this layer is
	// already throttled (see StartTask), so the iteration loop can call the
	// callback freely — only one update per throttle window reaches RAFT.
	// Errors from UpdateDistributedTaskUnitProgress are logged but do NOT
	// fail the unit: a transient RAFT hiccup that drops one progress tick
	// must not abort the underlying migration.
	for _, reindexTask := range tasks {
		// Capture per-iteration; the closure may outlive this stack frame
		// because the callback fires from inside the reindex loop.
		taskRef := reindexTask
		taskRef.SetProgressCallback(func(progress float32) {
			if err := recorder.UpdateDistributedTaskUnitProgress(
				ctx, task.Namespace, task.ID, task.Version, p.localNode, unitID, progress,
			); err != nil {
				logger.WithError(err).
					WithField("progress", progress).
					Debug("reindex provider: failed to report progress (will retry on next tick)")
			}
		})
	}

	// Cache task instances for semantic migrations so OnGroupCompleted can
	// call RunSwapOnShard on the same instances (with callbacks registered).
	// On the re-entry path we already retrieved tasks from the cache, so
	// this write is a no-op (same map value); guard the nil-map alloc and
	// write here for the fresh-task path.
	if semantic {
		p.mu.Lock()
		if p.reindexTasks[task.TaskDescriptor] == nil {
			p.reindexTasks[task.TaskDescriptor] = make(map[string][]*ShardReindexTaskGeneric)
		}
		p.reindexTasks[task.TaskDescriptor][unitID] = tasks
		p.mu.Unlock()
	}

	// Persist a recovery record so that a restart mid-flight can rebuild
	// these same task instances during shard init. Without this, writes
	// arriving between shard init and OnGroupCompleted's swap go only to
	// the old main bucket (no ingest double-write) and are lost on swap.
	// See [ReindexProvider.persistRecoveryRecord] for the on-disk shape.
	if err := p.persistRecoveryRecord(task, payload, unitID, concreteShard.pathLSM(), tasks); err != nil {
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
			p.failUnit(ctx, task, unitID, recorder,
				fmt.Sprintf("reindex (%s): %v", reindexTask.Name(), runErr))
			return
		}
	}

	logger.Info("reindex provider: unit completed")

	if err := recorder.RecordDistributedTaskUnitCompletion(
		ctx, task.Namespace, task.ID, task.Version, p.localNode, unitID,
	); err != nil {
		logger.WithError(err).Error("reindex provider: failed to record completion")
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
	case ReindexTypeRepairSearchable:
		gen, ok := genFor(MigrationDirSearchableMapToBlockmax, "")
		if !ok {
			return nil, nil
		}
		return []*ShardReindexTaskGeneric{
			NewRuntimeMapToBlockmaxTask(p.logger, p.schemaManager, payload.Properties, payload.Collection, gen),
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
	p.mu.Lock()
	cached := p.payloads[task.TaskDescriptor]
	p.mu.Unlock()
	if cached != nil {
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
	logger.Error("reindex provider: unit failed: " + errMsg)

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
// As a safety net during rolling upgrades, we keep the legacy
// substring matching as a fallback for the case where an old leader
// (pre-sentinel) returns the legacy plain-text error to a new
// follower. Whenever the fallback fires, we log at Warn level so
// operators can confirm in prod whether the sentinel path has fully
// rolled out.
//
// TODO(v1.40): remove the substring fallback below. The sentinel
// path ships in v1.38; once the supported rolling-upgrade window
// can no longer include a pre-sentinel binary (i.e. the v1.40
// cycle), the legacy markers and the operator-Warn become dead
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

	// Legacy fallback for mixed-version clusters: substring match
	// against the historical phrasings. If this fires, we want a Warn
	// log so an operator can see that the sentinel path isn't (yet)
	// fully rolled out in their cluster.
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

// OnGroupCompleted fires after all units in a group reach terminal state.
// For semantic migrations (change-tokenization), this is the barrier: all
// shards have finished reindexing, so we now run the swap phase on each local
// shard. For format-only migrations, this is a no-op because RunOnShard
// already completed the full lifecycle.
//
// localGroupUnitIDs contains ONLY units assigned to THIS node, not all units
// in the group. If a node has no units in the group, this callback does not
// fire on that node.
//
// The swap phase attempts to reuse cached task instances from processOneUnit
// (which preserve double-write callbacks). If the cache is empty (e.g. after
// node restart), a fresh task is created as fallback.
//
// Returns a non-nil error if ANY of this node's units in the group failed
// to complete its swap (rehydrate failure, RunSwapOnShard failure, missing
// shard/index, etc.). The scheduler propagates that error into the
// post-completion ack the cluster gates MarkTaskFinalized on; a failure
// here transitions the task to FAILED instead of FINISHED, which makes
// OnTaskCompleted skip the cluster-wide schema flip — the load-bearing
// invariant that prevents a per-node swap failure from leaving the
// cluster-wide schema pointing at not-yet-swapped buckets.
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
			Debug("reindex provider: OnGroupCompleted: skipping replay on past-terminal task")
		return nil
	}

	payload, err := p.loadPayload(task)
	if err != nil {
		logger.WithError(err).Error("reindex provider: OnGroupCompleted: failed to load payload")
		return fmt.Errorf("load payload: %w", err)
	}

	if !IsSemanticMigration(payload.MigrationType) {
		logger.Info("reindex provider: OnGroupCompleted (format-only, no-op)")
		return nil
	}

	logger.Info("reindex provider: OnGroupCompleted — starting swap phase for semantic migration")

	className := entschema.ClassName(payload.Collection)
	idx := p.db.GetIndex(className)
	if idx == nil {
		logger.Error("reindex provider: OnGroupCompleted: collection not found")
		return fmt.Errorf("collection %q not found on this node", payload.Collection)
	}

	// Run the swap phase on each local shard. Use the provider's server ctx
	// so a graceful shutdown aborts in-flight swaps rather than blocking
	// forever in the FlushAndSwitch / rename loop.
	//
	// unitErrs aggregates per-unit swap errors so we can return the full
	// failure picture to the scheduler. The scheduler aggregates this
	// across all of this node's groups for the task and publishes ONE
	// post-completion ack (success=false with the joined error message).
	// The cluster gates MarkTaskFinalized on every node's ack landing
	// successfully; any failure transitions the task to FAILED and
	// skips the cluster-wide schema flip.
	//
	// sawContextCanceled tracks whether at least one unit's swap failed
	// because the server context was cancelled (graceful shutdown /
	// rolling restart). The scheduler treats a context-canceled return
	// as a transient "didn't finish yet" — the recovery path on the
	// next boot re-runs OnGroupCompleted via the rehydrate branch and
	// the swap completes cleanly without an in-flight compaction to
	// abort. Returning context.Canceled up the stack lets [Scheduler]
	// errors.Is the error and skip ack emission for THIS process —
	// recovery owns the resolution.
	ctx := p.serverCtx
	var unitErrs []string
	var sawContextCanceled bool
	for _, unitID := range localGroupUnitIDs {
		// Skip units that failed during reindex.
		unit := task.Units[unitID]
		if unit != nil && unit.Status == distributedtask.UnitStatusFailed {
			logger.WithField("unit", unitID).Warn("reindex provider: skipping swap for failed unit")
			continue
		}

		shardName := payload.UnitToShard[unitID]
		shard, err := lookupShardByName(idx, shardName)
		if err != nil {
			logger.WithField("unit", unitID).WithField("shard", shardName).WithError(err).
				Error("reindex provider: OnGroupCompleted: shard lookup failed")
			unitErrs = append(unitErrs, fmt.Sprintf("unit %s shard %s lookup: %v", unitID, shardName, err))
			continue
		}

		// Retrieve the cached task instances that ran RunReindexOnlyOnShard.
		// These have the double-write callbacks registered; creating new
		// instances would lose them.
		p.mu.Lock()
		unitTasks := p.reindexTasks[task.TaskDescriptor][unitID]
		p.mu.Unlock()

		// rehydrate is true when we're rebuilding tasks from disk (cache
		// was lost across a node restart). The fresh task instances do
		// NOT have ingest/reindex buckets loaded into the LSM store and
		// do NOT have double-write callbacks registered, so calling
		// RunSwapOnShard directly would fail with "reindex bucket not
		// found" for every property. Before this fix, the failure was
		// swallowed, the task transitioned to FINISHED anyway, and the
		// migration was left permanently half-applied.
		//
		// RunReindexOnlyOnShard is idempotent once rt.IsReindexed() is
		// true on disk — it calls OnAfterLsmInit (which loads the
		// ingest/reindex buckets and registers callbacks because
		// IsReindexed && !IsSwapped), then exits on the first iteration
		// without doing additional reindex work. So we use it purely as
		// a rehydration step before RunSwapOnShard.
		rehydrate := false
		if len(unitTasks) == 0 {
			concreteShard, unwrapErr := unwrapShard(ctx, shard)
			if unwrapErr != nil {
				logger.WithField("unit", unitID).WithError(unwrapErr).
					Error("reindex provider: OnGroupCompleted: unwrap shard for rehydrate")
				unitErrs = append(unitErrs, fmt.Sprintf("unit %s unwrap shard: %v", unitID, unwrapErr))
				continue
			}
			var err error
			unitTasks, err = p.createReindexTasks(payload, concreteShard.pathLSM(), true)
			if err != nil {
				logger.WithField("unit", unitID).WithError(err).
					Error("reindex provider: OnGroupCompleted: creating reindex tasks")
				unitErrs = append(unitErrs, fmt.Sprintf("unit %s create tasks: %v", unitID, err))
				continue
			}
			if len(unitTasks) == 0 {
				// No in-flight migration state on disk for this strategy
				// on this shard — `FinalizeCompletedMigrations` at startup
				// already promoted the canonical bucket and cleared the
				// tracker, OR the end-of-swap trim cleaned up before we
				// crashed. Either way there is nothing to swap. This is
				// the normal post-restart path for a task whose FSM entry
				// is still FINISHED but whose local-process state was lost
				// — the swap callbacks already ran (and persisted to
				// disk via the canonical bucket) before the restart.
				logger.WithField("unit", unitID).
					Info("reindex provider: OnGroupCompleted: no in-flight state on disk for this unit (post-restart of already-finalized migration); skipping swap")
				continue
			}
			rehydrate = true
			logger.WithField("unit", unitID).
				Info("reindex provider: OnGroupCompleted: rebuilding tasks from disk (node likely restarted); will rehydrate before swap")
		}

		// Atomic-phase contract: the per-shard swap is split into three
		// phases, executed in order:
		//
		//   1. PREP (RunPrepareOnShard, per task) — disk-I/O-heavy work:
		//      FlushAndSwitch reindex bucket, ShutdownBucket (drains
		//      compaction), PrependSegmentsFromBucket. Bucket=OLD and
		//      schema=OLD throughout — safe to run with live queries.
		//      No overlay set yet; the overlay would expose a (NEW
		//      input, OLD bucket) mismatch during this phase.
		//   2. OVERLAY SET — maybeSetTokenizationOverlayPreSwap. After
		//      every task on this shard has its prep merged.
		//   3. ATOMIC SWAP (RunSwapOnShard, per task) — in-memory
		//      bucket-pointer flip + per-prop sentinel fsync only.
		//      Microseconds + a few ms per prop. The disk dirs aren't
		//      renamed here; that's deferred to next startup via
		//      OnBeforeLsmInit's recoverRuntimeSwapBuckets path.
		//
		// Why three phases instead of two: the overlay-vs-bucket window
		// is what the overlay was designed to protect. Setting the
		// overlay before prep would expose the very gap it closes
		// (NEW-tokenized analyzer input against OLD-tokenized bucket
		// content for hundreds of ms). Setting the overlay between
		// prep and atomic swap means the window is bounded by the
		// in-memory pointer flip (microseconds). See the file-level
		// godoc on inverted_reindex_task_generic.go for the full
		// phase contract.
		//
		// Failure handling:
		//   - PREP failure: overlay not set, swap not attempted for
		//     ANY task on this shard. Recovery on next OnGroupCompleted
		//     call (or post-restart rehydrate) re-runs PREP, which is
		//     idempotent (no-op for already-merged tasks).
		//   - SWAP failure (after overlay set): partial-success is
		//     possible. The defensive clear below handles the
		//     all-failed case; partial-success is the operator-repair
		//     surface (see the FAILED-task repair_command logging
		//     below).
		//
		// LazyLoadShard wrapping: production shards can be
		// *LazyLoadShard rather than *Shard. unwrapShard ensures the
		// overlay always reaches the concrete shard storage.

		// Phase 1: prep every task on this shard. Each task's
		// RunPrepareOnShard advances the sentinel through markMerged.
		// Idempotent — calls into already-merged tasks short-circuit.
		allSwapped := true
		anySwapped := false
		prepOK := true
		for _, reindexTask := range unitTasks {
			if rehydrate {
				if err := reindexTask.RunReindexOnlyOnShard(ctx, shard); err != nil {
					logger.WithField("unit", unitID).WithField("task", reindexTask.Name()).
						WithError(err).Error("reindex provider: OnGroupCompleted: rehydrate failed; prep+swap will not run for this task")
					unitErrs = append(unitErrs, fmt.Sprintf("unit %s task %s rehydrate: %v", unitID, reindexTask.Name(), err))
					if errors.Is(err, context.Canceled) {
						sawContextCanceled = true
					}
					prepOK = false
					allSwapped = false
					continue
				}
			}
			if err := reindexTask.RunPrepareOnShard(ctx, shard); err != nil {
				logger.WithField("unit", unitID).WithField("task", reindexTask.Name()).
					WithError(err).Error("reindex provider: OnGroupCompleted: prep failed; swap will not run for this task")
				unitErrs = append(unitErrs, fmt.Sprintf("unit %s task %s prepare: %v", unitID, reindexTask.Name(), err))
				if errors.Is(err, context.Canceled) {
					sawContextCanceled = true
				}
				prepOK = false
				allSwapped = false
			}
		}

		if !prepOK {
			// At least one task failed to prep. Skip the overlay set
			// and the atomic-swap pass entirely for this shard — the
			// recovery path re-runs prep on the next OnGroupCompleted
			// invocation. Logging is already loud (per-task error logs
			// above + the swap-INCOMPLETE log below).
			logger.WithField("unit", unitID).WithField("shard", shardName).
				Warn("reindex provider: prep phase incomplete; skipping overlay set + atomic swap for this shard")
			logger.WithField("unit", unitID).WithField("shard", shardName).
				Error("reindex provider: swap INCOMPLETE for this shard — prep phase failed; the cluster-wide post-completion ack will report this as a failure")
			continue
		}

		// Phase 2: now that every task on this shard is merged, set the
		// per-shard tokenization overlay. The atomic SwapBucketPointer
		// calls in Phase 3 run inside this overlay window.
		setShard, setUnwrapErr := unwrapShard(ctx, shard)
		if setUnwrapErr != nil && IsTokenizationChangingMigration(payload.MigrationType) {
			logger.WithField("unit", unitID).WithField("shard", shardName).
				WithError(setUnwrapErr).
				Warn("reindex provider: cannot set tokenization overlay — shard unwrap failed; queries during FINALIZING window may observe stale-tokenization results")
		}
		overlayWasSet := maybeSetTokenizationOverlayPreSwap(setShard, payload)

		// Phase 3: atomic in-memory swap per task. The disk-dir rename
		// is deferred to next startup.
		for _, reindexTask := range unitTasks {
			if err := reindexTask.RunSwapOnShard(ctx, shard); err != nil {
				logger.WithField("unit", unitID).WithField("task", reindexTask.Name()).
					WithError(err).Error("reindex provider: OnGroupCompleted: RunSwapOnShard failed — migration is half-applied on this shard")
				unitErrs = append(unitErrs, fmt.Sprintf("unit %s task %s swap: %v", unitID, reindexTask.Name(), err))
				if errors.Is(err, context.Canceled) {
					sawContextCanceled = true
				}
				allSwapped = false
			} else {
				anySwapped = true
			}
		}

		// Defensive overlay clear: when EVERY swap sub-task failed on
		// this shard, the overlay must come back down so the analyzer
		// stops claiming to be tokenizing with the new value. See
		// [maybeClearTokenizationOverlayOnAllFailed] godoc for the full
		// rationale; this call is the matching post-loop counterpart
		// to maybeSetTokenizationOverlayPreSwap above.
		if maybeClearTokenizationOverlayOnAllFailed(setShard, payload, overlayWasSet, anySwapped) {
			logger.WithField("unit", unitID).WithField("shard", shardName).
				Debug("reindex provider: cleared tokenization overlay — every swap sub-task failed; no bucket pointer was flipped on this shard")
		}

		if allSwapped {
			logger.WithField("unit", unitID).WithField("shard", shardName).
				Info("reindex provider: swap complete")
		} else {
			// Loud, structured log so operators and log queries can find
			// half-applied migrations. The aggregated error returned at
			// the bottom of this function feeds the cluster-wide
			// post-completion ack barrier: any per-shard swap failure
			// here causes the task's FSM to transition to FAILED before
			// the schema flip can commit, so no replica is left at the
			// new schema with old data.
			logger.WithField("unit", unitID).WithField("shard", shardName).
				Error("reindex provider: swap INCOMPLETE for this shard — at least one task's RunSwapOnShard returned an error; the cluster-wide post-completion ack will report this as a failure")
		}
	}
	if len(unitErrs) == 0 {
		return nil
	}
	// Preserve the context.Canceled chain when at least one unit failed
	// due to shutdown so the scheduler's errors.Is check routes through
	// the "transient — let recovery re-fire OnGroupCompleted" branch.
	// Without this %w wrap, the joined string would lose the sentinel
	// and the scheduler would emit a failure ack, flipping the task to
	// FAILED before the post-restart recovery gets a chance.
	if sawContextCanceled {
		return fmt.Errorf("OnGroupCompleted: %d unit(s) failed: %s: %w",
			len(unitErrs), strings.Join(unitErrs, "; "), context.Canceled)
	}
	return fmt.Errorf("OnGroupCompleted: %d unit(s) failed: %s",
		len(unitErrs), strings.Join(unitErrs, "; "))
}

// OnTaskCompleted fires after all units across all nodes are terminal.
// For semantic migrations (change-tokenization, enable-filterable,
// enable-searchable), this is the cluster-wide cutover point: every
// node's local OnGroupCompleted has already run the in-memory bucket
// pointer swap (RunSwapOnShard), so issuing the RAFT-idempotent schema
// flip here propagates the new schema to every node within RAFT-apply
// latency — well after every node already has the new bucket content
// pointed-to from its main bucket name.
//
// RAFT-idempotency: every node's scheduler fires OnTaskCompleted; every
// node's call issues the same UpdatePropertyInternal. applyPerPropertySchemaUpdate's
// mutator returns apply=false when the property is already at the
// target state, so RAFT applies exactly one commit (the first to land);
// the remaining N-1 calls are no-ops at the FSM layer.
//
// Failed / cancelled tasks: skip the schema flip. The migration did not
// complete successfully across the cluster, so the schema should reflect
// the pre-migration state. Per-shard cleanup of partial bucket state is
// handled by the existing CleanStalePartialReindexState path on next
// restart or next reindex submission.
//
// Also clears cached payload + reindex task data for the descriptor.
func (p *ReindexProvider) OnTaskCompleted(task *distributedtask.Task) {
	// Cleanup cached state up-front so a failed-task early return below
	// doesn't leak the cache. The schema flip path below re-loads payload
	// via the local task object (task.Payload), not the cache, so this
	// is safe.
	payload, payloadErr := p.loadPayload(task)
	p.mu.Lock()
	delete(p.payloads, task.TaskDescriptor)
	delete(p.reindexTasks, task.TaskDescriptor)
	p.mu.Unlock()

	logger := p.logger.WithField("taskID", task.ID).WithField("status", task.Status)
	logger.Info("reindex provider: OnTaskCompleted")

	if task.Status != distributedtask.TaskStatusSwapping {
		// FAILED / CANCELLED / STARTED / FINISHED: do not flip the schema.
		//  - FAILED: migration did not succeed cluster-wide; the schema
		//    should reflect the pre-migration state. Per-shard cleanup of
		//    partial bucket state is handled by CleanStalePartialReindexState
		//    on next restart or next reindex submission.
		//  - CANCELLED: scheduler already short-circuits before calling here
		//    (see [Scheduler.tick]), but guard defensively.
		//  - STARTED: shouldn't happen for OnTaskCompleted but we guard
		//    defensively rather than racing the FSM.
		//  - FINISHED: a previous OnTaskCompleted already ran and the
		//    scheduler issued MarkDistributedTaskFinalized. Re-firing on
		//    FINISHED would only happen if the [Scheduler.bootstrapProviders]
		//    pre-mark missed this task — never in normal flow.
		if task.Status == distributedtask.TaskStatusFailed && payloadErr == nil {
			logOperatorRepairGuidanceOnFailedSemanticMigration(logger, payload)
		}
		return
	}
	if payloadErr != nil {
		// Without a valid payload we cannot determine the migration type
		// or which schema field to flip. Log loudly — this should never
		// happen post-FINISHED on a well-formed task, but if it does the
		// schema flip cannot proceed and operators need a signal.
		logger.WithError(payloadErr).Error("reindex provider: OnTaskCompleted: failed to load payload; schema flip will not run")
		return
	}
	if !IsSemanticMigration(payload.MigrationType) {
		// Format-only migrations (repair-*, enable-rangeable) flip their
		// (non-query-semantic) metadata inside RunSwapOnShard per shard.
		// Nothing left to do at the cluster-wide level.
		return
	}

	// p.serverCtx survives task callback returns (the per-task ctx is gone
	// by the time OnTaskCompleted fires from a scheduler tick) and is
	// cancelled on graceful shutdown.
	ctx := p.serverCtx
	if err := p.flipSemanticMigrationSchema(ctx, payload, logger); err != nil {
		logger.WithError(err).Error("reindex provider: OnTaskCompleted: schema flip failed; migration result is half-applied (bucket swapped on every node, schema still reflects pre-migration state)")
		// Schema flip failed: don't clear the overlay. The bucket pointer
		// is still NEW-tokenized on this node's shards (set by
		// OnGroupCompleted), and the schema is still pre-flip on this
		// node. Queries need the overlay to keep tokenizing for the NEW
		// buckets until either (a) a retry of the schema flip succeeds
		// and we get another chance to clear, or (b) the defensive self-
		// clear in TokenizationFor fires when the live schema eventually
		// catches up.
		return
	}

	// Clear the per-shard tokenization overlay now that the cluster-wide
	// schema flip has committed and the live schema's `prop.Tokenization`
	// matches what the overlay has been serving. The defensive self-clear
	// in Shard.TokenizationFor would also handle this lazily, but
	// clearing explicitly here keeps the steady-state overlay map empty
	// (no entries lingering until the next query touches each prop).
	//
	// Per-shard, on every shard this node owns for the class. Multi-
	// node: each node runs OnTaskCompleted independently and clears its
	// own shards — the RAFT-idempotent flipSemanticMigrationSchema
	// ensures every node's local FSM has the new tokenization before
	// we clear here.
	if IsTokenizationChangingMigration(payload.MigrationType) {
		className := entschema.ClassName(payload.Collection)
		if idx := p.db.GetIndex(className); idx != nil {
			idx.ForEachShard(func(shardName string, sh ShardLike) error {
				// LazyLoadShard wrapping: use unwrapShard so the clear
				// always reaches the concrete shard whose overlay map
				// the set hook populated. If unwrap fails we log and
				// rely on TokenizationFor's self-clear-on-catchup as
				// the backstop (live schema is now NEW post-flip, so
				// the next query touching this prop self-clears).
				concreteShard, err := unwrapShard(ctx, sh)
				if err != nil {
					logger.WithField("shard", shardName).WithError(err).
						Warn("reindex provider: tokenization overlay clear skipped (unwrap failed); relying on TokenizationFor self-clear")
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

// logOperatorRepairGuidanceOnFailedSemanticMigration emits an
// operator-actionable repair command for a FAILED semantic migration.
//
// Background: a FAILED semantic-migration task (change-tokenization /
// change-tokenization-filterable / enable-*) is only ever reached
// when at least one sub-shard/sub-task hit a permanent failure. With
// the per-shard post-completion ack barrier in place, every sub-task
// that succeeded BEFORE the failed sibling already committed its
// local bucket-pointer flip — so the canonical bucket on those shards
// now holds NEW-tokenized data while the cluster-wide schema flip was
// correctly skipped (the task is FAILED). Bucket↔schema inversion on
// the migrated property is the user-visible outcome: every query
// against the inverted index returns 0, because the query-side
// tokenizer (driven by the OLD schema) produces tokens that don't
// match the NEW-tokenized bucket content.
//
// Re-enabling a deleted index (e.g. re-enable-searchable after a
// MutationGuard-blocked DELETE landed mid-migration) rebuilds THAT
// index from scratch, but DOES NOT touch the sibling index's torn
// pointer. So a naive operator response to a FAILED
// change-tokenization leaves the cluster in a state where the
// migrated property still returns 0 on every query.
//
// This function emits an ERROR-level log entry with the exact REST
// command an operator should issue to repair both inverted indexes
// on the property. Per-property `{"<index>":{"rebuild":true}}`
// rebuilds the index from raw objects against the current schema,
// which restores bucket↔schema consistency regardless of which
// direction the tokenizer mismatch landed in.
//
// Not a no-op for non-semantic FAILED tasks: format-only migrations
// (repair-*, enable-rangeable) don't produce the inversion, so this
// function early-returns on those.
//
// Forward-compatible with an eventual auto-submit path: the auto-submit
// can later wrap this same logging on its own success/failure path
// so operators still see the guidance even if auto-repair couldn't
// be queued (e.g. lowest-node-ID is unavailable).
func logOperatorRepairGuidanceOnFailedSemanticMigration(logger logrus.FieldLogger, payload *ReindexTaskPayload) {
	if !IsSemanticMigration(payload.MigrationType) {
		return
	}
	if len(payload.Properties) == 0 {
		// Reserved for a future whole-collection rebuild. No targeted
		// guidance possible; the generic operator runbook applies.
		logger.Error(
			fmt.Errorf(
				"reindex provider: %s on %s FAILED with empty Properties; manual repair guidance not available — inspect /v1/tasks and consider rebuild on every affected inverted index",
				payload.MigrationType, payload.Collection))
		return
	}
	for _, propName := range payload.Properties {
		// The exact PUT body depends on which sibling indexes might
		// have torn. For change-tokenization on a property with both
		// IndexFilterable and IndexSearchable, both can tear; we
		// can't tell from here which sub-task succeeded vs failed
		// (the per-shard ack records have the answer but we don't
		// thread them in for the logging-only path), so we direct
		// the operator to rebuild both. Idempotent at the rebuild
		// strategy level — a healthy index rebuilds to the same
		// content.
		var repairBody string
		switch payload.MigrationType {
		case ReindexTypeChangeTokenization,
			ReindexTypeEnableSearchable,
			ReindexTypeRepairSearchable:
			// Touches BOTH inverted buckets (searchable + filterable
			// for change-tokenization on a property with both
			// indexes; searchable-only strategies still benefit from
			// the joint rebuild in case the sibling filterable side
			// was racing the failure trigger).
			repairBody = `{"filterable":{"rebuild":true},"searchable":{"rebuild":true}}`
		case ReindexTypeChangeTokenizationFilterable,
			ReindexTypeEnableFilterable,
			ReindexTypeRepairFilterable:
			repairBody = `{"filterable":{"rebuild":true}}`
		case ReindexTypeEnableRangeable, ReindexTypeRepairRangeable:
			repairBody = `{"rangeable":{"rebuild":true}}`
		default:
			// Exhaustiveness: any newly-added ReindexMigrationType
			// must land in one of the cases above. Fall back to the
			// rebuild-everything body so a missing case still
			// produces actionable guidance (just slightly broader
			// than necessary).
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
// Returns false iff this node has at least one tracker dir for the
// task's (property, indexType) that is *started* but neither *tidied*
// nor *merged* — i.e., the local OnGroupCompleted's RunSwapOnShard was
// interrupted mid-flight (e.g. context-cancelled during rolling restart
// while shutting down a reindex bucket whose compaction was still
// running).
//
// Returning false tells the scheduler bootstrap pre-mark to skip this
// task so the next tick re-fires OnGroupCompleted; the rehydrate path
// inside OnGroupCompleted picks up the half-applied state from disk and
// completes the swap. Without this, the bootstrap silently suppresses
// the retry and the affected shard stays at the OLD tokenization while
// the cluster-wide schema flip has already committed → per-replica
// divergence (#10675 family, RollingRestartMidMigration repro).
//
// Returns true (no recovery needed) when:
//   - The task is not a semantic migration (format-only migrations do
//     their work entirely inside RunOnShard, no swap barrier to recover).
//   - No tracker dir matches the property/index tuple (already cleaned
//     up by a prior next-restart finalize — there is nothing to recover).
//   - All matching trackers have tidied.mig or merged.mig (swap
//     completed locally).
//   - Local payload load / index lookup / shard lookup fails (caller
//     cannot meaningfully retry; defaulting to "done" avoids an
//     infinite re-fire loop on a permanently broken task).
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
	case ReindexTypeRepairSearchable, ReindexTypeRepairFilterable,
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
		// Defensive: legacy test setups constructed without a schema
		// manager would have skipped the schema flip in the old
		// strategy-side code too. Match that behavior.
		return nil
	}

	switch payload.MigrationType {
	case ReindexTypeChangeTokenization, ReindexTypeChangeTokenizationFilterable:
		if payload.TargetTokenization == "" {
			return fmt.Errorf("change-tokenization without targetTokenization in payload")
		}
		// No stale-replay guard needed: with the FINALIZING status (see
		// [distributedtask.TaskStatusSwapping]), OnTaskCompleted only
		// fires while the task is FINALIZING — never on an already-FINISHED
		// task. The cluster-wide conflict check
		// ([db.checkConcurrentReindex]) treats FINALIZING as in-flight,
		// so a newer change-tokenization for the same property cannot be
		// submitted until this one transitions FINALIZING→FINISHED via
		// MarkDistributedTaskFinalized. The "older task replays past a
		// newer one" shape that motivated the OriginalTokenization guard
		// can no longer occur.
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
			// change-tokenization is a single-property migration; a
			// missing property between submit and finalize is a hard
			// error (matches FilterableRetokenizeStrategy's pre-existing
			// behavior).
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
		// Missing properties are tolerated for the multi-property
		// enable-* strategies — a property dropped between submit and
		// finalize is the same outcome we'd want anyway.
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

	default:
		// IsSemanticMigration gate above should exclude every other
		// migration type, so reaching this default is a programming
		// error.
		return fmt.Errorf("unexpected semantic migration type %q in OnTaskCompleted", payload.MigrationType)
	}
}

// IsSemanticMigration returns true for migration types that change query
// behavior. These require barrier semantics: all shards must finish
// reindexing before any shard swaps.
//
// Qualifying migrations:
//
//   - change-tokenization replaces the searchable and filterable bucket
//     content; a partial swap would serve mixed old/new tokenization
//     across shards.
//
//   - enable-filterable / enable-searchable flip a per-property schema
//     flag from false to true. The flag is global across shards once the
//     first shard flips it, so without a barrier readers would see the
//     index as "queryable" while shards still mid-reindex have empty
//     source buckets, returning partial results. Barrier semantics
//     shrink that window to the per-shard swap time, after every shard
//     has finished backfilling its ingest bucket.
//
// enable-rangeable is intentionally NOT semantic: it ships the same
// partial-results trade-off but was already deployed without a barrier
// before the enable-* family was introduced. Promoting it would change
// behavior for existing operators and is tracked separately.
//
// NOTE: with the FINALIZING state (see
// [distributedtask.TaskStatusSwapping]), the FSM stays at FINALIZING
// while OnGroupCompleted (per-node swap) + OnTaskCompleted
// (cluster-wide schema flip) run; only after the scheduler's
// MarkDistributedTaskFinalized RAFT command commits does the status
// flip to FINISHED. Callers polling for FINISHED can therefore trust
// that the schema flag has flipped cluster-wide.
func IsSemanticMigration(mt ReindexMigrationType) bool {
	return mt == ReindexTypeChangeTokenization ||
		mt == ReindexTypeChangeTokenizationFilterable ||
		mt == ReindexTypeEnableFilterable ||
		mt == ReindexTypeEnableSearchable
}

// IsTokenizationChangingMigration reports whether the migration type
// flips the property's stored `Tokenization` field as part of its
// schema commit in [flipSemanticMigrationSchema]. These are the
// migrations that open the FINALIZING-window misalignment between the
// per-shard bucket-pointer flip in OnGroupCompleted.RunSwapOnShard and
// the cluster-wide schema flip in OnTaskCompleted: queries on the
// migrated property would tokenize against the OLD schema value while
// hitting the NEW bucket content, and the per-shard tokenization
// overlay is what closes that window.
//
// Both change-tokenization and change-tokenization-filterable end up
// calling applyPerPropertySchemaUpdate with TargetTokenization (see the
// switch arm in flipSemanticMigrationSchema), so both qualify here.
// enable-searchable also writes Tokenization, but its window is closed
// by EnableSearchableStrategy.AnalyzerOverlay during backfill — the
// query-side gap that motivates the overlay specifically affects
// properties whose tokenization is being CHANGED.
func IsTokenizationChangingMigration(mt ReindexMigrationType) bool {
	return mt == ReindexTypeChangeTokenization ||
		mt == ReindexTypeChangeTokenizationFilterable
}

// maybeSetTokenizationOverlayPreSwap is the per-shard tokenization
// overlay SET hook — called by [OnGroupCompleted] just BEFORE the
// per-task swap loop kicks off on a shard. It records the per-shard
// query-time tokenization override for every property in the payload
// iff this is a tokenization-changing migration with a non-empty
// target tokenization. Returns true iff the overlay was written, so
// the caller can match the [maybeClearTokenizationOverlayOnAllFailed]
// decision after the swap loop.
//
// Extracted from OnGroupCompleted's per-shard branch to keep the
// overlay set/clear lifecycle unit-testable without standing up a
// full provider + DB + index. See
// [maybeClearTokenizationOverlayOnAllFailed] for the post-loop
// counterpart and the all-failed defensive-clear rationale.
func maybeSetTokenizationOverlayPreSwap(shard *Shard, payload *ReindexTaskPayload) bool {
	if shard == nil || payload == nil {
		return false
	}
	if !IsTokenizationChangingMigration(payload.MigrationType) {
		return false
	}
	if payload.TargetTokenization == "" {
		return false
	}
	for _, propName := range payload.Properties {
		shard.SetTokenizationOverlay(propName, payload.TargetTokenization)
	}
	return true
}

// maybeClearTokenizationOverlayOnAllFailed is the defensive CLEAR
// hook — called by [OnGroupCompleted] AFTER the per-task swap loop
// on a shard. It clears the per-shard tokenization overlay iff (a)
// the overlay was set pre-swap by
// [maybeSetTokenizationOverlayPreSwap] (the `wasSet` argument) AND
// (b) every per-task swap failed before flipping its bucket pointer
// (the `anySwapped` argument is false).
//
// Without this clear, an all-failed swap path leaves the overlay set
// against unchanged OLD buckets — the migration's FAILED transition
// then skips the cluster-wide schema flip, so neither
// [OnTaskCompleted]'s explicit clear nor [Shard.TokenizationFor]'s
// self-clear-on-catchup will ever fire (the live schema stays OLD
// and never matches the overlay's NEW value). Permanent misalignment
// until operator repair.
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
	p.mu.Lock()
	handle, ok := p.runningHandles[desc]
	p.mu.Unlock()
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
