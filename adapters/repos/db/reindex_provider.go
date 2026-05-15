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
// For format-only migrations (repair-searchable, repair-filterable), each shard
// runs the full lifecycle independently via RunOnShard.
//
// For semantic migrations (change-tokenization, enable-rangeable), barrier
// semantics apply: all shards reindex first (RunReindexOnlyOnShard), then once
// all units are terminal, OnGroupCompleted fires and runs the swap phase
// (RunSwapOnShard) on each local shard. This ensures no shard serves new data
// until ALL shards are ready.
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

	// For semantic migrations (change-tokenization, enable-rangeable), use
	// two-phase execution: reindex only, then swap after all units complete.
	// For format-only migrations, run the full lifecycle per shard.
	semantic := IsSemanticMigration(payload.MigrationType)

	// Cache task instances for semantic migrations so OnGroupCompleted can
	// call RunSwapOnShard on the same instances (with callbacks registered).
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

	// genFor returns the generation suffix N to use for this migration on this
	// shard, given the strategy's dir prefix and its props suffix (e.g. "_text"
	// or sorted-joined "_p1_p2", or "" for class-level strategies).
	genFor := func(prefix, propSuffix string) int {
		if rehydrate {
			// Reconstruct against the highest existing gen on disk. If none
			// exists (recovery race / corrupted state) fall back to 1 — the
			// strategy will fail loudly at runtimeSwap rather than silently
			// reusing stale state.
			if gen := maxMigrationGeneration(lsmPath, prefix, propSuffix); gen > 0 {
				return gen
			}
			return 1
		}
		return nextMigrationGeneration(lsmPath, prefix, propSuffix)
	}

	switch payload.MigrationType {
	case ReindexTypeRepairSearchable:
		gen := genFor(MigrationDirSearchableMapToBlockmax, "")
		return []*ShardReindexTaskGeneric{
			NewRuntimeMapToBlockmaxTask(p.logger, p.schemaManager, payload.Properties, payload.Collection, gen),
		}, nil

	case ReindexTypeRepairFilterable:
		gen := genFor(MigrationDirFilterableRoaringsetRefresh, "")
		return []*ShardReindexTaskGeneric{
			NewRuntimeRoaringSetRefreshTask(p.logger, payload.Properties, payload.Collection, gen),
		}, nil

	case ReindexTypeEnableRangeable, ReindexTypeRepairRangeable:
		// Repair-rangeable uses the same strategy as enable-rangeable —
		// rangeable is rebuilt from the existing filterable bucket either
		// way. The validator at submit time gates which one is allowed
		// based on the property's current IndexRangeFilters state.
		gen := genFor(MigrationDirPrefixFilterableToRangeable, propsSuffix(payload.Properties))
		return []*ShardReindexTaskGeneric{
			NewRuntimeFilterableToRangeableTask(p.logger, p.schemaManager, payload.Properties, payload.Collection, gen),
		}, nil

	case ReindexTypeEnableFilterable:
		gen := genFor(MigrationDirPrefixEnableFilterable, propsSuffix(payload.Properties))
		return []*ShardReindexTaskGeneric{
			NewRuntimeEnableFilterableTask(p.logger, payload.Properties, payload.Collection, gen),
		}, nil

	case ReindexTypeEnableSearchable:
		if payload.TargetTokenization == "" {
			return nil, fmt.Errorf("enable-searchable requires targetTokenization")
		}
		gen := genFor(MigrationDirPrefixEnableSearchable, propsSuffix(payload.Properties))
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
		searchableGen := genFor(MigrationDirPrefixSearchableRetokenize, "_"+propName)
		tasks := []*ShardReindexTaskGeneric{
			NewRuntimeSearchableRetokenizeTask(
				p.logger, propName, payload.TargetTokenization,
				payload.Collection, payload.BucketStrategy, payload.Collection,
				searchableGen,
			),
		}
		if p.propertyHasFilterableBucket(payload.Collection, propName) {
			filterableGen := genFor(MigrationDirPrefixFilterableRetokenize, "_"+propName)
			tasks = append(tasks, NewRuntimeFilterableRetokenizeTask(
				p.logger,
				propName, payload.TargetTokenization,
				payload.Collection, payload.Collection,
				filterableGen,
			))
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
		filterableGen := genFor(MigrationDirPrefixFilterableRetokenize, "_"+propName)
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
func (p *ReindexProvider) OnGroupCompleted(task *distributedtask.Task, groupID string, localGroupUnitIDs []string) {
	logger := p.logger.WithField("taskID", task.ID).WithField("groupID", groupID).
		WithField("localGroupUnitIDs", localGroupUnitIDs)

	payload, err := p.loadPayload(task)
	if err != nil {
		logger.WithError(err).Error("reindex provider: OnGroupCompleted: failed to load payload")
		return
	}

	if !IsSemanticMigration(payload.MigrationType) {
		logger.Info("reindex provider: OnGroupCompleted (format-only, no-op)")
		return
	}

	logger.Info("reindex provider: OnGroupCompleted — starting swap phase for semantic migration")

	className := entschema.ClassName(payload.Collection)
	idx := p.db.GetIndex(className)
	if idx == nil {
		logger.Error("reindex provider: OnGroupCompleted: collection not found")
		return
	}

	// Run the swap phase on each local shard. Use the provider's server ctx
	// so a graceful shutdown aborts in-flight swaps rather than blocking
	// forever in the FlushAndSwitch / rename loop.
	ctx := p.serverCtx
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
				continue
			}
			var err error
			unitTasks, err = p.createReindexTasks(payload, concreteShard.pathLSM(), true)
			if err != nil {
				logger.WithField("unit", unitID).WithError(err).
					Error("reindex provider: OnGroupCompleted: creating reindex tasks")
				continue
			}
			rehydrate = true
			logger.WithField("unit", unitID).
				Info("reindex provider: OnGroupCompleted: rebuilding tasks from disk (node likely restarted); will rehydrate before swap")
		}

		allSwapped := true
		for _, reindexTask := range unitTasks {
			if rehydrate {
				if err := reindexTask.RunReindexOnlyOnShard(ctx, shard); err != nil {
					logger.WithField("unit", unitID).WithField("task", reindexTask.Name()).
						WithError(err).Error("reindex provider: OnGroupCompleted: rehydrate failed; swap will not run for this task")
					allSwapped = false
					continue
				}
			}
			if err := reindexTask.RunSwapOnShard(ctx, shard); err != nil {
				logger.WithField("unit", unitID).WithField("task", reindexTask.Name()).
					WithError(err).Error("reindex provider: OnGroupCompleted: RunSwapOnShard failed — migration is half-applied on this shard")
				allSwapped = false
			}
		}
		if allSwapped {
			logger.WithField("unit", unitID).WithField("shard", shardName).
				Info("reindex provider: swap complete")
		} else {
			// Loud, structured log so operators and log queries can find
			// half-applied migrations. The DTM cannot be told via this
			// hook (the unit is already terminal by definition — that's
			// what triggered this callback), so we surface it the only
			// way we can: an unambiguous error-level log line at the end
			// of OnGroupCompleted. A proper fix would need a RAFT-stored
			// per-node post-swap acknowledgement.
			logger.WithField("unit", unitID).WithField("shard", shardName).
				Error("reindex provider: swap INCOMPLETE for this shard — at least one task's RunSwapOnShard returned an error; downstream schema state may be inconsistent")
		}
	}
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

	if task.Status != distributedtask.TaskStatusFinished {
		// Failed / cancelled / still-running: do not flip the schema.
		// (still-running shouldn't happen for OnTaskCompleted but we guard
		// defensively rather than racing the FSM.)
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
	}
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
// NOTE: the FINISHED transition in the DTM fires when all units are
// terminal, which is BEFORE OnGroupCompleted runs the swap on each
// node. So a poller waiting for FINISHED may see "done" before the
// schema flag has flipped on this node; callers must Eventually-poll
// the schema for the actual post-swap state.
func IsSemanticMigration(mt ReindexMigrationType) bool {
	return mt == ReindexTypeChangeTokenization ||
		mt == ReindexTypeChangeTokenizationFilterable ||
		mt == ReindexTypeEnableFilterable ||
		mt == ReindexTypeEnableSearchable
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
