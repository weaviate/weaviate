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
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/distributedtask"
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

	throttled := distributedtask.NewThrottledRecorder(p.recorder, 30*time.Second, clockwork.NewRealClock())

	enterrors.GoWrapper(func() {
		defer func() {
			p.mu.Lock()
			delete(p.runningHandles, task.TaskDescriptor)
			p.mu.Unlock()
			close(handle.doneCh)
		}()

		p.processUnits(ctx, task, &payload, idx, localUnits, throttled)
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

	// Create the reindex task(s) for this migration type.
	tasks, err := p.createReindexTasks(payload)
	if err != nil {
		p.failUnit(ctx, task, unitID, recorder, fmt.Sprintf("creating reindex tasks: %v", err))
		return
	}

	// For semantic migrations (change-tokenization, enable-rangeable), use
	// two-phase execution: reindex only, then swap after all units complete.
	// For format-only migrations, run the full lifecycle per shard.
	semantic := isSemanticMigration(payload.MigrationType)

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

func (p *ReindexProvider) createReindexTasks(payload *ReindexTaskPayload) ([]*ShardReindexTaskGeneric, error) {
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

	switch payload.MigrationType {
	case ReindexTypeRepairSearchable:
		return []*ShardReindexTaskGeneric{
			NewRuntimeMapToBlockmaxTask(p.logger, p.schemaManager, payload.Properties, payload.Collection),
		}, nil

	case ReindexTypeRepairFilterable:
		return []*ShardReindexTaskGeneric{
			NewRuntimeRoaringSetRefreshTask(p.logger, payload.Properties, payload.Collection),
		}, nil

	case ReindexTypeEnableRangeable, ReindexTypeRepairRangeable:
		// Repair-rangeable uses the same strategy as enable-rangeable —
		// rangeable is rebuilt from the existing filterable bucket either
		// way. The validator at submit time gates which one is allowed
		// based on the property's current IndexRangeFilters state.
		return []*ShardReindexTaskGeneric{
			NewRuntimeFilterableToRangeableTask(p.logger, p.schemaManager, payload.Properties, payload.Collection),
		}, nil

	case ReindexTypeEnableFilterable:
		return []*ShardReindexTaskGeneric{
			NewRuntimeEnableFilterableTask(p.logger, p.schemaManager, payload.Properties, payload.Collection),
		}, nil

	case ReindexTypeEnableSearchable:
		if payload.TargetTokenization == "" {
			return nil, fmt.Errorf("enable-searchable requires targetTokenization")
		}
		return []*ShardReindexTaskGeneric{
			NewRuntimeEnableSearchableTask(p.logger, p.schemaManager, payload.Properties, payload.Collection, payload.TargetTokenization),
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

		searchableTask := NewRuntimeSearchableRetokenizeTask(
			p.logger, propName, payload.TargetTokenization,
			payload.Collection, payload.BucketStrategy, payload.Collection,
		)
		filterableTask := NewRuntimeFilterableRetokenizeTask(
			p.logger, p.schemaManager,
			propName, payload.TargetTokenization,
			payload.Collection, payload.Collection,
		)
		return []*ShardReindexTaskGeneric{searchableTask, filterableTask}, nil

	default:
		return nil, fmt.Errorf("unknown migration type %q", payload.MigrationType)
	}
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
		if isPermanentRecorderRejection(recErr) {
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
// describes a stable FSM state that retrying cannot fix. The Manager in
// cluster/distributedtask returns fmt.Errorf-formatted strings rather
// than sentinel errors, so we match on substrings.
//
// The unmarshal-error path in Manager.RecordUnitCompletion (a malformed
// SubCommand) is technically permanent too but is deliberately NOT
// matched: applyDistributedTaskCommand marshals the request itself, so
// a malformed SubCommand reaching the Manager is a same-binary bug
// rather than a recoverable FSM state — it should surface as the loud
// alarm, not the quiet warning.
//
// TODO: replace with errors.Is once cluster/distributedtask exposes
// sentinel error vars (e.g. ErrTaskNotRunning, ErrUnitAlreadyTerminal,
// ErrUnitWrongNode). Note that the follower→leader gRPC forwarding
// path re-encodes the error as a status-code string and loses the %w
// chain, so any sentinel refactor needs to either keep this substring
// fallback or teach the cluster RPC layer to preserve sentinels (e.g.
// via gRPC status details).
func isPermanentRecorderRejection(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	// All four substrings come from cluster/distributedtask/manager.go:
	//   "task %s/%s/%d is no longer running"          → errTaskNotRunning
	//   "task %s/%s/%d does not exist"                → findVersionedTaskWithLock
	//   "unit %s in task %s/%s/%d is already terminal"
	//   "unit %s in task %s/%s/%d belongs to node X, not Y"
	for _, marker := range []string{
		"is no longer running",
		"does not exist",
		"is already terminal",
		"belongs to node",
	} {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
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

	if !isSemanticMigration(payload.MigrationType) {
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
			var err error
			unitTasks, err = p.createReindexTasks(payload)
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
			// of OnGroupCompleted. A proper fix (RAFT-stored per-node
			// post-swap acknowledgement) is tracked as follow-up work
			// on issue #10675.
			logger.WithField("unit", unitID).WithField("shard", shardName).
				Error("reindex provider: swap INCOMPLETE for this shard — at least one task's RunSwapOnShard returned an error; downstream schema state may be inconsistent")
		}
	}
}

// OnTaskCompleted fires after all units across all nodes are terminal.
// Cleanup cached payload data.
func (p *ReindexProvider) OnTaskCompleted(task *distributedtask.Task) {
	p.mu.Lock()
	delete(p.payloads, task.TaskDescriptor)
	delete(p.reindexTasks, task.TaskDescriptor)
	p.mu.Unlock()

	p.logger.WithField("taskID", task.ID).WithField("status", task.Status).
		Info("reindex provider: OnTaskCompleted")
}

// isSemanticMigration returns true for migration types that change query
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
//
// Must stay in sync with the handler-side isSemanticMigration in
// adapters/handlers/rest/handlers_reindex.go.
func isSemanticMigration(mt ReindexMigrationType) bool {
	return mt == ReindexTypeChangeTokenization ||
		mt == ReindexTypeEnableFilterable ||
		mt == ReindexTypeEnableSearchable
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
