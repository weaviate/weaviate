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

// NewReindexProvider creates a new ReindexProvider.
func NewReindexProvider(
	db *DB,
	schemaManager *schema.Manager,
	logger logrus.FieldLogger,
	localNode string,
) *ReindexProvider {
	return &ReindexProvider{
		db:             db,
		schemaManager:  schemaManager,
		logger:         logger,
		localNode:      localNode,
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
	limiter := distributedtask.NewConcurrencyLimiter(2)

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
	var shard ShardLike
	if err := idx.ForEachShard(func(name string, s ShardLike) error {
		if name == shardName {
			shard = s
		}
		return nil
	}); err != nil {
		p.failUnit(ctx, task, unitID, recorder, fmt.Sprintf("iterating shards: %v", err))
		return
	}
	if shard == nil {
		p.failUnit(ctx, task, unitID, recorder, fmt.Sprintf("shard %q not found", shardName))
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

func (p *ReindexProvider) createReindexTasks(payload *ReindexTaskPayload) ([]*ShardReindexTaskGeneric, error) {
	switch payload.MigrationType {
	case ReindexTypeRepairSearchable:
		return []*ShardReindexTaskGeneric{
			NewRuntimeMapToBlockmaxTask(p.logger, p.schemaManager),
		}, nil

	case ReindexTypeRepairFilterable:
		return []*ShardReindexTaskGeneric{
			NewRuntimeRoaringSetRefreshTask(p.logger),
		}, nil

	case ReindexTypeEnableRangeable:
		if len(payload.Properties) == 0 {
			return nil, fmt.Errorf("enable-rangeable requires at least one property")
		}
		return []*ShardReindexTaskGeneric{
			NewRuntimeFilterableToRangeableTask(p.logger, p.schemaManager, payload.Properties, payload.Collection),
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

func (p *ReindexProvider) failUnit(
	ctx context.Context,
	task *distributedtask.Task,
	unitID string,
	recorder distributedtask.TaskCompletionRecorder,
	errMsg string,
) {
	p.logger.WithField("taskID", task.ID).WithField("unit", unitID).
		Error("reindex provider: unit failed: " + errMsg)

	if err := recorder.RecordDistributedTaskUnitFailure(
		ctx, task.Namespace, task.ID, task.Version, p.localNode, unitID, errMsg,
	); err != nil {
		p.logger.WithError(err).Error("reindex provider: failed to record unit failure")
	}
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
	p.mu.Lock()
	payload := p.payloads[task.TaskDescriptor]
	p.mu.Unlock()

	logger := p.logger.WithField("taskID", task.ID).WithField("groupID", groupID).
		WithField("localGroupUnitIDs", localGroupUnitIDs)

	if payload == nil {
		// Payload not cached — this can happen if the node was restarted after
		// reindex completed but before the group callback fired. Deserialize
		// from the task.
		var pl ReindexTaskPayload
		if err := json.Unmarshal(task.Payload, &pl); err != nil {
			logger.WithError(err).Error("reindex provider: OnGroupCompleted: failed to unmarshal payload")
			return
		}
		payload = &pl
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

	// Run the swap phase on each local shard.
	ctx := context.Background()
	for _, unitID := range localGroupUnitIDs {
		// Skip units that failed during reindex.
		unit := task.Units[unitID]
		if unit != nil && unit.Status == distributedtask.UnitStatusFailed {
			logger.WithField("unit", unitID).Warn("reindex provider: skipping swap for failed unit")
			continue
		}

		shardName := payload.UnitToShard[unitID]
		var shard ShardLike
		if err := idx.ForEachShard(func(name string, s ShardLike) error {
			if name == shardName {
				shard = s
			}
			return nil
		}); err != nil {
			logger.WithField("unit", unitID).WithError(err).
				Error("reindex provider: OnGroupCompleted: iterating shards")
			continue
		}
		if shard == nil {
			logger.WithField("unit", unitID).WithField("shard", shardName).
				Error("reindex provider: OnGroupCompleted: shard not found")
			continue
		}

		// Retrieve the cached task instances that ran RunReindexOnlyOnShard.
		// These have the double-write callbacks registered; creating new
		// instances would lose them.
		p.mu.Lock()
		unitTasks := p.reindexTasks[task.TaskDescriptor][unitID]
		p.mu.Unlock()

		if len(unitTasks) == 0 {
			// Fallback: node was restarted after reindex but before swap.
			// Create new tasks — callbacks won't be registered but the swap
			// can still complete if the reindex data is on disk.
			var err error
			unitTasks, err = p.createReindexTasks(payload)
			if err != nil {
				logger.WithField("unit", unitID).WithError(err).
					Error("reindex provider: OnGroupCompleted: creating reindex tasks")
				continue
			}
			logger.WithField("unit", unitID).
				Info("reindex provider: OnGroupCompleted: using freshly created tasks (node likely restarted)")
		}

		for _, reindexTask := range unitTasks {
			if err := reindexTask.RunSwapOnShard(ctx, shard); err != nil {
				logger.WithField("unit", unitID).WithField("task", reindexTask.Name()).
					WithError(err).Error("reindex provider: OnGroupCompleted: RunSwapOnShard failed")
			}
		}
		logger.WithField("unit", unitID).WithField("shard", shardName).
			Info("reindex provider: swap complete")
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
// Currently only change-tokenization qualifies: it replaces the searchable
// and filterable bucket content, so partial swap would serve mixed
// old/new tokenization across shards.
//
// enable-rangeable is NOT semantic because it adds a new bucket alongside
// the existing one without changing existing bucket content.
func isSemanticMigration(mt ReindexMigrationType) bool {
	return mt == ReindexTypeChangeTokenization
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
