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
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	entschema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/schema"
)

// ReindexProvider implements distributedtask.UnitAwareProvider for reindex tasks.
// It uses the existing ShardReindexTaskGeneric machinery (RunOnShard) to execute
// the actual migration work, with the DTM providing cluster coordination, progress
// tracking, and lifecycle management.
type ReindexProvider struct {
	mu       sync.Mutex
	recorder distributedtask.TaskCompletionRecorder

	db            *DB
	schemaManager *schema.Manager
	logger        logrus.FieldLogger
	localNode     string

	runningHandles map[distributedtask.TaskDescriptor]*reindexTaskHandle
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

	// Run each reindex task on the shard sequentially.
	for _, reindexTask := range tasks {
		if err := reindexTask.RunOnShard(ctx, shard); err != nil {
			p.failUnit(ctx, task, unitID, recorder,
				fmt.Sprintf("RunOnShard (%s): %v", reindexTask.Name(), err))
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

// OnGroupCompleted is a no-op for the initial implementation. RunOnShard handles
// the full lifecycle (reindex + swap + tidy) per shard.
func (p *ReindexProvider) OnGroupCompleted(task *distributedtask.Task, groupID string, localGroupUnitIDs []string) {
	p.logger.WithField("taskID", task.ID).WithField("groupID", groupID).
		WithField("localGroupUnitIDs", localGroupUnitIDs).
		Info("reindex provider: OnGroupCompleted (no-op)")
}

// OnTaskCompleted fires after all units across all nodes are terminal.
// For the initial implementation this is a no-op because RunOnShard already
// calls OnMigrationComplete (schema update) per shard. The DTM adds cluster
// coordination and progress tracking; barrier semantics for semantic migrations
// (defer swap until all shards done) is a follow-up.
func (p *ReindexProvider) OnTaskCompleted(task *distributedtask.Task) {
	p.logger.WithField("taskID", task.ID).WithField("status", task.Status).
		Info("reindex provider: OnTaskCompleted")
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
