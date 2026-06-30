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
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// defaultDropVectorPollInterval is how often a running unit polls the edit-ops
// pending set and reports progress while the compaction/cleanup transformer
// drains it.
const defaultDropVectorPollInterval = 30 * time.Second

// editOpBucket is the slice of *lsmkv.Bucket the provider drives: register the
// drop op (flush + snapshot) and poll its remaining pending segments. Narrowed
// to an interface so the provider's poll loop is unit-testable.
type editOpBucket interface {
	RegisterEditOp(opID string, desc lsmkv.OpDescriptor) error
	EditOpPending(opID string) ([]string, error)
	DeleteEditOp(opID string) error
}

// dropVectorShards is the slice of *DB the provider needs: locate the edit-ops
// objects bucket for a shard, and (safety net) remove a dropped vector index's
// on-disk files for a shard.
type dropVectorShards interface {
	EditOpBucketForShard(collection, shard string) (editOpBucket, error)
	EnsureDroppedVectorFilesRemoved(collection, shard string, targets []string) error
}

// dropVectorSchemaFinalizer removes the dropped named-vector entries from a
// class's VectorConfig cluster-wide via fresh read-modify-write. Idempotent:
// re-running after the entries are already gone is a no-op.
type dropVectorSchemaFinalizer interface {
	RemoveDroppedVectorConfig(ctx context.Context, collection string, targets []string) error
}

// DropVectorIndexProvider executes drop-vector-index distributed tasks: each
// local unit registers a remove_target_vectors edit op on its shard's objects
// bucket and waits for the compaction/cleanup transformer to strip the dropped
// vectors from every segment; on task completion the named vectors are removed
// from the schema. It mirrors ReindexProvider's structure.
type DropVectorIndexProvider struct {
	mu       sync.Mutex
	recorder distributedtask.TaskCompletionRecorder

	shards    dropVectorShards
	schema    dropVectorSchemaFinalizer
	logger    logrus.FieldLogger
	localNode string

	// serverCtx is cancelled on shutdown; per-task contexts derive from it so a
	// graceful shutdown aborts the poll loops (the task resumes after restart).
	serverCtx context.Context

	pollInterval time.Duration

	runningHandles map[distributedtask.TaskDescriptor]*dropVectorTaskHandle
}

// NewDropVectorIndexProvider builds the provider. localNode filters units to the
// ones this node owns; serverCtx bounds the background poll loops.
func NewDropVectorIndexProvider(
	shards dropVectorShards,
	schema dropVectorSchemaFinalizer,
	logger logrus.FieldLogger,
	localNode string,
	serverCtx context.Context,
) *DropVectorIndexProvider {
	return &DropVectorIndexProvider{
		shards:         shards,
		schema:         schema,
		logger:         logger,
		localNode:      localNode,
		serverCtx:      serverCtx,
		pollInterval:   defaultDropVectorPollInterval,
		runningHandles: make(map[distributedtask.TaskDescriptor]*dropVectorTaskHandle),
	}
}

// --- distributedtask.Provider ---

func (p *DropVectorIndexProvider) SetCompletionRecorder(recorder distributedtask.TaskCompletionRecorder) {
	p.recorder = recorder
}

// GetLocalTasks returns nil: drop-vector tasks are discovered from RAFT-replicated
// state, not local on-disk task records.
func (p *DropVectorIndexProvider) GetLocalTasks() []distributedtask.TaskDescriptor {
	return nil
}

// CleanupTask drops the in-memory handle for a task whose local state is being
// torn down. The edit-ops bookkeeping is owned by the bucket, not the provider,
// so there is nothing else to remove here.
func (p *DropVectorIndexProvider) CleanupTask(desc distributedtask.TaskDescriptor) error {
	p.mu.Lock()
	delete(p.runningHandles, desc)
	p.mu.Unlock()
	return nil
}

func (p *DropVectorIndexProvider) StartTask(task *distributedtask.Task) (distributedtask.TaskHandle, error) {
	payload, err := decodeDropVectorIndexPayload(task.Payload)
	if err != nil {
		return nil, err
	}

	var localUnits []string
	for unitID, node := range payload.UnitToNode {
		if node == p.localNode {
			localUnits = append(localUnits, unitID)
		}
	}

	ctx, cancel := context.WithCancel(p.serverCtx)
	handle := &dropVectorTaskHandle{cancel: cancel, doneCh: make(chan struct{})}
	p.mu.Lock()
	p.runningHandles[task.TaskDescriptor] = handle
	p.mu.Unlock()

	enterrors.GoWrapper(func() {
		defer func() {
			p.mu.Lock()
			delete(p.runningHandles, task.TaskDescriptor)
			p.mu.Unlock()
			close(handle.doneCh)
		}()
		p.processUnits(ctx, task, payload, localUnits)
	}, p.logger)

	return handle, nil
}

// processUnits runs every local unit. Units are processed sequentially: the work
// is I/O-light polling (the actual rewriting is done by the compaction/cleanup
// cycles), so there is no benefit to fanning out.
func (p *DropVectorIndexProvider) processUnits(
	ctx context.Context, task *distributedtask.Task,
	payload *DropVectorIndexTaskPayload, localUnits []string,
) {
	for _, unitID := range localUnits {
		if ctx.Err() != nil {
			return // shutdown: leave units in place, the task resumes after restart
		}
		if unit, ok := task.Units[unitID]; ok && unit.Status == distributedtask.UnitStatusCompleted {
			continue // already done in a prior run
		}
		p.processOneUnit(ctx, task, payload, unitID)
	}
}

func (p *DropVectorIndexProvider) processOneUnit(
	ctx context.Context, task *distributedtask.Task,
	payload *DropVectorIndexTaskPayload, unitID string,
) {
	logger := p.unitLogger(task, payload, unitID)

	shardName := payload.UnitToShard[unitID]
	bucket, err := p.shards.EditOpBucketForShard(payload.Collection, shardName)
	if err != nil {
		p.failUnit(ctx, task, unitID, "locate objects bucket: "+err.Error())
		return
	}

	if err := p.recorder.UpdateDistributedTaskUnitProgress(ctx, task.Namespace, task.ID,
		task.Version, p.localNode, unitID, 0); err != nil {
		logger.Warnf("drop-vector: initial progress update failed: %v", err)
	}

	// Idempotent on resume. CreatedAt is per-node local time: the edit-ops store
	// is per-shard and RegisterOp keeps the first value, so it is stable.
	desc := lsmkv.OpDescriptor{
		Type:      lsmkv.OpTypeRemoveTargetVectors,
		Targets:   payload.Targets,
		CreatedAt: time.Now().UnixNano(),
	}
	if err := bucket.RegisterEditOp(payload.OpID, desc); err != nil {
		p.failUnit(ctx, task, unitID, "register drop edit op: "+err.Error())
		return
	}

	if err := p.pollUntilEmpty(ctx, bucket, task, unitID, payload.OpID); err != nil {
		if ctx.Err() != nil {
			return // shutdown: resume after restart, do not mark failed
		}
		p.failUnit(ctx, task, unitID, "drain pending segments: "+err.Error())
		return
	}

	if err := p.recorder.RecordDistributedTaskUnitCompletion(ctx, task.Namespace, task.ID,
		task.Version, p.localNode, unitID); err != nil {
		logger.Errorf("drop-vector: record unit completion failed: %v", err)
	}
}

// pollUntilEmpty waits until the op has no pending segments on the bucket,
// reporting progress each tick. The compaction/cleanup transformer (S6–S10) does
// the actual rewriting; this only observes the pending set shrink to zero.
func (p *DropVectorIndexProvider) pollUntilEmpty(
	ctx context.Context, bucket editOpBucket, task *distributedtask.Task,
	unitID, opID string,
) error {
	pending, err := bucket.EditOpPending(opID)
	if err != nil {
		return err
	}
	total := len(pending)
	if total == 0 {
		return nil
	}

	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			pending, err := bucket.EditOpPending(opID)
			if err != nil {
				return err
			}
			if len(pending) == 0 {
				return nil
			}
			// pending can transiently grow (re-queue), so clamp; completion is gated
			// on len==0, not on this value.
			done := total - len(pending)
			if done < 0 {
				done = 0
			}
			progress := float32(done) / float32(total)
			if err := p.recorder.UpdateDistributedTaskUnitProgress(ctx, task.Namespace,
				task.ID, task.Version, p.localNode, unitID, progress); err != nil {
				p.unitLogger(task, nil, unitID).Warnf("drop-vector: progress update failed: %v", err)
			}
		}
	}
}

func (p *DropVectorIndexProvider) failUnit(
	ctx context.Context, task *distributedtask.Task, unitID, msg string,
) {
	p.unitLogger(task, nil, unitID).Errorf("drop-vector: unit failed: %s", msg)
	if err := p.recorder.RecordDistributedTaskUnitFailure(ctx, task.Namespace, task.ID,
		task.Version, p.localNode, unitID, msg); err != nil {
		p.unitLogger(task, nil, unitID).Errorf("drop-vector: record unit failure failed: %v", err)
	}
}

// --- distributedtask.UnitAwareProvider ---

// OnGroupCompleted is the per-tenant file-removal safety net: Phase 1 already
// removes a dropped index's files on marker apply for active shards; this catches
// shards unavailable then (e.g. a later-activated frozen tenant). Idempotent.
func (p *DropVectorIndexProvider) OnGroupCompleted(
	task *distributedtask.Task, groupID string, localGroupUnitIDs []string,
) error {
	payload, err := decodeDropVectorIndexPayload(task.Payload)
	if err != nil {
		return err
	}
	for _, unitID := range localGroupUnitIDs {
		shardName := payload.UnitToShard[unitID]
		if err := p.shards.EnsureDroppedVectorFilesRemoved(payload.Collection, shardName, payload.Targets); err != nil {
			return err
		}
	}
	return nil
}

// OnSwapRequested is a no-op: drop-vector does not use the PREP/SWAP barrier
// (NeedsPreparationBarrier is false), so this callback never fires for it.
func (p *DropVectorIndexProvider) OnSwapRequested(
	task *distributedtask.Task, groupID string, localGroupUnitIDs []string,
) error {
	return nil
}

// OnTaskCompleted removes the dropped named vectors from the schema once the task
// FINISHED on every node. FAILED/CANCELLED do NOT mutate the schema (the marker
// stays so an operator can retry). Idempotent: a re-invocation after the entries
// are gone is a no-op.
func (p *DropVectorIndexProvider) OnTaskCompleted(task *distributedtask.Task) {
	payload, err := decodeDropVectorIndexPayload(task.Payload)
	if err != nil {
		p.logger.WithField("task", task.ID).Errorf("drop-vector: task-completion: decode payload: %v", err)
		return
	}
	logger := p.logger.WithField("task", task.ID).WithField("collection", payload.Collection)

	if task.Status != distributedtask.TaskStatusFinished {
		logger.WithField("status", task.Status).
			Info("drop-vector: task-completion: not FINISHED, leaving schema marker for operator")
		return
	}

	// Delete the op before removing the schema entry so "schema entry removed ⇒ no
	// op on a loaded shard" holds (see DeleteEditOp for why a lingering op is unsafe).
	// On failure, defer the schema removal for reconciliation rather than break it.
	if err := p.deleteLocalEditOps(payload); err != nil {
		logger.Errorf("drop-vector: task-completion: deleting completed edit op failed (reconcile will retry): %v", err)
		return
	}

	if err := p.schema.RemoveDroppedVectorConfig(p.serverCtx, payload.Collection, payload.Targets); err != nil {
		// Leave the marker for S14 reconciliation to retry; requires the S15/S16
		// FSM gate to allow the real→absent exit transition.
		logger.Errorf("drop-vector: task-completion: removing VectorConfig entries failed: %v", err)
		return
	}
	logger.Info("drop-vector: task-completion: dropped vector(s) removed from schema")
}

// deleteLocalEditOps removes the finished op from each local shard's sidecar. An
// unloaded shard (e.g. a deactivated tenant) is skipped — its cleanup is deferred
// to activation; a delete failure on a loaded shard is returned so the caller
// defers schema removal for reconciliation to retry.
func (p *DropVectorIndexProvider) deleteLocalEditOps(payload *DropVectorIndexTaskPayload) error {
	for unitID, node := range payload.UnitToNode {
		if node != p.localNode {
			continue
		}
		shardName := payload.UnitToShard[unitID]
		bucket, err := p.shards.EditOpBucketForShard(payload.Collection, shardName)
		if err != nil {
			p.logger.WithField("collection", payload.Collection).WithField("shard", shardName).
				Warnf("drop-vector: task-completion: locate bucket to delete edit op (shard not loaded; deferring): %v", err)
			continue
		}
		if err := bucket.DeleteEditOp(payload.OpID); err != nil {
			return fmt.Errorf("delete edit op on shard %q: %w", shardName, err)
		}
	}
	return nil
}

// --- internal ---

func (p *DropVectorIndexProvider) unitLogger(
	task *distributedtask.Task, payload *DropVectorIndexTaskPayload, unitID string,
) logrus.FieldLogger {
	l := p.logger.WithField("task", task.ID).WithField("unit", unitID)
	if payload != nil {
		l = l.WithField("collection", payload.Collection)
	}
	return l
}

// dropVectorTaskHandle implements distributedtask.TaskHandle.
type dropVectorTaskHandle struct {
	cancel context.CancelFunc
	doneCh chan struct{}
}

func (h *dropVectorTaskHandle) Terminate()            { h.cancel() }
func (h *dropVectorTaskHandle) Done() <-chan struct{} { return h.doneCh }
