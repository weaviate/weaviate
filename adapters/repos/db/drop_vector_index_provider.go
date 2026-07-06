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
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/modelsext"
	"github.com/weaviate/weaviate/entities/versioned"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// defaultDropVectorPollInterval is how often a running unit polls the edit-ops
// pending set and reports progress while the compaction/cleanup transformer
// drains it.
const defaultDropVectorPollInterval = 30 * time.Second

// maxConsecutivePollErrors tolerates transient pending-read blips before failing
// the unit, so a momentary I/O error doesn't flip the whole task to FAILED.
const maxConsecutivePollErrors = 3

// editOpBucket is the slice of *lsmkv.Bucket the provider drives: register the
// drop op (flush + snapshot) and poll its remaining pending segments. Narrowed
// to an interface so the provider's poll loop is unit-testable.
type editOpBucket interface {
	RegisterEditOp(opID string, desc lsmkv.OpDescriptor) error
	EditOpPending(opID string) ([]string, error)
	DeleteEditOp(opID string) error
}

// dropVectorShards is the slice of *DB the provider needs: locate the edit-ops
// objects buckets for shards (one walk), and (safety net) remove a dropped
// vector index's on-disk files for a shard.
type dropVectorShards interface {
	EditOpBucketsForShards(collection string, shardNames []string) (map[string]editOpBucket, error)
	EnsureDroppedVectorFilesRemoved(collection, shard string, targets []string) error
}

// dropVectorSchemaFinalizer removes the dropped named-vector entries from a
// class's VectorConfig cluster-wide via fresh read-modify-write. Idempotent:
// re-running after the entries are already gone is a no-op.
type dropVectorSchemaFinalizer interface {
	RemoveDroppedVectorConfig(ctx context.Context, collection string, targets []string) error
}

// dropVectorSchemaReader provides leader-consistent schema reads: the sharding
// state (so the finalize path can tell whether this task covered every current
// shard/tenant) and the class (so op-arming can re-verify the targets are still
// marked dropped). cluster.Raft satisfies it.
type dropVectorSchemaReader interface {
	QueryShardingState(class string) (*sharding.State, uint64, error)
	QueryReadOnlyClasses(classes ...string) (map[string]versioned.Class, error)
}

// DropVectorIndexProvider executes drop-vector-index distributed tasks: each
// local unit registers a remove_target_vectors edit op on its shard's objects
// bucket and waits for the compaction/cleanup transformer to strip the dropped
// vectors from every segment; on task completion the named vectors are removed
// from the schema.
type DropVectorIndexProvider struct {
	mu       sync.Mutex
	recorder distributedtask.TaskCompletionRecorder

	shards    dropVectorShards
	schema    dropVectorSchemaFinalizer
	sharding  dropVectorSchemaReader
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
	sharding dropVectorSchemaReader,
	logger logrus.FieldLogger,
	localNode string,
	serverCtx context.Context,
) *DropVectorIndexProvider {
	return &DropVectorIndexProvider{
		shards:         shards,
		schema:         schema,
		sharding:       sharding,
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
	buckets := p.localBuckets(payload, localUnits)
	for _, unitID := range localUnits {
		if ctx.Err() != nil {
			return // shutdown: leave units in place, the task resumes after restart
		}
		if unit, ok := task.Units[unitID]; ok && unit.Status == distributedtask.UnitStatusCompleted {
			continue // already done in a prior run
		}
		p.processOneUnit(ctx, task, payload, unitID, buckets[payload.UnitToShard[unitID]])
	}
}

// localBuckets resolves the objects bucket for every local unit's shard in one
// walk. Returns nil on a lookup error; per-unit handling then fails the units.
func (p *DropVectorIndexProvider) localBuckets(
	payload *DropVectorIndexTaskPayload, localUnits []string,
) map[string]editOpBucket {
	shardNames := make([]string, 0, len(localUnits))
	for _, unitID := range localUnits {
		shardNames = append(shardNames, payload.UnitToShard[unitID])
	}
	buckets, err := p.shards.EditOpBucketsForShards(payload.Collection, shardNames)
	if err != nil {
		p.logger.WithField("collection", payload.Collection).
			Errorf("drop-vector: resolve objects buckets: %v", err)
		return nil
	}
	return buckets
}

func (p *DropVectorIndexProvider) processOneUnit(
	ctx context.Context, task *distributedtask.Task,
	payload *DropVectorIndexTaskPayload, unitID string, bucket editOpBucket,
) {
	logger := p.unitLogger(task, payload, unitID)

	if bucket == nil {
		p.failUnit(ctx, task, unitID, "objects bucket for shard "+payload.UnitToShard[unitID]+" not locally available")
		return
	}

	// Last check before the destructive op arms: the marker commit and the task
	// enqueue are not atomic, so the class may have been deleted+re-created (or the
	// entry otherwise revived) since. Arming against a live vector would strip user
	// data; refuse instead — the failed task deletes its ops and leaves any real
	// marker for a retry.
	if ok, err := p.targetsStillDropped(payload); err != nil {
		p.failUnit(ctx, task, unitID, "verify targets still marked dropped: "+err.Error())
		return
	} else if !ok {
		p.failUnit(ctx, task, unitID, "a target vector is no longer marked dropped; refusing to arm cleanup")
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
// reporting progress each tick. The bucket's own compaction/cleanup transformer
// does the actual rewriting; this only observes the pending set shrink to zero.
func (p *DropVectorIndexProvider) pollUntilEmpty(
	ctx context.Context, bucket editOpBucket, task *distributedtask.Task,
	unitID, opID string,
) error {
	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	total := 0 // baseline from the first successful read; drives progress only
	consecutiveErrors := 0
	for {
		pending, err := bucket.EditOpPending(opID)
		switch {
		case err != nil:
			// Tolerate a few consecutive blips (incl. the first read); only
			// persistent errors fail the unit.
			consecutiveErrors++
			if consecutiveErrors >= maxConsecutivePollErrors {
				return fmt.Errorf("read pending after %d consecutive errors: %w", consecutiveErrors, err)
			}
			p.unitLogger(task, nil, unitID).Warnf("drop-vector: pending read failed (%d/%d), retrying: %v",
				consecutiveErrors, maxConsecutivePollErrors, err)
		case len(pending) == 0:
			return nil
		default:
			consecutiveErrors = 0
			if total == 0 {
				total = len(pending)
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

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
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

// OnGroupCompleted is the per-tenant file-removal safety net: the marker apply
// already removes a dropped index's files on active shards; this catches shards
// unavailable then (e.g. a later-activated frozen tenant). Idempotent.
func (p *DropVectorIndexProvider) OnGroupCompleted(
	task *distributedtask.Task, groupID string, localGroupUnitIDs []string,
) error {
	payload, err := decodeDropVectorIndexPayload(task.Payload)
	if err != nil {
		return err
	}
	// Accumulate instead of aborting so one failing shard can't block the file
	// cleanup of every other tenant in the group; the scheduler retries on error.
	var errs []error
	for _, unitID := range localGroupUnitIDs {
		shardName := payload.UnitToShard[unitID]
		if err := p.shards.EnsureDroppedVectorFilesRemoved(payload.Collection, shardName, payload.Targets); err != nil {
			errs = append(errs, fmt.Errorf("shard %q: %w", shardName, err))
		}
	}
	return errors.Join(errs...)
}

// OnSwapRequested is a no-op: drop-vector does not use the PREP/SWAP barrier
// (NeedsPreparationBarrier is false), so this callback never fires for it.
func (p *DropVectorIndexProvider) OnSwapRequested(
	task *distributedtask.Task, groupID string, localGroupUnitIDs []string,
) error {
	return nil
}

// OnTaskCompleted deletes the local edit ops and removes the dropped named
// vectors from the schema once the task succeeded on every node. Success is
// delivered as SWAPPING — a non-barrier task jumps there when its last unit
// completes, and the scheduler finalizes SWAPPING→FINISHED only after this
// callback — or as FINISHED on a node that first observes the task after a peer
// finalized. FAILED/CANCELLED do NOT mutate the schema (the marker stays so an
// operator can retry). It must be safe to invoke more than once:
// LocalCallbacksDone returns false, so the scheduler may replay this after a
// restart. Both steps are idempotent — deleting an absent op and removing
// already-gone schema entries are no-ops.
func (p *DropVectorIndexProvider) OnTaskCompleted(task *distributedtask.Task) {
	payload, err := decodeDropVectorIndexPayload(task.Payload)
	if err != nil {
		p.logger.WithField("task", task.ID).Errorf("drop-vector: task-completion: decode payload: %v", err)
		return
	}
	logger := p.logger.WithField("task", task.ID).WithField("collection", payload.Collection)

	switch task.Status {
	case distributedtask.TaskStatusSwapping, distributedtask.TaskStatusFinished:
		// Success — proceed.
	default:
		// FAILED/CANCELLED: the schema marker stays for an operator retry, but the
		// op must NOT stay armed — a later successful re-drop (fresh op ID) would
		// free the name while this op lingers, and it would strip a re-created
		// same-name vector on the next compaction. Best-effort: an unloaded shard's
		// op is caught by the orphan sweep on load (this task is terminal, so it is
		// absent from the live-op set).
		if err := p.deleteLocalEditOps(payload); err != nil {
			logger.Warnf("drop-vector: task-completion: deleting %s task's edit op failed (orphan sweep on shard load will catch it): %v",
				task.Status, err)
		}
		logger.WithField("status", task.Status).
			Info("drop-vector: task-completion: task did not succeed; edit ops deleted, schema marker left for operator")
		return
	}

	// Delete the op before removing the schema entry so "schema entry removed ⇒ no
	// op on a loaded shard" holds (see DeleteEditOp for why a lingering op is unsafe).
	// On failure, defer the schema removal for reconciliation rather than break it.
	if err := p.deleteLocalEditOps(payload); err != nil {
		logger.Errorf("drop-vector: task-completion: deleting completed edit op failed (reconcile will retry): %v", err)
		return
	}

	// Only remove the schema entry once THIS task covered every current shard.
	// A tenant that was inactive at enqueue (or created since) has no unit here;
	// removing the marker would strand its data — the marker is what activation
	// cleanup and re-enqueue key off — and free the name while stale files linger.
	// The marker stays until a later task covers everyone.
	uncovered, err := p.uncoveredShards(payload)
	if err != nil {
		logger.Errorf("drop-vector: task-completion: coverage check failed (leaving schema marker): %v", err)
		return
	}
	if len(uncovered) > 0 {
		logger.WithField("shards", uncovered).
			Info("drop-vector: task-completion: shards not covered by this task (inactive at enqueue or created since); " +
				"leaving schema marker — reconciliation re-enqueues once they are active")
		return
	}

	if err := p.schema.RemoveDroppedVectorConfig(p.serverCtx, payload.Collection, payload.Targets); err != nil {
		// Leave the marker in place; startup reconciliation retries the removal.
		logger.Errorf("drop-vector: task-completion: removing VectorConfig entries failed: %v", err)
		return
	}
	logger.Info("drop-vector: task-completion: dropped vector(s) removed from schema")
}

// targetsStillDropped reports whether every payload target is still present and
// marked dropped in the leader-consistent class. A missing class or entry means
// the drop was superseded (class deleted/re-created, or the name freed).
func (p *DropVectorIndexProvider) targetsStillDropped(payload *DropVectorIndexTaskPayload) (bool, error) {
	vclasses, err := p.sharding.QueryReadOnlyClasses(payload.Collection)
	if err != nil {
		return false, err
	}
	class := vclasses[payload.Collection].Class
	if class == nil {
		return false, nil
	}
	for _, target := range payload.Targets {
		dropped := false
		for name, cfg := range class.VectorConfig {
			if strings.EqualFold(name, target) && modelsext.IsVectorIndexDropped(cfg) {
				dropped = true
				break
			}
		}
		if !dropped {
			return false, nil
		}
	}
	return true, nil
}

// uncoveredShards returns the collection's current shards (leader-consistent)
// that had no unit in this task.
func (p *DropVectorIndexProvider) uncoveredShards(payload *DropVectorIndexTaskPayload) ([]string, error) {
	state, _, err := p.sharding.QueryShardingState(payload.Collection)
	if err != nil {
		return nil, err
	}
	if state == nil {
		return nil, fmt.Errorf("no sharding state for collection %q", payload.Collection)
	}
	covered := make(map[string]struct{}, len(payload.UnitToShard))
	for _, shardName := range payload.UnitToShard {
		covered[shardName] = struct{}{}
	}
	var uncovered []string
	for shardName := range state.Physical {
		if _, ok := covered[shardName]; !ok {
			uncovered = append(uncovered, shardName)
		}
	}
	sort.Strings(uncovered)
	return uncovered, nil
}

// deleteLocalEditOps removes the finished op from each local shard's sidecar,
// returning an error if any shard's op can't be deleted (delete failure, or an
// unloaded shard). That defers the schema removal: freeing the name while an op
// lingers in an unloaded shard would let a later reactivation re-arm it and strip
// the re-created vector.
func (p *DropVectorIndexProvider) deleteLocalEditOps(payload *DropVectorIndexTaskPayload) error {
	var shardNames []string
	for unitID, node := range payload.UnitToNode {
		if node == p.localNode {
			shardNames = append(shardNames, payload.UnitToShard[unitID])
		}
	}
	buckets, err := p.shards.EditOpBucketsForShards(payload.Collection, shardNames)
	if err != nil {
		return fmt.Errorf("resolve buckets to delete edit op (deferring finalize): %w", err)
	}
	for _, shardName := range shardNames {
		bucket, ok := buckets[shardName]
		if !ok {
			return fmt.Errorf("shard %q not locally available to delete edit op (deferring finalize)", shardName)
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
