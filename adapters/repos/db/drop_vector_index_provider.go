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
	EditOpQuarantined(opID string) ([]string, error)
	DeleteEditOp(opID string) error
}

// dropVectorShards is the slice of *DB the provider needs: locate the edit-ops
// objects buckets for shards (one walk), and (safety net) remove a dropped
// vector index's on-disk files for a shard.
type dropVectorShards interface {
	// EditOpBucketsForShards loads shards as needed (lazy shards included); used to arm ops.
	EditOpBucketsForShards(ctx context.Context, collection string, shardNames []string) (map[string]editOpBucket, error)
	// EditOpBucketsForLoadedShards never loads a shard; used by completion-time op
	// deletes so replayed callbacks can't mass-load inactive shards.
	EditOpBucketsForLoadedShards(collection string, shardNames []string) (map[string]editOpBucket, error)
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
	ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error)
}

// DropVectorIndexProvider executes drop-vector-index distributed tasks: each
// local unit registers a remove_target_vectors edit op on its shard's objects
// bucket and waits for the compaction/cleanup transformer to strip the dropped
// vectors from every segment; on task completion the named vectors are removed
// from the schema.
type DropVectorIndexProvider struct {
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
	// verifyRetryBackoff spaces the arm-time verify retries (leader-read blips);
	// overridable in tests.
	verifyRetryBackoff time.Duration
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
		shards:             shards,
		schema:             schema,
		sharding:           sharding,
		logger:             logger,
		localNode:          localNode,
		serverCtx:          serverCtx,
		pollInterval:       defaultDropVectorPollInterval,
		verifyRetryBackoff: 2 * time.Second,
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

// CleanupTask is a no-op: the provider keeps no per-task local state (the
// scheduler owns the task handle; edit-ops bookkeeping is owned by the bucket).
func (p *DropVectorIndexProvider) CleanupTask(desc distributedtask.TaskDescriptor) error {
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

	enterrors.GoWrapper(func() {
		defer func() {
			cancel() // release the serverCtx child on normal completion, not only on Terminate
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
	buckets := p.localBuckets(ctx, payload, localUnits)

	pending := make([]string, 0, len(localUnits))
	for _, unitID := range localUnits {
		if unit, ok := task.Units[unitID]; ok && unit.Status == distributedtask.UnitStatusCompleted {
			continue // already done in a prior run
		}
		pending = append(pending, unitID)
	}

	// Last check before the destructive ops arm: the marker commit and the task
	// enqueue are not atomic, so the class may have been deleted+re-created (or the
	// entry otherwise revived) since. Arming against a live vector would strip user
	// data; refuse instead — the failed task deletes its ops and leaves any real
	// marker for a retry. The leader read gets the same bounded tolerance as the
	// drain poll: a momentary leader blip must not fail a whole drop task.
	var stillDropped bool
	var verifyErr error
	for attempt := 0; attempt < maxConsecutivePollErrors; attempt++ {
		stillDropped, verifyErr = p.targetsStillDropped(payload)
		if verifyErr == nil {
			break
		}
		select {
		case <-ctx.Done():
			return // shutdown: leave units in place, the task resumes after restart
		case <-time.After(p.verifyRetryBackoff):
		}
	}
	if verifyErr != nil {
		for _, unitID := range pending {
			p.failUnit(ctx, task, unitID, "verify targets still marked dropped: "+verifyErr.Error())
		}
		return
	}
	if !stillDropped {
		for _, unitID := range pending {
			p.failUnit(ctx, task, unitID, "a target vector is no longer marked dropped; refusing to arm cleanup")
		}
		return
	}

	// Arm every unit up front so all shards' compaction/cleanup cycles
	// drain concurrently — arming lazily while polling unit-by-unit would
	// serialize days of rewrite work on multi-tenant nodes.
	armed := make([]string, 0, len(pending))
	for _, unitID := range pending {
		if ctx.Err() != nil {
			return // shutdown: leave units in place, the task resumes after restart
		}
		if p.armUnit(ctx, task, payload, unitID, buckets[payload.UnitToShard[unitID]]) {
			armed = append(armed, unitID)
		}
	}

	// Then watch the pending sets drain.
	for _, unitID := range armed {
		if ctx.Err() != nil {
			return
		}
		p.drainUnit(ctx, task, payload, unitID, buckets[payload.UnitToShard[unitID]])
	}
}

// localBuckets resolves the objects bucket for every local unit's shard in one
// walk. Returns nil on a lookup error; per-unit handling then fails the units.
func (p *DropVectorIndexProvider) localBuckets(
	ctx context.Context, payload *DropVectorIndexTaskPayload, localUnits []string,
) map[string]editOpBucket {
	shardNames := make([]string, 0, len(localUnits))
	for _, unitID := range localUnits {
		shardNames = append(shardNames, payload.UnitToShard[unitID])
	}
	buckets, err := p.shards.EditOpBucketsForShards(ctx, payload.Collection, shardNames)
	if err != nil {
		p.logger.WithField("collection", payload.Collection).
			Errorf("drop-vector: resolve objects buckets: %v", err)
		return nil
	}
	return buckets
}

// armUnit registers the edit op on the unit's shard, reporting whether the unit
// is armed and should be drained.
func (p *DropVectorIndexProvider) armUnit(
	ctx context.Context, task *distributedtask.Task,
	payload *DropVectorIndexTaskPayload, unitID string, bucket editOpBucket,
) bool {
	logger := p.unitLogger(task, payload, unitID)

	if bucket == nil {
		p.failUnit(ctx, task, unitID, "objects bucket for shard "+payload.UnitToShard[unitID]+" not locally available")
		return false
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
		return false
	}
	return true
}

// drainUnit waits for the unit's pending set to empty and records completion.
func (p *DropVectorIndexProvider) drainUnit(
	ctx context.Context, task *distributedtask.Task,
	payload *DropVectorIndexTaskPayload, unitID string, bucket editOpBucket,
) {
	if err := p.pollUntilEmpty(ctx, bucket, task, unitID, payload.OpID); err != nil {
		if ctx.Err() != nil {
			return // shutdown: resume after restart, do not mark failed
		}
		p.failUnit(ctx, task, unitID, "drain pending segments: "+err.Error())
		return
	}

	if err := p.recorder.RecordDistributedTaskUnitCompletion(ctx, task.Namespace, task.ID,
		task.Version, p.localNode, unitID); err != nil {
		p.unitLogger(task, payload, unitID).Errorf("drop-vector: record unit completion failed: %v", err)
	}
}

// pollUntilEmpty waits until the op has no pending segments on the bucket,
// reporting progress each tick. The bucket's own compaction/cleanup transformer
// does the actual rewriting; this only observes the pending set shrink to zero.
// A quarantined segment fails the unit instead: it left the pending set
// unstripped, so empty pending with a quarantine row is not success.
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
		if err == nil {
			// The quarantine read shares the blip tolerance below.
			var quarantined []string
			if quarantined, err = bucket.EditOpQuarantined(opID); err == nil && len(quarantined) > 0 {
				return fmt.Errorf("cleanup quarantined %d segment(s) after exhausting the retry budget: %v",
					len(quarantined), quarantined)
			}
		}
		switch {
		case err != nil:
			// Tolerate a few consecutive blips (incl. the first read); only
			// persistent errors fail the unit.
			consecutiveErrors++
			if consecutiveErrors >= maxConsecutivePollErrors {
				return fmt.Errorf("read pending/quarantine set after %d consecutive errors: %w", consecutiveErrors, err)
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
	// cleanup of every other tenant in the group. The joined error still fails the
	// group's completion ack (there is no in-process retry; a restart replays the
	// callback — LocalCallbacksDone is false).
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

	// A replayed completion (LocalCallbacksDone=false) is epoch-blind: this task's
	// finalize must not remove the marker of a NEWER drop of the same name that is
	// still running — that would free the name while the newer op strips it.
	if blocked, err := p.activeOverlappingDrop(task, payload); err != nil {
		logger.Errorf("drop-vector: task-completion: active-drop check failed (leaving schema marker): %v", err)
		return
	} else if blocked {
		logger.Info("drop-vector: task-completion: a newer drop task on the same target is active; leaving its schema marker")
		return
	}

	if err := p.schema.RemoveDroppedVectorConfig(p.serverCtx, payload.Collection, payload.Targets); err != nil {
		// Leave the marker in place; startup reconciliation retries the removal.
		logger.Errorf("drop-vector: task-completion: removing VectorConfig entries failed: %v", err)
		return
	}
	logger.Info("drop-vector: task-completion: dropped vector(s) removed from schema")
}

// activeOverlappingDrop reports whether another ACTIVE drop task overlaps this
// payload's collection+targets.
func (p *DropVectorIndexProvider) activeOverlappingDrop(task *distributedtask.Task, payload *DropVectorIndexTaskPayload) (bool, error) {
	tasks, err := p.sharding.ListDistributedTasks(p.serverCtx)
	if err != nil {
		return false, err
	}
	for _, other := range tasks[DropVectorIndexNamespace] {
		if other.ID == task.ID || !other.Status.IsActive() {
			continue
		}
		otherP, err := decodeDropVectorIndexPayload(other.Payload)
		if err != nil {
			continue
		}
		if !strings.EqualFold(otherP.Collection, payload.Collection) {
			continue
		}
		if len(intersectTargets(otherP.Targets, payload.Targets)) > 0 {
			return true, nil
		}
	}
	return false, nil
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
		cfg, ok := class.VectorConfig[target]
		if !ok || !modelsext.IsVectorIndexDropped(cfg) {
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
	// Loaded shards only: forcing loads here would mass-load inactive shards on
	// every replayed completion callback. An unloaded shard's op is disarmed by the
	// sweep on its next load (this task is then terminal, so absent from the
	// live-op set) and by the periodic cleanup-cycle sweep.
	buckets, err := p.shards.EditOpBucketsForLoadedShards(payload.Collection, shardNames)
	if err != nil {
		return fmt.Errorf("resolve buckets to delete edit op (deferring finalize): %w", err)
	}
	for _, shardName := range shardNames {
		bucket, ok := buckets[shardName]
		if !ok {
			p.logger.WithField("collection", payload.Collection).WithField("shard", shardName).
				Info("drop-vector: shard not loaded for edit-op delete; the sweep on its next load disarms it")
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
