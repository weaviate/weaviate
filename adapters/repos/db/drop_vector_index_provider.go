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

// maxVerifiedStillDroppedEntries bounds the OnGroupCompleted verify memo; see
// memoizedTargetsStillDropped's eviction comment.
const maxVerifiedStillDroppedEntries = 512

// maxConsecutivePollErrors tolerates transient pending-read blips before failing
// the unit, so a momentary I/O error doesn't flip the whole task to FAILED.
const maxConsecutivePollErrors = 3

// dropVectorRetainVerdictTTL bounds the retainer's leader reads: all of one
// drop's retained records share a single marker check per window instead of
// paying one QueryReadOnlyClasses per record per scheduler tick.
const dropVectorRetainVerdictTTL = 30 * time.Second

// maxRetainVerdictEntries bounds the retain-verdict memo (one entry per drop
// with retained-expired records; see retainVerdictForDrop).
const maxRetainVerdictEntries = 512

// retainVerdict is a memoized ShouldRetainCompletedTask marker check.
type retainVerdict struct {
	retain bool
	at     time.Time
}

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

	// verifiedStillDropped memoizes targetsStillDropped for OnGroupCompleted:
	// one callback fires per tenant (GroupID == shardName), each costing a
	// leader-consistent schema read without the memo. Keyed by task ID alone —
	// task.Version is written once at AddTask and never changes, so it cannot
	// key invalidation. A hit can still never be stale while the entry lives:
	// group callbacks are one-shot per process and a restart starts with an
	// empty memo, the marker cannot be removed while the task is active (only
	// the task's own SWAPPING finalize passes the removal gate, and SWAPPING
	// postdates every group callback), and a re-drop introduction is refused
	// while the task is active. OnTaskCompleted evicts the entry — CleanupTask
	// also evicts but is a dead path for this provider, whose GetLocalTasks
	// returns nil.
	verifiedMu           sync.Mutex
	verifiedStillDropped map[string]bool // task ID -> every target still marked dropped

	// retainVerdicts memoizes the retainer's marker check per drop
	// (collection+targets): the TTL sweep asks per retained record per tick,
	// and every record of one drop shares the answer. Time-bounded
	// (dropVectorRetainVerdictTTL) so a finalize is observed within a window.
	retainVerdictMu sync.Mutex
	retainVerdicts  map[string]retainVerdict

	// reconcileNudge pokes the reconcile loop (leader-gated there) when a
	// round ends with work remaining — a completed round that deferred over
	// uncovered shards (batch chains), or a failed one (e.g. tenant
	// deactivated mid-strip). Without it, follow-up rounds idle up to a full
	// reconcile interval apart. Nil-safe; set once at startup.
	reconcileNudge func()
}

// NewDropVectorIndexProvider builds the provider. localNode filters units to the
// ones this node owns; serverCtx bounds the background poll loops.
// reconcileNudge (nil-safe) wakes the reconcile loop when a completion leaves
// work behind; it must be non-blocking — called from task-completion callbacks.
func NewDropVectorIndexProvider(
	shards dropVectorShards,
	schema dropVectorSchemaFinalizer,
	sharding dropVectorSchemaReader,
	logger logrus.FieldLogger,
	localNode string,
	serverCtx context.Context,
	reconcileNudge func(),
) *DropVectorIndexProvider {
	return &DropVectorIndexProvider{
		shards:               shards,
		schema:               schema,
		sharding:             sharding,
		logger:               logger,
		localNode:            localNode,
		serverCtx:            serverCtx,
		reconcileNudge:       reconcileNudge,
		pollInterval:         defaultDropVectorPollInterval,
		verifyRetryBackoff:   2 * time.Second,
		verifiedStillDropped: map[string]bool{},
		retainVerdicts:       map[string]retainVerdict{},
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

// CleanupTask drops the task's verify memo; all other per-task state is owned
// elsewhere (the scheduler owns the task handle, the bucket the edit-ops).
// NOTE: the scheduler only calls CleanupTask for descriptors reported by
// GetLocalTasks, which this provider returns nil from — so in production the
// memo is evicted by OnTaskCompleted instead; this stays as a belt should
// GetLocalTasks ever report tasks.
func (p *DropVectorIndexProvider) CleanupTask(desc distributedtask.TaskDescriptor) error {
	p.evictStillDroppedMemo(desc.ID)
	return nil
}

func (p *DropVectorIndexProvider) nudgeReconcile() {
	if p.reconcileNudge != nil {
		p.reconcileNudge()
	}
}

func (p *DropVectorIndexProvider) evictStillDroppedMemo(taskID string) {
	p.verifiedMu.Lock()
	defer p.verifiedMu.Unlock()
	delete(p.verifiedStillDropped, taskID)
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
	stillDropped, verifyErr := p.targetsStillDroppedWithRetry(ctx, payload)
	if verifyErr != nil {
		if ctx.Err() != nil {
			return // shutdown: leave units in place, the task resumes after restart
		}
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
	shardGone := func() bool {
		return !p.shardLocallyLoaded(payload.Collection, payload.UnitToShard[unitID])
	}
	if err := p.pollUntilEmpty(ctx, bucket, task, unitID, payload.OpID, shardGone); err != nil {
		if ctx.Err() != nil {
			return // shutdown: resume after restart, do not mark failed
		}
		msg := "drain pending segments: " + err.Error()
		// Tenant lifecycle is decoupled from cleanups (see CheckTenantMutation):
		// a mid-strip deactivation surfaces here as persistent read errors on
		// the closed sidecar. Name the real cause — this failure is an expected
		// hand-off to the next reconcile round, not a fault.
		if shard := payload.UnitToShard[unitID]; !p.shardLocallyLoaded(payload.Collection, shard) {
			msg += " (shard no longer locally available — tenant deactivated, offloaded, or deleted; " +
				"reconciliation re-covers the remaining shards and the tenant on reactivation)"
		}
		p.failUnit(ctx, task, unitID, msg)
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
	unitID, opID string, shardGone func() bool,
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
			// A read error on a shard that is no longer locally loaded is not a
			// blip — the tenant deactivated/offloaded/deleted mid-drain (tenant
			// lifecycle is decoupled from cleanups). Fail the unit NOW instead
			// of burning the blip tolerance: the round ends and reconciliation
			// re-covers the surviving shards without multi-tick delay.
			if shardGone() {
				return fmt.Errorf("shard no longer locally loaded: %w", err)
			}
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

// ShouldRetainCompletedTask implements distributedtask.CompletedTaskRetainer:
// while a drop's marker still stands (finalize deferred over inactive
// tenants), its load-bearing terminal records outlive the TTL — a re-drop's
// marker-introduction purge deletes them anyway. Retained are:
//   - Coverage carriers: completed (SWAPPING/FINISHED) records feed their full
//     CoveredShards, FAILED/CANCELLED ones their COMPLETED units; expiring
//     either forces a full re-clean — with a permanently inactive (e.g.
//     offloaded) tenant, repeating every TTL, forever. Their count is bounded:
//     each adds newly covered shards to the chain.
//   - The NEWEST matching record, whatever its unit count: it anchors the op's
//     liveness (LiveOpIDs keeps a terminal round's op from being swept on
//     shard load) and the epoch identity (EpochAndInheritedCoverage inherits
//     from the newest record; a fresh epoch is a fresh op). With no record
//     left, a reactivated tenant sweeps its own resume point and the strip
//     restarts from scratch.
//
// OLDER zero-unit records feed nothing and are released — that bound is what
// keeps a fast-failing round loop (one FAILED record minted per nudge) from
// accumulating RAFT-replicated, snapshot-resident records without limit:
// every new round's record frees its predecessor. The marker check is
// memoized per drop for dropVectorRetainVerdictTTL — the sweep re-asks once
// per retained record per tick, and all of one drop's records share the
// answer; a leader-read failure retains (deletion is the irreversible
// direction; re-evaluated next window).
func (p *DropVectorIndexProvider) ShouldRetainCompletedTask(task *distributedtask.Task,
	namespaceTasks map[distributedtask.TaskDescriptor]*distributedtask.Task,
) bool {
	payload, err := decodeDropVectorIndexPayload(task.Payload)
	if err != nil {
		return false // unparseable records cannot feed coverage, epoch, or liveness
	}
	switch {
	case task.Status.IsCompleted():
	case terminalWithPartialWork(task.Status) && len(CompletedUnitShards(task, payload)) > 0:
	case newestMatchingRecord(task, payload, namespaceTasks):
	default:
		return false
	}
	return p.retainVerdictForDrop(payload)
}

// newestMatchingRecord reports whether task carries the highest raft Version
// among namespaceTasks records for its (collection, targets) — the record
// EpochAndInheritedCoverage inherits the epoch from. Matching mirrors that
// function; undecodable siblings are skipped, so they cannot suppress an
// anchor.
func newestMatchingRecord(task *distributedtask.Task, payload *DropVectorIndexTaskPayload,
	namespaceTasks map[distributedtask.TaskDescriptor]*distributedtask.Task,
) bool {
	for _, other := range namespaceTasks {
		if other.Version <= task.Version {
			continue
		}
		otherPayload, err := decodeDropVectorIndexPayload(other.Payload)
		if err != nil {
			continue
		}
		if strings.EqualFold(otherPayload.Collection, payload.Collection) &&
			SameTargetSet(otherPayload.Targets, payload.Targets) {
			return false
		}
	}
	return true
}

// retainVerdictForDrop answers "retain records of this drop?" (targets still
// marked dropped) from a short-lived per-drop memo, so one leader read per
// window serves every retained record of the drop instead of one per record
// per scheduler tick. Errors memoize as retain — N records must not fan out
// into N serial RPCs against a partitioned leader.
func (p *DropVectorIndexProvider) retainVerdictForDrop(payload *DropVectorIndexTaskPayload) bool {
	key := retainVerdictKey(payload)
	now := time.Now()
	memo, ok := func() (bool, bool) {
		p.retainVerdictMu.Lock()
		defer p.retainVerdictMu.Unlock()
		v, ok := p.retainVerdicts[key]
		return v.retain, ok && now.Sub(v.at) < dropVectorRetainVerdictTTL
	}()
	if ok {
		return memo
	}

	retain := true // leader-read failure retains
	if stillDropped, err := p.targetsStillDropped(payload); err == nil {
		retain = stillDropped
	}

	func() {
		p.retainVerdictMu.Lock()
		defer p.retainVerdictMu.Unlock()
		// Bounded like verifiedStillDropped: arbitrary eviction is safe, a
		// miss costs one leader re-read.
		if len(p.retainVerdicts) >= maxRetainVerdictEntries {
			for k := range p.retainVerdicts {
				delete(p.retainVerdicts, k)
				break
			}
		}
		p.retainVerdicts[key] = retainVerdict{retain: retain, at: now}
	}()
	return retain
}

func retainVerdictKey(payload *DropVectorIndexTaskPayload) string {
	targets := append([]string(nil), payload.Targets...)
	sort.Strings(targets)
	return payload.Collection + "\x00" + strings.Join(targets, "\x00")
}

// shardLocallyLoaded reports whether the shard is currently loaded on this
// node. Diagnostic only (enriches a unit-failure reason); errors read as
// "loaded" so a listing blip cannot mislabel a real drain failure.
func (p *DropVectorIndexProvider) shardLocallyLoaded(collection, shardName string) bool {
	buckets, err := p.shards.EditOpBucketsForLoadedShards(collection, []string{shardName})
	if err != nil {
		return true
	}
	_, ok := buckets[shardName]
	return ok
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
	// Restart-replay guard: this callback re-fires for up to the task TTL, and
	// the target may have been finalized and RE-CREATED since — removing files
	// then would delete the live index. Same verify (and same leader-blip
	// retry) processUnits runs before arming; an error after the retries fails
	// the group's ack, and a restart replays the callback. Memoized per task
	// version: one leader read serves every tenant's replayed callback.
	stillDropped, err := p.memoizedTargetsStillDropped(task, payload)
	if err != nil {
		return fmt.Errorf("verify targets still dropped: %w", err)
	}
	if !stillDropped {
		p.logger.WithField("task", task.ID).WithField("collection", payload.Collection).
			Info("drop-vector: group completion: target no longer marked dropped (finalized or re-created); skipping file removal")
		return nil
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
//
// Transient failures on the SWAPPING path (op delete, coverage read,
// active-drop read, the finalize write) return the error so the scheduler
// withholds FINISHED and retries this callback — bounded; exhaustion fails
// the task, which is safe: the marker stays, the FAILED completion disarms
// its completed units and keeps the rest as resume points, and
// reconciliation re-covers. Acking such a failure instead would mint a
// FINISHED record with complete coverage next to a standing marker, which
// the enqueuer reads as closed-epoch residue: a full re-clean per reconcile
// round. Designed deferrals (uncovered shards, a newer overlapping drop,
// replayed FINISHED) return nil. See
// [distributedtask.UnitAwareProvider.OnTaskCompleted].
func (p *DropVectorIndexProvider) OnTaskCompleted(task *distributedtask.Task) error {
	// The group-callback phase is over on every path that reaches this
	// callback, so its verify memo can go (see verifiedStillDropped for why
	// CleanupTask cannot be the eviction site).
	p.evictStillDroppedMemo(task.ID)

	payload, err := decodeDropVectorIndexPayload(task.Payload)
	if err != nil {
		p.logger.WithField("task", task.ID).Errorf("drop-vector: task-completion: decode payload: %v", err)
		return nil
	}
	logger := p.logger.WithField("task", task.ID).WithField("collection", payload.Collection)

	switch task.Status {
	case distributedtask.TaskStatusSwapping, distributedtask.TaskStatusFinished:
		// Success — proceed.
	default:
		// FAILED/CANCELLED: the schema marker stays, and the edit ops of
		// units that did NOT complete stay with it — their pending sets are
		// the resume point for the next round. Op identity is the drop epoch,
		// so the re-enqueued round re-arms the SAME op (idempotently) and
		// drains only what this round left unstripped. The op cannot outlive
		// its purpose: liveness keys on the marker, so the sweep collects it
		// once the marker leaves the schema, and a re-drop of a re-created name runs
		// under a fresh epoch (the marker-introduction purge guarantees a
		// clean record slate), whose own op never collides with this one.
		//
		// COMPLETED units are the exception: their shards enter the epoch's
		// inherited coverage (this record is retained while the marker
		// stands), so no later round re-arms them — disarm now, or every
		// future replace compaction pays a full object decode for nothing,
		// indefinitely if a cold tenant holds the marker open.
		p.deleteCompletedUnitEditOps(task, payload)
		logger.WithField("status", task.Status).
			Info("drop-vector: task-completion: task did not succeed; incomplete units' edit ops kept as the next round's resume point, schema marker stays")
		p.nudgeReconcile()
		return nil
	}

	// Delete the op before removing the schema entry so "schema entry removed ⇒
	// no op armed by THIS task on a loaded shard" holds (see DeleteEditOp for
	// why a lingering op is unsafe). Ops disarmed best-effort by an earlier
	// FAILED round of the same epoch may still linger on shards this task
	// inherited as cleaned; the dropped-target fence keeps them inert once
	// the marker is gone, and the orphan sweep collects them. On failure, defer the
	// schema removal for reconciliation rather than break it.
	if err := p.deleteLocalEditOps(payload); err != nil {
		// Non-nil return withholds FINISHED and the scheduler retries this
		// callback (bounded; exhaustion fails the task — safe: the marker
		// stays and reconciliation re-covers, resuming from the recorded
		// pending sets). Acking here instead would mint a FINISHED record
		// next to a standing marker, which the enqueuer must read as a
		// closed epoch: a full re-clean for what is usually a transient blip.
		return fmt.Errorf("deleting completed edit op: %w", err)
	}

	// Only the live completion (SWAPPING) finalizes; the FSM gate enforces the
	// same rule. A replayed FINISHED completion may belong to a drop that
	// already finalized and whose name was re-created — reconciliation heals a
	// genuinely missed finalize with a fresh-epoch re-clean instead. Checked
	// before the coverage and active-drop reads below: those are two
	// leader-consistent RPCs, and a replay pays them on every restart within
	// the task TTL just to discard the answers here.
	//
	// Known bounded race: a peer whose OnTaskCompleted early-returned (this
	// callback always acks) can flip the task to FINISHED before THIS node's
	// removal lands, so this check — or the FSM gate — rejects a legitimate
	// finalize. Reconciliation heals it, but not cheaply: the record being
	// FINISHED, the closed-epoch fence mints a fresh epoch and re-cleans every
	// shard, a full segment rewrite of the collection. The same full re-clean
	// recurs when a completed record expires (CompletedTaskTTL, default 5
	// days) while a never-activated tenant holds the marker open, because
	// coverage inheritance restarts from zero.
	if task.Status != distributedtask.TaskStatusSwapping {
		logger.WithField("status", task.Status).
			Info("drop-vector: task-completion replay: not SWAPPING; leaving the marker to reconciliation")
		return nil
	}

	// Only remove the schema entry once THIS task covered every current shard.
	// A tenant that was inactive at enqueue (or created since) has no unit here;
	// removing the marker would strand its data — the marker is what activation
	// cleanup and re-enqueue key off — and free the name while stale files linger.
	// The marker stays until a later task covers everyone.
	uncovered, err := p.uncoveredShards(payload)
	if err != nil {
		// Retry via the scheduler (see the delete-op failure above): acking a
		// leader-read blip would cost a full re-clean.
		return fmt.Errorf("coverage check: %w", err)
	}
	if len(uncovered) > 0 {
		// Count + sample only: on a large MT collection the full list is a
		// multi-MB log line of tenant names.
		logger.WithField("uncoveredCount", len(uncovered)).
			WithField("sample", uncovered[:min(len(uncovered), 10)]).
			Info("drop-vector: task-completion: shards not covered by this task (inactive at enqueue or created since); " +
				"leaving schema marker — reconciliation re-enqueues once they are active")
		p.nudgeReconcile()
		return nil
	}

	// A replayed completion (LocalCallbacksDone=false) is epoch-blind: this task's
	// finalize must not remove the marker of a NEWER drop of the same name that is
	// still running — that would free the name while the newer op strips it.
	if blocked, err := p.activeOverlappingDrop(task, payload); err != nil {
		// Retry via the scheduler (see the delete-op failure above).
		return fmt.Errorf("active-drop check: %w", err)
	} else if blocked {
		logger.Info("drop-vector: task-completion: a newer drop task on the same target is active; leaving its schema marker")
		return nil
	}

	if err := p.schema.RemoveDroppedVectorConfig(p.serverCtx, payload.Collection, payload.Targets); err != nil {
		// Retry via the scheduler (see the delete-op failure above). Acking a
		// failed finalize write is the worst case of the four: the FINISHED
		// record carries COMPLETE coverage, so every reconcile round would
		// re-read "complete chain + standing marker" as closed-epoch residue
		// and re-strip the whole collection, forever, while retained records
		// accumulate.
		return fmt.Errorf("removing VectorConfig entries: %w", err)
	}
	logger.Info("drop-vector: task-completion: dropped vector(s) removed from schema")
	return nil
}

// activeOverlappingDrop reports whether another ACTIVE drop task overlaps this
// payload's collection+targets.
func (p *DropVectorIndexProvider) activeOverlappingDrop(task *distributedtask.Task, payload *DropVectorIndexTaskPayload) (bool, error) {
	tasks, err := p.sharding.ListDistributedTasks(p.serverCtx)
	if err != nil {
		return false, err
	}
	other, _, _ := FirstActiveOverlappingDrop(
		tasks[DropVectorIndexNamespace], task.ID, payload.Collection, payload.Targets, p.logger)
	return other != nil, nil
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

// targetsStillDroppedWithRetry is targetsStillDropped with the bounded
// leader-blip tolerance every verify site shares: a momentary leader read
// failure must not fail a whole drop task. ctx aborts between attempts.
func (p *DropVectorIndexProvider) targetsStillDroppedWithRetry(
	ctx context.Context, payload *DropVectorIndexTaskPayload,
) (bool, error) {
	var stillDropped bool
	var err error
	for attempt := 0; attempt < maxConsecutivePollErrors; attempt++ {
		stillDropped, err = p.targetsStillDropped(payload)
		if err == nil {
			return stillDropped, nil
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(p.verifyRetryBackoff):
		}
	}
	return false, err
}

// memoizedTargetsStillDropped serves OnGroupCompleted's verify from the
// per-task memo (see verifiedStillDropped for why a hit can never be stale).
// Errors are not memoized.
func (p *DropVectorIndexProvider) memoizedTargetsStillDropped(
	task *distributedtask.Task, payload *DropVectorIndexTaskPayload,
) (bool, error) {
	memo, ok := func() (bool, bool) {
		p.verifiedMu.Lock()
		defer p.verifiedMu.Unlock()
		m, ok := p.verifiedStillDropped[task.ID]
		return m, ok
	}()
	if ok {
		return memo, nil
	}
	stillDropped, err := p.targetsStillDroppedWithRetry(p.serverCtx, payload)
	if err != nil {
		return false, err
	}
	func() {
		p.verifiedMu.Lock()
		defer p.verifiedMu.Unlock()
		// Bounded: OnTaskCompleted never fires for tasks cascade-deleted by
		// DeleteClass mid-drop, so uncapped entries would leak per cycle.
		// Evicting arbitrarily is safe — a miss costs one leader re-read.
		if len(p.verifiedStillDropped) >= maxVerifiedStillDroppedEntries {
			for id := range p.verifiedStillDropped {
				delete(p.verifiedStillDropped, id)
				break
			}
		}
		p.verifiedStillDropped[task.ID] = stillDropped
	}()
	return stillDropped, nil
}

// uncoveredShards returns the collection's current shards (leader-consistent)
// with no unit in this task and no entry in its inherited cleaned-shard set
// (shards cleaned by the same epoch's earlier tasks).
func (p *DropVectorIndexProvider) uncoveredShards(payload *DropVectorIndexTaskPayload) ([]string, error) {
	state, _, err := p.sharding.QueryShardingState(payload.Collection)
	if err != nil {
		return nil, err
	}
	if state == nil {
		return nil, fmt.Errorf("no sharding state for collection %q", payload.Collection)
	}
	shardNames := make([]string, 0, len(state.Physical))
	for shardName := range state.Physical {
		shardNames = append(shardNames, shardName)
	}
	return ShardsNotCovered(shardNames, payload.CoveredShards()), nil
}

// deleteCompletedUnitEditOps disarms the op on this node's loaded shards whose
// units COMPLETED in a terminal (FAILED/CANCELLED) round — those shards are in
// the epoch's inherited coverage, so no later round of this drop touches them,
// and a kept op would only cost decode work on every future compaction.
// Belt: a shard whose op still has pending or quarantined rows is skipped —
// completion implies both are empty, so anything else means the op still
// covers unstripped data and deleting it could resurrect the dropped vector.
// Best-effort: a failure is logged and the op left to the post-finalize orphan
// sweep (the dropped-target fence keeps a lingering op inert once the marker
// falls and the name is re-created).
func (p *DropVectorIndexProvider) deleteCompletedUnitEditOps(
	task *distributedtask.Task, payload *DropVectorIndexTaskPayload,
) {
	var shardNames []string
	for unitID, node := range payload.UnitToNode {
		if node != p.localNode {
			continue
		}
		if unit, ok := task.Units[unitID]; !ok || unit.Status != distributedtask.UnitStatusCompleted {
			continue
		}
		shardNames = append(shardNames, payload.UnitToShard[unitID])
	}
	if len(shardNames) == 0 {
		return
	}
	logger := p.logger.WithField("task", task.ID).WithField("collection", payload.Collection)
	buckets, err := p.shards.EditOpBucketsForLoadedShards(payload.Collection, shardNames)
	if err != nil {
		logger.Warnf("drop-vector: task-completion: resolve buckets to disarm completed units: %v", err)
		return
	}
	for _, shardName := range shardNames {
		bucket, ok := buckets[shardName]
		if !ok {
			continue // unloaded; the sweep on its next load disarms it once the marker leaves the schema
		}
		pending, err := bucket.EditOpPending(payload.OpID)
		if err != nil {
			logger.Warnf("drop-vector: task-completion: read pending before disarming shard %q: %v", shardName, err)
			continue
		}
		quarantined, err := bucket.EditOpQuarantined(payload.OpID)
		if err != nil {
			logger.Warnf("drop-vector: task-completion: read quarantine before disarming shard %q: %v", shardName, err)
			continue
		}
		if len(pending) > 0 || len(quarantined) > 0 {
			logger.Warnf("drop-vector: task-completion: unit completed but shard %q still has %d pending / %d quarantined segments; keeping its edit op",
				shardName, len(pending), len(quarantined))
			continue
		}
		if err := bucket.DeleteEditOp(payload.OpID); err != nil {
			logger.Warnf("drop-vector: task-completion: disarm completed unit's edit op on shard %q: %v", shardName, err)
		}
	}
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
