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
	"fmt"
	"strings"

	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// CheckConflict implements distributedtask.ConflictDetector. Called under the
// Manager lock on the RAFT-apply AddTask path before a new task is stored, it
// rejects (1) a new drop that overlaps an in-flight drop's targets on the same
// collection, and (2) a payload whose inherited CleanedShards claim has no
// surviving source records — the enqueue-to-commit TOCTOU guard.
// FSM-deterministic: a pure function of (newPayload, existingTasks).
func (p *DropVectorIndexProvider) CheckConflict(newPayload []byte, existingTasks []*distributedtask.Task) error {
	newP, err := decodeDropVectorIndexPayload(newPayload)
	if err != nil {
		return fmt.Errorf("unmarshal new drop-vector payload: %w", err)
	}

	if task, existP, overlap := FirstActiveOverlappingDrop(
		existingTasks, "", newP.Collection, newP.Targets, p.logger); task != nil {
		return fmt.Errorf(
			"drop-vector task %q is already in flight on %s for vector(s) %v (status=%s)",
			task.ID, existP.Collection, overlap, task.Status)
	}

	// CleanedShards is a CLAIM of prior cleaning, composed from a leader read
	// that predates this apply (the enqueue is not atomic with it). If the
	// claim's source records are gone by now — a DeleteClass + re-create +
	// re-drop landed in the gap (cascade/purge wiped them), or they expired —
	// the claim belongs to another class generation or a closed epoch, and a
	// task finalizing on it would remove the marker over unstripped shards.
	// Require every claimed shard to be covered by a completed same-epoch
	// record still in the FSM task list; the same matching rules as the
	// enqueuer's inheritance. Deterministic across nodes; a rejected enqueue
	// is retried by reconciliation, which derives coverage afresh.
	if len(newP.CleanedShards) > 0 {
		covered := EpochCoveredShards(existingTasks, newP.Collection, newP.Targets, newP.DropEpochID)
		if missing := ShardsNotCovered(newP.CleanedShards, covered); len(missing) > 0 {
			return fmt.Errorf(
				"drop-vector task claims %d cleaned shards with no surviving source record for epoch %q on %s "+
					"(records purged or expired since the enqueue was composed); a re-enqueue derives coverage afresh",
				len(missing), newP.DropEpochID, newP.Collection)
		}
	}
	return nil
}

// CheckPropertyUpdate implements distributedtask.SchemaMutationDetector. A
// drop-vector task touches named vectors, not inverted properties, so a property
// update never conflicts with it.
func (p *DropVectorIndexProvider) CheckPropertyUpdate(className, propertyName string, existingTasks []*distributedtask.Task) error {
	return nil
}

// CheckClassMutation does NOT block DeleteClass for an in-flight drop: deleting
// the class supersedes the drop (the whole objects bucket is going away, so there
// is no half-stripped state to protect). The schema FSM's DeleteClass apply
// cascade-deletes the namespace's tasks via DeleteTasksForCollection, so the
// in-flight task is cleaned up rather than left blocking the delete. Always
// returns nil.
func (p *DropVectorIndexProvider) CheckClassMutation(className string, existingTasks []*distributedtask.Task) error {
	return nil
}

// CheckTenantMutation is deliberately permissive: tenant lifecycle is not
// coupled to in-flight drop-vector cleanups. Deactivating (or deleting) a
// tenant mid-strip makes its unit unfinishable — the drain loop fails the
// unit on the FIRST errored poll once the shard is no longer locally loaded
// (the transient-blip tolerance is deliberately skipped), the round ends
// FAILED — and reconciliation then re-enqueues for the remaining active
// shards: already-stripped shards re-drain instantly (their pending sets are
// empty), and the deactivated tenant is picked up by the cold-tenant deferral
// once it activates again. The schema marker stays the source of truth
// throughout, so no path here can lose data; blocking would only trade tenant
// availability for one round's bookkeeping. Always returns nil.
func (p *DropVectorIndexProvider) CheckTenantMutation(className string, tenants []string, existingTasks []*distributedtask.Task) error {
	return nil
}

// CheckVectorConfigRemoval implements distributedtask.VectorConfigRemovalGate:
// a still-stripping drop on the vector blocks removal, and only a SWAPPING
// task whose CoveredShards span every current shard vouches — that is, only
// the completing task's own in-flight finalize (OnTaskCompleted fires at
// SWAPPING; the gate cannot recognize "self"). FINISHED records never vouch:
// they outlive finalize by the task TTL, and after a re-create + re-drop of
// the name a stale record would remove the new drop's marker over unstripped
// vectors. A marker whose finalize was missed heals through reconciliation
// (fresh-epoch re-clean), not through record replay.
func (p *DropVectorIndexProvider) CheckVectorConfigRemoval(className string, removedVectors, shards []string, existingTasks []*distributedtask.Task) error {
	for _, targetVector := range removedVectors {
		if id, active := p.dropCovers(className, targetVector, existingTasks, stillStrippingStatus); active {
			return fmt.Errorf(
				"cannot remove dropped vector %q on %s: cleanup task %q is still active for it",
				targetVector, className, id)
		}
		// An empty shard set holds no data to strand: an MT collection whose
		// every tenant was deleted after the marker landed can never be cleaned
		// (enqueue no-ops with no active shard) and no SWAPPING voucher will
		// ever exist — without this, the marker would be permanently stuck.
		// Only tenant-less MT reaches here: a non-MT collection always has
		// shards, and the FSM passes its own Physical set.
		if len(shards) == 0 {
			continue
		}
		vouched, coversVec, uncovered := p.completedDropVoucher(className, targetVector, shards, existingTasks)
		if vouched {
			continue
		}
		// A terminal round may also vouch, but only on the one proof that a
		// stale record cannot fabricate: it covers every shard that still
		// exists AND it recorded owing a shard that has since been deleted.
		// Re-cleaning that collection would rewrite already-stripped shards for
		// work that no longer has anywhere to happen. A record that owed
		// nothing stays refused — that is the closed-epoch residue shape, where
		// full coverage says nothing about the current marker's data.
		if p.deletionResolvedDropVoucher(className, targetVector, shards, existingTasks) {
			continue
		}
		if coversVec {
			// Count only in the error: it reaches the HTTP body of a caller
			// holding just collection-update rights, and on an MT collection the
			// shard names are tenant names — gated behind ShardsMetadata READ,
			// and a shifting sorted sample would let repeat calls enumerate past
			// any cap. Operators get the sample from the server-side log.
			if p.logger != nil {
				p.logger.WithField("collection", className).
					WithField("targetVector", targetVector).
					WithField("uncoveredCount", len(uncovered)).
					WithField("sample", uncovered[:min(len(uncovered), 10)]).
					Info("drop-vector: VectorConfig removal rejected: shards not covered by the completing cleanup task")
			}
			return fmt.Errorf(
				"cannot remove dropped vector %q on %s: %d shards are not covered by the completing cleanup task; "+
					"cleanup re-runs automatically and the entry is removed once every shard is covered",
				targetVector, className, len(uncovered))
		}
		return fmt.Errorf(
			"cannot remove dropped vector %q on %s: only the completing cleanup task may remove the entry; "+
				"cleanup re-runs automatically and the entry is removed once it completes",
			targetVector, className)
	}
	return nil
}

// completedDropVoucher scans SWAPPING tasks covering targetVector on className and
// reports whether one of them covers every shard in shards (vouched). When
// tasks cover the vector but none covers all shards, uncovered holds the
// missing shards of the closest task — mirroring the finalize deferral, which
// keeps the marker until a single task covers everyone.
func (p *DropVectorIndexProvider) completedDropVoucher(className, targetVector string, shards []string,
	existingTasks []*distributedtask.Task,
) (vouched, coversVec bool, uncovered []string) {
	swappingOnly := func(s distributedtask.TaskStatus) bool { return s == distributedtask.TaskStatusSwapping }
	p.eachDropCovering(className, targetVector, existingTasks, swappingOnly,
		func(task *distributedtask.Task, existP *DropVectorIndexTaskPayload) bool {
			coversVec = true
			missing := ShardsNotCovered(shards, existP.CoveredShards())
			if len(missing) == 0 {
				vouched, uncovered = true, nil
				return false // done
			}
			if uncovered == nil || len(missing) < len(uncovered) {
				uncovered = missing
			}
			return true
		})
	return vouched, coversVec, uncovered
}

// deletionResolvedDropVoucher reports whether a terminal round of this drop
// proves the cleanup is done for the collection as it now stands: it covers
// every current shard, and it recorded owing a shard that no longer exists.
//
// This is the one case where a non-SWAPPING record may remove a marker. It is
// safe where a bare FINISHED voucher is not, because a finalized drop's residue
// owes nothing — it covered everything before it finalized — so it can never
// satisfy the deleted-shard half. Terminal-with-partial-work rounds count too:
// a round that failed after completing its units still recorded what it owed.
//
// Mirrors the enqueuer, which finalizes on the same proof
// (EpochAndInheritedCoverage); the two must agree or the enqueuer would
// propose removals this apply refuses.
func (p *DropVectorIndexProvider) deletionResolvedDropVoucher(className, targetVector string,
	shards []string, existingTasks []*distributedtask.Task,
) bool {
	terminal := func(s distributedtask.TaskStatus) bool {
		return s.IsCompleted() || s == distributedtask.TaskStatusFailed || s == distributedtask.TaskStatusCancelled
	}
	var vouched bool
	p.eachDropCovering(className, targetVector, existingTasks, terminal,
		func(task *distributedtask.Task, existP *DropVectorIndexTaskPayload) bool {
			if existP.ResolvedByShardDeletion(shards) {
				vouched = true
				return false // done
			}
			return true
		})
	return vouched
}

// stillStrippingStatus matches pre-SWAPPING tasks; they block removal.
func stillStrippingStatus(s distributedtask.TaskStatus) bool {
	return s.IsActive() && s != distributedtask.TaskStatusSwapping
}

// dropCovers reports whether a drop-vector task matching statusMatch covers targetVector
// on className. Unparseable payloads warn and are skipped (fail-open).
func (p *DropVectorIndexProvider) dropCovers(className, targetVector string, existingTasks []*distributedtask.Task,
	statusMatch func(distributedtask.TaskStatus) bool,
) (id string, found bool) {
	p.eachDropCovering(className, targetVector, existingTasks, statusMatch,
		func(task *distributedtask.Task, _ *DropVectorIndexTaskPayload) bool {
			id, found = task.ID, true
			return false // done
		})
	return id, found
}

// eachDropCovering invokes fn for every task matching statusMatch whose payload
// covers targetVector on className, until fn returns false. Unparseable payloads warn
// and are skipped (fail-open).
func (p *DropVectorIndexProvider) eachDropCovering(className, targetVector string,
	existingTasks []*distributedtask.Task, statusMatch func(distributedtask.TaskStatus) bool,
	fn func(*distributedtask.Task, *DropVectorIndexTaskPayload) bool,
) {
	for _, task := range existingTasks {
		if !statusMatch(task.Status) {
			continue
		}
		existP, err := decodeDropVectorIndexPayload(task.Payload)
		if err != nil {
			p.logger.WithField("task", task.ID).
				Warnf("drop-vector: skipping task with unparseable payload in removal gate: %v", err)
			continue
		}
		if !strings.EqualFold(existP.Collection, className) {
			continue
		}
		if len(intersectTargets(existP.Targets, []string{targetVector})) == 0 {
			continue
		}
		if !fn(task, existP) {
			return
		}
	}
}

// LocalCallbacksDone implements distributedtask.RecoveryAwareProvider. It returns
// false so the bootstrap pre-mark does not suppress OnGroupCompleted replay: the
// file-removal safety net is idempotent, so re-firing it once after restart
// safely completes any removal interrupted mid-shutdown.
func (p *DropVectorIndexProvider) LocalCallbacksDone(task *distributedtask.Task, localNode string) bool {
	return false
}

// intersectTargets returns the exact-match intersection of two target lists.
// Target vector names are case-sensitive identifiers (distinct map keys in
// VectorConfig, matched exactly by the transformer); only collection names are
// compared case-insensitively.
func intersectTargets(a, b []string) []string {
	set := make(map[string]struct{}, len(a))
	for _, t := range a {
		set[t] = struct{}{}
	}
	var out []string
	for _, t := range b {
		if _, ok := set[t]; ok {
			out = append(out, t)
		}
	}
	return out
}
