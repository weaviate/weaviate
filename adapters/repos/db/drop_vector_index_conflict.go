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

	for _, task := range existingTasks {
		if !task.Status.IsActive() {
			continue
		}
		existP, err := decodeDropVectorIndexPayload(task.Payload)
		if err != nil {
			// Skip a corrupt payload rather than fail closed: erroring here would block
			// every new drop cluster-wide on one bad task. Deterministic across nodes.
			p.logger.WithField("task", task.ID).
				Warnf("drop-vector: skipping active task with unparseable payload in conflict check: %v", err)
			continue
		}
		if !strings.EqualFold(existP.Collection, newP.Collection) {
			continue
		}
		if overlap := intersectTargets(existP.Targets, newP.Targets); len(overlap) > 0 {
			return fmt.Errorf(
				"drop-vector task %q is already in flight on %s for vector(s) %v (status=%s)",
				task.ID, existP.Collection, overlap, task.Status)
		}
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
		covered := map[string]struct{}{}
		for _, task := range existingTasks {
			if !task.Status.IsCompleted() {
				continue
			}
			existP, err := decodeDropVectorIndexPayload(task.Payload)
			if err != nil {
				continue // same deterministic fail-open skip as above
			}
			if !strings.EqualFold(existP.Collection, newP.Collection) ||
				!SameTargetSet(existP.Targets, newP.Targets) ||
				existP.DropEpochID != newP.DropEpochID {
				continue
			}
			for shard := range existP.CoveredShards() {
				covered[shard] = struct{}{}
			}
		}
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

// CheckTenantMutation blocks tenant mutations that would make a tenant's shards
// locally unavailable while a drop-vector task on the class is in flight.
// Conservative: any in-flight drop on the class blocks every tenant mutation on
// it (the payload is class-scoped, not per-tenant).
func (p *DropVectorIndexProvider) CheckTenantMutation(className string, tenants []string, existingTasks []*distributedtask.Task) error {
	for _, task := range existingTasks {
		if !task.Status.IsActive() {
			continue
		}
		existP, err := decodeDropVectorIndexPayload(task.Payload)
		if err != nil {
			// Skip a corrupt payload rather than block every tenant mutation
			// cluster-wide on one bad task. Deterministic across nodes.
			p.logger.WithField("task", task.ID).
				Warnf("drop-vector: skipping active task with unparseable payload in tenant-mutation check: %v", err)
			continue
		}
		if !strings.EqualFold(existP.Collection, className) {
			continue
		}
		return fmt.Errorf(
			"drop-vector task %q is in flight on %s (status=%s); wait for it to complete before mutating tenants %v",
			task.ID, existP.Collection, task.Status, tenants)
	}
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
	for _, vec := range removedVectors {
		if id, active := p.dropCovers(className, vec, existingTasks, stillStrippingStatus); active {
			return fmt.Errorf(
				"cannot remove dropped vector %q on %s: cleanup task %q is still active for it",
				vec, className, id)
		}
		vouched, coversVec, uncovered := p.completedDropVoucher(className, vec, shards, existingTasks)
		if vouched {
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
					WithField("targetVector", vec).
					WithField("uncoveredCount", len(uncovered)).
					WithField("sample", uncovered[:min(len(uncovered), 10)]).
					Info("drop-vector: VectorConfig removal rejected: shards not covered by the completing cleanup task")
			}
			return fmt.Errorf(
				"cannot remove dropped vector %q on %s: %d shards are not covered by the completing cleanup task; "+
					"cleanup re-runs automatically and the entry is removed once every shard is covered",
				vec, className, len(uncovered))
		}
		return fmt.Errorf(
			"cannot remove dropped vector %q on %s: only the completing cleanup task may remove the entry; "+
				"cleanup re-runs automatically and the entry is removed once it completes",
			vec, className)
	}
	return nil
}

// completedDropVoucher scans SWAPPING tasks covering vec on className and
// reports whether one of them covers every shard in shards (vouched). When
// tasks cover the vector but none covers all shards, uncovered holds the
// missing shards of the closest task — mirroring the finalize deferral, which
// keeps the marker until a single task covers everyone.
func (p *DropVectorIndexProvider) completedDropVoucher(className, vec string, shards []string,
	existingTasks []*distributedtask.Task,
) (vouched, coversVec bool, uncovered []string) {
	swappingOnly := func(s distributedtask.TaskStatus) bool { return s == distributedtask.TaskStatusSwapping }
	p.eachDropCovering(className, vec, existingTasks, swappingOnly,
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

// stillStrippingStatus matches pre-SWAPPING tasks; they block removal.
func stillStrippingStatus(s distributedtask.TaskStatus) bool {
	return s.IsActive() && s != distributedtask.TaskStatusSwapping
}

// dropCovers reports whether a drop-vector task matching statusMatch covers vec
// on className. Unparseable payloads warn and are skipped (fail-open).
func (p *DropVectorIndexProvider) dropCovers(className, vec string, existingTasks []*distributedtask.Task,
	statusMatch func(distributedtask.TaskStatus) bool,
) (id string, found bool) {
	p.eachDropCovering(className, vec, existingTasks, statusMatch,
		func(task *distributedtask.Task, _ *DropVectorIndexTaskPayload) bool {
			id, found = task.ID, true
			return false // done
		})
	return id, found
}

// eachDropCovering invokes fn for every task matching statusMatch whose payload
// covers vec on className, until fn returns false. Unparseable payloads warn
// and are skipped (fail-open).
func (p *DropVectorIndexProvider) eachDropCovering(className, vec string,
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
		if len(intersectTargets(existP.Targets, []string{vec})) == 0 {
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
