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
// rejects a new drop that overlaps an in-flight drop's targets on the same
// collection. FSM-deterministic: a pure function of (newPayload, existingTasks).
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
// removing a dropped vector entry is allowed only once a FINISHED drop-vector
// task covers it, so the marker can't vanish before the vectors are stripped.
func (p *DropVectorIndexProvider) CheckVectorConfigRemoval(className string, removedVectors []string, existingTasks []*distributedtask.Task) error {
	for _, vec := range removedVectors {
		// A replayed old task's finalize is epoch-blind: while a NEWER drop on the
		// same name is still ACTIVE, removal would free the name mid-cleanup with
		// its op still armed. Deterministic twin of the provider-side
		// activeOverlappingDrop guard.
		if id, active := activeDropCovers(className, vec, existingTasks); active {
			return fmt.Errorf(
				"cannot remove dropped vector %q on %s: cleanup task %q is still active for it",
				vec, className, id)
		}
		if !finishedDropCovers(className, vec, existingTasks) {
			return fmt.Errorf(
				"cannot remove dropped vector %q on %s: no FINISHED cleanup task covers it; "+
					"the vector data is still being stripped, or the drop was never started",
				vec, className)
		}
	}
	return nil
}

// activeDropCovers reports whether a non-terminal drop-vector task strips vec on
// className, returning its ID. Unparseable payloads are skipped (fail-open,
// consistent with the other detectors).
func activeDropCovers(className, vec string, existingTasks []*distributedtask.Task) (string, bool) {
	for _, task := range existingTasks {
		if !task.Status.IsActive() {
			continue
		}
		existP, err := decodeDropVectorIndexPayload(task.Payload)
		if err != nil {
			continue
		}
		if !strings.EqualFold(existP.Collection, className) {
			continue
		}
		if len(intersectTargets(existP.Targets, []string{vec})) > 0 {
			return task.ID, true
		}
	}
	return "", false
}

// finishedDropCovers reports whether a FINISHED drop-vector task strips vec on
// className. An unparseable FINISHED payload is skipped rather than treated as a
// match, so a single corrupt task can't vouch for an unrelated removal.
func finishedDropCovers(className, vec string, existingTasks []*distributedtask.Task) bool {
	for _, task := range existingTasks {
		if task.Status != distributedtask.TaskStatusFinished {
			continue
		}
		existP, err := decodeDropVectorIndexPayload(task.Payload)
		if err != nil {
			continue
		}
		if !strings.EqualFold(existP.Collection, className) {
			continue
		}
		if len(intersectTargets(existP.Targets, []string{vec})) > 0 {
			return true
		}
	}
	return false
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
