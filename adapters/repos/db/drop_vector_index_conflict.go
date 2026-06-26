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
			return fmt.Errorf(
				"in-flight drop-vector task %q has an unparseable payload; cannot verify conflict: %w",
				task.ID, err)
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

// CheckClassMutation blocks DeleteClass while a drop-vector task on the class is
// in flight: deleting the class would wipe the objects bucket the task is
// rewriting.
func (p *DropVectorIndexProvider) CheckClassMutation(className string, existingTasks []*distributedtask.Task) error {
	for _, task := range existingTasks {
		if !task.Status.IsActive() {
			continue
		}
		existP, err := decodeDropVectorIndexPayload(task.Payload)
		if err != nil {
			return fmt.Errorf(
				"in-flight drop-vector task %q has an unparseable payload; cannot verify DeleteClass on %s: %w",
				task.ID, className, err)
		}
		if !strings.EqualFold(existP.Collection, className) {
			continue
		}
		return fmt.Errorf(
			"drop-vector task %q is in flight on %s (status=%s); cancel it before deleting the class",
			task.ID, existP.Collection, task.Status)
	}
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
			return fmt.Errorf(
				"in-flight drop-vector task %q has an unparseable payload; cannot verify tenant mutation on %s/%v: %w",
				task.ID, className, tenants, err)
		}
		if !strings.EqualFold(existP.Collection, className) {
			continue
		}
		return fmt.Errorf(
			"drop-vector task %q is in flight on %s (status=%s); cancel it before mutating tenants %v",
			task.ID, existP.Collection, task.Status, tenants)
	}
	return nil
}

// CheckVectorConfigRemoval implements distributedtask.VectorConfigRemovalGate:
// removing a dropped vector entry is allowed only once a FINISHED drop-vector
// task covers it, so the marker can't vanish before the vectors are stripped.
func (p *DropVectorIndexProvider) CheckVectorConfigRemoval(className string, removedVectors []string, existingTasks []*distributedtask.Task) error {
	for _, vec := range removedVectors {
		if !finishedDropCovers(className, vec, existingTasks) {
			return fmt.Errorf(
				"cannot remove dropped vector %q on %s: no FINISHED cleanup task covers it; "+
					"the vector data is still being stripped, or the drop was never started",
				vec, className)
		}
	}
	return nil
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

// intersectTargets returns the case-insensitive intersection of two target lists.
func intersectTargets(a, b []string) []string {
	set := make(map[string]struct{}, len(a))
	for _, t := range a {
		set[strings.ToLower(t)] = struct{}{}
	}
	var out []string
	for _, t := range b {
		if _, ok := set[strings.ToLower(t)]; ok {
			out = append(out, t)
		}
	}
	return out
}
