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

package reindex

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/weaviate/weaviate/adapters/repos/db/reindex"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// MaxConcurrentReindexPerCollection caps how many in-flight reindex
// tasks can target the same collection at once. Each task creates
// ingest + backup buckets on every replica; without a cap, a script
// that runs PUT /indexes/<prop> per property would fan out N tasks for
// an N-property collection and overwhelm both LSM compaction and disk.
//
// Sized to comfortably accommodate realistic batch property changes
// (e.g. retokenizing every text property on a ~20-property collection)
// while still preventing pathological unbounded fan-out. The original
// value of 4 was too restrictive; the reindex_concurrent acceptance
// test exercises 15 simultaneous non-conflicting submits.
const MaxConcurrentReindexPerCollection = 32

// CountStartedTasksForCollection counts every non-terminal status
// (STARTED / PREPARING / SWAPPING) because PREPARING / SWAPPING still
// hold tracker dirs and reindex buckets.
func CountStartedTasksForCollection(collection string, tasks []*distributedtask.Task) int {
	n := 0
	for _, task := range tasks {
		if !task.Status.IsActive() {
			continue
		}
		var payload reindex.ReindexTaskPayload
		if err := json.Unmarshal(task.Payload, &payload); err != nil {
			continue
		}
		if strings.EqualFold(payload.Collection, collection) {
			n++
		}
	}
	return n
}

// CheckReindexConflict returns a non-nil error when a running task has
// a payload it cannot decode: it cannot prove non-conflict, so the
// caller must reject the submit.
//
// An empty Properties list is reserved for a future whole-collection
// rebuild and is treated as matching any property for conflict purposes.
//
// Unparseable payloads (e.g. payload schema change across versions,
// RAFT replay of a task from an older binary) are treated as a hard
// error rather than silently skipped: silent-skip would let a real
// bucket-level conflict slip through and allow a second task to race
// against the in-flight one.
func CheckReindexConflict(collection string, newType reindex.ReindexMigrationType,
	newProps []string, tasks []*distributedtask.Task,
) (string, error) {
	for _, task := range tasks {
		if !task.Status.IsActive() {
			continue
		}

		var payload reindex.ReindexTaskPayload
		if err := json.Unmarshal(task.Payload, &payload); err != nil {
			return "", fmt.Errorf(
				"in-flight reindex task %q has an unparseable payload; cannot verify conflict; "+
					"retry after operator inspects the task: %w", task.ID, err)
		}
		// Successfully parsed but informationally empty: a `{}` payload,
		// or one missing Collection / MigrationType. Same epistemic
		// state as unparseable — refuse for the same reason.
		if payload.Collection == "" || payload.MigrationType == "" {
			return "", fmt.Errorf(
				"in-flight reindex task %q has an empty Collection or MigrationType "+
					"(payload may have been written by an older binary); cannot verify conflict; "+
					"retry after operator inspects the task", task.ID)
		}
		if !strings.EqualFold(payload.Collection, collection) {
			continue
		}

		if conflict := reindex.TypesConflictReason(newType, newProps, payload.MigrationType, payload.Properties); conflict != "" {
			return fmt.Sprintf("reindex task %q conflicts: %s", task.ID, conflict), nil
		}
	}
	return "", nil
}
