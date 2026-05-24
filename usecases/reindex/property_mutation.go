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
	"context"
	"encoding/json"
	"fmt"
	"strings"

	dbreindex "github.com/weaviate/weaviate/adapters/repos/db/reindex"
)

// PropertyMutationConflict reports whether mutating
// (className, propertyName) at the schema level would race an
// in-flight reindex task on the same tuple. Returns a non-empty
// human-readable reason when blocked, or "" when safe to proceed.
//
// Per-node, in-memory: two REST handlers on different nodes can both
// observe "no conflict" and both forward to RAFT — that's expected,
// the apply-time MutationGuard is what closes the multi-node race.
// This check exists purely to short-circuit the common single-node
// case with a clean 4xx instead of an apply-time rejection.
//
// Degrades gracefully: a TaskLister error returns "" (no conflict
// detected) so the request proceeds to RAFT and the apply-time guard
// handles correctness. Spurious rejections from a transient local
// error would be worse than the rare doubled-RAFT-then-rejected
// pattern.
func (s *Service) PropertyMutationConflict(ctx context.Context, className, propertyName string) string {
	if s.deps.Cluster == nil {
		return ""
	}
	tasksByNamespace, err := s.deps.Cluster.ListDistributedTasks(ctx)
	if err != nil {
		return ""
	}
	for _, task := range tasksByNamespace[dbreindex.ReindexNamespace] {
		// PREPARING and SWAPPING count as in-flight (via
		// distributedtask.TaskStatus.IsActive) — mutating the
		// property during either phase would race the in-flight
		// per-shard bucket-pointer flip.
		if !task.Status.IsActive() {
			continue
		}
		var payload dbreindex.ReindexTaskPayload
		if err := json.Unmarshal(task.Payload, &payload); err != nil {
			// Unparseable payload in flight is a hard reject reason on
			// the apply side; mirror that here so the REST caller
			// doesn't get a spurious "ok-then-FAILED" two-step.
			return fmt.Sprintf(
				"in-flight reindex task %q has unparseable payload; "+
					"refusing property mutation on %s.%s until the task "+
					"is inspected", task.ID, className, propertyName)
		}
		if !strings.EqualFold(payload.Collection, className) {
			continue
		}
		// Empty Properties means "all properties" (whole-collection
		// rebuild, reserved); treat as a match.
		matches := len(payload.Properties) == 0
		for _, p := range payload.Properties {
			if p == propertyName {
				matches = true
				break
			}
		}
		if !matches {
			continue
		}
		return fmt.Sprintf(
			"reindex task %q (%s) is in flight on %s.%s (status=%s); "+
				"schema mutations on this property are blocked until "+
				"the reindex completes or is cancelled — wait for the "+
				"task to reach a terminal state, or cancel it via the "+
				"reindex REST API before retrying",
			task.ID, payload.MigrationType, payload.Collection,
			propertyName, task.Status)
	}
	return ""
}
