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

package rest

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
)

func reindexTask(id, collection string, status distributedtask.TaskStatus) *distributedtask.Task {
	return &distributedtask.Task{
		Namespace:      "reindex",
		TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: 1},
		Status:         status,
		Payload: []byte(`{"migrationType":"enable-filterable","collection":"` +
			collection + `","properties":["title"]}`),
	}
}

// TestOrphanedReindexTasks is the defense-in-depth half of the
// resurrection chain: a task whose collection was deleted while it was
// live must be identified so it can be discarded before a same-name
// collection adopts it.
//
// Task payloads carry the collection **by name only** — there is no UUID
// or creation timestamp on models.Class to compare against — so identity
// cannot be verified at resume time. Discarding the orphan at startup is
// therefore the mechanism that breaks the chain, not a backstop for one.
func TestOrphanedReindexTasks(t *testing.T) {
	// "Books" was deleted; "Movies" still exists.
	classExists := func(collection string) bool { return collection == "Movies" }

	tests := []struct {
		name            string
		tasks           []*distributedtask.Task
		wantOrphanIDs   []string
		wantCollections []string
	}{
		{
			name:            "live task on a deleted collection is an orphan",
			tasks:           []*distributedtask.Task{reindexTask("t1", "Books", distributedtask.TaskStatusStarted)},
			wantOrphanIDs:   []string{"t1"},
			wantCollections: []string{"Books"},
		},
		{
			name:  "live task on a surviving collection is left alone",
			tasks: []*distributedtask.Task{reindexTask("t2", "Movies", distributedtask.TaskStatusStarted)},
		},
		{
			name: "terminal task on a deleted collection is not touched",
			tasks: []*distributedtask.Task{
				reindexTask("t3", "Books", distributedtask.TaskStatusFinished),
				reindexTask("t4", "Books", distributedtask.TaskStatusCancelled),
				reindexTask("t5", "Books", distributedtask.TaskStatusFailed),
			},
		},
		{
			name: "undecodable payload is left for the operator",
			tasks: []*distributedtask.Task{{
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "t6", Version: 1},
				Status:         distributedtask.TaskStatusStarted,
				Payload:        []byte("not json"),
			}},
		},
		{
			name: "payload without a collection is left for the operator",
			tasks: []*distributedtask.Task{{
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "t7", Version: 1},
				Status:         distributedtask.TaskStatusStarted,
				Payload:        []byte(`{"migrationType":"enable-filterable"}`),
			}},
		},
		{
			name: "mixed set picks out only the orphan",
			tasks: []*distributedtask.Task{
				reindexTask("t8", "Movies", distributedtask.TaskStatusStarted),
				reindexTask("t9", "Books", distributedtask.TaskStatusStarted),
				reindexTask("t10", "Books", distributedtask.TaskStatusFinished),
			},
			wantOrphanIDs:   []string{"t9"},
			wantCollections: []string{"Books"},
		},
		{
			name: "no tasks at all",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orphans, collections := orphanedReindexTasks(tt.tasks, classExists)

			gotIDs := make([]string, 0, len(orphans))
			for _, o := range orphans {
				gotIDs = append(gotIDs, o.ID)
			}
			require.Equal(t, tt.wantOrphanIDs, nilIfEmpty(gotIDs))
			require.Equal(t, tt.wantCollections, nilIfEmpty(collections))
		})
	}
}

func nilIfEmpty(s []string) []string {
	if len(s) == 0 {
		return nil
	}
	return s
}
