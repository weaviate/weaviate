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
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// finishedTaskAt builds a FINISHED reindex task with an explicit FinishedAt.
func finishedTaskAt(t *testing.T, id, collection, prop string, mt db.ReindexMigrationType, finishedAt time.Time) *distributedtask.Task {
	t.Helper()
	raw, err := json.Marshal(db.ReindexTaskPayload{Collection: collection, Properties: []string{prop}, MigrationType: mt})
	require.NoError(t, err)
	return &distributedtask.Task{
		Namespace:      db.ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: 1},
		Payload:        raw,
		Status:         distributedtask.TaskStatusFinished,
		FinishedAt:     finishedAt,
	}
}

// TestIsPostDeleteFinalizeBleed pins the suppression predicate that
// GET /indexes uses to drop the phantom "indexing@100%" finalize-window entry
// for an index the caller just deleted.
func TestIsPostDeleteFinalizeBleed(t *testing.T) {
	const (
		collection = "C"
		prop       = "score"
		indexType  = "rangeFilters"
		taskID     = "C:enable-rangeable:score:aaaa"
	)

	t.Run("FINISHED task deleted after finish -> suppress", func(t *testing.T) {
		markers := state.NewReindexDeleteMarkers()
		markers.Record(collection, prop, indexType) // deletedAt ≈ now
		h := &indexesHandlers{appState: &state.State{ReindexDeleteMarkers: markers}}

		// Task finished BEFORE the delete → the index was created, finished,
		// then deleted → the finalize window is a phantom → suppress.
		tasks := tasksMap(finishedTaskAt(t, taskID, collection, prop,
			db.ReindexTypeEnableRangeable, time.Now().Add(-time.Minute)))
		require.True(t, h.isPostDeleteFinalizeBleed(collection, prop, indexType, taskID, tasks))
	})

	t.Run("FINISHED task but no DELETE recorded -> keep", func(t *testing.T) {
		h := &indexesHandlers{appState: &state.State{ReindexDeleteMarkers: state.NewReindexDeleteMarkers()}}
		tasks := tasksMap(finishedTaskAt(t, taskID, collection, prop,
			db.ReindexTypeEnableRangeable, time.Now().Add(-time.Minute)))
		require.False(t, h.isPostDeleteFinalizeBleed(collection, prop, indexType, taskID, tasks))
	})

	t.Run("DELETE recorded BEFORE the task finished -> keep (fresh creation)", func(t *testing.T) {
		markers := state.NewReindexDeleteMarkers()
		markers.Record(collection, prop, indexType) // deletedAt ≈ now
		h := &indexesHandlers{appState: &state.State{ReindexDeleteMarkers: markers}}

		// Task finished AFTER the delete → this is a fresh re-creation that
		// legitimately just finished → the finalize window is real → keep.
		tasks := tasksMap(finishedTaskAt(t, taskID, collection, prop,
			db.ReindexTypeEnableRangeable, time.Now().Add(time.Minute)))
		require.False(t, h.isPostDeleteFinalizeBleed(collection, prop, indexType, taskID, tasks))
	})

	t.Run("live re-enable (STARTED task) is never suppressed", func(t *testing.T) {
		markers := state.NewReindexDeleteMarkers()
		markers.Record(collection, prop, indexType)
		h := &indexesHandlers{appState: &state.State{ReindexDeleteMarkers: markers}}

		// A STARTED task driving the entry means a live re-enable, not the
		// finalize-window override — must never be suppressed even though a
		// DELETE was recorded moments ago.
		started := buildTask(t, taskID, distributedtask.TaskStatusStarted,
			db.ReindexTaskPayload{Collection: collection, Properties: []string{prop}, MigrationType: db.ReindexTypeEnableRangeable}, nil)
		require.False(t, h.isPostDeleteFinalizeBleed(collection, prop, indexType, taskID, tasksMap(started)))
	})

	t.Run("nil markers -> keep", func(t *testing.T) {
		h := &indexesHandlers{appState: &state.State{}}
		tasks := tasksMap(finishedTaskAt(t, taskID, collection, prop,
			db.ReindexTypeEnableRangeable, time.Now().Add(-time.Minute)))
		require.False(t, h.isPostDeleteFinalizeBleed(collection, prop, indexType, taskID, tasks))
	})
}
