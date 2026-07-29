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
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	dbreindex "github.com/weaviate/weaviate/adapters/repos/db/reindex"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// fakeDeleteMarkers is the in-test stand-in for
// state.ReindexDeleteMarkers (which the usecases layer cannot import).
type fakeDeleteMarkers struct{ deleted map[string]time.Time }

func (f fakeDeleteMarkers) LastDeleted(collection, property, indexType string) time.Time {
	return f.deleted[strings.ToLower(collection)+"/"+property+"/"+indexType]
}

func markersWith(collection, property, indexType string, at time.Time) fakeDeleteMarkers {
	return fakeDeleteMarkers{deleted: map[string]time.Time{
		strings.ToLower(collection) + "/" + property + "/" + indexType: at,
	}}
}

// finishedTaskAt builds a FINISHED reindex task with an explicit FinishedAt.
func finishedTaskAt(t *testing.T, id, collection, prop string, mt dbreindex.ReindexMigrationType, finishedAt time.Time) *distributedtask.Task {
	t.Helper()
	raw, err := json.Marshal(dbreindex.ReindexTaskPayload{Collection: collection, Properties: []string{prop}, MigrationType: mt})
	require.NoError(t, err)
	return &distributedtask.Task{
		Namespace:      dbreindex.ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: 1},
		Payload:        raw,
		Status:         distributedtask.TaskStatusFinished,
		FinishedAt:     finishedAt,
	}
}

func startedTask(t *testing.T, id, collection, prop string, mt dbreindex.ReindexMigrationType) *distributedtask.Task {
	t.Helper()
	raw, err := json.Marshal(dbreindex.ReindexTaskPayload{Collection: collection, Properties: []string{prop}, MigrationType: mt})
	require.NoError(t, err)
	return &distributedtask.Task{
		Namespace:      dbreindex.ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: 1},
		Payload:        raw,
		Status:         distributedtask.TaskStatusStarted,
		StartedAt:      time.Now(),
	}
}

// Pins suppression of the phantom "indexing@100%" entry for a
// just-deleted index.
func TestIsPostDeleteFinalizeBleed(t *testing.T) {
	const (
		collection = "C"
		prop       = "score"
		indexType  = "rangeFilters"
		taskID     = "C:enable-rangeable:score:aaaa"
	)

	svcWith := func(markers DeleteMarkerReader) *Service {
		return New(Deps{DeleteMarkers: markers}, logrus.New())
	}

	t.Run("FINISHED task deleted after finish -> suppress", func(t *testing.T) {
		s := svcWith(markersWith(collection, prop, indexType, time.Now()))
		// Task finished BEFORE the delete → the index was created, finished,
		// then deleted → the finalize window is a phantom → suppress.
		tasks := ParseReindexTasks([]*distributedtask.Task{finishedTaskAt(t, taskID, collection, prop,
			dbreindex.ReindexTypeEnableRangeable, time.Now().Add(-time.Minute))})
		require.True(t, s.isPostDeleteFinalizeBleed(collection, prop, indexType, taskID, tasks))
	})

	t.Run("FINISHED task but no DELETE recorded -> keep", func(t *testing.T) {
		s := svcWith(fakeDeleteMarkers{deleted: map[string]time.Time{}})
		tasks := ParseReindexTasks([]*distributedtask.Task{finishedTaskAt(t, taskID, collection, prop,
			dbreindex.ReindexTypeEnableRangeable, time.Now().Add(-time.Minute))})
		require.False(t, s.isPostDeleteFinalizeBleed(collection, prop, indexType, taskID, tasks))
	})

	t.Run("DELETE recorded BEFORE the task finished -> keep (fresh creation)", func(t *testing.T) {
		s := svcWith(markersWith(collection, prop, indexType, time.Now()))
		// Task finished AFTER the delete → this is a fresh re-creation that
		// legitimately just finished → the finalize window is real → keep.
		tasks := ParseReindexTasks([]*distributedtask.Task{finishedTaskAt(t, taskID, collection, prop,
			dbreindex.ReindexTypeEnableRangeable, time.Now().Add(time.Minute))})
		require.False(t, s.isPostDeleteFinalizeBleed(collection, prop, indexType, taskID, tasks))
	})

	t.Run("live re-enable (STARTED task) is never suppressed", func(t *testing.T) {
		s := svcWith(markersWith(collection, prop, indexType, time.Now()))
		// A STARTED task driving the entry means a live re-enable, not the
		// finalize-window override — must never be suppressed even though a
		// DELETE was recorded moments ago.
		tasks := ParseReindexTasks([]*distributedtask.Task{startedTask(t, taskID, collection, prop,
			dbreindex.ReindexTypeEnableRangeable)})
		require.False(t, s.isPostDeleteFinalizeBleed(collection, prop, indexType, taskID, tasks))
	})

	t.Run("nil markers -> keep", func(t *testing.T) {
		s := svcWith(nil)
		tasks := ParseReindexTasks([]*distributedtask.Task{finishedTaskAt(t, taskID, collection, prop,
			dbreindex.ReindexTypeEnableRangeable, time.Now().Add(-time.Minute))})
		require.False(t, s.isPostDeleteFinalizeBleed(collection, prop, indexType, taskID, tasks))
	})
}
