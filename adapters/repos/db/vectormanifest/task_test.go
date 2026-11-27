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

package vectormanifest

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
	entlsmkv "github.com/weaviate/weaviate/entities/lsmkv"
)

func newTestTaskStore(t *testing.T) (*taskStore, *shardmeta.DB) {
	t.Helper()
	db, err := shardmeta.Open(t.TempDir(), entlsmkv.BoltFlockTimeout)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return newTaskStore(db), db
}

// testTask is a rebuild of a vector from hnsw at its name-derived
// ID to hfresh at a generated one.
func testTask(source string) rebuildTask {
	return rebuildTask{
		TaskID:      "task-1",
		TaskVersion: 3,
		Source:      taskIndex{PhysicalID: source, IndexType: "hnsw"},
		Candidate:   taskIndex{PhysicalID: "3f2a9c1e-0b7d-4c1a-9e2f-5d6c7b8a9f00", IndexType: "hfresh"},
	}
}

// TestTaskStore_Format pins the task section's keys and entry
// layout: a later version reads them.
func TestTaskStore_Format(t *testing.T) {
	assert.Equal(t, "vector_index_tasks", tasksNamespace)
	assert.Equal(t, "1", tasksFormatVersion)

	tasks, db := newTestTaskStore(t)
	require.NoError(t, tasks.Put("", testTask("main")))
	require.NoError(t, tasks.Put("title", testTask("vectors_title")))

	ns := db.Namespace("vector_index_tasks")
	version, err := ns.Get([]byte(".format_version"))
	require.NoError(t, err)
	assert.Equal(t, "1", string(version))
	legacy, err := ns.Get([]byte(".legacy"))
	require.NoError(t, err)
	assert.JSONEq(t, `{
		"task_id": "task-1",
		"task_version": 3,
		"source": {"physical_id": "main", "index_type": "hnsw"},
		"candidate": {"physical_id": "3f2a9c1e-0b7d-4c1a-9e2f-5d6c7b8a9f00", "index_type": "hfresh"}
	}`, string(legacy))
	title, err := ns.Get([]byte("title"))
	require.NoError(t, err)
	assert.Contains(t, string(title), `"physical_id":"vectors_title"`)
}

func TestTaskStore_Load(t *testing.T) {
	put := func(t *testing.T, db *shardmeta.DB, key, value string) {
		t.Helper()
		require.NoError(t, db.Namespace(tasksNamespace).Put([]byte(key), []byte(value)))
	}
	valid := `{"task_id":"t","task_version":1,` +
		`"source":{"physical_id":"vectors_title","index_type":"hnsw"},` +
		`"candidate":{"physical_id":"3f2a-9c1e","index_type":"hfresh"}}`

	t.Run("an empty section has no tasks", func(t *testing.T) {
		tasks, _ := newTestTaskStore(t)
		got, err := tasks.Load()
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("reads every task under its vector name, ignoring unknown fields", func(t *testing.T) {
		tasks, db := newTestTaskStore(t)
		put(t, db, ".format_version", "1")
		put(t, db, ".legacy", `{"task_id":"t0","task_version":2,"later":true,`+
			`"source":{"physical_id":"main","index_type":"hnsw","later":1},`+
			`"candidate":{"physical_id":"a-b","index_type":"flat"}}`)
		put(t, db, "title", valid)

		got, err := tasks.Load()
		require.NoError(t, err)
		assert.Equal(t, map[string]rebuildTask{
			"": {
				TaskID: "t0", TaskVersion: 2,
				Source:    taskIndex{PhysicalID: "main", IndexType: "hnsw"},
				Candidate: taskIndex{PhysicalID: "a-b", IndexType: "flat"},
			},
			"title": {
				TaskID: "t", TaskVersion: 1,
				Source:    taskIndex{PhysicalID: "vectors_title", IndexType: "hnsw"},
				Candidate: taskIndex{PhysicalID: "3f2a-9c1e", IndexType: "hfresh"},
			},
		}, got)
	})

	tests := []struct {
		name      string
		noVersion bool // do not write format_version "1" first
		key       string
		value     string
		wantErr   string
	}{
		{name: "unknown format version", noVersion: true, key: ".format_version", value: "2", wantErr: "format version"},
		{name: "unknown dotted key", key: ".something", value: "x", wantErr: `unknown key ".something"`},
		{name: "task without format version", noVersion: true, key: "title", value: valid, wantErr: "format version"},
		{name: "unparsable value", key: "title", value: "{", wantErr: `task "title"`},
		{name: "empty task id", key: "title", value: strings.Replace(valid, `"task_id":"t"`, `"task_id":""`, 1), wantErr: "empty task id"},
		{name: "empty source id", key: "title", value: strings.Replace(valid, `"vectors_title"`, `""`, 1), wantErr: "source: empty physical id"},
		{name: "source id outside the naming rule", key: "title", value: strings.Replace(valid, `"vectors_title"`, `"title"`, 1), wantErr: `source: physical id: "title" is neither`},
		{name: "empty candidate type", key: "title", value: strings.Replace(valid, `"hfresh"`, `""`, 1), wantErr: "candidate: empty index type"},
		{name: "candidate id with a character a file name must not have", key: "title", value: strings.Replace(valid, `"3f2a-9c1e"`, `"a/b-c"`, 1), wantErr: `candidate: physical id: "a/b-c" is neither`},
		{name: "source and candidate name the same files", key: "title", value: strings.Replace(valid, `"3f2a-9c1e"`, `"vectors_title"`, 1), wantErr: `both "vectors_title"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tasks, db := newTestTaskStore(t)
			if !tt.noVersion {
				put(t, db, ".format_version", "1")
			}
			put(t, db, tt.key, tt.value)
			_, err := tasks.Load()
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestTaskStore_PutGetDelete(t *testing.T) {
	tasks, _ := newTestTaskStore(t)

	_, ok, err := tasks.Get("title")
	require.NoError(t, err)
	assert.False(t, ok)

	task := testTask("vectors_title")
	require.NoError(t, tasks.Put("title", task))
	got, ok, err := tasks.Get("title")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, task, got)

	invalid := task
	invalid.Candidate.PhysicalID = ""
	require.ErrorContains(t, tasks.Put("body", invalid), "candidate: empty physical id")

	require.NoError(t, tasks.Delete("title"))
	require.NoError(t, tasks.Delete("title"), "a missing task is already deleted")
	all, err := tasks.Load()
	require.NoError(t, err)
	assert.Empty(t, all)
}

// The task's IDs name files by the mapping's naming rule, so the same ID
// names the same files whether a record or a task holds it.
func TestTask_StorageIDFollowsTheNamingRule(t *testing.T) {
	task := testTask("vectors_title")
	assert.Equal(t, "vectors_title", task.Source.storageID())
	assert.Equal(t, "vectors_3f2a9c1e-0b7d-4c1a-9e2f-5d6c7b8a9f00", task.Candidate.storageID())
	assert.Equal(t, record{PhysicalID: task.Candidate.PhysicalID}.storageID(), task.Candidate.storageID())
}
