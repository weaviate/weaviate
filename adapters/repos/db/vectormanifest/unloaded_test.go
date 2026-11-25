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
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
	entlsmkv "github.com/weaviate/weaviate/entities/lsmkv"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// A cold shard's vectors are found through their records; a vector without
// one, or a shard without a mapping, is where the naming rule puts it.
func TestUnloadedStorageIDs(t *testing.T) {
	const generated = "3f2a9c1e-0b7d-4c1a-9e2f-5d6c7b8a9f00"
	names := []string{"", "title", "body"}
	defaults := map[string]string{"": "main", "title": "vectors_title", "body": "vectors_body"}

	writeMapping := func(t *testing.T, dir, version string, records map[string]string) {
		t.Helper()
		db, err := shardmeta.Open(dir, entlsmkv.BoltFlockTimeout)
		require.NoError(t, err)
		ns := db.Namespace(namespace)
		require.NoError(t, ns.Put([]byte(formatVersionKey), []byte(version)))
		for name, rec := range records {
			require.NoError(t, ns.Put([]byte(recordKey(name)), []byte(rec)))
		}
		require.NoError(t, db.Close())
	}

	t.Run("no metadata file", func(t *testing.T) {
		ids, locked, err := UnloadedStorageIDs(t.TempDir(), names)
		require.NoError(t, err)
		assert.False(t, locked)
		assert.Equal(t, defaults, ids)
	})

	t.Run("metadata file without a mapping", func(t *testing.T) {
		dir := t.TempDir()
		db, err := shardmeta.Open(dir, entlsmkv.BoltFlockTimeout)
		require.NoError(t, err)
		require.NoError(t, db.Close())
		ids, locked, err := UnloadedStorageIDs(dir, names)
		require.NoError(t, err)
		assert.False(t, locked)
		assert.Equal(t, defaults, ids)
	})

	t.Run("records decide, a vector without one keeps the naming rule", func(t *testing.T) {
		dir := t.TempDir()
		writeMapping(t, dir, "1", map[string]string{
			"":      `{"physical_id":"main","index_type":"hnsw","state":"ready"}`,
			"title": `{"physical_id":"` + generated + `","index_type":"hnsw","state":"ready"}`,
		})
		ids, locked, err := UnloadedStorageIDs(dir, names)
		require.NoError(t, err)
		assert.False(t, locked)
		assert.Equal(t, map[string]string{"": "main", "title": "vectors_" + generated, "body": "vectors_body"}, ids)
	})

	t.Run("a load holding the file", func(t *testing.T) {
		dir := t.TempDir()
		db, err := shardmeta.Open(dir, entlsmkv.BoltFlockTimeout)
		require.NoError(t, err)
		t.Cleanup(func() { db.Close() })
		ids, locked, err := UnloadedStorageIDs(dir, names)
		require.NoError(t, err)
		assert.True(t, locked)
		assert.Equal(t, defaults, ids)
	})

	t.Run("an invalid record of a vector that is not asked for", func(t *testing.T) {
		dir := t.TempDir()
		writeMapping(t, dir, "1", map[string]string{
			"title": `{"physical_id":"` + generated + `","index_type":"hnsw","state":"ready"}`,
			"other": `{"physical_id":"title","index_type":"hnsw","state":"ready"}`,
		})
		ids, locked, err := UnloadedStorageIDs(dir, names)
		require.NoError(t, err)
		assert.False(t, locked)
		assert.Equal(t, map[string]string{"": "main", "title": "vectors_" + generated, "body": "vectors_body"}, ids)
	})

	t.Run("unsupported format version", func(t *testing.T) {
		dir := t.TempDir()
		writeMapping(t, dir, "2", nil)
		ids, _, err := UnloadedStorageIDs(dir, names)
		require.ErrorContains(t, err, "format version")
		assert.Equal(t, defaults, ids)
	})

	t.Run("records without a format version", func(t *testing.T) {
		dir := t.TempDir()
		db, err := shardmeta.Open(dir, entlsmkv.BoltFlockTimeout)
		require.NoError(t, err)
		require.NoError(t, db.Namespace(namespace).Put([]byte(recordKey("title")),
			[]byte(`{"physical_id":"`+generated+`","index_type":"hnsw","state":"ready"}`)))
		require.NoError(t, db.Close())
		ids, _, err := UnloadedStorageIDs(dir, names)
		require.ErrorContains(t, err, "records without a format version")
		assert.Equal(t, defaults, ids)
	})

	t.Run("invalid record", func(t *testing.T) {
		dir := t.TempDir()
		writeMapping(t, dir, "1", map[string]string{
			"":     `{"physical_id":"` + generated + `","index_type":"hnsw","state":"ready"}`,
			"body": `{"physical_id":"title","index_type":"hnsw","state":"ready"}`,
		})
		ids, _, err := UnloadedStorageIDs(dir, names)
		require.ErrorContains(t, err, `record "body": physical id`)
		assert.Equal(t, defaults, ids, "no ID read before the invalid record is kept")
	})
}

// The offline reader refuses what Load refuses: a deletion must not act on
// a record this binary cannot read.
func TestReadUnloaded(t *testing.T) {
	valid := `{"physical_id":"vectors_title","index_type":"hnsw","state":"ready"}`
	tests := []struct {
		name    string
		version string
		record  string
		wantErr string
	}{
		{name: "reads a record", version: "1", record: valid},
		{name: "unsupported format version", version: "2", record: valid, wantErr: "format version"},
		{name: "invalid record", version: "1", record: `{"physical_id":"","index_type":"hnsw","state":"ready"}`, wantErr: "physical id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			db, err := shardmeta.Open(dir, entlsmkv.BoltFlockTimeout)
			require.NoError(t, err)
			ns := db.Namespace(namespace)
			require.NoError(t, ns.Put([]byte(formatVersionKey), []byte(tt.version)))
			require.NoError(t, ns.Put([]byte("title"), []byte(tt.record)))
			require.NoError(t, db.Close())

			records, initialized, err := readUnloaded(dir)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.True(t, initialized)
			assert.Equal(t, map[string]record{"title": {PhysicalID: "vectors_title", IndexType: "hnsw", State: "ready"}}, records)
		})
	}
}

func TestReadUnloadedTasks(t *testing.T) {
	t.Run("a shard without a metadata db has no tasks", func(t *testing.T) {
		got, err := readUnloadedTasks(t.TempDir())
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("reads every task", func(t *testing.T) {
		dir := t.TempDir()
		db, err := shardmeta.Open(dir, entlsmkv.BoltFlockTimeout)
		require.NoError(t, err)
		tasks := newTaskStore(db)
		require.NoError(t, tasks.Put("title", testTask("vectors_title")))
		require.NoError(t, tasks.Put("body", testTask("vectors_body")))
		require.NoError(t, db.Close())

		got, err := readUnloadedTasks(dir)
		require.NoError(t, err)
		assert.Equal(t, map[string]rebuildTask{"title": testTask("vectors_title"), "body": testTask("vectors_body")}, got)
	})

	tests := []struct {
		name    string
		version string
		task    string
		wantErr string
	}{
		{name: "unsupported format version", version: "2", task: "{}", wantErr: "format version"},
		{
			name: "task without format version", wantErr: "format version",
			task: `{"task_id":"t","source":{"physical_id":"main","index_type":"hnsw"},"candidate":{"physical_id":"a-b","index_type":"flat"}}`,
		},
		{name: "invalid task", version: "1", task: `{"task_id":"t"}`, wantErr: `task "title": source`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			db, err := shardmeta.Open(dir, entlsmkv.BoltFlockTimeout)
			require.NoError(t, err)
			ns := db.Namespace(tasksNamespace)
			if tt.version != "" {
				require.NoError(t, ns.Put([]byte(formatVersionKey), []byte(tt.version)))
			}
			require.NoError(t, ns.Put([]byte("title"), []byte(tt.task)))
			require.NoError(t, db.Close())

			_, err = readUnloadedTasks(dir)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// A drop during a rebuild deletes both indexes, the task and the record, with
// or without a record left.
func TestDropUnloaded_DuringARebuild(t *testing.T) {
	for _, recorded := range []bool{true, false} {
		t.Run(fmt.Sprintf("recorded %v", recorded), func(t *testing.T) {
			dir := t.TempDir()
			m, db := openUnloaded(t, dir, "title", "body")
			require.NoError(t, m.tasks.Put("title", rebuildOf("vectors_title", "a-b")))
			if !recorded {
				require.NoError(t, m.records.Delete("title"))
			}
			require.NoError(t, db.Close())
			for _, id := range []string{"vectors_title", "vectors_a-b", "vectors_body"} {
				plant(t, files(dir, id)...)
			}

			require.NoError(t, DropUnloaded(dir, "title", []string{"body"}))

			for _, path := range append(files(dir, "vectors_title"), files(dir, "vectors_a-b")...) {
				assert.NoDirExists(t, path)
			}
			for _, path := range files(dir, "vectors_body") {
				assert.DirExists(t, path)
			}
			records, _, err := readUnloaded(dir)
			require.NoError(t, err)
			assert.Contains(t, records, "body")
			assert.NotContains(t, records, "title")
			tasks, err := readUnloadedTasks(dir)
			require.NoError(t, err)
			assert.Empty(t, tasks)
		})
	}
}

// The drop refuses a task that names another vector's index.
func TestDropUnloaded_RefusesATaskNamingASibling(t *testing.T) {
	dir := t.TempDir()
	m, db := openUnloaded(t, dir, "title", "body")
	require.NoError(t, m.tasks.Put("title", rebuildOf("vectors_title", "vectors_body")))
	require.NoError(t, db.Close())
	plant(t, files(dir, "vectors_body")...)

	// listed as a sibling or not
	for _, siblings := range [][]string{{"body"}, nil} {
		require.ErrorContains(t, DropUnloaded(dir, "title", siblings), `an index of vector "body"`)
	}
	for _, path := range files(dir, "vectors_body") {
		assert.DirExists(t, path)
	}
}

// openUnloaded writes the records of names and returns the manifest and the DB
// to close before the shard counts as not loaded.
func openUnloaded(t *testing.T, dir string, names ...string) (*Manifest, *shardmeta.DB) {
	t.Helper()
	db, err := shardmeta.Open(dir, entlsmkv.BoltFlockTimeout)
	require.NoError(t, err)
	logger, _ := test.NewNullLogger()
	m := New(Config{DB: db, ShardDir: dir, Logger: logger})
	records := map[string]record{}
	for _, name := range names {
		records[name] = recordFor(name, enthnsw.NewDefaultUserConfig(), stateReady)
	}
	require.NoError(t, m.records.Initialize(records))
	return m, db
}

// A vector the caller does not list as a sibling keeps its bucket and its
// queue when it has a record.
func TestDropUnloaded_SparesWhatEveryRecordOwns(t *testing.T) {
	dir := t.TempDir()
	db, err := shardmeta.Open(dir, entlsmkv.BoltFlockTimeout)
	require.NoError(t, err)
	cfg := enthnsw.NewDefaultUserConfig()
	store := newRecordStore(db)
	require.NoError(t, store.Initialize(map[string]record{
		"foo":                recordFor("foo", cfg, stateReady),
		"foo_muvera_vectors": recordFor("foo_muvera_vectors", cfg, stateReady),
	}))
	require.NoError(t, db.Close())
	own := filepath.Join(dir, "lsm", "vectors_foo")
	shared := filepath.Join(dir, "lsm", "vectors_foo_muvera_vectors")
	plant(t, own, shared)

	require.NoError(t, DropUnloaded(dir, "foo", nil))
	assert.NoDirExists(t, own)
	assert.DirExists(t, shared)

	records, _, err := readUnloaded(dir)
	require.NoError(t, err)
	assert.Equal(t, []string{"foo_muvera_vectors"}, slices.Collect(maps.Keys(records)))
}

// An unlisted vector that records the same queue refuses the drop.
func TestDropUnloaded_RefusesAQueueAnyRecordNames(t *testing.T) {
	dir := t.TempDir()
	db, err := shardmeta.Open(dir, entlsmkv.BoltFlockTimeout)
	require.NoError(t, err)
	cfg := enthnsw.NewDefaultUserConfig()
	other := recordFor("bar", cfg, stateReady)
	other.QueueID = "vectors_foo"
	require.NoError(t, newRecordStore(db).Initialize(map[string]record{
		"foo": recordFor("foo", cfg, stateReady), "bar": other,
	}))
	require.NoError(t, db.Close())
	queueDir := filepath.Join(dir, "vectors_foo.queue.d")
	plant(t, queueDir)

	require.ErrorContains(t, DropUnloaded(dir, "foo", nil), `both record queue "vectors_foo"`)
	assert.DirExists(t, queueDir)
}
