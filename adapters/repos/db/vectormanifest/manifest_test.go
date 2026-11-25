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
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	entlsmkv "github.com/weaviate/weaviate/entities/lsmkv"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	entflat "github.com/weaviate/weaviate/entities/vectorindex/flat"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

type schemaVectors = map[string]schemaConfig.VectorIndexConfig

func openTestManifest(t *testing.T, dir string) *Manifest {
	t.Helper()
	db, err := shardmeta.Open(dir, entlsmkv.BoltFlockTimeout)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	logger, _ := test.NewNullLogger()
	return New(Config{
		DB:       db,
		ShardDir: dir,
		RemoveBucket: func(_ context.Context, bucket string) error {
			return os.RemoveAll(filepath.Join(dir, "lsm", bucket))
		},
		Logger: logger,
	})
}

// buildHNSW leaves on disk what building an hnsw index at id leaves.
func buildHNSW(t *testing.T, dir, id string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, helpers.HNSWCommitLogDirNameForID(id)), 0o755))
}

// loaded is a manifest past its first load with the given hnsw vectors built.
func loaded(t *testing.T, dir string, names ...string) *Manifest {
	t.Helper()
	m := openTestManifest(t, dir)
	schema := schemaVectors{}
	for _, name := range names {
		schema[name] = enthnsw.NewDefaultUserConfig()
	}
	entries, firstLoad, err := m.Open(t.Context(), schema)
	require.NoError(t, err)
	require.True(t, firstLoad)
	for _, e := range entries {
		buildHNSW(t, dir, e.StorageID)
	}
	require.NoError(t, m.Ready(schema))
	return m
}

func plant(t *testing.T, paths ...string) {
	t.Helper()
	for _, path := range paths {
		require.NoError(t, os.MkdirAll(path, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(path, "data"), []byte("x"), 0o600))
	}
}

func TestManifest_FirstLoad(t *testing.T) {
	dir := t.TempDir()
	m := openTestManifest(t, dir)
	cfg := enthnsw.NewDefaultUserConfig()
	schema := schemaVectors{"": cfg, "title": cfg}

	entries, firstLoad, err := m.Open(t.Context(), schema)
	require.NoError(t, err)
	assert.True(t, firstLoad)
	assert.Equal(t, []Entry{
		{Name: "", StorageID: "main", QueueID: "main", IndexType: "hnsw", Config: cfg},
		{Name: "title", StorageID: "vectors_title", QueueID: "vectors_title", IndexType: "hnsw", Config: cfg},
	}, entries)
	_, initialized, err := m.records.Load()
	require.NoError(t, err)
	assert.False(t, initialized, "nothing is written before the indexes are built")

	buildHNSW(t, dir, "main")
	buildHNSW(t, dir, "vectors_title")
	require.ErrorContains(t, m.Ready(schemaVectors{"": cfg}), `vector "title" was opened but not built`)
	require.NoError(t, m.Ready(schema))
	records, initialized, err := m.records.Load()
	require.NoError(t, err)
	assert.True(t, initialized)
	assert.Equal(t, map[string]record{
		"":      {PhysicalID: "main", IndexType: "hnsw", State: "ready", QueueID: "main", Config: cfg},
		"title": {PhysicalID: "vectors_title", IndexType: "hnsw", State: "ready", QueueID: "vectors_title", Config: cfg},
	}, records)
}

func TestManifest_FirstLoadWithoutVectors(t *testing.T) {
	m := openTestManifest(t, t.TempDir())
	entries, firstLoad, err := m.Open(t.Context(), nil)
	require.NoError(t, err)
	assert.True(t, firstLoad)
	assert.Empty(t, entries)
	require.NoError(t, m.Ready(nil))
	_, initialized, err := m.records.Load()
	require.NoError(t, err)
	assert.True(t, initialized)
}

// One later load per state a record can be in.
func TestManifest_Open(t *testing.T) {
	cfg := enthnsw.NewDefaultUserConfig()
	ready := record{PhysicalID: "a-b", IndexType: "hnsw", State: "ready", QueueID: "vectors_title", Config: cfg}
	with := func(f func(*record)) *record {
		rec := ready
		f(&rec)
		return &rec
	}
	other := cfg
	other.EF = 42

	tests := []struct {
		name    string
		rec     *record
		storage string
		schema  schemaVectors
		// want is the entry's storage ID, empty when the vector is not opened
		want    string
		wantErr string
		// wantRecord is what Ready leaves, nil for no record
		wantRecord *record
	}{
		{name: "ready", rec: &ready, storage: "vectors_a-b", schema: schemaVectors{"title": cfg}, want: "vectors_a-b", wantRecord: &ready},
		{name: "ready with its storage gone", rec: &ready, schema: schemaVectors{"title": cfg}, want: "vectors_a-b", wantRecord: &ready},
		{
			name: "creating", rec: with(func(r *record) { r.State = "creating" }),
			schema: schemaVectors{"title": cfg}, want: "vectors_a-b", wantRecord: &ready,
		},
		{
			name: "another config", rec: &ready, storage: "vectors_a-b", schema: schemaVectors{"title": other}, want: "vectors_a-b",
			wantRecord: with(func(r *record) { r.Config = other }),
		},
		{
			name: "no record", schema: schemaVectors{"title": cfg}, want: "vectors_title",
			wantRecord: &record{PhysicalID: "vectors_title", IndexType: "hnsw", State: "ready", QueueID: "vectors_title", Config: cfg},
		},
		{name: "gone from the schema", rec: &ready, storage: "vectors_a-b"},
		{name: "dropping", rec: with(func(r *record) { r.State = "dropping" }), storage: "vectors_a-b"},
		{
			name: "dropping, then created again", rec: with(func(r *record) { r.State = "dropping" }), storage: "vectors_a-b",
			schema: schemaVectors{"title": cfg}, want: "vectors_title",
			wantRecord: &record{PhysicalID: "vectors_title", IndexType: "hnsw", State: "ready", QueueID: "vectors_title", Config: cfg},
		},
		{
			name: "another index type", rec: &ready, schema: schemaVectors{"title": entflat.NewDefaultUserConfig()},
			wantErr: `the mapping records a hnsw index at "a-b" but the schema says flat`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			m := loaded(t, dir)
			if tt.rec != nil {
				require.NoError(t, m.records.Put("title", *tt.rec))
			}
			if tt.storage != "" {
				buildHNSW(t, dir, tt.storage)
			}

			entries, firstLoad, err := m.Open(t.Context(), tt.schema)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.False(t, firstLoad)
			if tt.want == "" {
				assert.Empty(t, entries)
			} else {
				require.Len(t, entries, 1)
				assert.Equal(t, tt.want, entries[0].StorageID)
				buildHNSW(t, dir, tt.want)
			}
			require.NoError(t, m.Ready(tt.schema))

			rec, ok, err := m.records.Get("title")
			require.NoError(t, err)
			assert.Equal(t, tt.wantRecord != nil, ok)
			if tt.wantRecord != nil {
				assert.Equal(t, *tt.wantRecord, rec)
			}
		})
	}
}

// Records that share a queue or files refuse the load before anything goes.
func TestManifest_OpenRefusesSharedFiles(t *testing.T) {
	dir := t.TempDir()
	m := loaded(t, dir)
	require.NoError(t, m.records.Put("a", record{PhysicalID: "a-b", IndexType: "hnsw", State: "dropping"}))
	require.NoError(t, m.records.Put("b", record{PhysicalID: "vectors_a-b", IndexType: "hnsw", State: "ready", QueueID: "vectors_b"}))
	bucket := filepath.Join(dir, "lsm", "vectors_a-b")
	plant(t, bucket)

	_, _, err := m.Open(t.Context(), nil)
	require.ErrorContains(t, err, `vectors "a" and "b" both name "vectors_a-b"`)
	assert.DirExists(t, bucket)
}

func TestManifest_Create(t *testing.T) {
	cfg := enthnsw.NewDefaultUserConfig()
	dir := t.TempDir()
	m := loaded(t, dir, "")

	entry, err := m.Create("title", cfg)
	require.NoError(t, err)
	assert.Equal(t, Entry{Name: "title", StorageID: "vectors_title", QueueID: "vectors_title", IndexType: "hnsw", Config: cfg}, entry)
	rec, _, err := m.records.Get("title")
	require.NoError(t, err)
	assert.Equal(t, "creating", rec.State)

	_, err = m.Create("title", cfg)
	require.NoError(t, err, "a retry of an interrupted creation")

	buildHNSW(t, dir, "vectors_title")
	require.NoError(t, m.Ready(schemaVectors{"title": cfg}))
	rec, _, err = m.records.Get("title")
	require.NoError(t, err)
	assert.Equal(t, record{PhysicalID: "vectors_title", IndexType: "hnsw", State: "ready", QueueID: "vectors_title", Config: cfg}, rec)

	_, err = m.Create("compressed", cfg)
	require.ErrorContains(t, err, `share "vectors_compressed"`)
	_, ok, err := m.Get("compressed")
	require.NoError(t, err)
	assert.False(t, ok, "nothing was written")
}

func TestManifest_ReadyRefuses(t *testing.T) {
	cfg := enthnsw.NewDefaultUserConfig()
	m := loaded(t, t.TempDir(), "title")
	require.ErrorContains(t, m.Ready(schemaVectors{"ghost": cfg}), `vector "ghost" has no mapping record`)

	_, err := m.Create("body", cfg)
	require.NoError(t, err)
	require.Error(t, m.Ready(schemaVectors{"body": cfg}), "directories that cannot be synced")
	rec, _, err := m.records.Get("body")
	require.NoError(t, err)
	assert.Equal(t, "creating", rec.State)
}

// A reset keeps the record's IDs, and the files go without the queue or a
// bucket a sibling owns.
func TestManifest_Reset(t *testing.T) {
	cfg := enthnsw.NewDefaultUserConfig()
	dir := t.TempDir()
	m := loaded(t, dir, "title", "title_muvera_vectors")
	queueDir := filepath.Join(dir, "vectors_title.queue.d")
	siblings := filepath.Join(dir, "lsm", "vectors_title_muvera_vectors")
	plant(t, queueDir, siblings)

	require.NoError(t, m.Reset("title"))
	rec, _, err := m.records.Get("title")
	require.NoError(t, err)
	assert.Equal(t, "creating", rec.State)
	require.NoError(t, m.RemoveIndexFiles(t.Context(), "title"))
	assert.NoDirExists(t, filepath.Join(dir, "vectors_title.hnsw.commitlog.d"))
	assert.DirExists(t, queueDir)
	assert.DirExists(t, siblings)

	buildHNSW(t, dir, "vectors_title")
	require.NoError(t, m.Ready(schemaVectors{"title": cfg}))
	entry, ok, err := m.Get("title")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, Entry{Name: "title", StorageID: "vectors_title", QueueID: "vectors_title", IndexType: "hnsw", Config: cfg}, entry)

	require.ErrorContains(t, m.Reset("ghost"), `vector "ghost" has no mapping record`)
}

// A drop deletes what the record names, spares a bucket a sibling owns, and
// leaves the record until the files are gone.
func TestManifest_Drop(t *testing.T) {
	dir := t.TempDir()
	m := loaded(t, dir, "foo", "foo_muvera_vectors")
	own := filepath.Join(dir, "lsm", "vectors_foo")
	queueDir := filepath.Join(dir, "vectors_foo.queue.d")
	siblings := filepath.Join(dir, "lsm", "vectors_foo_muvera_vectors")
	plant(t, own, queueDir, siblings)

	found, err := m.BeginDrop("foo")
	require.NoError(t, err)
	assert.True(t, found)
	rec, _, err := m.records.Get("foo")
	require.NoError(t, err)
	assert.Equal(t, "dropping", rec.State)

	require.NoError(t, m.FinishDrop(t.Context(), "foo"))
	assert.NoDirExists(t, own)
	assert.NoDirExists(t, queueDir)
	assert.NoDirExists(t, filepath.Join(dir, "vectors_foo.hnsw.commitlog.d"))
	assert.DirExists(t, siblings)
	_, ok, err := m.Get("foo")
	require.NoError(t, err)
	assert.False(t, ok)

	require.NoError(t, m.FinishDrop(t.Context(), "foo"), "already dropped")
	found, err = m.BeginDrop("foo")
	require.NoError(t, err)
	assert.False(t, found)
}

// A drop of a vector that shares a queue or files with another one is refused
// before anything is recorded or deleted.
func TestManifest_DropRefusesSharedFiles(t *testing.T) {
	dir := t.TempDir()
	m := loaded(t, dir, "title", "body")
	rec, _, err := m.records.Get("title")
	require.NoError(t, err)
	rec.QueueID = "vectors_body"
	require.NoError(t, m.records.Put("title", rec))
	queueDir := filepath.Join(dir, "vectors_body.queue.d")
	plant(t, queueDir)

	const wantErr = `vectors "body" and "title" both record queue "vectors_body"`
	_, err = m.BeginDrop("title")
	require.ErrorContains(t, err, wantErr)
	got, _, err := m.records.Get("title")
	require.NoError(t, err)
	assert.Equal(t, rec, got)

	rec.State = "dropping"
	require.NoError(t, m.records.Put("title", rec))
	require.ErrorContains(t, m.FinishDrop(t.Context(), "title"), wantErr)
	assert.DirExists(t, queueDir)
}

func TestManifest_DropKeepsTheRecordWhenAFileStays(t *testing.T) {
	dir := t.TempDir()
	m := loaded(t, dir, "foo")
	_, err := m.BeginDrop("foo")
	require.NoError(t, err)
	m.removeBucket = func(context.Context, string) error { return assert.AnError }

	require.ErrorIs(t, m.FinishDrop(t.Context(), "foo"), assert.AnError)
	_, ok, err := m.Get("foo")
	require.NoError(t, err)
	assert.True(t, ok)
}

// A vector created again since its drop began keeps its record and files.
func TestManifest_FinishDropRefusesAVectorNotBeingDropped(t *testing.T) {
	dir := t.TempDir()
	m := loaded(t, dir, "foo")
	_, err := m.BeginDrop("foo")
	require.NoError(t, err)
	_, err = m.Create("foo", enthnsw.NewDefaultUserConfig())
	require.NoError(t, err)

	require.ErrorContains(t, m.FinishDrop(t.Context(), "foo"), `vector "foo" is creating, not dropping`)
	assert.DirExists(t, filepath.Join(dir, "vectors_foo.hnsw.commitlog.d"))
	rec, ok, err := m.records.Get("foo")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "creating", rec.State)
}

// A creation waits for a drop that is deleting files.
func TestManifest_CreateWaitsForAFinishingDrop(t *testing.T) {
	dir := t.TempDir()
	m := loaded(t, dir, "foo")
	_, err := m.BeginDrop("foo")
	require.NoError(t, err)

	created := make(chan error, 1)
	removeBucket := m.removeBucket
	started := false
	m.removeBucket = func(ctx context.Context, bucket string) error {
		if !started {
			started = true
			enterrors.GoWrapper(func() {
				_, err := m.Create("foo", enthnsw.NewDefaultUserConfig())
				created <- err
			}, m.logger)
			select {
			case err := <-created:
				t.Error("created while the drop deletes files")
				created <- err
			case <-time.After(50 * time.Millisecond):
			}
		}
		return removeBucket(ctx, bucket)
	}
	require.NoError(t, m.FinishDrop(t.Context(), "foo"))
	require.NoError(t, <-created)
	rec, ok, err := m.records.Get("foo")
	require.NoError(t, err)
	require.True(t, ok, "the new record outlives the drop")
	assert.Equal(t, "creating", rec.State)
}

func TestManifest_HasStorage(t *testing.T) {
	dir := t.TempDir()
	m := loaded(t, dir)
	e := Entry{Name: "title", StorageID: "vectors_title", IndexType: "hnsw"}
	exists, err := m.HasStorage(e)
	require.NoError(t, err)
	assert.False(t, exists)
	buildHNSW(t, dir, "vectors_title")
	exists, err = m.HasStorage(e)
	require.NoError(t, err)
	assert.True(t, exists)
}

// rebuildOf is a rebuild from source to candidate.
func rebuildOf(source, candidate string) rebuildTask {
	return rebuildTask{
		TaskID: "task-1", TaskVersion: 1,
		Source:    taskIndex{PhysicalID: source, IndexType: "hnsw"},
		Candidate: taskIndex{PhysicalID: candidate, IndexType: "hfresh"},
	}
}

// files lists the buckets and directories planted for an index at id.
func files(dir, id string) []string {
	return []string{filepath.Join(dir, "lsm", id), filepath.Join(dir, id+".hfresh.d")}
}

// The index the record names stays, the other one its task names goes, then
// the task.
func TestManifest_OpenFinishesARebuild(t *testing.T) {
	tests := []struct {
		name     string
		record   func(record) *record
		inSchema bool
		kept     string
		removed  []string
		// rewound is whether the chunks kept during the rebuild are served again
		rewound bool
	}{
		{
			name: "the record names the source", record: func(r record) *record { return &r },
			inSchema: true, kept: "vectors_title", removed: []string{"vectors_a-b"},
		},
		{
			name: "the record names the candidate", record: func(r record) *record { r.PhysicalID = "a-b"; return &r },
			inSchema: true, kept: "vectors_a-b", removed: []string{"vectors_title"}, rewound: true,
		},
		{
			name: "dropping", record: func(r record) *record { r.State = "dropping"; return &r },
			removed: []string{"vectors_title", "vectors_a-b"},
		},
		{
			name: "dropping an index neither of them", record: func(r record) *record {
				r.State, r.PhysicalID = "dropping", "c-d"
				return &r
			},
			removed: []string{"vectors_title", "vectors_a-b", "vectors_c-d"},
		},
		{
			name: "no record and gone from the schema", record: func(record) *record { return nil },
			removed: []string{"vectors_title", "vectors_a-b"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			m := loaded(t, dir, "title", "body")
			cfg := enthnsw.NewDefaultUserConfig()
			rec, _, err := m.records.Get("title")
			require.NoError(t, err)
			require.NoError(t, m.records.Delete("title"))
			if want := tt.record(rec); want != nil {
				require.NoError(t, m.records.Put("title", *want))
			}
			require.NoError(t, m.tasks.Put("title", rebuildOf("vectors_title", "a-b")))
			for _, id := range []string{"vectors_title", "vectors_a-b", "vectors_c-d", "vectors_body"} {
				plant(t, files(dir, id)...)
			}
			buildHNSW(t, dir, "vectors_a-b")
			chunk := filepath.Join(dir, "vectors_title.queue.d", "chunk-1.bin")
			plant(t, filepath.Dir(chunk))
			require.NoError(t, os.WriteFile(chunk, []byte("x"), 0o600))
			require.NoError(t, os.WriteFile(chunk+".processed", nil, 0o600))

			schema := schemaVectors{"body": cfg}
			if tt.inSchema {
				schema["title"] = cfg
			}
			entries, _, err := m.Open(t.Context(), schema)
			require.NoError(t, err)
			require.Len(t, entries, len(schema))

			for _, id := range tt.removed {
				for _, path := range files(dir, id) {
					assert.NoDirExists(t, path)
				}
			}
			for _, id := range []string{"vectors_body", tt.kept} {
				if id == "" {
					continue
				}
				for _, path := range files(dir, id) {
					assert.DirExists(t, path)
				}
			}
			if tt.kept != "" {
				assert.Equal(t, tt.kept, entries[1].StorageID)
				assert.Equal(t, "vectors_title", entries[1].QueueID)
				assert.FileExists(t, chunk)
				assert.Equal(t, !tt.rewound, pathExists(chunk+".processed"))
			}
			_, ok, err := m.tasks.Get("title")
			require.NoError(t, err)
			assert.False(t, ok)
		})
	}
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// A task the records cannot account for fails the load before anything goes.
func TestManifest_OpenRefusesATask(t *testing.T) {
	cfg := enthnsw.NewDefaultUserConfig()
	title := record{PhysicalID: "vectors_title", IndexType: "hnsw", State: "ready", QueueID: "vectors_title", Config: cfg}
	tests := []struct {
		name    string
		change  func(t *testing.T, m *Manifest)
		wantErr string
	}{
		{
			name: "the record names neither index",
			change: func(t *testing.T, m *Manifest) {
				other := title
				other.PhysicalID = "c-d"
				require.NoError(t, m.records.Put("title", other))
				require.NoError(t, m.tasks.Put("title", rebuildOf("vectors_title", "a-b")))
			},
			wantErr: `vector "title": the mapping names "c-d", the rebuild task names source "vectors_title" and candidate "a-b"`,
		},
		{
			name: "a vector of the schema without a record",
			change: func(t *testing.T, m *Manifest) {
				require.NoError(t, m.records.Delete("title"))
				require.NoError(t, m.tasks.Put("title", rebuildOf("vectors_title", "a-b")))
			},
			wantErr: `vector "title": a rebuild task but no record`,
		},
		{
			name: "the candidate is another vector's index",
			change: func(t *testing.T, m *Manifest) {
				require.NoError(t, m.tasks.Put("title", rebuildOf("vectors_title", "vectors_body")))
			},
			wantErr: `the rebuild task of vector "title" names "vectors_body", an index of vector "body"`,
		},
		{
			name: "the source is another vector's index",
			change: func(t *testing.T, m *Manifest) {
				require.NoError(t, m.tasks.Put("title", rebuildOf("vectors_body", "vectors_title")))
			},
			wantErr: `the rebuild task of vector "title" names "vectors_body", an index of vector "body"`,
		},
		{
			name: "two tasks name one candidate",
			change: func(t *testing.T, m *Manifest) {
				require.NoError(t, m.tasks.Put("body", rebuildOf("vectors_body", "a-b")))
				require.NoError(t, m.tasks.Put("title", rebuildOf("vectors_title", "vectors_a-b")))
			},
			wantErr: `the rebuild task of vector "body" names "a-b", an index of vector "title"`,
		},
		{
			name: "the candidate shares a bucket with another vector's index",
			change: func(t *testing.T, m *Manifest) {
				body := title
				body.PhysicalID, body.QueueID = "x-1", "vectors_body"
				require.NoError(t, m.records.Put("body", body))
				require.NoError(t, m.tasks.Put("title", rebuildOf("vectors_title", "x-1_centroids")))
			},
			wantErr: `the rebuild task of vector "title" names "x-1_centroids", which shares "vectors_compressed_x-1_centroids" with vector "body"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			m := loaded(t, dir, "title", "body")
			ids := []string{"vectors_title", "vectors_body", "vectors_a-b", "vectors_c-d", "vectors_x-1", "vectors_x-1_centroids"}
			for _, id := range ids {
				plant(t, files(dir, id)...)
			}
			tt.change(t, m)

			_, _, err := m.Open(t.Context(), schemaVectors{"title": cfg, "body": cfg})
			require.ErrorContains(t, err, tt.wantErr)
			for _, id := range ids {
				for _, path := range files(dir, id) {
					assert.DirExists(t, path, "nothing is deleted")
				}
			}
		})
	}
}

func TestManifest_OpenRefusesTasksWithoutRecords(t *testing.T) {
	m := openTestManifest(t, t.TempDir())
	require.NoError(t, m.tasks.Put("title", rebuildOf("vectors_title", "a-b")))
	_, _, err := m.Open(t.Context(), nil)
	require.ErrorContains(t, err, "rebuild tasks without a vector index mapping")
}

// The tasks of vectors whose names share a file, which predates the check, do
// not refuse the load: the shared bucket is spared instead.
func TestManifest_OpenAcceptsATaskOfCollidingNames(t *testing.T) {
	dir := t.TempDir()
	m := loaded(t, dir, "foo", "foo_muvera_vectors")
	require.NoError(t, m.tasks.Put("foo", rebuildOf("vectors_foo", "a-b")))
	rec, _, err := m.records.Get("foo")
	require.NoError(t, err)
	rec.PhysicalID = "a-b"
	require.NoError(t, m.records.Put("foo", rec))
	buildHNSW(t, dir, "vectors_a-b")
	shared := filepath.Join(dir, "lsm", "vectors_foo_muvera_vectors")
	own := filepath.Join(dir, "lsm", "vectors_foo")
	plant(t, shared, own)

	cfg := enthnsw.NewDefaultUserConfig()
	_, _, err = m.Open(t.Context(), schemaVectors{"foo": cfg, "foo_muvera_vectors": cfg})
	require.NoError(t, err)
	assert.DirExists(t, shared)
	assert.NoDirExists(t, own)
}

// A drop during a rebuild deletes both indexes, then the record and the task.
func TestManifest_DropDuringARebuild(t *testing.T) {
	for _, recorded := range []string{"vectors_title", "a-b"} {
		t.Run(recorded, func(t *testing.T) {
			dir := t.TempDir()
			m := loaded(t, dir, "title", "body")
			rec, _, err := m.records.Get("title")
			require.NoError(t, err)
			rec.PhysicalID = recorded
			require.NoError(t, m.records.Put("title", rec))
			require.NoError(t, m.tasks.Put("title", rebuildOf("vectors_title", "a-b")))
			for _, id := range []string{"vectors_title", "vectors_a-b", "vectors_body"} {
				plant(t, files(dir, id)...)
			}

			found, err := m.BeginDrop("title")
			require.NoError(t, err)
			assert.True(t, found)
			require.NoError(t, m.FinishDrop(t.Context(), "title"))

			for _, path := range append(files(dir, "vectors_title"), files(dir, "vectors_a-b")...) {
				assert.NoDirExists(t, path)
			}
			for _, path := range files(dir, "vectors_body") {
				assert.DirExists(t, path)
			}
			_, ok, err := m.records.Get("title")
			require.NoError(t, err)
			assert.False(t, ok)
			_, ok, err = m.tasks.Get("title")
			require.NoError(t, err)
			assert.False(t, ok)
		})
	}
}

// A drop refuses a task that names another vector's index.
func TestManifest_DropRefusesATaskNamingASibling(t *testing.T) {
	dir := t.TempDir()
	m := loaded(t, dir, "title", "body")
	require.NoError(t, m.tasks.Put("title", rebuildOf("vectors_title", "vectors_body")))
	plant(t, files(dir, "vectors_title")...)
	plant(t, files(dir, "vectors_body")...)

	_, err := m.BeginDrop("title")
	require.ErrorContains(t, err, `an index of vector "body"`)
	rec, _, err := m.records.Get("title")
	require.NoError(t, err)
	assert.Equal(t, "ready", rec.State)

	rec.State = "dropping"
	require.NoError(t, m.records.Put("title", rec))
	require.ErrorContains(t, m.FinishDrop(t.Context(), "title"), `an index of vector "body"`)
	for _, path := range append(files(dir, "vectors_title"), files(dir, "vectors_body")...) {
		assert.DirExists(t, path)
	}
}
