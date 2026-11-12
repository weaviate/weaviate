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
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"

	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
	entlsmkv "github.com/weaviate/weaviate/entities/lsmkv"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	entdynamic "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	entflat "github.com/weaviate/weaviate/entities/vectorindex/flat"
	enthfresh "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func newTestVectorIndexMapping(t *testing.T) (*vectorIndexMapping, *shardmeta.DB) {
	t.Helper()
	db, err := shardmeta.Open(t.TempDir(), entlsmkv.BoltFlockTimeout)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return newVectorIndexMapping(db), db
}

// recorded is name's record as the shard writes it today: built at
// physicalID with its queue beside it, holding the schema's config.
func recorded(shard *Shard, name, physicalID, state string) vectorIndexRecord {
	cfg := shard.index.GetVectorIndexConfig(name)
	return vectorIndexRecord{PhysicalID: physicalID, IndexType: cfg.IndexType(), State: state, QueueID: physicalID, Config: cfg}
}

// seedGeneratedVectorIndexRecords initializes the mapping of the shard at
// shardDir before its first load, every vector in configs (the legacy one
// under the empty name) recorded creating at a generated physical and queue
// ID, as a rebuild names them. The first load builds each index there.
func seedGeneratedVectorIndexRecords(t *testing.T, shardDir string,
	configs map[string]schemaConfig.VectorIndexConfig,
) map[string]vectorIndexRecord {
	t.Helper()
	require.NoError(t, os.MkdirAll(shardDir, 0o755))
	db, err := shardmeta.Open(shardDir, entlsmkv.BoltFlockTimeout)
	require.NoError(t, err)
	defer db.Close()
	records := make(map[string]vectorIndexRecord, len(configs))
	for name, cfg := range configs {
		if !vectorIndexHasStorage(cfg) {
			continue
		}
		records[name] = vectorIndexRecord{
			PhysicalID: uuid.NewString(),
			IndexType:  cfg.IndexType(),
			State:      vectorIndexStateCreating,
			QueueID:    uuid.NewString(),
			Config:     cfg,
		}
	}
	require.NoError(t, newVectorIndexMapping(db).Initialize(records))
	return records
}

// TestVectorIndexMapping_KeyLayout pins the on-disk keys. They are read by
// every later version, so a change here is a format change, not a rename.
func TestVectorIndexMapping_KeyLayout(t *testing.T) {
	assert.Equal(t, "vector_index_mapping", vectorIndexMappingNamespace)
	assert.Equal(t, ".format_version", vectorIndexMappingFormatVersionKey)
	assert.Equal(t, "1", vectorIndexMappingFormatVersion)

	tests := []struct {
		name string
		key  string
	}{
		{name: "", key: ".legacy"},
		{name: "title", key: "title"},
		{name: "default", key: "default"},
		{name: "legacy", key: "legacy"},
		{name: "format_version", key: "format_version"},
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			assert.Equal(t, tt.key, vectorIndexMappingKey(tt.name))
			name, ok := vectorIndexMappingName(tt.key)
			require.True(t, ok)
			assert.Equal(t, tt.name, name)
		})
	}

	// keys that name no vector: the format version, an unknown dotted key,
	// and an empty key
	for _, key := range []string{".format_version", ".unknown", ""} {
		_, ok := vectorIndexMappingName(key)
		assert.False(t, ok, key)
	}
}

func TestVectorIndexMapping_Load(t *testing.T) {
	put := func(t *testing.T, db *shardmeta.DB, key, value string) {
		t.Helper()
		require.NoError(t, db.Namespace(vectorIndexMappingNamespace).Put([]byte(key), []byte(value)))
	}

	t.Run("empty db is uninitialized", func(t *testing.T) {
		m, _ := newTestVectorIndexMapping(t)
		records, initialized, err := m.Load()
		require.NoError(t, err)
		assert.False(t, initialized)
		assert.Empty(t, records)
	})

	// a deferred deletion the process did not finish; the next load does
	t.Run("a dropping record loads", func(t *testing.T) {
		m, db := newTestVectorIndexMapping(t)
		put(t, db, ".format_version", "1")
		put(t, db, "title", `{"physical_id":"vectors_title","index_type":"hnsw","state":"dropping"}`)
		records, initialized, err := m.Load()
		require.NoError(t, err)
		assert.True(t, initialized)
		assert.Equal(t, vectorIndexRecord{PhysicalID: "vectors_title", IndexType: "hnsw", State: "dropping"}, records["title"])
	})

	t.Run("reads every record under its logical name", func(t *testing.T) {
		m, db := newTestVectorIndexMapping(t)
		put(t, db, ".format_version", "1")
		put(t, db, ".legacy", `{"physical_id":"main","index_type":"hnsw","state":"ready"}`)
		put(t, db, "title", `{"physical_id":"vectors_title","index_type":"dynamic","state":"ready"}`)
		put(t, db, "summary", `{"physical_id":"vectors_summary","index_type":"flat","state":"creating"}`)

		records, initialized, err := m.Load()
		require.NoError(t, err)
		assert.True(t, initialized)
		assert.Equal(t, map[string]vectorIndexRecord{
			"":        {PhysicalID: "main", IndexType: "hnsw", State: "ready"},
			"title":   {PhysicalID: "vectors_title", IndexType: "dynamic", State: "ready"},
			"summary": {PhysicalID: "vectors_summary", IndexType: "flat", State: "creating"},
		}, records)
	})

	t.Run("initialized with no records", func(t *testing.T) {
		m, db := newTestVectorIndexMapping(t)
		put(t, db, ".format_version", "1")
		records, initialized, err := m.Load()
		require.NoError(t, err)
		assert.True(t, initialized)
		assert.Empty(t, records)
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
		{name: "unparsable value", key: "title", value: "{", wantErr: `record "title"`},
		{name: "unknown state", key: "title", value: `{"physical_id":"vectors_title","index_type":"hnsw","state":"gone"}`, wantErr: `state "gone"`},
		{name: "empty physical id", key: "title", value: `{"physical_id":"","index_type":"hnsw","state":"ready"}`, wantErr: "physical id"},
		{name: "empty index type", key: "title", value: `{"physical_id":"vectors_title","index_type":"","state":"ready"}`, wantErr: "index type"},
		{name: "record without format version", noVersion: true, key: "title", value: `{"physical_id":"vectors_title","index_type":"hnsw","state":"ready"}`, wantErr: "format version"},
		{name: "config of an unknown index type", key: "title", value: `{"physical_id":"vectors_title","index_type":"none","state":"ready","config":{}}`, wantErr: `unknown vector index type "none"`},
		{name: "unparsable config", key: "title", value: `{"physical_id":"vectors_title","index_type":"hnsw","state":"ready","config":{"ef":"many"}}`, wantErr: `record "title": config`},
		{name: "physical id outside the naming rule", key: "title", value: `{"physical_id":"title","index_type":"hnsw","state":"ready"}`, wantErr: `physical id: "title" is neither`},
		{name: "physical id with a character a file name must not have", key: "title", value: `{"physical_id":"a/b-c","index_type":"hnsw","state":"ready"}`, wantErr: `physical id: "a/b-c" is neither`},
		{name: "physical id naming an invalid vector", key: "title", value: `{"physical_id":"vectors_1title","index_type":"hnsw","state":"ready"}`, wantErr: `physical id: "vectors_1title" is neither`},
		{name: "queue id outside the naming rule", key: "title", value: `{"physical_id":"vectors_title","index_type":"hnsw","state":"ready","queue_id":"title.q"}`, wantErr: `queue id: "title.q" is neither`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, db := newTestVectorIndexMapping(t)
			if !tt.noVersion {
				put(t, db, ".format_version", "1")
			}
			put(t, db, tt.key, tt.value)
			_, _, err := m.Load()
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// Every ID the naming rule gives a vector name loads, and so does a generated
// ID, which has a "-"; a record without a queue ID predates the field.
func TestVectorIndexMapping_LoadAcceptedIDs(t *testing.T) {
	tests := []struct {
		name       string
		physicalID string
		queueID    string
	}{
		{name: "legacy", physicalID: "main", queueID: "main"},
		{name: "named", physicalID: "vectors_title", queueID: "vectors_title"},
		{name: "named with an underscore first", physicalID: "vectors__title_2", queueID: "vectors__title_2"},
		{name: "generated", physicalID: "3f2a9c1e-0b7d-4c1a-9e2f-5d6c7b8a9f00", queueID: "8c1d2e3f-4a5b-4c6d-8e7f-9a0b1c2d3e4f"},
		{name: "generated with a vectors prefix", physicalID: "vectors_title-2", queueID: "vectors_title-2"},
		{name: "no queue id", physicalID: "vectors_title"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, _ := newTestVectorIndexMapping(t)
			rec := vectorIndexRecord{PhysicalID: tt.physicalID, IndexType: "hnsw", State: "ready", QueueID: tt.queueID}
			require.NoError(t, m.Initialize(map[string]vectorIndexRecord{"title": rec}))
			records, _, err := m.Load()
			require.NoError(t, err)
			assert.Equal(t, rec, records["title"])
		})
	}
}

// The naming rule: the IDs a vector name gets keep their files, any other ID
// is named as if it carried the vectors_ prefix, never as the legacy vector.
func TestVectorIndexStorageID(t *testing.T) {
	tests := []struct {
		physicalID string
		want       string
	}{
		{physicalID: "main", want: "main"},
		{physicalID: "vectors_title", want: "vectors_title"},
		{physicalID: "vectors_title-2", want: "vectors_title-2"},
		{physicalID: "3f2a9c1e-0b7d", want: "vectors_3f2a9c1e-0b7d"},
	}
	for _, tt := range tests {
		t.Run(tt.physicalID, func(t *testing.T) {
			rec := vectorIndexRecord{PhysicalID: tt.physicalID}
			assert.Equal(t, tt.want, rec.storageID())
			assert.Equal(t, tt.physicalID, rec.recordedID())
		})
	}
}

// Only the mapping reads a record's raw physical ID. Anywhere else a raw ID
// that is not main or vectors_<x> would name the legacy vector's files.
func TestVectorIndexRecord_PhysicalIDReadOnlyByTheMapping(t *testing.T) {
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	fset := token.NewFileSet()
	var offenders []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") ||
			name == "shard_vector_mapping.go" {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		require.NoError(t, err)
		ast.Inspect(f, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if ok && sel.Sel.Name == "PhysicalID" {
				offenders = append(offenders, fset.Position(sel.Pos()).String())
			}
			return true
		})
	}
	assert.Empty(t, offenders, "read the physical ID through storageID, or recordedID for messages")
}

// The schema's collision check reads vector names only. It is complete
// because every file name a generated ID owns has a "-" and no file name a
// vector name owns has one, so the two can never share a file.
func TestVectorIndexGeneratedIDsNeverCollideWithNames(t *testing.T) {
	generated := []string{"3f2a9c1e-0b7d-4c1a-9e2f-5d6c7b8a9f00", "a-b", "-", "x_-_y"}
	for _, id := range generated {
		require.NoError(t, validateVectorIndexID(id), id)
		for _, n := range ownerOf(vectorIndexRecord{PhysicalID: id, QueueID: id}).names() {
			assert.Contains(t, n, "-", "generated ID %q owns %q", id, n)
		}
	}

	names := []string{"", "title", "_", "compressed", "a_centroids", "x", "Muvera_Vectors", "mv_mappings"}
	for _, name := range names {
		require.Regexp(t, `^(`+schema.TargetVectorNameRegex+`)?$`, name)
		for _, n := range ownerAt(vectorIndexID(name)).names() {
			assert.NotContains(t, n, "-", "vector %q owns %q", name, n)
		}
	}

	// without a "-" an ID outside the naming rule would be one a vector name can reach
	for _, id := range []string{"title", "vectors_", "abc_def", "vectors_9x"} {
		assert.Error(t, validateVectorIndexID(id), id)
	}
}

// A record written by a newer version carries fields this one ignores; the
// strip rewrites exactly those records and leaves a mapping it wrote alone.
func TestVectorIndexMapping_LoadAndStrip(t *testing.T) {
	const (
		own   = `{"physical_id":"vectors_title","index_type":"hnsw","state":"ready"}`
		newer = `{"physical_id":"main","index_type":"hnsw","state":"ready","queue_id":"main","future":{"ef":64}}`
	)
	// lastTxID opens the closed index.db directly: bolt's transaction ID
	// moves on every committed write, even one that changes nothing
	lastTxID := func(t *testing.T, dir string) int {
		t.Helper()
		db, err := bbolt.Open(filepath.Join(dir, shardmeta.FileName), 0o600, nil)
		require.NoError(t, err)
		defer db.Close()
		tx, err := db.Begin(false)
		require.NoError(t, err)
		defer tx.Rollback()
		return tx.ID()
	}
	loadAndStrip := func(t *testing.T, dir string) (map[string]vectorIndexRecord, []string, map[string]string) {
		t.Helper()
		db, err := shardmeta.Open(dir, entlsmkv.BoltFlockTimeout)
		require.NoError(t, err)
		defer db.Close()
		records, initialized, stripped, err := newVectorIndexMapping(db).LoadAndStrip()
		require.NoError(t, err)
		assert.True(t, initialized)
		raw := map[string]string{}
		require.NoError(t, db.Namespace(vectorIndexMappingNamespace).ForEach(func(k, v []byte) error {
			raw[string(k)] = string(v)
			return nil
		}))
		return records, stripped, raw
	}
	setup := func(t *testing.T, values map[string]string) string {
		t.Helper()
		dir := t.TempDir()
		db, err := shardmeta.Open(dir, entlsmkv.BoltFlockTimeout)
		require.NoError(t, err)
		defer db.Close()
		ns := db.Namespace(vectorIndexMappingNamespace)
		require.NoError(t, ns.Put([]byte(".format_version"), []byte("1")))
		for k, v := range values {
			require.NoError(t, ns.Put([]byte(k), []byte(v)))
		}
		return dir
	}

	t.Run("records of a newer version are rewritten without its fields", func(t *testing.T) {
		dir := setup(t, map[string]string{".legacy": newer, "title": own, "summary": newer})
		records, stripped, raw := loadAndStrip(t, dir)
		assert.Equal(t, []string{"", "summary"}, stripped)
		assert.Equal(t, map[string]vectorIndexRecord{
			"":        {PhysicalID: "main", IndexType: "hnsw", State: "ready", QueueID: "main"},
			"title":   {PhysicalID: "vectors_title", IndexType: "hnsw", State: "ready"},
			"summary": {PhysicalID: "main", IndexType: "hnsw", State: "ready", QueueID: "main"},
		}, records)
		assert.Equal(t, map[string]string{
			".format_version": "1",
			".legacy":         `{"physical_id":"main","index_type":"hnsw","state":"ready","queue_id":"main"}`,
			"title":           own,
			"summary":         `{"physical_id":"main","index_type":"hnsw","state":"ready","queue_id":"main"}`,
		}, raw)

		// the next load finds nothing to strip and does not write
		before := lastTxID(t, dir)
		_, stripped, _ = loadAndStrip(t, dir)
		assert.Empty(t, stripped)
		assert.Equal(t, before, lastTxID(t, dir))
	})

	t.Run("a mapping this version wrote is not written to", func(t *testing.T) {
		dir := setup(t, map[string]string{"title": own})
		before := lastTxID(t, dir)
		records, stripped, raw := loadAndStrip(t, dir)
		assert.Empty(t, stripped)
		assert.Equal(t, map[string]vectorIndexRecord{
			"title": {PhysicalID: "vectors_title", IndexType: "hnsw", State: "ready"},
		}, records)
		assert.Equal(t, map[string]string{".format_version": "1", "title": own}, raw)
		assert.Equal(t, before, lastTxID(t, dir))
	})

	t.Run("an uninitialized mapping is not written to", func(t *testing.T) {
		dir := t.TempDir()
		db, err := shardmeta.Open(dir, entlsmkv.BoltFlockTimeout)
		require.NoError(t, err)
		require.NoError(t, db.Close())
		before := lastTxID(t, dir)
		db, err = shardmeta.Open(dir, entlsmkv.BoltFlockTimeout)
		require.NoError(t, err)
		records, initialized, stripped, err := newVectorIndexMapping(db).LoadAndStrip()
		require.NoError(t, err)
		require.NoError(t, db.Close())
		assert.False(t, initialized)
		assert.Empty(t, records)
		assert.Empty(t, stripped)
		assert.Equal(t, before, lastTxID(t, dir))
	})

	// a record that does not validate refuses the load before anything is
	// written, as Load does
	t.Run("an invalid record refuses the load", func(t *testing.T) {
		dir := setup(t, map[string]string{".legacy": newer, "title": `{"physical_id":"","index_type":"hnsw","state":"ready","config":{}}`})
		db, err := shardmeta.Open(dir, entlsmkv.BoltFlockTimeout)
		require.NoError(t, err)
		_, _, _, err = newVectorIndexMapping(db).LoadAndStrip()
		require.ErrorContains(t, err, "physical id")
		v, err := db.Namespace(vectorIndexMappingNamespace).Get([]byte(".legacy"))
		require.NoError(t, err)
		assert.Equal(t, newer, string(v))
		require.NoError(t, db.Close())
	})
}

func TestVectorIndexMapping_Initialize(t *testing.T) {
	t.Run("writes the format version and every record at once", func(t *testing.T) {
		m, db := newTestVectorIndexMapping(t)
		err := m.Initialize(map[string]vectorIndexRecord{
			"":      {PhysicalID: "main", IndexType: "hnsw", State: "ready"},
			"title": {PhysicalID: "vectors_title", IndexType: "flat", State: "ready"},
		})
		require.NoError(t, err)

		records, initialized, err := m.Load()
		require.NoError(t, err)
		assert.True(t, initialized)
		assert.Len(t, records, 2)
		assert.Equal(t, vectorIndexRecord{PhysicalID: "main", IndexType: "hnsw", State: "ready"}, records[""])
		assert.Equal(t, vectorIndexRecord{PhysicalID: "vectors_title", IndexType: "flat", State: "ready"}, records["title"])

		// the bytes on disk are the pinned layout
		ns := db.Namespace(vectorIndexMappingNamespace)
		v, err := ns.Get([]byte(".format_version"))
		require.NoError(t, err)
		assert.Equal(t, "1", string(v))
		v, err = ns.Get([]byte("title"))
		require.NoError(t, err)
		assert.JSONEq(t, `{"physical_id":"vectors_title","index_type":"flat","state":"ready"}`, string(v))
	})

	t.Run("no records is a valid initialization", func(t *testing.T) {
		m, _ := newTestVectorIndexMapping(t)
		require.NoError(t, m.Initialize(nil))
		records, initialized, err := m.Load()
		require.NoError(t, err)
		assert.True(t, initialized)
		assert.Empty(t, records)
	})

	t.Run("refuses to initialize twice", func(t *testing.T) {
		m, _ := newTestVectorIndexMapping(t)
		require.NoError(t, m.Initialize(nil))
		err := m.Initialize(map[string]vectorIndexRecord{
			"title": {PhysicalID: "vectors_title", IndexType: "flat", State: "ready"},
		})
		require.ErrorIs(t, err, errVectorIndexMappingInitialized)
		records, _, err := m.Load()
		require.NoError(t, err)
		assert.Empty(t, records)
	})

	t.Run("an invalid record writes nothing", func(t *testing.T) {
		m, _ := newTestVectorIndexMapping(t)
		err := m.Initialize(map[string]vectorIndexRecord{
			"":      {PhysicalID: "main", IndexType: "hnsw", State: "ready"},
			"title": {PhysicalID: "vectors_title", IndexType: "flat", State: "bogus"},
		})
		require.ErrorContains(t, err, `record "title"`)
		records, initialized, err := m.Load()
		require.NoError(t, err)
		assert.False(t, initialized)
		assert.Empty(t, records)
	})
}

func TestVectorIndexMapping_Put(t *testing.T) {
	t.Run("writes and overwrites one record", func(t *testing.T) {
		m, _ := newTestVectorIndexMapping(t)
		require.NoError(t, m.Initialize(nil))

		creating := vectorIndexRecord{PhysicalID: "vectors_title", IndexType: "hnsw", State: "creating"}
		require.NoError(t, m.Put("title", creating))
		records, _, err := m.Load()
		require.NoError(t, err)
		assert.Equal(t, map[string]vectorIndexRecord{"title": creating}, records)

		ready := creating
		ready.State = "ready"
		require.NoError(t, m.Put("title", ready))
		records, _, err = m.Load()
		require.NoError(t, err)
		assert.Equal(t, map[string]vectorIndexRecord{"title": ready}, records)

		// the legacy vector is the empty name
		legacy := vectorIndexRecord{PhysicalID: "main", IndexType: "hnsw", State: "ready"}
		require.NoError(t, m.Put("", legacy))
		records, _, err = m.Load()
		require.NoError(t, err)
		assert.Equal(t, map[string]vectorIndexRecord{"title": ready, "": legacy}, records)
	})

	t.Run("refuses an uninitialized mapping", func(t *testing.T) {
		m, _ := newTestVectorIndexMapping(t)
		err := m.Put("title", vectorIndexRecord{PhysicalID: "vectors_title", IndexType: "hnsw", State: "ready"})
		require.ErrorIs(t, err, errVectorIndexMappingUninitialized)
		_, initialized, err := m.Load()
		require.NoError(t, err)
		assert.False(t, initialized)
	})

	tests := []struct {
		name    string
		rec     vectorIndexRecord
		wantErr string
	}{
		{name: "unknown state", rec: vectorIndexRecord{PhysicalID: "vectors_title", IndexType: "hnsw", State: "done"}, wantErr: `state "done"`},
		{name: "empty physical id", rec: vectorIndexRecord{IndexType: "hnsw", State: "ready"}, wantErr: "physical id"},
		{name: "empty index type", rec: vectorIndexRecord{PhysicalID: "vectors_title", State: "ready"}, wantErr: "index type"},
		{name: "config of another type", rec: vectorIndexRecord{PhysicalID: "vectors_title", IndexType: "hnsw", State: "ready", Config: entflat.NewDefaultUserConfig()}, wantErr: "config is for a flat index, the record says hnsw"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, _ := newTestVectorIndexMapping(t)
			require.NoError(t, m.Initialize(nil))
			err := m.Put("title", tt.rec)
			require.ErrorContains(t, err, tt.wantErr)
			records, _, err := m.Load()
			require.NoError(t, err)
			assert.Empty(t, records)
		})
	}
}

func TestVectorIndexMapping_Get(t *testing.T) {
	m, db := newTestVectorIndexMapping(t)
	foo := vectorIndexRecord{PhysicalID: "vectors_foo", IndexType: "hnsw", State: "ready"}
	require.NoError(t, m.Initialize(map[string]vectorIndexRecord{"foo": foo}))

	rec, ok, err := m.Get("foo")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, foo, rec)

	_, ok, err = m.Get("bar")
	require.NoError(t, err)
	assert.False(t, ok)

	require.NoError(t, db.Namespace(vectorIndexMappingNamespace).Put([]byte("broken"), []byte("{")))
	_, _, err = m.Get("broken")
	require.ErrorContains(t, err, `record "broken"`)
}

// A record whose queue outlived the index it was created with keeps both
// IDs through a write and a read.
func TestVectorIndexMapping_QueueIDOtherThanThePhysicalID(t *testing.T) {
	m, _ := newTestVectorIndexMapping(t)
	require.NoError(t, m.Initialize(nil))
	title := vectorIndexRecord{PhysicalID: "vectors_title_2", IndexType: "hnsw", State: "ready", QueueID: "vectors_title"}
	require.NoError(t, m.Put("title", title))

	rec, ok, err := m.Get("title")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, title, rec)
	assert.Equal(t, "vectors_title", rec.queueIDOrDefault())
}

// A record without a queue ID finds its queue where its index's files are,
// under the naming rule, and adopting the record persists that ID.
func TestVectorIndexRecord_MissingQueueIDFollowsTheNamingRule(t *testing.T) {
	for _, tc := range []struct{ physicalID, want string }{
		{"main", "main"},
		{"vectors_title", "vectors_title"},
		{"abc-1", "vectors_abc-1"},
	} {
		rec := vectorIndexRecord{PhysicalID: tc.physicalID, IndexType: "hnsw", State: "ready"}
		assert.Equal(t, tc.want, rec.queueIDOrDefault(), tc.physicalID)
		adopted, changed := rec.adopted(nil)
		assert.True(t, changed, tc.physicalID)
		assert.Equal(t, tc.want, adopted.QueueID, tc.physicalID)
	}
}

// Records that name one file refuse the load when either has a generated ID;
// collisions between name-derived IDs predate the check and still load.
func TestRefuseGeneratedVectorIndexCollisions(t *testing.T) {
	rec := func(id string) vectorIndexRecord {
		return vectorIndexRecord{PhysicalID: id, IndexType: "hnsw", State: "ready", QueueID: id}
	}
	tests := []struct {
		name    string
		a, b    string
		wantErr string
	}{
		{"one storage ID", "a-b", "vectors_a-b", `both name "vectors_a-b"`},
		{"centroids of one, compressed of the other", "x-1", "x-1_centroids", `both name "vectors_compressed_x-1_centroids"`},
		{"compressed of one, vectors of the other", "a-b", "compressed_a-b", `both name "vectors_compressed_a-b"`},
		{"distinct generated IDs", "a-b", "c-d", ""},
		{"generated and name-derived", "a-b", "vectors_title", ""},
		{"name-derived only", "main", "vectors_compressed", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := refuseGeneratedVectorIndexCollisions(map[string]vectorIndexRecord{"a": rec(tc.a), "b": rec(tc.b)})
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
			require.ErrorContains(t, err, `vectors "a" and "b"`)
		})
	}
}

func TestVectorIndexMapping_Delete(t *testing.T) {
	t.Run("removes one record and leaves the rest", func(t *testing.T) {
		m, _ := newTestVectorIndexMapping(t)
		legacy := vectorIndexRecord{PhysicalID: "main", IndexType: "hnsw", State: "ready"}
		title := vectorIndexRecord{PhysicalID: "vectors_title", IndexType: "flat", State: "ready"}
		require.NoError(t, m.Initialize(map[string]vectorIndexRecord{"": legacy, "title": title}))

		require.NoError(t, m.Delete("title"))
		records, initialized, err := m.Load()
		require.NoError(t, err)
		assert.True(t, initialized)
		assert.Equal(t, map[string]vectorIndexRecord{"": legacy}, records)

		require.NoError(t, m.Delete(""))
		records, initialized, err = m.Load()
		require.NoError(t, err)
		assert.True(t, initialized)
		assert.Empty(t, records)
	})

	t.Run("a missing record is already deleted", func(t *testing.T) {
		m, _ := newTestVectorIndexMapping(t)
		require.NoError(t, m.Initialize(nil))
		require.NoError(t, m.Delete("title"))
	})

	t.Run("an uninitialized mapping has nothing to delete", func(t *testing.T) {
		m, _ := newTestVectorIndexMapping(t)
		require.NoError(t, m.Delete("title"))
		_, initialized, err := m.Load()
		require.NoError(t, err)
		assert.False(t, initialized, "a delete does not initialize the mapping")
	})
}

// The offline reader refuses what Load refuses: a deletion must not act on
// a record this binary cannot read.
// A cold shard's vectors are found through their records; a vector without
// one, or a shard without a mapping, is where the naming rule puts it.
func TestUnloadedVectorIndexStorageIDs(t *testing.T) {
	const generated = "3f2a9c1e-0b7d-4c1a-9e2f-5d6c7b8a9f00"
	names := []string{"", "title", "body"}
	defaults := map[string]string{"": "main", "title": "vectors_title", "body": "vectors_body"}

	writeMapping := func(t *testing.T, dir, version string, records map[string]string) {
		t.Helper()
		db, err := shardmeta.Open(dir, entlsmkv.BoltFlockTimeout)
		require.NoError(t, err)
		ns := db.Namespace(vectorIndexMappingNamespace)
		require.NoError(t, ns.Put([]byte(vectorIndexMappingFormatVersionKey), []byte(version)))
		for name, rec := range records {
			require.NoError(t, ns.Put([]byte(vectorIndexMappingKey(name)), []byte(rec)))
		}
		require.NoError(t, db.Close())
	}

	t.Run("no metadata file", func(t *testing.T) {
		ids, locked, err := unloadedVectorIndexStorageIDs(t.TempDir(), names)
		require.NoError(t, err)
		assert.False(t, locked)
		assert.Equal(t, defaults, ids)
	})

	t.Run("metadata file without a mapping", func(t *testing.T) {
		dir := t.TempDir()
		db, err := shardmeta.Open(dir, entlsmkv.BoltFlockTimeout)
		require.NoError(t, err)
		require.NoError(t, db.Close())
		ids, locked, err := unloadedVectorIndexStorageIDs(dir, names)
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
		ids, locked, err := unloadedVectorIndexStorageIDs(dir, names)
		require.NoError(t, err)
		assert.False(t, locked)
		assert.Equal(t, map[string]string{"": "main", "title": "vectors_" + generated, "body": "vectors_body"}, ids)
	})

	t.Run("a load holding the file", func(t *testing.T) {
		dir := t.TempDir()
		db, err := shardmeta.Open(dir, entlsmkv.BoltFlockTimeout)
		require.NoError(t, err)
		t.Cleanup(func() { db.Close() })
		ids, locked, err := unloadedVectorIndexStorageIDs(dir, names)
		require.NoError(t, err)
		assert.True(t, locked)
		assert.Equal(t, defaults, ids)
	})

	t.Run("unsupported format version", func(t *testing.T) {
		dir := t.TempDir()
		writeMapping(t, dir, "2", nil)
		ids, _, err := unloadedVectorIndexStorageIDs(dir, names)
		require.ErrorContains(t, err, "format version")
		assert.Equal(t, defaults, ids)
	})

	t.Run("records without a format version", func(t *testing.T) {
		dir := t.TempDir()
		db, err := shardmeta.Open(dir, entlsmkv.BoltFlockTimeout)
		require.NoError(t, err)
		require.NoError(t, db.Namespace(vectorIndexMappingNamespace).Put([]byte(vectorIndexMappingKey("title")),
			[]byte(`{"physical_id":"`+generated+`","index_type":"hnsw","state":"ready"}`)))
		require.NoError(t, db.Close())
		ids, _, err := unloadedVectorIndexStorageIDs(dir, names)
		require.ErrorContains(t, err, "records without a format version")
		assert.Equal(t, defaults, ids)
	})

	t.Run("invalid record", func(t *testing.T) {
		dir := t.TempDir()
		writeMapping(t, dir, "1", map[string]string{
			"":     `{"physical_id":"` + generated + `","index_type":"hnsw","state":"ready"}`,
			"body": `{"physical_id":"title","index_type":"hnsw","state":"ready"}`,
		})
		ids, _, err := unloadedVectorIndexStorageIDs(dir, names)
		require.ErrorContains(t, err, `record "body": physical id`)
		assert.Equal(t, defaults, ids, "no ID read before the invalid record is kept")
	})
}

func TestReadVectorIndexRecordOffline(t *testing.T) {
	valid := `{"physical_id":"vectors_title","index_type":"hnsw","state":"ready"}`
	tests := []struct {
		name    string
		version string
		record  string
		wantErr string
		wantOK  bool
	}{
		{name: "reads a record", version: "1", record: valid, wantOK: true},
		{name: "unsupported format version", version: "2", record: valid, wantErr: "format version"},
		{name: "invalid record", version: "1", record: `{"physical_id":"","index_type":"hnsw","state":"ready"}`, wantErr: "physical id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			db, err := shardmeta.Open(dir, entlsmkv.BoltFlockTimeout)
			require.NoError(t, err)
			ns := db.Namespace(vectorIndexMappingNamespace)
			require.NoError(t, ns.Put([]byte(vectorIndexMappingFormatVersionKey), []byte(tt.version)))
			require.NoError(t, ns.Put([]byte("title"), []byte(tt.record)))
			require.NoError(t, db.Close())

			rec, ok, initialized, err := readVectorIndexRecordOffline(dir, "title")
			assert.True(t, initialized)
			assert.Equal(t, tt.wantOK, ok)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, vectorIndexRecord{PhysicalID: "vectors_title", IndexType: "hnsw", State: "ready"}, rec)
		})
	}
}

// TestVectorIndexRecord_Format pins the fields a record carries beyond the
// first three: the queue ID and the adopted config, inside the same JSON
// object, so a reader from before them still reads the record.
func TestVectorIndexRecord_Format(t *testing.T) {
	hnswCfg := enthnsw.NewDefaultUserConfig()
	hnswCfg.EF = 200
	flatCfg := entflat.NewDefaultUserConfig()
	dynamicCfg := entdynamic.NewDefaultUserConfig()
	hfreshCfg := enthfresh.NewDefaultUserConfig()

	t.Run("every type round-trips through the store", func(t *testing.T) {
		for _, cfg := range []schemaConfig.VectorIndexConfig{hnswCfg, flatCfg, dynamicCfg, hfreshCfg} {
			t.Run(cfg.IndexType(), func(t *testing.T) {
				m, _ := newTestVectorIndexMapping(t)
				require.NoError(t, m.Initialize(nil))
				rec := vectorIndexRecord{
					PhysicalID: "vectors_title", IndexType: cfg.IndexType(), State: "ready",
					QueueID: "vectors_title", Config: cfg,
				}
				require.NoError(t, m.Put("title", rec))

				got, ok, err := m.Get("title")
				require.NoError(t, err)
				require.True(t, ok)
				assert.Equal(t, rec, got)
				records, _, err := m.Load()
				require.NoError(t, err)
				assert.Equal(t, map[string]vectorIndexRecord{"title": rec}, records)
			})
		}
	})

	t.Run("the bytes on disk", func(t *testing.T) {
		m, db := newTestVectorIndexMapping(t)
		require.NoError(t, m.Initialize(nil))
		require.NoError(t, m.Put("title", vectorIndexRecord{
			PhysicalID: "vectors_title", IndexType: "hnsw", State: "ready",
			QueueID: "vectors_title", Config: hnswCfg,
		}))
		v, err := db.Namespace(vectorIndexMappingNamespace).Get([]byte("title"))
		require.NoError(t, err)

		var onDisk map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(v, &onDisk))
		assert.JSONEq(t, `"vectors_title"`, string(onDisk["physical_id"]))
		assert.JSONEq(t, `"hnsw"`, string(onDisk["index_type"]))
		assert.JSONEq(t, `"ready"`, string(onDisk["state"]))
		assert.JSONEq(t, `"vectors_title"`, string(onDisk["queue_id"]))
		// the config is the parsed config under its API field names
		cfgBytes, err := json.Marshal(hnswCfg)
		require.NoError(t, err)
		assert.JSONEq(t, string(cfgBytes), string(onDisk["config"]))
		var cfgOnDisk map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(onDisk["config"], &cfgOnDisk))
		assert.JSONEq(t, `200`, string(cfgOnDisk["ef"]))
		assert.Contains(t, cfgOnDisk, "maxConnections")
	})

	t.Run("a record without the fields loads with them empty", func(t *testing.T) {
		m, db := newTestVectorIndexMapping(t)
		require.NoError(t, m.Initialize(nil))
		require.NoError(t, db.Namespace(vectorIndexMappingNamespace).Put([]byte("title"),
			[]byte(`{"physical_id":"vectors_title","index_type":"hnsw","state":"ready"}`)))
		rec, ok, err := m.Get("title")
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, vectorIndexRecord{PhysicalID: "vectors_title", IndexType: "hnsw", State: "ready"}, rec)
		assert.Nil(t, rec.Config)
		assert.Equal(t, "vectors_title", rec.queueIDOrDefault(), "the queue has always lived under the index's ID")
	})

	t.Run("a null config is no config", func(t *testing.T) {
		m, db := newTestVectorIndexMapping(t)
		require.NoError(t, m.Initialize(nil))
		require.NoError(t, db.Namespace(vectorIndexMappingNamespace).Put([]byte("title"),
			[]byte(`{"physical_id":"vectors_title","index_type":"hnsw","state":"ready","config":null}`)))
		rec, _, err := m.Get("title")
		require.NoError(t, err)
		assert.Nil(t, rec.Config)
	})

	t.Run("a record without the fields writes none", func(t *testing.T) {
		m, db := newTestVectorIndexMapping(t)
		require.NoError(t, m.Initialize(nil))
		require.NoError(t, m.Put("title", vectorIndexRecord{PhysicalID: "vectors_title", IndexType: "hnsw", State: "ready"}))
		v, err := db.Namespace(vectorIndexMappingNamespace).Get([]byte("title"))
		require.NoError(t, err)
		assert.JSONEq(t, `{"physical_id":"vectors_title","index_type":"hnsw","state":"ready"}`, string(v))
	})

	// What a version from before the fields reads: the record's struct as it
	// was then. Its decoder ignores the fields it does not know, and a
	// record it rewrites loses them, which the loading shard fills back in.
	t.Run("a reader from before the fields ignores them", func(t *testing.T) {
		m, db := newTestVectorIndexMapping(t)
		require.NoError(t, m.Initialize(nil))
		require.NoError(t, m.Put("title", vectorIndexRecord{
			PhysicalID: "vectors_title", IndexType: "hnsw", State: "ready",
			QueueID: "vectors_title", Config: hnswCfg,
		}))
		v, err := db.Namespace(vectorIndexMappingNamespace).Get([]byte("title"))
		require.NoError(t, err)

		type recordBeforeTheFields struct {
			PhysicalID string `json:"physical_id"`
			IndexType  string `json:"index_type"`
			State      string `json:"state"`
		}
		var old recordBeforeTheFields
		require.NoError(t, json.Unmarshal(v, &old))
		assert.Equal(t, recordBeforeTheFields{PhysicalID: "vectors_title", IndexType: "hnsw", State: "ready"}, old)
	})
}

// adopted records cfg and fills in a missing queue ID, and says whether
// that changed anything.
func TestVectorIndexRecord_Adopted(t *testing.T) {
	cfg := enthnsw.NewDefaultUserConfig()
	other := cfg
	other.EF = cfg.EF + 1

	t.Run("fills in a record from before the fields", func(t *testing.T) {
		old := vectorIndexRecord{PhysicalID: "vectors_title", IndexType: "hnsw", State: "ready"}
		rec, changed := old.adopted(cfg)
		assert.True(t, changed)
		assert.Equal(t, vectorIndexRecord{
			PhysicalID: "vectors_title", IndexType: "hnsw", State: "ready",
			QueueID: "vectors_title", Config: cfg,
		}, rec)
	})

	t.Run("the same config changes nothing", func(t *testing.T) {
		complete := vectorIndexRecord{PhysicalID: "vectors_title", IndexType: "hnsw", State: "ready", QueueID: "vectors_title", Config: cfg}
		rec, changed := complete.adopted(cfg)
		assert.False(t, changed)
		assert.Equal(t, complete, rec)
	})
}
