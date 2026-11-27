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
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"

	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
	entlsmkv "github.com/weaviate/weaviate/entities/lsmkv"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	entdynamic "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	entflat "github.com/weaviate/weaviate/entities/vectorindex/flat"
	enthfresh "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func newTestRecordStore(t *testing.T) (*recordStore, *shardmeta.DB) {
	t.Helper()
	db, err := shardmeta.Open(t.TempDir(), entlsmkv.BoltFlockTimeout)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return newRecordStore(db), db
}

// TestRecordStore_KeyLayout pins the on-disk keys. They are read by
// every later version, so a change here is a format change, not a rename.
func TestRecordStore_KeyLayout(t *testing.T) {
	assert.Equal(t, "vector_index_mapping", namespace)
	assert.Equal(t, ".format_version", formatVersionKey)
	assert.Equal(t, "1", formatVersion)

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
			assert.Equal(t, tt.key, recordKey(tt.name))
			name, ok := vectorName(tt.key)
			require.True(t, ok)
			assert.Equal(t, tt.name, name)
		})
	}

	// keys that name no vector: the format version, an unknown dotted key,
	// and an empty key
	for _, key := range []string{".format_version", ".unknown", ""} {
		_, ok := vectorName(key)
		assert.False(t, ok, key)
	}
}

func TestRecordStore_Load(t *testing.T) {
	put := func(t *testing.T, db *shardmeta.DB, key, value string) {
		t.Helper()
		require.NoError(t, db.Namespace(namespace).Put([]byte(key), []byte(value)))
	}

	t.Run("empty db is uninitialized", func(t *testing.T) {
		m, _ := newTestRecordStore(t)
		records, initialized, err := m.Load()
		require.NoError(t, err)
		assert.False(t, initialized)
		assert.Empty(t, records)
	})

	// a deferred deletion the process did not finish; the next load does
	t.Run("a dropping record loads", func(t *testing.T) {
		m, db := newTestRecordStore(t)
		put(t, db, ".format_version", "1")
		put(t, db, "title", `{"physical_id":"vectors_title","index_type":"hnsw","state":"dropping"}`)
		records, initialized, err := m.Load()
		require.NoError(t, err)
		assert.True(t, initialized)
		assert.Equal(t, record{PhysicalID: "vectors_title", IndexType: "hnsw", State: "dropping"}, records["title"])
	})

	t.Run("reads every record under its logical name", func(t *testing.T) {
		m, db := newTestRecordStore(t)
		put(t, db, ".format_version", "1")
		put(t, db, ".legacy", `{"physical_id":"main","index_type":"hnsw","state":"ready"}`)
		put(t, db, "title", `{"physical_id":"vectors_title","index_type":"dynamic","state":"ready"}`)
		put(t, db, "summary", `{"physical_id":"vectors_summary","index_type":"flat","state":"creating"}`)

		records, initialized, err := m.Load()
		require.NoError(t, err)
		assert.True(t, initialized)
		assert.Equal(t, map[string]record{
			"":        {PhysicalID: "main", IndexType: "hnsw", State: "ready"},
			"title":   {PhysicalID: "vectors_title", IndexType: "dynamic", State: "ready"},
			"summary": {PhysicalID: "vectors_summary", IndexType: "flat", State: "creating"},
		}, records)
	})

	t.Run("initialized with no records", func(t *testing.T) {
		m, db := newTestRecordStore(t)
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
			m, db := newTestRecordStore(t)
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
func TestRecordStore_LoadAcceptedIDs(t *testing.T) {
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
			m, _ := newTestRecordStore(t)
			rec := record{PhysicalID: tt.physicalID, IndexType: "hnsw", State: "ready", QueueID: tt.queueID}
			require.NoError(t, m.Initialize(map[string]record{"title": rec}))
			records, _, err := m.Load()
			require.NoError(t, err)
			assert.Equal(t, rec, records["title"])
		})
	}
}

// The naming rule: the IDs a vector name gets keep their files, any other ID
// is named as if it carried the vectors_ prefix, never as the legacy vector.
func TestStorageID(t *testing.T) {
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
			rec := record{PhysicalID: tt.physicalID}
			assert.Equal(t, tt.want, rec.storageID())
			assert.Equal(t, tt.physicalID, rec.recordedID())
		})
	}
}

// Only the record reads its raw physical ID. Anywhere else a raw ID that is
// not main or vectors_<x> would name the legacy vector's files.
func TestRecord_PhysicalIDReadOnlyByTheRecord(t *testing.T) {
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	fset := token.NewFileSet()
	var offenders []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") ||
			name == "record.go" {
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

// A record written by a newer version carries fields this one ignores; the
// strip rewrites exactly those records and leaves a mapping it wrote alone.
func TestRecordStore_LoadAndStrip(t *testing.T) {
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
	loadAndStrip := func(t *testing.T, dir string) (map[string]record, []string, map[string]string) {
		t.Helper()
		db, err := shardmeta.Open(dir, entlsmkv.BoltFlockTimeout)
		require.NoError(t, err)
		defer db.Close()
		records, initialized, stripped, err := newRecordStore(db).LoadAndStrip()
		require.NoError(t, err)
		assert.True(t, initialized)
		raw := map[string]string{}
		require.NoError(t, db.Namespace(namespace).ForEach(func(k, v []byte) error {
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
		ns := db.Namespace(namespace)
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
		assert.Equal(t, map[string]record{
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
		assert.Equal(t, map[string]record{
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
		records, initialized, stripped, err := newRecordStore(db).LoadAndStrip()
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
		_, _, _, err = newRecordStore(db).LoadAndStrip()
		require.ErrorContains(t, err, "physical id")
		v, err := db.Namespace(namespace).Get([]byte(".legacy"))
		require.NoError(t, err)
		assert.Equal(t, newer, string(v))
		require.NoError(t, db.Close())
	})
}

func TestRecordStore_Initialize(t *testing.T) {
	t.Run("writes the format version and every record at once", func(t *testing.T) {
		m, db := newTestRecordStore(t)
		err := m.Initialize(map[string]record{
			"":      {PhysicalID: "main", IndexType: "hnsw", State: "ready"},
			"title": {PhysicalID: "vectors_title", IndexType: "flat", State: "ready"},
		})
		require.NoError(t, err)

		records, initialized, err := m.Load()
		require.NoError(t, err)
		assert.True(t, initialized)
		assert.Len(t, records, 2)
		assert.Equal(t, record{PhysicalID: "main", IndexType: "hnsw", State: "ready"}, records[""])
		assert.Equal(t, record{PhysicalID: "vectors_title", IndexType: "flat", State: "ready"}, records["title"])

		// the bytes on disk are the pinned layout
		ns := db.Namespace(namespace)
		v, err := ns.Get([]byte(".format_version"))
		require.NoError(t, err)
		assert.Equal(t, "1", string(v))
		v, err = ns.Get([]byte("title"))
		require.NoError(t, err)
		assert.JSONEq(t, `{"physical_id":"vectors_title","index_type":"flat","state":"ready"}`, string(v))
	})

	t.Run("no records is a valid initialization", func(t *testing.T) {
		m, _ := newTestRecordStore(t)
		require.NoError(t, m.Initialize(nil))
		records, initialized, err := m.Load()
		require.NoError(t, err)
		assert.True(t, initialized)
		assert.Empty(t, records)
	})

	t.Run("refuses to initialize twice", func(t *testing.T) {
		m, _ := newTestRecordStore(t)
		require.NoError(t, m.Initialize(nil))
		err := m.Initialize(map[string]record{
			"title": {PhysicalID: "vectors_title", IndexType: "flat", State: "ready"},
		})
		require.ErrorIs(t, err, errInitialized)
		records, _, err := m.Load()
		require.NoError(t, err)
		assert.Empty(t, records)
	})

	t.Run("an invalid record writes nothing", func(t *testing.T) {
		m, _ := newTestRecordStore(t)
		err := m.Initialize(map[string]record{
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

func TestRecordStore_Put(t *testing.T) {
	t.Run("writes and overwrites one record", func(t *testing.T) {
		m, _ := newTestRecordStore(t)
		require.NoError(t, m.Initialize(nil))

		creating := record{PhysicalID: "vectors_title", IndexType: "hnsw", State: "creating"}
		require.NoError(t, m.Put("title", creating))
		records, _, err := m.Load()
		require.NoError(t, err)
		assert.Equal(t, map[string]record{"title": creating}, records)

		ready := creating
		ready.State = "ready"
		require.NoError(t, m.Put("title", ready))
		records, _, err = m.Load()
		require.NoError(t, err)
		assert.Equal(t, map[string]record{"title": ready}, records)

		// the legacy vector is the empty name
		legacy := record{PhysicalID: "main", IndexType: "hnsw", State: "ready"}
		require.NoError(t, m.Put("", legacy))
		records, _, err = m.Load()
		require.NoError(t, err)
		assert.Equal(t, map[string]record{"title": ready, "": legacy}, records)
	})

	t.Run("refuses an uninitialized mapping", func(t *testing.T) {
		m, _ := newTestRecordStore(t)
		err := m.Put("title", record{PhysicalID: "vectors_title", IndexType: "hnsw", State: "ready"})
		require.ErrorIs(t, err, errUninitialized)
		_, initialized, err := m.Load()
		require.NoError(t, err)
		assert.False(t, initialized)
	})

	tests := []struct {
		name    string
		rec     record
		wantErr string
	}{
		{name: "unknown state", rec: record{PhysicalID: "vectors_title", IndexType: "hnsw", State: "done"}, wantErr: `state "done"`},
		{name: "empty physical id", rec: record{IndexType: "hnsw", State: "ready"}, wantErr: "physical id"},
		{name: "empty index type", rec: record{PhysicalID: "vectors_title", State: "ready"}, wantErr: "index type"},
		{name: "config of another type", rec: record{PhysicalID: "vectors_title", IndexType: "hnsw", State: "ready", Config: entflat.NewDefaultUserConfig()}, wantErr: "config is for a flat index, the record says hnsw"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, _ := newTestRecordStore(t)
			require.NoError(t, m.Initialize(nil))
			err := m.Put("title", tt.rec)
			require.ErrorContains(t, err, tt.wantErr)
			records, _, err := m.Load()
			require.NoError(t, err)
			assert.Empty(t, records)
		})
	}
}

func TestRecordStore_Get(t *testing.T) {
	m, db := newTestRecordStore(t)
	foo := record{PhysicalID: "vectors_foo", IndexType: "hnsw", State: "ready"}
	require.NoError(t, m.Initialize(map[string]record{"foo": foo}))

	rec, ok, err := m.Get("foo")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, foo, rec)

	_, ok, err = m.Get("bar")
	require.NoError(t, err)
	assert.False(t, ok)

	require.NoError(t, db.Namespace(namespace).Put([]byte("broken"), []byte("{")))
	_, _, err = m.Get("broken")
	require.ErrorContains(t, err, `record "broken"`)
}

// A record whose queue outlived the index it was created with keeps both
// IDs through a write and a read.
func TestRecordStore_QueueIDOtherThanThePhysicalID(t *testing.T) {
	m, _ := newTestRecordStore(t)
	require.NoError(t, m.Initialize(nil))
	title := record{PhysicalID: "vectors_title_2", IndexType: "hnsw", State: "ready", QueueID: "vectors_title"}
	require.NoError(t, m.Put("title", title))

	rec, ok, err := m.Get("title")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, title, rec)
	assert.Equal(t, "vectors_title", rec.queueIDOrDefault())
}

// A record without a queue ID finds its queue where its index's files are,
// under the naming rule, and adopting the record persists that ID.
func TestRecord_MissingQueueIDFollowsTheNamingRule(t *testing.T) {
	for _, tc := range []struct{ physicalID, want string }{
		{"main", "main"},
		{"vectors_title", "vectors_title"},
		{"abc-1", "vectors_abc-1"},
	} {
		rec := record{PhysicalID: tc.physicalID, IndexType: "hnsw", State: "ready"}
		assert.Equal(t, tc.want, rec.queueIDOrDefault(), tc.physicalID)
		adopted, changed := rec.adopted(nil)
		assert.True(t, changed, tc.physicalID)
		assert.Equal(t, tc.want, adopted.QueueID, tc.physicalID)
	}
}

func TestRecordStore_Delete(t *testing.T) {
	t.Run("removes one record and leaves the rest", func(t *testing.T) {
		m, _ := newTestRecordStore(t)
		legacy := record{PhysicalID: "main", IndexType: "hnsw", State: "ready"}
		title := record{PhysicalID: "vectors_title", IndexType: "flat", State: "ready"}
		require.NoError(t, m.Initialize(map[string]record{"": legacy, "title": title}))

		require.NoError(t, m.Delete("title"))
		records, initialized, err := m.Load()
		require.NoError(t, err)
		assert.True(t, initialized)
		assert.Equal(t, map[string]record{"": legacy}, records)

		require.NoError(t, m.Delete(""))
		records, initialized, err = m.Load()
		require.NoError(t, err)
		assert.True(t, initialized)
		assert.Empty(t, records)
	})

	t.Run("a missing record is already deleted", func(t *testing.T) {
		m, _ := newTestRecordStore(t)
		require.NoError(t, m.Initialize(nil))
		require.NoError(t, m.Delete("title"))
	})

	t.Run("an uninitialized mapping has nothing to delete", func(t *testing.T) {
		m, _ := newTestRecordStore(t)
		require.NoError(t, m.Delete("title"))
		_, initialized, err := m.Load()
		require.NoError(t, err)
		assert.False(t, initialized, "a delete does not initialize the mapping")
	})
}

// TestRecord_Format pins the fields a record carries beyond the
// first three: the queue ID and the adopted config, inside the same JSON
// object, so a reader from before them still reads the record.
func TestRecord_Format(t *testing.T) {
	hnswCfg := enthnsw.NewDefaultUserConfig()
	hnswCfg.EF = 200
	flatCfg := entflat.NewDefaultUserConfig()
	dynamicCfg := entdynamic.NewDefaultUserConfig()
	hfreshCfg := enthfresh.NewDefaultUserConfig()

	t.Run("every type round-trips through the store", func(t *testing.T) {
		for _, cfg := range []schemaConfig.VectorIndexConfig{hnswCfg, flatCfg, dynamicCfg, hfreshCfg} {
			t.Run(cfg.IndexType(), func(t *testing.T) {
				m, _ := newTestRecordStore(t)
				require.NoError(t, m.Initialize(nil))
				rec := record{
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
				assert.Equal(t, map[string]record{"title": rec}, records)
			})
		}
	})

	t.Run("the bytes on disk", func(t *testing.T) {
		m, db := newTestRecordStore(t)
		require.NoError(t, m.Initialize(nil))
		require.NoError(t, m.Put("title", record{
			PhysicalID: "vectors_title", IndexType: "hnsw", State: "ready",
			QueueID: "vectors_title", Config: hnswCfg,
		}))
		v, err := db.Namespace(namespace).Get([]byte("title"))
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
		m, db := newTestRecordStore(t)
		require.NoError(t, m.Initialize(nil))
		require.NoError(t, db.Namespace(namespace).Put([]byte("title"),
			[]byte(`{"physical_id":"vectors_title","index_type":"hnsw","state":"ready"}`)))
		rec, ok, err := m.Get("title")
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, record{PhysicalID: "vectors_title", IndexType: "hnsw", State: "ready"}, rec)
		assert.Nil(t, rec.Config)
		assert.Equal(t, "vectors_title", rec.queueIDOrDefault(), "the queue has always lived under the index's ID")
	})

	t.Run("a null config is no config", func(t *testing.T) {
		m, db := newTestRecordStore(t)
		require.NoError(t, m.Initialize(nil))
		require.NoError(t, db.Namespace(namespace).Put([]byte("title"),
			[]byte(`{"physical_id":"vectors_title","index_type":"hnsw","state":"ready","config":null}`)))
		rec, _, err := m.Get("title")
		require.NoError(t, err)
		assert.Nil(t, rec.Config)
	})

	t.Run("a record without the fields writes none", func(t *testing.T) {
		m, db := newTestRecordStore(t)
		require.NoError(t, m.Initialize(nil))
		require.NoError(t, m.Put("title", record{PhysicalID: "vectors_title", IndexType: "hnsw", State: "ready"}))
		v, err := db.Namespace(namespace).Get([]byte("title"))
		require.NoError(t, err)
		assert.JSONEq(t, `{"physical_id":"vectors_title","index_type":"hnsw","state":"ready"}`, string(v))
	})

	// What a version from before the fields reads: the record's struct as it
	// was then. Its decoder ignores the fields it does not know, and a
	// record it rewrites loses them, which the loading shard fills back in.
	t.Run("a reader from before the fields ignores them", func(t *testing.T) {
		m, db := newTestRecordStore(t)
		require.NoError(t, m.Initialize(nil))
		require.NoError(t, m.Put("title", record{
			PhysicalID: "vectors_title", IndexType: "hnsw", State: "ready",
			QueueID: "vectors_title", Config: hnswCfg,
		}))
		v, err := db.Namespace(namespace).Get([]byte("title"))
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
func TestRecord_Adopted(t *testing.T) {
	cfg := enthnsw.NewDefaultUserConfig()
	other := cfg
	other.EF = cfg.EF + 1

	t.Run("fills in a record from before the fields", func(t *testing.T) {
		old := record{PhysicalID: "vectors_title", IndexType: "hnsw", State: "ready"}
		rec, changed := old.adopted(cfg)
		assert.True(t, changed)
		assert.Equal(t, record{
			PhysicalID: "vectors_title", IndexType: "hnsw", State: "ready",
			QueueID: "vectors_title", Config: cfg,
		}, rec)
	})

	t.Run("the same config changes nothing", func(t *testing.T) {
		complete := record{PhysicalID: "vectors_title", IndexType: "hnsw", State: "ready", QueueID: "vectors_title", Config: cfg}
		rec, changed := complete.adopted(cfg)
		assert.False(t, changed)
		assert.Equal(t, complete, rec)
	})
}
