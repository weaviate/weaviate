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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
	entlsmkv "github.com/weaviate/weaviate/entities/lsmkv"
)

func newTestVectorIndexMapping(t *testing.T) (*vectorIndexMapping, *shardmeta.DB) {
	t.Helper()
	db, err := shardmeta.Open(t.TempDir(), entlsmkv.BoltFlockTimeout)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return newVectorIndexMapping(db), db
}

// TestVectorIndexMapping_KeyLayout pins the on-disk keys. They are read by
// every later version, so a change here is a format change, not a rename.
func TestVectorIndexMapping_KeyLayout(t *testing.T) {
	assert.Equal(t, "vector_index_mapping", vectorIndexMappingNamespace)
	assert.Equal(t, "format_version", vectorIndexMappingFormatVersionKey)
	assert.Equal(t, "1", vectorIndexMappingFormatVersion)

	tests := []struct {
		name string
		key  string
	}{
		{name: "", key: "legacy"},
		{name: "title", key: "named/title"},
		{name: "default", key: "named/default"},
		{name: "with/slash", key: "named/with/slash"},
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			assert.Equal(t, tt.key, vectorIndexMappingKey(tt.name))
			name, ok := vectorIndexMappingName(tt.key)
			require.True(t, ok)
			assert.Equal(t, tt.name, name)
		})
	}

	// keys that name no vector; a bare "named/" would alias the legacy vector
	for _, key := range []string{"format_version", "unknown", "named/", "named"} {
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

	t.Run("reads every record under its logical name", func(t *testing.T) {
		m, db := newTestVectorIndexMapping(t)
		put(t, db, "format_version", "1")
		put(t, db, "legacy", `{"physical_id":"main","index_type":"hnsw","state":"ready"}`)
		put(t, db, "named/title", `{"physical_id":"vectors_title","index_type":"dynamic","state":"ready"}`)
		put(t, db, "named/summary", `{"physical_id":"vectors_summary","index_type":"flat","state":"creating"}`)

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
		put(t, db, "format_version", "1")
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
		{name: "unknown format version", noVersion: true, key: "format_version", value: "2", wantErr: "format version"},
		{name: "unknown key", key: "something", value: "x", wantErr: `unknown key "something"`},
		{name: "bare named prefix", key: "named/", value: `{"physical_id":"main","index_type":"hnsw","state":"ready"}`, wantErr: `unknown key "named/"`},
		{name: "unparsable value", key: "named/title", value: "{", wantErr: `record "title"`},
		{name: "unknown state", key: "named/title", value: `{"physical_id":"vectors_title","index_type":"hnsw","state":"gone"}`, wantErr: `state "gone"`},
		{name: "empty physical id", key: "named/title", value: `{"physical_id":"","index_type":"hnsw","state":"ready"}`, wantErr: "physical id"},
		{name: "empty index type", key: "named/title", value: `{"physical_id":"vectors_title","index_type":"","state":"ready"}`, wantErr: "index type"},
		{name: "record without format version", noVersion: true, key: "named/title", value: `{"physical_id":"vectors_title","index_type":"hnsw","state":"ready"}`, wantErr: "format version"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, db := newTestVectorIndexMapping(t)
			if !tt.noVersion {
				put(t, db, "format_version", "1")
			}
			put(t, db, tt.key, tt.value)
			_, _, err := m.Load()
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
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
		v, err := ns.Get([]byte("format_version"))
		require.NoError(t, err)
		assert.Equal(t, "1", string(v))
		v, err = ns.Get([]byte("named/title"))
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

	require.NoError(t, db.Namespace(vectorIndexMappingNamespace).Put([]byte("named/broken"), []byte("{")))
	_, _, err = m.Get("broken")
	require.ErrorContains(t, err, `record "broken"`)
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
