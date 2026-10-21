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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
	entlsmkv "github.com/weaviate/weaviate/entities/lsmkv"
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
		{name: "queue id other than the physical id", key: "title", value: `{"physical_id":"vectors_title_2","index_type":"hnsw","state":"ready","queue_id":"vectors_title"}`, wantErr: `queue id "vectors_title" differs from physical id "vectors_title_2"`},
		{name: "unparsable config", key: "title", value: `{"physical_id":"vectors_title","index_type":"hnsw","state":"ready","config":{"ef":"many"}}`, wantErr: `record "title": config`},
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
		{name: "queue id other than the physical id", rec: vectorIndexRecord{PhysicalID: "vectors_title_2", IndexType: "hnsw", State: "ready", QueueID: "vectors_title"}, wantErr: `queue id "vectors_title" differs from physical id "vectors_title_2"`},
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
