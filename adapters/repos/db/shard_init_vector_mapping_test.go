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

//go:build integrationTest

package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/dynamic"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/backup"
	entlsmkv "github.com/weaviate/weaviate/entities/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storobj"
	entdynamic "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	entflat "github.com/weaviate/weaviate/entities/vectorindex/flat"
	enthfresh "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// storageExistsFor reports whether rec's directories are all on disk.
func storageExistsFor(t *testing.T, s *Shard, rec vectorIndexRecord) bool {
	t.Helper()
	dirs, err := s.vectorIndexStorageDirsFor(rec)
	require.NoError(t, err)
	exists, err := vectorIndexStorageExists(dirs)
	require.NoError(t, err)
	return exists
}

// The first load records every index ready at the ID the naming rule gives.
func TestInitShardVectors_FirstLoadWritesTheMapping(t *testing.T) {
	ctx := testCtx()
	shard, _ := setupDropVectorShard(t, ctx)

	records, initialized, err := shard.mapping.Load()
	require.NoError(t, err)
	assert.True(t, initialized)
	assert.Equal(t, map[string]vectorIndexRecord{
		"":    recorded(shard, "", "main", "ready"),
		"foo": recorded(shard, "foo", "vectors_foo", "ready"),
		"mv":  recorded(shard, "mv", "vectors_mv", "ready"),
	}, records)
	for name, rec := range records {
		assert.True(t, storageExistsFor(t, shard, rec), "storage of %q", name)
	}
}

// Each index type leaves the directories the probe expects after construction.
func TestInitShardVectors_FirstLoadRecordsEveryType(t *testing.T) {
	flatCfg := entflat.UserConfig{}
	flatCfg.SetDefaults()
	dist := distancer.NewL2SquaredProvider()

	tests := []struct {
		name      string
		cfg       schemaConfig.VectorIndexConfig
		async     bool
		indexType string
	}{
		{name: "hnsw", cfg: enthnsw.NewDefaultUserConfig(), indexType: "hnsw"},
		{name: "flat", cfg: flatCfg, indexType: "flat"},
		{name: "hfresh", cfg: enthfresh.NewDefaultUserConfig(), indexType: "hfresh"},
		{name: "dynamic", indexType: "dynamic", async: true, cfg: entdynamic.UserConfig{
			Threshold: 1_000_000,
			Distance:  dist.Type(),
			HnswUC:    enthnsw.UserConfig{MaxConnections: 8, EFConstruction: 16, EF: 8, VectorCacheMaxObjects: 1000},
			FlatUC:    flatCfg,
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			shd, _ := testShardWithSettings(t, ctx, &models.Class{Class: "EveryType"}, tt.cfg, false, tt.async)
			shard := underlyingShard(t, shd)

			records, _, err := shard.mapping.Load()
			require.NoError(t, err)
			rec := recorded(shard, "", "main", "ready")
			assert.Equal(t, tt.indexType, rec.IndexType)
			assert.Equal(t, map[string]vectorIndexRecord{"": rec}, records)
			assert.True(t, storageExistsFor(t, shard, rec))
		})
	}
}

// A skipped hnsw vector owns no files and gets no record.
func TestInitShardVectors_SkippedIndexHasNoRecord(t *testing.T) {
	ctx := testCtx()
	shd, _ := testShard(t, ctx, "SkippedVector")
	shard := underlyingShard(t, shd)

	records, initialized, err := shard.mapping.Load()
	require.NoError(t, err)
	assert.True(t, initialized)
	assert.Empty(t, records)

	// a later load rebuilds the no-op index and its queue: callers need the slot
	shard = reload(t, ctx, shard, &models.Class{Class: "SkippedVector"})
	found, err := shard.WithVectorIndex("", func(VectorIndex) error { return nil })
	require.NoError(t, err)
	assert.True(t, found)
	found, err = shard.WithVectorIndexQueue("", func(*VectorIndexQueue) error { return nil })
	require.NoError(t, err)
	assert.True(t, found)
}

// reload shuts the shard down and opens it again from disk.
func reload(t *testing.T, ctx context.Context, shard *Shard, class *models.Class) *Shard {
	t.Helper()
	return reloadAfter(t, ctx, shard, class, nil)
}

// reloadAfter is reload with a step between the shutdown and the reopen,
// where a directory removal survives the shutdown's own writes.
func reloadAfter(t *testing.T, ctx context.Context, shard *Shard, class *models.Class, beforeOpen func()) *Shard {
	t.Helper()
	require.NoError(t, shard.Shutdown(ctx))
	simulateProcessRestartBucketCleanup(t, shard.pathLSM())
	if beforeOpen != nil {
		beforeOpen()
	}
	return openShardFromDisk(t, ctx, shard.index, class, shard.Name())
}

// reloadExpectingError is reloadAfter for a reopen that must fail with
// wantErr. The caller must repair and reload afterwards.
func reloadExpectingError(t *testing.T, ctx context.Context, shard *Shard, class *models.Class, wantErr string, beforeOpen func()) {
	t.Helper()
	require.NoError(t, shard.Shutdown(ctx))
	simulateProcessRestartBucketCleanup(t, shard.pathLSM())
	if beforeOpen != nil {
		beforeOpen()
	}
	_, err := shard.index.initShard(ctx, shard.Name(), class, nil, true, true)
	require.ErrorContains(t, err, wantErr)
}

// withOfflineMapping opens a shut-down shard's index.db for fn.
func withOfflineMapping(t *testing.T, shardDir string, fn func(m *vectorIndexMapping, ns *shardmeta.Namespace)) {
	t.Helper()
	db, err := shardmeta.Open(shardDir, entlsmkv.BoltFlockTimeout)
	require.NoError(t, err)
	defer db.Close()
	fn(newVectorIndexMapping(db), db.Namespace(vectorIndexMappingNamespace))
}

// One reload per state the mapping can be in at a later load.
func TestInitShardVectors_Reconcile(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)
	foo := recorded(shard, "foo", "vectors_foo", "ready")
	fooDir := filepath.Join(shard.path(), helpers.GetHNSWCommitLogDirName("foo"))

	// a ready record whose storage is on disk opens as before
	shard = reload(t, ctx, shard, class)
	records, _, err := shard.mapping.Load()
	require.NoError(t, err)
	assert.Equal(t, foo, records["foo"])
	found, err := shard.WithVectorIndex("foo", func(VectorIndex) error { return nil })
	require.NoError(t, err)
	assert.True(t, found)

	// a ready record whose storage is gone, with nothing to index, is rebuilt:
	// a backup or a transfer carries no directory for an empty index
	removeFooDir := func() { require.NoError(t, os.RemoveAll(fooDir)) }
	shard = reloadAfter(t, ctx, shard, class, removeFooDir)
	_, err = os.Stat(fooDir)
	require.NoError(t, err)
	records, _, err = shard.mapping.Load()
	require.NoError(t, err)
	assert.Equal(t, foo, records["foo"])

	// with vectors in the object store the same loss is rebuilt too: under
	// async indexing they may all still be queued, and a backup taken then
	// carries no directory for the index
	require.NoError(t, shard.PutObject(ctx, dropVecObject(t, "a", true)))
	shard = reloadAfter(t, ctx, shard, class, removeFooDir)
	_, err = os.Stat(fooDir)
	require.NoError(t, err)
	records, _, err = shard.mapping.Load()
	require.NoError(t, err)
	assert.Equal(t, foo, records["foo"])

	// a creating record resumes: built, synced, flipped to ready
	creating := foo
	creating.State = "creating"
	require.NoError(t, shard.mapping.Put("foo", creating))
	shard = reload(t, ctx, shard, class)
	records, _, err = shard.mapping.Load()
	require.NoError(t, err)
	assert.Equal(t, foo, records["foo"])

	// a vector the schema has but the mapping does not is recorded
	require.NoError(t, shard.mapping.Delete("foo"))
	shard = reload(t, ctx, shard, class)
	records, _, err = shard.mapping.Load()
	require.NoError(t, err)
	assert.Equal(t, foo, records["foo"])

	// a record from before the config and queue ID fields existed is filled
	// in: the queue from the physical ID, the config from the schema
	old := vectorIndexRecord{PhysicalID: "vectors_foo", IndexType: "hnsw", State: "ready"}
	require.NoError(t, shard.mapping.Put("foo", old))
	shard = reload(t, ctx, shard, class)
	records, _, err = shard.mapping.Load()
	require.NoError(t, err)
	assert.Equal(t, foo, records["foo"])

	// until the cutover the schema decides the config at load: a record
	// holding another config, as after a change the shard was cold for, is
	// rewritten with the schema's
	staleCfg := foo.Config.(enthnsw.UserConfig)
	staleCfg.EF = staleCfg.EF + 1
	stale := foo
	stale.Config = staleCfg
	require.NoError(t, shard.mapping.Put("foo", stale))
	shard = reload(t, ctx, shard, class)
	records, _, err = shard.mapping.Load()
	require.NoError(t, err)
	assert.Equal(t, foo, records["foo"])

	// a record the schema does not have is deleted, its storage untouched
	ghostDir := filepath.Join(shard.path(), helpers.GetHNSWCommitLogDirName("ghost"))
	require.NoError(t, os.MkdirAll(ghostDir, 0o755))
	require.NoError(t, shard.mapping.Put("ghost", vectorIndexRecord{PhysicalID: "vectors_ghost", IndexType: "hnsw", State: "ready"}))
	shard = reload(t, ctx, shard, class)
	records, _, err = shard.mapping.Load()
	require.NoError(t, err)
	_, hasGhost := records["ghost"]
	assert.False(t, hasGhost)
	_, err = os.Stat(ghostDir)
	assert.NoError(t, err, "a record the schema lost is not a reason to delete files")

	// a record whose type disagrees with the schema refuses the load
	wrongType := foo
	wrongType.IndexType = "flat"
	wrongType.Config = entflat.NewDefaultUserConfig()
	require.NoError(t, shard.mapping.Put("foo", wrongType))
	reloadExpectingError(t, ctx, shard, class, "flat", nil)
	withOfflineMapping(t, shard.path(), func(m *vectorIndexMapping, _ *shardmeta.Namespace) {
		require.NoError(t, m.Put("foo", foo))
	})
	shard = reload(t, ctx, shard, class)

	// a mapping the shard cannot read refuses the load
	require.NoError(t, shard.metadataDB.Namespace(vectorIndexMappingNamespace).Put([]byte(".bogus"), []byte("x")))
	reloadExpectingError(t, ctx, shard, class, `unknown key ".bogus"`, nil)
	withOfflineMapping(t, shard.path(), func(_ *vectorIndexMapping, ns *shardmeta.Namespace) {
		require.NoError(t, ns.Delete([]byte(".bogus")))
	})
	shard = reload(t, ctx, shard, class)
	_, _, err = shard.mapping.Load()
	require.NoError(t, err)
}

// Two records naming one queue refuse the load: each consumer would take
// the other's work.
func TestInitShardVectors_RefusesTwoRecordsNamingOneQueue(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)
	mv := recorded(shard, "mv", "vectors_mv", "ready")
	reloadExpectingError(t, ctx, shard, class, `vectors "foo" and "mv" both record queue "vectors_foo"`, func() {
		withOfflineMapping(t, shard.path(), func(m *vectorIndexMapping, _ *shardmeta.Namespace) {
			shared := mv
			shared.QueueID = "vectors_foo"
			require.NoError(t, m.Put("mv", shared))
		})
	})
	withOfflineMapping(t, shard.path(), func(m *vectorIndexMapping, _ *shardmeta.Namespace) {
		require.NoError(t, m.Put("mv", mv))
	})
	shard = reload(t, ctx, shard, class)
	rec, _, err := shard.mapping.Get("mv")
	require.NoError(t, err)
	assert.Equal(t, mv, rec)
}

// The vectors_compressed link 1.30.19 to 1.40 kept to a named vector's
// compressed bucket is gone after a load: a vector named "compressed" writes
// its raw vectors into a bucket of its own, not through the link.
func TestInitShardVectors_RemovesVectorsCompressedLink(t *testing.T) {
	ctx := testCtx()
	// the link only exists without a legacy vector, which owns vectors_compressed
	hnswCfg := enthnsw.NewDefaultUserConfig()
	class := &models.Class{
		Class:        "CompressedLink",
		VectorConfig: map[string]models.VectorConfig{"foo": {VectorIndexType: hnswCfg.IndexType(), VectorIndexConfig: hnswCfg}},
	}
	shardLike, _ := testShardWithSettings(t, ctx, class, nil, false, false, func(i *Index) {
		i.vectorIndexUserConfig = nil
		i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{"foo": hnswCfg}
	})
	shard := underlyingShard(t, shardLike)
	lsmDir := shard.pathLSM()
	link := filepath.Join(lsmDir, helpers.VectorsCompressedBucketLSM)
	fooCompressed := filepath.Join(lsmDir, helpers.GetCompressedBucketName("foo"))
	fooCodes := filepath.Join(fooCompressed, "foo-codes")

	flatCfg := entflat.UserConfig{}
	flatCfg.SetDefaults()
	class.VectorConfig["compressed"] = models.VectorConfig{VectorIndexType: flatCfg.IndexType(), VectorIndexConfig: flatCfg}
	shard.index.vectorIndexUserConfigLock.Lock()
	shard.index.vectorIndexUserConfigs["compressed"] = flatCfg
	shard.index.vectorIndexUserConfigLock.Unlock()

	shard = reloadAfter(t, ctx, shard, class, func() {
		require.NoError(t, os.MkdirAll(fooCompressed, 0o755))
		require.NoError(t, os.WriteFile(fooCodes, []byte("foo's codes"), 0o644))
		require.NoError(t, os.Symlink(helpers.GetCompressedBucketName("foo"), link))
	})
	obj := &storobj.Object{
		MarshallerVersion: 1,
		Object:            models.Object{ID: strfmt.UUID(uuid.NewString()), Class: class.Class},
		Vectors:           map[string][]float32{"compressed": {1, 2, 3}},
	}
	require.NoError(t, shard.PutObject(ctx, obj))
	require.NoError(t, shard.Store().FlushMemtables(ctx))

	fi, err := os.Lstat(link)
	require.NoError(t, err)
	assert.Zero(t, fi.Mode()&os.ModeSymlink, "%s is a bucket of its own", link)
	entries, err := os.ReadDir(fooCompressed)
	require.NoError(t, err)
	require.Len(t, entries, 1, "nothing written into foo's compressed bucket")
	assert.Equal(t, "foo-codes", entries[0].Name())
}

// Only a link is removed: a real vectors_compressed is the legacy vector's
// compressed bucket.
func TestRemoveVectorsCompressedLink_KeepsABucket(t *testing.T) {
	logger, _ := test.NewNullLogger()
	m := newCompressedVectorsMigrator(logger)
	lsmDir := t.TempDir()
	require.NoError(t, m.removeVectorsCompressedLink(lsmDir), "nothing to remove")

	segment := filepath.Join(lsmDir, helpers.VectorsCompressedBucketLSM, "segment-1.db")
	require.NoError(t, os.MkdirAll(filepath.Dir(segment), 0o755))
	require.NoError(t, os.WriteFile(segment, []byte("legacy codes"), 0o644))
	require.NoError(t, m.removeVectorsCompressedLink(lsmDir))
	assert.FileExists(t, segment)
}

// Records with generated IDs that name one file refuse the load before a
// dropping one is finished: finishing it would delete the live vector's files.
func TestInitShardVectors_RefusesGeneratedRecordsNamingOneFile(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)
	shardDir := shard.path()
	foo := recorded(shard, "foo", "vectors_foo", "ready")
	mv := recorded(shard, "mv", "vectors_mv", "ready")
	live := foo
	live.PhysicalID = "a-b"
	dropping := mv
	dropping.PhysicalID = "vectors_a-b"
	dropping.State = vectorIndexStateDropping
	commitLog := filepath.Join(shardDir, helpers.HNSWCommitLogDirNameForID("vectors_a-b"), "1")

	reloadExpectingError(t, ctx, shard, class, `vectors "foo" and "mv" both name "vectors_a-b`, func() {
		require.NoError(t, os.MkdirAll(filepath.Dir(commitLog), 0o755))
		require.NoError(t, os.WriteFile(commitLog, []byte("foo's graph"), 0o644))
		withOfflineMapping(t, shardDir, func(m *vectorIndexMapping, _ *shardmeta.Namespace) {
			require.NoError(t, m.Put("foo", live))
			require.NoError(t, m.Put("mv", dropping))
		})
	})
	assert.FileExists(t, commitLog, "the refusal deletes nothing")
	withOfflineMapping(t, shardDir, func(m *vectorIndexMapping, _ *shardmeta.Namespace) {
		records, _, err := m.Load()
		require.NoError(t, err)
		assert.Equal(t, live, records["foo"])
		assert.Equal(t, dropping, records["mv"])
		require.NoError(t, m.Put("foo", foo))
		require.NoError(t, m.Put("mv", mv))
	})
	require.NoError(t, os.RemoveAll(filepath.Dir(commitLog)))
	shard = reload(t, ctx, shard, class)
	rec, _, err := shard.mapping.Get("mv")
	require.NoError(t, err)
	assert.Equal(t, mv, rec)
}

// A dropping record sharing a queue with a live one refuses the load before
// the drop is finished: finishing it would delete the live vector's queue.
func TestInitShardVectors_RefusesADroppingRecordSharingAQueue(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)
	shardDir := shard.path()
	foo := recorded(shard, "foo", "vectors_foo", "ready")
	dropping := recorded(shard, "mv", "vectors_mv", "dropping")
	dropping.QueueID = "vectors_foo"
	queueChunk := filepath.Join(shardDir, "vectors_foo.queue.d", "chunk-1.bin")

	reloadExpectingError(t, ctx, shard, class, `vectors "foo" and "mv" both record queue "vectors_foo"`, func() {
		require.NoError(t, os.MkdirAll(filepath.Dir(queueChunk), 0o755))
		require.NoError(t, os.WriteFile(queueChunk, []byte("foo's pending work"), 0o644))
		withOfflineMapping(t, shardDir, func(m *vectorIndexMapping, _ *shardmeta.Namespace) {
			require.NoError(t, m.Put("mv", dropping))
		})
	})
	assert.FileExists(t, queueChunk, "the refusal deletes nothing")
	withOfflineMapping(t, shardDir, func(m *vectorIndexMapping, _ *shardmeta.Namespace) {
		records, _, err := m.Load()
		require.NoError(t, err)
		assert.Equal(t, dropping, records["mv"])
		assert.Equal(t, foo, records["foo"])
	})
}

// A shard last loaded by a newer version has records with fields this one
// does not know. The load drops them before any index opens, and the
// indexes serve as before.
func TestInitShardVectors_StripsFieldsOfANewerVersion(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)
	obj := dropVecObject(t, "a", true)
	require.NoError(t, shard.PutObject(ctx, obj))

	want := map[string]string{}
	shard = reloadAfter(t, ctx, shard, class, func() {
		withOfflineMapping(t, shard.path(), func(_ *vectorIndexMapping, ns *shardmeta.Namespace) {
			for _, k := range []string{".legacy", "foo", "mv"} {
				v, err := ns.Get([]byte(k))
				require.NoError(t, err)
				want[k] = string(v)
			}
			for _, k := range []string{".legacy", "foo"} {
				newer := strings.TrimSuffix(want[k], "}") + `,"future":{"ef":64}}`
				require.NoError(t, ns.Put([]byte(k), []byte(newer)))
			}
		})
	})

	ns := shard.metadataDB.Namespace(vectorIndexMappingNamespace)
	for key, w := range want {
		got, err := ns.Get([]byte(key))
		require.NoError(t, err)
		assert.Equal(t, w, string(got), key)
	}

	found, err := shard.WithVectorIndex("foo", func(index VectorIndex) error {
		if !index.ContainsDoc(obj.DocID) {
			return fmt.Errorf("doc %d is gone", obj.DocID)
		}
		return nil
	})
	require.NoError(t, err)
	assert.True(t, found)
}

// A vector added to a running shard is recorded ready at the naming rule's
// ID, with its storage durable, and survives a reload as itself.
func TestInitTargetVector_RecordsTheVector(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)

	require.NoError(t, shard.index.updateVectorIndexConfigs(ctx, map[string]schemaConfig.VectorIndexConfig{
		"added": enthnsw.NewDefaultUserConfig(),
	}))

	added := recorded(shard, "added", "vectors_added", "ready")
	rec, ok, err := shard.mapping.Get("added")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, added, rec)
	assert.True(t, storageExistsFor(t, shard, rec))

	// a second update for the same vector finds the slot and writes nothing new
	require.NoError(t, shard.index.updateVectorIndexConfigs(ctx, map[string]schemaConfig.VectorIndexConfig{
		"added": enthnsw.NewDefaultUserConfig(),
	}))

	shard = reload(t, ctx, shard, class)
	rec, ok, err = shard.mapping.Get("added")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, added, rec)
}

// A build that fails leaves a creating record and no slot; the next load
// resumes it once the config is fixed.
func TestInitTargetVector_FailedBuildLeavesCreating(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)

	broken := enthnsw.NewDefaultUserConfig()
	broken.Distance = "bogus"
	err := shard.index.updateVectorIndexConfigs(ctx, map[string]schemaConfig.VectorIndexConfig{"broken": broken})
	require.ErrorContains(t, err, "unrecognized distance metric")

	rec, ok, err := shard.mapping.Get("broken")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, vectorIndexRecord{
		PhysicalID: "vectors_broken", IndexType: "hnsw", State: "creating",
		QueueID: "vectors_broken", Config: broken,
	}, rec)
	found, err := shard.WithVectorIndex("broken", func(VectorIndex) error { return nil })
	require.NoError(t, err)
	assert.False(t, found)

	// the schema now carries a valid config: the reload resumes the creation
	shard.index.vectorIndexUserConfigLock.Lock()
	shard.index.vectorIndexUserConfigs["broken"] = enthnsw.NewDefaultUserConfig()
	shard.index.vectorIndexUserConfigLock.Unlock()
	shard = reload(t, ctx, shard, class)
	rec, _, err = shard.mapping.Get("broken")
	require.NoError(t, err)
	assert.Equal(t, "ready", rec.State)
	found, err = shard.WithVectorIndex("broken", func(VectorIndex) error { return nil })
	require.NoError(t, err)
	assert.True(t, found)
}

// A skipped vector added live gets its no-op slot and no record.
func TestInitTargetVector_SkippedVectorHasNoRecord(t *testing.T) {
	ctx := testCtx()
	shard, _ := setupDropVectorShard(t, ctx)

	require.NoError(t, shard.index.updateVectorIndexConfigs(ctx, map[string]schemaConfig.VectorIndexConfig{
		"skipped": enthnsw.UserConfig{Skip: true},
	}))

	found, err := shard.WithVectorIndex("skipped", func(VectorIndex) error { return nil })
	require.NoError(t, err)
	assert.True(t, found)
	_, ok, err := shard.mapping.Get("skipped")
	require.NoError(t, err)
	assert.False(t, ok)
}

// TestVectorIndexCollisions pins which pairs of owners share a physical
// name, and that a newcomer is checked against the owners only.
func TestVectorIndexCollisions(t *testing.T) {
	tests := []struct {
		name   string
		owners map[string]string
		want   []string
	}{
		{name: "no collision", owners: map[string]string{"": "main", "title": "vectors_title"}},
		{
			name:   "compressed next to the legacy vector",
			owners: map[string]string{"": "main", "compressed": "vectors_compressed"},
			want:   []string{`vectors "" and "compressed" share "vectors_compressed"`},
		},
		{
			name:   "muvera next to its sibling",
			owners: map[string]string{"foo": "vectors_foo", "foo_muvera_vectors": "vectors_foo_muvera_vectors"},
			want:   []string{`vectors "foo" and "foo_muvera_vectors" share "vectors_foo_muvera_vectors"`},
		},
		{
			name:   "centroids next to its sibling",
			owners: map[string]string{"foo": "vectors_foo", "foo_centroids": "vectors_foo_centroids"},
			want:   []string{`vectors "foo" and "foo_centroids" share "vectors_compressed_foo_centroids"`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, vectorIndexCollisions(ownersAt(tt.owners)))
		})
	}

	// a newcomer is checked against the owners, not the owners against each
	// other: an existing collision does not block an unrelated addition
	owners := ownersAt(map[string]string{"foo": "vectors_foo", "foo_muvera_vectors": "vectors_foo_muvera_vectors"})
	assert.Empty(t, vectorIndexCollisionsWith(owners, "bar", ownerAt("vectors_bar")))
	assert.Equal(t, []string{`vectors "foo" and "foo_mv_mappings" share "vectors_foo_mv_mappings"`},
		vectorIndexCollisionsWith(owners, "foo_mv_mappings", ownerAt("vectors_foo_mv_mappings")))
}

// A queue that outlived its index is still its vector's: another vector
// naming it collides, and so does a newcomer whose ID the index took.
func TestVectorIndexCollisions_QueueOutlivedItsIndex(t *testing.T) {
	owners := map[string]vectorIndexOwner{"foo": {physicalID: "vectors_foo_2", queueID: "vectors_foo"}}

	assert.Empty(t, vectorIndexCollisionsWith(owners, "bar", ownerAt("vectors_bar")))
	assert.Equal(t, []string{`vectors "foo" and "bar" share "vectors_foo.queue.d"`},
		vectorIndexCollisionsWith(owners, "bar", vectorIndexOwner{physicalID: "vectors_bar", queueID: "vectors_foo"}))
	assert.Equal(t, []string{`vectors "foo" and "foo_2" share "vectors_foo_2"`},
		vectorIndexCollisionsWith(owners, "foo_2", ownerAt("vectors_foo_2")))
}

// ownersAt maps each vector to an owner whose index and queue share its ID.
func ownersAt(ids map[string]string) map[string]vectorIndexOwner {
	owners := make(map[string]vectorIndexOwner, len(ids))
	for name, id := range ids {
		owners[name] = ownerAt(id)
	}
	return owners
}

// A live creation whose physical names another vector owns is refused
// before anything is written or built.
func TestInitTargetVector_RefusesACollision(t *testing.T) {
	ctx := testCtx()
	shard, _ := setupDropVectorShard(t, ctx)

	err := shard.index.updateVectorIndexConfigs(ctx, map[string]schemaConfig.VectorIndexConfig{
		"compressed": enthnsw.NewDefaultUserConfig(),
	})
	require.ErrorContains(t, err, `share "vectors_compressed"`)

	_, ok, err := shard.mapping.Get("compressed")
	require.NoError(t, err)
	assert.False(t, ok, "nothing was written")
	found, err := shard.WithVectorIndex("compressed", func(VectorIndex) error { return nil })
	require.NoError(t, err)
	assert.False(t, found, "nothing was built")

	// an existing collision does not block an unrelated addition
	require.NoError(t, shard.mapping.Put("foo_muvera_vectors", vectorIndexRecord{PhysicalID: "vectors_foo_muvera_vectors", IndexType: "hnsw", State: "ready"}))
	require.NoError(t, shard.index.updateVectorIndexConfigs(ctx, map[string]schemaConfig.VectorIndexConfig{"bar": enthnsw.NewDefaultUserConfig()}))
	require.NoError(t, shard.mapping.Delete("foo_muvera_vectors"))

	// a retry of a vector's own creating record is not a collision with itself
	broken := enthnsw.NewDefaultUserConfig()
	broken.Distance = "bogus"
	require.Error(t, shard.index.updateVectorIndexConfigs(ctx, map[string]schemaConfig.VectorIndexConfig{"again": broken}))
	require.NoError(t, shard.index.updateVectorIndexConfigs(ctx, map[string]schemaConfig.VectorIndexConfig{"again": enthnsw.NewDefaultUserConfig()}))
	rec, _, err := shard.mapping.Get("again")
	require.NoError(t, err)
	assert.Equal(t, "ready", rec.State)
}

// A schema vector without a record that collides with a recorded vector, or
// with another newcomer, refuses the load: only an older version could have
// written such a schema, and building it would share another index's files.
func TestInitShardVectors_ReconcileRefusesACollidingNewcomer(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)
	plant := func(names ...string) {
		shard.index.vectorIndexUserConfigLock.Lock()
		defer shard.index.vectorIndexUserConfigLock.Unlock()
		for _, name := range names {
			shard.index.vectorIndexUserConfigs[name] = enthnsw.NewDefaultUserConfig()
		}
	}
	unplant := func(names ...string) {
		shard.index.vectorIndexUserConfigLock.Lock()
		defer shard.index.vectorIndexUserConfigLock.Unlock()
		for _, name := range names {
			delete(shard.index.vectorIndexUserConfigs, name)
		}
	}

	// against a recorded vector
	plant("compressed")
	reloadExpectingError(t, ctx, shard, class, `vectors "" and "compressed" share "vectors_compressed"`, nil)
	unplant("compressed")
	shard = reload(t, ctx, shard, class)

	// against another newcomer
	plant("bar", "bar_muvera_vectors")
	reloadExpectingError(t, ctx, shard, class, `vectors "bar" and "bar_muvera_vectors" share "vectors_bar_muvera_vectors"`, nil)
	unplant("bar", "bar_muvera_vectors")
	shard = reload(t, ctx, shard, class)
	_, ok, err := shard.mapping.Get("bar")
	require.NoError(t, err)
	assert.False(t, ok, "a refused newcomer left no record")
}

// The operator's way out of a refused load: a schema written before the
// collision check carries "compressed" next to a legacy BQ vector, the shard
// refuses to load, the vector is dropped while the shard is cold, and the
// shard loads with the legacy vector's quantized data intact.
func TestInitShardVectors_RecoveryFromARefusedLoad(t *testing.T) {
	ctx := testCtx()
	cfg := enthnsw.NewDefaultUserConfig()
	cfg.BQ.Enabled = true
	class := &models.Class{Class: "Recovery", VectorIndexType: "hnsw", VectorIndexConfig: cfg}
	shd, idx := testShardWithSettings(t, ctx, class, cfg, false, false)
	shard := underlyingShard(t, shd)

	var objs []*storobj.Object
	for i := range 5 {
		obj := &storobj.Object{
			MarshallerVersion: 1,
			Object:            models.Object{ID: strfmt.UUID(uuid.NewString()), Class: class.Class},
			Vector:            []float32{float32(i), 1, 2, 3},
		}
		require.NoError(t, shard.PutObject(ctx, obj))
		objs = append(objs, obj)
	}
	compressed := filepath.Join(shard.pathLSM(), helpers.VectorsCompressedBucketLSM)
	_, err := os.Stat(compressed)
	require.NoError(t, err, "the legacy vector keeps its quantized bucket")

	// an older version's schema: a colliding vector, no record
	shard.index.vectorIndexUserConfigLock.Lock()
	shard.index.vectorIndexUserConfigs["compressed"] = enthnsw.NewDefaultUserConfig()
	shard.index.vectorIndexUserConfigLock.Unlock()

	// the shard goes cold, and refuses to load
	require.NoError(t, shard.Shutdown(ctx))
	simulateProcessRestartBucketCleanup(t, shard.pathLSM())
	coldShard, err := idx.initShard(ctx, shard.Name(), class, nil, false, true)
	require.NoError(t, err)
	cold := coldShard.(*LazyLoadShard)
	idx.shards.Store(shard.Name(), cold)
	_, _, err = cold.loadIfCold(ctx)
	require.ErrorContains(t, err, `vectors "" and "compressed" share "vectors_compressed"`)

	// the drop runs against the cold shard, by path; checked before the
	// reload, which would recreate an empty bucket and hide the loss
	require.NoError(t, idx.dropVectorIndex(ctx, "compressed"))
	entries, err := os.ReadDir(compressed)
	require.NoError(t, err, "the drop spared the legacy vector's bucket")
	require.NotEmpty(t, entries)

	_, _, err = cold.loadIfCold(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cold.Shutdown(context.Background())) })
	found, err := cold.loadedShard().WithVectorIndex("", func(index VectorIndex) error {
		for _, obj := range objs {
			if !index.ContainsDoc(obj.DocID) {
				return fmt.Errorf("doc %d is gone", obj.DocID)
			}
		}
		return nil
	})
	require.NoError(t, err)
	assert.True(t, found)
}

// With async indexing, a backup can land while every vector still waits in
// the queue. The index has written nothing, so the backup carries no
// directory for it, only the queue's chunks. The restored shard loads and
// indexes them (weaviate/dirk-claude-issues#253).
func TestRestoreShardWhoseVectorsAreStillQueued(t *testing.T) {
	cases := queuedVectorIndexCases()
	for _, tt := range []struct {
		name    string
		index   queuedVectorIndexCase
		drained bool
	}{
		{name: "legacy hnsw, queued at backup", index: cases[0]},
		{name: "legacy flat, queued at backup", index: cases[1]},
		{name: "named hnsw, queued at backup", index: cases[2]},
		// the control: the same restore with nothing queued
		{name: "legacy hnsw, drained before backup", index: cases[0], drained: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tt, drained := tt.index, tt.drained
			ctx := testCtx()
			shard, idx, class := newQueuedVectorShard(t, ctx, tt, !drained)

			for i := range queuedObjectCount {
				require.NoError(t, shard.PutObject(ctx, tt.object(i)))
			}
			if drained {
				waitForQueueToDrain(t, shard, tt.targetVector)
			} else {
				require.Equal(t, int64(queuedObjectCount), queuedVectorCount(t, shard, tt.targetVector),
					"precondition: every vector is still queued")
			}

			restoredShard := backupAndRestoreInPlace(t, ctx, shard, idx, class)

			waitForQueueToDrain(t, restoredShard, tt.targetVector)
			found, err := restoredShard.WithVectorIndex(tt.targetVector, func(index VectorIndex) error {
				for docID := range uint64(queuedObjectCount) {
					require.True(t, index.ContainsDoc(docID), "doc %d is indexed after restore", docID)
				}
				return nil
			})
			require.NoError(t, err)
			require.True(t, found)
		})
	}
}

// A replacement index keeps the queue it took over: the shard opens the
// queue the record names, indexes what it holds into the index the record
// names, and a drop of the vector removes both.
func TestInitShardVectors_QueueOutlivesItsIndex(t *testing.T) {
	ctx := testCtx()
	tt := queuedVectorIndexCases()[2]
	shard, idx, class := newQueuedVectorShard(t, ctx, tt, true)
	for i := range queuedObjectCount {
		require.NoError(t, shard.PutObject(ctx, tt.object(i)))
	}
	require.Equal(t, int64(queuedObjectCount), queuedVectorCount(t, shard, "foo"),
		"precondition: every vector is still queued")

	// the state a replacement leaves: the record names the new index and the
	// old queue, and the old index is gone
	replaced := recorded(shard, "foo", "vectors_foo_2", "ready")
	replaced.QueueID = "vectors_foo"
	shardName, shardDir := shard.Name(), shard.path()
	require.NoError(t, shard.Shutdown(ctx))
	simulateProcessRestartBucketCleanup(t, shard.pathLSM())
	old := helpers.VectorIndexArtifactNamesForID("vectors_foo")
	for _, bucket := range old.LSMBuckets {
		require.NoError(t, os.RemoveAll(filepath.Join(shard.pathLSM(), bucket)))
	}
	for _, dir := range old.ShardDirs {
		require.NoError(t, os.RemoveAll(filepath.Join(shardDir, dir)))
	}
	withOfflineMapping(t, shardDir, func(m *vectorIndexMapping, _ *shardmeta.Namespace) {
		require.NoError(t, m.Put("foo", replaced))
	})

	idx.scheduler = idx.db.scheduler
	reopened, err := idx.initShard(ctx, shardName, class, nil, true, true)
	require.NoError(t, err)
	idx.shards.Store(shardName, reopened)
	shard = underlyingShard(t, reopened)

	waitForQueueToDrain(t, shard, "foo")
	found, err := shard.WithVectorIndex("foo", func(index VectorIndex) error {
		for docID := range uint64(queuedObjectCount) {
			require.True(t, index.ContainsDoc(docID), "doc %d is indexed from the queue the record names", docID)
		}
		return nil
	})
	require.NoError(t, err)
	require.True(t, found)
	rec, _, err := shard.mapping.Get("foo")
	require.NoError(t, err)
	assert.Equal(t, replaced, rec)
	assert.DirExists(t, filepath.Join(shardDir, "vectors_foo.queue.d"))
	assert.DirExists(t, filepath.Join(shardDir, "vectors_foo_2.hnsw.commitlog.d"))
	assert.NoDirExists(t, filepath.Join(shardDir, "vectors_foo_2.queue.d"))

	// the drop takes the index and the queue it names, and nothing else
	require.NoError(t, shard.DropVectorIndex(ctx, "foo"))
	assert.Empty(t, entriesWithID(t, shard, "vectors_foo"))
	_, ok, err := shard.mapping.Get("foo")
	require.NoError(t, err)
	assert.False(t, ok)
	assert.DirExists(t, filepath.Join(shardDir, "main.queue.d"))
}

// backupAndRestoreInPlace backs shard up, replaces its directory with the
// backup, the way a restore lands it on the node, and loads it with idx's
// running scheduler.
func backupAndRestoreInPlace(t *testing.T, ctx context.Context, shard *Shard, idx *Index, class *models.Class) *Shard {
	t.Helper()
	stagingRoot := t.TempDir()
	sd := backup.ShardDescriptor{}
	files, err := shard.CreateBackupSnapshot(ctx, &sd, stagingRoot, newReindexGate(idx.db, reindexGateMaxAge))
	require.NoError(t, err)

	shardName := shard.Name()
	shardDir := shard.path()
	require.NoError(t, shard.Shutdown(ctx))
	require.NoError(t, os.RemoveAll(shardDir))
	rootPath := idx.Config.RootPath
	for _, relPath := range files {
		copyFileForTest(t, filepath.Join(stagingRoot, relPath), filepath.Join(rootPath, relPath))
	}
	for relPath, data := range map[string][]byte{
		sd.DocIDCounterPath:      sd.DocIDCounter,
		sd.PropLengthTrackerPath: sd.PropLengthTracker,
		sd.ShardVersionPath:      sd.Version,
	} {
		require.NoError(t, os.WriteFile(filepath.Join(rootPath, relPath), data, 0o644))
	}

	idx.scheduler = idx.db.scheduler
	restored, err := idx.initShard(ctx, shardName, class, nil, true, true)
	require.NoError(t, err, "the restored shard must load")
	idx.shards.Store(shardName, restored)
	return underlyingShard(t, restored)
}

// A backup of a shard whose queue outlived its index carries the queue and
// the record naming it: the restored shard indexes what was still queued.
func TestRestoreShardWhoseQueueOutlivedItsIndex(t *testing.T) {
	ctx := testCtx()
	tt := queuedVectorIndexCases()[2]
	shard, idx, class := newQueuedVectorShard(t, ctx, tt, true)
	replaced := recorded(shard, "foo", "vectors_foo_2", "ready")
	replaced.QueueID = "vectors_foo"
	// the switch happens on an empty index, so every vector lands in the
	// queue the replacement took over
	shard = reloadAfter(t, ctx, shard, class, func() {
		withOfflineMapping(t, shard.path(), func(m *vectorIndexMapping, _ *shardmeta.Namespace) {
			require.NoError(t, m.Put("foo", replaced))
		})
	})
	for i := range queuedObjectCount {
		require.NoError(t, shard.PutObject(ctx, tt.object(i)))
	}
	require.Equal(t, int64(queuedObjectCount), queuedVectorCount(t, shard, "foo"),
		"precondition: every vector is still queued")

	shard = backupAndRestoreInPlace(t, ctx, shard, idx, class)

	rec, _, err := shard.mapping.Get("foo")
	require.NoError(t, err)
	assert.Equal(t, replaced, rec)
	waitForQueueToDrain(t, shard, "foo")
	found, err := shard.WithVectorIndex("foo", func(index VectorIndex) error {
		for docID := range uint64(queuedObjectCount) {
			require.True(t, index.ContainsDoc(docID), "doc %d is indexed after restore", docID)
		}
		return nil
	})
	require.NoError(t, err)
	require.True(t, found)
}

const (
	queuedClassName   = "QueuedAtBackup"
	queuedObjectCount = 20
)

type queuedVectorIndexCase struct {
	legacy       schemaConfig.VectorIndexConfig
	named        map[string]schemaConfig.VectorIndexConfig
	targetVector string
	object       func(i int) *storobj.Object
}

// queuedVectorIndexCases: legacy hnsw, legacy flat, named hnsw. The probe
// looks at a commit log directory for hnsw and at a bucket for flat.
func queuedVectorIndexCases() []queuedVectorIndexCase {
	flatCfg := entflat.UserConfig{}
	flatCfg.SetDefaults()
	legacyObject := func(i int) *storobj.Object {
		obj := queuedObject()
		obj.Vector = []float32{float32(i), 1, 2}
		return obj
	}

	return []queuedVectorIndexCase{
		{legacy: enthnsw.NewDefaultUserConfig(), object: legacyObject},
		{legacy: flatCfg, object: legacyObject},
		{
			legacy:       enthnsw.UserConfig{Skip: true},
			named:        map[string]schemaConfig.VectorIndexConfig{"foo": enthnsw.NewDefaultUserConfig()},
			targetVector: "foo",
			object: func(i int) *storobj.Object {
				obj := queuedObject()
				obj.Vectors = map[string][]float32{"foo": {float32(i), 1, 2}}
				return obj
			},
		},
	}
}

// newQueuedVectorShard builds an async-indexing shard for tt. With stall, its
// scheduler never starts, so nothing leaves the queue.
func newQueuedVectorShard(t *testing.T, ctx context.Context, tt queuedVectorIndexCase, stall bool) (*Shard, *Index, *models.Class) {
	t.Helper()
	logger, _ := test.NewNullLogger()

	class := &models.Class{Class: queuedClassName}
	for name, cfg := range tt.named {
		if class.VectorConfig == nil {
			class.VectorConfig = map[string]models.VectorConfig{}
		}
		class.VectorConfig[name] = models.VectorConfig{VectorIndexType: cfg.IndexType(), VectorIndexConfig: cfg}
	}

	shardLike, idx := testShardWithSettings(t, ctx, class, tt.legacy, false, true, func(i *Index) {
		if stall {
			i.scheduler = queue.NewScheduler(queue.SchedulerOptions{Logger: logger})
		}
		if tt.named != nil {
			i.vectorIndexUserConfigs = tt.named
		}
	})
	return underlyingShard(t, shardLike), idx, class
}

func queuedObject() *storobj.Object {
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:    strfmt.UUID(uuid.NewString()),
			Class: queuedClassName,
		},
	}
}

func waitForQueueToDrain(t *testing.T, s *Shard, targetVector string) {
	t.Helper()
	require.Eventually(t, func() bool {
		return queuedVectorCount(t, s, targetVector) == 0
	}, 30*time.Second, 50*time.Millisecond, "the queue drains")
}

func queuedVectorCount(t *testing.T, s *Shard, targetVector string) int64 {
	t.Helper()
	var size int64
	found, err := s.WithVectorIndexQueue(targetVector, func(q *VectorIndexQueue) error {
		size = q.Size()
		return nil
	})
	require.NoError(t, err)
	require.True(t, found, "shard has a queue for %q", targetVector)
	return size
}

func copyFileForTest(t *testing.T, src, dst string) {
	t.Helper()
	data, err := os.ReadFile(src)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(dst), 0o755))
	require.NoError(t, os.WriteFile(dst, data, 0o644))
}

// A dropping record whose queue outlived its index: finishing the drop
// removes the queue the record names, not one under the index's ID.
func TestInitShardVectors_FinishesADroppingRecordWithItsQueue(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)
	dropping := vectorIndexRecord{PhysicalID: "vectors_foo_2", IndexType: "hnsw", State: "dropping", QueueID: "vectors_foo"}
	shardDir := shard.path()
	queueChunk := filepath.Join(shardDir, "vectors_foo.queue.d", "chunk-1.bin")
	commitLog := filepath.Join(shardDir, "vectors_foo_2.hnsw.commitlog.d")

	// gone from the schema rather than marked dropped, so only the record
	// says what to remove
	delete(class.VectorConfig, "foo")
	shard.index.vectorIndexUserConfigLock.Lock()
	delete(shard.index.vectorIndexUserConfigs, "foo")
	shard.index.vectorIndexUserConfigLock.Unlock()
	shard = reloadAfter(t, ctx, shard, class, func() {
		require.NoError(t, os.MkdirAll(filepath.Dir(queueChunk), 0o755))
		require.NoError(t, os.WriteFile(queueChunk, []byte("left by the replaced index"), 0o644))
		require.NoError(t, os.MkdirAll(commitLog, 0o755))
		withOfflineMapping(t, shardDir, func(m *vectorIndexMapping, _ *shardmeta.Namespace) {
			require.NoError(t, m.Put("foo", dropping))
		})
	})
	assert.NoDirExists(t, filepath.Dir(queueChunk))
	assert.NoDirExists(t, commitLog)
	_, ok, err := shard.mapping.Get("foo")
	require.NoError(t, err)
	assert.False(t, ok)
}

// A dropping record is a deferred deletion the process did not finish: the
// load deletes the files and the record, then treats the vector as unknown.
func TestInitShardVectors_FinishesADroppingRecord(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)
	require.NoError(t, shard.PutObject(ctx, dropVecObject(t, "one", true)))
	require.NotEmpty(t, entriesNamed(t, shard, "foo"))
	dropping := vectorIndexRecord{PhysicalID: "vectors_foo", IndexType: "hnsw", State: "dropping"}

	// dropped in the schema, the record left dropping: files and record go
	markDropped(class, "foo")
	shard.index.vectorIndexUserConfigLock.Lock()
	delete(shard.index.vectorIndexUserConfigs, "foo")
	shard.index.vectorIndexUserConfigLock.Unlock()
	shard = reloadAfter(t, ctx, shard, class, func() {
		withOfflineMapping(t, shard.path(), func(m *vectorIndexMapping, _ *shardmeta.Namespace) {
			require.NoError(t, m.Put("foo", dropping))
		})
	})
	assert.Empty(t, entriesNamed(t, shard, "foo"))
	_, ok, err := shard.mapping.Get("foo")
	require.NoError(t, err)
	assert.False(t, ok)
	found, err := shard.WithVectorIndex("foo", func(VectorIndex) error { return nil })
	require.NoError(t, err)
	assert.False(t, found)

	// active again in the schema with the record still dropping: the old
	// files go, then the vector is created fresh
	class.VectorConfig["foo"] = models.VectorConfig{VectorIndexType: enthnsw.NewDefaultUserConfig().IndexType(), VectorIndexConfig: enthnsw.NewDefaultUserConfig()}
	shard.index.vectorIndexUserConfigLock.Lock()
	shard.index.vectorIndexUserConfigs["foo"] = enthnsw.NewDefaultUserConfig()
	shard.index.vectorIndexUserConfigLock.Unlock()
	stale := filepath.Join(shard.path(), helpers.GetHNSWCommitLogDirName("foo"), "stale")
	shard = reloadAfter(t, ctx, shard, class, func() {
		require.NoError(t, os.MkdirAll(filepath.Dir(stale), 0o755))
		require.NoError(t, os.WriteFile(stale, []byte("old"), 0o600))
		withOfflineMapping(t, shard.path(), func(m *vectorIndexMapping, _ *shardmeta.Namespace) {
			require.NoError(t, m.Put("foo", dropping))
		})
	})
	rec, ok, err := shard.mapping.Get("foo")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, recorded(shard, "foo", "vectors_foo", "ready"), rec)
	_, err = os.Stat(stale)
	assert.True(t, os.IsNotExist(err), "the old files went before the fresh build")
	found, err = shard.WithVectorIndex("foo", func(VectorIndex) error { return nil })
	require.NoError(t, err)
	assert.True(t, found)
}

// The startup's marker sweep runs before the mapping is open: it must read
// the dropping record offline and delete at the recorded id, not at the
// naming rule's, or the remapped files outlive the record.
func TestInitShardVectors_FinishesADroppingRecordAtTheRecordedID(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)
	shard, decoy := remapFoo(t, ctx, shard, class)

	markDropped(class, "foo")
	shard.index.vectorIndexUserConfigLock.Lock()
	delete(shard.index.vectorIndexUserConfigs, "foo")
	shard.index.vectorIndexUserConfigLock.Unlock()
	shard = reloadAfter(t, ctx, shard, class, func() {
		withOfflineMapping(t, shard.path(), func(m *vectorIndexMapping, _ *shardmeta.Namespace) {
			require.NoError(t, m.Put("foo", vectorIndexRecord{PhysicalID: "vectors_foo_v2", IndexType: "hnsw", State: "dropping"}))
		})
	})
	assert.Empty(t, entriesWithID(t, shard, "vectors_foo_v2"))
	_, err := os.Stat(decoy)
	assert.NoError(t, err, "the naming rule's paths were not touched")
	_, ok, err := shard.mapping.Get("foo")
	require.NoError(t, err)
	assert.False(t, ok)
}

// The cold sweep, on a shard that is not loaded, deletes at the recorded id
// and deletes nothing for a name the mapping does not record.
func TestRemoveVectorIndexFiles_ReadsTheRecordOffline(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)
	shard, decoy := remapFoo(t, ctx, shard, class)
	ghost := filepath.Join(shard.path(), helpers.GetHNSWCommitLogDirName("ghost"), "somebody-elses")
	require.NoError(t, os.MkdirAll(filepath.Dir(ghost), 0o755))
	require.NoError(t, os.WriteFile(ghost, []byte("x"), 0o600))
	require.NoError(t, shard.Shutdown(ctx))
	withOfflineMapping(t, shard.path(), func(m *vectorIndexMapping, _ *shardmeta.Namespace) {
		require.NoError(t, m.Put("foo", vectorIndexRecord{PhysicalID: "vectors_foo_v2", IndexType: "hnsw", State: "dropping"}))
	})

	h := newVectorDropIndexHelper()
	require.NoError(t, h.removeVectorIndexFiles(shard.index.path(), shard.name, "foo", otherTargetVectors(class, "foo")))
	assert.Empty(t, entriesWithID(t, shard, "vectors_foo_v2"))
	_, err := os.Stat(decoy)
	assert.NoError(t, err, "the naming rule's paths were not touched")
	rec, ok, _, err := readVectorIndexRecordOffline(shard.path(), "foo")
	require.NoError(t, err)
	assert.False(t, ok, "the record went last: %+v", rec)

	require.NoError(t, h.removeVectorIndexFiles(shard.index.path(), shard.name, "ghost", nil))
	_, err = os.Stat(ghost)
	assert.NoError(t, err, "an initialized mapping without a record owns nothing")

	// a record this binary cannot trust stops the sweep before it deletes
	withOfflineMapping(t, shard.path(), func(_ *vectorIndexMapping, ns *shardmeta.Namespace) {
		require.NoError(t, ns.Put([]byte("ghost"), []byte(`{"physical_id":"","index_type":"hnsw","state":"ready"}`)))
	})
	err = h.removeVectorIndexFiles(shard.index.path(), shard.name, "ghost", nil)
	require.ErrorContains(t, err, "physical id")
	_, err = os.Stat(ghost)
	assert.NoError(t, err, "nothing deleted on a refused record")
}

// A drop that failed part-way leaves a dropping record and files, here an
// upgraded dynamic index whose state key is gone and whose commit log
// survives. When the schema has the name again, the load finishes the drop
// first, so the re-created index starts clean instead of inferring its
// upgrade from the surviving commit log and replaying it.
func TestInitShardVectors_FinishesADroppingRecordBeforeRecreatingTheName(t *testing.T) {
	ctx := testCtx()
	const className = "RecreateDynamic"
	duc := entdynamic.UserConfig{}
	duc.SetDefaults()
	duc.Threshold = 10
	class := &models.Class{Class: className, VectorConfig: map[string]models.VectorConfig{
		"dyn": {VectorIndexType: duc.IndexType(), VectorIndexConfig: duc},
	}}
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, true, func(i *Index) {
		i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{"dyn": duc}
	})
	shard := underlyingShard(t, shd)
	t.Cleanup(func() { _ = idx.drop() })

	var objs []*storobj.Object
	for i := 0; i < 20; i++ {
		obj := &storobj.Object{
			MarshallerVersion: 1,
			Object:            models.Object{ID: strfmt.UUID(uuid.NewString()), Class: className},
			Vectors:           map[string][]float32{"dyn": {float32(i), 1, 2, 3}},
		}
		require.NoError(t, shard.PutObject(ctx, obj))
		objs = append(objs, obj)
	}
	commitLog := filepath.Join(shard.path(), helpers.GetHNSWCommitLogDirName("dyn"))
	flatBucket := filepath.Join(shard.pathLSM(), helpers.VectorsBucketNameForID("vectors_dyn"))
	exists := func(path string) bool { _, err := os.Stat(path); return err == nil }
	require.Eventually(t, func() bool { return exists(commitLog) && !exists(flatBucket) }, 30*time.Second, 50*time.Millisecond,
		"the queue upgrades past the threshold")

	// the partial drop: state key gone, commit log left, record dropping;
	// the schema still has the name, as after a finalize and a re-create
	shard = reloadAfter(t, ctx, shard, class, func() {
		require.NoError(t, dynamic.RemoveStateKeyForID(shard.path(), "vectors_dyn"))
		withOfflineMapping(t, shard.path(), func(m *vectorIndexMapping, _ *shardmeta.Namespace) {
			require.NoError(t, m.Put("dyn", vectorIndexRecord{PhysicalID: "vectors_dyn", IndexType: "dynamic", State: "dropping"}))
		})
	})

	rec, ok, err := shard.mapping.Get("dyn")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "ready", rec.State)
	upgraded, err := dynamic.UpgradedInState(shard.metadataDB.Namespace(dynamic.StateNamespace), shard.path(), "vectors_dyn")
	require.NoError(t, err)
	assert.False(t, upgraded, "the re-created index starts flat")
	assert.True(t, exists(flatBucket), "the flat stage is back")
	found, err := shard.WithVectorIndex("dyn", func(index VectorIndex) error {
		for _, obj := range objs {
			assert.False(t, index.ContainsDoc(obj.DocID), "the surviving commit log was not replayed")
		}
		return nil
	})
	require.NoError(t, err)
	require.True(t, found)
}

// TestCompressedVectorsMigrator pins the pre-1.30.19 layout repair: the vector
// recorded at main owns vectors_compressed, and only a target with an index of
// its own gets a copy, at the bucket its recorded ID names.
func TestCompressedVectorsMigrator(t *testing.T) {
	const generated = "3f2a9c1e-0b7d"
	bq := enthnsw.NewDefaultUserConfig()
	bq.BQ.Enabled = true
	hnswRec := func(id string) vectorIndexRecord {
		return vectorIndexRecord{PhysicalID: id, IndexType: "hnsw", State: "ready", QueueID: id}
	}
	hfreshRec := vectorIndexRecord{PhysicalID: "main", IndexType: "hfresh", State: "ready", QueueID: "main"}

	tests := []struct {
		name    string
		records map[string]vectorIndexRecord
		// buckets that hold the shared bucket's content afterwards
		wantCopies []string
		// buckets that must not exist afterwards
		wantAbsent []string
		wantShared bool
		wantDone   bool
	}{
		{
			name:       "legacy owner keeps the bucket, targets get a copy",
			records:    map[string]vectorIndexRecord{"": hnswRec("main"), "foo": hnswRec("vectors_foo"), "mv": hnswRec("vectors_mv")},
			wantCopies: []string{"vectors_compressed_foo", "vectors_compressed_mv"},
			wantShared: true,
			wantDone:   true,
		},
		{
			name:       "a target without an index of its own gets nothing",
			records:    map[string]vectorIndexRecord{"": hnswRec("main"), "foo": hnswRec("vectors_foo"), "new": hnswRec("vectors_new")},
			wantCopies: []string{"vectors_compressed_foo"},
			wantAbsent: []string{"vectors_compressed_new"},
			wantShared: true,
			wantDone:   true,
		},
		{
			name:       "a named vector recorded at main owns the bucket",
			records:    map[string]vectorIndexRecord{"default": hnswRec("main"), "foo": hnswRec("vectors_foo")},
			wantCopies: []string{"vectors_compressed_foo"},
			wantAbsent: []string{"vectors_compressed_default"},
			wantShared: true,
			wantDone:   true,
		},
		{
			name:       "an hfresh owner's bucket holds its centroids",
			records:    map[string]vectorIndexRecord{"": hfreshRec, "foo": hnswRec("vectors_foo")},
			wantAbsent: []string{"vectors_compressed_foo"},
			wantShared: true,
			wantDone:   true,
		},
		{
			name:       "no owner, one target: the bucket moves",
			records:    map[string]vectorIndexRecord{"foo": hnswRec("vectors_foo")},
			wantCopies: []string{"vectors_compressed_foo"},
			wantDone:   true,
		},
		{
			name:       "no owner, one target and a newcomer: the bucket moves to the target",
			records:    map[string]vectorIndexRecord{"foo": hnswRec("vectors_foo"), "new": hnswRec("vectors_new")},
			wantCopies: []string{"vectors_compressed_foo"},
			wantAbsent: []string{"vectors_compressed_new"},
			wantDone:   true,
		},
		{
			name:       "no owner, two targets: copies, then the bucket goes",
			records:    map[string]vectorIndexRecord{"foo": hnswRec("vectors_foo"), "mv": hnswRec("vectors_mv")},
			wantCopies: []string{"vectors_compressed_foo", "vectors_compressed_mv"},
		},
		{
			name:       "no owner, only a newcomer: the bucket stays unattributed",
			records:    map[string]vectorIndexRecord{"new": hnswRec("vectors_new")},
			wantAbsent: []string{"vectors_compressed_new"},
			wantShared: true,
			wantDone:   true,
		},
		{
			name:       "a target at a generated ID gets its copy under that ID",
			records:    map[string]vectorIndexRecord{"": hnswRec("main"), "foo": hnswRec(generated)},
			wantCopies: []string{"vectors_compressed_" + generated},
			wantAbsent: []string{"vectors_compressed_foo"},
			wantShared: true,
			wantDone:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			shard, _ := setupDropVectorShard(t, ctx)
			lsmDir := shard.store.GetDir()
			m := newCompressedVectorsMigrator(shard.index.logger)
			require.NoError(t, os.RemoveAll(m.migrationDirectory(lsmDir)))
			// the generated target's index exists, as after a rebuild
			require.NoError(t, os.MkdirAll(filepath.Join(shard.path(), helpers.HNSWCommitLogDirNameForID("vectors_"+generated)), 0o755))

			shared := filepath.Join(lsmDir, helpers.VectorsCompressedBucketLSM)
			require.NoError(t, os.MkdirAll(shared, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(shared, "codes"), []byte("shared"), 0o600))

			configs := make(map[string]schemaConfig.VectorIndexConfig, len(tt.records))
			for name := range tt.records {
				configs[name] = bq
			}
			require.NoError(t, m.do(shard, tt.records, configs))

			for _, bucket := range tt.wantCopies {
				content, err := os.ReadFile(filepath.Join(lsmDir, bucket, "codes"))
				require.NoError(t, err, bucket)
				assert.Equal(t, "shared", string(content), bucket)
			}
			for _, bucket := range tt.wantAbsent {
				_, err := os.Stat(filepath.Join(lsmDir, bucket))
				assert.True(t, os.IsNotExist(err), "%s must not exist", bucket)
			}
			_, err := os.Stat(filepath.Join(shared, "codes"))
			assert.Equal(t, tt.wantShared, err == nil, "shared bucket kept")
			assert.Equal(t, tt.wantDone, m.isMigrationDone(lsmDir), "done flag")
		})
	}
}

// The repair runs only on a shard without a mapping: a mapping is written by
// a load that ran it. A repair that fails fails the load before the mapping
// is written, so the next load retries it.
func TestInitShardVectors_CompressedRepairOnlyWithoutMapping(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)
	m := newCompressedVectorsMigrator(shard.index.logger)
	shared := filepath.Join(shard.pathLSM(), helpers.VectorsCompressedBucketLSM)
	strayRename := filepath.Join(shard.pathLSM(), "vectors_compressed_foo")
	sharedWithCodes := func() {
		require.NoError(t, os.RemoveAll(m.migrationDirectory(shard.pathLSM())))
		require.NoError(t, os.MkdirAll(shared, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(shared, "codes"), []byte("shared"), 0o600))
	}
	unmapped := func() {
		withOfflineMapping(t, shard.path(), func(_ *vectorIndexMapping, ns *shardmeta.Namespace) {
			var keys [][]byte
			require.NoError(t, ns.ForEach(func(k, _ []byte) error {
				keys = append(keys, append([]byte(nil), k...))
				return nil
			}))
			for _, k := range keys {
				require.NoError(t, ns.Delete(k))
			}
		})
	}

	// with a mapping: a vectors_compressed folder is the legacy vector's
	shard = reloadAfter(t, ctx, shard, class, sharedWithCodes)
	assert.False(t, m.isMigrationDone(shard.pathLSM()), "the repair did not run")
	_, err := os.Stat(strayRename)
	assert.True(t, os.IsNotExist(err))

	// without one: it runs, and a failure stops the load
	migrations := m.migrationDirectory(shard.pathLSM())
	reloadExpectingError(t, ctx, shard, class, "migrate compressed vectors", func() {
		sharedWithCodes()
		unmapped()
		// the done flag cannot be written under a file
		require.NoError(t, os.WriteFile(migrations, nil, 0o600))
	})
	version, _, err := shardmeta.GetOffline(shard.path(), vectorIndexMappingNamespace, []byte(vectorIndexMappingFormatVersionKey))
	require.NoError(t, err)
	assert.Nil(t, version, "a failed repair writes no mapping")

	require.NoError(t, os.Remove(migrations))
	shard = openShardFromDisk(t, ctx, shard.index, class, shard.Name())
	assert.True(t, m.isMigrationDone(shard.pathLSM()), "the next load retried the repair")
	records, initialized, err := shard.mapping.Load()
	require.NoError(t, err)
	assert.True(t, initialized)
	assert.Len(t, records, 3)
}

const generatedIDsClassName = "GeneratedIDs"

// generatedIDsVectors is one vector of every index type. The names carry a
// token no other file name has, so a file named after a vector is found.
func generatedIDsVectors() (legacy schemaConfig.VectorIndexConfig, named map[string]schemaConfig.VectorIndexConfig) {
	bq := enthnsw.NewDefaultUserConfig()
	bq.BQ.Enabled = true
	rq := enthnsw.NewDefaultUserConfig()
	rq.RQ.Enabled = true
	flatBQ := entflat.UserConfig{}
	flatBQ.SetDefaults()
	flatBQ.BQ.Enabled = true
	// 8-dimension binary codes tie often; rescore every doc so search is exact
	flatBQ.BQ.RescoreLimit = 100
	flatCfg := entflat.UserConfig{}
	flatCfg.SetDefaults()
	dynamicCfg := func(threshold uint64) entdynamic.UserConfig {
		return entdynamic.UserConfig{
			Threshold: threshold,
			Distance:  flatCfg.Distance,
			HnswUC:    enthnsw.NewDefaultUserConfig(),
			FlatUC:    flatCfg,
		}
	}
	muvera := enthnsw.NewDefaultMultiVectorUserConfig()
	muvera.Multivector.MuveraConfig = enthnsw.MuveraConfig{
		Enabled:      true,
		KSim:         enthnsw.DefaultMultivectorKSim,
		DProjections: enthnsw.DefaultMultivectorDProjections,
		Repetitions:  enthnsw.DefaultMultivectorRepetitions,
	}

	return bq, map[string]schemaConfig.VectorIndexConfig{
		"zebrahnsw":    rq,
		"zebraflat":    flatBQ,
		"zebradyn":     dynamicCfg(10),
		"zebradynflat": dynamicCfg(1_000_000),
		"zebrafresh":   enthfresh.NewDefaultUserConfig(),
		"zebramuvera":  muvera,
		"zebramulti":   enthnsw.NewDefaultMultiVectorUserConfig(),
	}
}

func generatedIDsObject(i int, named map[string]schemaConfig.VectorIndexConfig) *storobj.Object {
	vec := func(seed int) []float32 {
		v := make([]float32, 8)
		for d := range v {
			v[d] = float32((seed*31+d*17)%97) - 48
		}
		return v
	}
	obj := &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:    strfmt.UUID(uuid.NewString()),
			Class: generatedIDsClassName,
		},
		Vector:       vec(i),
		Vectors:      map[string][]float32{},
		MultiVectors: map[string][][]float32{},
	}
	for name, cfg := range named {
		if hnswCfg, ok := cfg.(enthnsw.UserConfig); ok && hnswCfg.Multivector.Enabled {
			obj.MultiVectors[name] = [][]float32{vec(i), vec(i + 1000)}
			continue
		}
		obj.Vectors[name] = vec(i)
	}
	return obj
}

// TestShardWithGeneratedVectorIndexIDs pins that a shard finds every vector
// index and queue only through its mapping: every index type is built under a
// generated ID, as a rebuild names it, and the shard searches, writes,
// restarts, backs up and restores, and drops, without a file named after a
// vector or after the ID the naming rule would give it.
func TestShardWithGeneratedVectorIndexIDs(t *testing.T) {
	ctx := testCtx()
	legacy, named := generatedIDsVectors()
	class := &models.Class{Class: generatedIDsClassName, VectorConfig: map[string]models.VectorConfig{}}
	for name, cfg := range named {
		class.VectorConfig[name] = models.VectorConfig{VectorIndexType: cfg.IndexType(), VectorIndexConfig: cfg}
	}
	all := map[string]schemaConfig.VectorIndexConfig{"": legacy}
	for name, cfg := range named {
		all[name] = cfg
	}

	var seeded map[string]vectorIndexRecord
	shardLike, idx := testShardWithSettings(t, ctx, class, legacy, false, true, func(i *Index) {
		i.vectorIndexUserConfigs = named
		shardName := i.getSchema.(*fakeSchemaGetter).shardState.AllPhysicalShards()[0]
		seeded = seedGeneratedVectorIndexRecords(t, shardPath(i.path(), shardName), all)
	})
	shard := underlyingShard(t, shardLike)

	docs := 0
	put := func(t *testing.T, shard *Shard, n int) {
		for range n {
			require.NoError(t, shard.PutObject(ctx, generatedIDsObject(docs, named)))
			docs++
		}
		for name := range all {
			waitForQueueToDrain(t, shard, name)
		}
	}
	works := func(t *testing.T, shard *Shard) {
		t.Helper()
		for name, cfg := range all {
			found, err := shard.WithVectorIndex(name, func(index VectorIndex) error {
				for docID := range uint64(docs) {
					require.True(t, index.ContainsDoc(docID), "vector %q holds doc %d", name, docID)
				}
				probe := generatedIDsObject(docs-1, named)
				search := func() ([]uint64, error) {
					if hnswCfg, ok := cfg.(enthnsw.UserConfig); ok && hnswCfg.Multivector.Enabled {
						multi, ok := index.(VectorIndexMulti)
						require.True(t, ok, "vector %q", name)
						ids, _, err := multi.SearchByMultiVector(ctx, probe.MultiVectors[name], 1, nil)
						return ids, err
					}
					vec := probe.Vectors[name]
					if name == "" {
						vec = probe.Vector
					}
					ids, _, err := index.SearchByVector(ctx, vec, 1, nil)
					return ids, err
				}
				want := []uint64{uint64(docs - 1)}
				if _, ok := cfg.(enthfresh.UserConfig); ok {
					// after a restart, HFresh skips postings until its background
					// analyze tasks have persisted their sizes
					require.EventuallyWithT(t, func(c *assert.CollectT) {
						ids, err := search()
						assert.NoError(c, err)
						assert.Equal(c, want, ids)
					}, 30*time.Second, 50*time.Millisecond, "vector %q finds the last doc", name)
					return nil
				}
				ids, err := search()
				require.NoError(t, err)
				require.Equal(t, want, ids, "vector %q finds the last doc", name)
				return nil
			})
			require.NoError(t, err)
			require.True(t, found, "vector %q", name)
		}
	}
	// no file of the shard is named after a vector or a naming-rule ID
	noNameDerivedFiles := func(t *testing.T, shard *Shard) {
		t.Helper()
		for _, dir := range []string{shard.path(), shard.pathLSM()} {
			entries, err := os.ReadDir(dir)
			require.NoError(t, err)
			for _, e := range entries {
				n := e.Name()
				assert.NotContains(t, n, "zebra", "%s is named after a vector", filepath.Join(dir, n))
				assert.False(t, n == "vectors" || n == "vectors_compressed" || strings.HasPrefix(n, "main.") || strings.HasPrefix(n, "main_"),
					"%s is named after the legacy vector", filepath.Join(dir, n))
			}
		}
	}
	recordsAreSeeded := func(t *testing.T, shard *Shard) {
		t.Helper()
		records, _, err := shard.mapping.Load()
		require.NoError(t, err)
		require.Len(t, records, len(seeded))
		for name, want := range seeded {
			got := records[name]
			assert.Equal(t, want.recordedID(), got.recordedID(), "vector %q", name)
			assert.Equal(t, want.QueueID, got.QueueID, "vector %q", name)
			assert.Equal(t, vectorIndexStateReady, got.State, "vector %q", name)
			assert.NotEmpty(t, entriesWithID(t, shard, want.storageID()), "vector %q has files at its ID", name)
		}
	}

	put(t, shard, 40)
	require.Eventually(t, func() bool {
		upgraded, err := dynamic.UpgradedInState(shard.metadataDB.Namespace(dynamic.StateNamespace), shard.path(), seeded["zebradyn"].storageID())
		require.NoError(t, err)
		return upgraded
	}, 30*time.Second, 50*time.Millisecond, "precondition: the dynamic index went past its threshold")
	recordsAreSeeded(t, shard)
	works(t, shard)
	noNameDerivedFiles(t, shard)

	t.Run("restart and write", func(t *testing.T) {
		shard = reload(t, ctx, shard, class)
		recordsAreSeeded(t, shard)
		works(t, shard)
		put(t, shard, 5)
		works(t, shard)
		noNameDerivedFiles(t, shard)
	})

	t.Run("backup and restore", func(t *testing.T) {
		shard = backupAndRestoreInPlace(t, ctx, shard, idx, class)
		recordsAreSeeded(t, shard)
		works(t, shard)
		noNameDerivedFiles(t, shard)
	})

	t.Run("drop", func(t *testing.T) {
		dropFromSchema := func(name string) {
			markDropped(class, name)
			shard.index.vectorIndexUserConfigLock.Lock()
			delete(shard.index.vectorIndexUserConfigs, name)
			shard.index.vectorIndexUserConfigLock.Unlock()
		}

		// loaded
		dropFromSchema("zebrahnsw")
		require.NoError(t, shard.DropVectorIndex(ctx, "zebrahnsw"))
		assert.Empty(t, entriesWithID(t, shard, seeded["zebrahnsw"].storageID()))
		assert.Empty(t, entriesWithID(t, shard, seeded["zebrahnsw"].QueueID))

		// interrupted: the next load finishes it
		dropFromSchema("zebrafresh")
		dropping := seeded["zebrafresh"]
		dropping.State = vectorIndexStateDropping
		shard = reloadAfter(t, ctx, shard, class, func() {
			withOfflineMapping(t, shard.path(), func(m *vectorIndexMapping, _ *shardmeta.Namespace) {
				require.NoError(t, m.Put("zebrafresh", dropping))
			})
		})
		assert.Empty(t, entriesWithID(t, shard, seeded["zebrafresh"].storageID()))
		assert.Empty(t, entriesWithID(t, shard, seeded["zebrafresh"].QueueID))

		// cold
		dropFromSchema("zebradyn")
		require.NoError(t, shard.Shutdown(ctx))
		h := newVectorDropIndexHelper()
		require.NoError(t, h.removeVectorIndexFiles(shard.index.path(), shard.name, "zebradyn", otherTargetVectors(class, "zebradyn")))
		assert.Empty(t, entriesWithID(t, shard, seeded["zebradyn"].storageID()))
		assert.Empty(t, entriesWithID(t, shard, seeded["zebradyn"].QueueID))
		upgraded, err := dynamic.UpgradedOnDisk(shard.path(), seeded["zebradyn"].storageID())
		require.NoError(t, err)
		assert.False(t, upgraded, "the dynamic state key went with it")

		// the siblings are untouched
		shard = openShardFromDisk(t, ctx, shard.index, class, shard.Name())
		delete(all, "zebrahnsw")
		delete(all, "zebrafresh")
		delete(all, "zebradyn")
		for _, name := range []string{"zebrahnsw", "zebrafresh", "zebradyn"} {
			delete(seeded, name)
		}
		recordsAreSeeded(t, shard)
		works(t, shard)
		noNameDerivedFiles(t, shard)
	})
}
