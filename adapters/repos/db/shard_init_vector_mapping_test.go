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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	entlsmkv "github.com/weaviate/weaviate/entities/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
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
		"":    {PhysicalID: "main", IndexType: "hnsw", State: "ready"},
		"foo": {PhysicalID: "vectors_foo", IndexType: "hnsw", State: "ready"},
		"mv":  {PhysicalID: "vectors_mv", IndexType: "hnsw", State: "ready"},
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
			rec := vectorIndexRecord{PhysicalID: "main", IndexType: tt.indexType, State: "ready"}
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
	foo := vectorIndexRecord{PhysicalID: "vectors_foo", IndexType: "hnsw", State: "ready"}
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

	// with a vector to index, the same loss refuses the load
	require.NoError(t, shard.PutObject(ctx, dropVecObject(t, "a", true)))
	reloadExpectingError(t, ctx, shard, class, `vector "foo"`, removeFooDir)
	require.NoError(t, os.MkdirAll(fooDir, 0o755))
	shard = reload(t, ctx, shard, class)

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

// A vector added to a running shard is recorded ready at the naming rule's
// ID, with its storage durable, and survives a reload as itself.
func TestInitTargetVector_RecordsTheVector(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)

	require.NoError(t, shard.index.updateVectorIndexConfigs(ctx, map[string]schemaConfig.VectorIndexConfig{
		"added": enthnsw.NewDefaultUserConfig(),
	}))

	added := vectorIndexRecord{PhysicalID: "vectors_added", IndexType: "hnsw", State: "ready"}
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
	assert.Equal(t, vectorIndexRecord{PhysicalID: "vectors_broken", IndexType: "hnsw", State: "creating"}, rec)
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

// TestVectorIndexCollisions pins which pairs of recorded vectors share a
// physical name: the check that refuses a live creation and the warning a
// first load logs.
func TestVectorIndexCollisions(t *testing.T) {
	rec := func(id string) vectorIndexRecord {
		return vectorIndexRecord{PhysicalID: id, IndexType: "hnsw", State: "ready"}
	}
	tests := []struct {
		name    string
		records map[string]vectorIndexRecord
		want    []string
	}{
		{name: "no collision", records: map[string]vectorIndexRecord{"": rec("main"), "title": rec("vectors_title")}},
		{
			name:    "compressed next to the legacy vector",
			records: map[string]vectorIndexRecord{"": rec("main"), "compressed": rec("vectors_compressed")},
			want:    []string{`vectors "" and "compressed" share "vectors_compressed"`},
		},
		{
			name:    "muvera next to its sibling",
			records: map[string]vectorIndexRecord{"foo": rec("vectors_foo"), "foo_muvera_vectors": rec("vectors_foo_muvera_vectors")},
			want:    []string{`vectors "foo" and "foo_muvera_vectors" share "vectors_foo_muvera_vectors"`},
		},
		{
			name:    "centroids next to its sibling",
			records: map[string]vectorIndexRecord{"foo": rec("vectors_foo"), "foo_centroids": rec("vectors_foo_centroids")},
			want:    []string{`vectors "foo" and "foo_centroids" share "vectors_compressed_foo_centroids"`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, vectorIndexCollisions(tt.records))
		})
	}
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

	// a retry of a vector's own creating record is not a collision with itself
	broken := enthnsw.NewDefaultUserConfig()
	broken.Distance = "bogus"
	require.Error(t, shard.index.updateVectorIndexConfigs(ctx, map[string]schemaConfig.VectorIndexConfig{"again": broken}))
	require.NoError(t, shard.index.updateVectorIndexConfigs(ctx, map[string]schemaConfig.VectorIndexConfig{"again": enthnsw.NewDefaultUserConfig()}))
	rec, _, err := shard.mapping.Get("again")
	require.NoError(t, err)
	assert.Equal(t, "ready", rec.State)
}
