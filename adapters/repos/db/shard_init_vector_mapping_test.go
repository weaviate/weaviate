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
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
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
			assert.Equal(t, tt.want, vectorIndexCollisions(tt.owners))
		})
	}

	// a newcomer is checked against the owners, not the owners against each
	// other: an existing collision does not block an unrelated addition
	owners := map[string]string{"foo": "vectors_foo", "foo_muvera_vectors": "vectors_foo_muvera_vectors"}
	assert.Empty(t, vectorIndexCollisionsWith(owners, "bar", "vectors_bar"))
	assert.Equal(t, []string{`vectors "foo" and "foo_mv_mappings" share "vectors_foo_mv_mappings"`},
		vectorIndexCollisionsWith(owners, "foo_mv_mappings", "vectors_foo_mv_mappings"))
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
	require.ErrorContains(t, cold.Load(ctx), `vectors "" and "compressed" share "vectors_compressed"`)

	// the drop runs against the cold shard, by path; checked before the
	// reload, which would recreate an empty bucket and hide the loss
	require.NoError(t, idx.dropVectorIndex(ctx, "compressed"))
	entries, err := os.ReadDir(compressed)
	require.NoError(t, err, "the drop spared the legacy vector's bucket")
	require.NotEmpty(t, entries)

	require.NoError(t, cold.Load(ctx))
	found, err := cold.shard.WithVectorIndex("", func(index VectorIndex) error {
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
