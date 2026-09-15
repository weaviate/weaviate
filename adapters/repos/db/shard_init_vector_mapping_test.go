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
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
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

// With async indexing, a backup can land while every vector still waits in
// the queue. The index has written nothing, so the backup carries no
// directory for it, only the queue's chunks. The restored shard loads and
// indexes them (weaviate/dirk-claude-issues#253).
func TestRestoreShardWhoseVectorsAreStillQueued(t *testing.T) {
	for _, tt := range queuedVectorIndexCases() {
		for _, drained := range []bool{false, true} {
			name := tt.name + "/queued at backup"
			if drained {
				name = tt.name + "/drained before backup"
			}
			t.Run(name, func(t *testing.T) {
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

				stagingRoot := t.TempDir()
				sd := backup.ShardDescriptor{}
				files, err := shard.CreateBackupSnapshot(ctx, &sd, stagingRoot)
				require.NoError(t, err)

				// restore in place, the way a restore lands the backup on the node
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

				// the restored node indexes as usual
				idx.scheduler = idx.db.scheduler
				restored, err := idx.initShard(ctx, shardName, class, nil, true, true)
				require.NoError(t, err, "the restored shard must load")
				idx.shards.Store(shardName, restored)
				restoredShard := underlyingShard(t, restored)

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
}

const (
	queuedClassName   = "QueuedAtBackup"
	queuedObjectCount = 20
)

type queuedVectorIndexCase struct {
	name         string
	legacy       schemaConfig.VectorIndexConfig
	named        map[string]schemaConfig.VectorIndexConfig
	targetVector string
	object       func(i int) *storobj.Object
}

func queuedVectorIndexCases() []queuedVectorIndexCase {
	flatCfg := entflat.UserConfig{}
	flatCfg.SetDefaults()
	dynamicCfg := entdynamic.UserConfig{
		Threshold: 1_000_000,
		Distance:  distancer.NewL2SquaredProvider().Type(),
		HnswUC:    enthnsw.UserConfig{MaxConnections: 8, EFConstruction: 16, EF: 8, VectorCacheMaxObjects: 1000},
		FlatUC:    flatCfg,
	}
	legacyObject := func(i int) *storobj.Object {
		obj := queuedObject()
		obj.Vector = []float32{float32(i), 1, 2}
		return obj
	}

	return []queuedVectorIndexCase{
		{name: "legacy hnsw", legacy: enthnsw.NewDefaultUserConfig(), object: legacyObject},
		{name: "legacy flat", legacy: flatCfg, object: legacyObject},
		{name: "legacy dynamic", legacy: dynamicCfg, object: legacyObject},
		{
			name:         "named hnsw",
			legacy:       enthnsw.UserConfig{Skip: true},
			named:        map[string]schemaConfig.VectorIndexConfig{"foo": enthnsw.NewDefaultUserConfig()},
			targetVector: "foo",
			object: func(i int) *storobj.Object {
				obj := queuedObject()
				obj.Vectors = map[string][]float32{"foo": {float32(i), 1, 2}}
				return obj
			},
		},
		{
			name:         "named multivector hnsw",
			legacy:       enthnsw.UserConfig{Skip: true},
			named:        map[string]schemaConfig.VectorIndexConfig{"mv": enthnsw.NewDefaultMultiVectorUserConfig()},
			targetVector: "mv",
			object: func(i int) *storobj.Object {
				obj := queuedObject()
				obj.MultiVectors = map[string][][]float32{"mv": {{float32(i), 1}, {2, 3}}}
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
