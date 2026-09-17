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
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/backup"
	entlsmkv "github.com/weaviate/weaviate/entities/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storobj"
	esync "github.com/weaviate/weaviate/entities/sync"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/objects"
)

const dropVecClassName = "DropVectorWriteRejectClass"

// setupDropVectorShard builds a shard with named vectors "foo" (hnsw) and "mv"
// (multivector) plus a "label" property. The returned *models.Class is the live
// schema object the shard reads via getClass(), so markDropped simulates the
// drop marker applying before queue teardown has happened.
func setupDropVectorShard(t *testing.T, ctx context.Context) (*Shard, *models.Class) {
	t.Helper()
	class := &models.Class{
		Class: dropVecClassName,
		InvertedIndexConfig: &models.InvertedIndexConfig{
			UsingBlockMaxWAND: config.DefaultUsingBlockMaxWAND,
		},
		Properties: []*models.Property{
			{
				Name:         "label",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWord,
			},
		},
		VectorConfig: map[string]models.VectorConfig{
			"foo": {VectorIndexType: hnsw.NewDefaultUserConfig().IndexType(), VectorIndexConfig: hnsw.NewDefaultUserConfig()},
			"mv":  {VectorIndexType: hnsw.NewDefaultMultiVectorUserConfig().IndexType(), VectorIndexConfig: hnsw.NewDefaultMultiVectorUserConfig()},
		},
	}
	vic := hnsw.UserConfig{Distance: common.DefaultDistanceMetric}
	shardLike, _ := testShardWithSettings(t, ctx, class, vic, false, false, func(i *Index) {
		i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
			"foo": hnsw.NewDefaultUserConfig(),
			"mv":  hnsw.NewDefaultMultiVectorUserConfig(),
		}
	})

	switch s := shardLike.(type) {
	case *Shard:
		return s, class
	case *LazyLoadShard:
		require.NoError(t, s.Load(ctx))
		return s.shard, class
	default:
		t.Fatalf("unexpected shard type %T", shardLike)
		return nil, nil
	}
}

func dropVecObject(t *testing.T, label string, withFoo bool) *storobj.Object {
	t.Helper()
	obj := &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:         strfmt.UUID(uuid.NewString()),
			Class:      dropVecClassName,
			Properties: map[string]interface{}{"label": label},
		},
	}
	if withFoo {
		obj.Vectors = map[string][]float32{"foo": {1, 2, 3}}
	}
	return obj
}

func dropVecMultiObject(t *testing.T, label string) *storobj.Object {
	t.Helper()
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:         strfmt.UUID(uuid.NewString()),
			Class:      dropVecClassName,
			Properties: map[string]interface{}{"label": label},
		},
		MultiVectors: map[string][][]float32{"mv": {{1, 2}, {3, 4}}},
	}
}

func markDropped(class *models.Class, targetVector string) {
	class.VectorConfig[targetVector] = models.VectorConfig{VectorIndexType: modelsext.VectorIndexTypeNone}
}

// TestDropVectorIndex_PutRejected covers the put path (PutObject -> putOne),
// including the window where the marker is visible but the queue is still live.
func TestDropVectorIndex_PutRejected(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)

	require.NoError(t, shard.PutObject(ctx, dropVecObject(t, "a", true)))

	// Drop the marker but leave the queue alive (the race window).
	markDropped(class, "foo")

	err := shard.PutObject(ctx, dropVecObject(t, "b", true))
	require.Error(t, err)
	require.Contains(t, err.Error(), "vector index not found")
	require.Contains(t, err.Error(), "foo")

	// A write not carrying the dropped vector still succeeds.
	require.NoError(t, shard.PutObject(ctx, dropVecObject(t, "c", false)))
}

// TestDropVectorIndex_MergeRejected pins both halves of the merge contract: a
// client explicitly supplying the dropped vector is rejected, while a
// property-only merge on an object that still carries the dropped vector
// (carried over by mergeProps) succeeds — the carried-over vector is skipped,
// not errored. The latter is a regression guard for the pre-existing bug where
// such merges failed with "vector index not found" after a partial write.
func TestDropVectorIndex_MergeRejected(t *testing.T) {
	ctx := testCtx()

	t.Run("client-supplied dropped vector is rejected", func(t *testing.T) {
		shard, class := setupDropVectorShard(t, ctx)
		obj := dropVecObject(t, "a", true)
		require.NoError(t, shard.PutObject(ctx, obj))

		markDropped(class, "foo")

		err := shard.MergeObject(ctx, objects.MergeDocument{
			ID:              obj.ID(),
			Class:           dropVecClassName,
			PrimitiveSchema: map[string]interface{}{"label": "b"},
			Vectors:         models.Vectors{"foo": []float32{4, 5, 6}},
			UpdateTime:      2_000,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "vector index not found")
		require.Contains(t, err.Error(), "foo")
	})

	t.Run("property-only merge carrying a dropped vector succeeds", func(t *testing.T) {
		shard, class := setupDropVectorShard(t, ctx)
		obj := dropVecObject(t, "a", true)
		require.NoError(t, shard.PutObject(ctx, obj))

		markDropped(class, "foo")
		// Tear the queue down too, for the real post-drop state: the stored
		// object still carries foo, but mergeProps copies it forward and the
		// re-index loop must skip it rather than error on the missing queue.
		require.NoError(t, shard.DropVectorIndex(ctx, "foo"))

		require.NoError(t, shard.MergeObject(ctx, objects.MergeDocument{
			ID:              obj.ID(),
			Class:           dropVecClassName,
			PrimitiveSchema: map[string]interface{}{"label": "b"},
			UpdateTime:      2_000,
		}))

		retrieved, err := shard.ObjectByID(ctx, obj.ID(), nil, additional.Properties{})
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.Equal(t, "b", retrieved.Object.Properties.(map[string]interface{})["label"])
	})

	t.Run("client-supplied dropped multivector is rejected", func(t *testing.T) {
		shard, class := setupDropVectorShard(t, ctx)
		obj := dropVecMultiObject(t, "a")
		require.NoError(t, shard.PutObject(ctx, obj))

		markDropped(class, "mv")

		err := shard.MergeObject(ctx, objects.MergeDocument{
			ID:              obj.ID(),
			Class:           dropVecClassName,
			PrimitiveSchema: map[string]interface{}{"label": "b"},
			Vectors:         models.Vectors{"mv": [][]float32{{5, 6}, {7, 8}}},
			UpdateTime:      2_000,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "vector index not found")
		require.Contains(t, err.Error(), "mv")
	})

	t.Run("property-only merge carrying a dropped multivector succeeds", func(t *testing.T) {
		shard, class := setupDropVectorShard(t, ctx)
		obj := dropVecMultiObject(t, "a")
		require.NoError(t, shard.PutObject(ctx, obj))

		markDropped(class, "mv")
		require.NoError(t, shard.DropVectorIndex(ctx, "mv"))

		// mergeProps copies MultiVectors forward too; the re-index loop must skip
		// the carried-over dropped multivector instead of erroring.
		require.NoError(t, shard.MergeObject(ctx, objects.MergeDocument{
			ID:              obj.ID(),
			Class:           dropVecClassName,
			PrimitiveSchema: map[string]interface{}{"label": "b"},
			UpdateTime:      2_000,
		}))

		retrieved, err := shard.ObjectByID(ctx, obj.ID(), nil, additional.Properties{})
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.Equal(t, "b", retrieved.Object.Properties.(map[string]interface{})["label"])
	})
}

// TestDropVectorIndex_BatchRejected covers the batch path
// (storeObjectOfBatchInLSM): only the item carrying the dropped vector fails.
func TestDropVectorIndex_BatchRejected(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)

	require.NoError(t, shard.PutObject(ctx, dropVecObject(t, "warmup", true)))

	markDropped(class, "foo")

	withFoo := dropVecObject(t, "withfoo", true)
	withoutFoo := dropVecObject(t, "withoutfoo", false)

	errs := shard.PutObjectBatch(ctx, []*storobj.Object{withFoo, withoutFoo})
	require.Len(t, errs, 2)
	require.Error(t, errs[0])
	require.Contains(t, errs[0].Error(), "vector index not found")
	require.Contains(t, errs[0].Error(), "foo")
	require.NoError(t, errs[1])
}

// The completion sweep on a loaded shard re-runs the shard's drop: it finishes
// a drop that failed part-way and never opens index.db against the shard's lock.
func TestDropVectorIndex_CompletionSweepRetriesThroughLoadedShard(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)
	markDropped(class, "foo")
	require.NoError(t, shard.DropVectorIndex(ctx, "foo"))

	// leftovers of a removal that failed part-way
	leftovers := []string{
		filepath.Join(shard.path(), helpers.GetHNSWCommitLogDirName("foo")),
		filepath.Join(shard.pathLSM(), helpers.GetVectorsBucketName("foo")),
	}
	for _, dir := range leftovers {
		require.NoError(t, os.MkdirAll(dir, 0o755))
	}

	db := &DB{logger: shard.index.logger, indices: map[string]*Index{shard.index.ID(): shard.index}}
	start := time.Now()
	require.NoError(t, db.EnsureDroppedVectorFilesRemoved(class.Class, shard.name, []string{"foo"}))
	// the offline route waits a second per target on this shard's lock
	assert.Less(t, time.Since(start), time.Second)

	for _, dir := range leftovers {
		_, err := os.Stat(dir)
		assert.True(t, os.IsNotExist(err), "the sweep finished the drop: %s", dir)
	}

	// the sibling vector is untouched
	found, err := shard.WithVectorIndex("mv", func(VectorIndex) error { return nil })
	require.NoError(t, err)
	assert.True(t, found)
}

// A loaded drop removes the vector's record and leaves the siblings'.
func TestDropVectorIndex_DeletesTheMappingRecord(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)

	// the first load recorded every vector; the drop takes only foo's record
	markDropped(class, "foo")
	require.NoError(t, shard.DropVectorIndex(ctx, "foo"))

	records, initialized, err := shard.mapping.Load()
	require.NoError(t, err)
	assert.True(t, initialized)
	assert.Equal(t, map[string]vectorIndexRecord{
		"":   {PhysicalID: "main", IndexType: "hnsw", State: "ready"},
		"mv": {PhysicalID: "vectors_mv", IndexType: "hnsw", State: "ready"},
	}, records)

	// a retried drop still succeeds
	require.NoError(t, shard.DropVectorIndex(ctx, "foo"))
}

// A drop on a shard without a mapping (an older backup) succeeds and writes
// no record.
func TestDropVectorIndex_UninitializedMapping(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)
	wipeMapping(t, shard)

	markDropped(class, "foo")
	require.NoError(t, shard.DropVectorIndex(ctx, "foo"))

	records, initialized, err := shard.mapping.Load()
	require.NoError(t, err)
	assert.False(t, initialized)
	assert.Empty(t, records)
}

// wipeMapping removes every key of the shard's mapping namespace, the way an
// older backup restores it.
func wipeMapping(t *testing.T, shard *Shard) {
	t.Helper()
	ns := shard.metadataDB.Namespace(vectorIndexMappingNamespace)
	var keys [][]byte
	require.NoError(t, ns.ForEach(func(key, _ []byte) error {
		keys = append(keys, key)
		return nil
	}))
	for _, key := range keys {
		require.NoError(t, ns.Delete(key))
	}
}

// The completion sweep errors on a shard shutting down instead of going
// offline: the shard still holds index.db locked while its references drain.
func TestDropVectorIndex_CompletionSweepRefusesAShardShuttingDown(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)
	foo := vectorIndexRecord{PhysicalID: "vectors_foo", IndexType: "hnsw", State: "ready"}
	markDropped(class, "foo")

	shard.shutdownRequested.Store(true)
	defer shard.shutdownRequested.Store(false)

	db := &DB{logger: shard.index.logger, indices: map[string]*Index{shard.index.ID(): shard.index}}
	err := db.EnsureDroppedVectorFilesRemoved(class.Class, shard.name, []string{"foo"})
	require.ErrorIs(t, err, errShutdownInProgress)

	records, _, err := shard.mapping.Load()
	require.NoError(t, err)
	assert.Equal(t, foo, records["foo"], "nothing was swept, so the record stays for the retry")
}

// An unload has left the map but not yet shut down: the sweep waits on the
// create lock instead of deleting offline against the lock the shard holds.
// The shard stays alive past both offline timeouts (two seconds), which is
// when an unsynchronized sweep reports a false success.
func TestDropVectorIndex_CompletionSweepWaitsForAnUnload(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)
	markDropped(class, "foo")
	idx := shard.index

	// what an unload does first
	idx.shardCreateLocks.Lock(shard.name)
	_, ok := idx.shards.LoadAndDelete(shard.name)
	require.True(t, ok)

	db := &DB{logger: idx.logger, indices: map[string]*Index{idx.ID(): idx}}
	done := make(chan error, 1)
	go func() {
		done <- db.EnsureDroppedVectorFilesRemoved(class.Class, shard.name, []string{"foo"})
	}()

	// an offline sweep would have reported success by now
	select {
	case err := <-done:
		t.Fatalf("the sweep did not wait for the unload: %v", err)
	case <-time.After(2500 * time.Millisecond):
	}

	// the unload finishes
	require.NoError(t, shard.Shutdown(ctx))
	idx.shardCreateLocks.Unlock(shard.name)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the sweep did not run after the unload")
	}

	// foo's record was deleted offline, the others stay
	records, initialized, err := reopenMapping(t, shard.path())
	require.NoError(t, err)
	assert.True(t, initialized)
	_, hasFoo := records["foo"]
	assert.False(t, hasFoo)
	assert.Len(t, records, 2)
}

// reopenMapping reads a shut-down shard's mapping through a fresh handle.
func reopenMapping(t *testing.T, shardDir string) (map[string]vectorIndexRecord, bool, error) {
	t.Helper()
	db, err := shardmeta.Open(shardDir, entlsmkv.BoltFlockTimeout)
	require.NoError(t, err)
	defer db.Close()
	return newVectorIndexMapping(db).Load()
}

// entriesNamed lists every entry under the shard directory and its lsm
// directory whose name contains the vector's physical id.
func entriesNamed(t *testing.T, shard *Shard, name string) []string {
	t.Helper()
	id := helpers.VectorIndexIDForTarget(name)
	var out []string
	for _, dir := range []string{shard.path(), shard.pathLSM()} {
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		for _, e := range entries {
			if strings.Contains(e.Name(), id) {
				out = append(out, filepath.Join(dir, e.Name()))
			}
		}
	}
	return out
}

// A drop leaves nothing of the vector behind: no slot, no file, no record,
// and the sibling untouched.
func TestDropVectorIndex_LeavesNothingBehind(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)
	require.NoError(t, shard.PutObject(ctx, dropVecObject(t, "one", true)))
	require.NotEmpty(t, entriesNamed(t, shard, "foo"))
	markDropped(class, "foo")

	require.NoError(t, shard.DropVectorIndex(ctx, "foo"))

	found, err := shard.WithVectorIndex("foo", func(VectorIndex) error { return nil })
	require.NoError(t, err)
	assert.False(t, found)
	assert.Empty(t, entriesNamed(t, shard, "foo"), "no file of foo under the shard")
	_, ok, err := shard.mapping.Get("foo")
	require.NoError(t, err)
	assert.False(t, ok)
	found, err = shard.WithVectorIndex("mv", func(VectorIndex) error { return nil })
	require.NoError(t, err)
	assert.True(t, found, "the sibling is untouched")
}

// The record is dropping from before the teardown, so a crash mid-drop is
// finished at the next load.
func TestDropVectorIndex_MarksTheRecordDroppingFirst(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)
	markDropped(class, "foo")

	// a held lease keeps the drop in its drain, after the record write
	shard.vectors.drainTimeout = time.Second
	slot, ok := shard.vectors.Acquire("foo")
	require.True(t, ok)
	dropErr := make(chan error, 1)
	go func() { dropErr <- shard.DropVectorIndex(ctx, "foo") }()
	require.Eventually(t, func() bool {
		rec, ok, err := shard.mapping.Get("foo")
		require.NoError(t, err)
		return ok && rec.State == vectorIndexStateDropping
	}, time.Second, 10*time.Millisecond)

	slot.release()
	require.NoError(t, <-dropErr)
	_, ok, err := shard.mapping.Get("foo")
	require.NoError(t, err)
	assert.False(t, ok, "the record goes with the files")
}

// A drop under a halt tears the slot down, keeps every file, and the resume
// deletes them.
func TestDropVectorIndex_DeferredWhileHalted(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)
	require.NoError(t, shard.PutObject(ctx, dropVecObject(t, "one", true)))
	markDropped(class, "foo")
	before := entriesNamed(t, shard, "foo")
	require.NotEmpty(t, before)

	require.NoError(t, shard.HaltForTransfer(ctx, false, 0))
	start := time.Now()
	require.NoError(t, shard.DropVectorIndex(ctx, "foo"))
	assert.Less(t, time.Since(start), 2*time.Second, "no wait")

	found, err := shard.WithVectorIndex("foo", func(VectorIndex) error { return nil })
	require.NoError(t, err)
	assert.False(t, found, "the slot is gone")
	assert.Equal(t, before, entriesNamed(t, shard, "foo"), "the files are not")
	rec, ok, err := shard.mapping.Get("foo")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, vectorIndexStateDropping, rec.State)

	// a retry while still halted stays deferred and is not an error
	require.NoError(t, shard.DropVectorIndex(ctx, "foo"))

	require.NoError(t, shard.resumeMaintenanceCycles(ctx))
	require.Eventually(t, func() bool { return len(entriesNamed(t, shard, "foo")) == 0 }, 5*time.Second, 20*time.Millisecond)
	_, ok, err = shard.mapping.Get("foo")
	require.NoError(t, err)
	assert.False(t, ok)
}

// A halt waits for a drop that already read the shard as not halted, so the
// listing never sees a file vanish.
func TestDropVectorIndex_HaltWaitsForADeletionInFlight(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)
	markDropped(class, "foo")
	shard.vectors.drainTimeout = 2 * time.Second

	// a held lease keeps the drop in its drain, past its halt check
	slot, ok := shard.vectors.Acquire("foo")
	require.True(t, ok)
	dropErr := make(chan error, 1)
	go func() { dropErr <- shard.DropVectorIndex(ctx, "foo") }()
	require.Eventually(t, func() bool { return shard.vectorDeletions.running.Count() == 1 }, time.Second, 10*time.Millisecond)

	haltErr := make(chan error, 1)
	go func() { haltErr <- shard.HaltForTransfer(ctx, false, 0) }()
	select {
	case err := <-haltErr:
		t.Fatalf("the halt did not wait for the deletion: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	slot.release()
	require.NoError(t, <-dropErr)
	require.NoError(t, <-haltErr)
	assert.Empty(t, entriesNamed(t, shard, "foo"), "deleted before the halt was admitted")
	require.NoError(t, shard.resumeMaintenanceCycles(ctx))
}

// A shared halt keeps the deferral: the files go only at the last resume.
func TestDropVectorIndex_DeferredUntilTheLastResume(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)
	markDropped(class, "foo")
	require.NoError(t, shard.HaltForTransfer(ctx, false, 0))
	require.NoError(t, shard.HaltForTransfer(ctx, false, 0))

	require.NoError(t, shard.DropVectorIndex(ctx, "foo"))
	require.NoError(t, shard.resumeMaintenanceCycles(ctx))
	time.Sleep(200 * time.Millisecond)
	assert.NotEmpty(t, entriesNamed(t, shard, "foo"), "one halt is still held")

	require.NoError(t, shard.resumeMaintenanceCycles(ctx))
	require.Eventually(t, func() bool { return len(entriesNamed(t, shard, "foo")) == 0 }, 5*time.Second, 20*time.Millisecond)
}

// pausableIndex wraps a slot's index so a test can hold a snapshot between
// its listing and its index.db copy. The call runs under the slots' read
// lock, so a drop paused behind it removes its slot only once released.
type pausableIndex struct {
	VectorIndex
	snapshot func()
}

func (p *pausableIndex) SnapshotMutableFiles(ctx context.Context, basePath, stagingDir string) ([]string, error) {
	if p.snapshot != nil {
		p.snapshot()
	}
	return p.VectorIndex.SnapshotMutableFiles(ctx, basePath, stagingDir)
}

func pauseIndex(t *testing.T, shard *Shard, name string) *pausableIndex {
	t.Helper()
	slot, ok := shard.vectors.get(name)
	require.True(t, ok)
	p := &pausableIndex{VectorIndex: slot.index}
	require.True(t, shard.vectors.Replace(name, p))
	return p
}

// A drop that lands inside a backup snapshot removes nothing the listing
// holds: the snapshot succeeds with the files, and they go at its resume.
func TestDropVectorIndex_SnapshotKeepsTheFiles(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)
	require.NoError(t, shard.PutObject(ctx, dropVecObject(t, "one", true)))
	markDropped(class, "foo")

	dropErr := make(chan error, 1)
	pauseIndex(t, shard, "mv").snapshot = func() {
		go func() { dropErr <- shard.DropVectorIndex(ctx, "foo") }()
		// the record is written before the slot removal, which waits on us
		require.Eventually(t, func() bool {
			rec, ok, err := shard.mapping.Get("foo")
			require.NoError(t, err)
			return ok && rec.State == vectorIndexStateDropping
		}, time.Second, 10*time.Millisecond)
	}
	stagingRoot := t.TempDir()
	files, err := shard.CreateBackupSnapshot(ctx, &backup.ShardDescriptor{}, stagingRoot)
	require.NoError(t, err)
	require.NoError(t, <-dropErr)

	var staged []string
	for _, rel := range files {
		if strings.Contains(rel, helpers.VectorIndexIDForTarget("foo")) {
			staged = append(staged, rel)
			_, err := os.Stat(filepath.Join(stagingRoot, rel))
			require.NoError(t, err, "listed and staged: %s", rel)
		}
	}
	require.NotEmpty(t, staged, "the listing held foo's files")
	require.Eventually(t, func() bool { return len(entriesNamed(t, shard, "foo")) == 0 }, 5*time.Second, 20*time.Millisecond,
		"the snapshot's resume deleted them")
}

// The halt-for-duration fallback serves a dropped vector's files until its
// release, and they go then.
func TestDropVectorIndex_FallbackServesUntilRelease(t *testing.T) {
	t.Setenv("WEAVIATE_TEST_FORCE_NO_HARDLINK", "true")
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)
	require.NoError(t, shard.PutObject(ctx, dropVecObject(t, "one", true)))
	markDropped(class, "foo")
	shard.index.replicaSnapshotOpLocks = esync.NewKeyRWLocker()
	const opID = "drop-fallback"

	files, err := shard.index.IncomingCreateReplicaSnapshot(ctx, shard.name, opID)
	require.NoError(t, err)
	var foos []string
	for _, rel := range files {
		if strings.Contains(rel, helpers.VectorIndexIDForTarget("foo")) {
			foos = append(foos, rel)
		}
	}
	require.NotEmpty(t, foos)

	require.NoError(t, shard.DropVectorIndex(ctx, "foo"))
	for _, rel := range foos {
		_, err := shard.index.IncomingGetReplicaSnapshotFileMetadata(ctx, opID, rel)
		require.NoError(t, err, "still served: %s", rel)
	}

	require.NoError(t, shard.index.IncomingReleaseReplicaSnapshot(ctx, opID))
	require.Eventually(t, func() bool { return len(entriesNamed(t, shard, "foo")) == 0 }, 5*time.Second, 20*time.Millisecond)
}

// A deferred drop whose teardown fails leaves the vector published and
// queues nothing: the resume must not delete files under a live index. The
// dropping record keeps it for the retry, which finishes the drop.
func TestDropVectorIndex_FailedDeferredTeardownKeepsTheIndex(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)
	require.NoError(t, shard.PutObject(ctx, dropVecObject(t, "one", true)))
	markDropped(class, "foo")

	// halt first: the preparation talks to the real index, the mock only
	// sees the teardown, the resume and the retry
	require.NoError(t, shard.HaltForTransfer(ctx, false, 0))
	real, release, ok := shard.AcquireVectorIndex("foo")
	require.True(t, ok)
	release()
	t.Cleanup(func() { real.Shutdown(ctx) })
	failing := NewMockVectorIndex(t)
	failing.On("Shutdown", mock.Anything).Return(assert.AnError).Once()
	failing.On("ResumeAfterBackup", mock.Anything).Return(nil).Once()
	failing.On("Drop", mock.Anything, false).Return(nil).Once()
	require.True(t, shard.vectors.Replace("foo", failing))

	err := shard.DropVectorIndex(ctx, "foo")
	require.ErrorIs(t, err, assert.AnError)
	found, err := shard.WithVectorIndex("foo", func(VectorIndex) error { return nil })
	require.NoError(t, err)
	assert.True(t, found, "the slot reopened on the failed teardown")

	require.NoError(t, shard.resumeMaintenanceCycles(ctx))
	time.Sleep(200 * time.Millisecond)
	assert.NotEmpty(t, entriesNamed(t, shard, "foo"), "the resume left the live index's files alone")
	rec, ok, err := shard.mapping.Get("foo")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, vectorIndexStateDropping, rec.State)

	// the retry, no longer halted, tears the mock down and finishes
	require.NoError(t, shard.DropVectorIndex(ctx, "foo"))
	assert.Empty(t, entriesNamed(t, shard, "foo"))
	_, ok, err = shard.mapping.Get("foo")
	require.NoError(t, err)
	assert.False(t, ok)
}
