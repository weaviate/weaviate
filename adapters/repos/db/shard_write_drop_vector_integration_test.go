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
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
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

// A halt, offload included, is refused while a drop is in flight and
// admitted once it is done.
func TestVectorLayoutBarrier_HaltRefusedDuringDrop(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)
	shard.vectors.drainTimeout = 2 * time.Second

	// a held lease keeps the drop in its drain for the whole drainTimeout
	slot, ok := shard.vectors.Acquire("foo")
	require.True(t, ok)
	markDropped(class, "foo")

	dropErr := make(chan error, 1)
	go func() { dropErr <- shard.DropVectorIndex(ctx, "foo") }()
	countedIn(t, shard, 1)

	for _, offloading := range []bool{false, true} {
		err := shard.HaltForTransfer(ctx, offloading, 0)
		require.ErrorIs(t, err, enterrors.ErrShardBusyStructuralOp, "offloading=%v", offloading)
		assert.Zero(t, shard.haltForTransferCount.Load(), "a refusal leaves the shard unhalted")
	}

	slot.release()
	require.NoError(t, <-dropErr)

	require.NoError(t, shard.HaltForTransfer(ctx, false, 0))
	require.NoError(t, shard.resumeMaintenanceCycles(ctx))
}

// A drop started under a halt removes nothing until the resume.
func TestVectorLayoutBarrier_DropWaitsForAHalt(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)
	require.NoError(t, shard.HaltForTransfer(ctx, false, 0))
	token := shard.haltLayoutToken()

	markDropped(class, "foo")
	dropErr := make(chan error, 1)
	go func() { dropErr <- shard.DropVectorIndex(ctx, "foo") }()

	select {
	case err := <-dropErr:
		t.Fatalf("the drop ran under the halt: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	commitLog := filepath.Join(shard.path(), helpers.GetHNSWCommitLogDirName("foo"))
	_, err := os.Stat(commitLog)
	require.NoError(t, err, "the files are still there for the backup")

	require.NoError(t, shard.resumeMaintenanceCycles(ctx))
	select {
	case err := <-dropErr:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the drop did not proceed after the resume")
	}
	_, err = os.Stat(commitLog)
	assert.True(t, os.IsNotExist(err))
	assert.False(t, shard.layoutChangedSince(token), "a drop that waited does not fail the snapshot")
}

// A drop that outwaits the bound proceeds under the halt and marks the
// halt's snapshot as changed.
func TestVectorLayoutBarrier_DropProceedsAfterTheBound(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)
	shard.layoutWaitTimeout = 200 * time.Millisecond
	require.NoError(t, shard.HaltForTransfer(ctx, false, 0))
	defer func() { require.NoError(t, shard.resumeMaintenanceCycles(ctx)) }()
	token := shard.haltLayoutToken()

	markDropped(class, "foo")
	start := time.Now()
	require.NoError(t, shard.DropVectorIndex(ctx, "foo"))
	assert.Less(t, time.Since(start), 2*time.Second)
	assert.Equal(t, int64(1), shard.haltForTransferCount.Load(), "the halt is still held")
	assert.True(t, shard.layoutChangedSince(token))

	found, err := shard.WithVectorIndex("foo", func(VectorIndex) error { return nil })
	require.NoError(t, err)
	assert.False(t, found)
}

// A cancelled context ends the wait without removing anything.
func TestVectorLayoutBarrier_DropCancelledWhileWaiting(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)
	require.NoError(t, shard.HaltForTransfer(ctx, false, 0))
	defer func() { require.NoError(t, shard.resumeMaintenanceCycles(ctx)) }()

	markDropped(class, "foo")
	dropCtx, cancel := context.WithCancel(ctx)
	dropErr := make(chan error, 1)
	go func() { dropErr <- shard.DropVectorIndex(dropCtx, "foo") }()
	time.Sleep(200 * time.Millisecond)
	cancel()
	require.ErrorIs(t, <-dropErr, context.Canceled)

	found, err := shard.WithVectorIndex("foo", func(VectorIndex) error { return nil })
	require.NoError(t, err)
	assert.True(t, found, "nothing was removed")
	shard.vectorLayoutGate.Lock()
	assert.Zero(t, shard.vectorLayoutChanges, "the drop counted itself out")
	shard.vectorLayoutGate.Unlock()
}

// liveCreate runs the live creation of an hnsw vector named name.
func liveCreate(ctx context.Context, shard *Shard, name string) error {
	return shard.index.updateVectorIndexConfigs(ctx, map[string]schemaConfig.VectorIndexConfig{
		name: hnsw.NewDefaultUserConfig(),
	})
}

// holdCreates blocks every create between its count-in and its build, and
// returns the release. Create takes createMu through publication, so the
// create is in flight and unpublished for as long as the test holds it.
func holdCreates(shard *Shard) (release func()) {
	shard.vectors.createMu.Lock()
	return shard.vectors.createMu.Unlock
}

// countedIn waits until n creates or drops are counted in.
func countedIn(t *testing.T, shard *Shard, n int) {
	t.Helper()
	require.Eventually(t, func() bool {
		shard.vectorLayoutGate.Lock()
		defer shard.vectorLayoutGate.Unlock()
		return shard.vectorLayoutChanges == n
	}, time.Second, 10*time.Millisecond)
}

// layoutGen reads the layout generation.
func layoutGen(shard *Shard) uint64 {
	shard.vectorLayoutGate.Lock()
	defer shard.vectorLayoutGate.Unlock()
	return shard.vectorLayoutGen
}

// pausableIndex wraps a slot's index so a test can hold a halt inside its
// preparation or a snapshot between its listing and its index.db copy. Both
// calls run under the slots' read lock: a change paused behind them cannot
// publish or remove until the pause is released, so such a change runs in a
// goroutine and the test waits for its generation bump instead.
type pausableIndex struct {
	VectorIndex
	prepare  func(call int) error // nil passes through
	snapshot func()               // nil passes through
	calls    atomic.Int32
}

func (p *pausableIndex) PrepareForBackup(ctx context.Context) error {
	if p.prepare != nil {
		err := p.prepare(int(p.calls.Add(1)))
		if err != nil {
			return err
		}
	}
	return p.VectorIndex.PrepareForBackup(ctx)
}

func (p *pausableIndex) SnapshotMutableFiles(ctx context.Context, basePath, stagingDir string) ([]string, error) {
	if p.snapshot != nil {
		p.snapshot()
	}
	return p.VectorIndex.SnapshotMutableFiles(ctx, basePath, stagingDir)
}

// pauseIndex installs a pausableIndex on the named vector.
func pauseIndex(t *testing.T, shard *Shard, name string) *pausableIndex {
	t.Helper()
	slot, ok := shard.vectors.get(name)
	require.True(t, ok)
	p := &pausableIndex{VectorIndex: slot.index}
	require.True(t, shard.vectors.Replace(name, p))
	return p
}

// dropPastTheBound drops a vector the shard never had, a change with no
// files, in a goroutine, and returns once it moved the generation. The drop
// itself completes when the slots' write lock is free.
func dropPastTheBound(t *testing.T, ctx context.Context, shard *Shard) <-chan error {
	t.Helper()
	before := layoutGen(shard)
	done := make(chan error, 1)
	go func() { done <- shard.DropVectorIndex(ctx, "gone") }()
	require.Eventually(t, func() bool { return layoutGen(shard) > before }, 5*time.Second, 10*time.Millisecond)
	return done
}

// A halt is refused while a create is in flight and not yet published.
func TestVectorLayoutBarrier_HaltRefusedDuringCreate(t *testing.T) {
	ctx := testCtx()
	shard, _ := setupDropVectorShard(t, ctx)

	release := holdCreates(shard)
	createErr := make(chan error, 1)
	go func() { createErr <- liveCreate(ctx, shard, "bar") }()
	countedIn(t, shard, 1)

	err := shard.HaltForTransfer(ctx, false, 0)
	require.ErrorIs(t, err, enterrors.ErrShardBusyStructuralOp)
	assert.Zero(t, shard.haltForTransferCount.Load())

	release()
	require.NoError(t, <-createErr)
	require.NoError(t, shard.HaltForTransfer(ctx, false, 0))
	require.NoError(t, shard.resumeMaintenanceCycles(ctx))
}

// A create started under a halt writes nothing, not even its record, until
// the resume.
func TestVectorLayoutBarrier_CreateWaitsForAHalt(t *testing.T) {
	ctx := testCtx()
	shard, _ := setupDropVectorShard(t, ctx)
	require.NoError(t, shard.HaltForTransfer(ctx, false, 0))

	createErr := make(chan error, 1)
	go func() { createErr <- liveCreate(ctx, shard, "bar") }()
	select {
	case err := <-createErr:
		t.Fatalf("the create ran under the halt: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	_, ok, err := shard.mapping.Get("bar")
	require.NoError(t, err)
	assert.False(t, ok, "no record before the resume")

	require.NoError(t, shard.resumeMaintenanceCycles(ctx))
	select {
	case err := <-createErr:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the create did not proceed after the resume")
	}
	rec, ok, err := shard.mapping.Get("bar")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "ready", rec.State)
}

// backupShardRel is the shard's directory relative to the root, the layout
// a staging dir mirrors.
func backupShardRel(t *testing.T, shard *Shard) string {
	t.Helper()
	rel, err := filepath.Rel(shard.index.Config.RootPath, shard.path())
	require.NoError(t, err)
	return rel
}

// restoreInPlace lands the backup the way the restore pipeline does: the
// descriptor's three blobs are written at their paths (usecases/backup/zip.go
// does this), then the staged shard directory replaces the live one and the
// shard reopens.
func restoreInPlace(t *testing.T, ctx context.Context, shard *Shard, class *models.Class, stagingRoot string, sd *backup.ShardDescriptor) *Shard {
	t.Helper()
	for _, blob := range []struct {
		rel  string
		data []byte
	}{
		{sd.DocIDCounterPath, sd.DocIDCounter},
		{sd.PropLengthTrackerPath, sd.PropLengthTracker},
		{sd.ShardVersionPath, sd.Version},
	} {
		require.NotEmpty(t, blob.rel, "the snapshot filled the descriptor")
		require.NoError(t, os.WriteFile(filepath.Join(stagingRoot, blob.rel), blob.data, 0o600))
	}
	staged := filepath.Join(stagingRoot, backupShardRel(t, shard))
	return reloadAfter(t, ctx, shard, class, func() {
		require.NoError(t, os.RemoveAll(shard.path()))
		require.NoError(t, os.Rename(staged, shard.path()))
	})
}

// searchIDs runs a vector search on one target vector and returns the ids.
func searchIDs(t *testing.T, ctx context.Context, shard *Shard, target string, vec []float32) []strfmt.UUID {
	t.Helper()
	objs, _, err := shard.ObjectVectorSearch(ctx, []models.Vector{vec}, []string{target},
		0, 10, nil, nil, nil, additional.Properties{}, nil, nil)
	require.NoError(t, err)
	ids := make([]strfmt.UUID, 0, len(objs))
	for _, obj := range objs {
		ids = append(ids, obj.ID())
	}
	return ids
}

// addToSchema makes the class and the index config carry an hnsw vector, the
// schema a restore reads after the backup captured it.
func addToSchema(shard *Shard, class *models.Class, name string) {
	class.VectorConfig[name] = models.VectorConfig{
		VectorIndexType:   hnsw.NewDefaultUserConfig().IndexType(),
		VectorIndexConfig: hnsw.NewDefaultUserConfig(),
	}
	shard.index.vectorIndexUserConfigLock.Lock()
	shard.index.vectorIndexUserConfigs[name] = hnsw.NewDefaultUserConfig()
	shard.index.vectorIndexUserConfigLock.Unlock()
}

// A create that arrives after the listing waits, the snapshot completes
// without it, and the restore rebuilds the vector the schema carries.
func TestVectorLayoutBarrier_CreateAfterListingIsRestoredAsNew(t *testing.T) {
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)
	obj := dropVecObject(t, "one", true) // foo = {1, 2, 3}
	require.NoError(t, shard.PutObject(ctx, obj))

	createErr := make(chan error, 1)
	pauseIndex(t, shard, "mv").snapshot = func() {
		go func() { createErr <- liveCreate(ctx, shard, "bar") }()
		time.Sleep(300 * time.Millisecond)
		_, ok, err := shard.mapping.Get("bar")
		require.NoError(t, err)
		assert.False(t, ok, "the create is waiting, nothing written")
	}
	stagingRoot := t.TempDir()
	sd := &backup.ShardDescriptor{}
	files, err := shard.CreateBackupSnapshot(ctx, sd, stagingRoot)
	require.NoError(t, err)
	require.NotEmpty(t, files)
	require.NoError(t, <-createErr, "the create proceeded after the resume")

	// the schema a restore reads was marshalled after the snapshots: it has bar
	addToSchema(shard, class, "bar")
	shard = restoreInPlace(t, ctx, shard, class, stagingRoot, sd)

	rec, ok, err := shard.mapping.Get("bar")
	require.NoError(t, err)
	require.True(t, ok, "the load created the vector the schema carries")
	assert.Equal(t, vectorIndexRecord{PhysicalID: "vectors_bar", IndexType: "hnsw", State: "ready"}, rec)

	// what the backup held is back: the object and its foo vector
	restored, err := shard.ObjectByID(ctx, obj.ID(), nil, additional.Properties{})
	require.NoError(t, err)
	require.NotNil(t, restored)
	assert.Equal(t, "one", restored.Properties().(map[string]interface{})["label"])
	assert.Equal(t, []strfmt.UUID{obj.ID()}, searchIDs(t, ctx, shard, "foo", []float32{1, 2, 3}))

	// the rebuilt bar index is empty and takes new vectors
	assert.Empty(t, searchIDs(t, ctx, shard, "bar", []float32{4, 5, 6}))
	withBar := &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:         strfmt.UUID(uuid.NewString()),
			Class:      dropVecClassName,
			Properties: map[string]interface{}{"label": "two"},
		},
		Vectors: map[string][]float32{"bar": {4, 5, 6}},
	}
	require.NoError(t, shard.PutObject(ctx, withBar))
	assert.Equal(t, []strfmt.UUID{withBar.ID()}, searchIDs(t, ctx, shard, "bar", []float32{4, 5, 6}))
}

// A create that outwaits the bound inside the snapshot fails the snapshot.
func TestVectorLayoutBarrier_CreatePastTheBoundFailsTheSnapshot(t *testing.T) {
	ctx := testCtx()
	shard, _ := setupDropVectorShard(t, ctx)
	shard.layoutWaitTimeout = 100 * time.Millisecond

	createErr := make(chan error, 1)
	pauseIndex(t, shard, "mv").snapshot = func() {
		before := layoutGen(shard)
		go func() { createErr <- liveCreate(ctx, shard, "bar") }()
		require.Eventually(t, func() bool { return layoutGen(shard) > before }, 5*time.Second, 10*time.Millisecond)
	}
	stagingRoot := t.TempDir()
	_, err := shard.CreateBackupSnapshot(ctx, &backup.ShardDescriptor{}, stagingRoot)
	require.ErrorIs(t, err, errVectorLayoutChanged)
	assert.Zero(t, shard.haltForTransferCount.Load(), "the failed snapshot resumed the shard")
	require.NoError(t, <-createErr)

	// the shard itself is fine: bar is live and recorded
	rec, ok, err := shard.mapping.Get("bar")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "ready", rec.State)
}

// A create that outwaits the bound during the halt's preparation, and is
// still unpublished when the snapshot lists, fails the snapshot: the token
// predates the preparation.
func TestVectorLayoutBarrier_CreateDuringPreparationFailsTheSnapshot(t *testing.T) {
	ctx := testCtx()
	shard, _ := setupDropVectorShard(t, ctx)
	shard.layoutWaitTimeout = 100 * time.Millisecond

	releasePrep := make(chan struct{})
	pauseIndex(t, shard, "mv").prepare = func(int) error { <-releasePrep; return nil }
	release := holdCreates(shard)

	snapErr := make(chan error, 1)
	go func() {
		_, err := shard.CreateBackupSnapshot(ctx, &backup.ShardDescriptor{}, t.TempDir())
		snapErr <- err
	}()
	require.Eventually(t, func() bool { return shard.haltedForTransfer() }, time.Second, 10*time.Millisecond)

	createErr := make(chan error, 1)
	go func() { createErr <- liveCreate(ctx, shard, "bar") }()
	// the create outwaits the bound and moves the generation, still unpublished
	require.Eventually(t, func() bool { return layoutGen(shard) > 0 }, time.Second, 10*time.Millisecond)

	close(releasePrep)
	require.ErrorIs(t, <-snapErr, errVectorLayoutChanged)

	release()
	require.NoError(t, <-createErr)
}

// A second halt, admitted after a change moved the generation under the
// first, does not renew the token: the first snapshot still fails, and the
// token stays put whether the second halt's preparation succeeds or fails.
func TestVectorLayoutBarrier_SharedHaltKeepsTheFirstToken(t *testing.T) {
	injected := errors.New("injected preparation failure")
	for _, tc := range []struct {
		name          string
		secondPrepErr error
	}{
		{name: "second halt succeeds"},
		{name: "second halt fails its preparation", secondPrepErr: injected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			shard, _ := setupDropVectorShard(t, ctx)
			shard.layoutWaitTimeout = 100 * time.Millisecond

			// the first halt is held in preparation until released, then its
			// snapshot is held until the second admission has been observed
			releasePrep := make(chan struct{})
			secondAdmitted := make(chan struct{})
			paused := pauseIndex(t, shard, "mv")
			paused.prepare = func(call int) error {
				if call == 1 {
					<-releasePrep
					return nil
				}
				close(secondAdmitted)
				return tc.secondPrepErr
			}
			paused.snapshot = func() { <-secondAdmitted }
			before := layoutGen(shard)

			snapErr := make(chan error, 1)
			go func() {
				_, err := shard.CreateBackupSnapshot(ctx, &backup.ShardDescriptor{}, t.TempDir())
				snapErr <- err
			}()
			require.Eventually(t, func() bool { return shard.haltedForTransfer() }, time.Second, 10*time.Millisecond)

			// moves the generation during the first halt's preparation, and
			// completes once the preparation lets the slots' write lock go
			dropErr := dropPastTheBound(t, ctx, shard)
			close(releasePrep)
			require.NoError(t, <-dropErr)

			// admitted alongside the first, which is paused in its snapshot
			secondErr := make(chan error, 1)
			go func() { secondErr <- shard.HaltForTransfer(ctx, false, 0) }()

			require.ErrorIs(t, <-snapErr, errVectorLayoutChanged)
			err := <-secondErr
			if tc.secondPrepErr != nil {
				require.ErrorIs(t, err, injected)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, before, shard.haltLayoutToken(), "the shared halt kept the first token")
			if err == nil {
				require.NoError(t, shard.resumeMaintenanceCycles(ctx))
			}
			assert.Zero(t, shard.haltForTransferCount.Load())
		})
	}
}

// The hardlink replica snapshot applies the same check.
func TestVectorLayoutBarrier_ReplicaSnapshotRejectsAChange(t *testing.T) {
	ctx := testCtx()
	shard, _ := setupDropVectorShard(t, ctx)
	shard.layoutWaitTimeout = 100 * time.Millisecond

	var dropErr <-chan error
	pauseIndex(t, shard, "mv").prepare = func(int) error {
		dropErr = dropPastTheBound(t, ctx, shard)
		return nil
	}
	_, err := shard.CreateReplicaSnapshot(ctx, t.TempDir())
	require.ErrorIs(t, err, errVectorLayoutChanged)
	assert.Zero(t, shard.haltForTransferCount.Load())
	require.NoError(t, <-dropErr)
}

// The halt-for-duration fallback stops serving every file, the staged
// bookkeeping copies and the live segments alike, once the layout changed
// under its halt, so the copy fails instead of landing torn.
func TestVectorLayoutBarrier_FallbackStopsServingAfterAChange(t *testing.T) {
	t.Setenv("WEAVIATE_TEST_FORCE_NO_HARDLINK", "true")
	ctx := testCtx()
	shard, class := setupDropVectorShard(t, ctx)
	shard.layoutWaitTimeout = 100 * time.Millisecond
	const opID = "layout-fallback"
	shard.index.replicaSnapshotOpLocks = esync.NewKeyRWLocker()
	// a flushed object gives the listing a live segment next to the staged copies
	require.NoError(t, shard.PutObject(ctx, dropVecObject(t, "one", true)))

	files, err := shard.index.IncomingCreateReplicaSnapshot(ctx, shard.name, opID)
	require.NoError(t, err)
	defer func() { require.NoError(t, shard.index.IncomingReleaseReplicaSnapshot(ctx, opID)) }()

	// the fallback stages the three bookkeeping copies and serves the rest live
	stagingRoot := replicaStagingDir(shard.index.Config.RootPath, opID, schema.ClassName(shard.index.Config.ClassName))
	var staged, live []string
	for _, rel := range files {
		if _, err := os.Stat(filepath.Join(stagingRoot, rel)); err == nil {
			staged = append(staged, rel)
		} else {
			live = append(live, rel)
		}
	}
	require.NotEmpty(t, staged)
	require.NotEmpty(t, live)
	for _, rel := range []string{staged[0], live[0]} {
		_, err = shard.index.IncomingGetReplicaSnapshotFileMetadata(ctx, opID, rel)
		require.NoError(t, err, "served before the change: %s", rel)
	}

	markDropped(class, "foo")
	require.NoError(t, shard.DropVectorIndex(ctx, "foo"))

	for _, rel := range []string{staged[0], live[0]} {
		_, err = shard.index.IncomingGetReplicaSnapshotFileMetadata(ctx, opID, rel)
		require.ErrorIs(t, err, errVectorLayoutChanged, "metadata of %s", rel)
		_, err = shard.index.IncomingGetReplicaSnapshotFile(ctx, opID, rel)
		require.ErrorIs(t, err, errVectorLayoutChanged, "content of %s", rel)
	}
}

// A change that lands during the fallback's own listing fails the snapshot
// before its manifest is registered.
func TestVectorLayoutBarrier_FallbackRejectsAChangeDuringListing(t *testing.T) {
	t.Setenv("WEAVIATE_TEST_FORCE_NO_HARDLINK", "true")
	ctx := testCtx()
	shard, _ := setupDropVectorShard(t, ctx)
	shard.layoutWaitTimeout = 100 * time.Millisecond
	shard.index.replicaSnapshotOpLocks = esync.NewKeyRWLocker()
	var dropErr <-chan error
	pauseIndex(t, shard, "mv").prepare = func(int) error {
		dropErr = dropPastTheBound(t, ctx, shard)
		return nil
	}

	_, err := shard.index.IncomingCreateReplicaSnapshot(ctx, shard.name, "layout-listing")
	require.ErrorIs(t, err, errVectorLayoutChanged)
	assert.Zero(t, shard.haltForTransferCount.Load(), "the failed snapshot resumed the shard")
	require.NoError(t, <-dropErr)
	_, err = shard.index.IncomingGetReplicaSnapshotFileMetadata(ctx, "layout-listing", "anything")
	require.ErrorContains(t, err, "no replica snapshot registered")
}
