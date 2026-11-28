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
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storobj"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// heldVectorIndex is a vector index that takes no vector until it has been
// flushed once, or until it has waited that long for the flush. It stands
// between the queue of a shard and its index for one batch, and has the
// workers of the batch get to the index after the batch has flushed, which
// nothing in the batch keeps them from: they are the shared workers of the
// node, and take the vectors of a batch when they get to them.
type heldVectorIndex struct {
	VectorIndex
	wait time.Duration

	once sync.Once
	open chan struct{}
	// flushedFirst tells that the flush came before any vector went in.
	flushedFirst atomic.Bool
}

func newHeldVectorIndex(index VectorIndex, wait time.Duration) *heldVectorIndex {
	return &heldVectorIndex{VectorIndex: index, wait: wait, open: make(chan struct{})}
}

func (h *heldVectorIndex) Flush() error {
	err := h.VectorIndex.Flush()
	h.once.Do(func() {
		h.flushedFirst.Store(true)
		close(h.open)
	})
	return err
}

// hold returns once the index was flushed. A batch that flushes after its
// vectors are in never flushes before them: every vector then goes in when
// the first of them has waited long enough.
func (h *heldVectorIndex) hold() {
	select {
	case <-h.open:
	case <-time.After(h.wait):
		h.once.Do(func() { close(h.open) })
	}
}

func (h *heldVectorIndex) Add(ctx context.Context, id uint64, vector []float32) error {
	h.hold()
	return h.VectorIndex.Add(ctx, id, vector)
}

func (h *heldVectorIndex) AddBatch(ctx context.Context, ids []uint64, vectors [][]float32) error {
	h.hold()
	return h.VectorIndex.AddBatch(ctx, ids, vectors)
}

// A batch stores its objects in the store, hands their vectors to the shared
// workers of the node, has the store write its buffered logs and flushes the
// vector index, and only then waits for the workers (putBatch:
// objectsBatcher.Objects, with storeAdditionalStorageWithWorkers and
// flushWALs, then batcher.wg.Wait). What the workers write of the vectors
// after that flush, in the log of the index or in the buckets it keeps in the
// store, is still in a buffer of the process when the batch returns, and
// nothing of the batch hands it to the system. A process that dies then
// leaves the objects the batch answered for on disk and not their vectors: no
// search by vector finds them, and nothing after the restart puts them back
// in the index. An object that changed its vector is worse off: the delete
// of its old vector was flushed, its new vector was not.
//
// Every object a batch answered for without an error should come back after
// the restart with its vector.
//
// With asynchronous indexing the batch flushes the queue after it put the
// vectors in it (putBatchAsync), and does not leave this: its row is here to
// tell the two apart.
func TestShard_CrashAfterABatchWasAcknowledged(t *testing.T) {
	for _, tt := range []struct {
		name    string
		vic     schemaConfig.VectorIndexConfig
		async   bool
		updates bool
	}{
		{name: "hnsw", vic: hnswent.NewDefaultUserConfig()},
		{name: "hnsw, a batch that changes vectors too", vic: hnswent.NewDefaultUserConfig(), updates: true},
		{name: "flat", vic: flatent.NewDefaultUserConfig()},
		{name: "hnsw, asynchronous indexing", vic: hnswent.NewDefaultUserConfig(), async: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.async {
				t.Setenv("ASYNC_INDEXING_STALE_TIMEOUT", "100ms")
			}
			crashAfterABatchWasAcknowledged(t, tt.vic, tt.async, tt.updates)
		})
	}
}

func crashAfterABatchWasAcknowledged(t *testing.T, vic schemaConfig.VectorIndexConfig, async, updates bool) {
	const (
		class = "BatchCrash"
		dims  = 16
		base  = 24
		n     = 24
		// changed is how many objects of the batch are objects of before it
		// with another vector, in a batch that updates.
		changed = 16
		// held is how long a vector of the batch waits for the flush of the
		// batch before it goes in without it.
		held = 2 * time.Second
	)
	ctx := context.Background()
	shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: class}, vic, false, async)
	shard := underlyingShard(t, shd)
	r := rand.New(rand.NewSource(13))
	before := createRandomObjects(r, class, base, dims)
	for _, err := range shard.PutObjectBatch(ctx, before) {
		require.NoError(t, err)
	}
	vi, vq := getVectorIndexAndQueue(t, shard, "")
	if async {
		waitForVectorQueue(t, shard, "")
	}
	// Everything so far is on disk, as after any later request: the logs of
	// the store, and what the index logged of the vectors.
	require.NoError(t, shard.store.WriteWALs())
	require.NoError(t, vi.Flush())

	// The batch: new objects, and in a batch that updates, objects of before
	// it with another vector.
	batch := createRandomObjects(r, class, n, dims)
	untouched := before
	if updates {
		for i := range changed {
			batch[i].Object.ID = before[i].Object.ID
		}
		untouched = before[changed:]
	}
	var hold *heldVectorIndex
	if async {
		// The workers of the queue do not get to the batch before the crash.
		require.NoError(t, vq.Pause(ctx))
	} else {
		hold = newHeldVectorIndex(vi, held)
		vq.ResetWith(hold)
	}

	// The batch returns, and the files as a crash then leaves them.
	errs := shard.PutObjectBatch(ctx, batch)
	files := map[string][]byte{}
	require.NoError(t, filepath.WalkDir(shard.path(), func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(shard.path(), path)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		files[rel] = data
		return nil
	}))
	require.Len(t, errs, n)
	for i, err := range errs {
		require.NoError(t, err, "object %d of the batch", i)
	}
	if hold != nil {
		t.Logf("the batch flushed the index before its vectors went in: %v", hold.flushedFirst.Load())
	}

	// The node restarts on those files.
	dir := shard.path()
	require.NoError(t, shard.Shutdown(ctx))
	require.NoError(t, os.RemoveAll(dir))
	for rel, data := range files {
		path := filepath.Join(dir, rel)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, data, 0o644))
	}
	restoredLike, err := idx.initShard(ctx, shard.name, idx.getClass(), nil, true, true)
	require.NoError(t, err)
	idx.shards.Store(shard.name, restoredLike)
	restored := underlyingShard(t, restoredLike)
	if async {
		waitForVectorQueue(t, restored, "")
	}
	restoredIndex, release, ok := restored.AcquireVectorIndex("")
	require.True(t, ok)
	defer release()

	// back tells what came back of an object: the object, its vector in the
	// index, and whether a search for its vector over everything the index
	// can hold returns it.
	back := func(obj *storobj.Object) (object, vector, found bool) {
		got, err := restored.ObjectByID(ctx, obj.ID(), nil, additional.Properties{})
		require.NoError(t, err)
		ids, _, err := restoredIndex.SearchByVector(ctx, obj.Vector, base+n, nil)
		require.NoError(t, err)
		return got != nil, restoredIndex.ContainsDoc(obj.DocID), slices.Contains(ids, obj.DocID)
	}

	// What was on disk before the batch is back whole: the restart of the
	// test is a restart.
	for i, obj := range untouched {
		object, vector, found := back(obj)
		require.True(t, object && vector && found,
			"object %d of before the batch, doc %d: its object is back: %v; its vector: %v; a search for its vector "+
				"finds it: %v. It was all on disk before the batch", i, obj.DocID, object, vector, found)
	}

	var noObject, noVector, vectorAlone, notFound []uint64
	for _, obj := range batch {
		object, vector, found := back(obj)
		switch {
		case !object && vector:
			vectorAlone = append(vectorAlone, obj.DocID)
		case !object:
			noObject = append(noObject, obj.DocID)
		case !vector:
			noVector = append(noVector, obj.DocID)
		}
		if !found {
			notFound = append(notFound, obj.DocID)
		}
	}
	t.Logf("after the crash, of the %d objects the batch answered for: %d are not back, %d are back without their "+
		"vector, %d vectors are back without their object, %d are not found by a search for their vector",
		n, len(noObject)+len(vectorAlone), len(noVector), len(vectorAlone), len(notFound))
	assert.Empty(t, noObject, "docs %v: the batch answered for them without an error, and they are not there", noObject)
	assert.Empty(t, notFound,
		"docs %v: the batch answered for them without an error, and a search for their own vector does not find them",
		notFound)
	assert.Empty(t, noVector, "docs %v: the object came back and its vector did not", noVector)
	assert.Empty(t, vectorAlone, "docs %v: the vector came back and its object did not", vectorAlone)
}
