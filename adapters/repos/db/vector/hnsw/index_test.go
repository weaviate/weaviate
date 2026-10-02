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

package hnsw

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/compact"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw/packedconn"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

func TestHnswIndex(t *testing.T) {
	ctx := context.Background()
	index := createEmptyHnswIndexForTests(t, testVectorForID)

	for i, vec := range testVectors {
		err := index.Add(ctx, uint64(i), vec)
		require.Nil(t, err)
	}

	t.Run("searching within cluster 1", func(t *testing.T) {
		position := 0
		res, _, err := index.SearchByVector(ctx, testVectors[position], 3, nil)
		require.Nil(t, err)
		assert.ElementsMatch(t, []uint64{0, 1, 2}, res)
	})

	t.Run("searching within cluster 2", func(t *testing.T) {
		position := 3
		res, _, err := index.SearchByVector(ctx, testVectors[position], 3, nil)
		require.Nil(t, err)
		assert.ElementsMatch(t, []uint64{3, 4, 5}, res)
	})

	t.Run("searching within cluster 3", func(t *testing.T) {
		position := 6
		res, _, err := index.SearchByVector(ctx, testVectors[position], 3, nil)
		require.Nil(t, err)
		assert.ElementsMatch(t, []uint64{6, 7, 8}, res)
	})

	t.Run("searching within cluster 2 with a scope larger than the cluster", func(t *testing.T) {
		position := 3
		res, _, err := index.SearchByVector(ctx, testVectors[position], 50, nil)
		require.Nil(t, err)
		assert.Equal(t, []uint64{
			3, 5, 4, // cluster 2
			7, 8, 6, // cluster 3
			2, 1, 0, // cluster 1
		}, res)
	})

	t.Run("searching with negative value of k", func(t *testing.T) {
		position := 0
		_, _, err := index.SearchByVector(ctx, testVectors[position], -1, nil)
		require.Error(t, err)
	})
}

func TestHnswIndexGrow(t *testing.T) {
	ctx := context.Background()
	vector := []float32{0.1, 0.2}
	vecForIDFn := func(ctx context.Context, id uint64) ([]float32, error) {
		return vector, nil
	}
	index := createEmptyHnswIndexForTests(t, vecForIDFn)

	t.Run("should grow initial empty index", func(t *testing.T) {
		// when we invoke Add method suggesting a size bigger then the default
		// initial size, then if we don't grow an index at initial state
		// we get: panic: runtime error: index out of range [25001] with length 25000
		// in order to avoid this, insertInitialElement method is now able
		// to grow it's size at initial state
		err := index.Add(ctx, uint64(cache.InitialSize+1), vector)
		require.Nil(t, err)
	})

	t.Run("should grow index without panic", func(t *testing.T) {
		// This test shows that we had an edge case that was not covered
		// in growIndexToAccomodateNode method which was leading to panic:
		// panic: runtime error: index out of range [170001] with length 170001
		vector := []float32{0.11, 0.22}
		id := uint64(5*cache.InitialSize + 1)
		err := index.Add(ctx, id, vector)
		require.Nil(t, err)
		// index should grow to 5001
		assert.Equal(t, int(id)+cache.MinimumIndexGrowthDelta, len(index.nodes))
		// the exact cache size depends on the growth strategy (small caches
		// grow relative to the requested id); it must cover the nodes slice
		assert.GreaterOrEqual(t, index.cache.Len(), int32(len(index.nodes)))
		// try to add a vector with id: 8001
		id = uint64(6*cache.InitialSize + cache.MinimumIndexGrowthDelta + 1)
		err = index.Add(ctx, id, vector)
		require.Nil(t, err)
		// index should grow to at least 8001
		assert.GreaterOrEqual(t, len(index.nodes), 8001)
		assert.GreaterOrEqual(t, index.cache.Len(), int32(8001))
	})

	t.Run("should grow index", func(t *testing.T) {
		// should not increase the nodes size
		sizeBefore := len(index.nodes)
		cacheBefore := index.cache.Len()
		idDontGrowIndex := uint64(6*cache.InitialSize - 1)
		err := index.Add(ctx, idDontGrowIndex, vector)
		require.Nil(t, err)
		assert.Equal(t, sizeBefore, len(index.nodes))
		assert.Equal(t, cacheBefore, index.cache.Len())
		// should increase nodes
		id := uint64(8*cache.InitialSize + 1)
		err = index.Add(ctx, id, vector)
		require.Nil(t, err)
		assert.GreaterOrEqual(t, len(index.nodes), int(id))
		assert.GreaterOrEqual(t, index.cache.Len(), int32(id))
		// should increase nodes when a much greater id is passed
		id = uint64(20*cache.InitialSize + 22)
		err = index.Add(ctx, id, vector)
		require.Nil(t, err)
		assert.Equal(t, int(id)+cache.MinimumIndexGrowthDelta, len(index.nodes))
		assert.Equal(t, int32(id+2*cache.MinimumIndexGrowthDelta), index.cache.Len())
	})
}

func TestHnswIndexGrowSafely(t *testing.T) {
	vector := []float32{0.1, 0.2}
	vecForIDFn := func(ctx context.Context, id uint64) ([]float32, error) {
		return vector, nil
	}
	index := createEmptyHnswIndexForTests(t, vecForIDFn)

	t.Run("concurrently add nodes to grow index", func(t *testing.T) {
		growAttempts := 20
		var wg sync.WaitGroup
		offset := uint64(len(index.nodes))
		ctx := context.Background()

		addVectorPair := func(ids []uint64) {
			defer wg.Done()
			err := index.AddBatch(ctx, ids, [][]float32{vector, vector})
			require.Nil(t, err)
		}

		for i := 0; i < growAttempts; i++ {
			wg.Add(4)
			go addVectorPair([]uint64{offset - 4, offset - 5})
			go addVectorPair([]uint64{offset - 3, offset})
			go addVectorPair([]uint64{offset - 2, offset + 2})
			go addVectorPair([]uint64{offset - 1, offset + 3})
			wg.Wait()
			offset = uint64(len(index.nodes))
		}

		// Calculate non-nil nodes
		nonNilNodes := 0
		for _, node := range index.nodes {
			if node != nil {
				nonNilNodes++
			}
		}

		assert.Equal(t, growAttempts*8, nonNilNodes)
	})
}

func TestHnswIndexValidatePQSegments(t *testing.T) {
	cfg := createVectorHnswIndexTestConfig()
	cfg.VectorForIDThunk = func(ctx context.Context, id uint64) ([]float32, error) {
		return []float32{1, 2, 3, 4}, nil
	}

	uc := ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
		EF:             36,
		PQ: ent.PQConfig{
			Enabled:  true,
			Segments: 3,
		},
	}

	t.Run("segments are not a divisor of the vector dimensions", func(t *testing.T) {
		index, err := New(cfg, uc, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
		require.Nil(t, err)

		err = index.ValidateBeforeInsert([]float32{1, 2, 3, 4})
		require.ErrorContains(t, err, "pq segments must be a divisor of the vector dimensions")
	})

	t.Run("segments are not a divisor of the multivector dimensions", func(t *testing.T) {
		uc.Multivector = ent.MultivectorConfig{Enabled: true}
		index, err := New(cfg, uc, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
		require.Nil(t, err)

		err = index.ValidateMultiBeforeInsert([][]float32{{1, 2, 3, 4}})
		require.ErrorContains(t, err, "pq segments must be a divisor of the vector dimensions")
	})
}

func createEmptyHnswIndexForTests(t testing.TB, vecForIDFn common.VectorForID[float32]) *hnsw {
	cfg := createVectorHnswIndexTestConfig()
	cfg.VectorForIDThunk = vecForIDFn

	index, err := New(cfg, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
		EF:             36,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)
	return index
}

func createEmptyMultiVectorHnswIndexForTests(t testing.TB, vecForIDFn common.VectorForID[[]float32]) *hnsw {
	cfg := createVectorHnswIndexTestConfig()
	cfg.MultiVectorForIDThunk = vecForIDFn

	index, err := New(cfg, ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
		Multivector: ent.MultivectorConfig{
			Enabled: true,
		},
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)
	return index
}

func createVectorHnswIndexTestConfig() Config {
	// mock out commit logger before adding data so we don't leave a disk
	// footprint. Commit logging and deserializing from a (condensed) commit log
	// is tested in a separate integration test that takes care of providing and
	// cleaning up the correct place on disk to write test files
	return Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "unittest",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      testVectorForID,
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
		AllocChecker:          memwatch.NewDummyMonitor(),
		GetViewThunk:          func() common.BucketView { return &noopBucketView{} },
	}
}

func TestIsEmptyUnlocked_ZeroLengthNodes(t *testing.T) {
	// Regression test: when h.nodes has length 0 and h.entryPointID is 0,
	// isEmptyUnlocked must return true without panicking. Before the fix,
	// the check used ">" instead of ">=" which caused an out-of-bounds
	// access: h.nodes[0] on a zero-length slice.
	t.Run("zero-length nodes slice must not panic", func(t *testing.T) {
		index := createEmptyHnswIndexForTests(t, testVectorForID)
		// Simulate state loaded from a snapshot with zero nodes
		index.shardedNodeLocks.LockAll()
		index.nodes = make([]*vertex, 0)
		index.shardedNodeLocks.UnlockAll()
		index.entryPointID = 0

		assert.NotPanics(t, func() {
			assert.True(t, index.isEmpty())
		})
	})

	t.Run("entryPointID beyond nodes length must not panic", func(t *testing.T) {
		index := createEmptyHnswIndexForTests(t, testVectorForID)
		index.shardedNodeLocks.LockAll()
		index.nodes = make([]*vertex, 5)
		index.shardedNodeLocks.UnlockAll()
		index.entryPointID = 5

		assert.NotPanics(t, func() {
			assert.True(t, index.isEmpty())
		})
	})

	t.Run("search on empty nodes returns nil without panic", func(t *testing.T) {
		index := createEmptyHnswIndexForTests(t, testVectorForID)
		index.shardedNodeLocks.LockAll()
		index.nodes = make([]*vertex, 0)
		index.shardedNodeLocks.UnlockAll()
		index.entryPointID = 0

		res, dists, err := index.SearchByVector(context.Background(), []float32{0.1, 0.2, 0.3}, 10, nil)
		assert.NoError(t, err)
		assert.Nil(t, res)
		assert.Nil(t, dists)
	})
}

func TestHnswIndexContainsDoc(t *testing.T) {
	testHnswIndexContainsDoc(t, genericVecTestHelperSingle())
}

func TestHnswIndexContainsDoc_MultiVector(t *testing.T) {
	testHnswIndexContainsDoc(t, genericVecTestHelperMulti())
}

func testHnswIndexContainsDoc[T float32 | []float32](t *testing.T, h genericVecTestHelper[T]) {
	ctx := context.Background()

	t.Run("should return false if index is empty", func(t *testing.T) {
		vecForIDFn := func(ctx context.Context, id uint64) ([]T, error) {
			t.Fatalf("vecForID should not be called on empty index")
			return nil, nil
		}
		index := h.createIndex(t, vecForIDFn)
		require.False(t, index.ContainsDoc(1))
	})

	t.Run("should return true if node is in the index", func(t *testing.T) {
		index := h.createIndex(t, h.vecForIDFn)
		for i, vec := range h.testVectors {
			err := h.addDocToIndex(index, ctx, uint64(i), vec)
			require.NoError(t, err)
		}
		require.True(t, index.ContainsDoc(5))
	})

	t.Run("should return false if node is not in the index", func(t *testing.T) {
		index := h.createIndex(t, h.vecForIDFn)
		for i, vec := range h.testVectors {
			err := h.addDocToIndex(index, ctx, uint64(i), vec)
			require.NoError(t, err)
		}
		require.False(t, index.ContainsDoc(100))
	})

	t.Run("should return false if node is deleted", func(t *testing.T) {
		index := h.createIndex(t, h.vecForIDFn)
		for i, vec := range h.testVectors {
			err := h.addDocToIndex(index, ctx, uint64(i), vec)
			require.NoError(t, err)
		}
		err := h.deleteDocFromIndex(index, ctx, uint64(5))
		require.Nil(t, err)
		require.False(t, index.ContainsDoc(5))
	})
}

func TestHnswIndexIterate(t *testing.T) {
	testHnswIndexIterate(t, genericVecTestHelperSingle())
}

func TestHnswIndexIterate_MultiVector(t *testing.T) {
	testHnswIndexIterate(t, genericVecTestHelperMulti())
}

func testHnswIndexIterate[T float32 | []float32](t *testing.T, h genericVecTestHelper[T]) {
	ctx := context.Background()
	t.Run("should not run callback on empty index", func(t *testing.T) {
		vecForIDFn := func(ctx context.Context, id uint64) ([]T, error) {
			t.Fatalf("vecForID should not be called on empty index")
			return nil, nil
		}
		index := h.createIndex(t, vecForIDFn)
		index.Iterate(func(id uint64) bool {
			t.Fatalf("callback should not be called on empty index")
			return true
		})
	})

	t.Run("should iterate over all nodes", func(t *testing.T) {
		index := h.createIndex(t, h.vecForIDFn)
		for i, vec := range h.testVectors {
			err := h.addDocToIndex(index, ctx, uint64(i), vec)
			require.NoError(t, err)
		}

		visited := make([]bool, len(h.testVectors))
		index.Iterate(func(id uint64) bool {
			visited[id] = true
			return true
		})
		for i, v := range visited {
			assert.True(t, v, "node %d was not visited", i)
		}
	})

	t.Run("should stop iteration when callback returns false", func(t *testing.T) {
		index := h.createIndex(t, h.vecForIDFn)
		for i, vec := range h.testVectors {
			err := h.addDocToIndex(index, ctx, uint64(i), vec)
			require.NoError(t, err)
		}

		counter := 0
		index.Iterate(func(id uint64) bool {
			counter++
			return counter < 5
		})
		require.Equal(t, 5, counter)
	})

	t.Run("should stop iteration when shutdownCtx is canceled", func(t *testing.T) {
		index := h.createIndex(t, h.vecForIDFn)
		for i, vec := range h.testVectors {
			err := h.addDocToIndex(index, ctx, uint64(i), vec)
			require.NoError(t, err)
		}

		counter := 0
		index.Iterate(func(id uint64) bool {
			counter++
			if counter == 5 {
				err := index.Shutdown(context.Background())
				require.NoError(t, err)
			}
			return true
		})
		require.Equal(t, 5, counter)
	})

	t.Run("should stop iteration when resetCtx is canceled", func(t *testing.T) {
		index := h.createIndex(t, h.vecForIDFn)
		for i, vec := range h.testVectors {
			err := h.addDocToIndex(index, ctx, uint64(i), vec)
			require.NoError(t, err)
		}

		counter := 0
		index.Iterate(func(id uint64) bool {
			counter++
			if counter == 5 {
				index.resetCtxCancel()
			}
			return true
		})
		require.Equal(t, 5, counter)
	})

	t.Run("should skip deleted nodes", func(t *testing.T) {
		index := h.createIndex(t, h.vecForIDFn)
		for i, vec := range h.testVectors {
			err := h.addDocToIndex(index, ctx, uint64(i), vec)
			require.NoError(t, err)
		}

		err := h.deleteDocFromIndex(index, ctx, uint64(5))
		require.NoError(t, err)

		visited := make([]bool, len(h.testVectors))
		index.Iterate(func(id uint64) bool {
			visited[id] = true
			return true
		})
		for i, v := range visited {
			if i == 5 {
				assert.False(t, v, "node %d was visited", i)
			} else {
				assert.True(t, v, "node %d was not visited", i)
			}
		}
	})
}

type genericVecTestHelper[T float32 | []float32] struct {
	createIndex        func(t testing.TB, vecForIDFn common.VectorForID[T]) *hnsw
	vecForIDFn         common.VectorForID[T]
	testVectors        [][]T
	addDocToIndex      func(i *hnsw, ctx context.Context, docID uint64, vec []T) error
	deleteDocFromIndex func(i *hnsw, ctx context.Context, docIDs ...uint64) error
}

func genericVecTestHelperSingle() genericVecTestHelper[float32] {
	return genericVecTestHelper[float32]{
		createIndex: createEmptyHnswIndexForTests,
		vecForIDFn:  testVectorForID,
		testVectors: testVectors,
		addDocToIndex: func(i *hnsw, ctx context.Context, docID uint64, vec []float32) error {
			return i.Add(ctx, docID, vec)
		},
		deleteDocFromIndex: func(i *hnsw, ctx context.Context, docIDs ...uint64) error {
			return i.Delete(docIDs...)
		},
	}
}

func genericVecTestHelperMulti() genericVecTestHelper[[]float32] {
	return genericVecTestHelper[[]float32]{
		createIndex: createEmptyMultiVectorHnswIndexForTests,
		vecForIDFn:  testMultiVectorForID,
		testVectors: testMultiVectors,
		addDocToIndex: func(i *hnsw, ctx context.Context, docID uint64, vec [][]float32) error {
			return i.AddMulti(ctx, docID, vec)
		},
		deleteDocFromIndex: func(i *hnsw, ctx context.Context, docIDs ...uint64) error {
			return i.DeleteMulti(docIDs...)
		},
	}
}

func TestCalculateUnreachablePoints_NodeZero(t *testing.T) {
	index := createEmptyHnswIndexForTests(t, testVectorForID)

	// node 0 links to the entrypoint but nothing links back to it
	conns0, err := packedconn.NewWithElements([][]uint64{{1}})
	require.NoError(t, err)
	conns1, err := packedconn.NewWithElements([][]uint64{{2}})
	require.NoError(t, err)
	conns2, err := packedconn.NewWithElements([][]uint64{{1}})
	require.NoError(t, err)

	index.Lock()
	index.entryPointID = 1
	index.currentMaximumLayer = 0
	index.nodes = []*vertex{
		{connections: *conns0},
		{connections: *conns1},
		{connections: *conns2},
	}
	index.Unlock()

	assert.Equal(t, []uint64{0}, index.calculateUnreachablePoints())
}

// TestApplyLoadedState_PQWithoutEncodersFailsInsteadOfPanicking pins that
// loaded PQ data with no encoders fails startup with an error: no compressor
// can be restored from it, and GrowCache on the missing one panicked.
func TestApplyLoadedState_PQWithoutEncodersFailsInsteadOfPanicking(t *testing.T) {
	index := createEmptyHnswIndexForTests(t, testVectorForID)

	state := ent.NewDeserializationResult(cache.InitialSize)
	state.SetCompressionPQData(&ent.PQData{Dimensions: 4, Ks: 256, M: 0})
	state.SetCompressed(true)

	require.NotPanics(t, func() {
		err := index.applyLoadedState(state)
		require.ErrorContains(t, err, "no encoders")
	})
}

// writeRawCommitLogForTest writes a raw commit log with ten linked nodes, then
// tail, into the commit log directory of the index cfg describes.
func writeRawCommitLogForTest(t *testing.T, cfg Config, tail func(w *compact.WALWriter)) (path string, validSize int64) {
	t.Helper()
	dir := commitLogDirectory(cfg.RootPath, cfg.ID)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	path = filepath.Join(dir, "1000")
	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()

	w := compact.NewWALWriter(f)
	require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
	for id := uint64(0); id < 10; id++ {
		require.NoError(t, w.WriteAddNode(id, 0))
		require.NoError(t, w.WriteAddLinksAtLevel(id, 0, []uint64{(id + 1) % 10}))
	}
	st, err := f.Stat()
	require.NoError(t, err)
	tail(w)
	return path, st.Size()
}

// writeDocIDCounterForTest writes the shard's document-ID counter file, as
// indexcounter.Counter persists it, into dir.
func writeDocIDCounterForTest(t *testing.T, dir string, counter uint64) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "indexcount"), binary.LittleEndian.AppendUint64(nil, counter), 0o644))
}

// TestRestoreFromDisk_NodeIDBeyondDocIDCounter pins that an index whose node
// IDs are document IDs never loads a node far beyond the shard's document-ID
// counter: corruption decodes such IDs, and sizing the node index to one ran
// the node out of memory on every startup (weaviate/0-weaviate-issues#649).
func TestRestoreFromDisk_NodeIDBeyondDocIDCounter(t *testing.T) {
	const counter = 10
	garbage := uint64(counter + 1<<24 + 1000)

	multivector := ent.MultivectorConfig{Enabled: true}
	muvera := ent.MultivectorConfig{Enabled: true, MuveraConfig: ent.MuveraConfig{Enabled: true, KSim: 4, DProjections: 16, Repetitions: 10}}

	tests := []struct {
		name          string
		counterFile   bool
		counter       uint64
		multivector   ent.MultivectorConfig
		hfresh        bool
		wantTruncated bool
	}{
		{name: "beyond the counter and its slack", counterFile: true, counter: counter, wantTruncated: true},
		{name: "muvera node IDs are document IDs", counterFile: true, counter: counter, multivector: muvera, wantTruncated: true},
		{name: "multivector node IDs are not document IDs", counterFile: true, counter: counter, multivector: multivector},
		{name: "hfresh centroids live where there is no counter", counterFile: true, counter: counter, hfresh: true},
		{name: "zero counter means no limit", counterFile: true, counter: 0},
		{name: "no counter file means no limit"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			shardDir := t.TempDir()
			cfg := createVectorHnswIndexTestConfig()
			cfg.RootPath = shardDir
			if tc.hfresh {
				// HFresh's centroid index lives in HFresh's own directory.
				cfg.RootPath = filepath.Join(shardDir, helpers.HFreshDirName("main"))
				cfg.ID = helpers.CentroidsID("main")
			}
			cfg.MultiVectorForIDThunk = testMultiVectorForID
			if tc.counterFile {
				writeDocIDCounterForTest(t, shardDir, tc.counter)
			}
			path, validSize := writeRawCommitLogForTest(t, cfg, func(w *compact.WALWriter) {
				require.NoError(t, w.WriteAddNode(garbage, 0))
			})

			uc := ent.UserConfig{MaxConnections: 30, EFConstruction: 60, EF: 36, Multivector: tc.multivector}
			index, err := New(cfg, uc, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
			require.NoError(t, err)
			defer index.Shutdown(context.Background())

			st, err := os.Stat(path)
			require.NoError(t, err)
			// Without doc mappings in the store, a multivector index without Muvera
			// drops the loaded nodes, so only the file shows whether the limit applied.
			if !tc.multivector.Enabled || tc.multivector.MuveraConfig.Enabled {
				for id := 0; id < 10; id++ {
					require.NotNil(t, index.nodes[id], "node %d lost", id)
				}
			}
			if tc.wantTruncated {
				assert.Less(t, len(index.nodes), int(garbage), "node index sized to the garbage ID")
				assert.Equal(t, validSize, st.Size(), "commit log must be truncated before the record")
			} else {
				assert.Greater(t, len(index.nodes), int(garbage), "without a limit the node loads as before")
				assert.Greater(t, st.Size(), validSize)
			}
		})
	}
}
