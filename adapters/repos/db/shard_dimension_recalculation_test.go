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
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	shardusage "github.com/weaviate/weaviate/adapters/repos/db/shard_usage"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// newUnstartedTestDB returns a db that has not loaded its indices, as it is while
// the server waits for the meta store.
func newUnstartedTestDB(t *testing.T) *DB {
	t.Helper()
	logger, _ := test.NewNullLogger()
	dirName := t.TempDir()
	class := &models.Class{
		Class:               "Test",
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
	}

	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: []*models.Class{class}}},
		shardState: shardState,
	}
	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
			return readFunc(class, shardState)
		}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: []*models.Class{class}}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockSchemaReader.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()

	repo, err := New(logger, "node1", Config{
		RootPath:                  dirName,
		QueryMaximumResults:       1000,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil, nil,
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader, nil)
	require.NoError(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.False(t, repo.StartupComplete())
	return repo
}

// Before the db has loaded its indices there is none to go through, which must
// not pass for a completed reindex.
func TestRecalculateVectorDimensions_BeforeStartupCompleted(t *testing.T) {
	repo := newUnstartedTestDB(t)
	logger, _ := test.NewNullLogger()

	err := NewMigrator(repo, logger, "node1").RecalculateVectorDimensions(testCtx())
	require.Error(t, err, "no index was loaded yet, so nothing was reindexed, and that must not pass for success")
}

// dimensionsBucketRows reads the dimensions bucket the shard has in use.
func dimensionsBucketRows(t *testing.T, shard *Shard) map[string][]uint64 {
	t.Helper()
	b := shard.store.Bucket(helpers.DimensionsBucketLSM)
	require.NotNil(t, b)
	require.Equal(t, lsmkv.StrategyRoaringSet, b.Strategy())

	rows := map[string][]uint64{}
	c := b.CursorRoaringSet()
	defer c.Close()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if !v.IsEmpty() {
			rows[string(k)] = v.ToArray()
		}
	}
	return rows
}

func recalculationTestShard(t *testing.T, ctx context.Context) (*Shard, *Index, *models.Class) {
	t.Helper()
	class := &models.Class{Class: "TestClass"}
	shd, idx := testShard(t, ctx, class.Class, func(i *Index) {
		i.Config.TrackVectorDimensions = true
		i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
			"named": enthnsw.UserConfig{Skip: true},
			"multi": enthnsw.UserConfig{Skip: true},
		}
	})
	return shd.(*Shard), idx, class
}

func TestShard_RecalculateDimensions(t *testing.T) {
	ctx := testCtx()

	tests := []struct {
		name   string
		object func(obj *storobj.Object)
	}{
		{name: "legacy vector", object: func(obj *storobj.Object) { obj.Vector = randVector(3) }},
		{name: "named vector", object: func(obj *storobj.Object) {
			obj.Vectors = map[string][]float32{"named": randVector(5)}
		}},
		{name: "multi vector", object: func(obj *storobj.Object) {
			obj.MultiVectors = map[string][][]float32{"multi": {randVector(4), randVector(4)}}
		}},
		{name: "all of them, and properties", object: func(obj *storobj.Object) {
			obj.Vector = randVector(3)
			obj.Vectors = map[string][]float32{"named": randVector(5)}
			obj.MultiVectors = map[string][][]float32{"multi": {randVector(4), randVector(4), randVector(4)}}
			obj.Object.Properties = map[string]interface{}{"text": "some text"}
		}},
		{name: "no vector", object: func(obj *storobj.Object) {}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shard, _, class := recalculationTestShard(t, ctx)
			defer shard.Shutdown(ctx)

			for range 10 {
				obj := testObject(class.Class)
				tt.object(obj)
				require.NoError(t, shard.PutObject(ctx, obj))
			}
			tracked := dimensionsBucketRows(t, shard)

			// what the bucket tracks must not matter to the outcome
			b := shard.store.Bucket(helpers.DimensionsBucketLSM)
			require.NoError(t, b.RoaringSetAddList([]byte("named\x05\x00\x00\x00"), []uint64{1000, 1001}))
			require.NoError(t, b.RoaringSetAddList([]byte("gone\x07\x00\x00\x00"), []uint64{1}))
			for key, docIDs := range tracked {
				require.NoError(t, b.RoaringSetRemoveOne([]byte(key), docIDs[0]))
			}
			require.NotEqual(t, tracked, dimensionsBucketRows(t, shard))

			objects, err := shard.recalculateDimensions(ctx)
			require.NoError(t, err)
			assert.Equal(t, 10, objects)
			assert.Equal(t, tracked, dimensionsBucketRows(t, shard))
			require.NoDirExists(t, filepath.Join(shard.pathLSM(), "dimensions__to_roaringset_ready"))
			require.NoDirExists(t, filepath.Join(shard.pathLSM(), "dimensions___del"))

			obj := testObject(class.Class)
			obj.Vector = randVector(3)
			require.NoError(t, shard.PutObject(ctx, obj))
			assert.Contains(t, dimensionsBucketRows(t, shard)["\x03\x00\x00\x00"], obj.DocID)
		})
	}
}

// A recalculation replaces a map bucket by a roaring set one.
func TestShard_RecalculateDimensions_MapBucket(t *testing.T) {
	ctx := testCtx()
	shard, _, _, _ := dimsMigrationShard(t, ctx, 10)
	defer func() { shard.Shutdown(ctx) }()

	objects, err := shard.recalculateDimensions(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, objects)
	rows := dimensionsBucketRows(t, shard)
	require.Len(t, rows, 1, "the row the map bucket was seeded with belongs to no object")
	assert.Len(t, rows["\x03\x00\x00\x00"], 10)
}

// A bucket of the replacement's name, left over by a run that failed, must not
// end up in the recalculated bucket.
func TestShard_RecalculateDimensions_StaleReplacementBucket(t *testing.T) {
	ctx := testCtx()
	shard, idx, class := recalculationTestShard(t, ctx)
	defer shard.Shutdown(ctx)
	for range 3 {
		obj := testObject(class.Class)
		obj.Vector = randVector(3)
		require.NoError(t, shard.PutObject(ctx, obj))
	}
	tracked := dimensionsBucketRows(t, shard)

	stale, err := lsmkv.NewBucketCreator().NewBucket(ctx, filepath.Join(shard.pathLSM(), "dimensions__to_roaringset_ready"), "",
		idx.logger, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet))
	require.NoError(t, err)
	require.NoError(t, stale.RoaringSetAddList([]byte("\x03\x00\x00\x00"), []uint64{100, 101, 102}))
	require.NoError(t, stale.FlushAndSwitch())
	require.NoError(t, stale.Shutdown(ctx))

	_, err = shard.recalculateDimensions(ctx)
	require.NoError(t, err)
	assert.Equal(t, tracked, dimensionsBucketRows(t, shard))
}

// A recalculation that does not finish must leave what the shard tracks alone.
func TestShard_RecalculateDimensions_Interrupted(t *testing.T) {
	ctx := testCtx()
	shard, idx, class := recalculationTestShard(t, ctx)
	for range 10 {
		obj := testObject(class.Class)
		obj.Vector = randVector(3)
		require.NoError(t, shard.PutObject(ctx, obj))
	}
	tracked := dimensionsBucketRows(t, shard)

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err := shard.recalculateDimensions(cancelled)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, tracked, dimensionsBucketRows(t, shard))

	obj := testObject(class.Class)
	obj.Vector = randVector(3)
	require.NoError(t, shard.PutObject(ctx, obj), "writes must not be held back by a recalculation that is over")
	tracked = dimensionsBucketRows(t, shard)
	require.Len(t, tracked["\x03\x00\x00\x00"], 11)

	require.NoError(t, shard.Shutdown(ctx))
	reloaded, err := idx.initShard(ctx, shard.Name(), class, nil, true, true)
	require.NoError(t, err)
	defer reloaded.Shutdown(ctx)
	assert.Equal(t, tracked, dimensionsBucketRows(t, reloaded.(*Shard)))
}

func TestShard_RecalculateDimensions_OneAtATime(t *testing.T) {
	ctx := testCtx()
	shard, _, _ := recalculationTestShard(t, ctx)
	defer shard.Shutdown(ctx)

	shard.dimensionsLock.Lock()
	shard.dimensionsRecalculation = newDimensionsRecalculation()
	shard.dimensionsLock.Unlock()

	_, err := shard.recalculateDimensions(ctx)
	require.ErrorContains(t, err, "already")
}

// Objects are added, updated and deleted while the shard recalculates over and
// over. In the end the bucket must track exactly what the objects hold.
func TestShard_RecalculateDimensions_ConcurrentWrites(t *testing.T) {
	// long under -race on a loaded runner, the -timeout of the test bounds it
	ctx := context.Background()
	shard, _, class := recalculationTestShard(t, ctx)
	defer shard.Shutdown(ctx)

	// enough of them for every scan to take longer than a few writes
	const updated, deleted = 100, 4900
	var ids []strfmt.UUID
	for range updated + deleted {
		obj := testObject(class.Class)
		obj.Vector = randVector(3)
		require.NoError(t, shard.PutObject(ctx, obj))
		ids = append(ids, obj.ID())
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var stopOnce sync.Once
	stopWriters := func() {
		stopOnce.Do(func() { close(stop) })
		wg.Wait()
	}
	// also when the test fails, or shutting down the shard waits for them forever
	defer stopWriters()
	writeErrs := make(chan error, 3)
	write := func(f func(i int) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				if err := f(i); err != nil {
					writeErrs <- err
					return
				}
			}
		}()
	}
	write(func(i int) error { // new objects
		obj := testObject(class.Class)
		obj.Vector = randVector(3)
		return shard.PutObject(ctx, obj)
	})
	write(func(i int) error { // updates, to another vector length and back
		obj := testObject(class.Class)
		obj.Object.ID = ids[i%updated]
		obj.Vector = randVector(3 + i%2)
		return shard.PutObject(ctx, obj)
	})
	write(func(i int) error { // deletes, each object once
		if i >= deleted {
			return nil
		}
		return shard.DeleteObject(ctx, ids[updated+i], time.Time{})
	})

	for range 5 {
		_, err := shard.recalculateDimensions(ctx)
		require.NoError(t, err)
	}
	stopWriters()
	close(writeErrs)
	for err := range writeErrs {
		require.NoError(t, err)
	}

	tracked := dimensionsBucketRows(t, shard)
	fromObjects, _, err := shard.scanObjectDimensions(ctx)
	require.NoError(t, err)
	expected := map[string][]uint64{}
	for key, docIDs := range fromObjects {
		expected[key] = docIDs.ToArray()
	}
	assert.Equal(t, expected, tracked)
}

func TestDimensionsRecalculation(t *testing.T) {
	key, other := []byte("a"), []byte("b")

	tests := []struct {
		name     string
		scanned  map[string][]uint64
		writes   func(r *dimensionsRecalculation)
		expected map[string][]uint64
	}{
		{
			name:     "no writes",
			scanned:  map[string][]uint64{"a": {1, 2}},
			writes:   func(r *dimensionsRecalculation) {},
			expected: map[string][]uint64{"a": {1, 2}},
		},
		{
			name:     "added, scanned or not",
			scanned:  map[string][]uint64{"a": {1, 2}},
			writes:   func(r *dimensionsRecalculation) { r.record(key, 2, false); r.record(key, 3, false) },
			expected: map[string][]uint64{"a": {1, 2, 3}},
		},
		{
			name:     "added to a row the scan has not found",
			scanned:  map[string][]uint64{},
			writes:   func(r *dimensionsRecalculation) { r.record(key, 1, false) },
			expected: map[string][]uint64{"a": {1}},
		},
		{
			name:     "removed, scanned or not",
			scanned:  map[string][]uint64{"a": {1, 2}},
			writes:   func(r *dimensionsRecalculation) { r.record(key, 2, true); r.record(key, 3, true) },
			expected: map[string][]uint64{"a": {1}},
		},
		{
			name:     "added and removed",
			scanned:  map[string][]uint64{"a": {1}},
			writes:   func(r *dimensionsRecalculation) { r.record(key, 2, false); r.record(key, 2, true) },
			expected: map[string][]uint64{"a": {1}},
		},
		{
			name:     "removed and added again, as an update keeping the doc id does",
			scanned:  map[string][]uint64{"a": {1}},
			writes:   func(r *dimensionsRecalculation) { r.record(key, 1, true); r.record(key, 1, false) },
			expected: map[string][]uint64{"a": {1}},
		},
		{
			name:     "moved to another row",
			scanned:  map[string][]uint64{"a": {1, 2}},
			writes:   func(r *dimensionsRecalculation) { r.record(key, 1, true); r.record(other, 1, false) },
			expected: map[string][]uint64{"a": {2}, "b": {1}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows := dimensionsRows{}
			for k, docIDs := range tt.scanned {
				for _, docID := range docIDs {
					rows.set([]byte(k), docID)
				}
			}
			r := newDimensionsRecalculation()
			tt.writes(r)
			r.applyTo(rows)

			actual := map[string][]uint64{}
			for k, docIDs := range rows {
				if !docIDs.IsEmpty() {
					actual[k] = docIDs.ToArray()
				}
			}
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func putRecalculationObjects(t *testing.T, ctx context.Context, shard *Shard, class *models.Class, objects int) {
	t.Helper()
	for range objects {
		obj := testObject(class.Class)
		obj.Vector = randVector(3)
		require.NoError(t, shard.PutObject(ctx, obj))
	}
}

// The migrator goes through the shards by name, a few at a time. By the time a
// shard gets its turn it can be gone, and must then neither fail nor be loaded
// again behind the back of the index.
func TestIndex_RecalculateShardDimensions(t *testing.T) {
	ctx := testCtx()

	t.Run("loaded shard", func(t *testing.T) {
		shard, idx, class := recalculationTestShard(t, ctx)
		defer shard.Shutdown(ctx)
		putRecalculationObjects(t, ctx, shard, class, 5)

		objects, skipped, err := idx.recalculateShardDimensions(ctx, shard.Name())
		require.NoError(t, err)
		assert.False(t, skipped)
		assert.Equal(t, 5, objects)
	})

	t.Run("lazy shard is loaded in place", func(t *testing.T) {
		shard, idx, class := recalculationTestShard(t, ctx)
		putRecalculationObjects(t, ctx, shard, class, 5)
		require.NoError(t, shard.Shutdown(ctx))

		lazy, err := idx.initShard(ctx, shard.Name(), class, nil, false, true)
		require.NoError(t, err)
		defer lazy.Shutdown(ctx)
		require.False(t, lazy.(*LazyLoadShard).isLoaded())
		idx.shards.Store(shard.Name(), lazy)

		objects, skipped, err := idx.recalculateShardDimensions(ctx, shard.Name())
		require.NoError(t, err)
		assert.False(t, skipped)
		assert.Equal(t, 5, objects)
		assert.True(t, lazy.(*LazyLoadShard).isLoaded())
		assert.Same(t, lazy, idx.shards.Load(shard.Name()))
	})

	// startup keeps empty tenants unloaded, the recalculation must not load them all
	t.Run("lazy shard never written to is done without loading it", func(t *testing.T) {
		shard, idx, class := recalculationTestShard(t, ctx)
		require.NoError(t, shard.Shutdown(ctx))

		lazy, err := idx.initShard(ctx, shard.Name(), class, nil, false, true)
		require.NoError(t, err)
		defer lazy.Shutdown(ctx)
		idx.shards.Store(shard.Name(), lazy)

		objects, skipped, err := idx.recalculateShardDimensions(ctx, shard.Name())
		require.NoError(t, err)
		assert.False(t, skipped)
		assert.Zero(t, objects)
		assert.False(t, lazy.(*LazyLoadShard).isLoaded())
	})

	t.Run("unloaded shard is skipped and stays unloaded", func(t *testing.T) {
		shard, idx, class := recalculationTestShard(t, ctx)
		putRecalculationObjects(t, ctx, shard, class, 5)
		require.NoError(t, idx.UnloadLocalShard(ctx, shard.Name()))

		objects, skipped, err := idx.recalculateShardDimensions(ctx, shard.Name())
		require.NoError(t, err)
		assert.True(t, skipped)
		assert.Zero(t, objects)
		require.Nil(t, idx.shards.Load(shard.Name()))

		// a shard loaded behind the back of the index would still hold its buckets
		reloaded, err := idx.initShard(ctx, shard.Name(), class, nil, true, true)
		require.NoError(t, err)
		require.NoError(t, reloaded.Shutdown(ctx))
	})

	t.Run("shard shut down meanwhile is skipped", func(t *testing.T) {
		shard, idx, class := recalculationTestShard(t, ctx)
		putRecalculationObjects(t, ctx, shard, class, 5)
		require.NoError(t, shard.Shutdown(ctx))

		_, skipped, err := idx.recalculateShardDimensions(ctx, shard.Name())
		require.NoError(t, err)
		assert.True(t, skipped)
	})
}

// Shutting down the store waits for the scan to let go of the objects bucket, so
// the scan has to end when the shard is told to shut down, not when it is done.
func TestShard_RecalculateDimensions_EndsOnShutdown(t *testing.T) {
	ctx := testCtx()
	shard, _, class := recalculationTestShard(t, ctx)
	defer shard.Shutdown(ctx)
	putRecalculationObjects(t, ctx, shard, class, 5)
	tracked := dimensionsBucketRows(t, shard)

	cause := errors.New("shard is told to shut down")
	shard.shutCtxCancel(cause)

	_, err := shard.recalculateDimensions(ctx)
	require.ErrorIs(t, err, cause)
	assert.Equal(t, tracked, dimensionsBucketRows(t, shard))
}

func TestShard_RecalculateDimensions_ShutdownMeanwhile(t *testing.T) {
	ctx := testCtx()
	shard, _, class := recalculationTestShard(t, ctx)
	putRecalculationObjects(t, ctx, shard, class, 2000)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, err := shard.recalculateDimensions(ctx); err != nil {
				return
			}
		}
	}()

	shutdownCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	require.NoError(t, shard.Shutdown(shutdownCtx))
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		require.FailNow(t, "recalculation did not end with the shard")
	}
}

// A shard halted for a backup, a replica copy or an offload has its files listed
// and copied. Replacing the bucket would remove files from under the copy.
func TestShard_RecalculateDimensions_WaitsForTransfer(t *testing.T) {
	ctx := testCtx()
	shard, _, class := recalculationTestShard(t, ctx)
	defer shard.Shutdown(ctx)
	putRecalculationObjects(t, ctx, shard, class, 5)

	require.NoError(t, shard.HaltForTransfer(ctx, false, 0))
	bucketPath := filepath.Join(shard.pathLSM(), helpers.DimensionsBucketLSM)
	halted := dirListingForTest(t, bucketPath)
	require.NotEmpty(t, halted)

	waiting, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, err := shard.recalculateDimensions(waiting)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, halted, dirListingForTest(t, bucketPath), "the files of a halted shard must stay")

	require.NoError(t, shard.resumeMaintenanceCycles(ctx))
	objects, err := shard.recalculateDimensions(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, objects)
}

func dirListingForTest(t *testing.T, path string) []string {
	t.Helper()
	entries, err := os.ReadDir(path)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

// A usage scan that takes the shard for unloaded recovers what looks like an
// interrupted migration, and would remove the replacement while it is built.
func TestShard_RecalculateDimensions_KeepsUsageScanOut(t *testing.T) {
	ctx := testCtx()
	shard, idx, class := recalculationTestShard(t, ctx)
	defer shard.Shutdown(ctx)
	putRecalculationObjects(t, ctx, shard, class, 5)

	unlock, err := shardusage.LockUnloadedDimensionsBucket(ctx, idx.path(), shard.Name())
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		_, err := shard.recalculateDimensions(ctx)
		done <- err
	}()

	select {
	case err := <-done:
		unlock()
		require.FailNowf(t, "bucket was replaced while a usage scan had it", "err: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	require.NoDirExists(t, filepath.Join(shard.pathLSM(), "dimensions__to_roaringset_ready"))

	unlock()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		require.FailNow(t, "recalculation did not go on once the usage scan was done")
	}
}

// What a torn migration leaves behind, which the operator is told to repair by
// recalculating: the bucket in use next to a replacement and a bucket moved aside.
func TestShard_RecalculateDimensions_TornMigrationLeftovers(t *testing.T) {
	ctx := testCtx()
	shard, _, class := recalculationTestShard(t, ctx)
	putRecalculationObjects(t, ctx, shard, class, 5)
	tracked := dimensionsBucketRows(t, shard)

	bucketPath := filepath.Join(shard.pathLSM(), helpers.DimensionsBucketLSM)
	for _, suffix := range []string{"__to_roaringset_ready", "___del"} {
		require.NoError(t, os.Mkdir(bucketPath+suffix, 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(bucketPath+suffix, "segment-1.db"), []byte("stale"), 0o600))
	}

	_, err := shard.recalculateDimensions(ctx)
	require.NoError(t, err)
	assert.Equal(t, tracked, dimensionsBucketRows(t, shard))
	require.NoDirExists(t, bucketPath+"__to_roaringset_ready")
	require.NoDirExists(t, bucketPath+"___del")

	putRecalculationObjects(t, ctx, shard, class, 2)
	tracked = dimensionsBucketRows(t, shard)
	require.Len(t, tracked["\x03\x00\x00\x00"], 7)

	idx := shard.index
	require.NoError(t, shard.Shutdown(ctx))
	reloaded, err := idx.initShard(ctx, shard.Name(), class, nil, true, true)
	require.NoError(t, err)
	defer reloaded.Shutdown(ctx)
	assert.Equal(t, tracked, dimensionsBucketRows(t, reloaded.(*Shard)), "writes after the recalculation must survive a restart")
}

// ReplaceBuckets gives the replacement the name of the dimensions bucket before it
// can fail. Whatever failed, the shard has to end up with a dimensions bucket it can
// write to and load again, and not with one in a dir the next load removes.
func TestShard_RecalculateDimensions_FailedSwitch(t *testing.T) {
	ctx := testCtx()
	name := helpers.DimensionsBucketLSM + shardusage.DimensionsReplacementBucketSuffix

	// stuckDir makes the removal of the dir it is put into fail
	stuckDir := func(t *testing.T, dir string) {
		stuck := filepath.Join(dir, "stuck")
		require.NoError(t, os.Mkdir(stuck, 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(stuck, "file"), []byte("x"), 0o600))
		require.NoError(t, os.Chmod(stuck, 0o500))
		// wherever the dir has been moved to meanwhile
		t.Cleanup(func() {
			_ = filepath.WalkDir(filepath.Dir(dir), func(path string, d fs.DirEntry, err error) error {
				if err == nil && d.IsDir() && d.Name() == "stuck" {
					_ = os.Chmod(path, 0o700)
				}
				return nil
			})
		})
	}

	tests := []struct {
		name string
		// switchBucket fails the switch in its own way and reports how many
		// objects the shard tracks afterwards
		switchBucket func(t *testing.T, shard *Shard, bucketPath string) (tracked int, err error)
		expectErr    bool
	}{
		{
			name: "moving the bucket aside fails",
			switchBucket: func(t *testing.T, shard *Shard, bucketPath string) (int, error) {
				require.NoError(t, shard.store.CreateOrLoadBucket(ctx, name, shard.makeDefaultBucketOptions(lsmkv.StrategyRoaringSet)...))
				require.NoError(t, os.Mkdir(bucketPath+"___del", 0o700))
				require.NoError(t, os.WriteFile(filepath.Join(bucketPath+"___del", "in-the-way"), []byte("x"), 0o600))
				t.Cleanup(func() { _ = os.RemoveAll(bucketPath + "___del") })
				return 5, shard.switchToDimensionsReplacement(ctx, name, bucketPath)
			},
			expectErr: true,
		},
		{
			name: "moving the bucket aside fails on a shard gone read only",
			switchBucket: func(t *testing.T, shard *Shard, bucketPath string) (int, error) {
				require.NoError(t, shard.store.CreateOrLoadBucket(ctx, name, shard.makeDefaultBucketOptions(lsmkv.StrategyRoaringSet)...))
				require.NoError(t, os.Mkdir(bucketPath+"___del", 0o700))
				require.NoError(t, os.WriteFile(filepath.Join(bucketPath+"___del", "in-the-way"), []byte("x"), 0o600))
				t.Cleanup(func() { _ = os.RemoveAll(bucketPath + "___del") })
				require.NoError(t, shard.SetStatusReadonly("test"))
				err := shard.switchToDimensionsReplacement(ctx, name, bucketPath)
				require.NoError(t, shard.UpdateStatus(storagestate.StatusReady.String(), "test"))
				return 5, err
			},
			expectErr: true,
		},
		{
			name: "only removing the bucket moved aside fails",
			switchBucket: func(t *testing.T, shard *Shard, bucketPath string) (int, error) {
				stuckDir(t, bucketPath)
				objects, err := shard.recalculateDimensions(ctx)
				require.DirExists(t, bucketPath+"___del")
				return objects, err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shard, idx, class := recalculationTestShard(t, ctx)
			putRecalculationObjects(t, ctx, shard, class, 5)
			bucketPath := filepath.Join(shard.pathLSM(), helpers.DimensionsBucketLSM)

			objects, err := tt.switchBucket(t, shard, bucketPath)
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, 5, objects)
			require.NotNil(t, shard.store.Bucket(helpers.DimensionsBucketLSM), "the shard must not be left without a dimensions bucket")
			require.Nil(t, shard.store.Bucket(name))
			require.NoDirExists(t, bucketPath+"__to_roaringset_ready")
			require.Len(t, dimensionsBucketRows(t, shard)["\x03\x00\x00\x00"], 5)

			putRecalculationObjects(t, ctx, shard, class, 2)
			tracked := dimensionsBucketRows(t, shard)
			require.Len(t, tracked["\x03\x00\x00\x00"], 7)

			require.NoError(t, shard.Shutdown(ctx))
			reloaded, err := idx.initShard(ctx, shard.Name(), class, nil, true, true)
			require.NoError(t, err, "the shard must load again")
			defer reloaded.Shutdown(ctx)
			assert.Equal(t, tracked, dimensionsBucketRows(t, reloaded.(*Shard)), "writes after the failed switch must survive a restart")
		})
	}
}

// A failed switch that cannot load the dimensions bucket again, or that must not,
// leaves the shard without one. A write would then store its object and fail before
// it reaches the vector index, and a retry keeping the doc id would not reach it
// either: the object would be found by filters and never by a vector search.
func TestShard_RecalculateDimensions_FailedSwitchLeavesNoBucket(t *testing.T) {
	ctx := testCtx()

	tests := []struct {
		name string
		// fail sets up the dirs as the switch left them, returns what it failed with,
		// and undoes what would also fail the next load of the shard, or a usage report
		fail func(t *testing.T, bucketPath string) (repair func(), switchErr error)
	}{
		{
			name: "bucket to replace failed to shut down, it may still flush into its dir",
			fail: func(t *testing.T, bucketPath string) (func(), error) {
				return func() {}, fmt.Errorf("replace: %w", lsmkv.ErrReplacedBucketNotShutDown)
			},
		},
		{
			name: "whether the switch went through cannot be told",
			fail: func(t *testing.T, bucketPath string) (func(), error) {
				// the bucket in place is the complete one, and the one moved aside must
				// not be taken for a leftover
				require.NoError(t, os.Mkdir(bucketPath+"___del", 0o700))
				require.NoError(t, os.WriteFile(filepath.Join(bucketPath+"___del", "segment-1.db"), []byte("x"), 0o600))
				require.NoError(t, os.Symlink(bucketPath+"__to_roaringset_ready", bucketPath+"__to_roaringset_ready"))
				return func() {
					require.DirExists(t, bucketPath+"___del", "nothing is removed while the state is unknown")
					require.NoError(t, os.Remove(bucketPath+"__to_roaringset_ready"))
				}, errors.New("replace: failed renaming")
			},
		},
		{
			name: "loading the bucket again fails",
			fail: func(t *testing.T, bucketPath string) (func(), error) {
				require.NoError(t, os.Chmod(bucketPath, 0o300))
				t.Cleanup(func() { _ = os.Chmod(bucketPath, 0o700) })
				return func() { require.NoError(t, os.Chmod(bucketPath, 0o700)) }, errors.New("replace: failed removing dir")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			class := &models.Class{Class: "TestClass"}
			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.NewDefaultUserConfig(), false, false, false,
				func(i *Index) { i.Config.TrackVectorDimensions = true })
			shard := shd.(*Shard)
			putRecalculationObjects(t, ctx, shard, class, 5)
			tracked := dimensionsBucketRows(t, shard)
			bucketPath := filepath.Join(shard.pathLSM(), helpers.DimensionsBucketLSM)

			repair, switchErr := tt.fail(t, bucketPath)
			shard.dimensionsLock.Lock()
			err := shard.recoverFailedDimensionsSwitch(ctx, bucketPath, switchErr)
			shard.dimensionsLock.Unlock()
			require.ErrorIs(t, err, switchErr)
			require.Nil(t, shard.store.Bucket(helpers.DimensionsBucketLSM))

			require.ErrorContains(t, shard.isReadOnly(), "read-only", "writes must be refused")
			obj := testObject(class.Class)
			obj.Vector = randVector(3)
			require.Error(t, shard.PutObject(ctx, obj))

			// as by an operator, or the end of a vector index config update
			require.ErrorIs(t, shard.UpdateStatus(storagestate.StatusReady.String(), "test"), errDimensionsBucketLost)
			require.ErrorContains(t, shard.isReadOnly(), "read-only", "READY must be refused until the shard is loaded again")

			// the usage report of the node must not fail on the shard
			repair()
			_, err = idx.calculateLoadedShardUsage(ctx, shard, true)
			require.NoError(t, err)
			_, err = shard.Dimensions(ctx, "")
			require.NoError(t, err)
			// as for a target vector added since, or with MUVERA
			_, err = shard.DimensionsUsage(ctx, "", 0)
			require.NoError(t, err)

			// as a write that passed the read only check before the switch failed
			shard.dimensionsBucketLost.Store(false)
			require.NoError(t, shard.UpdateStatus(storagestate.StatusReady.String(), "test"))
			shard.dimensionsBucketLost.Store(true)
			require.NoError(t, shard.PutObject(ctx, obj), "a write under way must not fail halfway")
			stored, err := shard.ObjectByID(ctx, obj.ID(), nil, additional.Properties{})
			require.NoError(t, err)
			vectorIndex, ok := shard.GetVectorIndex("")
			require.True(t, ok)
			require.True(t, vectorIndex.ContainsDoc(stored.DocID), "the object must be in the vector index")

			require.NoError(t, shard.Shutdown(ctx))
			reloaded, err := idx.initShard(ctx, shard.Name(), class, nil, true, true)
			require.NoError(t, err, "the shard must load again")
			defer reloaded.Shutdown(ctx)
			assert.Equal(t, tracked, dimensionsBucketRows(t, reloaded.(*Shard)), "the bucket in place must be recovered")
			require.NoError(t, reloaded.(*Shard).isReadOnly())
		})
	}
}

// Set read only while the scan runs, as the disk-usage monitor does, the switch
// must not write, flush and rename on the shard all the same.
func TestShard_RecalculateDimensions_ReadOnlyMeanwhile(t *testing.T) {
	ctx := testCtx()
	shard, _, class := recalculationTestShard(t, ctx)
	defer shard.Shutdown(ctx)
	putRecalculationObjects(t, ctx, shard, class, 5)
	tracked := dimensionsBucketRows(t, shard)
	bucketPath := filepath.Join(shard.pathLSM(), helpers.DimensionsBucketLSM)

	// holds the switch in lockNotHaltedForTransfer
	require.NoError(t, shard.HaltForTransfer(ctx, false, 0))
	before := dirListingForTest(t, bucketPath)
	done := make(chan error, 1)
	go func() {
		_, err := shard.recalculateDimensions(ctx)
		done <- err
	}()
	require.Eventually(t, func() bool {
		shard.dimensionsLock.RLock()
		defer shard.dimensionsLock.RUnlock()
		return shard.dimensionsRecalculation != nil
	}, 10*time.Second, time.Millisecond)
	require.NoError(t, shard.SetStatusReadonly("disk full"))
	require.NoError(t, shard.resumeMaintenanceCycles(ctx))

	select {
	case err := <-done:
		require.ErrorContains(t, err, "read-only")
	case <-time.After(30 * time.Second):
		require.FailNow(t, "recalculation did not end")
	}
	assert.Equal(t, before, dirListingForTest(t, bucketPath), "the bucket in place must be left alone")
	assert.Nil(t, shard.store.Bucket(helpers.DimensionsBucketLSM+"__to_roaringset_ready"))
	require.NoDirExists(t, bucketPath+"__to_roaringset_ready")
	require.NoError(t, shard.UpdateStatus(storagestate.StatusReady.String(), "test"))
	assert.Equal(t, tracked, dimensionsBucketRows(t, shard))
}

// A shutdown requested while the switch holds the last reference to the shard runs
// when that reference is released. It must not find the switch still holding a
// lock the shutdown takes.
func TestShard_RecalculateDimensions_ShutdownDuringSwitch(t *testing.T) {
	ctx := testCtx()
	shard, _, class := recalculationTestShard(t, ctx)
	putRecalculationObjects(t, ctx, shard, class, 500)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, err := shard.recalculateDimensions(ctx); err != nil {
				return
			}
		}
	}()

	deadline := time.Now().Add(20 * time.Second)
	for shard.inUseCounter.Load() != 1 {
		require.True(t, time.Now().Before(deadline), "never saw the switch hold its reference")
	}
	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = shard.Shutdown(shutdownCtx)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "recalculation is stuck with the shard shutting down")
	}
	require.Eventually(t, shard.shut.Load, 10*time.Second, 10*time.Millisecond, "the shard must shut down")
}

// Readers of the dimensions bucket must not see it empty while it is replaced.
func TestShard_RecalculateDimensions_ReadersDuringSwitch(t *testing.T) {
	// long under -race on a loaded runner, the -timeout of the test bounds it
	ctx := context.Background()
	shard, _, class := recalculationTestShard(t, ctx)
	defer shard.Shutdown(ctx)
	putRecalculationObjects(t, ctx, shard, class, 5)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var wrong atomic.Int64
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if dims, err := shard.Dimensions(ctx, ""); err != nil || dims != 15 {
					wrong.Add(1)
				}
			}
		}()
	}
	for range 100 {
		_, err := shard.recalculateDimensions(ctx)
		require.NoError(t, err)
	}
	close(stop)
	wg.Wait()
	assert.Zero(t, wrong.Load(), "reads that did not see the tracked dimensions")
}

// The operator is told to remove the flag once the reindex is complete. After a
// cancel, a failure, or a shard that went away before its turn that would leave
// wrong dimensions in place for good, so then the caller gets an error to report,
// and nothing is called complete.
func TestRecalculateVectorDimensions_ReportsOutcome(t *testing.T) {
	class := &models.Class{
		Class:               "Test",
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
	}
	const complete = "Reindexing dimensions complete"

	tests := []struct {
		name        string
		ctx         func() context.Context
		prepare     func(t *testing.T, index *Index)
		expectedErr func(t *testing.T, err error)
	}{
		{name: "complete", ctx: testCtx},
		{
			name: "cancelled",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(testCtx())
				cancel()
				return ctx
			},
			expectedErr: func(t *testing.T, err error) { require.ErrorIs(t, err, context.Canceled) },
		},
		{
			name: "shard gone before its turn",
			ctx:  testCtx,
			prepare: func(t *testing.T, index *Index) {
				// shut down but not taken out of the index yet, as it is while unloading
				require.NoError(t, index.ForEachShard(func(_ string, shard ShardLike) error {
					lazy := shard.(*LazyLoadShard)
					require.NoError(t, lazy.Load(testCtx()))
					return lazy.shard.Shutdown(testCtx())
				}))
			},
			expectedErr: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "1 shards skipped")
			},
		},
		{
			name: "inactive tenant",
			ctx:  testCtx,
			prepare: func(t *testing.T, index *Index) {
				// only active tenants are loaded, so the run never sees it
				require.NoError(t, index.schemaReader.Read(index.Config.ClassName.String(), true,
					func(_ *models.Class, state *sharding.State) error {
						for _, physical := range state.Physical {
							physical.Name = "cold"
							physical.Status = models.TenantActivityStatusCOLD
							state.Physical["cold"] = physical
							break
						}
						require.True(t, state.IsLocalShard("cold"))
						return nil
					}))
			},
			expectedErr: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "1 inactive tenants not reindexed")
			},
		},
		{
			name: "shard panics",
			ctx:  testCtx,
			prepare: func(t *testing.T, index *Index) {
				// nothing in it is set up, the recalculation dereferences what is not there
				index.shards.Store("panicking", &Shard{index: index, name: "panicking"})
				t.Cleanup(func() { index.shards.LoadAndDelete("panicking") })
			},
			expectedErr: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "1 failed")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
			logger, hook := test.NewNullLogger()
			migrator := NewMigrator(db, logger, "node1")
			require.NoError(t, db.PutObject(testCtx(), &models.Object{Class: class.Class, ID: strfmt.UUID(uuid.NewString())},
				randVector(3), nil, nil, nil, 0))
			if tt.prepare != nil {
				tt.prepare(t, db.GetIndex(schema.ClassName(class.Class)))
			}

			err := migrator.RecalculateVectorDimensions(tt.ctx())

			if tt.expectedErr != nil {
				require.Error(t, err)
				tt.expectedErr(t, err)
				for _, entry := range hook.AllEntries() {
					assert.NotContains(t, entry.Message, complete)
				}
				return
			}
			require.NoError(t, err)
			last := hook.LastEntry()
			require.NotNil(t, last)
			assert.Contains(t, last.Message, complete)
			assert.EqualValues(t, 1, last.Data["shards"])
			assert.EqualValues(t, 1, last.Data["objects"])
		})
	}
}

// A tenant activated while the run goes on is not in the list of shards the run
// started from, and would keep the dimensions it had.
func TestRecalculateVectorDimensions_ShardAddedMeanwhile(t *testing.T) {
	ctx := testCtx()
	class := &models.Class{
		Class:               "Test",
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
	}
	db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
	logger, _ := test.NewNullLogger()
	index := db.GetIndex(schema.ClassName(class.Class))
	require.NoError(t, db.PutObject(ctx, &models.Object{Class: class.Class, ID: strfmt.UUID(uuid.NewString())},
		randVector(3), nil, nil, nil, 0))

	var first *Shard
	require.NoError(t, index.ForEachShard(func(_ string, shard ShardLike) error {
		lazy := shard.(*LazyLoadShard)
		require.NoError(t, lazy.Load(ctx))
		first = lazy.shard
		return nil
	}))
	// holds the run at the first shard until the other one is there
	require.NoError(t, first.HaltForTransfer(ctx, false, 0))

	done := make(chan error, 1)
	go func() { done <- NewMigrator(db, logger, "node1").RecalculateVectorDimensions(ctx) }()
	require.Eventually(t, func() bool {
		first.dimensionsLock.RLock()
		defer first.dimensionsLock.RUnlock()
		return first.dimensionsRecalculation != nil
	}, 10*time.Second, time.Millisecond)

	added, err := index.initShard(ctx, "added", class, nil, true, true)
	require.NoError(t, err)
	defer added.Shutdown(ctx)
	putRecalculationObjects(t, ctx, added.(*Shard), class, 3)
	bogus := []byte("bogus\x05\x00\x00\x00")
	require.NoError(t, added.Store().Bucket(helpers.DimensionsBucketLSM).RoaringSetAddList(bogus, []uint64{1}))
	index.shards.Store("added", added)

	require.NoError(t, first.resumeMaintenanceCycles(ctx))
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		require.FailNow(t, "run did not end")
	}
	assert.NotContains(t, dimensionsBucketRows(t, added.(*Shard)), string(bogus))
}
