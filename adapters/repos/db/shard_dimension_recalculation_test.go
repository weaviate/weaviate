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
	"sync"
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
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
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
	ctx := testCtx()
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
// can fail. Left at that, the shard writes to a dir that the next load removes.
func TestShard_RecalculateDimensions_FailedSwitchIsRolledBack(t *testing.T) {
	ctx := testCtx()
	shard, _, class := recalculationTestShard(t, ctx)
	putRecalculationObjects(t, ctx, shard, class, 5)
	tracked := dimensionsBucketRows(t, shard)

	name := helpers.DimensionsBucketLSM + shardusage.DimensionsReplacementBucketSuffix
	bucketPath := filepath.Join(shard.pathLSM(), helpers.DimensionsBucketLSM)
	require.NoError(t, shard.store.CreateOrLoadBucket(ctx, name, shard.makeDefaultBucketOptions(lsmkv.StrategyRoaringSet)...))
	// fails the first rename of the switch
	require.NoError(t, os.Mkdir(bucketPath+"___del", 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(bucketPath+"___del", "in-the-way"), []byte("x"), 0o600))

	err := shard.fillAndSwitchDimensionsBucket(ctx, name, dimensionsRows{})
	require.Error(t, err)
	require.NoError(t, shard.rollBackDimensionsBucketSwitch(ctx, name, bucketPath))

	assert.Equal(t, tracked, dimensionsBucketRows(t, shard))
	require.NoDirExists(t, bucketPath+"__to_roaringset_ready")

	putRecalculationObjects(t, ctx, shard, class, 2)
	tracked = dimensionsBucketRows(t, shard)
	require.Len(t, tracked["\x03\x00\x00\x00"], 7)

	idx := shard.index
	require.NoError(t, os.RemoveAll(bucketPath+"___del"))
	require.NoError(t, shard.Shutdown(ctx))
	reloaded, err := idx.initShard(ctx, shard.Name(), class, nil, true, true)
	require.NoError(t, err)
	defer reloaded.Shutdown(ctx)
	assert.Equal(t, tracked, dimensionsBucketRows(t, reloaded.(*Shard)), "writes after the failed switch must survive a restart")
}

// The operator is told to remove the flag once the reindex is complete. After a
// cancel or a failure that would leave the wrong dimensions in place for good, so
// then the caller gets an error to report, and nothing is called complete.
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
		expectedErr error
	}{
		{name: "complete", ctx: testCtx},
		{
			name: "cancelled",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(testCtx())
				cancel()
				return ctx
			},
			expectedErr: context.Canceled,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
			logger, hook := test.NewNullLogger()
			migrator := NewMigrator(db, logger, "node1")
			require.NoError(t, db.PutObject(testCtx(), &models.Object{Class: class.Class, ID: strfmt.UUID(uuid.NewString())},
				randVector(3), nil, nil, nil, 0))

			err := migrator.RecalculateVectorDimensions(tt.ctx())

			if tt.expectedErr != nil {
				require.ErrorIs(t, err, tt.expectedErr)
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
