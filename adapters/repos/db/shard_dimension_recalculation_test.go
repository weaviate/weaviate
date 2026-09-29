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
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
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
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	shardusage "github.com/weaviate/weaviate/adapters/repos/db/shard_usage"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/sharding"
)

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

func putRecalculationObjects(t *testing.T, ctx context.Context, shard *Shard, class *models.Class, objects int) {
	t.Helper()
	for range objects {
		obj := testObject(class.Class)
		obj.Vector = randVector(3)
		require.NoError(t, shard.PutObject(ctx, obj))
	}
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

// A replacement bucket left over by a failed run must not end up in the result.
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
	require.NoError(t, shard.PutObject(ctx, obj))
	tracked = dimensionsBucketRows(t, shard)
	require.Len(t, tracked["\x03\x00\x00\x00"], 11)

	require.NoError(t, shard.Shutdown(ctx))
	reloaded, err := idx.initShard(ctx, shard.Name(), class, nil, true, true)
	require.NoError(t, err)
	defer reloaded.Shutdown(ctx)
	assert.Equal(t, tracked, dimensionsBucketRows(t, reloaded.(*Shard)))
}

// A usage scan of the unloaded shard would take the replacement for a torn migration.
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

// A torn migration leaves a replacement and a moved-aside bucket next to the one in use.
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

// ReplaceBuckets takes the name before it can fail; the shard must still end up with
// a bucket it can write to and load again.
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
		name         string
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

// addLocalPhysicalForTest adds a local shard to the schema only.
func addLocalPhysicalForTest(t *testing.T, index *Index, name, status string) {
	require.NoError(t, index.schemaReader.Read(index.Config.ClassName.String(), true,
		func(_ *models.Class, state *sharding.State) error {
			for _, physical := range state.Physical {
				physical.Name = name
				physical.Status = status
				state.Physical[name] = physical
				break
			}
			require.True(t, state.IsLocalShard(name))
			return nil
		}))
}

func writeIndexCountForTest(t *testing.T, index *Index, shardName string, count uint64) {
	dir := shardPath(index.path(), shardName)
	require.NoError(t, os.MkdirAll(dir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "indexcount"), binary.LittleEndian.AppendUint64(nil, count), 0o600))
}

// A failed switch that cannot load the dimensions bucket again, or must not, leaves
// the shard without one. That fails the load, and the next load recovers the bucket.
func TestShard_RecalculateDimensions_FailedSwitchLeavesNoBucket(t *testing.T) {
	ctx := testCtx()

	tests := []struct {
		name string
		// repair undoes what would also fail the next load
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
			shard, idx, class := recalculationTestShard(t, ctx)
			putRecalculationObjects(t, ctx, shard, class, 5)
			tracked := dimensionsBucketRows(t, shard)
			bucketPath := filepath.Join(shard.pathLSM(), helpers.DimensionsBucketLSM)

			repair, switchErr := tt.fail(t, bucketPath)
			err := shard.recoverFailedDimensionsSwitch(ctx, bucketPath, switchErr)
			require.ErrorIs(t, err, switchErr)
			require.Nil(t, shard.store.Bucket(helpers.DimensionsBucketLSM))

			// as it happens while the shard loads, which must then fail
			idx.Config.DimensionsReindex = &dimensionsReindex{enabled: true}
			require.Error(t, shard.reindexDimensionsOnLoad(ctx))

			repair()
			require.NoError(t, shard.Shutdown(ctx))
			idx.Config.DimensionsReindex = nil
			reloaded, err := idx.initShard(ctx, shard.Name(), class, nil, true, true)
			require.NoError(t, err, "the shard must load again")
			defer reloaded.Shutdown(ctx)
			assert.Equal(t, tracked, dimensionsBucketRows(t, reloaded.(*Shard)), "the bucket in place must be recovered")
		})
	}
}

// With REINDEX_VECTOR_DIMENSIONS_AT_STARTUP a shard rebuilds its dimensions bucket
// while it loads, once per process.
func TestShard_ReindexDimensionsOnLoad(t *testing.T) {
	ctx := testCtx()
	bogus := "bogus\x05\x00\x00\x00"

	// stuckDir makes the removal of the dir it is put into fail
	stuckDir := func(t *testing.T, dir string) {
		stuck := filepath.Join(dir, "stuck")
		require.NoError(t, os.MkdirAll(stuck, 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(stuck, "file"), []byte("x"), 0o600))
		require.NoError(t, os.Chmod(stuck, 0o500))
		t.Cleanup(func() { _ = os.Chmod(stuck, 0o700) })
	}

	tests := []struct {
		name string
		// prepare runs with the shard shut down, before it loads with reindex
		prepare       func(t *testing.T, shard *Shard, reindex *dimensionsReindex)
		reindex       *dimensionsReindex
		expectRebuilt bool
		expectFailed  bool
	}{
		{name: "not enabled", reindex: nil},
		{name: "enabled", reindex: &dimensionsReindex{enabled: true}, expectRebuilt: true},
		{
			name:    "rebuilt by this process already",
			reindex: &dimensionsReindex{enabled: true},
			prepare: func(t *testing.T, shard *Shard, reindex *dimensionsReindex) {
				reindex.record(shard.ID(), 5, true)
			},
		},
		{
			name:    "failed in this process before",
			reindex: &dimensionsReindex{enabled: true},
			prepare: func(t *testing.T, shard *Shard, reindex *dimensionsReindex) {
				reindex.record(shard.ID(), 0, false)
			},
			expectRebuilt: true,
		},
		{
			// the shard serves with the dimensions it had
			name:    "failed, the bucket is kept",
			reindex: &dimensionsReindex{enabled: true},
			prepare: func(t *testing.T, shard *Shard, _ *dimensionsReindex) {
				stuckDir(t, filepath.Join(shard.pathLSM(), helpers.DimensionsBucketLSM+"__to_roaringset_ready"))
			},
			expectFailed: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shard, idx, class := recalculationTestShard(t, ctx)
			putRecalculationObjects(t, ctx, shard, class, 5)
			tracked := dimensionsBucketRows(t, shard)
			require.NoError(t, shard.store.Bucket(helpers.DimensionsBucketLSM).RoaringSetAddList([]byte(bogus), []uint64{1}))
			require.NoError(t, shard.Shutdown(ctx))

			if tt.prepare != nil {
				tt.prepare(t, shard, tt.reindex)
			}
			idx.Config.DimensionsReindex = tt.reindex
			reloaded, err := idx.initShard(ctx, shard.Name(), class, nil, true, true)
			require.NoError(t, err)
			defer reloaded.Shutdown(ctx)
			rows := dimensionsBucketRows(t, reloaded.(*Shard))

			if tt.expectRebuilt {
				assert.Equal(t, tracked, rows)
				assert.True(t, tt.reindex.done(shard.ID()))
			} else {
				assert.Contains(t, rows, bogus)
			}
			if tt.expectFailed {
				_, failed, _ := tt.reindex.summary()
				assert.Equal(t, 1, failed)
			}

			obj := testObject(class.Class)
			obj.Vector = randVector(3)
			require.NoError(t, reloaded.PutObject(ctx, obj))
			assert.Contains(t, dimensionsBucketRows(t, reloaded.(*Shard))["\x03\x00\x00\x00"], obj.DocID)
		})
	}
}

func TestShard_ReindexDimensionsOnLoad_MapBucket(t *testing.T) {
	ctx := testCtx()
	shard, idx, class, _ := dimsMigrationShard(t, ctx, 10)
	require.NoError(t, shard.Shutdown(ctx))

	idx.Config.DimensionsReindex = &dimensionsReindex{enabled: true}
	reloaded, err := idx.initShard(ctx, shard.Name(), class, nil, true, true)
	require.NoError(t, err)
	defer reloaded.Shutdown(ctx)

	rows := dimensionsBucketRows(t, reloaded.(*Shard))
	require.Len(t, rows, 1, "the row the map bucket was seeded with belongs to no object")
	assert.Len(t, rows["\x03\x00\x00\x00"], 10)
}

// Complete is reported only when nothing is left, as the operator removes the flag then.
func TestReportVectorDimensionsReindex(t *testing.T) {
	class := &models.Class{
		Class:               "Test",
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
	}
	const complete = "Reindexing dimensions complete"

	tests := []struct {
		name        string
		ctx         func() context.Context
		prepare     func(t *testing.T, db *DB, index *Index)
		expectedErr string
	}{
		{name: "complete"},
		{
			name: "startup not complete",
			prepare: func(t *testing.T, db *DB, _ *Index) {
				db.startupComplete.Store(false)
			},
			expectedErr: "has not completed startup",
		},
		{
			name: "shards still loading",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(testCtx())
				cancel()
				return ctx
			},
			prepare: func(t *testing.T, _ *DB, index *Index) {
				index.allShardsReady.Store(false)
			},
			expectedErr: "wait for shards to load",
		},
		{
			name: "failed shard",
			prepare: func(t *testing.T, db *DB, _ *Index) {
				db.dimensionsReindex.record("other", 0, false)
			},
			expectedErr: "1 failed",
		},
		{
			name: "inactive tenant",
			prepare: func(t *testing.T, _ *DB, index *Index) {
				addLocalPhysicalForTest(t, index, "cold", models.TenantActivityStatusCOLD)
				writeIndexCountForTest(t, index, "cold", 3)
			},
			expectedErr: "1 inactive tenants with objects",
		},
		{
			name: "inactive tenant never written to",
			prepare: func(t *testing.T, _ *DB, index *Index) {
				addLocalPhysicalForTest(t, index, "cold", models.TenantActivityStatusCOLD)
				writeIndexCountForTest(t, index, "cold", 0)
			},
		},
		{
			// its dir is removed at freeze, and reads as never written to
			name: "frozen tenant",
			prepare: func(t *testing.T, _ *DB, index *Index) {
				addLocalPhysicalForTest(t, index, "frozen", models.TenantActivityStatusFROZEN)
			},
			expectedErr: "1 inactive tenants with objects",
		},
		{
			// as while it is activated, or after loading it failed
			name: "active shard not loaded",
			prepare: func(t *testing.T, _ *DB, index *Index) {
				addLocalPhysicalForTest(t, index, "loading", models.TenantActivityStatusHOT)
				writeIndexCountForTest(t, index, "loading", 3)
			},
			expectedErr: "1 active shards not loaded",
		},
		{
			// as when creating it failed at startup, before its shards were reindexed
			name: "class without index",
			prepare: func(t *testing.T, db *DB, index *Index) {
				db.dimensionsReindex.mu.Lock()
				db.dimensionsReindex.outcomes = nil
				db.dimensionsReindex.mu.Unlock()
				db.indexLock.Lock()
				delete(db.indices, index.ID())
				db.indexLock.Unlock()
				t.Cleanup(func() {
					db.indexLock.Lock()
					db.indices[index.ID()] = index
					db.indexLock.Unlock()
				})
			},
			expectedErr: "1 active shards not loaded",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
			db.dimensionsReindex.enabled = true
			logger, hook := test.NewNullLogger()
			migrator := NewMigrator(db, logger, "node1")
			index := db.GetIndex(schema.ClassName(class.Class))
			require.NoError(t, db.PutObject(testCtx(), &models.Object{Class: class.Class, ID: strfmt.UUID(uuid.NewString())},
				randVector(3), nil, nil, nil, 0))
			rebuilt, _, _ := db.dimensionsReindex.summary()
			require.Equal(t, 1, rebuilt, "the shard is reindexed as it loads")
			if tt.prepare != nil {
				tt.prepare(t, db, index)
			}
			ctx := testCtx()
			if tt.ctx != nil {
				ctx = tt.ctx()
			}

			err := migrator.ReportVectorDimensionsReindex(ctx)

			if tt.expectedErr != "" {
				require.ErrorContains(t, err, tt.expectedErr)
				for _, entry := range hook.AllEntries() {
					assert.NotContains(t, entry.Message, complete)
				}
				return
			}
			require.NoError(t, err)
			last := hook.LastEntry()
			require.NotNil(t, last)
			assert.Contains(t, last.Message, complete)
		})
	}
}
