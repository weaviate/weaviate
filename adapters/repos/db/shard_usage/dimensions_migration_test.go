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

package shardusage

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/usage/types"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

const migrationTestShard = "tenant"

// dimsOp adds the doc ids to a dimensions row, or removes them from it.
type dimsOp struct {
	targetVector string
	dims         uint32
	docIDs       []uint64
	remove       bool
}

func dimsKey(targetVector string, dims uint32) string {
	key := make([]byte, len(targetVector)+4)
	copy(key, targetVector)
	binary.LittleEndian.PutUint32(key[len(targetVector):], dims)
	return string(key)
}

// seedDimensionsBucket writes one segment per entry of segments.
func seedDimensionsBucket(t *testing.T, logger logrus.FieldLogger, indexPath, strategy string, segments [][]dimsOp) {
	t.Helper()
	ctx := context.Background()

	b, err := lsmkv.NewBucketCreator().NewBucket(ctx, shardPathDimensionsLSM(indexPath, migrationTestShard), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		lsmkv.WithStrategy(strategy))
	require.NoError(t, err)

	for _, segment := range segments {
		for _, op := range segment {
			key := []byte(dimsKey(op.targetVector, op.dims))
			for _, docID := range op.docIDs {
				if strategy == lsmkv.StrategyRoaringSet {
					if op.remove {
						require.NoError(t, b.RoaringSetRemoveOne(key, docID))
					} else {
						require.NoError(t, b.RoaringSetAddOne(key, docID))
					}
					continue
				}
				docIDBytes := make([]byte, 8)
				binary.LittleEndian.PutUint64(docIDBytes, docID)
				require.NoError(t, b.MapSet(key, lsmkv.MapPair{Key: docIDBytes, Value: []byte{}, Tombstone: op.remove}))
			}
		}
		require.NoError(t, b.FlushAndSwitch())
	}
	require.NoError(t, b.Shutdown(ctx))
}

// readRoaringSetDimensions fails unless the bucket is a roaring set one.
func readRoaringSetDimensions(t *testing.T, logger logrus.FieldLogger, indexPath string) map[string][]uint64 {
	t.Helper()
	ctx := context.Background()
	bucketPath := shardPathDimensionsLSM(indexPath, migrationTestShard)

	strategy, err := lsmkv.DetermineUnloadedBucketStrategyAmong(bucketPath, lsmkv.DimensionsBucketPrioritizedStrategies)
	require.NoError(t, err)
	require.Equal(t, lsmkv.StrategyRoaringSet, strategy)

	b, err := openUnloadedDimensionsBucket(ctx, logger, indexPath, bucketPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, b.Shutdown(ctx)) }()

	rows := map[string][]uint64{}
	c := b.CursorRoaringSet()
	defer c.Close()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		rows[string(k)] = v.ToArray()
	}
	return rows
}

func requireNoMigrationLeftovers(t *testing.T, indexPath string) {
	t.Helper()
	bucketPath := shardPathDimensionsLSM(indexPath, migrationTestShard)
	for _, suffix := range []string{dimensionsMigrationBuildSuffix, dimensionsMigrationReadySuffix, dimensionsMigrationDelSuffix} {
		require.NoDirExists(t, bucketPath+suffix)
	}
}

func TestMigrateDimensionsBucketToRoaringSet(t *testing.T) {
	logger, _ := test.NewNullLogger()
	ctx := context.Background()

	tests := []struct {
		name     string
		strategy string
		segments [][]dimsOp
		migrated bool
		expected map[string][]uint64
	}{
		{
			name:     "legacy vector",
			strategy: lsmkv.StrategyMapCollection,
			segments: [][]dimsOp{{{targetVector: "", dims: 128, docIDs: []uint64{1, 2, 3}}}},
			migrated: true,
			expected: map[string][]uint64{dimsKey("", 128): {1, 2, 3}},
		},
		{
			name:     "named vectors, one a prefix of the other",
			strategy: lsmkv.StrategyMapCollection,
			segments: [][]dimsOp{{
				{targetVector: "text", dims: 384, docIDs: []uint64{1, 2}},
				{targetVector: "texts", dims: 64, docIDs: []uint64{2, 3, 4}},
			}},
			migrated: true,
			expected: map[string][]uint64{
				dimsKey("text", 384): {1, 2},
				dimsKey("texts", 64): {2, 3, 4},
			},
		},
		{
			name:     "several dimensions of one vector, as multi vectors have",
			strategy: lsmkv.StrategyMapCollection,
			segments: [][]dimsOp{{
				{targetVector: "colbert", dims: 96, docIDs: []uint64{1}},
				{targetVector: "colbert", dims: 192, docIDs: []uint64{2, 3}},
			}},
			migrated: true,
			expected: map[string][]uint64{
				dimsKey("colbert", 96):  {1},
				dimsKey("colbert", 192): {2, 3},
			},
		},
		{
			name:     "doc ids beyond 32 bits",
			strategy: lsmkv.StrategyMapCollection,
			segments: [][]dimsOp{{{targetVector: "text", dims: 8, docIDs: []uint64{7, 1 << 40}}}},
			migrated: true,
			expected: map[string][]uint64{dimsKey("text", 8): {7, 1 << 40}},
		},
		{
			name:     "doc ids removed in a later segment",
			strategy: lsmkv.StrategyMapCollection,
			segments: [][]dimsOp{
				{{targetVector: "text", dims: 128, docIDs: []uint64{1, 2, 3, 4}}},
				{{targetVector: "text", dims: 128, docIDs: []uint64{2, 4}, remove: true}},
				{{targetVector: "text", dims: 128, docIDs: []uint64{5}}},
			},
			migrated: true,
			expected: map[string][]uint64{dimsKey("text", 128): {1, 3, 5}},
		},
		{
			name:     "doc id removed and added again",
			strategy: lsmkv.StrategyMapCollection,
			segments: [][]dimsOp{
				{{targetVector: "text", dims: 128, docIDs: []uint64{1}}},
				{{targetVector: "text", dims: 128, docIDs: []uint64{1}, remove: true}},
				{{targetVector: "text", dims: 128, docIDs: []uint64{1}}},
			},
			migrated: true,
			expected: map[string][]uint64{dimsKey("text", 128): {1}},
		},
		{
			name:     "row with every doc id removed",
			strategy: lsmkv.StrategyMapCollection,
			segments: [][]dimsOp{
				{
					{targetVector: "gone", dims: 16, docIDs: []uint64{1, 2}},
					{targetVector: "kept", dims: 16, docIDs: []uint64{1, 2}},
				},
				{{targetVector: "gone", dims: 16, docIDs: []uint64{1, 2}, remove: true}},
			},
			migrated: true,
			expected: map[string][]uint64{dimsKey("kept", 16): {1, 2}},
		},
		{
			name:     "already a roaring set",
			strategy: lsmkv.StrategyRoaringSet,
			segments: [][]dimsOp{{{targetVector: "text", dims: 128, docIDs: []uint64{1, 2}}}},
			migrated: false,
			expected: map[string][]uint64{dimsKey("text", 128): {1, 2}},
		},
		{
			name:     "bucket never written to",
			strategy: lsmkv.StrategyMapCollection,
			segments: nil,
			migrated: false,
			expected: map[string][]uint64{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			indexPath := t.TempDir()
			seedDimensionsBucket(t, logger, indexPath, tt.strategy, tt.segments)

			require.NoError(t, RecoverDimensionsBucketMigration(logger, indexPath, migrationTestShard))
			migrated, err := MigrateDimensionsBucketToRoaringSet(ctx, logger, indexPath, migrationTestShard)
			require.NoError(t, err)
			assert.Equal(t, tt.migrated, migrated)
			assert.Equal(t, tt.expected, readRoaringSetDimensions(t, logger, indexPath))
			requireNoMigrationLeftovers(t, indexPath)

			again, err := MigrateDimensionsBucketToRoaringSet(ctx, logger, indexPath, migrationTestShard)
			require.NoError(t, err)
			assert.False(t, again, "a migrated bucket must not be migrated again")
			assert.Equal(t, tt.expected, readRoaringSetDimensions(t, logger, indexPath))
		})
	}

	t.Run("missing bucket dir is not created", func(t *testing.T) {
		indexPath := t.TempDir()
		migrated, err := MigrateDimensionsBucketToRoaringSet(ctx, logger, indexPath, migrationTestShard)
		require.NoError(t, err)
		assert.False(t, migrated)
		require.NoDirExists(t, shardPathDimensionsLSM(indexPath, migrationTestShard))
	})
}

// The usage numbers of a shard must not change with the strategy of its bucket.
func TestMigrateDimensionsBucketToRoaringSet_UsageUnchanged(t *testing.T) {
	logger, _ := test.NewNullLogger()
	ctx := context.Background()
	indexPath := t.TempDir()

	seedDimensionsBucket(t, logger, indexPath, lsmkv.StrategyMapCollection, [][]dimsOp{
		{
			{targetVector: "", dims: 128, docIDs: []uint64{1, 2, 3, 4, 5}},
			{targetVector: "text", dims: 384, docIDs: []uint64{1, 2, 3}},
			{targetVector: "texts", dims: 64, docIDs: []uint64{4, 5}},
		},
		{{targetVector: "text", dims: 384, docIDs: []uint64{2}, remove: true}},
	})
	targetVectors := map[string]int{"": 0, "text": 0, "texts": 2560, "missing": 0}

	before, err := CalculateUnloadedDimensionsUsageAll(ctx, logger, indexPath, migrationTestShard, targetVectors)
	require.NoError(t, err)
	require.Equal(t, types.Dimensionality{Dimensions: 384, Count: 2}, before["text"].Raw)

	migrated, err := MigrateDimensionsBucketToRoaringSet(ctx, logger, indexPath, migrationTestShard)
	require.NoError(t, err)
	require.True(t, migrated)

	after, err := CalculateUnloadedDimensionsUsageAll(ctx, logger, indexPath, migrationTestShard, targetVectors)
	require.NoError(t, err)
	assert.Equal(t, before, after)
}

func TestMigrateDimensionsBucketToRoaringSet_CancelledKeepsMapBucket(t *testing.T) {
	logger, _ := test.NewNullLogger()
	indexPath := t.TempDir()
	bucketPath := shardPathDimensionsLSM(indexPath, migrationTestShard)
	seedDimensionsBucket(t, logger, indexPath, lsmkv.StrategyMapCollection,
		[][]dimsOp{{{targetVector: "text", dims: 128, docIDs: []uint64{1, 2}}}})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	migrated, err := MigrateDimensionsBucketToRoaringSet(ctx, logger, indexPath, migrationTestShard)
	require.Error(t, err)
	assert.False(t, migrated)
	requireNoMigrationLeftovers(t, indexPath)

	strategy, err := lsmkv.DetermineUnloadedBucketStrategyAmong(bucketPath, lsmkv.DimensionsBucketPrioritizedStrategies)
	require.NoError(t, err)
	assert.Equal(t, lsmkv.StrategyMapCollection, strategy)

	migrated, err = MigrateDimensionsBucketToRoaringSet(context.Background(), logger, indexPath, migrationTestShard)
	require.NoError(t, err)
	assert.True(t, migrated)
	assert.Equal(t, map[string][]uint64{dimsKey("text", 128): {1, 2}}, readRoaringSetDimensions(t, logger, indexPath))
}

// Every state a crash can leave on disk, in the order the migration goes through them.
func TestRecoverDimensionsBucketMigration(t *testing.T) {
	ctx := context.Background()
	seed := [][]dimsOp{{{targetVector: "text", dims: 128, docIDs: []uint64{1, 2, 3}}}}
	expected := map[string][]uint64{dimsKey("text", 128): {1, 2, 3}}

	// buildReady leaves the map bucket in place and a complete roaring set bucket next to it
	buildReady := func(t *testing.T, logger logrus.FieldLogger, indexPath string) {
		bucketPath := shardPathDimensionsLSM(indexPath, migrationTestShard)
		_, err := buildRoaringSetDimensionsBucket(ctx, logger, shardPathLSM(indexPath, migrationTestShard),
			bucketPath, bucketPath+dimensionsMigrationBuildSuffix)
		require.NoError(t, err)
		require.NoError(t, os.Rename(bucketPath+dimensionsMigrationBuildSuffix, bucketPath+dimensionsMigrationReadySuffix))
	}
	moveMapAside := func(t *testing.T, indexPath string) {
		bucketPath := shardPathDimensionsLSM(indexPath, migrationTestShard)
		require.NoError(t, os.Rename(bucketPath, bucketPath+dimensionsMigrationDelSuffix))
	}

	tests := []struct {
		name  string
		crash func(t *testing.T, logger logrus.FieldLogger, indexPath string)
		// the map bucket is still the one in place, to be migrated by a next run
		mapBucketKept bool
	}{
		{
			name:          "nothing in flight",
			crash:         func(t *testing.T, logger logrus.FieldLogger, indexPath string) {},
			mapBucketKept: true,
		},
		{
			name: "while building",
			crash: func(t *testing.T, logger logrus.FieldLogger, indexPath string) {
				buildPath := shardPathDimensionsLSM(indexPath, migrationTestShard) + dimensionsMigrationBuildSuffix
				require.NoError(t, os.MkdirAll(buildPath, 0o700))
				require.NoError(t, os.WriteFile(buildPath+"/segment-1.db", []byte("incomplete"), 0o600))
			},
			mapBucketKept: true,
		},
		{
			name:          "built, map bucket not moved aside",
			crash:         buildReady,
			mapBucketKept: true,
		},
		{
			name: "map bucket moved aside, roaring set bucket not in place",
			crash: func(t *testing.T, logger logrus.FieldLogger, indexPath string) {
				buildReady(t, logger, indexPath)
				moveMapAside(t, indexPath)
			},
		},
		{
			name: "map bucket moved aside, missing bucket opened meanwhile",
			crash: func(t *testing.T, logger logrus.FieldLogger, indexPath string) {
				buildReady(t, logger, indexPath)
				moveMapAside(t, indexPath)
				b, err := openUnloadedDimensionsBucket(ctx, logger, indexPath, shardPathDimensionsLSM(indexPath, migrationTestShard))
				require.NoError(t, err)
				require.NoError(t, b.Shutdown(ctx))
				require.DirExists(t, shardPathDimensionsLSM(indexPath, migrationTestShard))
			},
		},
		{
			name: "map bucket moved aside, roaring set bucket lost",
			crash: func(t *testing.T, logger logrus.FieldLogger, indexPath string) {
				moveMapAside(t, indexPath)
			},
			mapBucketKept: true,
		},
		{
			name: "switched, map bucket not removed",
			crash: func(t *testing.T, logger logrus.FieldLogger, indexPath string) {
				buildReady(t, logger, indexPath)
				moveMapAside(t, indexPath)
				bucketPath := shardPathDimensionsLSM(indexPath, migrationTestShard)
				require.NoError(t, os.Rename(bucketPath+dimensionsMigrationReadySuffix, bucketPath))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			indexPath := t.TempDir()
			bucketPath := shardPathDimensionsLSM(indexPath, migrationTestShard)
			seedDimensionsBucket(t, logger, indexPath, lsmkv.StrategyMapCollection, seed)
			tt.crash(t, logger, indexPath)

			for range 2 {
				require.NoError(t, RecoverDimensionsBucketMigration(logger, indexPath, migrationTestShard))
				requireNoMigrationLeftovers(t, indexPath)
			}

			if tt.mapBucketKept {
				strategy, err := lsmkv.DetermineUnloadedBucketStrategyAmong(bucketPath, lsmkv.DimensionsBucketPrioritizedStrategies)
				require.NoError(t, err)
				require.Equal(t, lsmkv.StrategyMapCollection, strategy)

				migrated, err := MigrateDimensionsBucketToRoaringSet(ctx, logger, indexPath, migrationTestShard)
				require.NoError(t, err)
				require.True(t, migrated)
			}
			assert.Equal(t, expected, readRoaringSetDimensions(t, logger, indexPath))
			requireNoMigrationLeftovers(t, indexPath)
		})
	}

	t.Run("bucket written to between the renames is reported and nothing removed", func(t *testing.T) {
		logger, hook := test.NewNullLogger()
		indexPath := t.TempDir()
		bucketPath := shardPathDimensionsLSM(indexPath, migrationTestShard)
		seedDimensionsBucket(t, logger, indexPath, lsmkv.StrategyMapCollection, seed)
		buildReady(t, logger, indexPath)
		moveMapAside(t, indexPath)
		seedDimensionsBucket(t, logger, indexPath, lsmkv.StrategyRoaringSet,
			[][]dimsOp{{{targetVector: "text", dims: 128, docIDs: []uint64{9}}}})

		require.NoError(t, RecoverDimensionsBucketMigration(logger, indexPath, migrationTestShard))

		require.DirExists(t, bucketPath+dimensionsMigrationReadySuffix)
		require.DirExists(t, bucketPath+dimensionsMigrationDelSuffix)
		assert.Equal(t, map[string][]uint64{dimsKey("text", 128): {9}}, readRoaringSetDimensions(t, logger, indexPath))
		require.NotNil(t, hook.LastEntry())
		assert.Equal(t, logrus.ErrorLevel, hook.LastEntry().Level)
		assert.Contains(t, hook.LastEntry().Message, "torn state")
	})
}

// A usage scan of an unloaded shard opens the bucket by itself, and would create
// an empty one and report no dimensions if it found the dir missing.
func TestCalculateUnloadedDimensionsUsage_RecoversInterruptedMigration(t *testing.T) {
	logger, _ := test.NewNullLogger()
	ctx := context.Background()
	expected := types.Dimensionality{Dimensions: 128, Count: 3}

	interrupt := func(t *testing.T) string {
		indexPath := t.TempDir()
		bucketPath := shardPathDimensionsLSM(indexPath, migrationTestShard)
		seedDimensionsBucket(t, logger, indexPath, lsmkv.StrategyMapCollection,
			[][]dimsOp{{{targetVector: "text", dims: 128, docIDs: []uint64{1, 2, 3}}}})
		_, err := buildRoaringSetDimensionsBucket(ctx, logger, shardPathLSM(indexPath, migrationTestShard),
			bucketPath, bucketPath+dimensionsMigrationBuildSuffix)
		require.NoError(t, err)
		require.NoError(t, os.Rename(bucketPath+dimensionsMigrationBuildSuffix, bucketPath+dimensionsMigrationReadySuffix))
		require.NoError(t, os.Rename(bucketPath, bucketPath+dimensionsMigrationDelSuffix))
		return indexPath
	}

	t.Run("single target vector", func(t *testing.T) {
		indexPath := interrupt(t)
		dimensionality, err := CalculateUnloadedDimensionsUsage(ctx, logger, indexPath, migrationTestShard, "text")
		require.NoError(t, err)
		assert.Equal(t, expected, dimensionality)
		requireNoMigrationLeftovers(t, indexPath)
	})

	t.Run("all target vectors", func(t *testing.T) {
		indexPath := interrupt(t)
		scans, err := CalculateUnloadedDimensionsUsageAll(ctx, logger, indexPath, migrationTestShard, map[string]int{"text": 0})
		require.NoError(t, err)
		assert.Equal(t, expected, scans["text"].Raw)
		requireNoMigrationLeftovers(t, indexPath)
	})
}

// A small bucket can be shut down with its data in the commit log only, without any segment.
func TestMigrateDimensionsBucketToRoaringSet_CommitLogOnly(t *testing.T) {
	logger, _ := test.NewNullLogger()
	ctx := context.Background()
	indexPath := t.TempDir()
	bucketPath := shardPathDimensionsLSM(indexPath, migrationTestShard)

	b, err := lsmkv.NewBucketCreator().NewBucket(ctx, bucketPath, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		lsmkv.WithStrategy(lsmkv.StrategyMapCollection), lsmkv.WithMinWalThreshold(1024*1024))
	require.NoError(t, err)
	writeDims(t, b, "text", 128, []uint64{1, 2, 3})
	require.NoError(t, b.Shutdown(ctx))

	entries, err := os.ReadDir(bucketPath)
	require.NoError(t, err)
	for _, entry := range entries {
		require.NotEqual(t, ".db", filepath.Ext(entry.Name()), "the seed must not have written a segment")
	}

	migrated, err := MigrateDimensionsBucketToRoaringSet(ctx, logger, indexPath, migrationTestShard)
	require.NoError(t, err)
	assert.True(t, migrated)
	assert.Equal(t, map[string][]uint64{dimsKey("text", 128): {1, 2, 3}}, readRoaringSetDimensions(t, logger, indexPath))
	requireNoMigrationLeftovers(t, indexPath)
}

func TestPrepareDimensionsBucket(t *testing.T) {
	ctx := context.Background()
	seed := [][]dimsOp{{{targetVector: "text", dims: 128, docIDs: []uint64{1, 2, 3}}}}

	t.Run("migration off leaves the map bucket alone", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		indexPath := t.TempDir()
		seedDimensionsBucket(t, logger, indexPath, lsmkv.StrategyMapCollection, seed)

		require.NoError(t, PrepareDimensionsBucket(ctx, logger, indexPath, migrationTestShard, false))

		strategy, err := lsmkv.DetermineUnloadedBucketStrategyAmong(
			shardPathDimensionsLSM(indexPath, migrationTestShard), lsmkv.DimensionsBucketPrioritizedStrategies)
		require.NoError(t, err)
		assert.Equal(t, lsmkv.StrategyMapCollection, strategy)
	})

	t.Run("migration on", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		indexPath := t.TempDir()
		seedDimensionsBucket(t, logger, indexPath, lsmkv.StrategyMapCollection, seed)

		require.NoError(t, PrepareDimensionsBucket(ctx, logger, indexPath, migrationTestShard, true))

		assert.Equal(t, map[string][]uint64{dimsKey("text", 128): {1, 2, 3}}, readRoaringSetDimensions(t, logger, indexPath))
	})

	t.Run("failed migration is logged and leaves the map bucket alone", func(t *testing.T) {
		logger, hook := test.NewNullLogger()
		indexPath := t.TempDir()
		bucketPath := shardPathDimensionsLSM(indexPath, migrationTestShard)
		seedDimensionsBucket(t, logger, indexPath, lsmkv.StrategyMapCollection, seed)
		before := dirListing(t, bucketPath)

		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		require.NoError(t, PrepareDimensionsBucket(cancelled, logger, indexPath, migrationTestShard, true))

		assert.Equal(t, before, dirListing(t, bucketPath))
		requireNoMigrationLeftovers(t, indexPath)
		require.NotNil(t, hook.LastEntry())
		assert.Equal(t, logrus.ErrorLevel, hook.LastEntry().Level)
		assert.Contains(t, hook.LastEntry().Message, "migrate dimensions bucket to roaring set")
	})
}

// A dir recovery cannot remove stays next to the map bucket. Migrating over it
// would fail at the switch, after a full copy, on every load of the shard, and
// with the replaced one left the switch would report a torn state that is not.
func TestPrepareDimensionsBucket_LeftoverCannotBeRemoved(t *testing.T) {
	ctx := context.Background()
	seed := [][]dimsOp{{{targetVector: "text", dims: 128, docIDs: []uint64{1, 2, 3}}}}

	for _, suffix := range []string{dimensionsMigrationReadySuffix, dimensionsMigrationDelSuffix} {
		t.Run(suffix, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			indexPath := t.TempDir()
			bucketPath := shardPathDimensionsLSM(indexPath, migrationTestShard)
			seedDimensionsBucket(t, logger, indexPath, lsmkv.StrategyMapCollection, seed)
			stuckDirForTest(t, bucketPath+suffix)
			before := dirListing(t, bucketPath)

			for range 2 {
				require.NoError(t, PrepareDimensionsBucket(ctx, logger, indexPath, migrationTestShard, true), "a load of the shard")
			}

			assert.Equal(t, before, dirListing(t, bucketPath))
			require.NoDirExists(t, bucketPath+dimensionsMigrationBuildSuffix)
			refused := 0
			for _, entry := range hook.AllEntries() {
				assert.NotContains(t, entry.Message, "torn state")
				assert.NotContains(t, entry.Message, "move roaring set dimensions bucket in place")
				if entry.Level == logrus.ErrorLevel && strings.Contains(entry.Message, "is still there") {
					refused++
				}
			}
			assert.Equal(t, 2, refused, "one refused migration per load")
		})
	}
}

// stuckDirForTest creates dir with a dir in it that makes its removal fail.
func stuckDirForTest(t *testing.T, dir string) {
	t.Helper()
	stuck := filepath.Join(dir, "stuck")
	require.NoError(t, os.MkdirAll(stuck, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(stuck, "file"), []byte("x"), 0o600))
	require.NoError(t, os.Chmod(stuck, 0o500))
	t.Cleanup(func() { _ = os.Chmod(stuck, 0o700) })
}

// A replacement left next to the bucket in place, which recovery cannot remove, is
// a leftover dir only and must not keep the shard from loading.
func TestRecoverDimensionsBucketMigration_UnusedBucketCannotBeRemoved(t *testing.T) {
	logger, hook := test.NewNullLogger()
	indexPath := t.TempDir()
	bucketPath := shardPathDimensionsLSM(indexPath, migrationTestShard)
	seedDimensionsBucket(t, logger, indexPath, lsmkv.StrategyRoaringSet,
		[][]dimsOp{{{targetVector: "text", dims: 128, docIDs: []uint64{1, 2, 3}}}})
	stuckDirForTest(t, bucketPath+dimensionsMigrationReadySuffix)

	require.NoError(t, RecoverDimensionsBucketMigration(logger, indexPath, migrationTestShard))

	require.DirExists(t, bucketPath+dimensionsMigrationReadySuffix)
	assert.Equal(t, map[string][]uint64{dimsKey("text", 128): {1, 2, 3}}, readRoaringSetDimensions(t, logger, indexPath))
	require.NotNil(t, hook.LastEntry())
	assert.Equal(t, logrus.WarnLevel, hook.LastEntry().Level)
}

func dirListing(t *testing.T, path string) map[string]int64 {
	t.Helper()
	entries, err := os.ReadDir(path)
	require.NoError(t, err)
	listing := make(map[string]int64, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		require.NoError(t, err)
		listing[entry.Name()] = info.Size()
	}
	return listing
}

// A switch that went through leaves the bucket moved aside to remove. A crash in
// the middle of that removal leaves part of it, and the bucket in place is the one
// to keep even when it holds nothing: the part left can miss the segments that
// deleted doc ids, and would bring them back.
func TestRecoverDimensionsBucketMigration_SwitchedToEmptyBucket(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	indexPath := t.TempDir()
	bucketPath := shardPathDimensionsLSM(indexPath, migrationTestShard)
	seedDimensionsBucket(t, logger, indexPath, lsmkv.StrategyMapCollection, [][]dimsOp{
		{{targetVector: "text", dims: 128, docIDs: []uint64{1, 2}}},
		{{targetVector: "text", dims: 128, docIDs: []uint64{1, 2}, remove: true}},
	})

	_, err := buildRoaringSetDimensionsBucket(ctx, logger, shardPathLSM(indexPath, migrationTestShard),
		bucketPath, bucketPath+dimensionsMigrationBuildSuffix)
	require.NoError(t, err)
	require.NoError(t, os.Rename(bucketPath+dimensionsMigrationBuildSuffix, bucketPath+dimensionsMigrationReadySuffix))
	require.NoError(t, os.Rename(bucketPath, bucketPath+dimensionsMigrationDelSuffix))
	require.NoError(t, os.Rename(bucketPath+dimensionsMigrationReadySuffix, bucketPath))

	// the removal got as far as the newest segment, the one with the deletes
	segments, err := filepath.Glob(filepath.Join(bucketPath+dimensionsMigrationDelSuffix, "*.db"))
	require.NoError(t, err)
	require.Len(t, segments, 2)
	require.NoError(t, os.Remove(segments[1]))

	for range 2 {
		require.NoError(t, RecoverDimensionsBucketMigration(logger, indexPath, migrationTestShard))
		requireNoMigrationLeftovers(t, indexPath)
	}
	assert.Equal(t, map[string][]uint64{}, readRoaringSetDimensions(t, logger, indexPath))
}

// Removing the bucket moved aside is only cleanup: the switch is done, and failing
// it, or the next load over it, would take the shard down for a leftover dir.
func TestMigrateDimensionsBucketToRoaringSet_RemovalOfOldBucketFails(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	indexPath := t.TempDir()
	bucketPath := shardPathDimensionsLSM(indexPath, migrationTestShard)
	seedDimensionsBucket(t, logger, indexPath, lsmkv.StrategyMapCollection,
		[][]dimsOp{{{targetVector: "text", dims: 128, docIDs: []uint64{1, 2, 3}}}})

	stuck := filepath.Join(bucketPath, "stuck")
	require.NoError(t, os.Mkdir(stuck, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(stuck, "file"), []byte("x"), 0o600))
	require.NoError(t, os.Chmod(stuck, 0o500))
	t.Cleanup(func() {
		_ = os.Chmod(stuck, 0o700)
		_ = os.Chmod(filepath.Join(bucketPath+dimensionsMigrationDelSuffix, "stuck"), 0o700)
	})

	migrated, err := MigrateDimensionsBucketToRoaringSet(ctx, logger, indexPath, migrationTestShard)
	require.NoError(t, err, "a migration that went through must not be reported as failed")
	require.True(t, migrated)
	require.DirExists(t, bucketPath+dimensionsMigrationDelSuffix)
	expected := map[string][]uint64{dimsKey("text", 128): {1, 2, 3}}
	assert.Equal(t, expected, readRoaringSetDimensions(t, logger, indexPath))

	require.NoError(t, PrepareDimensionsBucket(ctx, logger, indexPath, migrationTestShard, true), "the next load")
	assert.Equal(t, expected, readRoaringSetDimensions(t, logger, indexPath))
}
