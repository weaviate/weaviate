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
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/usage/types"
)

func TestCalculateSegmentHeap(t *testing.T) {
	const minMMapSize = 1000

	type buckets map[string]map[string]int

	calculate := func(t *testing.T, fixture buckets, minMMapSize int64) types.SegmentHeap {
		t.Helper()
		lsmPath := filepath.Join(t.TempDir(), "lsm")
		directories := make([]string, 0, len(fixture))
		for bucket, files := range fixture {
			directories = append(directories, bucket)
			for name, size := range files {
				writeSizedFile(t, filepath.Join(lsmPath, bucket, name), size)
			}
		}
		got, err := CalculateSegmentHeap(lsmPath, directories, minMMapSize)
		require.NoError(t, err)
		return got
	}

	type fixtureCase struct {
		name      string
		buckets   buckets
		wantBloom int64
		wantSmall int64
	}
	run := func(t *testing.T, cases []fixtureCase) {
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				got := calculate(t, tc.buckets, minMMapSize)
				assert.Equal(t, types.SegmentHeap{
					BloomBytes:        tc.wantBloom,
					SmallSegmentBytes: tc.wantSmall,
					MinMMapSize:       minMMapSize,
				}, got)
			})
		}
	}

	t.Run("small segment counts its file and not its sidecars", func(t *testing.T) {
		run(t, []fixtureCase{{
			name: "plain names",
			buckets: buckets{"objects": {
				"segment-1.db":                600,
				"segment-1.bloom":             50,
				"segment-1.secondary.0.bloom": 40,
				"segment-1.metadata":          30,
				"segment-1.cna":               12,
			}},
			wantSmall: 600,
		}, {
			name: "exactly MinMMapSize bytes, level and strategy names",
			buckets: buckets{"property_title": {
				"segment-2.l0.s4.db":                minMMapSize,
				"segment-2.l0.s4.bloom":             50,
				"segment-2.l0.s4.secondary.0.bloom": 40,
				"segment-2.l0.s4.metadata":          30,
			}},
			wantSmall: minMMapSize,
		}})
	})

	t.Run("large segment counts its bloom and metadata sidecars", func(t *testing.T) {
		run(t, []fixtureCase{{
			name: "plain names",
			buckets: buckets{"objects": {
				"segment-1.db":                minMMapSize + 1,
				"segment-1.bloom":             50,
				"segment-1.secondary.0.bloom": 40,
				"segment-1.secondary.1.bloom": 35,
				"segment-1.metadata":          30,
				"segment-1.cna":               12,
				"segment-3.wal":               777,
			}},
			wantBloom: 50 + 40 + 35 + 30,
		}, {
			name: "beside a small segment in one bucket, level and strategy names",
			buckets: buckets{"objects": {
				"segment-1.l1.s4.db":                5000,
				"segment-1.l1.s4.bloom":             50,
				"segment-1.l1.s4.secondary.0.bloom": 40,
				"segment-1.l1.s4.metadata":          30,
				"segment-2.l0.s4.db":                400,
				"segment-2.l0.s4.bloom":             61,
				"segment-2.l0.s4.secondary.0.bloom": 7,
				"segment-2.l0.s4.metadata":          9,
			}},
			wantBloom: 50 + 40 + 30,
			wantSmall: 400,
		}, {
			name: "segment ids that are string prefixes of one another",
			buckets: buckets{"objects": {
				"segment-1.db":        400,
				"segment-1.bloom":     61,
				"segment-1.metadata":  9,
				"segment-10.db":       5000,
				"segment-10.bloom":    50,
				"segment-10.metadata": 30,
				"segment-2.db":        6000,
				"segment-2.bloom":     23,
				"segment-2.metadata":  4,
				"segment-20.db":       300,
				"segment-20.bloom":    17,
				"segment-20.metadata": 3,
			}},
			wantBloom: 50 + 30 + 23 + 4,
			wantSmall: 400 + 300,
		}, {
			name: "two buckets sum, and stray files add nothing",
			buckets: buckets{
				"objects": {
					"segment-1.db":          5000,
					"segment-1.bloom":       50,
					"segment-1.metadata":    30,
					"segment-2.db":          300,
					"segment-4.db.tmp":      900,
					"segment-4.bloom.tmp":   70,
					"segment-4.wal":         800,
					"segment-8.db.deleteme": 500,
				},
				"property_title": {
					"segment-5.l2.s4.db":       8000,
					"segment-5.l2.s4.bloom":    20,
					"segment-5.l2.s4.metadata": 10,
					"segment-5.l2.s4.cna":      12,
					"segment-6.l0.s4.db":       200,
					"segment-7.l0.s4.bloom":    90,
				},
			},
			wantBloom: 50 + 30 + 20 + 10,
			wantSmall: 300 + 200,
		}})
	})

	t.Run("result records the MinMMapSize used", func(t *testing.T) {
		fixture := buckets{"objects": {"segment-1.db": 2000, "segment-1.bloom": 50}}

		assert.Equal(t, types.SegmentHeap{BloomBytes: 50, MinMMapSize: 1000},
			calculate(t, fixture, 1000))
		assert.Equal(t, types.SegmentHeap{SmallSegmentBytes: 2000, MinMMapSize: 4096},
			calculate(t, fixture, 4096))
	})

	t.Run("missing bucket dir gives zero", func(t *testing.T) {
		lsmPath := filepath.Join(t.TempDir(), "lsm")
		writeSizedFile(t, filepath.Join(lsmPath, "objects", "segment-1.db"), 300)

		got, err := CalculateSegmentHeap(lsmPath, []string{"gone", "objects"}, minMMapSize)
		require.NoError(t, err)
		assert.Equal(t, types.SegmentHeap{SmallSegmentBytes: 300, MinMMapSize: minMMapSize}, got)
	})

	t.Run("unreadable dir returns an error", func(t *testing.T) {
		// A regular file in place of the dir opens but cannot be listed. This
		// fails the same way for any user, root included.
		lsmPath := filepath.Join(t.TempDir(), "lsm")
		writeSizedFile(t, filepath.Join(lsmPath, "objects"), 10)

		_, err := CalculateSegmentHeap(lsmPath, []string{"objects"}, minMMapSize)
		require.Error(t, err)
		assert.NotErrorIs(t, err, fs.ErrNotExist)
	})
}

func TestSaveComputedUsageDataWithSegmentHeap(t *testing.T) {
	const shardName = "shard1"
	usage := &types.ShardUsage{Name: shardName, ObjectsCount: 7}

	newShard := func(t *testing.T) string {
		t.Helper()
		indexPath := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(indexPath, shardName), 0o700))
		return indexPath
	}

	t.Run("segment heap round-trips through LoadComputedUsageData", func(t *testing.T) {
		indexPath := newShard(t)
		heap := &types.SegmentHeap{BloomBytes: 11, SmallSegmentBytes: 22, MinMMapSize: 33}

		gen := ComputedUsageGeneration(indexPath, shardName)
		saved, err := SaveComputedUsageDataWithSegmentHeap(indexPath, shardName, usage, "fp", gen, heap)
		require.NoError(t, err)
		require.True(t, saved)

		got, err := LoadComputedUsageData(indexPath, shardName)
		require.NoError(t, err)
		assert.Equal(t, heap, got.SegmentHeap)
		assert.Equal(t, int64(7), got.ShardUsage.ObjectsCount)
		assert.Equal(t, "fp", got.VectorConfigsFingerprint)
	})

	t.Run("SaveComputedUsageData writes no segment heap", func(t *testing.T) {
		indexPath := newShard(t)

		gen := ComputedUsageGeneration(indexPath, shardName)
		saved, err := SaveComputedUsageData(indexPath, shardName, usage, "fp", gen)
		require.NoError(t, err)
		require.True(t, saved)

		data, err := os.ReadFile(usageTmpFilePath(indexPath, shardName))
		require.NoError(t, err)
		var keys map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(data, &keys))
		assert.NotContains(t, keys, "segmentHeap")

		got, err := LoadComputedUsageData(indexPath, shardName)
		require.NoError(t, err)
		assert.Nil(t, got.SegmentHeap)
	})

	t.Run("declines a save when the generation moved", func(t *testing.T) {
		indexPath := newShard(t)
		heap := &types.SegmentHeap{BloomBytes: 11, SmallSegmentBytes: 22, MinMMapSize: 33}

		gen := ComputedUsageGeneration(indexPath, shardName)
		require.NoError(t, RemoveComputedUsageDataForUnloadedShard(indexPath, shardName))
		saved, err := SaveComputedUsageDataWithSegmentHeap(indexPath, shardName, usage, "fp", gen, heap)
		require.NoError(t, err)
		require.False(t, saved)
		require.NoFileExists(t, usageTmpFilePath(indexPath, shardName))
	})
}
