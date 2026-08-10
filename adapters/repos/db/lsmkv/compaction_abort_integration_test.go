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

package lsmkv

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestCompactor_AbortOnShouldAbort exercises the abort contract for every
// strategy that SegmentGroup.compactOnce dispatches to. See
// weaviate/0-weaviate-issues#250.
func TestCompactor_AbortOnShouldAbort(t *testing.T) {
	cases := []struct {
		name     string
		strategy string
		seed     func(t *testing.T, bucket *Bucket, seg, n int)
	}{
		{
			name:     "replace",
			strategy: StrategyReplace,
			seed: func(t *testing.T, bucket *Bucket, seg, n int) {
				for i := 0; i < n; i++ {
					key := []byte(fmt.Sprintf("seg-%d-key-%08d", seg, i))
					val := []byte(fmt.Sprintf("value-%d", i))
					require.NoError(t, bucket.Put(key, val))
				}
			},
		},
		{
			name:     "set",
			strategy: StrategySetCollection,
			seed: func(t *testing.T, bucket *Bucket, seg, n int) {
				for i := 0; i < n; i++ {
					key := []byte(fmt.Sprintf("seg-%d-key-%08d", seg, i))
					vals := [][]byte{
						[]byte(fmt.Sprintf("val-a-%d", i)),
						[]byte(fmt.Sprintf("val-b-%d", i)),
					}
					require.NoError(t, bucket.SetAdd(key, vals))
				}
			},
		},
		{
			name:     "map",
			strategy: StrategyMapCollection,
			seed: func(t *testing.T, bucket *Bucket, seg, n int) {
				for i := 0; i < n; i++ {
					key := []byte(fmt.Sprintf("seg-%d-key-%08d", seg, i))
					pair := MapPair{
						Key:   []byte(fmt.Sprintf("mk-%d", i)),
						Value: []byte(fmt.Sprintf("mv-%d", i)),
					}
					require.NoError(t, bucket.MapSet(key, pair))
				}
			},
		},
		{
			name:     "inverted",
			strategy: StrategyInverted,
			seed: func(t *testing.T, bucket *Bucket, seg, n int) {
				for i := 0; i < n; i++ {
					key := []byte(fmt.Sprintf("seg-%d-row-%08d", seg, i))
					pair := NewMapPairFromDocIdAndTf(uint64(seg*n+i), float32(i+1), float32(i+2), false)
					require.NoError(t, bucket.MapSet(key, pair))
				}
			},
		},
		{
			name:     "roaringset",
			strategy: StrategyRoaringSet,
			seed: func(t *testing.T, bucket *Bucket, seg, n int) {
				for i := 0; i < n; i++ {
					key := []byte(fmt.Sprintf("seg-%d-key-%08d", seg, i))
					require.NoError(t, bucket.RoaringSetAddOne(key, uint64(seg*n+i)))
				}
			},
		},
		{
			name:     "roaringsetrange",
			strategy: StrategyRoaringSetRange,
			seed: func(t *testing.T, bucket *Bucket, seg, n int) {
				// roaringsetrange uses a single bitmap keyed by a value;
				// stamping `n` distinct ids per seg keeps both segments
				// non-empty so the merge has work to do.
				for i := 0; i < n; i++ {
					require.NoError(t, bucket.RoaringSetRangeAdd(uint64(seg*n+i), uint64(i)))
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dirName := t.TempDir()

			bucket, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, nullLogger(), nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(tc.strategy),
			)
			require.NoError(t, err)
			defer bucket.Shutdown(ctx)
			bucket.SetMemtableThreshold(1e9)

			for seg := 0; seg < 2; seg++ {
				tc.seed(t, bucket, seg, 5000)
				require.NoError(t, bucket.FlushAndSwitch())
			}
			require.GreaterOrEqual(t, len(bucket.disk.segments), 2,
				"need at least two segments on disk to exercise compactOnce")

			// direct path: pre-cancelled ctx into the inner compactor
			t.Run("direct", func(t *testing.T) {
				abortCtx, cancel := context.WithCancel(ctx)
				cancel()
				start := time.Now()
				compacted, err := bucket.disk.compactOnce(abortCtx)
				elapsed := time.Since(start)
				require.NoError(t, err)
				assert.False(t, compacted)
				assert.Less(t, elapsed, 3*time.Second, "observed %s", elapsed)
				assertNoTempFiles(t, dirName)
			})

			// bridge path: shouldAbort=true exercised through compactOrCleanup;
			// the bridge inside compactOrCleanup pre-cancels the ctx so the
			// compactor sees the abort on its first sample.
			t.Run("bridge", func(t *testing.T) {
				start := time.Now()
				didWork := bucket.disk.compactOrCleanup(func() bool { return true })
				elapsed := time.Since(start)
				assert.False(t, didWork)
				assert.Less(t, elapsed, 3*time.Second, "observed %s", elapsed)
				assertNoTempFiles(t, dirName)
			})

			// the aborts above must not have compromised the segments
			t.Run("compaction still succeeds afterwards", func(t *testing.T) {
				compacted, err := bucket.disk.compactOnce(ctx)
				require.NoError(t, err)
				require.True(t, compacted)
				require.Len(t, bucket.disk.segments, 1)

				_, err = os.Stat(bucket.disk.segments[0].getPath())
				require.NoError(t, err, "compacted segment must survive on disk")
				assertNoTempFiles(t, dirName)
			})
		})
	}
}

// TestCompactor_FailedCompactionDiscardsNewSegment covers the non-abort failure
// exit: a compactor error must leave no .tmp behind either. The error is injected
// through shouldSkipKey, which only the set strategy consults.
func TestCompactor_FailedCompactionDiscardsNewSegment(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()

	bucket, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategySetCollection),
	)
	require.NoError(t, err)
	defer bucket.Shutdown(ctx)
	bucket.SetMemtableThreshold(1e9)

	for seg := 0; seg < 2; seg++ {
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("seg-%d-key-%08d", seg, i))
			require.NoError(t, bucket.SetAdd(key, [][]byte{[]byte("val")}))
		}
		require.NoError(t, bucket.FlushAndSwitch())
	}
	require.Len(t, bucket.disk.segments, 2)

	// set after flushing so only the compaction sees the failure
	bucket.disk.shouldSkipKey = func(key []byte, ctx context.Context) (bool, error) {
		return false, fmt.Errorf("cannot decide on key %q", key)
	}

	compacted, err := bucket.disk.compactOnce(ctx)
	require.Error(t, err)
	assert.False(t, compacted)
	assert.Len(t, bucket.disk.segments, 2, "both sources must survive a failed compaction")
	assertNoTempFiles(t, dirName)
}

// TestCompactor_FailedSwitchKeepsNewSegment pins the other side of the discard
// flag. Once switchOnDisk has marked a source for deletion the new file is the
// only copy of the merged data, so a failure after that point must keep it.
//
// The switch is made to fail by planting a directory where stripTmpExtensions
// renames the new segment to. Segment info in the filename keeps that name
// distinct from the right segment's own file, which is marked for deletion first.
func TestCompactor_FailedSwitchKeepsNewSegment(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()

	bucket, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace),
		WithWriteSegmentInfoIntoFileName(true),
	)
	require.NoError(t, err)
	defer bucket.Shutdown(ctx)
	bucket.SetMemtableThreshold(1e9)

	for seg := 0; seg < 2; seg++ {
		for i := 0; i < 10; i++ {
			key := []byte(fmt.Sprintf("seg-%d-key-%02d", seg, i))
			require.NoError(t, bucket.Put(key, []byte("v")))
		}
		require.NoError(t, bucket.FlushAndSwitch())
	}
	require.Len(t, bucket.disk.segments, 2)

	left, right := bucket.disk.segments[0], bucket.disk.segments[1]
	leftID, rightID := segmentID(left.getPath()), segmentID(right.getPath())
	// two level-0 segments compact into level 1
	blocker := filepath.Join(dirName,
		"segment-"+rightID+segmentExtraInfo(1, left.getStrategy())+".db")
	require.NoError(t, os.Mkdir(blocker, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(blocker, "occupied"), []byte("x"), 0o644))

	compacted, err := bucket.disk.compactOnce(ctx)
	require.Error(t, err, "the planted directory must make the rename fail")
	assert.False(t, compacted)

	// both sources are marked by now, so the merged data lives only in the .tmp
	assert.NotEmpty(t, deleteMarkers(t, dirName),
		"the switch must have marked a source before failing")
	tmpName := "segment-" + leftID + "_" + rightID +
		segmentExtraInfo(1, left.getStrategy()) + ".db.tmp"
	_, err = os.Stat(filepath.Join(dirName, tmpName))
	require.NoError(t, err, "the new segment must survive a failed switch")
}

// deleteMarkers lists the files in dir that are marked for deletion.
func deleteMarkers(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	var names []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), DeleteMarkerSuffix) {
			names = append(names, e.Name())
		}
	}
	return names
}
