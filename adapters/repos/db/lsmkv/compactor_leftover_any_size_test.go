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

package lsmkv

import (
	"context"
	"fmt"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// A bucket that compacts its left-over segments leaves a large segment next
// to the small ones that come after it: a left-over pair is compacted only if
// the two are of similar sizes. Asked to (CompactLeftOverSegmentsOfAnySize),
// it compacts them whatever their sizes, down to one segment, with what it
// holds unchanged; and once that is switched off, the next small segment
// stays apart again.
func TestCompactionOfLeftOverSegmentsOfAnySize(t *testing.T) {
	ctx := context.Background()
	// A megabyte a value: twelve of them make a segment past the ten
	// megabytes under which any two segments count as of similar sizes.
	large := make([]byte, 1<<20)
	for i := range large {
		large[i] = byte(i)
	}
	small := []byte("small")
	tests := []struct {
		name     string
		strategy string
		// force is whether the bucket compacts its left-over segments.
		force bool
	}{
		{name: "set, left-over segments compacted", strategy: StrategySetCollection, force: true},
		{name: "replace, left-over segments compacted", strategy: StrategyReplace, force: true},
		{name: "set, left-over segments not compacted", strategy: StrategySetCollection},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			logger, _ := test.NewNullLogger()
			open := func() *Bucket {
				bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
					cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
					WithStrategy(tt.strategy), WithForceCompaction(tt.force))
				require.NoError(t, err)
				// so big it never triggers as part of this test
				bucket.SetMemtableThreshold(1e9)
				return bucket
			}
			bucket := open()
			defer func() { require.NoError(t, bucket.Shutdown(ctx)) }()

			key := func(i int) []byte { return []byte(fmt.Sprintf("key-%04d", i)) }
			put := func(i int, value []byte) {
				if tt.strategy == StrategyReplace {
					require.NoError(t, bucket.Put(key(i), value))
					return
				}
				require.NoError(t, bucket.SetAdd(key(i), [][]byte{value}))
			}
			drop := func(i int, value []byte) {
				if tt.strategy == StrategyReplace {
					require.NoError(t, bucket.Delete(key(i)))
					return
				}
				require.NoError(t, bucket.SetDeleteSingle(key(i), value))
			}
			// check reads every key written so far: those in gone were deleted.
			written := map[int]bool{}
			gone := map[int]bool{}
			check := func(at string) {
				t.Helper()
				for i := range written {
					if tt.strategy == StrategyReplace {
						got, err := bucket.Get(key(i))
						require.NoError(t, err, "%s: key %d", at, i)
						require.Equal(t, gone[i], got == nil, "%s: key %d, deleted: %v", at, i, gone[i])
						continue
					}
					got, err := bucket.SetList(key(i))
					require.NoError(t, err, "%s: key %d", at, i)
					require.Equal(t, gone[i], len(got) == 0, "%s: key %d, deleted: %v", at, i, gone[i])
				}
			}
			compact := func() {
				for {
					compacted, err := bucket.disk.compactOnce(ctx)
					require.NoError(t, err)
					if !compacted {
						return
					}
				}
			}

			// A large segment a level up: two flushes of six megabytes, which
			// are compacted as two segments of the same level always are.
			for i := range 12 {
				put(i, large)
				written[i] = true
				if i == 5 || i == 11 {
					require.NoError(t, bucket.FlushAndSwitch())
				}
			}
			compact()
			require.Equal(t, 1, bucket.SegmentCount())
			// A small one after it, with a delete of something the large one
			// holds.
			put(100, small)
			written[100] = true
			drop(7, large)
			gone[7] = true
			require.NoError(t, bucket.FlushAndSwitch())
			require.Equal(t, 2, bucket.SegmentCount())
			check("flushed")

			compact()
			require.Equal(t, 2, bucket.SegmentCount(), "a large segment is not compacted with a small one after it")
			check("compacted as always")

			bucket.CompactLeftOverSegmentsOfAnySize(true)
			compact()
			if !tt.force {
				require.Equal(t, 2, bucket.SegmentCount(), "nothing is asked of a bucket that keeps its left-over segments")
				check("asked for any size")
				return
			}
			require.Equal(t, 1, bucket.SegmentCount(), "asked to, the bucket ends as one segment")
			check("one segment")

			// Switched off, a small segment that comes next stays apart.
			bucket.CompactLeftOverSegmentsOfAnySize(false)
			put(101, small)
			written[101] = true
			drop(8, large)
			gone[8] = true
			require.NoError(t, bucket.FlushAndSwitch())
			compact()
			require.Equal(t, 2, bucket.SegmentCount(), "switched off, the large segment stays apart again")
			check("switched off")

			// And it reads the same after it is opened again.
			require.NoError(t, bucket.Shutdown(ctx))
			bucket = open()
			require.Equal(t, 2, bucket.SegmentCount())
			check("opened again")
		})
	}
}
