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
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestRemoveTargetVectorDimensions_RowIsNeverExpanded pins that a clear never
// holds a row's doc IDs as one slice. A plain vector keeps every object carrying
// it under a single row, so the expanded row costs eight bytes per object —
// hundreds of megabytes on a large shard — where the compressed bitmap costs a
// small fraction of that.
func TestRemoveTargetVectorDimensions_RowIsNeverExpanded(t *testing.T) {
	const ids = 2_000_000
	ctx := t.Context()

	b, err := lsmkv.NewBucketCreator().NewBucket(ctx, filepath.Join(t.TempDir(), "dimensions"), "",
		logrus.New(), nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet))
	require.NoError(t, err)
	defer b.Shutdown(ctx)

	values := make([]uint64, ids)
	for i := range values {
		values[i] = uint64(i)
	}
	require.NoError(t, b.RoaringSetAddList(dimKey("vec", 384), values))
	require.NoError(t, b.FlushMemtable())

	defer func(rate int) { runtime.MemProfileRate = rate }(runtime.MemProfileRate)
	runtime.MemProfileRate = 1

	require.NoError(t, RemoveTargetVectorDimensions(ctx, b, "vec"))

	scan, err := ScanTargetVectorDimensions(ctx, b, "vec", 0)
	require.NoError(t, err)
	require.Zero(t, scan.Raw.Count, "precondition: the clear has to remove the row")

	// The profile publishes what the last completed collection saw.
	runtime.GC()
	runtime.GC()
	largest := largestAllocationWithin(t, "shard_usage.removeRoaringSetRow")
	require.Positive(t, largest, "precondition: the profile has to see the clear allocate")
	require.Less(t, largest, int64(ids),
		"the clear made a single allocation of %d bytes, which is the row expanded; the "+
			"expanded row is %d bytes", largest, 8*ids)
}

// largestAllocationWithin reports the average size of the largest allocation
// site whose stack passes through fn.
func largestAllocationWithin(t *testing.T, fn string) int64 {
	t.Helper()
	n, _ := runtime.MemProfile(nil, true)
	var records []runtime.MemProfileRecord
	for {
		records = make([]runtime.MemProfileRecord, n+64)
		var ok bool
		if n, ok = runtime.MemProfile(records, true); ok {
			records = records[:n]
			break
		}
	}

	var largest int64
	for _, r := range records {
		if r.AllocObjects == 0 {
			continue
		}
		frames := runtime.CallersFrames(r.Stack())
		for {
			f, more := frames.Next()
			if strings.HasSuffix(f.Function, fn) {
				largest = max(largest, r.AllocBytes/r.AllocObjects)
				break
			}
			if !more {
				break
			}
		}
	}
	return largest
}
