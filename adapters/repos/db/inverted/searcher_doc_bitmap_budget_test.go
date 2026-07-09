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

package inverted

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/concurrency/testinghelpers"
	entcfg "github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
)

// TestDocBitmapInvertedRoaringSet_RowMergeBudget pins the row-merge path in
// docBitmapInvertedRoaringSet: a >= filter that spans many rows unions each
// row bitmap via OrConc(_, mergeConc). Two guarantees:
//   - equivalence: a budget-1 query returns the exact same docIDs as an
//     unconstrained one (the budget bounds merge concurrency, never the result)
//   - ceiling: under budget-1 the per-row merge spawns no workers, so the path
//     holds only its own goroutine
func TestDocBitmapInvertedRoaringSet_RowMergeBudget(t *testing.T) {
	ctx := context.Background()
	b := buildMultiRowRoaringSetBucket(t, ctx)

	// GreaterThanEqual from the lowest key walks every row, so the readFn runs
	// numKeys-1 OrConc merges into the accumulated result
	pv := &propValuePair{
		operator: filters.OperatorGreaterThanEqual,
		value:    []byte("k000"),
	}

	run := func(queryCtx context.Context) []uint64 {
		s := &Searcher{} // RoaringSet path dereferences no Searcher fields
		bm, err := s.docBitmapInvertedRoaringSet(queryCtx, b, 0, pv)
		require.NoError(t, err)
		defer bm.release()
		return bm.docIDs.ToArray()
	}

	defaultIDs := run(ctx)
	budget1IDs := run(concurrency.CtxWithBudget(ctx, 1))

	require.Equal(t, defaultIDs, budget1IDs)
	// guard the fixture: the merge must actually union every row
	require.Len(t, defaultIDs, numRoaringRows*idsPerRow)

	t.Run("goroutine ceiling under budget-1", func(t *testing.T) {
		if entcfg.Enabled(os.Getenv("DISABLE_SROAR_MERGE_BUDGET")) {
			t.Skip("budget cap disabled via kill switch")
		}
		if concurrency.SROAR_MERGE < 2 {
			t.Skipf("SROAR_MERGE=%d < 2: no merge fan-out possible, nothing to bound",
				concurrency.SROAR_MERGE)
		}

		budget1 := concurrency.CtxWithBudget(ctx, 1)
		// budget=1 => each OrConc spawns no workers, so an in-flight query holds
		// only its own goroutine; slack absorbs sampler and runtime/GC noise
		testinghelpers.AssertGoroutineCeiling(t, 8, 1, 8, 200*time.Millisecond, func() error {
			s := &Searcher{}
			bm, err := s.docBitmapInvertedRoaringSet(budget1, b, 0, pv)
			if err != nil {
				return err
			}
			bm.release()
			return nil
		})
	})
}

const (
	numRoaringRows = 32
	idsPerRow      = 200
)

// buildMultiRowRoaringSetBucket creates a real RoaringSet bucket with
// numRoaringRows keys, each carrying idsPerRow docIDs spread across as many
// sroar containers (stride 2^16). Distinct docIDs per row so the union grows,
// enough containers that an unconstrained merge would want >1 worker.
func buildMultiRowRoaringSetBucket(t *testing.T, ctx context.Context) *lsmkv.Bucket {
	t.Helper()

	logger, _ := test.NewNullLogger()
	tmpDir := t.TempDir()

	b, err := lsmkv.NewBucketCreator().NewBucket(ctx, tmpDir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(context.Background())) })

	b.SetMemtableThreshold(1e9) // no auto-flush; keep the fixture deterministic

	for row := 0; row < numRoaringRows; row++ {
		values := make([]uint64, idsPerRow)
		for i := range values {
			values[i] = uint64(i)<<16 + uint64(row)
		}
		require.NoError(t, b.RoaringSetAddList([]byte(fmt.Sprintf("k%03d", row)), values))
	}
	require.NoError(t, b.FlushAndSwitch())

	return b
}
