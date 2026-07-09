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

// TestDocBitmapInvertedRoaringSet_RowMergeBudget pins docBitmap row-merges to
// the per-query budget without changing results.
func TestDocBitmapInvertedRoaringSet_RowMergeBudget(t *testing.T) {
	ctx := context.Background()
	b := buildMultiRowRoaringSetBucket(t, ctx)

	// spans every row, so readFn accumulates via numKeys-1 OrConc merges
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
		// The kill switch (DISABLE_SROAR_MERGE_BUDGET=true) is this bound's red
		// control, but it is a manual/local check: run with the env var set and
		// this skip bypassed, and the ceiling assertion below must fail. No CI job
		// sets the kill switch, so CI exercises only the green (budget-enforced) leg.
		if entcfg.Enabled(os.Getenv("DISABLE_SROAR_MERGE_BUDGET")) {
			t.Skip("budget cap disabled via kill switch")
		}
		// Merge fan-out only exists at SROAR_MERGE>=2 (GOMAXPROCS>=4). Skipping on a
		// <4-vCPU runner would silently evaporate the guard, so fail loudly on CI
		// and only skip on dev machines.
		if concurrency.SROAR_MERGE < 2 {
			if os.Getenv("CI") != "" {
				t.Fatalf("bounding tests require GOMAXPROCS>=4, refusing to skip silently on CI (SROAR_MERGE=%d)",
					concurrency.SROAR_MERGE)
			}
			t.Skipf("SROAR_MERGE=%d < 2: no merge fan-out possible, nothing to bound",
				concurrency.SROAR_MERGE)
		}

		budget1 := concurrency.CtxWithBudget(ctx, 1)
		// budget=1 spawns no extra workers; slack absorbs sampler/GC noise
		testinghelpers.AssertGoroutineCeiling(t, numMergeWorkers, 1, 8, 200*time.Millisecond, func() error {
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
	numRoaringRows     = 32
	containersPerRow   = 128
	valuesPerContainer = 128
	idsPerRow          = containersPerRow * valuesPerContainer
	numMergeWorkers    = 24
)

// buildMultiRowRoaringSetBucket builds a bucket with enough rows and
// containers that an unconstrained merge would want more than one worker.
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

	// Each row spans containersPerRow sroar containers (stride 2^16), each
	// holding valuesPerContainer values. Dense containers make every OrConc
	// merge real work, so the extra worker an ignored budget spawns lives
	// across several 1ms sampler ticks instead of finishing between them.
	// Low-16 bits are blocked per row (row*valuesPerContainer + j) so every
	// value across the whole fixture is distinct: cardinality == rows*idsPerRow.
	for row := 0; row < numRoaringRows; row++ {
		values := make([]uint64, 0, idsPerRow)
		for c := 0; c < containersPerRow; c++ {
			for j := 0; j < valuesPerContainer; j++ {
				values = append(values, uint64(c)<<16+uint64(row*valuesPerContainer+j))
			}
		}
		require.NoError(t, b.RoaringSetAddList([]byte(fmt.Sprintf("k%03d", row)), values))
	}
	require.NoError(t, b.FlushAndSwitch())

	return b
}
