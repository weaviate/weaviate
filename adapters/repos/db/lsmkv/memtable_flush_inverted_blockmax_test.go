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

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/schema"
)

// TestInvertedFlushBakesImpactArgmax pins the k1/b argument order on the
// memtable-flush block-serialization path: the flushed block's
// (MaxImpactTf, MaxImpactPropLength) pair must be the argmax of the BM25
// impact tf/(tf + k1*(1-b + b*pl/avg)). With the arguments transposed, the
// baked pair recomputes to a bound below the block's true maximum score and
// BlockMax-WAND wrongly prunes the true top-1 doc.
func TestInvertedFlushBakesImpactArgmax(t *testing.T) {
	const term = "impactterm"
	ctx := context.Background()

	b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", nullLoggerB(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyInverted))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(context.Background())) })

	put := func(id uint64, key string, tf, pl float32) {
		require.NoError(t, b.MapSet([]byte(key), NewMapPairFromDocIdAndTf(id, tf, pl, false)))
	}

	// One posting block (14 docs), k1=1.2 / b=0.75, corpus average exactly 100:
	//   docs 1-12 (tf=6,  pl=150): impact 0.7843
	//   doc  13   (tf=10, pl=150): impact 0.8584 <- correct argmax
	//   doc  14   (tf=1,  pl=2):   impact 0.7587, but 1.152 under the k1/b-swapped
	//                              formula tf/(tf + b*(1-k1 + k1*pl/avg)) <- its argmax
	// A baked (1,2) pair bounds the block at 0.7587 < 0.7843, so WAND prunes the
	// block once doc 1 fills the top-1 heap and never scores doc 13.
	for i := uint64(1); i <= 12; i++ {
		put(i, term, 6, 150)
	}
	put(13, term, 10, 150)
	put(14, term, 1, 2)
	// 138 fillers of length 96: (12*150 + 150 + 2 + 138*96) / 152 = 100 exactly
	for i := 0; i < 138; i++ {
		put(uint64(100+i), fmt.Sprintf("filler%05d", i), 1, 96)
	}
	require.NoError(t, b.FlushAndSwitch())

	avg, count := b.GetAveragePropertyLength()
	require.EqualValues(t, 152, count)
	require.InDelta(t, 100.0, avg, 1e-9, "geometry requires the exact average")

	view := b.GetConsistentView()
	defer view.ReleaseView()
	require.Len(t, view.Disk, 1)
	sbm := view.Disk[0].newSegmentBlockMax(nil, []byte(term), 0, 1, 1,
		nil, nil, nil, avg, schema.BM25Config{K1: 1.2, B: 0.75})
	require.NotNil(t, sbm)
	require.Len(t, sbm.blockEntries, 1, "probe postings must share one block")
	require.Equal(t, uint32(10), sbm.blockEntries[0].MaxImpactTf,
		"flushed block must bake the impact-argmax pair (k1/b order)")
	require.Equal(t, uint32(150), sbm.blockEntries[0].MaxImpactPropLength,
		"flushed block must bake the impact-argmax pair (k1/b order)")

	got := runBlockMaxWand(t, b, []string{term}, 152, 1)
	require.Len(t, got, 1)
	require.Contains(t, got, uint64(13), "true top-1 pruned: flush baked an unsound block-max bound")
}
