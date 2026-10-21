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
	"math"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/varenc"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// A segment flushed from typed memtable entries must be byte-identical to one
// the compaction encoder writes from MapPairs.
func TestInvertedFlushEncodingMatchesMapPairEncoding(t *testing.T) {
	tests := []struct {
		name  string
		count int
		tf    func(i int) float32
	}{
		{name: "single value", count: 1},
		{name: "below the block boundary", count: 5},
		{name: "across the block boundary", count: 200},
		{
			name:  "term frequency bits that are not a normal float",
			count: 1,
			tf:    func(int) float32 { return float32(math.NaN()) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pairs := make([]invertedPair, tt.count)
			mapPairs := make([]MapPair, tt.count)
			ids := make([]uint64, tt.count)
			lens := make([]uint32, tt.count)

			for i := 0; i < tt.count; i++ {
				docID := uint64(i*3 + 1)
				tf, propLen := float32(i%7+1), float32(i%5+1)
				if tt.tf != nil {
					tf = tt.tf(i)
				}
				pairs[i] = newInvertedPair(docID, tf, propLen, false)
				mapPairs[i] = NewMapPairFromDocIdAndTf(docID, tf, propLen, false)
				ids[i], lens[i] = docID, uint32(propLen)
			}

			want, _ := createAndEncodeBlocksWithLengths(pairs,
				&varenc.VarIntDeltaEncoder{}, &varenc.VarIntEncoder{}, 1.2, 0.75, 1.0)

			bufs := newCompactorInvertedBuffers()
			got := createAndEncodeBlocksCompaction(mapPairs, &propLengthsView{ids: ids, lens: lens},
				&bufs, &varenc.VarIntDeltaEncoder{}, &varenc.VarIntEncoder{}, 1.2, 0.75, 1.0)

			require.Equal(t, want, got)
		})
	}
}

func TestInvertedFlushResolvesRowWithinMemtable(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyInverted))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })

	term := []byte("term")
	require.NoError(t, b.InvertedSet(term, 3, 4, 40))
	require.NoError(t, b.InvertedSet(term, 1, 2, 10))
	require.NoError(t, b.InvertedSet(term, 1, 5, 20))
	require.NoError(t, b.InvertedSet(term, 2, 3, 30))
	require.NoError(t, b.InvertedDeleteDoc(term, 2))
	require.NoError(t, b.InvertedDeleteDoc(term, 3))
	require.NoError(t, b.InvertedSet(term, 3, 6, 60))

	require.NoError(t, b.FlushAndSwitch())

	got, err := b.MapList(ctx, term)
	require.NoError(t, err)

	want := []MapPair{
		NewMapPairFromDocIdAndTf(1, 5, 20, false),
		NewMapPairFromDocIdAndTf(3, 6, 60, false),
	}
	require.Equal(t, want, got)
}
