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

package blockenc

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/varenc"
)

func TestPackedEncodeDecodeRoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		docIds   []uint64
		termFreq []uint64
	}{
		{name: "single", docIds: []uint64{1}, termFreq: []uint64{1}},
		{name: "ascending", docIds: []uint64{1, 5, 9, 100, 1000}, termFreq: []uint64{3, 1, 7, 2, 9}},
		{name: "large gaps", docIds: []uint64{0, 1 << 20, 1 << 40}, termFreq: []uint64{1, 2, 3}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			block := PackedEncode(tt.docIds, tt.termFreq,
				&varenc.VarIntDeltaEncoder{}, &varenc.VarIntEncoder{})

			gotDocs, gotTfs := PackedDecode(block, len(tt.docIds),
				&varenc.VarIntDeltaEncoder{}, &varenc.VarIntEncoder{})

			require.Equal(t, tt.docIds, gotDocs)
			require.Equal(t, tt.termFreq, gotTfs)
		})
	}
}

// TestPackedEncodeArenaMatchesPackedEncode confirms the reusable arena variant
// produces the same encoded bytes as the allocating one.
func TestPackedEncodeArenaMatchesPackedEncode(t *testing.T) {
	docIds := []uint64{2, 4, 8, 16, 32}
	tfs := []uint64{5, 4, 3, 2, 1}

	want := PackedEncode(docIds, tfs, &varenc.VarIntDeltaEncoder{}, &varenc.VarIntEncoder{})

	var got terms.BlockData
	PackedEncodeArena(docIds, tfs,
		&varenc.VarIntDeltaEncoder{}, &varenc.VarIntEncoder{}, nil, &got)
	require.Equal(t, want.DocIds, got.DocIds)
	require.Equal(t, want.Tfs, got.Tfs)
}
