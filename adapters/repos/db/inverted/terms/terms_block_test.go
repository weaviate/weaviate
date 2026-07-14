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

package terms

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBlockEntryEncodeInto(t *testing.T) {
	entries := []BlockEntry{
		{MaxId: 0, Offset: 0, MaxImpactTf: 0, MaxImpactPropLength: 0},
		{MaxId: 1, Offset: 2, MaxImpactTf: 3, MaxImpactPropLength: 4},
		{MaxId: 1 << 40, Offset: 1 << 20, MaxImpactTf: 1<<31 + 7, MaxImpactPropLength: 9999},
	}

	t.Run("matches Encode", func(t *testing.T) {
		for i := range entries {
			buf := make([]byte, entries[i].Size())
			entries[i].EncodeInto(buf)
			assert.Equal(t, entries[i].Encode(), buf)
		}
	})

	t.Run("packs contiguously and round-trips", func(t *testing.T) {
		buf := make([]byte, len(entries)*20)
		for i := range entries {
			entries[i].EncodeInto(buf[i*20:])
		}
		for i := range entries {
			got := DecodeBlockEntry(buf[i*20 : (i+1)*20])
			assert.Equal(t, entries[i], *got, "entry %d survived neighbouring writes", i)
		}
	})
}

func TestBlockDataEncodeInto(t *testing.T) {
	datas := []BlockData{
		{DocIds: nil, Tfs: nil},
		{DocIds: []byte{1, 2, 3}, Tfs: []byte{4, 5}},
		{DocIds: []byte{9, 8, 7, 6, 5}, Tfs: []byte{1}},
	}

	t.Run("matches Encode", func(t *testing.T) {
		for i := range datas {
			buf := make([]byte, datas[i].Size())
			datas[i].EncodeInto(buf)
			assert.Equal(t, datas[i].Encode(), buf)
		}
	})

	t.Run("packs contiguously and round-trips", func(t *testing.T) {
		total := 0
		for i := range datas {
			total += datas[i].Size()
		}
		buf := make([]byte, total)
		offsets := make([]int, len(datas))
		off := 0
		for i := range datas {
			offsets[i] = off
			datas[i].EncodeInto(buf[off:])
			off += datas[i].Size()
		}
		for i := range datas {
			got := DecodeBlockData(buf[offsets[i]:])
			assert.True(t, bytes.Equal(datas[i].DocIds, got.DocIds), "data %d docids", i)
			assert.True(t, bytes.Equal(datas[i].Tfs, got.Tfs), "data %d tfs", i)
		}
	})
}
