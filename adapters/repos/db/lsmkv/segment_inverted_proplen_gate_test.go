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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestSelectPropLengthLayout is the deterministic pin for the load-side layout
// gate. It calls the gate directly with fixed slices, bypassing the gob-map
// round-trip whose iteration order is random per process — the reason a
// flush-based test cannot reliably reproduce the early-stop trap.
func TestSelectPropLengthLayout(t *testing.T) {
	const highID = uint64(1) << 33 // > MaxUint32

	type kind int
	const (
		empty kind = iota
		dense
		narrow // uint32 pairs
		wide   // uint64 pairs
	)

	cases := []struct {
		name string
		ids  []uint64
		lens []uint32
		want kind
	}{
		{"empty", nil, nil, empty},
		{"contiguous_no_zero_is_dense", []uint64{0, 1, 2, 3, 4, 5}, []uint32{5, 4, 3, 2, 1, 6}, dense},
		{"sparse_no_zero_small_max_is_narrow", []uint64{0, 500}, []uint32{1, 2}, narrow},
		{"sparse_no_zero_high_max_is_wide", []uint64{0, highID}, []uint32{1, 2}, wide},
		{"boundary_maxuint32_is_narrow", []uint64{0, 1, 2, math.MaxUint32}, []uint32{1, 1, 1, 1}, narrow},
		{"boundary_maxuint32_plus_1_is_wide", []uint64{0, 1, 2, math.MaxUint32 + 1}, []uint32{1, 1, 1, 1}, wide},
		{"zero_length_small_max_is_narrow", []uint64{3, 7, 50}, []uint32{0, 5, 9}, narrow},

		// The two order-sensitive cases below both MUST resolve to wide. They pin
		// the early-stop trap: the dense gate's min/max loop stops at the first
		// zero-length doc, so its max excludes anything after it. The uint32 gate
		// must instead use sortPropLenPairs' full-scan max.
		//
		// zero_before_high: the zero-length doc precedes highID in slice order, so
		// the early-stopped max is 3 (<=uint32). A gate that trusted it would narrow
		// and truncate highID — this subtest catches that regression on every run.
		{"zero_before_high_stays_wide", []uint64{3, highID, 7, 50}, []uint32{0, 12, 5, 9}, wide},
		// zero_after_high: highID is scanned before the early-stop, so even the
		// buggy gate happens to see the true max here — this subtest proves the
		// correct gate is order-independent, not that it catches the bug.
		{"zero_after_high_stays_wide", []uint64{highID, 3, 7, 50}, []uint32{12, 0, 5, 9}, wide},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var trueMax uint64 // capture before selectPropLengthLayout sorts in place
			for _, id := range tc.ids {
				if id > trueMax {
					trueMax = id
				}
			}
			layout := selectPropLengthLayout(tc.ids, tc.lens)
			switch tc.want {
			case empty:
				assert.Nil(t, layout.dense)
				assert.Nil(t, layout.ids32)
				assert.Nil(t, layout.ids64)
				assert.Nil(t, layout.lens)
			case dense:
				assert.NotNil(t, layout.dense, "expected dense layout")
				assert.Nil(t, layout.ids32, "dense: no narrow pairs")
				assert.Nil(t, layout.ids64, "dense: no wide pairs")
			case narrow:
				assert.NotNil(t, layout.ids32, "expected uint32 pairs")
				assert.Nil(t, layout.ids64, "narrow: no uint64 pairs")
				assert.Nil(t, layout.dense, "narrow: not dense")
				assert.NotNil(t, layout.lens, "pairs must carry lens")
			case wide:
				assert.NotNil(t, layout.ids64, "expected uint64 pairs — true max exceeds uint32")
				assert.Nil(t, layout.ids32, "wide: must not narrow, the high docID would truncate")
				assert.Nil(t, layout.dense, "wide: not dense")
				assert.Greater(t, trueMax, uint64(math.MaxUint32), "wide precondition: true max exceeds uint32")
				assert.Contains(t, layout.ids64, trueMax, "the >uint32 docID must survive intact")
			}
		})
	}
}
