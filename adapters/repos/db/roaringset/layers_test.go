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

package roaringset

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/entities/concurrency"
)

func Test_BitmapLayers_Flatten(t *testing.T) {
	type inputSegment struct {
		additions []uint64
		deletions []uint64
	}

	type test struct {
		name                 string
		inputs               []inputSegment
		expectedContained    []uint64
		expectedNotContained []uint64
	}

	tests := []test{
		{
			name:                 "no inputs",
			inputs:               nil,
			expectedContained:    nil,
			expectedNotContained: nil,
		},
		{
			name: "single segment",
			inputs: []inputSegment{
				{
					additions: []uint64{4, 5},
				},
			},
			expectedContained:    []uint64{4, 5},
			expectedNotContained: nil,
		},
		{
			name: "three segments, only additions",
			inputs: []inputSegment{
				{
					additions: []uint64{4, 5},
				},
				{
					additions: []uint64{5, 6},
				},
				{
					additions: []uint64{6, 7, 8},
				},
			},
			expectedContained:    []uint64{4, 5, 6, 7, 8},
			expectedNotContained: nil,
		},
		{
			name: "two segments, including a delete",
			inputs: []inputSegment{
				{
					additions: []uint64{4, 5},
				},
				{
					additions: []uint64{5, 6},
					deletions: []uint64{4},
				},
			},
			expectedContained:    []uint64{5, 6},
			expectedNotContained: []uint64{4},
		},
		{
			name: "three segments, including a delete, and a re-add",
			inputs: []inputSegment{
				{
					additions: []uint64{3, 4, 5},
				},
				{
					additions: []uint64{6},
					deletions: []uint64{4, 5},
				},
				{
					additions: []uint64{5},
				},
			},
			expectedContained:    []uint64{3, 5, 6},
			expectedNotContained: []uint64{4},
		},
	}

	// Flatten must be identical regardless of the merge concurrency: single
	// threaded (1), the minimum fan-out (2), and the default cap (SROAR_MERGE).
	maxConcs := []int{1, 2, concurrency.SROAR_MERGE}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, maxConc := range maxConcs {
				t.Run(fmt.Sprintf("maxConc=%d", maxConc), func(t *testing.T) {
					input := make(BitmapLayers, len(test.inputs))
					for i, inp := range test.inputs {
						input[i].Additions = NewBitmap(inp.additions...)
						input[i].Deletions = NewBitmap(inp.deletions...)
					}

					res := input.Flatten(false, maxConc)
					for _, x := range test.expectedContained {
						assert.True(t, res.Contains(x))
					}

					for _, x := range test.expectedNotContained {
						assert.False(t, res.Contains(x))
					}
				})
			}
		})
	}
}

func Test_LayerMerger_MatchesFlatten(t *testing.T) {
	// A left-fold via LayerMerger (base = first layer's additions, Add the rest
	// in order) must produce the exact same set as BitmapLayers.Flatten over the
	// same layers. Reuses Flatten's own cases as the oracle.
	type inputSegment struct {
		additions []uint64
		deletions []uint64
	}

	tests := []struct {
		name   string
		inputs []inputSegment
	}{
		{name: "no inputs", inputs: nil},
		{name: "single segment", inputs: []inputSegment{{additions: []uint64{4, 5}}}},
		{name: "three segments, only additions", inputs: []inputSegment{
			{additions: []uint64{4, 5}}, {additions: []uint64{5, 6}}, {additions: []uint64{6, 7, 8}},
		}},
		{name: "two segments, including a delete", inputs: []inputSegment{
			{additions: []uint64{4, 5}}, {additions: []uint64{5, 6}, deletions: []uint64{4}},
		}},
		{name: "three segments, delete and re-add", inputs: []inputSegment{
			{additions: []uint64{3, 4, 5}}, {additions: []uint64{6}, deletions: []uint64{4, 5}}, {additions: []uint64{5}},
		}},
	}

	maxConcs := []int{1, 2, concurrency.SROAR_MERGE}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, maxConc := range maxConcs {
				t.Run(fmt.Sprintf("maxConc=%d", maxConc), func(t *testing.T) {
					layers := make(BitmapLayers, len(test.inputs))
					for i, inp := range test.inputs {
						layers[i].Additions = NewBitmap(inp.additions...)
						layers[i].Deletions = NewBitmap(inp.deletions...)
					}

					// oracle: Flatten (clone so it doesn't mutate the layers we
					// reuse for the merger below)
					want := layers.Flatten(true, maxConc).ToArray()

					// LayerMerger: seed with base additions, Add the rest
					var got []uint64
					if len(layers) == 0 {
						got = NewLayerMerger(nil, false, maxConc).Result().ToArray()
					} else {
						m := NewLayerMerger(layers[0].Additions, true, maxConc)
						for i := 1; i < len(layers); i++ {
							m.Add(layers[i])
						}
						got = m.Result().ToArray()
					}

					assert.Equal(t, want, got)

					// nil base: the merger adopts the first Add'd layer, so
					// folding every layer through Add must equal Flatten too;
					// fresh layers because the adopted bitmap is mutated in
					// place
					layers = make(BitmapLayers, len(test.inputs))
					for i, inp := range test.inputs {
						layers[i].Additions = NewBitmap(inp.additions...)
						layers[i].Deletions = NewBitmap(inp.deletions...)
					}
					m := NewLayerMerger(nil, false, maxConc)
					for _, layer := range layers {
						m.Add(layer)
					}
					assert.Equal(t, want, m.Result().ToArray())
				})
			}
		})
	}
}

func Test_LayerMerger_NilBase(t *testing.T) {
	t.Run("layers fold like Flatten", func(t *testing.T) {
		m := NewLayerMerger(nil, false, concurrency.SROAR_MERGE)
		m.Add(BitmapLayer{Additions: NewBitmap(1, 2), Deletions: NewBitmap()})
		m.Add(BitmapLayer{Additions: NewBitmap(3), Deletions: NewBitmap(1)})
		assert.Equal(t, []uint64{2, 3}, m.Result().ToArray())
	})

	t.Run("no layers yield an empty, non-nil result", func(t *testing.T) {
		m := NewLayerMerger(nil, false, concurrency.SROAR_MERGE)
		require.NotNil(t, m.Result())
		assert.Empty(t, m.Result().ToArray())
	})

	t.Run("first layer's additions are adopted, not copied", func(t *testing.T) {
		first := NewBitmap(1, 2)
		m := NewLayerMerger(nil, false, concurrency.SROAR_MERGE)
		m.Add(BitmapLayer{Additions: first, Deletions: NewBitmap()})
		assert.Same(t, first, m.Result())
		m.Add(BitmapLayer{Additions: NewBitmap(3), Deletions: NewBitmap(1)})
		assert.Same(t, first, m.Result())
		assert.Equal(t, []uint64{2, 3}, m.Result().ToArray())
	})

	t.Run("adopted layer's own deletions are dropped", func(t *testing.T) {
		// the first layer's deletions delete from older state, of which there
		// is none — Flatten drops the base layer's deletions the same way
		m := NewLayerMerger(nil, false, concurrency.SROAR_MERGE)
		m.Add(BitmapLayer{Additions: NewBitmap(1, 2), Deletions: NewBitmap(1)})
		assert.Equal(t, []uint64{1, 2}, m.Result().ToArray())
	})

	t.Run("nil-additions layer defers adoption to the next layer", func(t *testing.T) {
		m := NewLayerMerger(nil, false, concurrency.SROAR_MERGE)
		m.Add(BitmapLayer{Additions: nil, Deletions: NewBitmap(1)})
		m.Add(BitmapLayer{Additions: NewBitmap(2, 3), Deletions: NewBitmap(2)})
		assert.Equal(t, []uint64{2, 3}, m.Result().ToArray())
	})
}

func Test_BitmapLayers_Merge(t *testing.T) {
	type inputSegment struct {
		additions []uint64
		deletions []uint64
	}

	type test struct {
		name              string
		inputs            []inputSegment
		expectedAdditions []uint64
		expectedDeletions []uint64
		expectErr         bool
	}

	tests := []test{
		{
			name:              "no inputs - should error",
			inputs:            nil,
			expectedAdditions: nil,
			expectedDeletions: nil,
			expectErr:         true,
		},
		{
			name: "single layer - should error",
			inputs: []inputSegment{
				{
					additions: []uint64{4, 5},
				},
			},
			expectedAdditions: nil,
			expectedDeletions: nil,
			expectErr:         true,
		},
		{
			name: "three layers - should error",
			inputs: []inputSegment{
				{
					additions: []uint64{4, 5},
				},
				{
					additions: []uint64{4, 5},
				},
				{
					additions: []uint64{4, 5},
				},
			},
			expectedAdditions: nil,
			expectedDeletions: nil,
			expectErr:         true,
		},
		{
			name: "two layers, only additions",
			inputs: []inputSegment{
				{
					additions: []uint64{4, 5},
				},
				{
					additions: []uint64{5, 6, 7},
				},
			},
			expectedAdditions: []uint64{4, 5, 6, 7},
			expectedDeletions: nil,
		},
		{
			name: "additions and deletions without overlap",
			inputs: []inputSegment{
				{
					additions: []uint64{4, 5},
					deletions: []uint64{1, 2},
				},
				{
					additions: []uint64{5, 6, 7},
					deletions: []uint64{2, 3},
				},
			},
			expectedAdditions: []uint64{4, 5, 6, 7},
			expectedDeletions: []uint64{1, 2, 3},
		},
		{
			name: "previously deleted element, re-added",
			inputs: []inputSegment{
				{
					additions: []uint64{},
					deletions: []uint64{1, 2},
				},
				{
					additions: []uint64{2},
					deletions: []uint64{},
				},
			},
			expectedAdditions: []uint64{2},
			expectedDeletions: []uint64{1},
		},
		{
			name: "previously added element deleted later",
			inputs: []inputSegment{
				{
					additions: []uint64{3, 4},
					deletions: []uint64{},
				},
				{
					additions: []uint64{},
					deletions: []uint64{3},
				},
			},
			expectedAdditions: []uint64{4},
			expectedDeletions: []uint64{3},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := make(BitmapLayers, len(test.inputs))
			for i, inp := range test.inputs {
				input[i].Additions = NewBitmap(inp.additions...)
				input[i].Deletions = NewBitmap(inp.deletions...)
			}

			res, err := input.Merge()
			if test.expectErr {
				require.NotNil(t, err)
				return
			} else {
				require.Nil(t, err)
			}
			for _, x := range test.expectedAdditions {
				assert.True(t, res.Additions.Contains(x))
			}

			for _, x := range test.expectedDeletions {
				assert.True(t, res.Deletions.Contains(x))
			}

			intersect := sroar.And(res.Additions, res.Deletions)
			assert.True(t, intersect.IsEmpty(),
				"verify that additions and deletions never intersect")
		})
	}
}

// Test_BitmapLayers_FlattenFirstLayerWithoutAdditions covers a first layer that
// only deletes, which every later layer's deletions would otherwise be applied
// to as a nil receiver. A delete-only row is ordinary — a memtable that has
// only ever removed a document produces one — but Flatten reaches the case only
// when the caller does not clone, and every production caller does; cloning a
// nil bitmap yields an empty one, so no production caller reaches it.
func Test_BitmapLayers_FlattenFirstLayerWithoutAdditions(t *testing.T) {
	tests := []struct {
		name  string
		first BitmapLayer
		want  []uint64
	}{
		{name: "nil additions", first: BitmapLayer{Deletions: NewBitmap(9)}, want: []uint64{1, 2}},
		{name: "nil additions, deletes a later addition", first: BitmapLayer{Deletions: NewBitmap(1)}, want: []uint64{1, 2}},
		{name: "empty additions", first: BitmapLayer{Additions: NewBitmap(), Deletions: NewBitmap(9)}, want: []uint64{1, 2}},
		{name: "both sides nil", first: BitmapLayer{}, want: []uint64{1, 2}},
		{name: "populated additions", first: BitmapLayer{Additions: NewBitmap(7)}, want: []uint64{1, 2, 7}},
	}

	for _, tc := range tests {
		for _, clone := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/clone=%t", tc.name, clone), func(t *testing.T) {
				layers := BitmapLayers{tc.first, {Additions: NewBitmap(1, 2)}}
				got := layers.Flatten(clone, 1)
				require.NotNil(t, got)
				assert.Equal(t, tc.want, got.ToArray())
			})
		}
	}
}

// Test_BitmapLayer_CloneDroppingEmpty pins what the variant does differently
// from [BitmapLayer.Clone]: a side holding nothing comes back nil rather than
// as an allocated empty bitmap. Nothing else asserts it, so swapping the one
// call site used Clone, no other test would fail.
//
// Dropping a side is what puts nil layers in front of [LayerMerger], which has
// to fold them as it would empty ones — TestNilAndEmptyBitmapsMergeAlike is
// where that equivalence is pinned.
func Test_BitmapLayer_CloneDroppingEmpty(t *testing.T) {
	populated := func(v uint64) *sroar.Bitmap {
		bm := NewBitmap()
		bm.Set(v)
		return bm
	}

	tests := []struct {
		name          string
		layer         BitmapLayer
		wantAdditions []uint64 // nil means the side must come back nil
		wantDeletions []uint64
	}{
		{name: "both sides populated", layer: BitmapLayer{populated(1), populated(2)}, wantAdditions: []uint64{1}, wantDeletions: []uint64{2}},
		{name: "additions only", layer: BitmapLayer{Additions: populated(1)}, wantAdditions: []uint64{1}},
		{name: "deletions only", layer: BitmapLayer{Deletions: populated(2)}, wantDeletions: []uint64{2}},
		{name: "allocated but empty", layer: BitmapLayer{NewBitmap(), NewBitmap()}},
		{name: "both nil", layer: BitmapLayer{}},
		{name: "one side allocated and empty", layer: BitmapLayer{populated(1), NewBitmap()}, wantAdditions: []uint64{1}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.layer.CloneDroppingEmpty()

			for _, side := range []struct {
				name string
				want []uint64
				got  *sroar.Bitmap
				src  *sroar.Bitmap
			}{
				{"additions", tc.wantAdditions, got.Additions, tc.layer.Additions},
				{"deletions", tc.wantDeletions, got.Deletions, tc.layer.Deletions},
			} {
				if side.want == nil {
					assert.Nilf(t, side.got, "%s holding nothing must come back nil", side.name)
					continue
				}
				require.NotNilf(t, side.got, "%s", side.name)
				assert.Equalf(t, side.want, side.got.ToArray(), "%s", side.name)
				assert.NotSamef(t, side.src, side.got, "%s must be a copy, not the original", side.name)
			}
		})
	}
}

func Test_BitmapLayer_Clone(t *testing.T) {
	t.Run("cloning empty BitmapLayer", func(t *testing.T) {
		layerEmpty := BitmapLayer{}

		cloned := layerEmpty.Clone()

		assert.Nil(t, cloned.Additions)
		assert.Nil(t, cloned.Deletions)
	})

	t.Run("cloning partially inited BitmapLayer", func(t *testing.T) {
		additions := NewBitmap(1)
		deletions := NewBitmap(100)

		layerAdditions := BitmapLayer{Additions: additions}
		layerDeletions := BitmapLayer{Deletions: deletions}

		clonedLayerAdditions := layerAdditions.Clone()
		clonedLayerDeletions := layerDeletions.Clone()
		additions.Remove(1)
		deletions.Remove(100)

		assert.True(t, layerAdditions.Additions.IsEmpty())
		assert.ElementsMatch(t, []uint64{1}, clonedLayerAdditions.Additions.ToArray())
		assert.Nil(t, clonedLayerAdditions.Deletions)

		assert.True(t, layerDeletions.Deletions.IsEmpty())
		assert.Nil(t, clonedLayerDeletions.Additions)
		assert.ElementsMatch(t, []uint64{100}, clonedLayerDeletions.Deletions.ToArray())
	})

	t.Run("cloning fully inited BitmapLayer", func(t *testing.T) {
		additions := NewBitmap(1)
		deletions := NewBitmap(100)

		layer := BitmapLayer{Additions: additions, Deletions: deletions}

		clonedLayer := layer.Clone()
		additions.Remove(1)
		deletions.Remove(100)

		assert.True(t, layer.Additions.IsEmpty())
		assert.True(t, layer.Deletions.IsEmpty())
		assert.ElementsMatch(t, []uint64{1}, clonedLayer.Additions.ToArray())
		assert.ElementsMatch(t, []uint64{100}, clonedLayer.Deletions.ToArray())
	})
}

// This test aims to prevent a regression on
// https://github.com/weaviate/sroar/issues/1
// found in Serialized Roaring Bitmaps library
func Test_BitmapLayers_Merge_PanicSliceBoundOutOfRange(t *testing.T) {
	genSlice := func(fromInc, toExc uint64) []uint64 {
		slice := []uint64{}
		for i := fromInc; i < toExc; i++ {
			slice = append(slice, i)
		}
		return slice
	}

	leftLayer := BitmapLayer{Deletions: NewBitmap(genSlice(289_800, 290_100)...)}
	rightLayer := BitmapLayer{Additions: NewBitmap(genSlice(290_000, 293_000)...)}

	failingDeletionsLayer, err := BitmapLayers{leftLayer, rightLayer}.Merge()
	assert.Nil(t, err)

	assert.ElementsMatch(t, genSlice(289_800, 290_000), failingDeletionsLayer.Deletions.ToArray())
}

// TestNilAndEmptyBitmapsMergeAlike pins that a nil bitmap and an empty one are
// interchangeable to the merger, which is what lets a reader leave an empty one
// nil rather than pay to clone it.
//
// The case worth the test is not AndNot or Or but Add's first branch: with
// nothing merged yet it adopts the layer's additions and returns, so a nil
// there leaves nothing merged and that layer's deletions are never applied.
// Sound, because a deletion only has to apply to layers older than itself and
// nothing merged means no older layer contributed — but sound by argument
// rather than by construction, so it is checked.
func TestNilAndEmptyBitmapsMergeAlike(t *testing.T) {
	nilIfEmpty := func(b *sroar.Bitmap) *sroar.Bitmap {
		if b != nil && b.IsEmpty() {
			return nil
		}
		return b
	}

	type layer struct{ add, del []uint64 }
	for _, tt := range []struct {
		name     string
		disk     []uint64
		diskMiss bool
		layers   []layer
	}{
		{"disk hit, write-only layer", []uint64{1, 2}, false, []layer{{add: []uint64{3}}}},
		{"disk hit, delete-only layer", []uint64{1, 2}, false, []layer{{del: []uint64{2}}}},
		{"disk hit, delete then re-add", []uint64{1}, false, []layer{{del: []uint64{1}}, {add: []uint64{1}}}},
		{"disk miss, delete-only then add", nil, true, []layer{{del: []uint64{5}}, {add: []uint64{5}}}},
		{"disk miss, delete-only then unrelated add", nil, true, []layer{{del: []uint64{5}}, {add: []uint64{9}}}},
		{"disk miss, delete-only alone", nil, true, []layer{{del: []uint64{5}}}},
		{"disk miss, two delete-only", nil, true, []layer{{del: []uint64{5}}, {del: []uint64{6}}}},
		{"disk miss, add then delete", nil, true, []layer{{add: []uint64{7}}, {del: []uint64{7}}}},
		{"disk hit, layer holding nothing", []uint64{1, 2}, false, []layer{{}}},
		{"disk miss, layer holding nothing", nil, true, []layer{{}}},
		{"disk hit, layer holding nothing between two that do", []uint64{1}, false, []layer{{add: []uint64{2}}, {}, {del: []uint64{1}}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			run := func(nilEmpty bool) []uint64 {
				var base *sroar.Bitmap
				if !tt.diskMiss {
					base = NewBitmap(tt.disk...)
				}
				m := NewLayerMerger(base, false, 1)
				for _, l := range tt.layers {
					lay := BitmapLayer{Additions: NewBitmap(l.add...), Deletions: NewBitmap(l.del...)}
					if nilEmpty {
						lay.Additions = nilIfEmpty(lay.Additions)
						lay.Deletions = nilIfEmpty(lay.Deletions)
					}
					m.Add(lay)
				}
				return m.Result().ToArray()
			}
			require.Equal(t, run(false), run(true),
				"nilling an empty bitmap must not change the merged result")
		})
	}
}

// TestLayerHoldingNothingMergesAsIfAbsent pins what the windowed reader encodes
// absence with. It drops a layer whose sides are both empty, so a key the
// memtable holds an empty row for is folded as if the memtable did not hold it
// — the same shape an empty write produces, which reaches the tree through an
// ordinary AddList with no values.
//
// The two are indistinguishable to the fold and to nothing else: an empty layer
// deletes nothing and adds nothing. A consumer reading layers rather than
// folding them would see a difference, and none does.
func TestLayerHoldingNothingMergesAsIfAbsent(t *testing.T) {
	empty := func() BitmapLayer { return BitmapLayer{Additions: sroar.NewBitmap(), Deletions: sroar.NewBitmap()} }

	for _, tt := range []struct {
		name     string
		disk     []uint64
		diskMiss bool
		before   []BitmapLayer // layers folded before the empty one
		after    []BitmapLayer // and after it
	}{
		{name: "alone, over a disk row", disk: []uint64{1, 2}},
		{name: "alone, no disk row", diskMiss: true},
		{
			name: "between two that contribute", disk: []uint64{1},
			before: []BitmapLayer{{Additions: NewBitmap(2)}},
			after:  []BitmapLayer{{Deletions: NewBitmap(1)}},
		},
		{
			name: "first, ahead of a delete-only layer", diskMiss: true,
			after: []BitmapLayer{{Deletions: NewBitmap(5)}, {Additions: NewBitmap(5)}},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fold := func(withEmpty bool) []uint64 {
				var base *sroar.Bitmap
				if !tt.diskMiss {
					base = NewBitmap(tt.disk...)
				}
				m := NewLayerMerger(base, false, 1)
				for _, l := range tt.before {
					m.Add(l)
				}
				if withEmpty {
					m.Add(empty())
				}
				for _, l := range tt.after {
					m.Add(l)
				}
				return m.Result().ToArray()
			}
			require.Equal(t, fold(false), fold(true),
				"a layer holding nothing must fold as if it were not there")
		})
	}
}
