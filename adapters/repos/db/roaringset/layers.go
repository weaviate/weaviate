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

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/entities/concurrency"
)

// A BitmapLayer contains all the bitmap related delta-information stored for a
// specific key in one layer. A layer typically corresponds to one disk segment
// or a memtable layer
//
// A layer is essentially a snapshot in time and to get an accurate few of the
// set in its entirety multiple layers need to be combined using
// [BitmapLayers].
//
// The contents of Additions and Deletions must be mutually exclusive. A layer
// cannot both add and delete an element. The only way to create new layers is
// through inserting into a Memtable. The memtable must make sure that:
//
//   - When an element is added, any previous deletion of this element is
//     removed
//   - When an element is deleted, any previous addition of this element is
//     removed.
//
// As a result, an element is either a net addition or a net deletion in a
// layer, but it can never be both.
type BitmapLayer struct {
	Additions *sroar.Bitmap
	Deletions *sroar.Bitmap
}

func (l *BitmapLayer) Clone() BitmapLayer {
	clone := BitmapLayer{}
	if l.Additions != nil {
		clone.Additions = l.Additions.Clone()
	}
	if l.Deletions != nil {
		clone.Deletions = l.Deletions.Clone()
	}
	return clone
}

// CloneDroppingEmpty copies the layer as [BitmapLayer.Clone] does, but leaves a
// side that holds nothing nil rather than cloning it. A memtable node always
// allocates both bitmaps, so a key only ever written to would otherwise pay a
// clone to carry an empty deletion set.
//
// nil and empty are interchangeable to [LayerMerger]: its AndNot and Or take
// either, and the branch that adopts a layer's additions outright is reached
// only when no older layer contributed, which is when that layer's own
// deletions have nothing to delete from. Pinned by
// TestNilAndEmptyBitmapsMergeAlike.
func (l *BitmapLayer) CloneDroppingEmpty() BitmapLayer {
	clone := BitmapLayer{}
	// IsEmpty is nil-safe, so a nil bitmap takes the same path as an empty one.
	if !l.Additions.IsEmpty() {
		clone.Additions = l.Additions.Clone()
	}
	if !l.Deletions.IsEmpty() {
		clone.Deletions = l.Deletions.Clone()
	}
	return clone
}

// BitmapLayers are a helper type to perform operations on multiple layers,
// such as [BitmapLayers.Flatten] or [BitmapLayers.Merge].
type BitmapLayers []BitmapLayer

// Flatten reduces all snapshots into a single Bitmap. This bitmap no longer
// contains separate additions and deletions, but a single set where all
// additions and deletions have been applied in the correct order.
//
// If you do not wish to flatten all of history, but rather combine two layers,
// such as would happen in a Compaction, use [BitmapLayers.Merge] instead.
//
// Flatten is typically used when serving a specific key to the user: It
// flattens all disk segments, a currently flushing memtable if it exists, and
// the active memtable into a single bitmap. The final bitmap is returned to
// the user.
//
// # Flattening Logic
//
//   - The first layer is seen as chronologically first. Deletions in the
//     first layers are ignored, as there is nothing to be deleted. As a
//     result, the additions of the first segment become the root state in the
//     first iteration.
//   - Any subsequent layer is merged into the root layer in the following way:
//     Deletions remove any existing additions, Additions are added.
//   - This process happens one layer at a time. This way delete-and-readd
//     cycles are reflected correctly. For example, if layer 2 deletes an element
//     X and layer 3 adds element X, then it is a net addition overall, and X
//     should be represented in the final bitmap. If the order is reversed and
//     layer 2 adds X, whereas layer 3 removes X, it is should not be contained
//     in the final map.
//
// maxConc caps sroar merge concurrency; pass the per-query budget on read
// paths, or concurrency.SROAR_MERGE for background work.
func (bml BitmapLayers) Flatten(clone bool, maxConc int) *sroar.Bitmap {
	if len(bml) == 0 {
		return sroar.NewBitmap()
	}

	// A first layer that only deletes has nothing to fold into, and every later
	// layer's deletions would then be applied to a nil receiver. Cloning a nil
	// bitmap yields an empty one, so a caller that clones never reaches this;
	// one that does not would panic in sroar's OrConc without it.
	merged := bml[0].Additions
	switch {
	case clone:
		merged = merged.Clone()
	case merged == nil:
		merged = sroar.NewBitmap()
	}

	for i := 1; i < len(bml); i++ {
		merged.AndNotConc(bml[i].Deletions, maxConc)
		merged.OrConc(bml[i].Additions, maxConc)
	}

	return merged
}

// LayerMerger performs the same left-fold as [BitmapLayers.Flatten], one
// layer at a time, so callers that produce layers incrementally (disk →
// flushing → active on a read) need not materialize a []BitmapLayer. Seed it
// with the chronologically-first layer's additions (or nil when there is
// none yet), Add each later layer in order, then read Result. Result returns
// the accumulator itself: do not Add after reading Result, and do not copy a
// merger.
type LayerMerger struct {
	merged  *sroar.Bitmap
	maxConc int
}

// NewLayerMerger starts a fold from base, which becomes the accumulator and
// is mutated in place by Add; pass clone=true when base must not be mutated.
// A nil base means no layer yet: the first Add'd layer with non-nil additions
// has them adopted as the accumulator without a copy, as in Flatten. One
// holding only deletions is not that layer and leaves the fold unseeded.
func NewLayerMerger(base *sroar.Bitmap, clone bool, maxConc int) LayerMerger {
	if clone && base != nil {
		base = base.Clone()
	}
	return LayerMerger{merged: base, maxConc: maxConc}
}

// Add folds one layer into the accumulator: deletions remove existing
// elements, then additions are unioned in — one iteration of Flatten's loop.
// With no accumulator yet, the layer's additions are adopted (and mutated by
// later Adds); its deletions would delete from nothing and are dropped, as
// Flatten drops the base layer's. A nil Additions/Deletions is treated as
// empty.
func (m *LayerMerger) Add(layer BitmapLayer) {
	if m.merged == nil {
		m.merged = layer.Additions
		return
	}
	m.merged.AndNotConc(layer.Deletions, m.maxConc)
	m.merged.OrConc(layer.Additions, m.maxConc)
}

// Result returns the flattened bitmap accumulated so far, never nil.
func (m LayerMerger) Result() *sroar.Bitmap {
	if m.merged == nil {
		return sroar.NewBitmap()
	}
	return m.merged
}

// Merge turns two successive layers into one. It does not flatten the segment,
// but keeps additions and deletions separate. This is because there are no
// guarantees that the first segment was the root segment. A merge could run on
// segments 3+4 and they could contain deletions of elements that were added in
// segments 1 or 2.
//
// Merge is intended to be used as part of compactions.
func (bml BitmapLayers) Merge() (BitmapLayer, error) {
	out := BitmapLayer{}
	if len(bml) != 2 {
		return out, fmt.Errorf("merge requires exactly two input segments")
	}

	left, right := bml[0], bml[1]

	additions := left.Additions.Clone()
	additions.AndNotConc(right.Deletions, concurrency.SROAR_MERGE)
	additions.OrConc(right.Additions, concurrency.SROAR_MERGE)

	deletions := left.Deletions.Clone()
	deletions.AndNotConc(right.Additions, concurrency.SROAR_MERGE)
	deletions.OrConc(right.Deletions, concurrency.SROAR_MERGE)

	out.Additions = additions
	out.Deletions = deletions
	return out, nil
}
