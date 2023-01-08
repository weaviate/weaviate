package roaringset

import (
	"fmt"

	"github.com/dgraph-io/sroar"
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

// BitmapLayers are a helper type to perform operations on mulitple layers,
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
func (bml BitmapLayers) Flatten() *sroar.Bitmap {
	if len(bml) == 0 {
		return sroar.NewBitmap()
	}

	cur := bml[0]
	// TODO: is this copy really needed? aren't we already operating on copied
	// bms?
	merged := cur.Additions.Clone()

	for i := 1; i < len(bml); i++ {
		merged.AndNot(bml[i].Deletions)
		merged.Or(bml[i].Additions)
	}

	return merged
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

	out.Additions = left.Additions.Clone()
	out.Additions.Or(right.Additions)
	out.Additions.AndNot(right.Deletions)

	out.Deletions = left.Deletions.Clone()
	out.Deletions.AndNot(right.Additions)
	out.Deletions.Or(right.Deletions)

	return out, nil
}
