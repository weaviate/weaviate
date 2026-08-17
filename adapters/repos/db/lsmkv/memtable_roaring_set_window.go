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
	"bytes"
	"errors"
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/inverted"
	entlsmkv "github.com/weaviate/weaviate/entities/lsmkv"
)

// roaringSetGetWindow reads the keys of keys[from:to] that this memtable holds
// under one read lock, rather than taking it once per key. A memtable is a
// delta, so a large filter asks it about far more keys than it has, and most of
// those acquisitions find nothing.
//
// keys must be sorted and hold no duplicates, both of which SortedKeys
// guarantees. The walk advances the batch and the tree past a key it has
// matched, so a repeat of that key would be jumped over and read back as a row
// the memtable does not hold.
//
// Neither side is stepped through: whichever is behind jumps to where the other
// already is, so the pass costs what the sparser side holds and there is no
// ratio for the caller to pick.
//
// It reports the bytes it copied into dst, which is free to count here because the
// rows are in hand.
//
// dst is the caller's, one slot per key of the range, so the key at position i
// lands in dst[i-from]. Slots of keys this memtable does not hold are left as
// the zero layer, which is how the caller reads absence; a row holding neither
// additions nor deletions is absence too, and writing it changes nothing. The
// bitmaps are copied, as roaringSetGet's are, so they outlive the lock.
func (m *Memtable) roaringSetGetWindow(keys inverted.SortedKeys, from, to int, dst []roaringset.BitmapLayer) (int, error) {
	if err := CheckStrategyRoaringSet(m.strategy); err != nil {
		return 0, err
	}
	// keys is indexed by the range and dst by the offset within it, so a slice
	// wider than the range would write each row at the offset of a key the
	// window does not cover — answering those keys with another's row, and the
	// covered ones with none. An inverted range fails the same test, and an
	// empty one is legal and takes no slots.
	if from < 0 || to > keys.Len() || len(dst) != to-from {
		return 0, fmt.Errorf("roaring set window read: range [%d,%d) of %d keys into %d slots",
			from, to, keys.Len(), len(dst))
	}
	if from == to {
		return 0, nil
	}
	// Absence is the zero layer, so the slots have to start that way. Clearing
	// them here rather than asking the caller to keeps a reused buffer from
	// answering this window's keys with the last one's rows.
	clear(dst)

	m.RLock()
	defer m.RUnlock()

	// The no-copy cursor is safe only under the read lock held above. It starts
	// at the window rather than at the tree's first key, so a late window pays
	// one descent instead of walking everything before it.
	cursor := roaringset.NewBinarySearchTreeCursorNoCopy(m.roaringSet)
	key, layer, err := cursor.Seek(keys.At(from))

	// Counted where the copy is made rather than by rescanning dst afterwards,
	// so the accounting costs what the window matched rather than how wide it is.
	cloned := 0

	// Both seeks below report NotFound once the tree is past its last key, which
	// is exhaustion rather than failure, so the one check covers both.
	for keyIdx := from; ; {
		if err != nil {
			if errors.Is(err, entlsmkv.NotFound) {
				return cloned, nil
			}
			return cloned, err
		}
		if key == nil || keyIdx >= to {
			return cloned, nil
		}
		switch cmp := bytes.Compare(key, keys.At(keyIdx)); {
		case cmp == 0:
			row := layer.CloneDroppingEmpty()
			cloned += len(row.Additions.ToBuffer()) + len(row.Deletions.ToBuffer())
			dst[keyIdx-from] = row
			keyIdx++
			key, layer, err = cursor.Next()
		case cmp < 0:
			// The memtable is behind. Seek lands on the first key at or past the
			// one wanted, which is strictly past where the cursor sits, so this
			// always advances.
			key, layer, err = cursor.Seek(keys.At(keyIdx))
		default:
			// The batch is behind, and its first key at or past the memtable's
			// is strictly past keyIdx for the same reason.
			keyIdx = keys.FirstAtOrAfter(keyIdx, to, key)
		}
	}
}
