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

// windowFill is what one window read produced: To is one past the last key it
// filled, which is `to` unless the budget stopped it short, and Bytes is what
// the rows it copied cost.
type windowFill struct {
	To    int
	Bytes int
}

// roaringSetGetWindow reads the keys of keys[from:to] that this memtable holds
// under one read lock, rather than taking it once per key. A memtable is a delta,
// so a large filter asks it about far more keys than it has and most of those
// acquisitions find nothing.
//
// keys must be sorted and free of duplicates, which SortedKeys guarantees: the
// walk advances past a key it has matched, so a repeat would be jumped over and
// read back as absent. Neither side is stepped through — whichever is behind
// jumps to where the other is — so the pass costs what the sparser side holds.
//
// budget caps the bytes copied and the window ends before the row that would
// cross it, so what it holds is the budget, or one row where a row alone is
// bigger, since the key at from is taken whatever it costs. Where it ended is
// reported.
//
// dst takes at least one slot per key of the range, the key at i landing in
// dst[i-from], and all of it is cleared first — so a slot with no row reads as
// absence, and a buffer wider than the range answers nothing from an earlier one.
// The bitmaps are copied, as roaringSetGet's are, so they outlive the lock.
func (m *Memtable) roaringSetGetWindow(
	keys inverted.SortedKeys, from, to int, dst []roaringset.BitmapLayer, budget int,
) (windowFill, error) {
	if err := CheckStrategyRoaringSet(m.strategy); err != nil {
		return windowFill{}, err
	}
	// dst is indexed by the offset within the range, so too few slots would put
	// a row past its end. More is legal, and is how a caller holding one buffer
	// at the widest a window gets asks for a narrower range without reslicing
	// it. An inverted range is rejected on its own, since a negative width is
	// under every slice count; an empty one is legal and takes no slots.
	if from < 0 || to < from || to > keys.Len() || len(dst) < to-from {
		return windowFill{}, fmt.Errorf("roaring set window read: range [%d,%d) of %d keys into %d slots",
			from, to, keys.Len(), len(dst))
	}
	// Absence is the zero layer, so the slots have to start that way. Clearing
	// them here rather than asking the caller to keeps a reused buffer from
	// answering this window's keys with the last one's rows, and covers the
	// slots past the range for a caller that passed more than it asked for.
	clear(dst)

	if from == to {
		return windowFill{To: to}, nil
	}

	m.RLock()
	defer m.RUnlock()

	// The no-copy cursor is safe only under the read lock held above. It starts
	// at the window rather than at the tree's first key, so a late window pays
	// one descent instead of walking everything before it.
	cursor := roaringset.NewBinarySearchTreeCursorNoCopy(m.roaringSet)
	key, layer, err := cursor.Seek(keys.At(from))

	// Counted where the copy is made rather than by rescanning dst afterwards,
	// so the accounting costs what the window matched rather than how wide it is.
	// It is also what the budget is spent against.
	fill := windowFill{To: to}

	// Both seeks below report NotFound once the tree is past its last key, which
	// is exhaustion rather than failure, so the one check covers both.
	for keyIdx := from; ; {
		if err != nil {
			if errors.Is(err, entlsmkv.NotFound) {
				return fill, nil
			}
			return fill, err
		}
		if key == nil || keyIdx >= to {
			return fill, nil
		}
		switch cmp := bytes.Compare(key, keys.At(keyIdx)); {
		case cmp == 0:
			// Priced before copying, which is free — ToBuffer hands back the
			// bitmap's own bytes, and a clone copies exactly that many. Pricing it
			// afterwards would bound the row after the one that broke the budget.
			// The first key is taken whatever it costs: refusing it would leave the
			// slot unwritten, read as a key this memtable does not hold rather than
			// a row too big to fit.
			cost := len(layer.Additions.ToBuffer()) + len(layer.Deletions.ToBuffer())
			if fill.Bytes+cost > budget && keyIdx > from {
				fill.To = keyIdx
				return fill, nil
			}
			dst[keyIdx-from] = layer.CloneDroppingEmpty()
			fill.Bytes += cost
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
