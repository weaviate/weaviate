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
	"math"

	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/inverted"
	entlsmkv "github.com/weaviate/weaviate/entities/lsmkv"
)

// windowFill is what one window read produced: To is one past the last key
// filled (== to unless budget cut it short), Bytes is the copy cost.
type windowFill struct {
	To    int
	Bytes int
}

// roaringSetGetWindow reads keys[from:to] from this memtable under a single
// read lock instead of one acquisition per key. keys must be sorted and
// deduplicated (SortedKeys guarantees this).
//
// budget caps the bytes copied: the window ends before the row that would
// cross it, except the key at from is always taken regardless of cost. Where
// it actually ended is reported in the result, which is meaningful only when
// the error is nil.
//
// dst must have exactly one slot per key of the range, key i landing in
// dst[i-from]; it is cleared in full first, so a reused buffer can't answer
// with a stale window's rows. Bitmaps are copied, as roaringSetGet's are, so
// they outlive the lock.
func (m *Memtable) roaringSetGetWindow(
	keys inverted.SortedKeys, from, to int, dst []roaringset.BitmapLayer, budget int,
) (windowFill, error) {
	if err := CheckStrategyRoaringSet(m.strategy); err != nil {
		return windowFill{}, err
	}
	// dst is exactly the range: clear wipes all of it, so only an exact length
	// makes what this read erases the same slots as the range it was asked for.
	// A caller handing over a wider slice has computed its window wrong.
	if from < 0 || to < from || to > keys.Len() || len(dst) != to-from {
		return windowFill{}, fmt.Errorf("roaring set window read: range [%d,%d) of %d keys into %d slots",
			from, to, keys.Len(), len(dst))
	}
	clear(dst)

	if from == to {
		return windowFill{To: to}, nil
	}

	m.RLock()
	defer m.RUnlock()

	// Seeking straight to from avoids walking every earlier key for a late window.
	cursor := roaringset.NewBinarySearchTreeCursorNoCopy(m.roaringSet)
	key, layer, err := cursor.Seek(keys.At(from))
	fill := windowFill{To: to}

	for keyIdx := from; ; {
		if err != nil {
			// NotFound means the tree is exhausted, not that something failed.
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
			// The row that breaks the budget is priced but never copied. The
			// first key is taken whatever it costs, so an unwritten slot always
			// means "not held", never "refused".
			avail := budget - fill.Bytes
			if keyIdx == from {
				avail = math.MaxInt
			}
			row, cost, ok := layer.CloneIfWithin(avail)
			if !ok {
				fill.To = keyIdx
				return fill, nil
			}
			dst[keyIdx-from] = row
			fill.Bytes += cost
			keyIdx++
			key, layer, err = cursor.Next()
		case cmp < 0:
			// Memtable is behind the batch: jump the cursor forward.
			key, layer, err = cursor.Seek(keys.At(keyIdx))
		default:
			// Batch is behind the memtable: jump keyIdx forward instead.
			keyIdx = keys.FirstAtOrAfter(keyIdx, to, key)
		}
	}
}
