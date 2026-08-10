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

	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/inverted"
	entlsmkv "github.com/weaviate/weaviate/entities/lsmkv"
)

// roaringSetGetBatch reads every key of a sorted batch that this memtable holds,
// under one read lock, and returns them in the batch's order.
//
// The per-key alternative — roaringSetGet once per key — takes and releases the
// lock, and descends the tree, for every key. Most of those miss: a memtable is
// a delta, so a large filter asks it about far more keys than it has. Reading it
// once per batch replaces that with one acquisition and one pass.
//
// Neither side is walked a step at a time. Whichever is behind jumps to where
// the other already is — the tree by descending from its root, the batch by
// reaching ahead exponentially — so the pass costs what the sparser side holds
// rather than what the denser one does. A memtable holding one key that a batch
// of 100,000 never asks about is found and dismissed in a handful of
// comparisons, and a memtable far larger than the batch costs one descent per
// key, which is what reading it per key would have cost anyway. Which side is
// sparse decides itself, so there is no ratio to pick.
//
// The bitmaps are copied, as roaringSetGet's are, so they outlive the lock.
func (m *Memtable) roaringSetGetBatch(keys inverted.SortedKeys) (roaringset.LayerMatches, error) {
	if err := CheckStrategyRoaringSet(m.strategy); err != nil {
		return roaringset.LayerMatches{}, err
	}
	if keys.Len() == 0 {
		return roaringset.LayerMatches{}, nil
	}

	m.RLock()
	defer m.RUnlock()

	// The cursor's precondition is that the tree does not change while it is
	// walked, which the read lock gives it here in place of the drained writers
	// the flush path relies on.
	cursor := roaringset.NewSealedBinarySearchTreeCursor(m.roaringSet)
	key, layer, err := cursor.First()
	if err != nil {
		return roaringset.LayerMatches{}, err
	}

	var matches roaringset.LayerMatches
	for qi := 0; key != nil && qi < keys.Len(); {
		switch cmp := bytes.Compare(key, keys.At(qi)); {
		case cmp == 0:
			matches.At = append(matches.At, uint32(qi))
			matches.Layers = append(matches.Layers, layer.Clone())
			qi++
			key, layer, err = cursor.Next()
		case cmp < 0:
			// The memtable is behind. Seek lands on the first key at or past the
			// one wanted, which is strictly past where the cursor sits, so this
			// always advances. Past the end it reports NotFound, which is
			// exhaustion rather than failure.
			key, layer, err = cursor.Seek(keys.At(qi))
			if errors.Is(err, entlsmkv.NotFound) {
				return matches, nil
			}
		default:
			// The batch is behind, and its first key at or past the memtable's
			// is strictly past qi for the same reason.
			qi = searchGE(keys, qi, key)
		}
		if err != nil {
			return roaringset.LayerMatches{}, err
		}
	}
	return matches, nil
}

// searchGE returns the first position after from whose key is at or past target,
// or keys.Len() if there is none. The key at from must be before target, which
// is what the caller has just established.
//
// It reaches ahead exponentially before binary-searching the window that
// brackets, so a target just ahead costs a comparison or two and a distant one
// costs the log of the distance rather than the distance. That is what stops a
// memtable holding a few of a large batch's keys from walking the whole batch.
func searchGE(keys inverted.SortedKeys, from int, target []byte) int {
	n := keys.Len()
	lo := from + 1
	hi := lo
	for step := 1; hi < n && bytes.Compare(keys.At(hi), target) < 0; step *= 2 {
		lo = hi + 1
		hi = lo + step
	}
	if hi > n {
		hi = n
	}
	// Everything before lo is before target, and hi is either past the end or at
	// or past target, so the answer is in [lo, hi].
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if bytes.Compare(keys.At(mid), target) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}
