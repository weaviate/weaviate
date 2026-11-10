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

package tachiomfresh

import (
	"math/bits"
	"sync"
)

// docSet is the set of documents the index holds, one bit per doc id.
// ContainsDoc and Iterate answer from it. This is not hfresh's rule.
// hfresh answers ContainsDoc from its version map, which reports the
// implicit first version, not deleted, for any id it has never seen
// (VersionMap.loadInto maps a missing entry to v1), so every id looks
// indexed there. A document here is a set of posting entries, with no map
// from the document to them, so the bit is the only record that
// the document was indexed. The insert sets the bit once every token is
// in. The delete clears it. restore rebuilds the set from the postings,
// which it decodes anyway. Cost: one bit per doc id, 1 MB per 8 million
// documents.
type docSet struct {
	mu    sync.RWMutex
	words []uint64
}

func (d *docSet) add(id uint64) {
	word, bit := id/64, id%64
	d.mu.Lock()
	if uint64(len(d.words)) <= word {
		d.words = append(d.words, make([]uint64, word+1-uint64(len(d.words)))...)
	}
	d.words[word] |= 1 << bit
	d.mu.Unlock()
}

func (d *docSet) remove(id uint64) {
	word, bit := id/64, id%64
	d.mu.Lock()
	if uint64(len(d.words)) > word {
		d.words[word] &^= 1 << bit
	}
	d.mu.Unlock()
}

func (d *docSet) contains(id uint64) bool {
	word, bit := id/64, id%64
	d.mu.RLock()
	defer d.mu.RUnlock()
	return uint64(len(d.words)) > word && d.words[word]&(1<<bit) != 0
}

// iterate calls fn with every doc id in the set, in ascending order, until
// fn returns false. It walks a copy taken under the read lock, so fn may
// change the set (the shard's repair deletes documents while it iterates).
func (d *docSet) iterate(fn func(id uint64) bool) {
	d.mu.RLock()
	words := make([]uint64, len(d.words))
	copy(words, d.words)
	d.mu.RUnlock()
	for i, w := range words {
		for w != 0 {
			bit := bits.TrailingZeros64(w)
			if !fn(uint64(i)*64 + uint64(bit)) {
				return
			}
			w &^= 1 << bit
		}
	}
}

// count is the number of documents in the set.
func (d *docSet) count() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var n int
	for _, w := range d.words {
		n += bits.OnesCount64(w)
	}
	return n
}
