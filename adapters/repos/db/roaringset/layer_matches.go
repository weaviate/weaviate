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

// LayerMatches is what one layer holds among a batch of query keys: At[j] is the
// position, in the batch, of the key whose state is Layers[j]. Positions ascend,
// and only keys the layer actually holds appear — a batch of 100,000 keys
// against a layer holding 10,000 yields at most 10,000 entries, usually far
// fewer.
//
// That sparseness is the point, and it is why this is not simply a layer per
// key. Reading a layer one key at a time costs a lock acquisition and a tree
// descent for each, and against an unflushed memtable most of them miss: the
// memtable is a delta, so a large filter asks it about far more keys than it
// has. Matching the whole batch in one pass leaves only the keys with something
// to say.
//
// The layers are copies, so they stay valid after whatever lock guarded the read
// is released.
type LayerMatches struct {
	At     []uint32
	Layers []BitmapLayer
}

// Len is the number of keys the layer held.
func (m LayerMatches) Len() int { return len(m.At) }
