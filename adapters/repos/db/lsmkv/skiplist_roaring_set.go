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
	"unsafe"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

// roaringSetIndex is the memtable's ordered key -> BitmapLayer index for the
// RoaringSet strategy, implemented by both the red-black tree (locked reads,
// bitmaps merged in place at insert) and the skip list (lock-free reads over a
// per-key op log, merged at read). insert returns the bytes of index backing
// newly allocated by the call.
type roaringSetIndex interface {
	insert(key []byte, values roaringset.Insert) int
	get(key []byte) (roaringset.BitmapLayer, error)
	flattenInOrder() []*roaringset.BinarySearchNode
}

var roaringOpSize = int(unsafe.Sizeof(roaringset.Insert{}))

func newRoaringSetIndex(lockFree bool) roaringSetIndex {
	if lockFree {
		return newSkipListRoaringSet()
	}
	return &rbRoaringSetIndex{t: &roaringset.BinarySearchTree{}}
}

type rbRoaringSetIndex struct {
	t *roaringset.BinarySearchTree
}

func (r *rbRoaringSetIndex) insert(key []byte, values roaringset.Insert) int {
	r.t.Insert(key, values)
	return 0
}

func (r *rbRoaringSetIndex) get(key []byte) (roaringset.BitmapLayer, error) {
	return r.t.Get(key)
}

func (r *rbRoaringSetIndex) flattenInOrder() []*roaringset.BinarySearchNode {
	return r.t.FlattenInOrder()
}

// skipListRoaringSet stores each key's writes as an append-only op log and
// reduces it to a BitmapLayer on read, applying ops in insertion order with the
// same add/remove ordering the red-black tree applies in place — so both
// indexes resolve an add-then-delete (or delete-then-add) of one docID
// identically. Bitmaps cannot be merged in place here: sroar mutation is not
// safe against lock-free readers, immutable ops are.
type skipListRoaringSet struct {
	sl *skipList[roaringset.Insert]
}

func newSkipListRoaringSet() *skipListRoaringSet {
	return &skipListRoaringSet{sl: newSkipList[roaringset.Insert]()}
}

func (s *skipListRoaringSet) insert(key []byte, values roaringset.Insert) int {
	// copy the slices: the log outlives the call, and callers own their buffers
	// (the red-black tree copies into bitmaps at this point, too)
	op := roaringset.Insert{
		Additions: append([]uint64(nil), values.Additions...),
		Deletions: append([]uint64(nil), values.Deletions...),
	}
	slots := s.sl.insert(key, op)
	return slots*roaringOpSize + (len(op.Additions)+len(op.Deletions))*8
}

// mergeOps replays an op log into a fresh BitmapLayer. The four-step order per
// op mirrors roaringset.BinarySearchNode.insert: additions revive previously
// deleted entries, deletions cancel previous additions, and a deletion is
// always recorded so it propagates to older segments.
func mergeOps(ops []roaringset.Insert) roaringset.BitmapLayer {
	layer := roaringset.BitmapLayer{
		Additions: sroar.NewBitmap(),
		Deletions: sroar.NewBitmap(),
	}
	for _, op := range ops {
		for _, x := range op.Additions {
			layer.Deletions.Remove(x)
			layer.Additions.Set(x)
		}
		for _, x := range op.Deletions {
			layer.Additions.Remove(x)
			layer.Deletions.Set(x)
		}
	}
	return layer
}

// get returns freshly built bitmaps, safe for the caller to mutate.
func (s *skipListRoaringSet) get(key []byte) (roaringset.BitmapLayer, error) {
	ops, ok := s.sl.get(key)
	if !ok {
		return roaringset.BitmapLayer{}, lsmkv.NotFound
	}
	return mergeOps(ops), nil
}

func (s *skipListRoaringSet) flattenInOrder() []*roaringset.BinarySearchNode {
	var out []*roaringset.BinarySearchNode
	s.sl.forEach(func(key []byte, ops []roaringset.Insert) {
		layer := mergeOps(ops)
		out = append(out, &roaringset.BinarySearchNode{
			Key: key,
			// condensed like the red-black tree's flatten, so flushed segment
			// bitmaps have the same compact representation either way
			Value: roaringset.BitmapLayer{
				Additions: roaringset.Condense(layer.Additions),
				Deletions: roaringset.Condense(layer.Deletions),
			},
		})
	})
	return out
}
