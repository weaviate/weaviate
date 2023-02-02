//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package helpers

import (
	"github.com/dgraph-io/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
)

type AllowList interface {
	Insert(ids ...uint64)
	Contains(id uint64) bool
	DeepCopy() AllowList
	Slice() []uint64
	Len() int
	IsEmpty() bool
	Size() uint64
	Iterator() AllowListIterator
	LimitedIterator(limit int) AllowListIterator
}

type AllowListIterator interface {
	Next() (uint64, bool)
	Len() int
}

func NewAllowList(ids ...uint64) AllowList {
	return NewAllowListFromBitmap(roaringset.NewBitmap(ids...))
}

func NewAllowListFromBitmap(bm *sroar.Bitmap) AllowList {
	return &bitmapAllowList{bm: bm}
}

func NewAllowListFromBitmapDeepCopy(bm *sroar.Bitmap) AllowList {
	return NewAllowListFromBitmap(bm.Clone())
}

type bitmapAllowList struct {
	bm *sroar.Bitmap
}

func (al *bitmapAllowList) Insert(ids ...uint64) {
	al.bm.SetMany(ids)
}

func (al *bitmapAllowList) Contains(id uint64) bool {
	return al.bm.Contains(id)
}

func (al *bitmapAllowList) DeepCopy() AllowList {
	return NewAllowListFromBitmapDeepCopy(al.bm)
}

func (al *bitmapAllowList) Slice() []uint64 {
	return al.bm.ToArray()
}

func (al *bitmapAllowList) Len() int {
	return al.bm.GetCardinality()
}

func (al *bitmapAllowList) IsEmpty() bool {
	return al.bm.IsEmpty()
}

func (al *bitmapAllowList) Size() uint64 {
	// TODO provide better size estimation
	return uint64(1.5 * float64(len(al.bm.ToBuffer())))
}

func (al *bitmapAllowList) Iterator() AllowListIterator {
	return al.LimitedIterator(0)
}

func (al *bitmapAllowList) LimitedIterator(limit int) AllowListIterator {
	return newBitmapAllowListIterator(al.bm, limit)
}

type bitmapAllowListIterator struct {
	len     int
	counter int
	it      *sroar.Iterator
}

func newBitmapAllowListIterator(bm *sroar.Bitmap, limit int) AllowListIterator {
	len := bm.GetCardinality()
	if limit > 0 && limit < len {
		len = limit
	}

	return &bitmapAllowListIterator{
		len:     len,
		counter: 0,
		it:      bm.NewIterator(),
	}
}

func (i *bitmapAllowListIterator) Next() (uint64, bool) {
	if i.counter >= i.len {
		return 0, false
	}
	i.counter++
	return i.it.Next(), true
}

func (i *bitmapAllowListIterator) Len() int {
	return i.len
}

// // AllowList groups a list of possible indexIDs to be passed to a secondary
// // index. The secondary index must make sure that it only returns result
// // present on the AllowList
// type AllowList map[uint64]struct{}

// // Inserting and reading is not thread-safe. However, if inserting has
// // completed, and the list can be considered read-only, it is safe to read from
// // it concurrently
// func (al AllowList) Insert(id uint64) {
// 	// no need to check if it's already present, simply overwrite
// 	al[id] = struct{}{}
// }

// // Contains is not thread-safe if the list is still being filled. However, if
// // you can guarantee that the list is no longer being inserted into and it
// // effectively becomes read-only, you can safely read concurrently
// func (al AllowList) Contains(id uint64) bool {
// 	_, ok := al[id]
// 	return ok
// }

// func (al AllowList) DeepCopy() AllowList {
// 	out := AllowList{}

// 	for id := range al {
// 		out[id] = struct{}{}
// 	}

// 	return out
// }

// func (al AllowList) Slice() []uint64 {
// 	out := make([]uint64, len(al))
// 	i := 0
// 	for id := range al {
// 		out[i] = id
// 		i++
// 	}

// 	return out
// }
