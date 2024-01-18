//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package helpers

import (
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
)

type AllowList interface {
	Insert(ids ...uint64)
	Contains(id uint64) bool
	DeepCopy() AllowList
	Slice() []uint64

	IsEmpty() bool
	Len() int
	Min() uint64
	Max() uint64
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

func (al *bitmapAllowList) IsEmpty() bool {
	return al.bm.IsEmpty()
}

func (al *bitmapAllowList) Len() int {
	return al.bm.GetCardinality()
}

func (al *bitmapAllowList) Min() uint64 {
	return al.bm.Minimum()
}

func (al *bitmapAllowList) Max() uint64 {
	return al.bm.Maximum()
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
