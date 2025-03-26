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
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

type AllowList interface {
	Close()

	Insert(ids ...uint64)
	Contains(id uint64) bool
	DeepCopy() AllowList
	WrapOnWrite() AllowList
	Slice() []uint64

	IsEmpty() bool
	Len() int
	Min() uint64
	Max() uint64
	Size() uint64
	Truncate(uint64) AllowList

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
	return NewAllowListCloseableFromBitmap(bm, func() {})
}

func NewAllowListCloseableFromBitmap(bm *sroar.Bitmap, release func()) AllowList {
	return &BitmapAllowList{Bm: bm, release: release}
}

func NewAllowListFromBitmapDeepCopy(bm *sroar.Bitmap) AllowList {
	return NewAllowListFromBitmap(bm.Clone())
}

// this was changed to be public to allow for accessing the underlying bitmap and intersecting it with other *sroar.Bitmap for faster keyword retrieval
// We should consider making this private again and adding a method to intersect two AllowLists, but at the same time, it would also make the interface bloated
// and add the burden of supporting this method in all (future, if any) implementations of AllowList
type BitmapAllowList struct {
	Bm      *sroar.Bitmap
	release func()
}

func (al *BitmapAllowList) Close() {
	al.release()
}

func (al *BitmapAllowList) Insert(ids ...uint64) {
	al.Bm.SetMany(ids)
}

func (al *BitmapAllowList) Contains(id uint64) bool {
	return al.Bm.Contains(id)
}

func (al *BitmapAllowList) DeepCopy() AllowList {
	return NewAllowListFromBitmapDeepCopy(al.Bm)
}

func (al *BitmapAllowList) WrapOnWrite() AllowList {
	return newWrappedAllowList(al)
}

func (al *BitmapAllowList) Slice() []uint64 {
	return al.Bm.ToArray()
}

func (al *BitmapAllowList) IsEmpty() bool {
	return al.Bm.IsEmpty()
}

func (al *BitmapAllowList) Len() int {
	return al.Bm.GetCardinality()
}

func (al *BitmapAllowList) Min() uint64 {
	return al.Bm.Minimum()
}

func (al *BitmapAllowList) Max() uint64 {
	return al.Bm.Maximum()
}

func (al *BitmapAllowList) Size() uint64 {
	// TODO provide better size estimation
	return uint64(1.5 * float64(len(al.Bm.ToBuffer())))
}

func (al *BitmapAllowList) Truncate(upTo uint64) AllowList {
	card := al.Bm.GetCardinality()
	if upTo < uint64(card) {
		al.Bm.RemoveRange(upTo, uint64(al.Bm.GetCardinality()+1))
	}
	return al
}

func (al *BitmapAllowList) Iterator() AllowListIterator {
	return al.LimitedIterator(0)
}

func (al *BitmapAllowList) LimitedIterator(limit int) AllowListIterator {
	return newBitmapAllowListIterator(al.Bm, limit)
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
