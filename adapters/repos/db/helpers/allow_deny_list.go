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

package helpers

import (
	"github.com/weaviate/sroar"
)

// this was changed to be public to allow for accessing the underlying bitmap and intersecting it with other *sroar.Bitmap for faster keyword retrieval
// We should consider making this private again and adding a method to intersect two AllowLists, but at the same time, it would also make the interface bloated
// and add the burden of supporting this method in all (future, if any) implementations of AllowList
type BitmapAllowDenyList struct {
	Bm         *sroar.Bitmap
	release    func()
	isDenyList bool
	maxId      uint64
}

func NewAllowDenyListCloseableFromBitmap(bm *sroar.Bitmap, isDenyList bool, release func(), maxId uint64) AllowList {
	return &BitmapAllowDenyList{Bm: bm, release: release, isDenyList: isDenyList, maxId: maxId}
}

func NewAllowDenyListFromBitmap(bm *sroar.Bitmap, isDenyList bool, maxId uint64) AllowList {
	return NewAllowDenyListCloseableFromBitmap(bm, isDenyList, func() {}, maxId)
}

func NewAllowDenyListFromBitmapDeepCopy(bm *sroar.Bitmap, isDenyList bool, maxId uint64) AllowList {
	return NewAllowDenyListFromBitmap(bm.Clone(), isDenyList, maxId)
}

func (al *BitmapAllowDenyList) Close() {
	al.release()
}

func (al *BitmapAllowDenyList) Insert(ids ...uint64) {
	if al.isDenyList {
		for _, id := range ids {
			al.Bm.Remove(id)
		}
		return
	}
	al.Bm.SetMany(ids)
}

func (al *BitmapAllowDenyList) Contains(id uint64) bool {
	// XOR logic: if it's a deny list, we want to return true if the ID is NOT in the bitmap, and false if it is. If it's an allow list, we want to return true if the ID is in the bitmap, and false if it is not.
	return al.Bm.Contains(id) != al.isDenyList
}

func (al *BitmapAllowDenyList) DeepCopy() AllowList {
	return NewAllowDenyListFromBitmapDeepCopy(al.Bm, al.isDenyList, al.maxId)
}

func (al *BitmapAllowDenyList) WrapOnWrite() AllowList {
	return newWrappedAllowList(al)
}

func (al *BitmapAllowDenyList) Slice() []uint64 {
	if al.isDenyList {
		result := make([]uint64, 0, al.maxId)
		for id := 0; id <= int(al.maxId); id++ {
			if !al.Bm.Contains(uint64(id)) {
				result = append(result, uint64(id))
			}
		}
		return result
	}
	return al.Bm.ToArray()
}

func (al *BitmapAllowDenyList) IsEmpty() bool {
	if al.isDenyList {
		return al.Bm.GetCardinality() == int(al.maxId+1)
	}
	return al.Bm.IsEmpty()
}

func (al *BitmapAllowDenyList) Len() int {
	if al.isDenyList {
		return int(al.maxId+1) - al.Bm.GetCardinality()
	}
	return al.Cardinality()
}

func (al *BitmapAllowDenyList) Cardinality() int {
	return al.Bm.GetCardinality()
}

func (al *BitmapAllowDenyList) Min() uint64 {
	if al.isDenyList {
		alMin := al.Bm.Minimum()
		if alMin > 0 {
			return 0
		}
		for id := uint64(0); id <= al.maxId; id++ {
			if !al.Bm.Contains(id) {
				return id
			}
		}
		return 0
	}
	return al.Bm.Minimum()
}

func (al *BitmapAllowDenyList) Max() uint64 {
	if al.isDenyList {
		if al.Bm.Maximum() != al.maxId {
			return al.maxId
		}
		for id := al.maxId - 1; id > 0; id-- {
			if !al.Bm.Contains(id) {
				return id
			}
		}
		return 0
	}
	return al.Bm.Maximum()
}

func (al *BitmapAllowDenyList) Size() uint64 {
	return uint64(1.5 * float64(len(al.Bm.ToBuffer())))
}

func (al *BitmapAllowDenyList) Truncate(upTo uint64) AllowList {
	card := al.Bm.GetCardinality()
	if upTo < uint64(card) {
		al.Bm.RemoveRange(upTo, uint64(al.Bm.GetCardinality()+1))
	}
	return al
}

func (al *BitmapAllowDenyList) Iterator() AllowListIterator {
	// if it's a deny list, we need to invert it to get the actual doc ids to iterate over
	return al.LimitedIterator(0)
}

func (al *BitmapAllowDenyList) LimitedIterator(limit int) AllowListIterator {
	// if it's a deny list, we need to invert it to get the actual doc ids to iterate over
	if al.isDenyList {
		return newBitmapAllowDenyListIterator(al.Bm, limit, al.maxId+1, al.Bm.GetCardinality())
	}
	return newBitmapAllowListIterator(al.Bm, limit)
}

func (al *BitmapAllowDenyList) IsDenyList() bool {
	return al.isDenyList
}

type bitmapAllowDenyListIterator struct {
	universeSize    uint64
	deny            *sroar.Bitmap
	denyCardinality int
	limit           int
	itCount         int
	index           uint64
}

func newBitmapAllowDenyListIterator(deny *sroar.Bitmap, limit int, universeSize uint64, denyCardinality int) AllowListIterator {
	return &bitmapAllowDenyListIterator{
		universeSize:    universeSize,
		deny:            deny,
		denyCardinality: denyCardinality,
		limit:           limit,
	}
}

func (i *bitmapAllowDenyListIterator) Next() (uint64, bool) {
	if i.limit > 0 && i.itCount >= i.limit {
		return 0, false
	}
	for i.index < i.universeSize {
		id := i.index
		i.index++
		if !i.deny.Contains(id) {
			i.itCount++
			return id, true
		}
	}
	return 0, false
}

func (i *bitmapAllowDenyListIterator) Len() int {
	return int(i.universeSize) - i.denyCardinality
}
