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
	"fmt"
	"sync"

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
	Cardinality() int

	Min() uint64
	Max() uint64
	Size() uint64
	Truncate(uint64) AllowList

	Iterator() AllowListIterator
	LimitedIterator(limit int) AllowListIterator

	IsDenyList() bool
}

type AllowListIterator interface {
	Next() (uint64, bool)
	Len() int
}

func NewAllowList(ids ...uint64) AllowList {
	return NewAllowListFromBitmap(roaringset.NewBitmap(ids...))
}

func NewAllowListFromBitmap(bm *sroar.Bitmap) AllowList {
	return NewAllowListCloseableFromBitmap(bm, false, func() {}, nil)
}

func NewAllowListCloseableFromBitmap(bm *sroar.Bitmap, isDenyList bool, release func(), bitmapFactory *roaringset.BitmapFactory) AllowList {
	return &BitmapAllowList{Bm: bm, release: release, isDenyList: isDenyList, bitmapFactory: bitmapFactory}
}

func NewAllowListFromBitmapDeepCopy(bm *sroar.Bitmap) AllowList {
	return NewAllowListFromBitmap(bm.Clone())
}

// this was changed to be public to allow for accessing the underlying bitmap and intersecting it with other *sroar.Bitmap for faster keyword retrieval
// We should consider making this private again and adding a method to intersect two AllowLists, but at the same time, it would also make the interface bloated
// and add the burden of supporting this method in all (future, if any) implementations of AllowList
type BitmapAllowList struct {
	sync.Mutex
	Bm            *sroar.Bitmap
	universe      *sroar.Bitmap
	release       func()
	isDenyList    bool
	bitmapFactory *roaringset.BitmapFactory
}

func (al *BitmapAllowList) Close() {
	al.release()
}

func (al *BitmapAllowList) Insert(ids ...uint64) {
	if al.isDenyList {
		for _, id := range ids {
			al.Bm.Remove(id)
		}
		return
	}
	al.Bm.SetMany(ids)
}

func (al *BitmapAllowList) Contains(id uint64) bool {
	// XOR logic: if it's a deny list, we want to return true if the ID is NOT in the bitmap, and false if it is. If it's an allow list, we want to return true if the ID is in the bitmap, and false if it is not.
	return al.Bm.Contains(id) != al.isDenyList
}

func (al *BitmapAllowList) DeepCopy() AllowList {
	return NewAllowListFromBitmapDeepCopy(al.Bm)
}

func (al *BitmapAllowList) WrapOnWrite() AllowList {
	return newWrappedAllowList(al)
}

func (al *BitmapAllowList) Slice() []uint64 {
	if al.isDenyList {
		err := al.invert()
		if err != nil {
			panic(fmt.Sprintf("failed to invert bitmap allow list: %v", err))
		}
		cardinality := al.universe.GetCardinality() - al.Bm.GetCardinality()
		result := make([]uint64, 0, cardinality)
		it := al.universe.NewIterator()
		for i := 0; i < al.universe.GetCardinality(); i++ {
			id := it.Next()
			if !al.Bm.Contains(id) {
				result = append(result, id)
			}
		}
		return result
	}
	return al.Bm.ToArray()
}

func (al *BitmapAllowList) IsEmpty() bool {
	if al.isDenyList {
		err := al.invert()
		if err != nil {
			panic(fmt.Sprintf("failed to invert bitmap allow list: %v", err))
		}
		return al.universe.GetCardinality() == al.Bm.GetCardinality()
	}
	return al.Bm.IsEmpty()
}

func (al *BitmapAllowList) Len() int {
	if al.isDenyList {
		err := al.invert()
		if err != nil {
			panic(fmt.Sprintf("failed to invert bitmap allow list: %v", err))
		}
		return int(al.universe.GetCardinality() - al.Bm.GetCardinality())
	}
	return al.Cardinality()
}

func (al *BitmapAllowList) Cardinality() int {
	return al.Bm.GetCardinality()
}

func (al *BitmapAllowList) Min() uint64 {
	if al.isDenyList {
		err := al.invert()
		if err != nil {
			panic(fmt.Sprintf("failed to invert bitmap allow list: %v", err))
		}
		for i := 0; i < al.universe.GetCardinality(); i++ {
			id := al.universe.NewIterator().Next()
			if !al.Bm.Contains(id) {
				return id
			}
		}
	}
	return al.Bm.Minimum()
}

func (al *BitmapAllowList) Max() uint64 {
	if al.isDenyList {
		err := al.invert()
		if err != nil {
			panic(fmt.Sprintf("failed to invert bitmap allow list: %v", err))
		}
		for i := al.universe.GetCardinality() - 1; i >= 0; i-- {
			id, err := al.universe.Select(uint64(i))
			if err != nil {
				panic(fmt.Sprintf("failed to select id from universe bitmap: %v", err))
			}
			if !al.Bm.Contains(id) {
				return id
			}
		}
	}
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
	// if it's a deny list, we need to invert it to get the actual doc ids to iterate over
	if al.isDenyList {
		err := al.invert()
		if err != nil {
			panic(fmt.Sprintf("failed to invert bitmap allow list: %v", err))
		}
		return newCombinedUniverseDenyListIterator(newBitmapAllowListIterator(al.universe, 0), al.Bm, 0, al.universe.GetCardinality(), al.Bm.GetCardinality())
	}
	return al.LimitedIterator(0)
}

func (al *BitmapAllowList) LimitedIterator(limit int) AllowListIterator {
	// if it's a deny list, we need to invert it to get the actual doc ids to iterate over
	if al.isDenyList {
		err := al.invert()
		if err != nil {
			panic(fmt.Sprintf("failed to invert bitmap allow list: %v", err))
		}
		return newCombinedUniverseDenyListIterator(newBitmapAllowListIterator(al.universe, 0), al.Bm, limit, al.universe.GetCardinality(), al.Bm.GetCardinality())
	}
	return newBitmapAllowListIterator(al.Bm, limit)
}

func (al *BitmapAllowList) IsDenyList() bool {
	return al.isDenyList
}

func (al *BitmapAllowList) invert() error {
	al.Lock()
	defer al.Unlock()
	if al.universe != nil {
		return nil
	}
	if al.isDenyList && al.bitmapFactory == nil {
		return fmt.Errorf("bitmap factory is not set")
	}
	if al.isDenyList && al.bitmapFactory != nil {
		var release, oldRelease func()
		al.universe, release = al.bitmapFactory.GetBitmap()
		oldRelease = al.release
		al.release = func() {
			release()
			oldRelease()
		}
	}
	return nil
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

type combinedUniverseDenyListIterator struct {
	universe        AllowListIterator
	universeLen     int
	deny            *sroar.Bitmap
	denyCardinality int
	limit           int
	itCount         int
}

func newCombinedUniverseDenyListIterator(universe AllowListIterator, deny *sroar.Bitmap, limit int, universeLen int, denyCardinality int) AllowListIterator {
	return &combinedUniverseDenyListIterator{
		universe:        universe,
		universeLen:     universeLen,
		deny:            deny,
		denyCardinality: denyCardinality,
		limit:           limit,
	}
}

func (i *combinedUniverseDenyListIterator) Next() (uint64, bool) {
	if i.limit > 0 && i.itCount >= i.limit {
		return 0, false
	}
	for {
		val, ok := i.universe.Next()
		if !ok {
			return 0, false
		}
		if !i.deny.Contains(val) {
			i.itCount++
			if i.limit > 0 && i.itCount >= i.limit {
				return 0, false
			}
			return val, true
		}
	}
}

func (i *combinedUniverseDenyListIterator) Len() int {
	return i.universeLen - i.denyCardinality
}
