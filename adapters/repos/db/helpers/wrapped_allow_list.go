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

type wrappedAllowList struct {
	wAllowList AllowList
	allowList  AllowList
}

func newWrappedAllowList(al AllowList) AllowList {
	return &wrappedAllowList{
		wAllowList: al,
	}
}

func (al *wrappedAllowList) Insert(ids ...uint64) {
	fids := make([]uint64, 0, len(ids))

	for _, id := range ids {
		if al.wAllowList.Contains(id) {
			continue
		}

		fids = append(fids, id)
	}

	if len(fids) == 0 {
		return
	}

	if al.allowList == nil {
		al.allowList = NewAllowList()
	}

	al.allowList.Insert(fids...)
}

func (al *wrappedAllowList) Contains(id uint64) bool {
	if al.allowList != nil && al.allowList.Contains(id) {
		return true
	}
	return al.wAllowList.Contains(id)
}

func (al *wrappedAllowList) DeepCopy() AllowList {
	var innerAllowListCopy AllowList

	if al.allowList != nil {
		innerAllowListCopy = al.allowList.DeepCopy()
	}

	return &wrappedAllowList{
		wAllowList: al.wAllowList.DeepCopy(),
		allowList:  innerAllowListCopy,
	}
}

func (al *wrappedAllowList) WrapCopy() AllowList {
	return newWrappedAllowList(al)
}

func (al *wrappedAllowList) Slice() []uint64 {
	var allowListSlice []uint64

	if al.allowList != nil {
		allowListSlice = al.allowList.Slice()
	}

	return append(al.wAllowList.Slice(), allowListSlice...)
}

func (al *wrappedAllowList) IsEmpty() bool {
	return (al.allowList == nil || al.allowList.IsEmpty()) && al.wAllowList.IsEmpty()
}

func (al *wrappedAllowList) Len() int {
	var allowListLen int

	if al.allowList != nil {
		allowListLen = al.allowList.Len()
	}

	return allowListLen + al.wAllowList.Len()
}

func (al *wrappedAllowList) Min() uint64 {
	var min uint64

	if al.allowList != nil {
		min = al.allowList.Min()
	}

	wmin := al.wAllowList.Min()

	if min <= wmin {
		return min
	}

	return wmin
}

func (al *wrappedAllowList) Max() uint64 {
	var max uint64

	if al.allowList != nil {
		max = al.allowList.Max()
	}

	wmax := al.wAllowList.Max()

	if max >= wmax {
		return max
	}

	return wmax
}

func (al *wrappedAllowList) Size() uint64 {
	var allowListSize uint64

	if al.allowList != nil {
		allowListSize = al.allowList.Size()
	}

	return allowListSize + al.wAllowList.Size()
}

func (al *wrappedAllowList) Truncate(upTo uint64) AllowList {
	if al.allowList != nil {
		al.allowList = al.allowList.Truncate(upTo)
	}

	al.wAllowList = al.wAllowList.Truncate(upTo)
	return al
}

func (al *wrappedAllowList) Iterator() AllowListIterator {
	return al.LimitedIterator(0)
}

func (al *wrappedAllowList) LimitedIterator(limit int) AllowListIterator {
	if al.allowList == nil {
		return al.wAllowList.LimitedIterator(limit)
	}

	return newComposedAllowListIterator(
		al.allowList.LimitedIterator(limit),
		al.wAllowList.LimitedIterator(limit),
	)
}

type composedAllowListIterator struct {
	it1 AllowListIterator
	it2 AllowListIterator
}

func newComposedAllowListIterator(it1, it2 AllowListIterator) AllowListIterator {
	return &composedAllowListIterator{
		it1: it1,
		it2: it2,
	}
}

func (i *composedAllowListIterator) Next() (uint64, bool) {
	id, ok := i.it1.Next()
	if ok {
		return id, ok
	}

	return i.it2.Next()
}

func (i *composedAllowListIterator) Len() int {
	return i.it1.Len() + i.it2.Len()
}
