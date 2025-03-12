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

func (al *wrappedAllowList) Close() {
	al.wAllowList.Close()
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

func (al *wrappedAllowList) WrapOnWrite() AllowList {
	return newWrappedAllowList(al)
}

func (al *wrappedAllowList) Slice() []uint64 {
	if al.allowList == nil {
		return al.wAllowList.Slice()
	}

	return append(al.wAllowList.Slice(), al.allowList.Slice()...)
}

func (al *wrappedAllowList) IsEmpty() bool {
	return (al.allowList == nil || al.allowList.IsEmpty()) && al.wAllowList.IsEmpty()
}

func (al *wrappedAllowList) Len() int {
	if al.allowList == nil {
		return al.wAllowList.Len()
	}

	return al.allowList.Len() + al.wAllowList.Len()
}

func (al *wrappedAllowList) Min() uint64 {
	if al.allowList == nil {
		return al.wAllowList.Min()
	}

	min := al.allowList.Min()
	wmin := al.wAllowList.Min()

	if min <= wmin {
		return min
	}

	return wmin
}

func (al *wrappedAllowList) Max() uint64 {
	if al.allowList == nil {
		return al.wAllowList.Max()
	}

	max := al.allowList.Max()
	wmax := al.wAllowList.Max()

	if max >= wmax {
		return max
	}

	return wmax
}

func (al *wrappedAllowList) Size() uint64 {
	if al.allowList == nil {
		return al.wAllowList.Size()
	}

	return al.allowList.Size() + al.wAllowList.Size()
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
		limit,
	)
}

type composedAllowListIterator struct {
	it1     AllowListIterator
	it2     AllowListIterator
	limit   int
	itCount int
}

func newComposedAllowListIterator(it1, it2 AllowListIterator, limit int) AllowListIterator {
	return &composedAllowListIterator{
		it1:   it1,
		it2:   it2,
		limit: limit,
	}
}

func (i *composedAllowListIterator) Next() (uint64, bool) {
	if i.limit > 0 && i.itCount >= i.limit {
		return 0, false
	}

	id, ok := i.it1.Next()
	if ok {
		i.itCount++
		return id, ok
	}

	i.itCount++
	return i.it2.Next()
}

func (i *composedAllowListIterator) Len() int {
	return i.it1.Len() + i.it2.Len()
}
