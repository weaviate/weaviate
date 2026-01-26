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

package hfresh

import (
	"context"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
)

func (h *HFresh) wrapAllowList(ctx context.Context, allowList helpers.AllowList) helpers.AllowList {
	wrappedIdVisited := h.visitedPool.Borrow()
	idVisited := h.visitedPool.Borrow()
	return &hfAllowList{
		wrapped:          allowList,
		ctx:              ctx,
		h:                h,
		idVisited:        idVisited,
		wrappedIdVisited: wrappedIdVisited,
	}
}

func (h *HFresh) NewHFALIterator(allowList helpers.AllowList) helpers.AllowListIterator {
	return &HFALIterator{
		len:       int(h.Centroids.GetMaxID()),
		allowList: allowList,
	}
}

type HFALIterator struct {
	len       int
	current   uint64
	allowList helpers.AllowList
}

func (i *HFALIterator) Len() int {
	return i.len
}

func (i *HFALIterator) Next() (uint64, bool) {
	if i.current >= uint64(i.len) {
		return 0, false
	}
	for i.current < uint64(i.len) {
		if i.allowList.Contains(i.current) {
			i.current++
			return i.current - 1, true
		}
		i.current++
	}
	return 0, false
}

type hfAllowList struct {
	wrapped          helpers.AllowList
	ctx              context.Context
	h                *HFresh
	wrappedIdVisited visited.ListSet
	idVisited        visited.ListSet
}

func (a *hfAllowList) Contains(id uint64) bool {
	if a.idVisited.Visited(id) {
		return true
	}

	p, err := a.h.PostingMap.Get(a.ctx, id)
	if err != nil {
		return false
	}

	p.RLock()
	defer p.RUnlock()

	for _, metadata := range p.Iter() {
		/*valid, err := metadata.IsValid(a.ctx, a.h.VersionMap)
		if err != nil {
			continue
		}
		if !valid {
			continue
		}
		*/
		if !a.wrappedIdVisited.Visited(metadata.ID) && a.wrapped.Contains(metadata.ID) {
			a.wrappedIdVisited.Visit(metadata.ID)
			a.idVisited.Visit(id)
			return true
		}
	}
	return false
}

// DeepCopy implements [helpers.AllowList].
func (a *hfAllowList) DeepCopy() helpers.AllowList {
	panic("unimplemented")
}

// Insert implements [helpers.AllowList].
func (a *hfAllowList) Insert(ids ...uint64) {
	panic("unimplemented")
}

// IsEmpty implements [helpers.AllowList].
func (a *hfAllowList) IsEmpty() bool {
	panic("unimplemented")
}

// Iterator implements [helpers.AllowList].
func (a *hfAllowList) Iterator() helpers.AllowListIterator {
	return a.h.NewHFALIterator(a)
}

// Len implements [helpers.AllowList].
func (a *hfAllowList) Len() int {
	return int(a.h.Centroids.GetMaxID())
}

// LimitedIterator implements [helpers.AllowList].
func (a *hfAllowList) LimitedIterator(limit int) helpers.AllowListIterator {
	panic("unimplemented")
}

// Max implements [helpers.AllowList].
func (a *hfAllowList) Max() uint64 {
	panic("unimplemented")
}

// Min implements [helpers.AllowList].
func (a *hfAllowList) Min() uint64 {
	panic("unimplemented")
}

// Size implements [helpers.AllowList].
func (a *hfAllowList) Size() uint64 {
	panic("unimplemented")
}

// Slice implements [helpers.AllowList].
func (a *hfAllowList) Slice() []uint64 {
	panic("unimplemented")
}

// Truncate implements [helpers.AllowList].
func (a *hfAllowList) Truncate(uint64) helpers.AllowList {
	panic("unimplemented")
}

// WrapOnWrite implements [helpers.AllowList].
func (a *hfAllowList) WrapOnWrite() helpers.AllowList {
	panic("unimplemented")
}

func (a *hfAllowList) Close() {
	defer a.h.visitedPool.Return(a.wrappedIdVisited)
	defer a.h.visitedPool.Return(a.idVisited)
}
