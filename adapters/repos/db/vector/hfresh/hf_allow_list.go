package hfresh

import (
	"context"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
)

func (h *HFresh) wrapAllowList(ctx context.Context, allowList helpers.AllowList) helpers.AllowList {
	wrappedIdVisited := h.visitedPool.Borrow()
	defer h.visitedPool.Return(wrappedIdVisited)
	idVisited := h.visitedPool.Borrow()
	defer h.visitedPool.Return(idVisited)
	return &HfAllowList{
		wrapped:          allowList,
		ctx:              ctx,
		h:                h,
		idVisited:        idVisited,
		wrappedIdVisited: wrappedIdVisited,
		Culprit:          make([]uint64, h.Centroids.GetMaxID()),
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

type HfAllowList struct {
	wrapped          helpers.AllowList
	ctx              context.Context
	h                *HFresh
	wrappedIdVisited visited.ListSet
	idVisited        visited.ListSet
	Culprit          []uint64
}

func (a *HfAllowList) Contains(id uint64) bool {
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
			a.Culprit[id] = metadata.ID
			return true
		}
	}
	return false
}

// DeepCopy implements [helpers.AllowList].
func (a *HfAllowList) DeepCopy() helpers.AllowList {
	panic("unimplemented")
}

// Insert implements [helpers.AllowList].
func (a *HfAllowList) Insert(ids ...uint64) {
	panic("unimplemented")
}

// IsEmpty implements [helpers.AllowList].
func (a *HfAllowList) IsEmpty() bool {
	panic("unimplemented")
}

// Iterator implements [helpers.AllowList].
func (a *HfAllowList) Iterator() helpers.AllowListIterator {
	return a.h.NewHFALIterator(a)
}

// Len implements [helpers.AllowList].
func (a *HfAllowList) Len() int {
	return int(a.h.Centroids.GetMaxID())
}

// LimitedIterator implements [helpers.AllowList].
func (a *HfAllowList) LimitedIterator(limit int) helpers.AllowListIterator {
	panic("unimplemented")
}

// Max implements [helpers.AllowList].
func (a *HfAllowList) Max() uint64 {
	panic("unimplemented")
}

// Min implements [helpers.AllowList].
func (a *HfAllowList) Min() uint64 {
	panic("unimplemented")
}

// Size implements [helpers.AllowList].
func (a *HfAllowList) Size() uint64 {
	panic("unimplemented")
}

// Slice implements [helpers.AllowList].
func (a *HfAllowList) Slice() []uint64 {
	panic("unimplemented")
}

// Truncate implements [helpers.AllowList].
func (a *HfAllowList) Truncate(uint64) helpers.AllowList {
	panic("unimplemented")
}

// WrapOnWrite implements [helpers.AllowList].
func (a *HfAllowList) WrapOnWrite() helpers.AllowList {
	panic("unimplemented")
}

func (a *HfAllowList) Close() {}
