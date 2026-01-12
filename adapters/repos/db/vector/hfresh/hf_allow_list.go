package hfresh

import (
	"context"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
)

func (h *HFresh) wrapAllowList(ctx context.Context, allowList helpers.AllowList) helpers.AllowList {
	return &hfAllowList{
		wrapped: allowList,
		ctx:     ctx,
		h:       h,
		visited: visited.NewList(1_000_000),
	}
}

type hfAllowList struct {
	wrapped helpers.AllowList
	ctx     context.Context
	h       *HFresh
	visited visited.ListSet
}

func (a *hfAllowList) Contains(id uint64) bool {
	p, err := a.h.PostingMap.Get(a.ctx, id)
	if err != nil {
		return true
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
		}*/
		//ToDo: vid is valid and not deleted...
		if !a.visited.Visited(metadata.ID) && a.wrapped.Contains(metadata.ID) {
			a.visited.Visit(metadata.ID)
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
	panic("unimplemented")
}

// Len implements [helpers.AllowList].
func (a *hfAllowList) Len() int {
	return 1_000_000_000
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

func (a *hfAllowList) Close() {}
