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

func (h *HFresh) wrapAllowList(ctx context.Context, al helpers.AllowList) helpers.AllowList {
	return &allowList{
		AllowList:        al,
		ctx:              ctx,
		h:                h,
		idVisited:        h.visitedPool.Borrow(),
		wrappedIdVisited: h.visitedPool.Borrow(),
	}
}

func (h *HFresh) NewAllowListIterator(al helpers.AllowList) helpers.AllowListIterator {
	return &AllowListIterator{
		len:       int(h.Centroids.GetMaxID()),
		allowList: al,
	}
}

type AllowListIterator struct {
	len       int
	current   uint64
	allowList helpers.AllowList
}

func (i *AllowListIterator) Len() int {
	return i.len
}

func (i *AllowListIterator) Next() (uint64, bool) {
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

type allowList struct {
	helpers.AllowList
	ctx              context.Context
	h                *HFresh
	wrappedIdVisited visited.ListSet
	idVisited        visited.ListSet
}

func (a *allowList) Contains(id uint64) bool {
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
		if !a.wrappedIdVisited.Visited(metadata.ID) && a.AllowList.Contains(metadata.ID) {
			a.wrappedIdVisited.Visit(metadata.ID)
			a.idVisited.Visit(id)
			return true
		}
	}
	return false
}

// Iterator implements [helpers.AllowList].
func (a *allowList) Iterator() helpers.AllowListIterator {
	return a.h.NewAllowListIterator(a)
}

// Len implements [helpers.AllowList].
func (a *allowList) Len() int {
	return int(a.h.Centroids.GetMaxID())
}

func (a *allowList) Close() {
	a.h.visitedPool.Return(a.wrappedIdVisited)
	a.h.visitedPool.Return(a.idVisited)
}
