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
	"iter"

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
	all := h.PostingMap.Iter()
	next, stop := iter.Pull2(all)

	return &AllowListIterator{
		allowList: al,
		next:      next,
		stop:      stop,
		len:       int(h.Centroids.GetMaxID()),
	}
}

type AllowListIterator struct {
	len       int
	next      func() (uint64, *PostingMetadata, bool)
	stop      func()
	allowList helpers.AllowList
}

func (i *AllowListIterator) Len() int {
	return i.len
}

func (i *AllowListIterator) Stop() {
	i.stop()
}

func (i *AllowListIterator) Next() (uint64, bool) {
	id, metadata, ok := i.next()
	al, isOurType := i.allowList.(*allowList)

	for ok {
		if i.contains(id, metadata, al, isOurType) {
			return id, true
		}
		id, metadata, ok = i.next()
	}

	return id, false
}

func (i *AllowListIterator) contains(id uint64, metadata *PostingMetadata, al *allowList, isOurType bool) bool {
	if isOurType && metadata != nil {
		return al.containsPosting(id, metadata)
	}
	return i.allowList.Contains(id)
}

type allowList struct {
	helpers.AllowList
	ctx              context.Context
	h                *HFresh
	wrappedIdVisited *visited.SparseSet
	idVisited        *visited.SparseSet
}

func (a *allowList) Contains(id uint64) bool {
	if a.idVisited.Visited(id) {
		return true
	}

	p, err := a.h.PostingMap.Get(a.ctx, id)
	if err != nil {
		return false
	}

	return a.containsPosting(id, p)
}

func (a *allowList) containsPosting(id uint64, p *PostingMetadata) bool {
	a.h.PostingMap.RLock(id)
	defer a.h.PostingMap.RUnlock(id)

	for vectorID := range p.Iter() {
		if !a.wrappedIdVisited.Visited(vectorID) && a.AllowList.Contains(vectorID) {
			a.wrappedIdVisited.Visit(vectorID)
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
