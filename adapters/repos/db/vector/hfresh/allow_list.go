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
	all := h.PostingMap.cache.All() // snapshot of what is currently in cache
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

	// Cache the metadata if allowList is our concrete type
	if al, isOurType := i.allowList.(*allowList); isOurType && metadata != nil {
		al.cacheMetadata(id, metadata)
	}

	for ok && !i.allowList.Contains(id) {
		id, metadata, ok = i.next()
		// Cache the metadata for each iteration
		if al, isOurType := i.allowList.(*allowList); isOurType && metadata != nil {
			al.cacheMetadata(id, metadata)
		}
	}
	if !ok {
		return id, ok
	}

	return id, i.allowList.Contains(id)
}

type allowList struct {
	helpers.AllowList
	ctx              context.Context
	h                *HFresh
	wrappedIdVisited visited.ListSet
	idVisited        visited.ListSet
	// Cache to store PostingMetadata from iterator to avoid expensive Get calls
	cachedMetadata *PostingMetadata
	cachedID       uint64
}

func (a *allowList) cacheMetadata(id uint64, metadata *PostingMetadata) {
	a.cachedID = id
	a.cachedMetadata = metadata
}

func (a *allowList) Contains(id uint64) bool {
	if a.idVisited.Visited(id) {
		return true
	}

	var p *PostingMetadata
	var err error

	// Use cached metadata if available for this id
	if a.cachedID == id && a.cachedMetadata != nil {
		p = a.cachedMetadata
	} else {
		// Fall back to expensive Get only if not cached
		p, err = a.h.PostingMap.Get(a.ctx, id)
		if err != nil {
			return false
		}
	}

	p.RLock()
	defer p.RUnlock()

	for id := range p.Iter() {
		if !a.wrappedIdVisited.Visited(id) && a.AllowList.Contains(id) {
			a.wrappedIdVisited.Visit(id)
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
