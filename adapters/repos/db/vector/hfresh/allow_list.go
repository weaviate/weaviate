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
	"encoding/binary"
	"iter"
	"sync"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
)

// wrapAllowList bridges a docID allowlist into the posting domain for the
// centroid search: Contains(postingID) answers whether the posting holds at
// least one allowed vector.
//
// The check is PURE — probing a posting never affects the answer for
// another one. It used to "claim" the first passing vector for the first
// posting probed, so that replica postings would not consume probe budget;
// but claims followed probe order (posting-id order on the flat path,
// traversal order under ACORN), so a far posting could claim a vector, miss
// the budget cut, and leave the near replica excluded — an allowlisted
// vector became unreachable depending only on probe order. Replica repeats
// across selected postings are instead absorbed by the posting scan's
// visited set, which measures within noise of claim-based dedup on the 10M
// benchmark while keeping this check stateless and order-independent.
func (h *HFresh) wrapAllowList(_ context.Context, al helpers.AllowList) helpers.AllowList {
	return &allowList{
		AllowList:       al,
		h:               h,
		allowedPostings: h.visitedPool.Borrow(),
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
		return al.containsAllowedMember(id, metadata)
	}
	return i.allowList.Contains(id)
}

// Avoid caching small postings, where map maintenance can cost more than rescanning.
const minCachedPostingMembers = 256

const maxPooledRejectedPostings = 4096

var rejectedPostingsPool = sync.Pool{
	New: func() any { return make(map[*PostingMetadata][5]byte) },
}

type allowList struct {
	helpers.AllowList
	h *HFresh
	// allowedPostings memoizes postings known to contain at least one
	// allowed vector (the answer is stable within a query, so re-probes
	// skip the member scan)
	allowedPostings *visited.SparseSet

	// A miss is valid only for this metadata and its packed header: appends
	// change the count or encoding, while rewrites replace the metadata pointer.
	rejectedPostings map[*PostingMetadata][5]byte
	// Flat centroid search makes one iterator pass, with no random probes.
	// Only start memoizing misses when Contains is used.
	cacheRejected bool
}

// Contains reports whether the posting holds at least one allowed vector.
func (a *allowList) Contains(id uint64) bool {
	a.cacheRejected = true
	if a.allowedPostings.Visited(id) {
		return true
	}

	p, err := a.h.PostingMap.Get(context.Background(), id)
	if err != nil {
		return false
	}

	return a.containsAllowedMember(id, p)
}

func (a *allowList) containsAllowedMember(id uint64, p *PostingMetadata) bool {
	if a.allowedPostings.Visited(id) {
		return true
	}

	a.h.PostingMap.RLock(id)
	defer a.h.PostingMap.RUnlock(id)

	data := p.PackedPostingMetadata
	if len(data) < 5 {
		return false
	}
	count := binary.LittleEndian.Uint32(data[1:5])
	cacheRejected := a.cacheRejected && count >= minCachedPostingMembers
	var header [5]byte
	if cacheRejected {
		header = [5]byte(data)
		if cached, ok := a.rejectedPostings[p]; ok && cached == header {
			return false
		}
	}
	width := Scheme(data[0]).BytesPerValue()
	data = data[5:]
	for i := uint32(0); i < count; i++ {
		vectorID := decodePostingID(data[i*uint32(width):], width)
		if a.AllowList.Contains(vectorID) {
			a.allowedPostings.Visit(id)
			return true
		}
	}
	if cacheRejected {
		if a.rejectedPostings == nil {
			a.rejectedPostings = rejectedPostingsPool.Get().(map[*PostingMetadata][5]byte)
		}
		a.rejectedPostings[p] = header
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
	a.h.visitedPool.Return(a.allowedPostings)
	if a.rejectedPostings != nil {
		size := len(a.rejectedPostings)
		if size <= maxPooledRejectedPostings {
			clear(a.rejectedPostings)
			rejectedPostingsPool.Put(a.rejectedPostings)
		}
		a.rejectedPostings = nil
	}
}
