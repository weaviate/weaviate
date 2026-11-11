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

package tachiomfresh

import (
	"context"
	"math"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hfresh"
	"github.com/weaviate/weaviate/entities/storobj"
)

// flatSearchFactor bounds the flat path. A filter that allows at most
// flatSearchFactor x kDocsToScore documents is answered by flatSearch,
// which scores every allowed document exactly and skips the stages.
// hfresh has a flat path for single vectors with muvera off, below a
// constant of 5,000 documents (its flatSearchCutoff). That constant was
// measured too high for this index. A flat query pays one document read
// and one MaxSim per allowed document, the same as a staged query pays
// per candidate. The break-even is therefore kDocsToScore plus the cost
// of stage 1, counted in documents. The factor 2 takes the flat path
// only where it is exact and about as fast as the stages. The factor
// also covers the range of filter sizes where the widening in
// SearchByMultiVector would run to its cap.
const flatSearchFactor = 2

// maxWidenings caps how many times a filtered search doubles kCentroids
// when stage 1 reached too few allowed documents (SearchByMultiVector).
// The search uses at most 16 times the configured kCentroids.
const maxWidenings = 4

// SearchByMultiVector runs TACHIOM's three stages. It returns
// the top k documents with their MaxSim distance: the sum over the query
// tokens of the minimum token distance, lower is better. This is the
// convention of HFresh.SearchByMultiVector. With an allow list, stage 1
// skips the disallowed documents as it skips the deleted ones, so the
// stage-1 scores of the allowed documents are their unfiltered scores,
// and stage 2 keeps the best kDocsToScore of the allowed documents. A
// selective filter skips the stages, and every allowed document is scored
// exactly (flatSearch, bounded by flatSearchFactor). In between,
// kCentroids is doubled when the scan reached too few allowed documents
// (maxWidenings).
func (t *TachiomFresh) SearchByMultiVector(ctx context.Context, vectors [][]float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	// dims == 0 means nothing has been indexed yet. The search returns
	// empty results and no error, as HFresh.SearchByMultiVector does.
	if atomic.LoadUint32(&t.dims) == 0 {
		return nil, nil, nil
	}

	// Under the threshold every staged document is scored exactly
	// (stagedSearch in staging.go). The staging bucket is emptied only
	// after built is published, and stagedSearch checks built with its
	// cursor open, so a search that races the end of the build finds every
	// document in one place or the other.
	if !t.built.Load() {
		ids, dists, stale, err := t.stagedSearch(ctx, vectors, k, allow)
		if !stale {
			return ids, dists, err
		}
	}

	kCentroids := int(atomic.LoadUint32(&t.kCentroids))
	kDocsToScore := int(atomic.LoadUint32(&t.kDocsToScore))
	if allow != nil {
		if allow.IsEmpty() {
			return nil, nil, nil
		}
		if allow.Len() <= t.opts.flatFactor*kDocsToScore {
			return t.flatSearch(ctx, vectors, k, allow)
		}
	}

	scratch := getSearchScratch()
	defer putSearchScratch(scratch)
	for widenings := 0; ; widenings++ {
		probed, err := t.scanPostings(ctx, vectors, kCentroids, allow, scratch)
		if err != nil {
			return nil, nil, err
		}
		if allow == nil {
			break
		}
		// Widening. This is not hfresh's mechanism: hfresh routes to the
		// allowed documents through a centroid-level allow list (see
		// scanPostings). Here, when the scan reached fewer allowed
		// documents than stage 2 would keep, kCentroids is doubled and
		// the scan is redone from scratch. This happens at most
		// maxWidenings times, or until every centroid was searched (a
		// token's search returned fewer than asked). The target is
		// kDocsToScore. With k as the target, a mild filter would thin
		// the candidates of stage 3. The target is capped by the filter's
		// size. The filter's count may include documents the index does
		// not hold (deleted, not yet indexed), so the target may never be
		// reached. This is why the doublings are capped. hnsw's filtered
		// search widens nothing: it picks a strategy from the filter's
		// selectivity (acornEnabled, AcornFilterRatio). Here
		// flatSearchFactor plays that role.
		if widenings == t.opts.maxWiden || scratch.live >= min(kDocsToScore, allow.Len()) || probed < kCentroids {
			break
		}
		kCentroids *= 2
		scratch.reset()
		if t.opts.onWiden != nil {
			t.opts.onWiden()
		}
	}

	// Stage 2: keep the top kDocsToScore documents by accumulated
	// similarity, ties by doc id ascending. This is TACHIOM's candidate
	// selection. Alpha pruning is not implemented.
	// At least k candidates are scored: the max(k, budget) rule of
	// hfresh.muveraSearchBudgets.
	candidates := scratch.top(max(k, kDocsToScore))
	candidateIDs := make([]uint64, len(candidates))
	for i, c := range candidates {
		candidateIDs[i] = uint64(c.id)
	}

	// Stage 3: exact MaxSim on the candidates' tokens. The tokens are
	// stored as float32; Compressed returns false.
	return t.computeLateInteraction(ctx, vectors, k, candidateIDs, &scratch.container)
}

// flatSearch scores every allowed document the index holds exactly. It
// has the shape of hfresh.flatSearch, over the exact scoring of stage 3.
// The answer is exact, and the cost is bounded by the filter's size
// (flatSearchFactor). The doc set (docs.go) says which allowed ids the
// index holds, so a deleted or not yet indexed id costs nothing.
func (t *TachiomFresh) flatSearch(ctx context.Context, vectors [][]float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	scratch := getSearchScratch()
	defer putSearchScratch(scratch)
	candidates := make([]uint64, 0, allow.Len())
	it := allow.Iterator()
	defer it.Stop()
	for id, ok := it.Next(); ok; id, ok = it.Next() {
		if t.docs.contains(id) {
			candidates = append(candidates, id)
		}
	}
	return t.computeLateInteraction(ctx, vectors, k, candidates, &scratch.container)
}

// scanPostings is stage 1. For each query token it searches kCentroids
// centroids and adds the best centroid similarity per document into
// scratch, as TACHIOM does. A document counts once per posting per
// token, however many of its tokens the posting holds: the scan keeps the
// best similarity per document and query token.
// Unseen documents keep 0. The function returns the number of centroids
// the last token's search returned, which is below kCentroids only when
// the index has fewer.
//
// The scores are kept in one table per query (see searchScratch), and a
// document is checked for deletion and against the allow list once per
// scan (a widening scans again). Two Go maps and a check per entry would cost more: with large
// postings the cost of a scanned entry decides the cost of this stage.
// The scores are the same, added in the same order.
//
// The allow list is applied to the scanned documents; the centroid search
// is unfiltered. hfresh hands its centroid HNSW a centroid-level allow
// list: a centroid passes when its posting holds an allowed vector
// (hfresh/allow_list.go). The same wrapper over the posting table was
// measured and removed. A posting of n entries spans about n documents,
// so for a filter allowing a fraction f a centroid passes with
// probability about 1-(1-f)^n, and nearly all pass unless f is small.
// The wrapper costs one posting scan per node the centroid search
// touches, per query token. Measured, it cost a large share of the QPS
// at mild filters, with no gain in recall. At selective filters it was
// far slower than the flat path's exact answer.
func (t *TachiomFresh) scanPostings(ctx context.Context, vectors [][]float32, kCentroids int, allow helpers.AllowList, scratch *searchScratch) (int, error) {
	var probed int
	for i, token := range vectors {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		centroids, err := t.Centroids.Search(ctx, token, kCentroids, nil)
		if err != nil {
			return 0, err
		}
		probed = centroids.Len()
		// 0 marks a document no query token has reached yet
		tokenNo := uint32(i + 1)
		for cid, dist := range centroids.Iter() {
			sim := t.similarity(dist)
			t.postingLocks.RLock(cid)
			refs := t.postings.get(cid)
			// keep track of the posting size
			postingSize := refs.len()
			// only the doc ids, decoded into the query's buffer (a posting
			// is a packed array, see packedPosting in posting.go)
			scratch.ids = refs.docIDs(scratch.ids)
			// the entries' versions, nil when all are at version 1
			versions := refs.versions()
			for i, id := range scratch.ids {
				doc, isNew := scratch.slot(id)
				if isNew {
					// skip deleted documents, as hfresh.searchByFDE does
					version, err := t.VersionMap.Get(ctx, uint64(id))
					if err != nil {
						t.postingLocks.RUnlock(cid)
						return 0, errors.Wrapf(err, "failed to check if doc %d is deleted", id)
					}
					doc.deleted = version.Deleted()
					doc.version = version.Version()
					// and the disallowed ones, as hfresh's scan does
					doc.skip = doc.deleted || (allow != nil && !allow.Contains(uint64(id)))
				}
				// a stale entry, older than its document, counts as a dead
				// one (live in posting.go)
				entryVersion := uint8(1)
				if versions != nil {
					entryVersion = versions[i]
				}
				if doc.deleted || entryVersion < doc.version {
					postingSize--
					continue
				}
				if doc.skip {
					continue
				}
				if doc.token != tokenNo {
					// The first live entry of the document counts it as
					// reached. A document whose entries are all stale is
					// not reached: it counts toward neither the widening
					// target nor stage 2 (top).
					if doc.token == 0 {
						scratch.live++
					}
					// first posting of this query token that holds the
					// document: the best similarity under the previous token
					// that reached it is final and joins the sum
					doc.acc += doc.best
					doc.best = sim
					doc.token = tokenNo
				} else if sim > doc.best {
					doc.best = sim
				}
			}
			t.postingLocks.RUnlock(cid)

			// A posting below the configured minimum size enqueues a merge
			// (hfresh's merge trigger), with two differences from
			// hfresh. A posting alone in its token type is left out,
			// unless it is empty: a merge only joins postings of one type,
			// so it would find nothing to do, on every search that scans
			// the posting; an empty one it deletes (deleteEmptyPosting). And
			// the minimum is the one of the posting's token type (see
			// postingLimits in types.go).
			if minPostingSize, _ := t.postingLimits(cid); postingSize < int(minPostingSize) && (postingSize == 0 || !t.aloneInGroup(cid)) {
				err = t.taskQueue.EnqueueMerge(cid)
				if err != nil {
					return 0, errors.Wrapf(err, "failed to enqueue merge for posting %d", cid)
				}
			} else if dead := len(scratch.ids) - postingSize; 3*dead > len(scratch.ids) {
				// Cleanup trigger, not in hfresh, which cleans a posting
				// only when a split or a merge rewrites it. When
				// more than a third of the posting's entries are dead (of
				// deleted documents, or stale), a cleanup of the posting is
				// enqueued
				// (cleanup.go). A third is the ratio of hfresh's merge
				// threshold (minPostingSize is maxPostingSize/3). The scan
				// is the trigger because it already pays for the dead
				// entries, one decode and one version check each, and
				// counts them in postingSize above, per entry and not per
				// document. No cleanup is enqueued when a merge was just
				// enqueued: the merge garbage-collects the posting whether
				// or not it finds a candidate.
				err = t.taskQueue.EnqueueCleanup(cid)
				if err != nil {
					return 0, errors.Wrapf(err, "failed to enqueue cleanup for posting %d", cid)
				}
			}
		}
	}
	return probed, nil
}

// computeLateInteraction is copied from hfresh.computeLateInteraction. It
// also skips a candidate that was deleted after stage 1, and, as hfresh's
// copy does, one whose object is gone.
func (t *TachiomFresh) computeLateInteraction(ctx context.Context, queryVectors [][]float32, k int, candidateIDs []uint64, container *common.VectorSlice) ([]uint64, []float32, error) {
	results := hfresh.NewResultSet(k)

	// With a container and the buffered thunk, every candidate's bytes
	// and tokens go into the query's reused memory (see readMultiVector).
	for _, id := range candidateIDs {
		// a cancelled query reads and scores no more candidates
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		docVectors, err := t.readMultiVector(ctx, id, container)
		if err != nil {
			// A shard removes the object before it deletes the document
			// from the index. A candidate read in between is left out, as
			// the hnsw search treats a missing object as deleted.
			var notFound storobj.ErrNotFound
			if errors.As(err, &notFound) {
				continue
			}
			// Not in hfresh: a candidate deleted after stage 1 has no doc
			// record any more. It is left out of the results.
			if deleted, derr := t.VersionMap.IsDeleted(ctx, id); derr == nil && deleted {
				continue
			}
			return nil, nil, errors.Wrapf(err, "get multi-vector for id %d", id)
		}
		score, err := t.maxSimScore(queryVectors, docVectors)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "compute maxsim for id %d", id)
		}
		results.Insert(id, score)
	}

	ids := make([]uint64, results.Len())
	dists := make([]float32, results.Len())
	i := 0
	for id, dist := range results.Iter() {
		ids[i] = id
		dists[i] = dist
		i++
	}

	return ids, dists, nil
}

// readMultiVector reads a document's tokens for the exact scoring. When
// the config has a buffered thunk and a container is given, the tokens
// are read into the container through that thunk, which writes the key
// into Buff8. Otherwise the plain thunk returns them in fresh memory. The
// function has the shape of hfresh's readMultiVector.
func (t *TachiomFresh) readMultiVector(ctx context.Context, id uint64, container *common.VectorSlice) ([][]float32, error) {
	if container != nil && t.tempMultiVectorForID != nil {
		if len(container.Buff8) < 8 {
			container.Buff8 = make([]byte, 8)
		}
		return t.tempMultiVectorForID(ctx, id, container)
	}
	return t.multiVectorForID(ctx, id)
}

// maxSimScore computes the MaxSim score between a query and a document
// multi-vector. For each query token it finds the minimum distance to any
// document token, then sums over all query tokens. A lower score means
// more similar. Copied from hfresh.maxSimScore.
func (t *TachiomFresh) maxSimScore(queryVectors, docVectors [][]float32) (float32, error) {
	var score float32
	for _, queryToken := range queryVectors {
		d := t.config.DistanceProvider.New(queryToken)
		minDist := float32(math.MaxFloat32)
		for _, docToken := range docVectors {
			dist, err := d.Distance(docToken)
			if err != nil {
				return 0, err
			}
			if dist < minDist {
				minDist = dist
			}
		}
		score += minDist
	}
	return score, nil
}

// docScore is the stage 1 state of one document in one query.
type docScore struct {
	id      uint32
	token   uint32  // number of the last query token that reached it, from 1
	best    float32 // best centroid similarity for that token
	acc     float32 // sum of the best similarities of the earlier tokens
	deleted bool    // deleted: left out of the scores and of the posting's size
	version uint8   // the document's version: an older entry is stale
	skip    bool    // deleted or outside the allow list: not a candidate
}

// candidate is a document that goes on to stage 3.
type candidate struct {
	id    uint32
	score float32
}

// worse orders candidates for stage 2. A lower score is worse; at equal
// scores a higher doc id is worse. The best candidate is therefore the one
// with the highest score and, among equals, the lowest doc id.
func (c candidate) worse(o candidate) bool {
	if c.score != o.score {
		return c.score < o.score
	}
	return c.id > o.id
}

// searchScratch holds the stage 1 scores of one query: the documents in
// the order they were first reached, and an open-addressing table from
// doc id to their position. Two maps (one per query token for the best
// similarity, one for the sum) and a full sort after them cost more: with
// large postings, their lookups and the sort were most of the stage's
// cost. Scratches are pooled, so a search allocates nothing here after
// the first few.
type searchScratch struct {
	docs  []docScore
	table []uint32 // position in docs plus one, 0 when the cell is free
	cands []candidate
	ids   []uint32 // the doc ids of the posting being scanned
	live  int      // the documents reached by a live entry that are candidates (not deleted, allowed)
	// container is stage 3's reused memory: the key, the node's bytes
	// and the tokens' backing array. It grows to the largest document
	// seen and stays with the scratch across queries.
	container common.VectorSlice
}

var searchScratchPool = sync.Pool{
	New: func() any { return &searchScratch{table: make([]uint32, 1<<12)} },
}

func getSearchScratch() *searchScratch {
	return searchScratchPool.Get().(*searchScratch)
}

func putSearchScratch(s *searchScratch) {
	s.reset()
	searchScratchPool.Put(s)
}

// reset forgets the scores of a scan and keeps the memory. It clears only the
// cells the scan used: the table keeps the size of the largest scan the
// scratch has seen, and most scans use a small part of it. A document's cell
// is found by probing from its hash, past cells this loop already cleared.
func (s *searchScratch) reset() {
	mask := uint32(len(s.table) - 1)
	for pos := range s.docs {
		i := (s.docs[pos].id * 2654435769) >> 7 & mask
		for s.table[i] != uint32(pos+1) {
			i = (i + 1) & mask
		}
		s.table[i] = 0
	}
	s.docs = s.docs[:0]
	s.cands = s.cands[:0]
	s.live = 0
}

// cell returns the table cell where id is, or where it goes.
func (s *searchScratch) cell(id uint32) int {
	mask := uint32(len(s.table) - 1)
	// Fibonacci hashing: doc ids are mostly consecutive integers
	i := (id * 2654435769) >> 7 & mask
	for {
		pos := s.table[i]
		if pos == 0 || s.docs[pos-1].id == id {
			return int(i)
		}
		i = (i + 1) & mask
	}
}

// slot returns the state of document id, adding it when the query reaches
// it for the first time. The pointer is valid until the next call.
func (s *searchScratch) slot(id uint32) (doc *docScore, isNew bool) {
	i := s.cell(id)
	if pos := s.table[i]; pos != 0 {
		return &s.docs[pos-1], false
	}
	// the table is kept at most half full
	if 2*(len(s.docs)+1) > len(s.table) {
		s.table = make([]uint32, 2*len(s.table))
		for pos := range s.docs {
			s.table[s.cell(s.docs[pos].id)] = uint32(pos + 1)
		}
		i = s.cell(id)
	}
	s.docs = append(s.docs, docScore{id: id})
	s.table[i] = uint32(len(s.docs))
	return &s.docs[len(s.docs)-1], true
}

// top returns the n documents with the highest accumulated similarity,
// best first, ties by doc id ascending. The result is the same as sorting
// all of them and cutting at n. A heap of the n best so far keeps the
// cost at O(documents x log n); a sort of every reached document would
// cost more.
func (s *searchScratch) top(n int) []candidate {
	heap := s.cands[:0]
	for i := range s.docs {
		doc := &s.docs[i]
		// a skipped document, or one reached only by stale entries
		if doc.skip || doc.token == 0 {
			continue
		}
		// the last token's best similarity has not joined the sum yet
		c := candidate{id: doc.id, score: doc.acc + doc.best}
		switch {
		case len(heap) < n:
			// sift up: the worst candidate is at the root
			heap = append(heap, c)
			for j := len(heap) - 1; j > 0; {
				parent := (j - 1) / 2
				if !heap[j].worse(heap[parent]) {
					break
				}
				heap[j], heap[parent] = heap[parent], heap[j]
				j = parent
			}
		case heap[0].worse(c):
			// replace the worst and sift down
			heap[0] = c
			for j := 0; ; {
				child := 2*j + 1
				if child >= len(heap) {
					break
				}
				if child+1 < len(heap) && heap[child+1].worse(heap[child]) {
					child++
				}
				if !heap[child].worse(heap[j]) {
					break
				}
				heap[j], heap[child] = heap[child], heap[j]
				j = child
			}
		}
	}
	sort.Slice(heap, func(i, j int) bool { return heap[j].worse(heap[i]) })
	s.cands = heap
	return heap
}
