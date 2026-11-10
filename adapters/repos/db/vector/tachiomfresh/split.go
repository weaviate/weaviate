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
	"slices"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hfresh"
)

// doSplit replaces posting postingID by two postings. The two are clustered
// with a balanced 2-means on the full-precision token vectors. If reassign
// is true, it enqueues a reassign task for every token that may belong in
// another posting after the split. The code follows hfresh.doSplit and
// hfresh.splitPosting. The posting is the in-memory list, and every change
// is written through to the posting store. Both new postings get the type
// of the old one: a split of a type-T posting yields
// two type-T centroids.
func (t *TachiomFresh) doSplit(ctx context.Context, postingID uint64, reassign bool) error {
	start := time.Now()
	defer t.metrics.SplitDuration(start)

	t.postingLocks.Lock(postingID)

	var markedAsDone bool
	defer func() {
		if !markedAsDone {
			// Clear the queue's dedup marker before the posting lock is
			// released, as hfresh.doSplit does. An append that waits on
			// the lock enqueues its split only after it has the lock.
			t.taskQueue.SplitDone(postingID)
			t.postingLocks.Unlock(postingID)
		}
	}()

	if !t.Centroids.Exists(postingID) {
		t.logger.WithField("postingID", postingID).
			Trace("centroid not found, skipping split operation")
		return nil
	}

	refs := t.postings.get(postingID).decode()
	lp := len(refs)

	if lp == 0 {
		t.logger.WithField("postingID", postingID).
			Debug("posting is empty, skipping split operation")
		return nil
	}

	dims := atomic.LoadUint32(&t.dims)
	if dims == 0 {
		return errors.New("split called on uninitialized index")
	}

	// loadTokens fetches the document of every entry. The fetch fails when
	// a document was deleted between the garbage collection and the fetch.
	// Such a failure is transient, so the garbage collection and the fetch
	// are retried a few times, as hfresh.doSplit does.
	const maxSplitAttempts = 3
	// Not in hfresh: the limit is that of the posting's token type (see
	// postingLimits in types.go).
	_, maxPostingSize := t.postingLimits(postingID)
	var data [][]float32
	filtered := refs
	for attempt := 1; ; attempt++ {
		var err error
		// garbage collect the entries of deleted documents
		filtered, err = t.garbageCollect(ctx, filtered)
		if err != nil {
			return errors.Wrapf(err, "failed to garbage collect posting %d", postingID)
		}

		// skip if the filtered posting is now too small
		if lf := len(filtered); lf < int(maxPostingSize) {
			if lf == lp {
				// no changes, just return
				return nil
			}

			// persist the garbage-collected posting
			return t.putPosting(ctx, postingID, filtered)
		}

		data, err = t.loadTokens(ctx, filtered)
		if err == nil {
			break
		}
		if attempt == maxSplitAttempts {
			return errors.Wrapf(err, "failed to split vectors for posting %d", postingID)
		}

		t.logger.WithField("postingID", postingID).
			WithField("attempt", attempt).
			Debugf("retrying split after vector fetch failure: %v", err)
	}

	enc := compressionhelpers.NewKMeansEncoder(2, int(dims), 0)
	assignments, err := enc.FitBalanced(data)
	if err != nil {
		return errors.Wrapf(err, "failed to fit KMeans encoder for split of posting %d", postingID)
	}

	var result [2]splitResult
	for i := range result {
		result[i].Centroid = enc.Centroid(byte(i))
	}
	for i, side := range assignments {
		result[side].Posting = append(result[side].Posting, filtered[i])
		result[side].Tokens = append(result[side].Tokens, data[i])
	}
	// if one of the postings is empty, ignore the split
	if len(result[0].Posting) == 0 || len(result[1].Posting) == 0 {
		t.logger.WithField("postingID", postingID).
			Debug("split resulted in empty posting, skipping split operation")
		return nil
	}

	// hfresh.enqueueReassignAfterSplit reads the old centroid after it was
	// deleted. Here it is read before the deletion. The reassign check then
	// does not depend on what the HNSW returns for a tombstoned node.
	var oldCentroid []float32
	if reassign {
		oldCentroid, err = t.centroid(postingID)
		if err != nil {
			return errors.Wrapf(err, "failed to get centroid for posting %d", postingID)
		}
	}

	typ := t.types.typeOf(postingID)

	var newPostingIDs [2]uint64
	// searchable holds the new postings whose centroid was inserted; a
	// failure from here on is undone by unsplit
	var searchable []int
	for i := range 2 {
		newPostingID, err := t.IDs.Next()
		if err != nil {
			t.unsplit(ctx, postingID, result, searchable)
			return errors.Wrap(err, "failed to allocate new posting ID during split operation")
		}
		newPostingIDs[i] = newPostingID
		// The type is registered before the posting is stored. The entries
		// are then counted under their type (see countEntries in types.go).
		if err = t.types.register(newPostingID, typ); err != nil {
			t.unsplit(ctx, postingID, result, searchable)
			return err
		}

		// The posting is stored before its centroid becomes searchable. An
		// insert routed to the new centroid therefore never appends to a
		// missing list. newPostingID is not locked: no other goroutine can
		// know the id yet, and its lock stripe may be the one already held
		// for postingID.
		err = t.putPosting(ctx, newPostingID, result[i].Posting)
		if err != nil {
			// what reached the disk is left out (dropPosting)
			t.dropPosting(newPostingID)
			t.unsplit(ctx, postingID, result, searchable)
			return errors.Wrapf(err, "failed to put new posting %d after split operation", newPostingID)
		}
		// not hfresh's: the posting reaches the disk before its centroid
		// (see writeWALs)
		if err = t.writeWALs(); err != nil {
			t.dropPosting(newPostingID)
			t.unsplit(ctx, postingID, result, searchable)
			return err
		}

		// add the new centroid to the centroid index
		err = t.Centroids.Insert(newPostingID, &hfresh.Centroid{
			Uncompressed: result[i].Centroid,
			Deleted:      false,
		})
		if err != nil {
			if t.Centroids.Exists(newPostingID) {
				t.types.join(newPostingID, typ)
				searchable = append(searchable, i)
			} else {
				t.dropPosting(newPostingID)
			}
			t.unsplit(ctx, postingID, result, searchable)
			return errors.Wrapf(err, "failed to upsert new centroid %d after split operation", newPostingID)
		}
		t.types.join(newPostingID, typ)
		searchable = append(searchable, i)
	}

	// The old centroid leaves its type group after the new ones joined, so
	// the group is never empty. It leaves before it is deleted from the
	// centroid index (see typeTable.leave in types.go).
	t.types.leave(postingID, typ)

	// Delete the old centroid last, so a concurrent search never sees a
	// gap. This is the order of hfresh.doSplit.
	var delErr error
	if t.opts.beforeCentroidDelete != nil {
		delErr = t.opts.beforeCentroidDelete(postingID)
	}
	if delErr == nil {
		delErr = t.Centroids.MarkAsDeleted(postingID)
	}
	if delErr != nil {
		if t.Centroids.Exists(postingID) {
			// the old centroid stays: it rejoins its group, and keeps what
			// the new postings do not hold
			t.types.join(postingID, typ)
			t.unsplit(ctx, postingID, result, searchable)
		}
		return errors.Wrapf(delErr, "failed to delete old centroid %d after split operation", postingID)
	}
	// hfresh puts an empty posting for postingID, to bump its version and
	// let the old entries be cleaned up on disk. Here the posting is
	// dropped (see dropPosting in posting.go). Copied from hfresh, which
	// may have the same problem: the delete above can still be in the
	// commit log's buffer when the dropped entries leave the disk (see
	// writeWALs in tachiomfresh.go).
	t.dropPosting(postingID)

	// Mark the split operation as done
	markedAsDone = true
	t.postingLocks.Unlock(postingID)
	t.taskQueue.SplitDone(postingID)

	if !reassign || t.opts.disableReassign {
		return nil
	}

	err = t.enqueueReassignAfterSplit(ctx, postingID, typ, oldCentroid, newPostingIDs, result)
	if err != nil {
		return errors.Wrapf(err, "failed to enqueue reassign after split for posting %d", postingID)
	}

	return nil
}

// splitResult is one side of a split. It is hfresh.SplitResult with the
// float32 tokens kept next to their entries. An entry carries no code from
// which a distance could be computed.
type splitResult struct {
	Centroid []float32
	Posting  []tokenRef
	Tokens   [][]float32
}

// loadTokens returns the full-precision token of every entry: the split
// reads each (docID, tokenPos) from the objects bucket. With tokenForID
// set, each token is read on its own. Without it, each document is fetched
// once and its tokens are taken from it. It is the counterpart of the fetch
// loop in hfresh.splitPosting.
func (t *TachiomFresh) loadTokens(ctx context.Context, refs []tokenRef) ([][]float32, error) {
	data := make([][]float32, len(refs))
	if t.tokenForID != nil {
		// One token is read per entry and the document is never decoded
		// (see tokenForID in tachiomfresh.go). The slice the thunk returns
		// is owned by the caller and is kept without a copy. The dimension
		// is published before any posting exists (initDimensions).
		dims := int(atomic.LoadUint32(&t.dims))
		for i, ref := range refs {
			token, err := t.tokenForID(ctx, uint64(ref.DocID), int(ref.Pos), dims)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to fetch token %d of doc %d for split", ref.Pos, ref.DocID)
			}
			// centered when the index has a mean, the form in which the
			// centroids see every token (centerVec)
			data[i] = t.centerVec(token)
		}
		return data, nil
	}
	docs := make(map[uint32][][]float32)
	for i, ref := range refs {
		tokens, ok := docs[ref.DocID]
		if !ok {
			var err error
			tokens, err = t.multiVectorForID(ctx, uint64(ref.DocID))
			if err != nil {
				return nil, errors.Wrapf(err, "failed to fetch doc %d for split", ref.DocID)
			}
			docs[ref.DocID] = tokens
		}
		if int(ref.Pos) >= len(tokens) {
			return nil, errors.Errorf("doc %d has %d tokens but a posting references token %d",
				ref.DocID, len(tokens), ref.Pos)
		}
		// The thunk may return memory it owns, so the token is cloned.
		// hfresh.clusteringVector clones for the same reason.
		data[i] = slices.Clone(tokens[ref.Pos])
		// centered when the index has a mean, the form in which the
		// centroids see every token (centerVec)
		data[i] = t.centerVec(data[i])
	}
	return data, nil
}

// fetchToken returns the full-precision token of one entry in the form in
// which the centroids see it: centered when the index has a mean (see
// center). It is the counterpart
// of the vector fetch in hfresh.doReassign.
func (t *TachiomFresh) fetchToken(ctx context.Context, ref tokenRef) ([]float32, error) {
	if t.tokenForID != nil {
		// the token alone, without its document (see tokenForID)
		token, err := t.tokenForID(ctx, uint64(ref.DocID), int(ref.Pos), int(atomic.LoadUint32(&t.dims)))
		if err != nil {
			return nil, err
		}
		return t.centerVec(token), nil
	}
	tokens, err := t.multiVectorForID(ctx, uint64(ref.DocID))
	if err != nil {
		return nil, err
	}
	if int(ref.Pos) >= len(tokens) {
		return nil, errors.Errorf("doc %d has %d tokens but a posting references token %d",
			ref.DocID, len(tokens), ref.Pos)
	}
	return t.centerVec(tokens[ref.Pos]), nil
}

// centroid returns centroid id as a float32 vector with the dimensionality
// of the tokens. Centroids.Get decodes the RQ-8 code of the centroid HNSW,
// as hfresh does. That code is as long as the quantizer's rotation, which
// pads the dimensions to a multiple of 64. The padding is cut off so that
// the vector can be compared with a token.
func (t *TachiomFresh) centroid(id uint64) ([]float32, error) {
	c, err := t.Centroids.Get(id)
	if err != nil {
		return nil, err
	}
	dims := int(atomic.LoadUint32(&t.dims))
	if len(c.Uncompressed) < dims {
		return nil, errors.Errorf("centroid %d has %d dimensions, expected %d", id, len(c.Uncompressed), dims)
	}
	return c.Uncompressed[:dims], nil
}

// centroidDistance is the distance between a centroid and a token, computed
// on float32 vectors. hfresh's Centroid.Distance compares the RQ-1 codes
// stored in the posting. A posting here stores no codes, so the split,
// merge and reassign checks use the tokens read from the object store. The
// checks compare squared L2 distances, the distance by which a token is
// placed in a posting (see nearestCentroids). Each check asks whether a
// token is closer to one centroid than to another, and the answer must
// agree with the placement. hfresh compares the index distance.
func (t *TachiomFresh) centroidDistance(centroid, token []float32) (float32, error) {
	return t.l2.SingleDist(centroid, token)
}

// enqueueReassignAfterSplit follows hfresh.enqueueReassignAfterSplit: the
// same two checks with the same comparisons, on float32 distances (see
// centroidDistance). hfresh compares versions; here only the deleted flag
// is checked, because versions are only tombstones in this index. The
// neighbouring centroids of the second check are those of the split
// posting's type typ. A token only moves between postings of its
// own type.
func (t *TachiomFresh) enqueueReassignAfterSplit(ctx context.Context, oldPostingID uint64, typ uint32, oldCentroid []float32, newPostingIDs [2]uint64, newPostings [2]splitResult) error {
	reassignedTokens := make(map[tokenRef]struct{})

	// First check. If a token is closer to one of the new centroids than to
	// the old centroid, no neighbouring centroid can be better.
	for i := range newPostings {
		// test each token
		for j, ref := range newPostings[i].Posting {
			live, err := t.live(ctx, ref)
			if err != nil {
				return err
			}
			if !live {
				continue
			}
			token := newPostings[i].Tokens[j]

			// compute distance from the token to its new centroid
			newDist, err := t.centroidDistance(newPostings[i].Centroid, token)
			if err != nil {
				return errors.Wrapf(err, "failed to compute distance for token %+v in new posting %d", ref, newPostingIDs[i])
			}

			// compute distance from the token to the old centroid
			oldDist, err := t.centroidDistance(oldCentroid, token)
			if err != nil {
				return errors.Wrapf(err, "failed to compute distance for token %+v in old posting %d", ref, oldPostingID)
			}

			if newDist >= oldDist {
				// The token is closer to the old centroid. It may then be
				// closer to a neighbouring centroid as well, so it is
				// reassigned.
				err = t.taskQueue.EnqueueReassign(newPostingIDs[i], ref)
				if err != nil {
					return errors.Wrapf(err, "failed to enqueue reassign for token %+v after split", ref)
				}
				reassignedTokens[ref] = struct{}{}
			}
		}
	}

	// Second check. A token of a neighbouring posting that is closer to one
	// of the new centroids than to the old centroid is reassigned.
	if t.config.ReassignNeighbors <= 0 {
		return nil
	}

	// search for neighboring centroids
	nearest, err := t.nearestCentroids(oldCentroid, typ, t.config.ReassignNeighbors)
	if err != nil {
		return errors.Wrapf(err, "failed to search for nearest centroids for reassign after split for posting %d", oldPostingID)
	}

	seen := make(map[uint64]struct{})
	for _, id := range newPostingIDs {
		seen[id] = struct{}{}
	}
	// check every token of each neighbouring posting against the new
	// centroids
	for neighborID := range nearest.Iter() {
		_, exists := seen[neighborID]
		if exists {
			continue
		}
		seen[neighborID] = struct{}{}

		// The posting is decoded into its own slice, so no posting lock
		// is held across the document fetches.
		t.postingLocks.RLock(neighborID)
		p := t.postings.get(neighborID).decode()
		t.postingLocks.RUnlock(neighborID)
		if len(p) == 0 {
			t.logger.WithField("postingID", neighborID).
				Debug("posting not found, skipping reassign after split")
			continue // Skip if the posting is not found
		}

		neighborCentroid, err := t.centroid(neighborID)
		if err != nil {
			return errors.Wrapf(err, "failed to get centroid for posting %d", neighborID)
		}
		for _, ref := range p {
			_, exists := reassignedTokens[ref]
			live, err := t.live(ctx, ref)
			if err != nil {
				return err
			}
			if exists || !live {
				continue
			}

			token, err := t.fetchToken(ctx, ref)
			if err != nil {
				// deleted or replaced between the check above and the fetch
				if live, lerr := t.live(ctx, ref); lerr == nil && !live {
					continue
				}
				return errors.Wrapf(err, "failed to fetch token %+v in neighbor posting %d", ref, neighborID)
			}

			distNeighbor, err := t.centroidDistance(neighborCentroid, token)
			if err != nil {
				return errors.Wrapf(err, "failed to compute distance for token %+v in neighbor posting %d", ref, neighborID)
			}

			distOld, err := t.centroidDistance(oldCentroid, token)
			if err != nil {
				return errors.Wrapf(err, "failed to compute distance for token %+v in old posting %d", ref, oldPostingID)
			}

			distA0, err := t.centroidDistance(newPostings[0].Centroid, token)
			if err != nil {
				return errors.Wrapf(err, "failed to compute distance for token %+v in new posting %d", ref, newPostingIDs[0])
			}

			distA1, err := t.centroidDistance(newPostings[1].Centroid, token)
			if err != nil {
				return errors.Wrapf(err, "failed to compute distance for token %+v in new posting %d", ref, newPostingIDs[1])
			}

			if distOld <= distA0 && distOld <= distA1 {
				// The token is closer to the old centroid than to both new
				// ones, so the new postings are no better than its current
				// posting.
				continue
			}

			if distNeighbor < distA0 && distNeighbor < distA1 {
				// The token is closer to its current centroid than to both
				// new centroids. No reassign is needed.
				continue
			}

			// The token is closer to one of the new centroids, so it is
			// reassigned.
			err = t.taskQueue.EnqueueReassign(neighborID, ref)
			if err != nil {
				return errors.Wrapf(err, "failed to enqueue reassign for token %+v after split", ref)
			}
			reassignedTokens[ref] = struct{}{}
		}
	}

	return nil
}

// unsplit undoes a split that failed after it stored a new posting, with
// postingID's lock held. A new posting whose centroid is searchable stays:
// an insert may have appended to it since, so it cannot be taken back. Its
// entries leave the old posting instead, so that every token is in one
// posting. A new posting without a centroid is dropped by the caller. A
// failure here is logged; restore removes what it leaves in two postings
// (resolveDuplicates).
func (t *TachiomFresh) unsplit(ctx context.Context, postingID uint64, result [2]splitResult, searchable []int) {
	if len(searchable) == 0 {
		return
	}
	moved := make(map[tokenRef]struct{})
	for _, i := range searchable {
		for _, ref := range result[i].Posting {
			moved[ref] = struct{}{}
		}
	}
	var kept []tokenRef
	for _, ref := range t.postings.get(postingID).decode() {
		if _, ok := moved[ref]; !ok {
			kept = append(kept, ref)
		}
	}
	if err := t.putPosting(ctx, postingID, kept); err != nil {
		t.logger.WithField("postingID", postingID).
			Warnf("failed to undo a failed split: %v", err)
	}
}
