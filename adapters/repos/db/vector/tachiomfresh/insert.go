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
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hfresh"
)

// AddMulti indexes one document. The document must already be readable
// through MultiVectorForIDThunk: a split, merge or reassign that touches
// one of its tokens fetches it back. Production and hfresh's
// addMultiVectorToIndex test helper both satisfy this precondition. The
// doc record is the caller's: the objects bucket already holds it. Doc
// ids are not reused after a delete, as in Weaviate, where an update
// writes a new doc id.
//
// Every token is routed among the centroids of its type, the token id the
// model's tokenizer gave it (types.go). The multivector interface carries
// the vectors only, so the token ids are read from the stored object,
// through TokenIDsForIDThunk. A document without token ids is refused.
func (t *TachiomFresh) AddMulti(ctx context.Context, docID uint64, vectors [][]float32) error {
	return t.addMulti(ctx, docID, vectors, nil)
}

// readTokenIDs reads the token ids of document docID from the stored object.
func (t *TachiomFresh) readTokenIDs(ctx context.Context, docID uint64, tokens int) ([]uint32, error) {
	if t.tokenIDsForID == nil {
		return nil, errors.New("tachiomfresh needs token ids, and the index has no reader for them")
	}
	types, err := t.tokenIDsForID(ctx, docID)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read the token ids of doc %d", docID)
	}
	if len(types) == 0 {
		return nil, errors.Errorf("doc %d has no token ids: tachiomfresh needs the token id of every token, from the tokenizer of the model that produced the vectors", docID)
	}
	if len(types) != tokens {
		return nil, errors.Errorf("doc %d has %d tokens but %d token ids", docID, tokens, len(types))
	}
	return types, nil
}

// addMulti indexes one document. With types nil, the token ids are read
// from the stored object, as AddMulti does; the tests pass them directly.
func (t *TachiomFresh) addMulti(ctx context.Context, docID uint64, vectors [][]float32, types []uint32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := t.ValidateMultiBeforeInsert(vectors); err != nil {
		return err
	}
	// posting entries are (u32, u16)
	if docID > math.MaxUint32 {
		return errors.Errorf("doc id %d does not fit the u32 posting entry", docID)
	}
	if len(vectors) > math.MaxUint16+1 {
		return errors.Errorf("multi-vector with %d tokens does not fit the u16 token position", len(vectors))
	}

	if types == nil {
		var err error
		if types, err = t.readTokenIDs(ctx, docID, len(vectors)); err != nil {
			return err
		}
	}

	start := time.Now()
	defer t.metrics.InsertVector(start)

	if err := t.initDimensions(vectors[0]); err != nil {
		return err
	}
	// under the threshold the document is staged, and the index is built
	// from the staged documents when the threshold is reached
	// (staging.go). built is published at the end of the build, under
	// stageMu, so an insert that saw it false checks again under the lock.
	if !t.built.Load() {
		t.stageMu.Lock()
		if !t.built.Load() {
			err := t.stageDocument(ctx, docID, vectors, types)
			t.stageMu.Unlock()
			return err
		}
		t.stageMu.Unlock()
	}
	return t.fileDocument(ctx, docID, vectors, types, false)
}

// fileDocument puts every token of a document into the posting of its
// nearest centroid. It is the insert of a built index, and the build from
// the staging bucket runs it on each staged document, with staged true.
// vectors are the tokens as inserted, types their token ids.
func (t *TachiomFresh) fileDocument(ctx context.Context, docID uint64, vectors [][]float32, types []uint32, staged bool) error {
	// the tokens as the centroids see them: centered when the index has a
	// mean (staging.go). The stored document is the shard's and keeps its
	// raw tokens, which stage 3 scores. The raw tokens also feed the
	// spread of their type (addToken, typeGroup.observe in types.go).
	raw := vectors
	vectors = t.centerMultiVec(vectors)

	// An id with tokens placed before, whole or in part, gets a new
	// version: the entries it has become stale, and nobody has to find
	// them. Splits, merges and cleanups drop them.
	var version uint8
	if staged {
		v, ok, err := t.stagedVersion(ctx, docID)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		version = v
	} else if t.placed.contains(docID) {
		v, err := t.VersionMap.Reinsert(ctx, docID)
		if err != nil {
			return errors.Wrapf(err, "failed to raise the version of doc %d", docID)
		}
		version = v.Version()
	} else {
		v, err := t.insertVersion(ctx, docID)
		if err != nil {
			return err
		}
		version = v
	}
	// the count before the first token: restore takes the document as
	// indexed only when its entries cover the count (tokencount.go)
	if err := t.counts.put(docID, len(vectors)); err != nil {
		return errors.Wrapf(err, "failed to store the token count of doc %d", docID)
	}
	t.placed.add(docID)

	for pos, token := range vectors {
		if err := t.addToken(ctx, newTokenRef(uint32(docID), uint16(pos), version), token, raw[pos], types[pos]); err != nil {
			return errors.Wrapf(err, "failed to add token %d of doc %d", pos, docID)
		}
	}
	// The document is indexed once every token is (ContainsDoc, docs.go).
	// A Delete that ran meanwhile marked the version map before it cleared
	// the bit, so the check after the add sees it, whichever came first.
	t.docs.add(docID)
	if v, err := t.VersionMap.Get(ctx, docID); err != nil || v.Deleted() || v.Version() != version {
		t.docs.remove(docID)
		if err != nil {
			return errors.Wrapf(err, "failed to check the version of doc %d", docID)
		}
	}

	return nil
}

// stagedVersion returns the version the build files a staged document at,
// and false when the document was deleted after it was staged. A Delete
// marks the version map before it waits for a build to finish (Delete in
// delete.go), so the build must not give a deleted document a new live
// version, as insertVersion and Reinsert do for an insert. A document whose
// tokens an earlier attempt of the build placed gets a new version, so that
// those entries become stale.
func (t *TachiomFresh) stagedVersion(ctx context.Context, docID uint64) (uint8, bool, error) {
	t.VersionMap.EnsureDefault(docID)
	for {
		v, err := t.VersionMap.Get(ctx, docID)
		if err != nil {
			return 0, false, errors.Wrapf(err, "failed to get the version of doc %d", docID)
		}
		if v.Deleted() {
			return 0, false, nil
		}
		if !t.placed.contains(docID) {
			return v.Version(), true, nil
		}
		next, err := t.VersionMap.Increment(ctx, docID, v)
		if errors.Is(err, hfresh.ErrVersionIncrementFailed) {
			// a Delete marked the document meanwhile: read it again
			continue
		}
		if err != nil {
			return 0, false, errors.Wrapf(err, "failed to raise the version of doc %d", docID)
		}
		return next.Version(), true, nil
	}
}

// insertVersion returns the version a document gets when it is inserted
// with no tokens placed before. It starts at hfresh's implicit first
// version, installed here so that searches do not fault it in from the
// store one document at a time (hfresh.warmVersionMap does the same for
// restored vectors). A deleted id becomes live again with a new version
// (VersionMap.Reinsert).
func (t *TachiomFresh) insertVersion(ctx context.Context, docID uint64) (uint8, error) {
	t.VersionMap.EnsureDefault(docID)
	v, err := t.VersionMap.Get(ctx, docID)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to get the version of doc %d", docID)
	}
	if v.Deleted() {
		if v, err = t.VersionMap.Reinsert(ctx, docID); err != nil {
			return 0, errors.Wrapf(err, "failed to raise the version of doc %d", docID)
		}
	}
	return v.Version(), nil
}

// addToken routes one token to the nearest centroid of its type typ (no
// replicas), and appends the entry to that
// posting. It has the shape of hfresh.add for a single target. token is
// the token as the centroids see it (centered when the index has a mean);
// raw is the token as inserted, which the type's spread observes (see
// fileDocument).
func (t *TachiomFresh) addToken(ctx context.Context, ref tokenRef, token, raw []float32, typ uint32) error {
	for {
		postingID, err := t.nearestPosting(token, typ)
		if err != nil {
			return err
		}

		if t.opts.beforeAppend != nil {
			t.opts.beforeAppend(postingID)
		}
		added, err := t.append(ctx, ref, postingID)
		if err != nil {
			return errors.Wrapf(err, "failed to append token to posting %d", postingID)
		}
		if added {
			// not hfresh's: the token joins the sums the spread of its
			// type is computed from (postingLimits in types.go). Inserts
			// only: a reassign moves a token inside its type.
			if g := t.types.group(typ); g != nil {
				g.observe(raw)
			}
			return nil
		}
		// the posting was split or merged away between the search and the
		// lock. hfresh.add enqueues a reassign, because its vector is still
		// held in other replicas meanwhile. A token has one posting only,
		// so it is routed again at once.
	}
}

// append adds a token to the specified posting, on disk and in memory.
// It returns true if the token was successfully added, false if the posting no longer exists.
// It is called by inserts; a reassign moves its token with moveToken (reassign.go).
// copied from hfresh.append, without the analyze task (see task_queue.go)
func (t *TachiomFresh) append(ctx context.Context, ref tokenRef, centroidID uint64) (bool, error) {
	t.postingLocks.Lock(centroidID)

	// check if the posting still exists
	if !t.Centroids.Exists(centroidID) {
		// the posting might have been deleted concurrently,
		// might happen if we are inserting while
		// background maintenance deletes a posting.
		t.postingLocks.Unlock(centroidID)
		return false, nil
	}

	// append the new entry to the existing posting
	err := t.PostingStore.Append(centroidID, ref)
	if err != nil {
		t.postingLocks.Unlock(centroidID)
		return false, err
	}
	count := uint32(t.postings.append(centroidID, ref))
	t.countEntries(centroidID, 1)

	t.postingLocks.Unlock(centroidID)

	// not hfresh's: the split threshold is the one of the posting's token
	// type, which grows with the type (postingLimits in types.go)
	_, maxPostingSize := t.postingLimits(centroidID)

	if count > maxPostingSize*5 {
		// The entry is in, so a failed split is not the insert's failure:
		// the insert would be retried and place the token again. The split
		// is left to the queue. Not hfresh's, which returns the error.
		if err := t.doSplit(ctx, centroidID, true); err != nil {
			t.logger.WithField("postingID", centroidID).
				Warnf("immediate split failed, enqueued instead: %v", err)
			if err := t.taskQueue.EnqueueSplit(centroidID); err != nil {
				t.logger.WithField("postingID", centroidID).
					Warnf("failed to enqueue the split: %v", err)
			}
		}

		return true, nil
	}

	// If the posting is too big, split it asynchronously.
	if count > maxPostingSize {
		err = t.taskQueue.EnqueueSplit(centroidID)
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

// nearestPosting returns the posting whose centroid is nearest to token,
// creating the first posting of type typ when it has none. The centroids
// are those of type typ (see nearestCentroids).
func (t *TachiomFresh) nearestPosting(token []float32, typ uint32) (uint64, error) {
	targets, err := t.nearestCentroids(token, typ, 1)
	if err != nil {
		return 0, err
	}
	if targets.Len() == 0 {
		return t.ensureInitialPosting(token, typ)
	}
	var postingID uint64
	for id := range targets.Iter() {
		postingID = id
		break
	}
	return postingID, nil
}

// ensureInitialPosting creates a new posting for token v if the index is
// empty. copied from hfresh.ensureInitialPosting; this copy returns the
// posting id, because the ResultSet hfresh's function returns has an
// unexported data field. "Empty" means that type typ has no centroid yet:
// the first token of every type becomes the type's first
// centroid, as the first vector of the index does in hfresh.
func (t *TachiomFresh) ensureInitialPosting(v []float32, typ uint32) (uint64, error) {
	t.initialPostingLock.Lock()
	defer t.initialPostingLock.Unlock()

	// check if a posting was created concurrently
	targets, err := t.nearestCentroids(v, typ, 1)
	if err != nil {
		return 0, err
	}
	for id := range targets.Iter() {
		return id, nil
	}

	// no postings were found, create a new posting while holding the lock
	postingID, err := t.IDs.Next()
	if err != nil {
		return 0, err
	}
	if err = t.types.register(postingID, typ); err != nil {
		return 0, err
	}
	// not hfresh's: the id sequence and the type reach the disk before
	// the centroid (see writeWALs)
	if err = t.writeWALs(); err != nil {
		return 0, err
	}
	// use the vector as the centroid and register it in the centroid index
	err = t.Centroids.Insert(postingID, &hfresh.Centroid{
		Uncompressed: v,
		Deleted:      false,
	})
	if err != nil {
		return 0, errors.Wrapf(err, "failed to upsert new centroid %d", postingID)
	}
	t.types.join(postingID, typ)
	return postingID, nil
}

// copied from hfresh.AddMultiBatch, without the muvera check
func (t *TachiomFresh) AddMultiBatch(ctx context.Context, docIDs []uint64, vectors [][][]float32) error {
	if len(docIDs) != len(vectors) {
		return errors.Errorf("ids and vectors sizes do not match")
	}
	if len(docIDs) == 0 {
		return errors.Errorf("addMultiBatch called with empty lists")
	}

	for i, docID := range docIDs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := t.AddMulti(ctx, docID, vectors[i]); err != nil {
			return err
		}
	}

	return nil
}

// copied from hfresh.ValidateMultiBeforeInsert; this copy checks against
// the published dims, hfresh's function against the muvera encoder's
func (t *TachiomFresh) ValidateMultiBeforeInsert(vectors [][]float32) error {
	if len(vectors) == 0 {
		return errors.New("multi-vector cannot be empty")
	}

	firstDim := len(vectors[0])
	if firstDim == 0 {
		return errors.New("multi-vector cannot be empty")
	}
	for i, v := range vectors[1:] {
		if len(v) != firstDim {
			return fmt.Errorf("multi-vector has inconsistent dimensions: vector[0] has %d but vector[%d] has %d",
				firstDim, i+1, len(v))
		}
	}

	if dims := atomic.LoadUint32(&t.dims); dims != 0 && firstDim != int(dims) {
		return fmt.Errorf("new node has a multi-vector with dimension %d. "+
			"Existing nodes have dimensions %d", firstDim, dims)
	}

	return nil
}
