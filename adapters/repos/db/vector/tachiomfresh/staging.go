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
	"encoding/binary"
	"math"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hfresh"
)

// The two phases of an index: up to defaultCenterTokens tokens, an
// exhaustive search over the staged documents; from there on, the index.
//
// Under defaultCenterTokens document tokens the index holds no centroid
// and no posting. Every document inserted goes, with its token types, into
// the staging bucket, keyed by its doc id, and a search scores every
// staged document exactly (stagedSearch). The flat index does the same
// with its vectors (flat.storeGenericVector, flat.findTopVectors over the
// bucket's cursor), and the dynamic index runs the same way until its
// threshold. When the count of staged tokens first reaches the threshold,
// the index is built from the staging bucket (buildFromStaging). The mean
// of every staged token becomes the centering mean (TachiomFresh.center,
// see below). The staged documents are inserted as the normal insert path
// inserts them, and the staging bucket is emptied. Every document after
// that takes the normal path. The mean is taken over every token held at
// that moment, so the whole document, or batch, that crosses the threshold
// contributes: all the initial tokens should contribute, for a better
// estimate of the mean. Documents reach the index one at a time or in
// batches from the shard's queue, and the index is not told whether more
// will follow. The phase is persisted in the type bucket (phaseKey): a
// restart under the threshold finds the staged documents again, as the
// flat index finds its vectors.
//
// hnsw's rule for its centered RQ has the same shape: raw until
// RQ.TrainingLimit vectors (10,000), then one fit on what is there
// (hnsw/compress.go). hnsw re-encodes what it indexed raw; here nothing
// was indexed, so nothing moves.
//
// Centering. Once the index has a mean, every document token is centered
// (the mean is subtracted, centerVec) before it meets a centroid: at the
// insert, in a split, in a reassign, and in the merge and neighbour
// checks. The centroids are therefore in the centered space. Later inserts
// and deletes never change the mean, as hnsw keeps the mean of its
// centered RQ once it is fitted. A query token is never centered: it
// searches the centroid HNSW raw. <q, v - u> = <q, v> - <q, u> is the raw
// dot product minus a constant per query token, so stage 1 ranks the
// centroids as if nothing had been centered. Stage 3 scores the original
// tokens, which the shard stores unchanged. Centering rests on the dot
// distance, the only one the index accepts (New).

// defaultCenterTokens is how many document tokens an index stages before
// it is built, hnsw's RQ.TrainingLimit. Under 10,000 tokens a corpus has
// about a hundred documents, and an exhaustive search over them is cheap.
const defaultCenterTokens = 10000

// The persisted phase of the index (phaseKey in the type bucket).
// initDimensions writes it with the dimensions and writes both out before
// the first document is staged. No key on an index with centroids means
// built: the index was saved before the phase existed. No key and no
// centroid means nothing was placed yet, and restore treats the index as
// new.
const (
	phaseUnset   byte = 0
	phaseStaging byte = 's'
	phaseBuilt   byte = 'b'
)

// stagingStore is the bucket that holds the staged documents. The key is
// the doc id (8 bytes big-endian, as the flat index keys its vectors). The
// value is the document's tokens and types (encodeStaged). The store has
// its own bucket, as the posting store and the type table have.
type stagingStore struct {
	store  *lsmkv.Store
	bucket string
}

func newStagingStore(store *lsmkv.Store, id string, cfg hfresh.StoreConfig) (*stagingStore, error) {
	name := helpers.TachiomFreshStagingBucketName(id)
	err := store.CreateOrLoadBucket(context.Background(), name,
		cfg.MakeBucketOptions(lsmkv.StrategyReplace)...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create or load bucket %s", name)
	}
	return &stagingStore{store: store, bucket: name}, nil
}

// acquire pins the bucket for one operation, as the posting store's.
func (s *stagingStore) acquire() (*lsmkv.Bucket, func(), error) {
	bucket, release := s.store.AcquireBucketForRead(s.bucket)
	if bucket == nil {
		release()
		return nil, nil, errors.Wrapf(lsmkv.ErrBucketNotFound, "bucket %s", s.bucket)
	}
	return bucket, release, nil
}

func stagingKey(docID uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, docID)
	return key
}

// encodeStaged lays a document out as tokens (u32), dims (u32), a byte
// set to 1 (the document has token ids), the tokens (f32 each,
// little-endian) and the token ids (u32 each). decodeStaged refuses a
// document whose byte is not 1.
func encodeStaged(vectors [][]float32, types []uint32) []byte {
	dims := len(vectors[0])
	size := 9 + 4*len(vectors)*dims + 4*len(types)
	val := make([]byte, size)
	binary.LittleEndian.PutUint32(val, uint32(len(vectors)))
	binary.LittleEndian.PutUint32(val[4:], uint32(dims))
	val[8] = 1
	off := 9
	for _, vec := range vectors {
		for _, x := range vec {
			binary.LittleEndian.PutUint32(val[off:], math.Float32bits(x))
			off += 4
		}
	}
	for _, typ := range types {
		binary.LittleEndian.PutUint32(val[off:], typ)
		off += 4
	}
	return val
}

func decodeStaged(val []byte) (vectors [][]float32, types []uint32, err error) {
	if len(val) < 9 {
		return nil, nil, errors.Errorf("staged document of %d bytes", len(val))
	}
	n := int(binary.LittleEndian.Uint32(val))
	dims := int(binary.LittleEndian.Uint32(val[4:]))
	if val[8] != 1 {
		return nil, nil, errors.New("staged document without token ids")
	}
	size := 9 + 4*n*dims
	size += 4 * n
	if len(val) != size {
		return nil, nil, errors.Errorf("staged document of %d bytes, %d tokens of %d dims need %d", len(val), n, dims, size)
	}
	flat := make([]float32, n*dims)
	for i := range flat {
		flat[i] = math.Float32frombits(binary.LittleEndian.Uint32(val[9+4*i:]))
	}
	vectors = make([][]float32, n)
	for i := range vectors {
		vectors[i] = flat[i*dims : (i+1)*dims]
	}
	types = make([]uint32, n)
	off := 9 + 4*n*dims
	for i := range types {
		types[i] = binary.LittleEndian.Uint32(val[off+4*i:])
	}
	return vectors, types, nil
}

func (s *stagingStore) put(docID uint64, vectors [][]float32, types []uint32) error {
	bucket, release, err := s.acquire()
	if err != nil {
		return err
	}
	defer release()
	return bucket.Put(stagingKey(docID), encodeStaged(vectors, types))
}

func (s *stagingStore) delete(docID uint64) error {
	bucket, release, err := s.acquire()
	if err != nil {
		return err
	}
	defer release()
	return bucket.Delete(stagingKey(docID))
}

// iterate calls fn with every staged document, doc ids ascending, until fn
// returns an error.
func (s *stagingStore) iterate(fn func(docID uint64, vectors [][]float32, types []uint32) error) error {
	return s.iterateIf(func() bool { return true }, fn)
}

// iterateIf is iterate after a check made with the cursor open, so the cursor
// sees every document the bucket held when the check passed. With ok false it
// returns without calling fn.
func (s *stagingStore) iterateIf(ok func() bool, fn func(docID uint64, vectors [][]float32, types []uint32) error) error {
	bucket, release, err := s.acquire()
	if err != nil {
		return err
	}
	defer release()
	c := bucket.Cursor()
	defer c.Close()
	if !ok() {
		return nil
	}
	for key, val := c.First(); key != nil; key, val = c.Next() {
		if len(key) != 8 {
			return errors.Errorf("staging key of %d bytes", len(key))
		}
		vectors, types, err := decodeStaged(val)
		if err != nil {
			return err
		}
		if err := fn(binary.BigEndian.Uint64(key), vectors, types); err != nil {
			return err
		}
	}
	return nil
}

// stageDocument puts a document into the staging bucket and builds the
// index when the staged tokens reach the threshold. It is called with
// stageMu held and built false.
func (t *TachiomFresh) stageDocument(ctx context.Context, docID uint64, vectors [][]float32, types []uint32) error {
	if t.stagedTokens == 0 {
		// the first staged document: the phase is persisted so that a
		// restart under the threshold reads the staging bucket (restore)
		if err := t.types.setPhase(phaseStaging); err != nil {
			return errors.Wrap(err, "could not persist the index phase")
		}
	}
	// the document starts at hfresh's implicit first version, as on the
	// normal path; a document deleted before is live again (see
	// insertVersion)
	if _, err := t.insertVersion(ctx, docID); err != nil {
		return err
	}
	if err := t.staging.put(docID, vectors, types); err != nil {
		return errors.Wrapf(err, "failed to stage doc %d", docID)
	}
	if old, ok := t.stagedDocs[docID]; ok {
		// the same id again: the entry is replaced
		t.stagedTokens -= uint64(old)
	}
	t.stagedDocs[docID] = uint32(len(vectors))
	t.stagedTokens += uint64(len(vectors))
	t.docs.add(docID)
	if t.stagedTokens < t.centerTokens {
		return nil
	}
	return t.buildFromStaging(ctx)
}

// unstageDocument drops a staged document (Delete). It is called with
// stageMu held and built false.
func (t *TachiomFresh) unstageDocument(docID uint64) error {
	n, ok := t.stagedDocs[docID]
	if !ok {
		return nil
	}
	if err := t.staging.delete(docID); err != nil {
		return errors.Wrapf(err, "failed to drop staged doc %d", docID)
	}
	delete(t.stagedDocs, docID)
	t.stagedTokens -= uint64(n)
	return nil
}

// buildFromStaging turns the staged documents into the index. The mean of
// every staged token becomes the centering mean. A staged document deleted
// meanwhile is not filed (stagedVersion). Whatever an earlier attempt left
// is dropped (resetBuild), so a retried build starts from the same state as
// the first one. The documents are inserted as the normal path inserts
// them, in doc id order. It is called with stageMu held.
//
// The end of the build is ordered for a crash at any point. Everything the
// index holds reaches the disk, then the phase, then the staged documents
// are deleted. A restart that finds the staging phase rebuilds from the
// staging bucket, which still holds every document (restore). A restart that
// finds the built phase has the postings, and deletes what is left of the
// staged documents.
func (t *TachiomFresh) buildFromStaging(ctx context.Context) error {
	dims := int(t.dims)
	sum := make([]float64, dims)
	var n uint64
	err := t.staging.iterate(func(docID uint64, vectors [][]float32, types []uint32) error {
		for _, vec := range vectors {
			for j, x := range vec {
				sum[j] += float64(x)
			}
			n++
		}
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "failed to read the staged documents")
	}
	if n == 0 {
		return errors.New("no staged token to build from")
	}
	mean := make([]float32, dims)
	for j := range mean {
		mean[j] = float32(sum[j] / float64(n))
	}
	if err := t.startBuild(ctx, mean, n); err != nil {
		return err
	}

	err = t.staging.iterate(func(docID uint64, vectors [][]float32, types []uint32) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return t.fileDocument(ctx, docID, vectors, types, true)
	})
	if err != nil {
		return errors.Wrap(err, "failed to index the staged documents")
	}
	if err := t.types.setCenter(mean); err != nil {
		return errors.Wrap(err, "could not persist the centering mean")
	}
	if err := t.Flush(); err != nil {
		return errors.Wrap(err, "could not write out the built index")
	}
	if err := t.types.setPhase(phaseBuilt); err != nil {
		return errors.Wrap(err, "could not persist the index phase")
	}
	var err2 error
	if t.opts.beforeBuiltWAL != nil {
		err2 = t.opts.beforeBuiltWAL()
	}
	if err2 == nil {
		err2 = t.writeWALs()
	}
	if err2 != nil {
		// The build has not ended. The staging phase is written back, so
		// that a later write of the log and a restart rebuild from the
		// staging bucket instead of dropping it (dropLeftoverStaged).
		if err := t.types.setPhase(phaseStaging); err != nil {
			t.logger.Warnf("failed to write the staging phase back after a failed build: %v", err)
		}
		return err2
	}
	t.built.Store(true)
	// the staged documents leave the bucket once the index answers for
	// them: a search that started on the staging bucket still finds them.
	// The index answers from here on, so a failure is not the insert's: a
	// restart deletes what is left (restore).
	for docID := range t.stagedDocs {
		if err := t.staging.delete(docID); err != nil {
			t.logger.WithField("docID", docID).Warnf("failed to drop staged doc: %v", err)
			break
		}
	}
	t.stagedDocs = map[uint64]uint32{}
	t.stagedTokens = 0
	return nil
}

// startBuild drops what an earlier attempt left and installs the mean. The
// task queues are paused meanwhile: a running task could otherwise add a
// centroid behind the reset, or read the mean while it changes. Tasks still
// queued find their postings gone and do nothing.
func (t *TachiomFresh) startBuild(ctx context.Context, mean []float32, tokens uint64) error {
	queues := t.taskQueue.queues()
	defer func() {
		for _, q := range queues {
			q.Resume()
		}
	}()
	for _, q := range queues {
		if err := q.Pause(ctx); err != nil {
			return errors.Wrap(err, "failed to pause the task queues for the build")
		}
	}
	if err := t.resetBuild(); err != nil {
		return err
	}
	t.center = mean
	if t.opts.onCentered != nil {
		t.opts.onCentered(tokens)
	}
	return nil
}

// resetBuild drops what an interrupted build placed: every centroid and
// posting, the type groups, the spread sums and the mean. The staged
// documents, the dimensions and the type recorded per posting id
// stay: a queued reassign may still name a dropped posting, and posting ids
// are never reused. The phase goes back to staging first, so a crash during
// the reset finds the staging phase. No task runs meanwhile: the queues are
// paused, or not started yet in restore.
func (t *TachiomFresh) resetBuild() error {
	var centroids []uint64
	t.Centroids.hnsw.Iterate(func(id uint64) bool {
		if t.Centroids.Exists(id) {
			centroids = append(centroids, id)
		}
		return true
	})
	if len(centroids) > 0 {
		if err := t.types.setPhase(phaseStaging); err != nil {
			return errors.Wrap(err, "could not persist the index phase")
		}
		if err := t.writeWALs(); err != nil {
			return err
		}
	}
	for _, id := range centroids {
		if err := t.Centroids.MarkAsDeleted(id); err != nil {
			return errors.Wrapf(err, "failed to delete centroid %d of an interrupted build", id)
		}
		t.dropPosting(id)
	}
	if err := t.types.resetGroups(); err != nil {
		return errors.Wrap(err, "failed to reset the type groups")
	}
	t.center = nil
	return nil
}

// dropLeftoverStaged deletes the staged documents of an index whose phase is
// built: a crash came after the phase was written and before the deletes
// finished (buildFromStaging).
func (t *TachiomFresh) dropLeftoverStaged() error {
	var leftover []uint64
	err := t.staging.iterate(func(docID uint64, vectors [][]float32, types []uint32) error {
		leftover = append(leftover, docID)
		return nil
	})
	if err != nil {
		return err
	}
	for _, docID := range leftover {
		if err := t.staging.delete(docID); err != nil {
			return errors.Wrapf(err, "failed to drop staged doc %d", docID)
		}
	}
	return nil
}

// restoreStaging reloads the staged documents of an index reopened under
// the threshold: their token counts, and the doc set minus the deleted
// documents. The tokens stay in the bucket.
func (t *TachiomFresh) restoreStaging() error {
	t.stageMu.Lock()
	defer t.stageMu.Unlock()
	return t.staging.iterate(func(docID uint64, vectors [][]float32, types []uint32) error {
		// a document the version store does not hold was never deleted:
		// the store sees deletes and increments only
		t.VersionMap.EnsureDefault(docID)
		version, err := t.VersionMap.Get(t.ctx, docID)
		if err != nil {
			return errors.Wrapf(err, "failed to restore the version of doc %d", docID)
		}
		if version.Deleted() {
			// the entry is dropped now
			return t.staging.delete(docID)
		}
		t.docs.add(docID)
		t.stagedDocs[docID] = uint32(len(vectors))
		t.stagedTokens += uint64(len(vectors))
		return nil
	})
}

// stagedSearch scores every staged document exactly. It is the search of
// an index under the threshold. It computes the MaxSim distance of the
// query against the tokens the staging bucket holds, applies the allow
// list and the deleted documents, and returns the top k as
// SearchByMultiVector returns them. A build publishes built before it deletes
// the staged documents, so built is read again with the cursor open: if it is
// still false, the cursor sees every document. If it became true, stale is
// true and the caller runs the search of the built index.
func (t *TachiomFresh) stagedSearch(ctx context.Context, vectors [][]float32, k int, allow helpers.AllowList) (ids []uint64, dists []float32, stale bool, err error) {
	results := hfresh.NewResultSet(k)
	notBuilt := func() bool {
		stale = t.built.Load()
		return !stale
	}
	err = t.staging.iterateIf(notBuilt, func(docID uint64, docVectors [][]float32, types []uint32) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !t.docs.contains(docID) || (allow != nil && !allow.Contains(docID)) {
			return nil
		}
		score, err := t.maxSimScore(vectors, docVectors)
		if err != nil {
			return errors.Wrapf(err, "compute maxsim for id %d", docID)
		}
		results.Insert(docID, score)
		return nil
	})
	if err != nil || stale {
		return nil, nil, stale, err
	}
	ids = make([]uint64, results.Len())
	dists = make([]float32, results.Len())
	i := 0
	for id, dist := range results.Iter() {
		ids[i] = id
		dists[i] = dist
		i++
	}
	return ids, dists, false, nil
}
