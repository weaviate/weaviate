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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/multivector"
	"github.com/weaviate/weaviate/entities/vectorindex/compression"
)

func (h *HFresh) AddBatch(ctx context.Context, ids []uint64, vectors [][]float32) error {
	if len(ids) != len(vectors) {
		return errors.Errorf("ids and vectors sizes does not match")
	}
	if len(ids) == 0 {
		return errors.Errorf("insertBatch called with empty lists")
	}

	for i, id := range ids {
		err := ctx.Err()
		if err != nil {
			return err
		}

		err = h.Add(ctx, id, vectors[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func (h *HFresh) Add(ctx context.Context, id uint64, vector []float32) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	start := time.Now()
	defer h.metrics.InsertVector(start)

	vector = h.normalizeVec(vector)

	err := h.initDimensions(vector)
	if err != nil {
		return err
	}

	var v Vector

	compressed := h.quantizer.CompressedBytes(h.quantizer.Encode(vector))
	v = NewVector(id, VectorVersion(1), compressed)

	targets, _, err := h.RNGSelect(vector, 0)
	if err != nil {
		return err
	}

	// if there are no postings found, ensure an initial posting is created
	if targets.Len() == 0 {
		targets, err = h.ensureInitialPosting(vector, compressed)
		if err != nil {
			return err
		}
	}

	for postingID := range targets.Iter() {
		added, err := h.append(ctx, v, postingID, false)
		if err != nil {
			return errors.Wrapf(err, "failed to append vector %d to posting %d", v.ID(), postingID)
		}
		if !added {
			err = h.taskQueue.EnqueueReassign(postingID, v.ID())
			if err != nil {
				return errors.Wrapf(err, "failed to enqueue reassign for vector %d after posting %d disappeared", v.ID(), postingID)
			}
		}
	}

	return nil
}

// initDimensions initializes dimension-dependent components (quantizer, distancer,
// posting sizes) on the first vector received. Uses a mutex+flag pattern instead of
// sync.Once so that initialization can be retried if it fails.
func (h *HFresh) initDimensions(vector []float32) error {
	h.initMu.Lock()
	defer h.initMu.Unlock()

	if h.initDone {
		return nil
	}

	size := uint32(len(vector))
	atomic.StoreUint32(&h.dims, size)

	if err := h.setMaxPostingSize(); err != nil {
		return err
	}
	if err := h.IndexMetadata.SetDimensions(size); err != nil {
		return errors.Wrap(err, "could not persist dimensions")
	}

	quantizer, err := compressionhelpers.NewBinaryRotationalQuantizer(int(h.dims), 42, h.config.DistanceProvider)
	if err != nil {
		return errors.Wrap(err, "could not create quantizer")
	}
	h.quantizer = quantizer
	h.Centroids.SetQuantizer(h.quantizer)

	if err := h.persistQuantizationData(); err != nil {
		return errors.Wrap(err, "could not persist RQ data")
	}

	h.distancer = NewDistancer(h.quantizer, h.config.DistanceProvider)
	h.initDone = true
	return nil
}

func (h *HFresh) normalizeVec(vec []float32) []float32 {
	if h.config.DistanceProvider.Type() == "cosine-dot" {
		// cosine-dot requires normalized vectors, as the dot product and cosine
		// similarity are only identical if the vector is normalized
		return distancer.Normalize(vec)
	}
	return vec
}

// ensureInitialPosting creates a new posting for vector v if the index is empty
func (h *HFresh) ensureInitialPosting(v []float32, compressed []byte) (*ResultSet, error) {
	h.initialPostingLock.Lock()
	defer h.initialPostingLock.Unlock()

	// check if a posting was created concurrently
	targets, _, err := h.RNGSelect(v, 0)
	if err != nil {
		return nil, err
	}

	// if no postings were found, create a new posting while holding the lock
	if targets.Len() == 0 {
		postingID, err := h.IDs.Next()
		if err != nil {
			return nil, err
		}
		// use the vector as the centroid and register it in the SPTAG
		err = h.Centroids.Insert(postingID, &Centroid{
			Uncompressed: v,
			Compressed:   compressed,
			Deleted:      false,
		})
		if err != nil {
			return nil, errors.Wrapf(err, "failed to upsert new centroid %d", postingID)
		}
		// return the new posting ID
		targets = NewResultSet(1)
		targets.data = append(targets.data, Result{ID: postingID, Distance: 0})
	}

	return targets, nil
}

// Append adds a vector to the specified posting.
// It returns true if the vector was successfully added, false if the posting no longer exists.
// It is called synchronously during imports but also asynchronously by reassign operations.
func (h *HFresh) append(ctx context.Context, vector Vector, centroidID uint64, reassigned bool) (bool, error) {
	h.postingLocks.Lock(centroidID)

	// check if the posting still exists
	if !h.Centroids.Exists(centroidID) {
		// the posting might have been deleted concurrently,
		// might happen if we are reassigning or inserting while
		// background maintenance deletes a posting.
		h.postingLocks.Unlock(centroidID)
		return false, nil
	}

	// append the new vector to the existing posting
	err := h.PostingStore.Append(ctx, centroidID, vector)
	if err != nil {
		h.postingLocks.Unlock(centroidID)
		return false, err
	}

	// update the posting membership and size caches
	_, err = h.PostingMap.FastAddVectorID(ctx, centroidID, vector.ID())
	if err != nil {
		h.postingLocks.Unlock(centroidID)
		return false, err
	}
	count := h.PostingSizes.FastIncrement(centroidID)

	h.postingLocks.Unlock(centroidID)

	if !reassigned {
		if count > h.maxPostingSize*5 {
			err = h.doSplit(ctx, centroidID, true)
			if err != nil {
				return false, err
			}

			return true, nil
		}

		// If the posting is too big, split it asynchronously.
		if count > h.maxPostingSize {
			err = h.taskQueue.EnqueueSplit(centroidID)
			if err != nil {
				return false, err
			}

			return true, nil
		}

		// enqueue an analyze operation to persist the changes and update the posting map on disk
		err = h.taskQueue.EnqueueAnalyze(centroidID)
		if err != nil {
			return false, err
		}
		return true, nil
	}

	// If the posting is too big, we need to split it.
	// During an insert, we want to split asynchronously
	// however during a reassign, we want to split immediately.
	max := h.maxPostingSize
	if count > max {
		err = h.doSplit(ctx, centroidID, false)
	} else {
		// enqueue an analyze operation to persist the changes and update the posting map on disk
		err = h.taskQueue.EnqueueAnalyze(centroidID)
	}
	if err != nil {
		return false, err
	}

	return true, nil
}

func (h *HFresh) AddMulti(ctx context.Context, docID uint64, vectors [][]float32) error {
	if !h.muvera.Load() {
		h.logger.Error(ErrMuveraNotEnabled)
		return ErrMuveraNotEnabled
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(vectors) == 0 || len(vectors[0]) == 0 {
		return errors.New("multi-vector cannot be empty")
	}

	var initErr error
	h.trackMuveraOnce.Do(func() {
		h.muveraEncoder.InitEncoder(len(vectors[0]))
		capture := &muveraDataCapture{}
		if err := h.muveraEncoder.PersistMuvera(capture); err != nil {
			initErr = errors.Wrap(err, "persist muvera data")
			return
		}
		if err := h.IndexMetadata.SetMuveraData(capture.data); err != nil {
			initErr = errors.Wrap(err, "store muvera data in index metadata")
		}
	})
	if initErr != nil {
		return initErr
	}

	encoded := h.muveraEncoder.EncodeDoc(vectors)

	idBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idBytes, docID)
	if err := h.store.Bucket(h.id+"_muvera_vectors").Put(idBytes, multivector.MuveraBytesFromFloat32(encoded)); err != nil {
		return errors.Wrap(err, "put muvera vector into bucket")
	}

	return h.Add(ctx, docID, encoded)
}

func (h *HFresh) AddMultiBatch(ctx context.Context, docIDs []uint64, vectors [][][]float32) error {
	if !h.muvera.Load() {
		h.logger.Error(ErrMuveraNotEnabled)
		return ErrMuveraNotEnabled
	}
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
		if err := h.AddMulti(ctx, docID, vectors[i]); err != nil {
			return err
		}
	}

	return nil
}

func (h *HFresh) ValidateMultiBeforeInsert(vectors [][]float32) error {
	if !h.muvera.Load() {
		return errors.New("multi-vector not supported: muvera is not enabled")
	}
	if len(vectors) == 0 {
		return errors.New("multi-vector cannot be empty")
	}

	firstDim := len(vectors[0])
	for i, v := range vectors[1:] {
		if len(v) != firstDim {
			return fmt.Errorf("multi-vector has inconsistent dimensions: vector[0] has %d but vector[%d] has %d",
				firstDim, i+1, len(v))
		}
	}

	if muveraDims := h.muveraEncoder.Dimensions(); muveraDims != 0 && firstDim != muveraDims {
		return fmt.Errorf("new node has a multi-vector with dimension %d. "+
			"Existing nodes have dimensions %d", firstDim, muveraDims)
	}

	return nil
}

// muveraDataCapture implements multivector.CommitLogger to capture muvera data
// so it can be stored in the index metadata store.
type muveraDataCapture struct {
	data *compression.MuveraData
}

func (m *muveraDataCapture) AddMuvera(data compression.MuveraData) error {
	m.data = &data
	return nil
}

func (h *HFresh) ValidateBeforeInsert(vector []float32) error {
	dims := atomic.LoadUint32(&h.dims)
	if dims == 0 {
		return nil
	}

	if len(vector) != int(dims) {
		return fmt.Errorf("new node has a vector with length %v. "+
			"Existing nodes have vectors with length %v", len(vector), dims)
	}

	return nil
}
