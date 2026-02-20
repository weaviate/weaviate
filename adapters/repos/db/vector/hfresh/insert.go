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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
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

func (h *HFresh) Add(ctx context.Context, id uint64, vector []float32) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}

	start := time.Now()
	defer h.metrics.InsertVector(start)

	vector = h.normalizeVec(vector)

	// init components that require knowing the vector dimensions
	// and compressed size
	h.initDimensionsOnce.Do(func() {
		size := uint32(len(vector))
		atomic.StoreUint32(&h.dims, size)
		err = h.setMaxPostingSize()
		if err != nil {
			return
		}
		err = h.IndexMetadata.SetDimensions(size)
		if err != nil {
			err = errors.Wrap(err, "could not persist dimensions")
			return // Fail the entire initialization
		}
		h.quantizer = compressionhelpers.NewBinaryRotationalQuantizer(int(h.dims), 42, h.config.DistanceProvider)
		h.Centroids.SetQuantizer(h.quantizer)

		if err = h.persistQuantizationData(); err != nil {
			err = errors.Wrap(err, "could not persist RQ data")
			return // Fail the entire initialization
		}

		h.distancer = NewDistancer(h.quantizer, h.config.DistanceProvider)
	})
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

	for id := range targets.Iter() {
		_, err = h.append(ctx, v, id, false)
		if err != nil {
			return errors.Wrapf(err, "failed to append vector %d to posting %d", id, id)
		}
	}

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
		// might happen if we are reassigning
		version, err := h.VersionMap.Get(h.ctx, vector.ID())
		if err != nil {
			return false, err
		}
		if version == vector.Version() {
			err := h.taskQueue.EnqueueReassign(centroidID, vector.ID(), vector.Version())
			if err != nil {
				h.postingLocks.Unlock(centroidID)
				return false, err
			}
		}

		h.postingLocks.Unlock(centroidID)
		return false, nil
	}

	// append the new vector to the existing posting
	err := h.PostingStore.Append(ctx, centroidID, vector)
	if err != nil {
		h.postingLocks.Unlock(centroidID)
		return false, err
	}

	// increment the size of the posting
	count, err := h.PostingMap.FastAddVectorID(ctx, centroidID, vector.ID(), vector.Version())
	if err != nil {
		h.postingLocks.Unlock(centroidID)
		return false, err
	}

	h.postingLocks.Unlock(centroidID)

	if !reassigned {
		// If the posting is way too big, we need to split it immediately.
		if count > h.maxPostingSize*5 {
			err = h.doSplit(ctx, centroidID, true)
			if err != nil {
				return false, err
			}

			return true, nil
		} else if count > h.maxPostingSize {
			err = h.taskQueue.EnqueueSplit(centroidID)
			if err != nil {
				return false, err
			}
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
