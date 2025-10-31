//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package spfresh

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

func (s *SPFresh) AddBatch(ctx context.Context, ids []uint64, vectors [][]float32) error {
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

		err = s.Add(ctx, id, vectors[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *SPFresh) Add(ctx context.Context, id uint64, vector []float32) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}

	start := time.Now()
	defer s.metrics.InsertVector(start)

	vector = s.normalizeVec(vector)

	// init components that require knowing the vector dimensions
	// and compressed size
	s.initDimensionsOnce.Do(func() {
		s.dims = int32(len(vector))
		s.setMaxPostingSize()
		if s.config.Compressed {
			s.quantizer = compressionhelpers.NewRotationalQuantizer(int(s.dims), 42, 8, s.config.DistanceProvider)
			s.vectorSize = int32(compressedVectorSize(int(s.dims)))
		} else {
			s.vectorSize = s.dims * 4
		}
		s.distancer = &Distancer{
			quantizer: s.quantizer,
			distancer: s.config.DistanceProvider,
		}
		s.PostingStore.Init(s.vectorSize, s.config.Compressed)
	})

	// add the vector to the version map.
	// TODO: if the vector already exists, invalidate all previous instances
	// by incrementing the version
	s.VersionMap.AllocPageFor(id)
	version, ok := s.VersionMap.Increment(0, id)
	if !ok {
		panic("version map increment failed for new vector")
	}

	var v Vector

	var compressed []byte

	if s.config.Compressed {
		compressed = s.quantizer.Encode(vector)
		v = NewCompressedVector(id, version, compressed)
	} else {
		v = NewRawVector(id, version, vector)
	}

	targets, _, err := s.RNGSelect(vector, 0)
	if err != nil {
		return err
	}

	// if there are no postings found, ensure an initial posting is created
	if targets.Len() == 0 {
		targets, err = s.ensureInitialPosting(vector, compressed)
		if err != nil {
			return err
		}
	}

	for id := range targets.Iter() {
		_, err = s.append(ctx, v, id, false)
		if err != nil {
			return errors.Wrapf(err, "failed to append vector %d to posting %d", id, id)
		}
	}

	return nil
}

func (s *SPFresh) normalizeVec(vec []float32) []float32 {
	if s.config.DistanceProvider.Type() == "cosine-dot" {
		// cosine-dot requires normalized vectors, as the dot product and cosine
		// similarity are only identical if the vector is normalized
		return distancer.Normalize(vec)
	}
	return vec
}

// ensureInitialPosting creates a new posting for vector v if the index is empty
func (s *SPFresh) ensureInitialPosting(v []float32, compressed []byte) (*ResultSet, error) {
	s.initialPostingLock.Lock()
	defer s.initialPostingLock.Unlock()

	// check if a posting was created concurrently
	targets, _, err := s.RNGSelect(v, 0)
	if err != nil {
		return nil, err
	}

	// if no postings were found, create a new posting while holding the lock
	if targets.Len() == 0 {
		postingID := s.IDs.Next()
		// use the vector as the centroid and register it in the SPTAG
		err = s.Centroids.Insert(postingID, &Centroid{
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
func (s *SPFresh) append(ctx context.Context, vector Vector, centroidID uint64, reassigned bool) (bool, error) {
	s.postingLocks.Lock(centroidID)

	// check if the posting still exists
	if !s.Centroids.Exists(centroidID) {
		// the posting might have been deleted concurrently,
		// might happen if we are reassigning
		if s.VersionMap.Get(vector.ID()) == vector.Version() {
			err := s.taskQueue.EnqueueReassign(ctx, centroidID, vector.ID(), vector.Version())
			if err != nil {
				s.postingLocks.Unlock(centroidID)
				return false, err
			}
		}

		s.postingLocks.Unlock(centroidID)
		return false, nil
	}

	// append the new vector to the existing posting
	err := s.PostingStore.Append(ctx, centroidID, vector)
	if err != nil {
		s.postingLocks.Unlock(centroidID)
		return false, err
	}

	// increment the size of the posting
	count, err := s.PostingSizes.Inc(ctx, centroidID, 1)
	if err != nil {
		s.postingLocks.Unlock(centroidID)
		return false, err
	}

	s.postingLocks.Unlock(centroidID)

	// If the posting is too big, we need to split it.
	// During an insert, we want to split asynchronously
	// however during a reassign, we want to split immediately.
	// Also, reassign operations may cause the posting to grow beyond the max size
	// temporarily. To avoid triggering unnecessary splits, we add a fine-tuned threshold.
	max := s.maxPostingSize
	if reassigned {
		max += reassignThreshold
	}
	if count > max {
		if reassigned {
			err = s.doSplit(centroidID, false)
		} else {
			err = s.taskQueue.EnqueueSplit(ctx, centroidID)
		}
		if err != nil {
			return false, err
		}
	}

	return true, nil
}

func (s *SPFresh) ValidateBeforeInsert(vector []float32) error {
	if s.dims == 0 {
		return nil
	}

	if dims := int(s.dims); len(vector) != dims {
		return fmt.Errorf("new node has a vector with length %v. "+
			"Existing nodes have vectors with length %v", len(vector), dims)
	}

	return nil
}
