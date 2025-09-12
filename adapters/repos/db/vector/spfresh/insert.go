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

	"github.com/pkg/errors"
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

func (s *SPFresh) Add(ctx context.Context, id uint64, vector []float32) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	var compressed []byte

	// init components that require knowing the vector dimensions
	// and compressed size
	s.initDimensionsOnce.Do(func() {
		s.dims = int32(len(vector))
		s.SPTAG.Init(s.dims, s.Config.Distancer)
		compressed = s.SPTAG.Quantizer().Encode(vector)
		s.distancer = &Distancer{
			quantizer: s.SPTAG.Quantizer(),
			distancer: s.Config.Distancer,
		}
		s.vectorSize = int32(len(compressed))
		s.Store.Init(s.vectorSize)
	})

	if compressed == nil {
		compressed = s.SPTAG.Quantizer().Encode(vector)
	}

	s.VersionMap.AllocPageFor(id)

	version, _ := s.VersionMap.Increment(0, id)

	v := NewCompressedVector(id, version, compressed)

	// TODO can this ever return 0 replicas even if a posting/centroid already exists?
	replicas, _, err := s.selectReplicas(v, 0)
	if err != nil {
		return err
	}

	// if there are no postings found, ensure an initial posting is created
	if len(replicas) == 0 {
		replicas, err = s.ensureInitialPosting(v)
		if err != nil {
			return err
		}
	}

	for _, replica := range replicas {
		_, err = s.append(ctx, v, replica, false)
		if err != nil {
			return errors.Wrapf(err, "failed to append vector %d to replica %d", id, replica.ID)
		}
	}

	return nil
}

// ensureInitialPosting creates a new posting for Vector v if no replicas are found and ensures
// using a critical section to avoid multiple initial postings being created concurrently.
func (s *SPFresh) ensureInitialPosting(v Vector) ([]SearchResult, error) {
	s.initialPostingLock.Lock()
	defer s.initialPostingLock.Unlock()

	// check if a posting was created concurrently
	replicas, _, err := s.selectReplicas(v, 0)
	if err != nil {
		return nil, err
	}

	// if no replicas were found, create a new posting while holding the lock
	if len(replicas) == 0 {
		postingID := s.IDs.Next()
		s.PostingSizes.AllocPageFor(postingID)
		// use the vector as the centroid and register it in the SPTAG
		err = s.SPTAG.Upsert(postingID, &Centroid{
			Vector: v,
		})
		if err != nil {
			return nil, errors.Wrapf(err, "failed to upsert new centroid %d", postingID)
		}
		// return the new posting ID
		replicas = append(replicas, SearchResult{
			ID: postingID,
			// distance is zero since we are using the vector as the centroid
			Distance: 0,
		})
	}

	return replicas, nil
}

// append adds a vector to the specified posting.
// It returns true if the vector was successfully added, false if the posting no longer exists.
func (s *SPFresh) append(ctx context.Context, vector Vector, centroid SearchResult, reassigned bool) (bool, error) {
	s.postingLocks.Lock(centroid.ID)

	// check if the posting still exists
	if !s.SPTAG.Exists(centroid.ID) {
		// the posting might have been deleted concurrently,
		// might happen if we are reassigning
		if s.VersionMap.Get(vector.ID()) == vector.Version() {
			err := s.enqueueReassign(ctx, centroid.ID, vector)
			if err != nil {
				s.postingLocks.Unlock(centroid.ID)
				return false, err
			}
		}

		s.postingLocks.Unlock(centroid.ID)
		return false, nil
	}

	// append the new vector to the existing posting
	err := s.Store.Merge(ctx, centroid.ID, vector)
	if err != nil {
		s.postingLocks.Unlock(centroid.ID)
		return false, err
	}
	// Update the radius of the associated centroid
	// Note: only update PostingSizes after the centroid update succeeded

	// Update the radius of the associated centroid
	prev := s.SPTAG.Get(centroid.ID)
	err = s.SPTAG.Upsert(centroid.ID, &Centroid{
		Vector: prev.Vector,
		Radius: max(prev.Radius, centroid.Distance),
	})
	if err != nil {
		s.postingLocks.Unlock(centroid.ID)
		return false, err
	}

	// increment the size of the posting after successful Store.Merge and SPTAG.Upsert
	s.PostingSizes.Inc(centroid.ID, 1)

	s.postingLocks.Unlock(centroid.ID)

	// ensure the posting size is within the configured limits
	count := s.PostingSizes.Get(centroid.ID)

	// If the posting is too big, we need to split it.
	// During an insert, we want to split asynchronously
	// however during a reassign, we want to split immediately.
	// Also, reassign operations may cause the posting to grow beyond the max size
	// temporarily. To avoid triggering unnecessary splits, we add a fine-tuned threshold.
	max := s.Config.MaxPostingSize
	if reassigned {
		max += reassignThreshold
	}
	if count > max {
		if reassigned {
			err = s.doSplit(centroid.ID, false)
		} else {
			err = s.enqueueSplit(ctx, centroid.ID)
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
