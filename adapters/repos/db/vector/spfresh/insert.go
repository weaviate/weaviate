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
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

func (s *SPFresh) Add(ctx context.Context, id uint64, vector []float32) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// track the dimensions of the vectors to ensure they are consistent
	s.trackDimensionsOnce.Do(func() {
		s.dims.Store(int32(len(vector)))
	})

	v := distancer.Normalize(vector)
	// TODO i need to understand how RQ should be working more, some code assumes
	// vector is 73 bytes vs 64?
	compressed := compressionhelpers.RQCode(s.Quantizer.Encode(v)).Bytes()

	return s.addOne(ctx, id, compressed)
}

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

// addOne adds a vector into one or more postings.
// The number of replicas is determined by the UserConfig.Replicas setting.
func (s *SPFresh) addOne(ctx context.Context, id uint64, vector []byte) error {
	// TODO can this ever return 0 replicas even if a posting/centroid already exists?
	replicas, _, err := s.selectReplicas(vector, 0)
	if err != nil {
		return err
	}
	s.VersionMap.AllocPageFor(id)
	v := NewVector(id, s.VersionMap.Get(id), vector)
	// if there are no postings found, ensure an initial posting is created
	if len(replicas) == 0 {
		replicas, err = s.ensureInitialPosting(ctx, v)
		if err != nil {
			return err
		}
	}

	for _, replica := range replicas {
		_, err = s.append(ctx, v, replica, false)
		if err != nil {
			return errors.Wrapf(err, "failed to append vector %d to replica %d", id, replica)
		}
	}

	return nil
}

// ensureInitialPosting creates a new posting for Vector v if no replicas are found and ensures
// using a critical section to avoid multiple initial postings being created concurrently.
func (s *SPFresh) ensureInitialPosting(ctx context.Context, v Vector) ([]uint64, error) {
	s.initialPostingLock.Lock()
	defer s.initialPostingLock.Unlock()

	// check if a posting was created concurrently
	replicas, _, err := s.selectReplicas(v.Data(), 0)
	if err != nil {
		return nil, err
	}
	// if no replicas were found, create a new posting while holding the lock
	if len(replicas) == 0 {
		postingID := s.IDs.Next()
		s.PostingSizes.AllocPageFor(postingID)
		// use the vector as the centroid and register it in the SPTAG
		err = s.SPTAG.Upsert(postingID, v.Data())
		if err != nil {
			return nil, errors.Wrapf(err, "failed to upsert new centroid %d", postingID)
		}
		// return the new posting ID
		replicas = append(replicas, postingID)
	}
	return replicas, nil
}

// append adds a vector to the specified posting.
// It returns true if the vector was successfully added, false if the posting no longer exists.
func (s *SPFresh) append(ctx context.Context, vector Vector, postingID uint64, reassigned bool) (bool, error) {
	s.postingLocks.Lock(postingID)

	// check if the posting still exists
	if !s.SPTAG.Exists(postingID) {
		// the vector might have been deleted concurrently,
		// might happen if we are reassigning
		if s.VersionMap.Get(vector.ID()) == vector.Version() {
			err := s.enqueueReassign(ctx, postingID, vector)
			s.postingLocks.Unlock(postingID)
			if err != nil {
				return false, err
			}
		}

		return false, nil
	}

	// append the new vector to the existing posting
	err := s.Store.Merge(ctx, postingID, vector)
	if err != nil {
		s.postingLocks.Unlock(postingID)
		return false, err
	}
	// increment the size of the posting
	s.PostingSizes.Inc(postingID, 1)

	s.postingLocks.Unlock(postingID)

	// ensure the posting size is within the configured limits
	count := s.PostingSizes.Get(postingID)

	// If the posting is too big, we need to split it.
	// During an insert, we want to split asynchronously
	// however during a reassign, we want to split immediately.
	// Also, reassign operations may cause the posting to grow beyond the max size
	// temporarily. To avoid triggering unnecessary splits, we add a fine-tuned threshold.
	max := s.UserConfig.MaxPostingSize
	if reassigned {
		max += reassignThreshold
	}
	if count > max {
		if reassigned {
			err = s.doSplit(postingID)
		} else {
			err = s.enqueueSplit(ctx, postingID)
		}
		if err != nil {
			return false, err
		}
	}

	return true, nil
}

func (s *SPFresh) ValidateBeforeInsert(vector []float32) error {
	if s.dims.Load() == 0 {
		return nil
	}

	if dims := int(s.dims.Load()); len(vector) != dims {
		return fmt.Errorf("new node has a vector with length %v. "+
			"Existing nodes have vectors with length %v", len(vector), dims)
	}

	return nil
}
