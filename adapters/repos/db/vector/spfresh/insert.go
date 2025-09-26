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

	var compressed []byte

	// init components that require knowing the vector dimensions
	// and compressed size
	s.initDimensionsOnce.Do(func() {
		s.dims = int32(len(vector))
		s.setMaxPostingSize()
		s.SPTAG.Init(s.dims, s.config.Distancer)
		compressed = s.SPTAG.Quantizer().Encode(vector)
		s.distancer = &Distancer{
			quantizer: s.SPTAG.Quantizer(),
			distancer: s.config.Distancer,
		}
		s.vectorSize = int32(len(compressed))
		s.Store.Init(s.vectorSize)
	})

	if compressed == nil {
		compressed = s.SPTAG.Quantizer().Encode(vector)
	}

	// add the vector to the version map.
	// TODO: if the vector already exists, invalidate all previous instances
	// by incrementing the version
	s.VersionMap.AllocPageFor(id)
	version, _ := s.VersionMap.Increment(0, id)

	v := NewCompressedVector(id, version, compressed)

	targets, _, err := s.RNGSelect(v, 0)
	if err != nil {
		return err
	}

	// if there are no postings found, ensure an initial posting is created
	if len(targets) == 0 {
		targets, err = s.ensureInitialPosting(v)
		if err != nil {
			return err
		}
	}

	for i, target := range targets {
		_, err = s.append(ctx, v, target, false)
		if err != nil {
			if i == 0 {
				// if the first append fails, the vector was not added anywhere.
				// we must delete the version from the version map
				s.VersionMap.Delete(id)
			}
			return errors.Wrapf(err, "failed to append vector %d to posting %d", id, target.ID)
		}
	}

	return nil
}

// ensureInitialPosting creates a new posting for vector v if the index is empty
func (s *SPFresh) ensureInitialPosting(v Vector) ([]SearchResult, error) {
	s.initialPostingLock.Lock()
	defer s.initialPostingLock.Unlock()

	// check if a posting was created concurrently
	targets, _, err := s.RNGSelect(v, 0)
	if err != nil {
		return nil, err
	}

	// if no postings were found, create a new posting while holding the lock
	if len(targets) == 0 {
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
		targets = append(targets, SearchResult{
			ID: postingID,
			// distance is zero since we are using the vector as the centroid
		})
	}

	return targets, nil
}

// Append adds a vector to the specified posting.
// It returns true if the vector was successfully added, false if the posting no longer exists.
// It is called synchronously during imports but also asynchronously by reassign operations.
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
	err := s.Store.Append(ctx, centroid.ID, vector)
	if err != nil {
		s.postingLocks.Unlock(centroid.ID)
		return false, err
	}

	// increment the size of the posting
	s.PostingSizes.Inc(centroid.ID, 1)

	s.postingLocks.Unlock(centroid.ID)

	// ensure the posting size is within the configured limits
	count := s.PostingSizes.Get(centroid.ID)

	// If the posting is too big, we need to split it.
	// During an insert, we want to split asynchronously
	// however during a reassign, we want to split immediately.
	// Also, reassign operations may cause the posting to grow beyond the max size
	// temporarily. To avoid triggering unnecessary splits, we add a fine-tuned threshold.
	max := s.config.MaxPostingSize
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

// MaxPostingVectors returns how many vectors can fit in one posting
// given the dimensions, compression and I/O budget.
// I/O budget: SPANN recommends 12KB per posting for byte vectors
// and 48KB for float32 vectors.
// Dims is the number of dimensions of the vector, after compression
// if applicable.
func computeMaxPostingSize(dims int, compressed bool) uint32 {
	bytesPerDim := 4
	maxBytes := 48 * 1024 // default to float32 budget
	if compressed {
		bytesPerDim = 1
		maxBytes = 12 * 1024 // compressed budget
	}

	vBytes := dims*bytesPerDim + 4 + 1 + compressionhelpers.RQMetadataSize // id + version + RQ metadata

	return uint32(maxBytes / vBytes)
}

func (s *SPFresh) setMaxPostingSize() {
	if s.config.MaxPostingSize == 0 {
		isCompressed := s.Compressed()
		s.config.MaxPostingSize = computeMaxPostingSize(int(s.dims), isCompressed)
	}
	if s.config.MaxPostingSize < s.config.MinPostingSize { // either set by the user or computed by the index we want to make sure it's at least the min posting size
		s.config.MaxPostingSize = s.config.MinPostingSize
	}
}
