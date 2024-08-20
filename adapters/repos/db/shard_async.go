//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
	"github.com/weaviate/weaviate/entities/storobj"
)

// PreloadQueue goes through the LSM store from the last checkpoint
// and enqueues any unindexed vector.
func (s *Shard) PreloadQueue(targetVector string) error {
	if !asyncEnabled() {
		return nil
	}

	start := time.Now()

	var counter int

	vectorIndex := s.getVectorIndex(targetVector)
	if vectorIndex == nil {
		s.index.logger.Warn("preload queue: vector index not found")
		// shard was never initialized, possibly because of a failed shard
		// initialization. No op.
		return nil
	}

	q, err := s.getIndexQueue(targetVector)
	if err != nil {
		s.index.logger.WithError(err).Warn("preload queue: queue not found")
		// queue was never initialized, possibly because of a failed shard
		// initialization. No op.
		return nil
	}

	// load non-indexed vectors and add them to the queue
	checkpoint, exists, err := s.indexCheckpoints.Get(s.ID(), targetVector)
	if err != nil {
		return errors.Wrap(err, "get last indexed id")
	}
	if !exists {
		return nil
	}

	defer func() {
		q.metrics.Preload(start, counter)
	}()

	ctx := context.Background()

	maxDocID := s.Counter().Get()

	err = s.iterateOnLSMVectors(ctx, checkpoint, targetVector, func(id uint64, vector []float32) error {
		if vectorIndex.ContainsNode(id) {
			return nil
		}
		if len(vector) == 0 {
			return nil
		}

		desc := vectorDescriptor{
			id:     id,
			vector: vector,
		}

		counter++
		return q.Push(ctx, desc)
	})
	if err != nil {
		return errors.Wrap(err, "iterate on LSM")
	}

	s.index.logger.
		WithField("checkpoint", checkpoint).
		WithField("last_stored_id", maxDocID).
		WithField("count", counter).
		WithField("took", time.Since(start)).
		WithField("shard_id", s.ID()).
		WithField("target_vector", targetVector).
		Info("enqueued vectors from last indexed checkpoint")

	return nil
}

func (s *Shard) iterateOnLSMVectors(ctx context.Context, fromID uint64, targetVector string, fn func(id uint64, vector []float32) error) error {
	maxDocID := s.Counter().Get()
	bucket := s.Store().Bucket(helpers.ObjectsBucketLSM)

	buf := make([]byte, 8)
	for i := fromID; i < maxDocID; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		binary.LittleEndian.PutUint64(buf, i)

		v, err := bucket.GetBySecondary(0, buf)
		if err != nil {
			return errors.Wrap(err, "get last indexed object")
		}
		if v == nil {
			continue
		}
		obj, err := storobj.FromBinary(v)
		if err != nil {
			return errors.Wrap(err, "unmarshal last indexed object")
		}
		id := obj.DocID

		var vector []float32
		if targetVector == "" {
			vector = obj.Vector
		} else {
			if len(obj.Vectors) > 0 {
				vector = obj.Vectors[targetVector]
			}
		}

		err = fn(id, vector)
		if err != nil {
			return err
		}
	}

	return nil
}

// RepairIndex ensures the vector index is consistent with the LSM store.
// It goes through the LSM store and enqueues any unindexed vector, and
// it also removes any indexed vector that is not in the LSM store.
// It it safe to call or interrupt this method at any time.
// If ASYNC_INDEXING is disabled, it's a no-op.
func (s *Shard) RepairIndex(ctx context.Context, targetVector string) error {
	if !asyncEnabled() {
		return nil
	}

	start := time.Now()

	vectorIndex := s.getVectorIndex(targetVector)
	if vectorIndex == nil {
		s.index.logger.Warn("repair index: vector index not found")
		// shard was never initialized, possibly because of a failed shard
		// initialization. No op.
		return nil
	}

	// if it's HNSW, trigger a tombstone cleanup
	if hnsw.IsHNSWIndex(vectorIndex) {
		err := hnsw.AsHNSWIndex(vectorIndex).CleanUpTombstonedNodes(func() bool {
			return ctx.Err() != nil
		})
		if err != nil {
			return errors.Wrap(err, "clean up tombstoned nodes")
		}
	}

	q, err := s.getIndexQueue(targetVector)
	if err != nil {
		s.index.logger.WithError(err).Warn("repair index: queue not found")
		// queue was never initialized, possibly because of a failed shard
		// initialization. No op.
		return nil
	}

	maxDocID := s.Counter().Get()

	visited := visited.NewList(int(maxDocID))

	var added, deleted int

	// add non-indexed vectors to the queue
	err = s.iterateOnLSMVectors(ctx, 0, targetVector, func(id uint64, vector []float32) error {
		visited.Visit(id)

		if vectorIndex.ContainsNode(id) {
			return nil
		}
		if len(vector) == 0 {
			return nil
		}

		desc := vectorDescriptor{
			id:     id,
			vector: vector,
		}

		added++
		return q.Push(ctx, desc)
	})
	if err != nil {
		return errors.Wrap(err, "iterate on LSM")
	}

	// if no nodes were visited, it either means the LSM store is empty or
	// there was an uncaught error during the iteration.
	// in any case, we should not touch the index.
	if visited.Len() == 0 {
		s.index.logger.Warn("repair index: empty LSM store")
		return nil
	}

	// remove any indexed vector that is not in the LSM store
	vectorIndex.Iterate(func(id uint64) bool {
		if visited.Visited(id) {
			return true
		}

		deleted++
		err := vectorIndex.Delete(id)
		if err != nil {
			s.index.logger.WithError(err).WithField("id", id).Warn("delete vector from queue")
		}

		return true
	})

	s.index.logger.
		WithField("added", added).
		WithField("deleted", deleted).
		WithField("shard_id", s.ID()).
		WithField("took", time.Since(start)).
		WithField("target_vector", targetVector).
		Info("repaired vector index")

	return nil
}
