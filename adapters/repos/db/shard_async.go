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
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/storobj"
)

// ConvertQueue converts a legacy in-memory queue to an on-disk queue.
// It detects if the queue has a checkpoint then it enqueues all the
// remaining vectors to the on-disk queue, then deletes the checkpoint.
func (s *Shard) ConvertQueue(targetVector string) error {
	if !asyncEnabled() {
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

	err = s.FillQueue(targetVector, checkpoint)
	if err != nil {
		return errors.WithStack(err)
	}

	// we can now safely remove the checkpoint
	err = s.indexCheckpoints.Delete(s.ID(), targetVector)
	if err != nil {
		return errors.Wrap(err, "delete checkpoint")
	}

	return nil
}

// FillQueue is a helper function that enqueues all vectors from the
// LSM store to the on-disk queue.
func (s *Shard) FillQueue(targetVector string, from uint64) error {
	if !asyncEnabled() {
		return nil
	}

	start := time.Now()

	var counter int

	vectorIndex, ok := s.GetVectorIndex(targetVector)
	if !ok {
		s.index.logger.WithField("targetVector", targetVector).Warn("preload queue: vector index not found")
		// shard was never initialized, possibly because of a failed shard
		// initialization. No op.
		return nil
	}

	q, ok := s.GetVectorIndexQueue(targetVector)
	if !ok {
		s.index.logger.WithField("targetVector", targetVector).Warn("preload queue: queue not found")
		// queue was never initialized, possibly because of a failed shard
		// initialization. No op.
		return nil
	}

	ctx := context.Background()

	maxDocID := s.Counter().Get()

	var batch []common.VectorRecord

	if vectorIndex.Multivector() {
		err := s.iterateOnLSMMultiVectors(ctx, from, targetVector, func(id uint64, vector [][]float32) error {
			if vectorIndex.ContainsDoc(id) {
				return nil
			}
			if len(vector) == 0 {
				return nil
			}

			rec := &common.Vector[[][]float32]{
				ID:     id,
				Vector: vector,
			}
			counter++

			batch = append(batch, rec)

			if len(batch) < 1000 {
				return nil
			}

			err := q.Insert(ctx, batch...)
			if err != nil {
				return err
			}

			batch = batch[:0]
			return nil
		})
		if err != nil {
			return errors.Wrap(err, "iterate on LSM multi vectors")
		}
	} else {
		err := s.iterateOnLSMVectors(ctx, from, targetVector, func(id uint64, vector []float32) error {
			if vectorIndex.ContainsDoc(id) {
				return nil
			}
			if len(vector) == 0 {
				return nil
			}

			rec := &common.Vector[[]float32]{
				ID:     id,
				Vector: vector,
			}
			counter++

			batch = append(batch, rec)

			if len(batch) < 1000 {
				return nil
			}

			err := q.Insert(ctx, batch...)
			if err != nil {
				return err
			}

			batch = batch[:0]
			return nil
		})
		if err != nil {
			return errors.Wrap(err, "iterate on LSM vectors")
		}
	}

	if len(batch) > 0 {
		err := q.Insert(ctx, batch...)
		if err != nil {
			return errors.Wrap(err, "insert batch")
		}
	}

	s.index.logger.
		WithField("last_stored_id", maxDocID).
		WithField("count", counter).
		WithField("took", time.Since(start)).
		WithField("shard_id", s.ID()).
		WithField("target_vector", targetVector).
		Info("enqueued vectors from LSM store")

	return nil
}

func (s *Shard) iterateOnLSMVectors(ctx context.Context, fromID uint64, targetVector string, fn func(id uint64, vector []float32) error) error {
	properties := additional.Properties{
		NoProps: true,
		Vector:  true,
	}
	if targetVector != "" {
		properties.Vectors = []string{targetVector}
	}

	return s.iterateOnLSMObjects(ctx, fromID, func(obj *storobj.Object) error {
		var vector []float32
		if targetVector == "" {
			vector = obj.Vector
		} else {
			if len(obj.Vectors) > 0 {
				vector = obj.Vectors[targetVector]
			}
		}
		return fn(obj.DocID, vector)
	}, properties, nil)
}

func (s *Shard) iterateOnLSMMultiVectors(ctx context.Context, fromID uint64, targetVector string, fn func(id uint64, vector [][]float32) error) error {
	properties := additional.Properties{
		NoProps: true,
		Vectors: []string{targetVector},
	}

	return s.iterateOnLSMObjects(ctx, fromID, func(obj *storobj.Object) error {
		var vector [][]float32
		if len(obj.MultiVectors) > 0 {
			vector = obj.MultiVectors[targetVector]
		}
		return fn(obj.DocID, vector)
	}, properties, nil)
}

func (s *Shard) iterateOnLSMObjects(
	ctx context.Context,
	fromID uint64,
	fn func(obj *storobj.Object) error,
	addProps additional.Properties,
	properties *storobj.PropertyExtraction,
) error {
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
		obj, err := storobj.FromBinaryOptional(v, addProps, properties)
		if err != nil {
			return errors.Wrap(err, "unmarshal last indexed object")
		}

		err = fn(obj)
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
	className := s.index.Config.ClassName.String()
	shardName := s.Name()

	vectorIndex, ok := s.GetVectorIndex(targetVector)
	if !ok {
		s.index.logger.
			WithField("class", className).
			WithField("shard", shardName).
			WithField("targetVector", targetVector).
			Warn("repair index: vector index not found")
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

	q, ok := s.GetVectorIndexQueue(targetVector)
	if !ok {
		s.index.logger.
			WithField("class", className).
			WithField("shard", shardName).
			WithField("targetVector", targetVector).
			Warn("repair index: queue not found")
		// queue was never initialized, possibly because of a failed shard
		// initialization. No op.
		return nil
	}

	maxDocID := s.Counter().Get()
	visited := visited.NewList(int(maxDocID))

	var added, deleted int
	var batch []common.VectorRecord

	if vectorIndex.Multivector() {
		// add non-indexed multi vectors to the queue
		err := s.iterateOnLSMMultiVectors(ctx, 0, targetVector, func(docID uint64, vector [][]float32) error {
			visited.Visit(docID)

			if vectorIndex.ContainsDoc(docID) {
				s.index.logger.
					WithField("class", className).
					WithField("shard", shardName).
					WithField("targetVector", targetVector).
					WithField("docID", docID).
					Info("repair index: skipping doc, already in index")
				return nil
			}
			if len(vector) == 0 {
				s.index.logger.
					WithField("class", className).
					WithField("shard", shardName).
					WithField("targetVector", targetVector).
					WithField("docID", docID).
					WithField("vectorLen", 0).
					Info("repair index: skipping doc, empty vector")
				return nil
			}

			rec := &common.Vector[[][]float32]{
				ID:     docID,
				Vector: vector,
			}
			added++

			batch = append(batch, rec)

			dim := 0
			if len(vector[0]) > 0 {
				dim = len(vector[0])
			}
			s.index.logger.
				WithField("class", className).
				WithField("shard", shardName).
				WithField("targetVector", targetVector).
				WithField("docID", docID).
				WithField("vectorLen", len(vector)).
				WithField("vectorDim", dim).
				WithField("currentBatchSize", len(batch)).
				Info("repair index: added doc to batch")

			if len(batch) < 1000 {
				return nil
			}

			s.index.logger.
				WithField("class", className).
				WithField("shard", shardName).
				WithField("targetVector", targetVector).
				WithField("batchSize", len(batch)).
				Info("repair index: inserting batch")

			err := q.Insert(ctx, batch...)
			if err != nil {
				s.index.logger.
					WithError(err).
					WithField("class", className).
					WithField("shard", shardName).
					WithField("targetVector", targetVector).
					Warn("repair index: batch insert failed")
				return err
			}

			batch = batch[:0]
			return nil
		})
		if err != nil {
			return errors.Wrap(err, "iterate on LSM multi vectors")
		}
	} else {
		// add non-indexed vectors to the queue
		err := s.iterateOnLSMVectors(ctx, 0, targetVector, func(docID uint64, vector []float32) error {
			visited.Visit(docID)

			if vectorIndex.ContainsDoc(docID) {
				s.index.logger.
					WithField("class", className).
					WithField("shard", shardName).
					WithField("targetVector", targetVector).
					WithField("docID", docID).
					Info("repair index: iterateOnLSMVectors: skipping doc, already in index")
				return nil
			}
			if len(vector) == 0 {
				s.index.logger.
					WithField("class", className).
					WithField("shard", shardName).
					WithField("targetVector", targetVector).
					WithField("docID", docID).
					WithField("vectorLen", 0).
					Info("repair index: iterateOnLSMVectors: skipping doc, empty vector")
				return nil
			}

			rec := &common.Vector[[]float32]{
				ID:     docID,
				Vector: vector,
			}
			added++

			batch = append(batch, rec)

			s.index.logger.
				WithField("class", className).
				WithField("shard", shardName).
				WithField("targetVector", targetVector).
				WithField("docID", docID).
				WithField("vectorLen", len(vector)).
				WithField("vectorDim", len(vector)).
				WithField("currentBatchSize", len(batch)).
				Info("repair index: iterateOnLSMVectors: added doc to batch")

			if len(batch) < 1000 {
				return nil
			}

			s.index.logger.
				WithField("class", className).
				WithField("shard", shardName).
				WithField("targetVector", targetVector).
				WithField("batchSize", len(batch)).
				Info("repair index: iterateOnLSMVectors: inserting batch")

			err := q.Insert(ctx, batch...)
			if err != nil {
				return err
			}

			batch = batch[:0]
			return nil
		})
		if err != nil {
			return errors.Wrap(err, "iterate on LSM vectors")
		}
	}

	if len(batch) > 0 {
		s.index.logger.
			WithField("class", className).
			WithField("shard", shardName).
			WithField("targetVector", targetVector).
			WithField("batchSize", len(batch)).
			Info("repair index: flushing final batch")

		err := q.Insert(ctx, batch...)
		if err != nil {
			return errors.Wrap(err, "insert batch")
		}
	}

	s.index.logger.
		WithField("class", className).
		WithField("shard", shardName).
		WithField("targetVector", targetVector).
		WithField("visited", visited.Len()).
		WithField("added", added).
		Info("repair index: completed LSM iteration")

	if visited.Len() > 0 && added == 0 {
		s.index.logger.
			WithField("class", className).
			WithField("shard", shardName).
			WithField("targetVector", targetVector).
			WithField("visited", visited.Len()).
			Warn("repair index: visited documents but none added — all already indexed?")
	}
	// if no nodes were visited, it either means the LSM store is empty or
	// there was an uncaught error during the iteration.
	// in any case, we should not touch the index.
	if visited.Len() == 0 {
		s.index.logger.
			WithField("class", className).
			WithField("shard", shardName).
			WithField("targetVector", targetVector).
			Warn("repair index: empty LSM store")
		return nil
	}

	// remove any indexed vector that is not in the LSM store
	vectorIndex.Iterate(func(docID uint64) bool {
		if visited.Visited(docID) {
			return true
		}

		deleted++
		if vectorIndex.Multivector() {
			if err := vectorIndex.DeleteMulti(docID); err != nil {
				s.index.logger.
					WithError(err).
					WithField("class", className).
					WithField("shard", shardName).
					WithField("targetVector", targetVector).
					WithField("id", docID).
					Warn("delete multi-vector from queue")
			}
		} else {
			if err := vectorIndex.Delete(docID); err != nil {
				s.index.logger.
					WithError(err).
					WithField("class", className).
					WithField("shard", shardName).
					WithField("targetVector", targetVector).
					WithField("id", docID).
					Warn("delete vector from queue")
			}
		}

		return true
	})

	s.index.logger.
		WithField("class", className).
		WithField("shard", shardName).
		WithField("targetVector", targetVector).
		WithField("added", added).
		WithField("deleted", deleted).
		WithField("took", time.Since(start)).
		Info("repaired vector index")

	return nil
}
