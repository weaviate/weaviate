package db

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/storobj"
)

// PreloadShard goes through the LSM store from the last checkpoint
// and enqueues any unindexed vector.
func (s *Shard) PreloadShard(targetVector string) error {
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
		s.index.logger.Warn("preload queue: queue not found")
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
		WithField("shard_id", q.shardID).
		WithField("target_vector", q.targetVector).
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
