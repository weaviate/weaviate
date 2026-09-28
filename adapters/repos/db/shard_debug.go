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

package db

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/propertyspecific"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/geo"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hfresh"
	"github.com/weaviate/weaviate/entities/additional"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
)

// IMPORTANT:
// DebugResetVectorIndex is intended to be used for debugging purposes only.
// It creates a new vector index and replaces the existing one if any.
// This function assumes the node is not receiving any traffic besides the
// debug endpoints and that async indexing is enabled.
// It refills the new index with every stored vector in the background; a
// restart before the refill completes leaves it partial.
func (s *Shard) DebugResetVectorIndex(ctx context.Context, targetVector string) error {
	if !s.index.AsyncIndexingEnabled {
		return fmt.Errorf("async indexing is not enabled")
	}

	rec, ok, err := s.mapping.Get(targetVector)
	if err != nil {
		return errors.Wrap(err, "read mapping record")
	}
	if !ok {
		return fmt.Errorf("vector %q has no mapping record", targetVector)
	}

	vidx, releaseIndex, vok := s.AcquireVectorIndex(targetVector)
	if !vok {
		return fmt.Errorf("vector index %q not found", targetVector)
	}
	defer releaseIndex()
	q, releaseQueue, qok := s.AcquireVectorIndexQueue(targetVector)
	if !qok {
		return fmt.Errorf("vector index %q not found", targetVector)
	}
	defer releaseQueue()

	// read before the drop: afterwards nothing tells the type
	_, isHFresh := vidx.(*hfresh.HFresh)

	if err := q.Pause(ctx); err != nil {
		return errors.Wrap(err, "pause vector index")
	}

	// bracketed like a creation: a crash in between leaves creating, which
	// the next load resumes instead of refusing
	err = s.markVectorIndexCreating(targetVector, rec)
	if err != nil {
		q.Resume()
		return errors.Wrap(err, "mark vector index creating")
	}

	err = vidx.Drop(ctx, false)
	if err != nil {
		return errors.Wrap(err, "drop vector index")
	}

	if isHFresh {
		err = s.removeHFreshArtifacts(ctx, targetVector, rec.PhysicalID)
		if err != nil {
			return errors.Wrap(err, "remove hfresh files")
		}
	}

	newConfig := s.index.GetVectorIndexConfig(targetVector)

	vidx, err = s.initVectorIndex(ctx, targetVector, rec.PhysicalID, newConfig, false)
	if err != nil {
		return errors.Wrap(err, "init vector index")
	}
	s.setVectorIndex(targetVector, vidx)

	// the queue follows the new index whatever the record write says: the
	// record is repaired at the next load, the shard must keep indexing now
	q.ResetWith(vidx)
	q.Resume()

	// the new index is empty: nothing to skip
	enterrors.GoWrapper(func() {
		err := s.fillQueue(targetVector, 0, false)
		if err != nil {
			s.index.logger.WithField("shard", s.name).WithField("targetVector", targetVector).
				Errorf("failed to refill vector index: %v", err)
		}
	}, s.index.logger)

	err = s.markVectorIndexReady(targetVector, rec)
	if err != nil {
		return errors.Wrap(err, "mark vector index ready")
	}
	return nil
}

// removeHFreshArtifacts deletes what a dropped hfresh index left on disk,
// which the new index would otherwise load. The queue stays: the reset reuses it.
func (s *Shard) removeHFreshArtifacts(ctx context.Context, targetVector, physicalID string) error {
	var otherIDs []string
	for _, other := range otherTargetVectors(s.class, targetVector) {
		rec, ok, err := s.mapping.Get(other)
		if err != nil {
			return fmt.Errorf("vector %q: %w", other, err)
		}
		if ok {
			otherIDs = append(otherIDs, rec.PhysicalID)
		}
	}
	artifacts := helpers.VectorIndexArtifactsForID(physicalID, otherIDs)
	for _, bucket := range artifacts.LSMBuckets {
		err := s.removeBucket(ctx, bucket)
		if err != nil {
			return fmt.Errorf("drop bucket %q: %w", bucket, err)
		}
	}
	for _, dir := range artifacts.ShardDirs {
		if dir == physicalID+".queue.d" {
			continue
		}
		err := s.removeDirIfExists(s.path(), dir)
		if err != nil {
			return fmt.Errorf("drop directory %q: %w", dir, err)
		}
	}
	return nil
}

// DebugResetGeoIndex is DebugResetVectorIndex for propName's geo index. It is
// for debugging only, under the same assumptions.
func (s *Shard) DebugResetGeoIndex(ctx context.Context, propName string) error {
	if !s.index.AsyncIndexingEnabled {
		return fmt.Errorf("async indexing is not enabled")
	}

	// one writer at a time on the prop's commit log directory, as in initGeoProp
	s.geoInitLock.Lock()
	defer s.geoInitLock.Unlock()

	s.propertyIndicesLock.RLock()
	propIndex, ok := s.propertyIndices[propName]
	q := s.geoQueues[propName]
	s.propertyIndicesLock.RUnlock()
	if !ok || propIndex.Type != schema.DataTypeGeoCoordinates || propIndex.GeoIndex == nil || q == nil {
		return fmt.Errorf("geo index %q not found", propName)
	}
	graph, ok := propIndex.GeoIndex.UnderlyingVectorIndex().(VectorIndex)
	if !ok {
		return fmt.Errorf("geo index %q: unexpected graph type %T", propName, propIndex.GeoIndex.UnderlyingVectorIndex())
	}

	if err := q.Pause(ctx); err != nil {
		return errors.Wrap(err, "pause geo index")
	}

	// drop the graph, not the geo index: searches may still hold the geo index,
	// and geo.Index.Drop leaves it with a nil graph
	err := graph.Drop(ctx, false)
	if err != nil {
		return errors.Wrap(err, "drop geo index")
	}

	idx, err := s.newGeoIndex(propName)
	if err != nil {
		return err
	}
	graph, ok = idx.UnderlyingVectorIndex().(VectorIndex)
	if !ok {
		return fmt.Errorf("geo index %q: unexpected graph type %T", propName, idx.UnderlyingVectorIndex())
	}

	s.propertyIndicesLock.Lock()
	s.propertyIndices[propName] = propertyspecific.Index{
		Type:     schema.DataTypeGeoCoordinates,
		GeoIndex: idx,
		Name:     propName,
	}
	s.propertyIndicesLock.Unlock()
	idx.PostStartup(s.shutCtx)

	q.ResetWith(graph)
	q.Resume()

	// the new index is empty: every stored coordinate goes back in
	enterrors.GoWrapper(func() {
		err := s.fillGeoQueue(propName, q)
		if err != nil {
			s.index.logger.WithField("shard", s.name).WithField("geo", propName).
				Errorf("failed to refill geo index: %v", err)
		}
	}, s.index.logger)

	return nil
}

// fillGeoQueue enqueues the coordinates of every stored object that has
// propName.
func (s *Shard) fillGeoQueue(propName string, q *VectorIndexQueue) error {
	ctx := context.Background()
	start := time.Now()

	var counter int
	var batch []common.VectorRecord
	err := s.iterateOnLSMObjects(ctx, 0, func(obj *storobj.Object) error {
		coordinates, err := geoCoordinatesOfProp(obj, propName)
		if err != nil {
			return fmt.Errorf("doc %d: %w", obj.DocID, err)
		}
		if coordinates == nil {
			return nil
		}
		vec, err := geo.GeoCoordinatesToVector(coordinates)
		if err != nil {
			return fmt.Errorf("doc %d: %w", obj.DocID, err)
		}

		batch = append(batch, &common.Vector[[]float32]{ID: obj.DocID, Vector: vec})
		counter++
		if len(batch) < 1000 {
			return nil
		}

		err = q.Insert(ctx, batch...)
		if err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}, additional.Properties{}, storobj.NewPropExtraction().Add(propName))
	if err != nil {
		return errors.Wrap(err, "iterate on LSM objects")
	}

	if len(batch) > 0 {
		err = q.Insert(ctx, batch...)
		if err != nil {
			return errors.Wrap(err, "insert batch")
		}
	}

	s.index.logger.
		WithField("count", counter).
		WithField("took", time.Since(start)).
		WithField("shard_id", s.ID()).
		WithField("geo", propName).
		Info("enqueued geo coordinates from LSM store")

	return nil
}
