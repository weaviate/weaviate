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

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hfresh"
	enterrors "github.com/weaviate/weaviate/entities/errors"
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

	vidx, vok := s.GetVectorIndex(targetVector)
	q, qok := s.GetVectorIndexQueue(targetVector)

	if !(vok && qok) {
		return fmt.Errorf("vector index %q not found", targetVector)
	}

	// read before the drop: afterwards nothing tells the type
	_, isHFresh := vidx.(*hfresh.HFresh)

	if err := q.Pause(ctx); err != nil {
		return errors.Wrap(err, "pause vector index")
	}

	err := vidx.Drop(ctx, false)
	if err != nil {
		return errors.Wrap(err, "drop vector index")
	}

	if isHFresh {
		err = s.removeHFreshArtifacts(ctx, targetVector)
		if err != nil {
			return errors.Wrap(err, "remove hfresh files")
		}
	}

	newConfig := s.index.GetVectorIndexConfig(targetVector)

	vidx, err = s.initVectorIndex(ctx, targetVector, newConfig, false)
	if err != nil {
		return errors.Wrap(err, "init vector index")
	}
	s.setVectorIndex(targetVector, vidx)

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

	return nil
}

// removeHFreshArtifacts deletes what a dropped hfresh index left on disk,
// which the new index would otherwise load. The queue stays: the reset reuses it.
func (s *Shard) removeHFreshArtifacts(ctx context.Context, targetVector string) error {
	id := s.vectorIndexID(targetVector)
	var otherIDs []string
	for _, other := range otherTargetVectors(s.class, targetVector) {
		otherIDs = append(otherIDs, vectorIndexID(other))
	}
	artifacts := helpers.VectorIndexArtifactsForID(id, otherIDs)
	for _, bucket := range artifacts.LSMBuckets {
		err := s.removeBucket(ctx, bucket)
		if err != nil {
			return fmt.Errorf("drop bucket %q: %w", bucket, err)
		}
	}
	for _, dir := range artifacts.ShardDirs {
		if dir == id+".queue.d" {
			continue
		}
		err := s.removeDirIfExists(s.path(), dir)
		if err != nil {
			return fmt.Errorf("drop directory %q: %w", dir, err)
		}
	}
	return nil
}
