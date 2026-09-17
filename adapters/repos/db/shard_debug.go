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
)

// IMPORTANT:
// DebugResetVectorIndex is intended to be used for debugging purposes only.
// It creates a new vector index and replaces the existing one if any.
// This function assumes the node is not receiving any traffic besides the
// debug endpoints and that async indexing is enabled.
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

	err = s.markVectorIndexReady(targetVector, rec)
	if err != nil {
		return errors.Wrap(err, "mark vector index ready")
	}
	return nil
}
