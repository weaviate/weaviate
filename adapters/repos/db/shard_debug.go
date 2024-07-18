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
	"fmt"

	"github.com/pkg/errors"
)

// IMPORTANT:
// DebugResetVectorIndex is intended to be used for debugging purposes only.
// It creates a new vector index and replaces the existing one if any.
// This function assumes the node is not receiving any traffic besides the
// debug endpoints and that async indexing is enabled.
func (s *Shard) DebugResetVectorIndex(ctx context.Context, targetVector string) error {
	if !asyncEnabled() {
		return fmt.Errorf("async indexing is not enabled")
	}

	var vidx VectorIndex
	var q *IndexQueue
	if s.hasTargetVectors() {
		vidx = s.vectorIndexes[targetVector]
		q = s.queues[targetVector]
	} else {
		vidx = s.vectorIndex
		q = s.queue
	}

	if vidx == nil {
		return fmt.Errorf("vector index %q not found", targetVector)
	}

	q.PauseIndexing()

	err := vidx.Drop(ctx)
	if err != nil {
		return err
	}

	if s.hasTargetVectors() {
		s.vectorIndexes[targetVector], err = s.initVectorIndex(ctx, targetVector, s.index.vectorIndexUserConfigs[targetVector])
		if err != nil {
			return errors.Wrap(err, "init vector index")
		}
		vidx = s.vectorIndexes[targetVector]
	} else {
		s.vectorIndex, err = s.initVectorIndex(ctx, targetVector, s.index.vectorIndexUserConfig)
		if err != nil {
			return errors.Wrap(err, "init vector index")
		}
		vidx = s.vectorIndex
	}

	err = q.ResetWith(vidx)
	if err != nil {
		return errors.Wrap(err, "reset queue")
	}

	q.ResumeIndexing()
	return nil
}
