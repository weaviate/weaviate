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
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
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

	vidx, vok := s.GetVectorIndex(targetVector)
	q, qok := s.GetVectorIndexQueue(targetVector)

	if !(vok && qok) {
		return fmt.Errorf("vector index %q not found", targetVector)
	}

	q.Pause()
	q.Wait()

	err := vidx.Drop(ctx)
	if err != nil {
		return errors.Wrap(err, "drop vector index")
	}

	var newConfig schemaConfig.VectorIndexConfig
	if targetVector == "" {
		newConfig = s.index.vectorIndexUserConfig
	} else {
		newConfig = s.index.vectorIndexUserConfigs[targetVector]
	}

	vidx, err = s.initVectorIndex(ctx, targetVector, newConfig, false)
	if err != nil {
		return errors.Wrap(err, "init vector index")
	}
	s.setVectorIndex(targetVector, vidx)

	q.ResetWith(vidx)
	q.Resume()
	return nil
}
