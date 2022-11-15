//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package db

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/noop"
	hnswent "github.com/semi-technologies/weaviate/entities/vectorindex/hnsw"
)

func (ind *Index) IncomingFilePutter(ctx context.Context, shardName,
	filePath string,
) (io.WriteCloser, error) {
	localShard, ok := ind.Shards[shardName]
	if !ok {
		return nil, fmt.Errorf("shard %q does not exist locally", shardName)
	}

	return localShard.filePutter(ctx, filePath)
}

func (s *Shard) filePutter(ctx context.Context,
	filePath string,
) (io.WriteCloser, error) {
	// TODO: validate file prefix to rule out that we're accidentally writing
	// into another shard
	finalPath := filepath.Join(s.index.Config.RootPath, filePath)
	f, err := os.Create(finalPath)
	if err != nil {
		return nil, fmt.Errorf("open file %q for writing: %w", filePath, err)
	}

	return f, nil
}

func (ind *Index) IncomingCreateShard(ctx context.Context,
	shardName string,
) error {
	// TODO: locking???
	if _, ok := ind.Shards[shardName]; ok {
		return fmt.Errorf("shard %q exists already", shardName)
	}

	// TODO: metrics
	s, err := NewShard(ctx, nil, shardName, ind)
	if err != nil {
		return err
	}

	// TODO: locking???
	ind.Shards[shardName] = s

	return nil
}

func (ind *Index) IncomingReinitShard(ctx context.Context,
	shardName string,
) error {
	shard, ok := ind.Shards[shardName]
	if !ok {
		return fmt.Errorf("shard %q does not exist locally", shardName)
	}

	return shard.reinit(ctx)
}

func (s *Shard) reinit(ctx context.Context) error {
	if err := s.shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown shard: %w", err)
	}

	hnswUserConfig, ok := s.index.vectorIndexUserConfig.(hnswent.UserConfig)
	if !ok {
		return fmt.Errorf("hnsw vector index: config is not hnsw.UserConfig: %T",
			s.index.vectorIndexUserConfig)
	}

	if hnswUserConfig.Skip {
		s.vectorIndex = noop.NewIndex()
	} else {
		if err := s.initVectorIndex(ctx, hnswUserConfig); err != nil {
			return fmt.Errorf("init vector index: %w", err)
		}

		defer s.vectorIndex.PostStartup()
	}

	if err := s.initNonVector(ctx); err != nil {
		return fmt.Errorf("init non-vector: %w", err)
	}

	// TODO: reinit vector
	return nil
}
