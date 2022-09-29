package db

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
