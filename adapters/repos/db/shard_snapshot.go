package db

import (
	"context"

	"github.com/pkg/errors"
)

func (s *Shard) createStoreLevelSnapshot(ctx context.Context) ([]string, error) {
	if err := s.store.PauseCompaction(ctx); err != nil {
		return nil, errors.Wrap(err, "create snapshot")
	}

	if err := s.store.FlushMemtables(ctx); err != nil {
		return nil, errors.Wrap(err, "create snapshot")
	}

	files, err := s.store.ListFiles(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "create snapshot")
	}

	return files, nil
}

func (s *Shard) createVectorIndexLevelSnapshot(ctx context.Context) ([]string, error) {
	if err := s.vectorIndex.PauseMaintenance(ctx); err != nil {
		return nil, errors.Wrap(err, "create snapshot")
	}

	if err := s.vectorIndex.SwitchCommitLogs(ctx); err != nil {
		return nil, errors.Wrap(err, "create snapshot")
	}

	files, err := s.vectorIndex.ListFiles(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "create snapshot")
	}

	return files, nil
}
