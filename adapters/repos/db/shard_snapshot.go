package db

import (
	"context"
	"io/ioutil"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/snapshots"
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

func (s *Shard) readSnapshotMetadata() (*snapshots.ShardMetadata, error) {
	counterContents, err := s.readIndexCounter()
	if err != nil {
		return nil, errors.Wrapf(err,
			"failed to read index counter for shard '%s'", s.name)
	}

	propLenContents, err := s.readPropLengthTracker()
	if err != nil {
		return nil, errors.Wrapf(err,
			"failed to read prop length tracker for shard '%s'", s.name)
	}

	return &snapshots.ShardMetadata{
		DocIDCounter:      counterContents,
		PropLengthTracker: propLenContents,
	}, nil
}

func (s *Shard) readIndexCounter() ([]byte, error) {
	return ioutil.ReadFile(s.counter.FileName())
}

func (s *Shard) readPropLengthTracker() ([]byte, error) {
	return ioutil.ReadFile(s.propLengths.FileName())
}
