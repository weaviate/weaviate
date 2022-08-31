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
	"os"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/backup"
	"golang.org/x/sync/errgroup"
)

func (s *Shard) createBackup(ctx context.Context, snap *backup.Snapshot) error {
	var g errgroup.Group

	g.Go(func() error {
		files, err := s.createStoreLevelSnapshot(ctx)
		if err != nil {
			return err
		}
		snap.Lock()
		defer snap.Unlock()
		snap.Files = append(snap.Files, files...)
		return nil
	})

	g.Go(func() error {
		files, err := s.createVectorIndexLevelSnapshot(ctx)
		if err != nil {
			return err
		}
		snap.Lock()
		defer snap.Unlock()
		snap.Files = append(snap.Files, files...)
		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}

	shardMeta, err := s.readSnapshotMetadata()
	if err != nil {
		return errors.Wrap(err, "create snapshot")
	}

	snap.Lock()
	snap.ShardMetadata[s.name] = shardMeta
	snap.Unlock()

	return nil
}

func (s *Shard) resumeMaintenanceCycles(ctx context.Context) error {
	var g errgroup.Group

	g.Go(func() error {
		return s.store.ResumeCompaction(ctx)
	})

	g.Go(func() error {
		return s.vectorIndex.ResumeMaintenance(ctx)
	})

	if err := g.Wait(); err != nil {
		return errors.Wrapf(err,
			"failed to resume maintenance cycles for shard '%s'", s.name)
	}

	return nil
}

func (s *Shard) createStoreLevelSnapshot(ctx context.Context) ([]backup.SnapshotFile, error) {
	var g errgroup.Group

	g.Go(func() error {
		if err := s.store.PauseCompaction(ctx); err != nil {
			return errors.Wrap(err, "create snapshot")
		}
		return nil
	})

	g.Go(func() error {
		if err := s.store.FlushMemtables(ctx); err != nil {
			return errors.Wrap(err, "create snapshot")
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	paths, err := s.store.ListFiles(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "create snapshot")
	}

	files := make([]backup.SnapshotFile, len(paths))
	for i, pth := range paths {
		files[i] = backup.SnapshotFile{
			Path:  pth,
			Class: s.index.Config.ClassName.String(),
			Node:  s.index.Config.NodeName,
			Shard: s.name,
		}
	}

	return files, nil
}

func (s *Shard) createVectorIndexLevelSnapshot(ctx context.Context) ([]backup.SnapshotFile, error) {
	var g errgroup.Group

	g.Go(func() error {
		if err := s.vectorIndex.PauseMaintenance(ctx); err != nil {
			return errors.Wrap(err, "create snapshot")
		}
		return nil
	})

	g.Go(func() error {
		if err := s.vectorIndex.SwitchCommitLogs(ctx); err != nil {
			return errors.Wrap(err, "create snapshot")
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	paths, err := s.vectorIndex.ListFiles(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "create snapshot")
	}

	files := make([]backup.SnapshotFile, len(paths))
	for i, pth := range paths {
		files[i] = backup.SnapshotFile{
			Path:  pth,
			Class: s.index.Config.ClassName.String(),
			Node:  s.index.Config.NodeName,
			Shard: s.name,
		}
	}

	return files, nil
}

func (s *Shard) readSnapshotMetadata() (*backup.ShardMetadata, error) {
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

	shardVersion, err := s.readShardVersion()
	if err != nil {
		return nil, errors.Wrapf(err,
			"failed to read shard version for shard '%s'", s.name)
	}

	return &backup.ShardMetadata{
		DocIDCounter:      counterContents,
		PropLengthTracker: propLenContents,
		ShardVersion:      shardVersion,
	}, nil
}

func (s *Shard) readIndexCounter() ([]byte, error) {
	return os.ReadFile(s.counter.FileName())
}

func (s *Shard) readPropLengthTracker() ([]byte, error) {
	return os.ReadFile(s.propLengths.FileName())
}

func (s *Shard) readShardVersion() ([]byte, error) {
	return os.ReadFile(s.versioner.path)
}
