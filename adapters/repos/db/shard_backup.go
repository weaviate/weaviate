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
	"os"
	"path"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/backup"
	"golang.org/x/sync/errgroup"
)

// beginBackup stops compaction, and flushing memtable and commit log to begin with the backup
func (s *Shard) beginBackup(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("pause compaction: %w", err)
			if err2 := s.resumeMaintenanceCycles(ctx); err2 != nil {
				err = fmt.Errorf("%w: resume maintenance: %v", err, err2)
			}
		}
	}()
	if err = s.store.PauseCompaction(ctx); err != nil {
		return errors.Wrap(err, "pause compaction")
	}
	if err = s.store.FlushMemtables(ctx); err != nil {
		return errors.Wrap(err, "flush memtables")
	}
	if err = s.vectorIndex.PauseMaintenance(ctx); err != nil {
		return errors.Wrap(err, "pause maintenance")
	}
	if err = s.vectorIndex.SwitchCommitLogs(ctx); err != nil {
		return errors.Wrap(err, "switch commit logs")
	}
	return nil
}

// listBackupFiles lists all files used to backup a shard
func (s *Shard) listBackupFiles(ctx context.Context, ret *backup.ShardDescriptor) error {
	var err error
	if err := s.readBackupMetadata(ret); err != nil {
		return err
	}
	if ret.Files, err = s.store.ListFiles(ctx); err != nil {
		return err
	}
	files2, err := s.vectorIndex.ListFiles(ctx)
	if err != nil {
		return err
	}
	ret.Files = append(ret.Files, files2...)
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

func (s *Shard) readBackupMetadata(d *backup.ShardDescriptor) (err error) {
	d.Name = s.name
	d.Node = s.nodeName()
	fpath := s.counter.FileName()
	if d.DocIDCounter, err = os.ReadFile(fpath); err != nil {
		return fmt.Errorf("read shard doc-id-counter %s: %w", fpath, err)
	}
	d.DocIDCounterPath = path.Base(fpath)

	fpath = s.propLengths.FileName()
	if d.PropLengthTracker, err = os.ReadFile(fpath); err != nil {
		return fmt.Errorf("read shard prop-lengths %s: %w", fpath, err)
	}
	d.PropLengthTrackerPath = path.Base(fpath)

	fpath = s.versioner.path
	if d.Version, err = os.ReadFile(fpath); err != nil {
		return fmt.Errorf("read shard version %s: %w", fpath, err)
	}
	d.ShardVersionPath = path.Base(fpath)
	return nil
}

func (s *Shard) nodeName() string {
	ss := s.index.getSchema.ShardingState(s.index.Config.ClassName.String())
	node := ss.Physical[s.name].BelongsToNode()
	return node
}
