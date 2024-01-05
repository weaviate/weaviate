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
	"os"
	"path/filepath"

	"github.com/weaviate/weaviate/entities/backup"
	"golang.org/x/sync/errgroup"
)

// BeginBackup stops compaction, and flushing memtable and commit log to begin with the backup
func (s *Shard) BeginBackup(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("pause compaction: %w", err)
			if err2 := s.resumeMaintenanceCycles(ctx); err2 != nil {
				err = fmt.Errorf("%w: resume maintenance: %v", err, err2)
			}
		}
	}()
	if err = s.store.PauseCompaction(ctx); err != nil {
		return fmt.Errorf("pause compaction: %w", err)
	}
	if err = s.store.FlushMemtables(ctx); err != nil {
		return fmt.Errorf("flush memtables: %w", err)
	}
	if err = s.cycleCallbacks.vectorCombinedCallbacksCtrl.Deactivate(ctx); err != nil {
		return fmt.Errorf("pause vector maintenance: %w", err)
	}
	if err = s.cycleCallbacks.geoPropsCombinedCallbacksCtrl.Deactivate(ctx); err != nil {
		return fmt.Errorf("pause geo props maintenance: %w", err)
	}
	if err = s.VectorIndex().SwitchCommitLogs(ctx); err != nil {
		return fmt.Errorf("switch commit logs: %w", err)
	}
	return nil
}

// ListBackupFiles lists all files used to backup a shard
func (s *Shard) ListBackupFiles(ctx context.Context, ret *backup.ShardDescriptor) error {
	var err error
	if err := s.readBackupMetadata(ret); err != nil {
		return err
	}

	if ret.Files, err = s.store.ListFiles(ctx, s.index.Config.RootPath); err != nil {
		return err
	}
	files, err := s.VectorIndex().ListFiles(ctx, s.index.Config.RootPath)
	if err != nil {
		return err
	}

	ret.Files = append(ret.Files, files...)
	return nil
}

func (s *Shard) resumeMaintenanceCycles(ctx context.Context) error {
	var g errgroup.Group

	g.Go(func() error {
		return s.store.ResumeCompaction(ctx)
	})
	g.Go(func() error {
		return s.cycleCallbacks.vectorCombinedCallbacksCtrl.Activate()
	})
	g.Go(func() error {
		return s.cycleCallbacks.geoPropsCombinedCallbacksCtrl.Activate()
	})

	if err := g.Wait(); err != nil {
		return fmt.Errorf("failed to resume maintenance cycles for shard '%s': %w", s.name, err)
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
	d.DocIDCounterPath, err = filepath.Rel(s.index.Config.RootPath, fpath)
	if err != nil {
		return fmt.Errorf("docid counter path: %w", err)
	}
	fpath = s.GetPropertyLengthTracker().FileName()
	if d.PropLengthTracker, err = os.ReadFile(fpath); err != nil {
		return fmt.Errorf("read shard prop-lengths %s: %w", fpath, err)
	}
	d.PropLengthTrackerPath, err = filepath.Rel(s.index.Config.RootPath, fpath)
	if err != nil {
		return fmt.Errorf("proplength tracker path: %w", err)
	}
	fpath = s.versioner.path
	if d.Version, err = os.ReadFile(fpath); err != nil {
		return fmt.Errorf("read shard version %s: %w", fpath, err)
	}
	d.ShardVersionPath, err = filepath.Rel(s.index.Config.RootPath, fpath)
	if err != nil {
		return fmt.Errorf("shard version path: %w", err)
	}
	return nil
}

func (s *Shard) nodeName() string {
	node, _ := s.index.getSchema.ShardOwner(
		s.index.Config.ClassName.String(), s.name)
	return node
}
