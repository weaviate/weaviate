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
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// HaltForTransfer stops compaction, and flushing memtable and commit log to begin with backup or cloud offload
func (s *Shard) HaltForTransfer(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("pause compaction: %w", err)
			if err2 := s.resumeMaintenanceCycles(ctx); err2 != nil {
				err = fmt.Errorf("%w: resume maintenance: %v", err, err2)
			}
		}
	}()

	// NOTE: async replication may be paused during backup
	// only if the hashtree can be resumed but object updates
	// or deletions requires special handling.
	// Curently hashtree is not included into the backup files,
	// the hashtree will be automatically regenerated when restoring
	// s.mayStopHashBeater()
	// s.mayCloseHashTree()

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

	// pause indexing
	s.queue.Pause()
	for _, q := range s.Queues() {
		q.Pause()
	}

	if s.hasTargetVectors() {
		for targetVector, vectorIndex := range s.vectorIndexes {
			if err = vectorIndex.SwitchCommitLogs(ctx); err != nil {
				return fmt.Errorf("switch commit logs of vector %q: %w", targetVector, err)
			}
		}
	} else {
		if err = s.vectorIndex.SwitchCommitLogs(ctx); err != nil {
			return fmt.Errorf("switch commit logs: %w", err)
		}
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

	if s.hasTargetVectors() {
		for targetVector, vectorIndex := range s.vectorIndexes {
			files, err := vectorIndex.ListFiles(ctx, s.index.Config.RootPath)
			if err != nil {
				return fmt.Errorf("list files of vector %q: %w", targetVector, err)
			}
			ret.Files = append(ret.Files, files...)
		}
	} else {
		files, err := s.vectorIndex.ListFiles(ctx, s.index.Config.RootPath)
		if err != nil {
			return err
		}
		ret.Files = append(ret.Files, files...)
	}

	return nil
}

func (s *Shard) resumeMaintenanceCycles(ctx context.Context) error {
	g := enterrors.NewErrorGroupWrapper(s.index.logger)

	// NOTE: async replication may be resumed if paused in HaltForTransfer method
	//g.Go(func() error {
	//	return s.UpdateAsyncReplication(ctx, s.index.asyncReplicationEnabled())
	//})
	g.Go(func() error {
		return s.store.ResumeCompaction(ctx)
	})
	g.Go(func() error {
		return s.cycleCallbacks.vectorCombinedCallbacksCtrl.Activate()
	})
	g.Go(func() error {
		return s.cycleCallbacks.geoPropsCombinedCallbacksCtrl.Activate()
	})

	g.Go(func() error {
		s.queue.Resume()
		for _, q := range s.Queues() {
			q.Resume()
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return fmt.Errorf("failed to resume maintenance cycles for shard '%s': %w", s.name, err)
	}

	return nil
}

func (s *Shard) readBackupMetadata(d *backup.ShardDescriptor) (err error) {
	d.Name = s.name

	d.Node, err = s.nodeName()
	if err != nil {
		return fmt.Errorf("node name: %w", err)
	}

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

func (s *Shard) nodeName() (string, error) {
	node, err := s.index.getSchema.ShardOwner(
		s.index.Config.ClassName.String(), s.name)
	return node, err
}
