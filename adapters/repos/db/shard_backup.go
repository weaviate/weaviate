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
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/file"
)

// HaltForTransfer stops compaction, and flushing memtable and commit log to begin with backup or cloud offload.
// This method could be called multiple times with different inactivity timeouts,
// a zeroed `inactivityTimeout` implies no timeout.
// If inactivity timeout is reached it will resume maintenance cycle independently on how many halt request has been made.
func (s *Shard) HaltForTransfer(ctx context.Context, offloading bool, inactivityTimeout time.Duration) (err error) {
	s.haltForTransferMux.Lock()
	defer s.haltForTransferMux.Unlock()

	s.haltForTransferCount++

	defer func() {
		if err == nil && inactivityTimeout > 0 {
			s.mayUpdateInactivityTimeout(inactivityTimeout)
			s.mayInitInactivityMonitoring()
		}
	}()

	if offloading {
		// TODO: tenant offloading is calling HaltForTransfer but
		// if Shutdown is called this step is not needed
		s.mayStopAsyncReplication()
	}

	if s.haltForTransferCount > 1 {
		// shard was already halted
		return nil
	}

	defer func() {
		if err != nil {
			err = fmt.Errorf("pause compaction: %w", err)
			if err2 := s.mayForceResumeMaintenanceCycles(ctx, false); err2 != nil {
				err = fmt.Errorf("%w: resume maintenance: %w", err, err2)
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

	// pause indexing
	_ = s.ForEachVectorQueue(func(_ string, q *VectorIndexQueue) error {
		q.Pause()
		return nil
	})

	err = s.ForEachVectorIndex(func(targetVector string, index VectorIndex) error {
		if err = index.SwitchCommitLogs(ctx); err != nil {
			return fmt.Errorf("switch commit logs of vector %q: %w", targetVector, err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *Shard) mayUpdateInactivityTimeout(inactivityTimeout time.Duration) {
	if s.haltForTransferInactivityTimeout != 0 && s.haltForTransferInactivityTimeout <= inactivityTimeout {
		// no need to update current inactivity timeout
		return
	}

	s.haltForTransferInactivityTimeout = inactivityTimeout

	s.mayResetInactivityTimer()
}

func (s *Shard) mayResetInactivityTimer() {
	if s.haltForTransferInactivityTimer == nil {
		return
	}

	if !s.haltForTransferInactivityTimer.Stop() {
		<-s.haltForTransferInactivityTimer.C // drain the channel if necessary
	}
	s.haltForTransferInactivityTimer.Reset(s.haltForTransferInactivityTimeout)
}

func (s *Shard) mayInitInactivityMonitoring() {
	if s.haltForTransferCancel != nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.haltForTransferCancel = cancel

	s.haltForTransferInactivityTimer = time.NewTimer(s.haltForTransferInactivityTimeout)

	enterrors.GoWrapper(func() {
		// this goroutine will release maintenance cycles if no file activity
		// is detected in the specified inactivity timeout
		defer func() {
			s.haltForTransferMux.Lock()
			s.haltForTransferInactivityTimer.Stop()
			s.haltForTransferCancel = nil
			s.haltForTransferMux.Unlock()
		}()

		select {
		case <-ctx.Done():
			return
		case <-s.haltForTransferInactivityTimer.C:
			s.haltForTransferMux.Lock()
			s.mayForceResumeMaintenanceCycles(context.Background(), true)
			s.haltForTransferMux.Unlock()
			return
		}
	}, s.index.logger)
}

// ListBackupFiles lists all files used to backup a shard
func (s *Shard) ListBackupFiles(ctx context.Context, ret *backup.ShardDescriptor) error {
	s.haltForTransferMux.Lock()
	defer s.haltForTransferMux.Unlock()

	if s.haltForTransferCount == 0 {
		return fmt.Errorf("can not list files: illegal state: shard %q is not paused for transfer", s.name)
	}

	s.mayResetInactivityTimer()

	var err error
	if err := s.readBackupMetadata(ret); err != nil {
		return err
	}

	if ret.Files, err = s.store.ListFiles(ctx, s.index.Config.RootPath); err != nil {
		return err
	}

	return s.ForEachVectorIndex(func(targetVector string, idx VectorIndex) error {
		files, err := idx.ListFiles(ctx, s.index.Config.RootPath)
		if err != nil {
			return fmt.Errorf("list files of vector %q: %w", targetVector, err)
		}
		ret.Files = append(ret.Files, files...)
		return nil
	})
}

func (s *Shard) resumeMaintenanceCycles(ctx context.Context) error {
	s.haltForTransferMux.Lock()
	defer s.haltForTransferMux.Unlock()

	return s.mayForceResumeMaintenanceCycles(ctx, false)
}

func (s *Shard) mayForceResumeMaintenanceCycles(ctx context.Context, forced bool) error {
	if s.haltForTransferCount == 0 {
		// noop, maintenance cycles not halted
		return nil
	}

	if forced {
		s.haltForTransferCount = 0
	} else {
		s.haltForTransferCount--

		if s.haltForTransferCount > 0 {
			// maintenance cycles are not resumed as there is at least one active halt request
			return nil
		}
	}

	if s.haltForTransferCancel != nil {
		// terminate background goroutine checking for inactivity timeout
		s.haltForTransferCancel()
	}

	g := enterrors.NewErrorGroupWrapper(s.index.logger)

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
		return s.ForEachVectorQueue(func(_ string, q *VectorIndexQueue) error {
			q.Resume()
			return nil
		})
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

func (s *Shard) GetFileMetadata(ctx context.Context, relativeFilePath string) (file.FileMetadata, error) {
	s.haltForTransferMux.Lock()
	defer s.haltForTransferMux.Unlock()

	if s.haltForTransferCount == 0 {
		return file.FileMetadata{}, fmt.Errorf("can not open file %q for reading: illegal state: shard %q is not paused for transfer",
			relativeFilePath, s.name)
	}

	s.mayResetInactivityTimer()

	finalPath := filepath.Join(s.Index().Config.RootPath, relativeFilePath)
	return file.GetFileMetadata(finalPath)
}

func (s *Shard) GetFile(ctx context.Context, relativeFilePath string) (io.ReadCloser, error) {
	s.haltForTransferMux.Lock()
	defer s.haltForTransferMux.Unlock()

	if s.haltForTransferCount == 0 {
		return nil, fmt.Errorf("can not open file %q for reading: illegal state: shard %q is not paused for transfer",
			relativeFilePath, s.name)
	}

	s.mayResetInactivityTimer()

	finalPath := filepath.Join(s.Index().Config.RootPath, relativeFilePath)

	reader, err := os.Open(finalPath)
	if err != nil {
		return nil, fmt.Errorf("open file %q for reading: %w", relativeFilePath, err)
	}

	return reader, nil
}
