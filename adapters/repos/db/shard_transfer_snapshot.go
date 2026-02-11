//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/weaviate/weaviate/cluster/shard"
	"github.com/weaviate/weaviate/usecases/integrity"
)

// CreateTransferSnapshot creates a hardlink snapshot of all shard files for
// out-of-band state transfer. It flushes memtables, briefly pauses compaction,
// lists all segment and vector index files, and creates hardlinks into a staging
// directory. Compaction resumes as soon as hardlinks are created.
//
// The caller must call ReleaseTransferSnapshot when the transfer is complete.
func (s *Shard) CreateTransferSnapshot(ctx context.Context) (shard.TransferSnapshot, error) {
	snapshotID := uuid.New().String()
	stagingDir := filepath.Join(s.path(), ".transfer-snapshot-"+snapshotID)
	rootPath := s.index.Config.RootPath
	allFiles := []string{}

	if err := func() error {
		// 1. Flush memtables to ensure all in-memory data is in segments.
		if err := s.store.FlushMemtables(ctx); err != nil {
			return fmt.Errorf("flush memtables: %w", err)
		}

		// 2. Pause compaction for the duration of file listing + hardlink creation.
		//    Writes continue uninterrupted; only compaction is paused.
		if err := s.store.PauseCompaction(ctx); err != nil {
			return fmt.Errorf("pause compaction: %w", err)
		}
		defer s.store.ResumeCompaction(ctx)

		// 3. List all shard files (LSM segments).
		lsmFiles, err := s.store.ListFiles(ctx, rootPath)
		if err != nil {
			return fmt.Errorf("list LSM files: %w", err)
		}

		// 4. List vector index files.
		var vectorFiles []string
		err = s.ForEachVectorIndex(func(targetVector string, idx VectorIndex) error {
			files, err := idx.ListFiles(ctx, rootPath)
			if err != nil {
				return fmt.Errorf("list files of vector index %q: %w", targetVector, err)
			}
			vectorFiles = append(vectorFiles, files...)
			return nil
		})
		if err != nil {
			return fmt.Errorf("list vector index files: %w", err)
		}

		// 5. List vector queue files (force-switches the current partial chunk).
		err = s.ForEachVectorQueue(func(targetVector string, queue *VectorIndexQueue) error {
			files, err := queue.ForceSwitch(ctx, rootPath)
			if err != nil {
				return fmt.Errorf("list files of queue %q: %w", targetVector, err)
			}
			vectorFiles = append(vectorFiles, files...)
			return nil
		})
		if err != nil {
			return fmt.Errorf("list vector queue files: %w", err)
		}

		allFiles = append(lsmFiles, vectorFiles...)

		// 6. Create staging directory and hardlinks for all segment files.
		if err := os.MkdirAll(stagingDir, 0o755); err != nil {
			return fmt.Errorf("create staging dir: %w", err)
		}

		for _, relPath := range allFiles {
			srcPath := filepath.Join(rootPath, relPath)
			dstPath := filepath.Join(stagingDir, relPath)
			if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
				_ = os.RemoveAll(stagingDir)
				return fmt.Errorf("create parent dir for %s: %w", relPath, err)
			}
			if err := os.Link(srcPath, dstPath); err != nil {
				_ = os.RemoveAll(stagingDir)
				return fmt.Errorf("hardlink %s: %w", relPath, err)
			}
		}

		return nil
	}(); err != nil {
		return shard.TransferSnapshot{}, err
	}

	// Compaction resumes here (deferred inside func above).

	// 7. Copy metadata files into staging dir. These are small files that may
	//    be actively written to, so we copy bytes rather than hardlink.
	metadataFiles := []struct {
		srcPath string
		relPath string
	}{
		{s.counter.FileName(), ""},
		{s.GetPropertyLengthTracker().FileName(), ""},
		{s.versioner.path, ""},
	}
	for i := range metadataFiles {
		rel, err := filepath.Rel(rootPath, metadataFiles[i].srcPath)
		if err != nil {
			_ = os.RemoveAll(stagingDir)
			return shard.TransferSnapshot{}, fmt.Errorf("metadata relative path: %w", err)
		}
		metadataFiles[i].relPath = rel
	}

	for _, mf := range metadataFiles {
		data, err := os.ReadFile(mf.srcPath)
		if err != nil {
			_ = os.RemoveAll(stagingDir)
			return shard.TransferSnapshot{}, fmt.Errorf("read metadata file %s: %w", mf.relPath, err)
		}
		dstPath := filepath.Join(stagingDir, mf.relPath)
		if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
			_ = os.RemoveAll(stagingDir)
			return shard.TransferSnapshot{}, fmt.Errorf("create dir for metadata %s: %w", mf.relPath, err)
		}
		if err := os.WriteFile(dstPath, data, 0o644); err != nil {
			_ = os.RemoveAll(stagingDir)
			return shard.TransferSnapshot{}, fmt.Errorf("write metadata file %s: %w", mf.relPath, err)
		}
		allFiles = append(allFiles, mf.relPath)
	}

	// 8. Build file info list with sizes and CRC32 checksums.
	fileInfos := make([]shard.TransferFileInfo, 0, len(allFiles))
	for _, relPath := range allFiles {
		fullPath := filepath.Join(stagingDir, relPath)
		size, checksum, err := integrity.CRC32(fullPath)
		if err != nil {
			_ = os.RemoveAll(stagingDir)
			return shard.TransferSnapshot{}, fmt.Errorf("compute CRC32 for %s: %w", relPath, err)
		}
		fileInfos = append(fileInfos, shard.TransferFileInfo{
			Name:  relPath,
			Size:  size,
			CRC32: checksum,
		})
	}

	return shard.TransferSnapshot{
		ID:    snapshotID,
		Dir:   stagingDir,
		Files: fileInfos,
	}, nil
}

// ReleaseTransferSnapshot deletes the staging directory for a completed or
// failed transfer snapshot.
func (s *Shard) ReleaseTransferSnapshot(snapshotID string) error {
	stagingDir := filepath.Join(s.path(), ".transfer-snapshot-"+snapshotID)
	return os.RemoveAll(stagingDir)
}
