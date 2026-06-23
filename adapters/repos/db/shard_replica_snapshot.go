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
	"strings"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/usecases/file"
)

// CreateReplicaSnapshot requires stagingRoot to exist and the filesystem to
// support hardlinks; the Index probes both before calling. Returned paths are
// shard-relative so the wire protocol doesn't carry the redundant <class>/<shard>/
// prefix and resolution on the source can be naturally shard-scoped.
func (s *Shard) CreateReplicaSnapshot(ctx context.Context, stagingRoot string) (files []string, err error) {
	if err := s.HaltForTransfer(ctx, false, 0); err != nil {
		return nil, fmt.Errorf("halt for replica snapshot: %w", err)
	}
	defer func() {
		if rerr := s.resumeMaintenanceCycles(context.Background()); rerr != nil && err == nil {
			err = fmt.Errorf("resume maintenance after replica snapshot: %w", rerr)
		}
	}()

	files, err = s.collectShardRelativeFiles(ctx, stagingRoot, true)
	if err != nil {
		return nil, err
	}
	return files, nil
}

// ListReplicaSnapshotFiles copies mutable bookkeeping files into stagingRoot and
// returns the shard-relative file list. It does NOT hardlink segments.
//
// In halt-for-duration fallback mode the shard is halted by the caller and stays
// halted until the caller releases it.
func (s *Shard) ListReplicaSnapshotFiles(ctx context.Context, stagingRoot string) ([]string, error) {
	return s.collectShardRelativeFiles(ctx, stagingRoot, false)
}

func (s *Shard) collectShardRelativeFiles(ctx context.Context, stagingRoot string, hardlinkSegments bool) ([]string, error) {
	sd := backup.ShardDescriptor{Name: s.name}
	dbRootFiles, err := s.ListBackupFiles(ctx, &sd)
	if err != nil {
		return nil, fmt.Errorf("list backup files: %w", err)
	}

	out := make([]string, 0, len(dbRootFiles)+3)
	var hardlinks []file.HardlinkPair
	for _, dbRel := range dbRootFiles {
		shardRel, err := s.shardRelativePath(dbRel)
		if err != nil {
			return nil, err
		}
		if hardlinkSegments {
			hardlinks = append(hardlinks, file.HardlinkPair{
				Src: filepath.Join(s.index.Config.RootPath, dbRel),
				Dst: filepath.Join(stagingRoot, shardRel),
			})
		}
		out = append(out, shardRel)
	}
	// hardlinks is nil in halt-for-duration mode, where HardlinkFiles is a no-op.
	if err := file.HardlinkFiles(hardlinks); err != nil {
		return nil, fmt.Errorf("hardlink replica snapshot files to staging: %w", err)
	}

	mutables, err := s.writeReplicaSnapshotMutableFiles(stagingRoot, &sd)
	if err != nil {
		return nil, err
	}
	return append(out, mutables...), nil
}

// shardRelativePath converts a path returned by ListBackupFiles (relative to
// the DB root) into a shard-relative path, rejecting anything outside the shard.
func (s *Shard) shardRelativePath(dbRootRel string) (string, error) {
	abs := filepath.Join(s.index.Config.RootPath, dbRootRel)
	rel, err := filepath.Rel(s.path(), abs)
	if err != nil {
		return "", fmt.Errorf("compute shard-relative path for %q: %w", dbRootRel, err)
	}
	if rel == "." || strings.HasPrefix(rel, "..") {
		return "", fmt.Errorf("path %q is outside shard root", dbRootRel)
	}
	return rel, nil
}

// Bookkeeping files are mmap'd in place by the live shard, so they must be
// copied rather than hard-linked.
func (s *Shard) writeReplicaSnapshotMutableFiles(stagingRoot string, sd *backup.ShardDescriptor) ([]string, error) {
	mutables := []struct {
		dbRootRel string
		data      []byte
	}{
		{sd.DocIDCounterPath, sd.DocIDCounter},
		{sd.PropLengthTrackerPath, sd.PropLengthTracker},
		{sd.ShardVersionPath, sd.Version},
	}
	out := make([]string, 0, len(mutables))
	for _, m := range mutables {
		if m.dbRootRel == "" {
			continue
		}
		shardRel, err := s.shardRelativePath(m.dbRootRel)
		if err != nil {
			return nil, err
		}
		dst := filepath.Join(stagingRoot, shardRel)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return nil, fmt.Errorf("create staging subdir for %s: %w", shardRel, err)
		}
		if err := os.WriteFile(dst, m.data, 0o644); err != nil {
			return nil, fmt.Errorf("write mutable %s: %w", shardRel, err)
		}
		out = append(out, shardRel)
	}
	return out, nil
}
