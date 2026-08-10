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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/file"
	"github.com/weaviate/weaviate/usecases/integrity"
)

const replicaStagingPrefix = ".replica-staging-"

func replicaStagingDir(rootPath, opID string, className schema.ClassName) string {
	name := file.SafeStagingDirName(replicaStagingPrefix, opID, indexID(className))
	return filepath.Join(rootPath, name)
}

type replicaSnapshotState struct {
	shardName string
}

func (i *Index) IncomingCreateReplicaSnapshot(ctx context.Context, shardName, opID string) ([]string, error) {
	// Target retries can land twice server-side for the same opID; without
	// this lock they race on the staging dir.
	i.replicaSnapshotOpLocks.Lock(opID)
	defer i.replicaSnapshotOpLocks.Unlock(opID)

	// The guard precedes GetShard so a rejected op allocates and halts
	// nothing. The error surfaces as codes.Internal on purpose:
	// FailedPrecondition is the defer-forever "shard busy" class, and a
	// missing filesystem capability is permanent, so the op must consume its
	// error budget and fail visibly instead of parking.
	if !file.ProbeHardlinkSupport(i.Config.RootPath) {
		return nil, fmt.Errorf("replica movement requires a filesystem that supports hard links; "+
			"the data directory for class %s does not", i.Config.ClassName)
	}

	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return nil, fmt.Errorf("incoming create replica snapshot get shard %s: %w", shardName, err)
	}
	defer release()
	if shard == nil {
		return nil, fmt.Errorf("incoming create replica snapshot: shard %q not found", shardName)
	}

	// On retry the prior snapshot may be stale relative to current shard contents.
	if rerr := i.releaseReplicaSnapshot(opID); rerr != nil {
		return nil, fmt.Errorf("clean prior replica snapshot for op %q: %w", opID, rerr)
	}

	stagingRoot := replicaStagingDir(i.Config.RootPath, opID, schema.ClassName(i.Config.ClassName))
	if err := os.MkdirAll(stagingRoot, 0o755); err != nil {
		return nil, fmt.Errorf("create replica staging dir: %w", err)
	}

	files, err := shard.CreateReplicaSnapshot(ctx, stagingRoot)
	if err != nil {
		i.cleanupFailedReplicaSnapshot(stagingRoot, opID)
		return nil, err
	}
	i.logger.WithField("op_id", opID).WithField("shard", shardName).
		Debugf("created replica snapshot: %d files", len(files))
	i.recordReplicaSnapshot(opID, replicaSnapshotState{shardName: shardName})
	return files, nil
}

func (i *Index) IncomingReleaseReplicaSnapshot(ctx context.Context, opID string) error {
	// Without the lock, Release can RemoveAll the staging dir mid-hardlink
	// of a concurrent Create for the same opID.
	i.replicaSnapshotOpLocks.Lock(opID)
	defer i.replicaSnapshotOpLocks.Unlock(opID)

	return i.releaseReplicaSnapshot(opID)
}

func (i *Index) IncomingGetReplicaSnapshotFileMetadata(ctx context.Context, opID, relativeFilePath string) (file.FileMetadata, error) {
	abs, err := i.resolveReplicaSnapshotPath(opID, relativeFilePath)
	if err != nil {
		return file.FileMetadata{}, err
	}
	st, err := os.Stat(abs)
	if err != nil {
		return file.FileMetadata{}, fmt.Errorf("stat %q: %w", relativeFilePath, err)
	}
	_, crc, err := integrity.CRC32(abs)
	if err != nil {
		return file.FileMetadata{}, fmt.Errorf("crc %q: %w", relativeFilePath, err)
	}
	return file.FileMetadata{Name: relativeFilePath, Size: st.Size(), CRC32: crc}, nil
}

func (i *Index) IncomingGetReplicaSnapshotFile(ctx context.Context, opID, relativeFilePath string) (io.ReadCloser, error) {
	abs, err := i.resolveReplicaSnapshotPath(opID, relativeFilePath)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(abs)
	if err != nil {
		return nil, err
	}

	return f, nil
}

// rel is shard-relative. Every snapshot file lives in the staging dir —
// segments hard-linked and bookkeeping files written at create time — so
// resolution never touches the live shard root. The base is shard-scoped;
// the only escape to defend against is `..` traversal.
func (i *Index) resolveReplicaSnapshotPath(opID, rel string) (string, error) {
	i.replicaSnapshotsMu.Lock()
	_, ok := i.replicaSnapshots[opID]
	i.replicaSnapshotsMu.Unlock()
	if !ok {
		return "", fmt.Errorf("no replica snapshot registered for op %q", opID)
	}

	stagingRoot := replicaStagingDir(i.Config.RootPath, opID, schema.ClassName(i.Config.ClassName))
	abs, err := containedPath(stagingRoot, rel)
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(abs); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("file %q not in snapshot for op %q", rel, opID)
		}
		return "", fmt.Errorf("stat staging %q: %w", rel, err)
	}
	return abs, nil
}

// containedPath joins base and rel, rejecting any rel that escapes base via `..`.
func containedPath(base, rel string) (string, error) {
	base = filepath.Clean(base)
	abs := filepath.Clean(filepath.Join(base, rel))
	if abs != base && !strings.HasPrefix(abs, base+string(filepath.Separator)) {
		return "", fmt.Errorf("path %q escapes %q", rel, base)
	}
	return abs, nil
}

// Logs rather than returns so the caller's primary error stays the signal;
// a silent failure here would leak a staging dir.
func (i *Index) cleanupFailedReplicaSnapshot(stagingRoot, opID string) {
	if rerr := os.RemoveAll(stagingRoot); rerr != nil {
		i.logger.WithField("op_id", opID).WithField("staging_dir", stagingRoot).
			Errorf("remove staging dir after failed replica snapshot: %v", rerr)
	}
}

func (i *Index) recordReplicaSnapshot(opID string, st replicaSnapshotState) {
	i.replicaSnapshotsMu.Lock()
	defer i.replicaSnapshotsMu.Unlock()
	if i.replicaSnapshots == nil {
		i.replicaSnapshots = map[string]replicaSnapshotState{}
	}
	i.replicaSnapshots[opID] = st
}

// releaseReplicaSnapshot forgets the op and removes its staging directory.
// The shard needs no resume here: CreateReplicaSnapshot resumed it before
// returning, success or failure.
func (i *Index) releaseReplicaSnapshot(opID string) error {
	i.replicaSnapshotsMu.Lock()
	delete(i.replicaSnapshots, opID)
	i.replicaSnapshotsMu.Unlock()

	stagingRoot := replicaStagingDir(i.Config.RootPath, opID, schema.ClassName(i.Config.ClassName))
	if err := os.RemoveAll(stagingRoot); err != nil {
		return fmt.Errorf("remove replica staging dir: %w", err)
	}
	return nil
}
