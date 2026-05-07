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
	name := safeSnapshotName(replicaStagingPrefix, opID, indexID(className))
	return filepath.Join(rootPath, name)
}

type replicaSnapshotState struct {
	shardName string
	// isSnapshot=false means halt-for-duration mode; Release must resume the shard.
	isSnapshot bool
}

func (i *Index) IncomingCreateReplicaSnapshot(ctx context.Context, shardName, opID string) ([]string, error) {
	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return nil, fmt.Errorf("incoming create replica snapshot get shard %s: %w", shardName, err)
	}
	defer release()
	if shard == nil {
		return nil, fmt.Errorf("incoming create replica snapshot: shard %q not found", shardName)
	}

	// On retry the prior snapshot may be stale relative to current shard contents.
	if rerr := i.releaseReplicaSnapshot(ctx, opID); rerr != nil {
		return nil, fmt.Errorf("clean prior replica snapshot for op %q: %w", opID, rerr)
	}

	stagingRoot := replicaStagingDir(i.Config.RootPath, opID, schema.ClassName(i.Config.ClassName))
	if err := os.MkdirAll(stagingRoot, 0o755); err != nil {
		return nil, fmt.Errorf("create replica staging dir: %w", err)
	}

	if i.probeHardlinkSupport() {
		files, err := shard.CreateReplicaSnapshot(ctx, stagingRoot)
		if err != nil {
			i.cleanupFailedReplicaSnapshot(stagingRoot, opID, false, nil)
			return nil, err
		}
		i.recordReplicaSnapshot(opID, replicaSnapshotState{shardName: shardName, isSnapshot: true})
		return files, nil
	}

	// Halt-for-duration fallback: shard stays halted until Release; segments
	// are served from the live shard root in this mode. The inactivity timeout
	// backstops a target crash so the halt can't leak forever waiting on a peer that's gone.
	if err := shard.HaltForTransfer(ctx, false, i.Config.TransferInactivityTimeout); err != nil {
		i.cleanupFailedReplicaSnapshot(stagingRoot, opID, false, nil)
		return nil, fmt.Errorf("halt shard %q for transfer: %w", shardName, err)
	}

	files, err := shard.ListReplicaSnapshotFiles(ctx, stagingRoot)
	if err != nil {
		i.cleanupFailedReplicaSnapshot(stagingRoot, opID, true, shard)
		return nil, fmt.Errorf("shard %q could not list replica snapshot files: %w", shardName, err)
	}

	i.recordReplicaSnapshot(opID, replicaSnapshotState{shardName: shardName, isSnapshot: false})
	return files, nil
}

func (i *Index) IncomingReleaseReplicaSnapshot(ctx context.Context, opID string) error {
	return i.releaseReplicaSnapshot(ctx, opID)
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
	return os.Open(abs)
}

// rel is shard-relative. Resolution prefers the staging dir (snapshot mode, or
// bookkeeping files in halt-for-duration mode); falls back to the live shard
// root for segments under halt-for-duration mode. Both bases are inherently
// shard-scoped, so the only escape to defend against is `..` traversal.
func (i *Index) resolveReplicaSnapshotPath(opID, rel string) (string, error) {
	i.replicaSnapshotsMu.Lock()
	st, ok := i.replicaSnapshots[opID]
	i.replicaSnapshotsMu.Unlock()
	if !ok {
		return "", fmt.Errorf("no replica snapshot registered for op %q", opID)
	}

	stagingRoot := replicaStagingDir(i.Config.RootPath, opID, schema.ClassName(i.Config.ClassName))
	stagingCandidate, err := containedPath(stagingRoot, rel)
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(stagingCandidate); err == nil {
		return stagingCandidate, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("stat staging %q: %w", rel, err)
	}
	shardRoot := shardPath(i.path(), st.shardName)
	return containedPath(shardRoot, rel)
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
// silent failures here would leak a halted shard or staging dir.
func (i *Index) cleanupFailedReplicaSnapshot(stagingRoot, opID string, resumeShard bool, shard ShardLike) {
	if resumeShard && shard != nil {
		if rerr := shard.resumeMaintenanceCycles(context.Background()); rerr != nil {
			i.logger.WithField("op_id", opID).WithField("staging_dir", stagingRoot).
				Error(fmt.Errorf("resume maintenance after failed replica snapshot: %w", rerr))
		}
	}
	if rerr := os.RemoveAll(stagingRoot); rerr != nil {
		i.logger.WithField("op_id", opID).WithField("staging_dir", stagingRoot).
			Error(fmt.Errorf("remove staging dir after failed replica snapshot: %w", rerr))
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

func (i *Index) releaseReplicaSnapshot(ctx context.Context, opID string) error {
	i.replicaSnapshotsMu.Lock()
	st, ok := i.replicaSnapshots[opID]
	delete(i.replicaSnapshots, opID)
	i.replicaSnapshotsMu.Unlock()

	stagingRoot := replicaStagingDir(i.Config.RootPath, opID, schema.ClassName(i.Config.ClassName))
	if rerr := os.RemoveAll(stagingRoot); rerr != nil {
		return fmt.Errorf("remove replica staging dir: %w", rerr)
	}
	// Return early if the snapshot isn't local anymore or if it was a halt-for-duration snapshot, which doesn't require cleanup beyond staging dir removal.
	if !ok || st.isSnapshot {
		return nil
	}

	shard, release, err := i.GetShard(ctx, st.shardName)
	if err != nil {
		return fmt.Errorf("get shard for replica snapshot release: %w", err)
	}
	defer release()
	if shard == nil {
		return nil
	}
	if err := shard.resumeMaintenanceCycles(ctx); err != nil {
		return fmt.Errorf("resume maintenance after replica transfer: %w", err)
	}
	return nil
}
