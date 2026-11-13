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
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/weaviate/weaviate/adapters/repos/db/indexcounter"
	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/file"
	"github.com/weaviate/weaviate/usecases/integrity"
)

const replicaStagingPrefix = ".replica-staging-"

func replicaStagingDir(rootPath, opID string, className schema.ClassName) string {
	name := file.SafeStagingDirName(replicaStagingPrefix, opID, indexID(className))
	return filepath.Join(rootPath, name)
}

// IncomingProbeShardData reports whether this node holds shard data, with hosted true when a usable copy answered. ErrShardRecovering means "not usable now".
// It reads no namespace state, because it only counts the objects of a shard this node already holds and loads none.
func (i *Index) IncomingProbeShardData(ctx context.Context, shardName string) (hasData, hosted bool, err error) {
	s := i.shards.Load(shardName)
	if rec, ok := s.(*RecoveringShard); ok && rec.IsRecovering() {
		return false, false, fmt.Errorf("incoming probe shard data for shard %s: %w", shardName, enterrors.ErrShardRecovering)
	}
	if lazy, ok := asLazyLoadShard(s); ok && !lazy.isLoadedAfterTeardown() {
		return i.probeShardDataOnDisk(shardName)
	}
	if s == nil {
		// A deactivated tenant is missing from the map but can still hold data on disk.
		if hasData, onDisk, err := i.probeShardDataOnDisk(shardName); err != nil || onDisk {
			return hasData, onDisk, err
		}
	}
	shard, release, err := i.getLoadedShard(shardName)
	if err != nil {
		return false, false, fmt.Errorf("incoming probe shard data get shard %s: %w", shardName, err)
	}
	defer release()
	if shard == nil {
		// A shard unloaded since the map read above can still hold data on disk,
		// which a "not found" answer would report as none.
		if hasData, onDisk, err := i.probeShardDataOnDisk(shardName); err != nil || onDisk {
			return hasData, onDisk, err
		}
		return false, false, fmt.Errorf("incoming probe shard data get shard is nil: %s", shardName)
	}
	count, err := shard.ObjectCount(ctx)
	if err != nil {
		return false, false, fmt.Errorf("incoming probe shard data object count %s: %w", shardName, err)
	}
	return count > 0, true, nil
}

func (i *Index) asShardBusyIfReindexInFlight(err error) error {
	busyWithReindex := errors.Is(err, entitiesbackup.ErrBackupBlockedByInFlightReindex) &&
		!errors.Is(err, ErrReindexGateUnavailable)
	if !busyWithReindex {
		return err
	}
	return fmt.Errorf("%w: %w; transfer deferred until it completes",
		enterrors.ErrShardBusyStructuralOp, err)
}

// probeShardDataOnDisk reads the shard's object counter from its folder rather
// than loading the shard, which would create the folder. A missing folder answers
// onDisk false, so a shard that lost its copy is not confirmed empty.
func (i *Index) probeShardDataOnDisk(shardName string) (hasData, onDisk bool, err error) {
	dir := shardPath(i.path(), shardName)
	if _, err := os.Stat(dir); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, false, nil
		}
		return false, false, fmt.Errorf("incoming probe shard data stat %s: %w", shardName, err)
	}
	count, err := indexcounter.Read(dir)
	if err != nil {
		return false, false, fmt.Errorf("incoming probe shard data read counter %s: %w", shardName, err)
	}
	return count > 0, true, nil
}

func (i *Index) IncomingCreateReplicaSnapshot(ctx context.Context, shardName, opID string) ([]string, error) {
	// Target retries can land twice server-side for the same opID; without
	// this lock they race on the staging dir.
	i.replicaSnapshotOpLocks.Lock(opID)
	defer i.replicaSnapshotOpLocks.Unlock(opID)

	// A recovering shard cannot be a copy source. Shard-data probes must read this as "not usable now", not "definitively empty".
	if s := i.shards.Load(shardName); s != nil {
		if rec, ok := s.(*RecoveringShard); ok && rec.IsRecovering() {
			return nil, fmt.Errorf("incoming create replica snapshot for shard %s: %w", shardName, enterrors.ErrShardRecovering)
		}
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
		return nil, i.asShardBusyIfReindexInFlight(err)
	}
	i.logger.WithField("op_id", opID).WithField("shard", shardName).
		Debugf("created replica snapshot: %d files", len(files))
	i.recordReplicaSnapshot(opID)
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
	return os.Open(abs)
}

// rel is shard-relative and resolves inside the op's staging dir. The staging
// dir is shard-scoped, so the only escape to defend against is `..` traversal.
func (i *Index) resolveReplicaSnapshotPath(opID, rel string) (string, error) {
	i.replicaSnapshotsMu.Lock()
	_, ok := i.replicaSnapshots[opID]
	i.replicaSnapshotsMu.Unlock()
	if !ok {
		return "", fmt.Errorf("no replica snapshot registered for op %q", opID)
	}

	stagingRoot := replicaStagingDir(i.Config.RootPath, opID, schema.ClassName(i.Config.ClassName))
	stagingCandidate, err := containedPath(stagingRoot, rel)
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(stagingCandidate); err != nil {
		return "", fmt.Errorf("stat staging %q: %w", rel, err)
	}
	return stagingCandidate, nil
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
// silent failures here would leak a staging dir.
func (i *Index) cleanupFailedReplicaSnapshot(stagingRoot, opID string) {
	if rerr := os.RemoveAll(stagingRoot); rerr != nil {
		i.logger.WithField("op_id", opID).WithField("staging_dir", stagingRoot).
			Error(fmt.Errorf("remove staging dir after failed replica snapshot: %w", rerr))
	}
}

func (i *Index) recordReplicaSnapshot(opID string) {
	i.replicaSnapshotsMu.Lock()
	defer i.replicaSnapshotsMu.Unlock()
	if i.replicaSnapshots == nil {
		i.replicaSnapshots = map[string]struct{}{}
	}
	i.replicaSnapshots[opID] = struct{}{}
}

// releaseReplicaSnapshot forgets opID and removes its staging directory. It
// resumes no shard, because Shard.CreateReplicaSnapshot resumes it before
// returning.
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
