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
	// isSnapshot=false means halt-for-duration mode; Release must resume the shard.
	isSnapshot bool
}

func (i *Index) IncomingCreateReplicaSnapshot(ctx context.Context, shardName, opID string) ([]string, error) {
	// Target retries can land twice server-side for the same opID; without
	// this lock they race on the staging dir.
	i.replicaSnapshotOpLocks.Lock(opID)
	defer i.replicaSnapshotOpLocks.Unlock(opID)

	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return nil, fmt.Errorf("incoming create replica snapshot get shard %s: %w", shardName, err)
	}
	defer release()
	if shard == nil {
		return nil, fmt.Errorf("incoming create replica snapshot: shard %q not found", shardName)
	}

	// On retry the prior snapshot may be stale relative to current shard contents.
	i.clearPriorReplicaSnapshot(ctx, opID)

	owner := replicaHaltOwner(opID)

	stagingRoot := replicaStagingDir(i.Config.RootPath, opID, schema.ClassName(i.Config.ClassName))
	if err := os.MkdirAll(stagingRoot, 0o755); err != nil {
		return nil, fmt.Errorf("create replica staging dir: %w", err)
	}

	if file.ProbeHardlinkSupport(i.Config.RootPath) {
		files, err := shard.CreateReplicaSnapshot(ctx, owner, stagingRoot)
		if err != nil {
			i.cleanupFailedReplicaSnapshot(stagingRoot, opID, false, nil)
			return nil, err
		}
		i.logger.WithField("op_id", opID).WithField("shard", shardName).
			Debugf("created replica snapshot: %d files", len(files))
		i.recordReplicaSnapshot(opID, replicaSnapshotState{shardName: shardName, isSnapshot: true})
		return files, nil
	}

	// Halt-for-duration fallback: shard stays halted until Release; segments
	// are served from the live shard root in this mode. The inactivity timeout
	// backstops a target crash so the halt can't leak forever waiting on a peer that's gone.
	if err := shard.HaltForTransfer(ctx, owner, false, i.Config.TransferInactivityTimeout); err != nil {
		i.cleanupFailedReplicaSnapshot(stagingRoot, opID, false, nil)
		return nil, fmt.Errorf("halt shard %q for transfer: %w", shardName, err)
	}

	files, err := shard.ListReplicaSnapshotFiles(ctx, stagingRoot)
	if err != nil {
		i.cleanupFailedReplicaSnapshot(stagingRoot, opID, true, shard)
		return nil, fmt.Errorf("shard %q could not list replica snapshot files: %w", shardName, err)
	}

	i.logger.WithField("op_id", opID).WithField("shard", shardName).
		Debugf("created replica snapshot: %d files", len(files))
	i.recordReplicaSnapshot(opID, replicaSnapshotState{shardName: shardName, isSnapshot: false})
	return files, nil
}

func (i *Index) IncomingReleaseReplicaSnapshot(ctx context.Context, opID string) error {
	// Without the lock, Release can RemoveAll the staging dir mid-hardlink
	// of a concurrent Create for the same opID.
	i.replicaSnapshotOpLocks.Lock(opID)
	defer i.replicaSnapshotOpLocks.Unlock(opID)

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
	f, err := os.Open(abs)
	if err != nil {
		return nil, err
	}

	i.replicaSnapshotsMu.Lock()
	st, ok := i.replicaSnapshots[opID]
	i.replicaSnapshotsMu.Unlock()
	if ok && !st.isSnapshot {
		return &transferActivityReader{
			ReadCloser: f,
			reset:      func() { i.mayResetReplicaSnapshotInactivity(opID) },
		}, nil
	}
	return f, nil
}

type transferActivityReader struct {
	io.ReadCloser
	reset func()
}

func (r *transferActivityReader) Read(p []byte) (int, error) {
	r.reset()
	return r.ReadCloser.Read(p)
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

	// Halt-for-duration serves segments from the live root; without this
	// reset, a slow transfer trips the watchdog and compaction can delete
	// segments mid-stream.
	i.mayResetReplicaSnapshotInactivity(opID)

	shardRoot := shardPath(i.path(), st.shardName)
	return containedPath(shardRoot, rel)
}

func (i *Index) mayResetReplicaSnapshotInactivity(opID string) {
	i.replicaSnapshotsMu.Lock()
	st, ok := i.replicaSnapshots[opID]
	i.replicaSnapshotsMu.Unlock()
	if !ok || st.isSnapshot {
		return
	}
	shard, release, err := i.GetShard(context.Background(), st.shardName)
	if err == nil && shard != nil {
		shard.MayResetTransferInactivityTimer()
		release()
	}
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
		if rerr := shard.resumeMaintenanceCycles(context.Background(), replicaHaltOwner(opID)); rerr != nil {
			i.logger.WithField("op_id", opID).WithField("staging_dir", stagingRoot).
				Error(fmt.Errorf("resume maintenance after failed replica snapshot: %w", rerr))
		}
	}
	if rerr := os.RemoveAll(stagingRoot); rerr != nil {
		i.logger.WithField("op_id", opID).WithField("staging_dir", stagingRoot).
			Error(fmt.Errorf("remove staging dir after failed replica snapshot: %w", rerr))
	}
}

// purgeReplicaSnapshots drops the whole registry at index teardown: the entries
// describe shards this index owns, so none of them can outlive it.
func (i *Index) purgeReplicaSnapshots() {
	i.replicaSnapshotsMu.Lock()
	defer i.replicaSnapshotsMu.Unlock()
	i.replicaSnapshots = nil
}

func (i *Index) recordReplicaSnapshot(opID string, st replicaSnapshotState) {
	i.replicaSnapshotsMu.Lock()
	defer i.replicaSnapshotsMu.Unlock()
	if i.replicaSnapshots == nil {
		i.replicaSnapshots = map[string]replicaSnapshotState{}
	}
	i.replicaSnapshots[opID] = st
}

// releaseReplicaSnapshot removes the staging dir and, in halt-for-duration mode,
// resumes the shard. The registry entry is deleted LAST, so it doubles as the retry
// handle: a release whose RESUME fails leaves the entry in place and the next attempt
// (the cancellation backstop included) does the work again instead of taking the
// unknown-op early return and reporting success on a still-halted shard. That is
// safe here precisely because the registry is read only by the replica-snapshot
// paths themselves, never by a halt gate.
//
// A staging-removal failure alone does NOT retain the entry, and does not need to:
// the removal is derived from opID only, needs no registry state, and runs
// unconditionally at the top of every later release for that opID.
func (i *Index) releaseReplicaSnapshot(ctx context.Context, opID string) error {
	i.replicaSnapshotsMu.Lock()
	st, ok := i.replicaSnapshots[opID]
	i.replicaSnapshotsMu.Unlock()

	stagingRoot := replicaStagingDir(i.Config.RootPath, opID, schema.ClassName(i.Config.ClassName))
	var removeErr error
	if rerr := os.RemoveAll(stagingRoot); rerr != nil {
		removeErr = fmt.Errorf("remove replica staging dir: %w", rerr)
	}
	// Return early if the snapshot isn't local anymore or if it was a hardlink snapshot (already resumed at create time).
	if !ok || st.isSnapshot {
		i.deleteReplicaSnapshot(opID)
		return removeErr
	}

	shard, release, err := i.GetShard(ctx, st.shardName)
	if err != nil {
		if removeErr != nil {
			return fmt.Errorf("%w; get shard for replica snapshot release: %w", removeErr, err)
		}
		return fmt.Errorf("get shard for replica snapshot release: %w", err)
	}
	defer release()
	if shard != nil {
		if err := shard.resumeMaintenanceCycles(ctx, replicaHaltOwner(opID)); err != nil {
			if removeErr != nil {
				return fmt.Errorf("%w; resume maintenance after replica transfer: %w", removeErr, err)
			}
			return fmt.Errorf("resume maintenance after replica transfer: %w", err)
		}
	}
	i.deleteReplicaSnapshot(opID)
	return removeErr
}

// clearPriorReplicaSnapshot drops any prior snapshot for opID. A failure is logged,
// not returned: this create is itself the retry that replaces the prior snapshot, it
// has already re-acquired the shard, and recordReplicaSnapshot overwrites the entry
// afterwards. The only way to reach here with a failed release is a resume-machinery
// error, and resumeOwnerLocked removes the owner before the fallible
// completeResumeLocked runs — so the halt refcount is already correct and the
// re-halt cannot double-count. The staging dir has already been removed
// unconditionally before any fallible step, so proceeding cannot leave stale files.
func (i *Index) clearPriorReplicaSnapshot(ctx context.Context, opID string) {
	if err := i.releaseReplicaSnapshot(ctx, opID); err != nil {
		i.logger.WithField("op_id", opID).
			Warnf("clean prior replica snapshot before re-create: %v", err)
	}
}

func (i *Index) deleteReplicaSnapshot(opID string) {
	i.replicaSnapshotsMu.Lock()
	defer i.replicaSnapshotsMu.Unlock()
	delete(i.replicaSnapshots, opID)
}
