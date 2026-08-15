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

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/file"
	"github.com/weaviate/weaviate/usecases/reindex"
)

// CreateReplicaSnapshot requires stagingRoot to exist and the filesystem to
// support hardlinks; the Index probes both before calling. Returned paths are
// shard-relative so the wire protocol doesn't carry the redundant <class>/<shard>/
// prefix and resolution on the source can be naturally shard-scoped.
func (s *Shard) CreateReplicaSnapshot(ctx context.Context, stagingRoot string) (files []string, err error) {
	// Before the halt, not after: HaltForTransfer pauses the compaction cycle
	// that drives the edit-ops drain, so refusing from inside it would pause
	// the very work this is waiting for — and pay a memtable flush for a
	// read-only check each attempt.
	if err := s.refuseIfEditOpsPending(); err != nil {
		return nil, err
	}
	if err := s.HaltForTransfer(ctx, false, 0); err != nil {
		// One snapshot of one shard is one operation, so unlike the backup walk
		// this rung also feeds, the refusal is counted where it is met.
		s.index.countReindexGateRefusal(reindex.GateTransfer, err)
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

// refuseIfEditOpsPending refuses a replica snapshot while an in-place edit
// (a drop-vector strip) is mid-flight on this shard's objects bucket. The
// edit-ops sidecar is deliberately excluded from the copied file list — it is
// a live, mutating bolt file — so moving a shard with pending rows would land
// the unstripped bytes on the target with nothing recording that they still
// need stripping, while the shard's NAME already counts as covered. Nothing
// would ever re-arm it.
//
// The error MUST carry [enterrors.ErrShardBusyStructuralOp]. That sentinel is
// the plumbed "not now, try later" contract: the file-replication service maps
// it to codes.FailedPrecondition, and the replication consumer recognizes that
// and re-dispatches WITHOUT registering an error. Any other error registers
// against the op's MaxErrors budget on every attempt, and the FSM cancels the
// movement outright once that runs out — a deferral would become a
// cancellation.
//
// Scoped to replica movement on purpose: backups exclude the sidecar too, but
// a restore replays the drop from the schema marker, so a backup taken
// mid-strip is sound and must not be refused.
func (s *Shard) refuseIfEditOpsPending() error {
	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	if bucket == nil {
		return nil
	}
	hasRows, err := bucket.EditOpsHaveRows()
	if err != nil {
		// Unknown answer defers too — deferral is the reversible direction.
		return fmt.Errorf("%w: shard %q: inspect edit-ops before replica snapshot: %w",
			enterrors.ErrShardBusyStructuralOp, s.name, err)
	}
	if hasRows {
		return fmt.Errorf("%w: shard %q has an in-flight drop-vector strip (edit-op rows pending); "+
			"the snapshot is deferred until the cleanup drains it",
			enterrors.ErrShardBusyStructuralOp, s.name)
	}
	return nil
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
	// Backstop for the pre-halt refusal (see refuseIfEditOpsPending): it closes
	// the window where an op arms between that check and this one, and covers
	// the fallback path, which is already halted by the Index before it gets
	// here.
	//
	// The wait can be long. Draining shares one goroutine with compaction,
	// which takes precedence, so on a write-active shard the rows can sit
	// until the segment group's force-cleanup interval gives cleanup a turn.
	// The movement is deferred, not failed, for as long as that takes — a move
	// that keeps deferring means a drop is still stripping this shard, not a
	// stuck transfer.
	if err := s.refuseIfEditOpsPending(); err != nil {
		return nil, err
	}

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
