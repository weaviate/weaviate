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

// Bucket snapshots provide a frozen, read-only view of an LSM bucket's
// on-disk data at the moment the internal flush completes.
//
// How it works:
//
//  1. CreateSnapshot pauses both compaction and the flush cycle, flushes the
//     active memtable to disk, and hard-links every immutable segment file
//     (.db, .bloom, .cna) into a separate snapshot directory. Both cycles are
//     resumed on return. Pausing the flush cycle ensures no concurrent
//     FlushAndSwitch can produce new segment files during the hard-link
//     window, and Deactivate waits for any in-progress flush to complete
//     before returning.
//
//     Note: the snapshot captures all data that was on disk after the flush.
//     Concurrent writers may insert new keys into the active memtable between
//     the flush and the hard-link step; those writes will NOT be included in
//     the snapshot. The effective cut-off point is the moment FlushMemtable
//     returns, not the moment CreateSnapshot is called or returns.
//
//  2. NewSnapshotBucket opens a read-only Bucket on the snapshot directory.
//     Because segment files are immutable (compaction produces new files rather
//     than modifying existing ones), the hard-links remain valid even after the
//     original bucket compacts or flushes new segments.
//
//  3. The caller creates cursors on the snapshot bucket to scan data, then
//     calls Shutdown followed by os.RemoveAll to clean up.
//
// Lifecycle:
//
//   - Each index owns a snapshots root directory (<indexPath>/.snapshots/).
//     On startup the entire directory is removed, cleaning up any snapshots
//     left behind by a crash. This is safe because snapshots are ephemeral
//     and only live for the duration of an in-flight operation.
//
// Safety:
//
//   - Snapshot directories are named with the SnapshotDirPrefix (".snapshot-").
//     CreateSnapshot prepends this automatically, NewSnapshotBucket requires
//     it, and NewBucket rejects directories with the prefix to prevent
//     accidental regular opens.
//
//   - Snapshot buckets are opened with WithImmutable(true). All write operations
//     (Put, Delete, SetAdd, MapSet, FlushAndSwitch, etc.) return ErrImmutable.
package lsmkv

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

const (
	// SnapshotDirPrefix is the prefix used for individual snapshot directories
	// created by CreateSnapshot. NewSnapshotBucket requires this prefix and
	// NewBucket rejects directories that contain it, preventing accidental misuse.
	SnapshotDirPrefix = ".snapshot-"

	// SnapshotsRootDir is the name of the directory under the index path where
	// all bucket snapshots are stored. The entire directory is removed on
	// index startup to clean up orphaned snapshots.
	SnapshotsRootDir = ".snapshots"
)

// IsSnapshotDir returns true if the directory base name starts with
// SnapshotDirPrefix.
func IsSnapshotDir(dir string) bool {
	return strings.HasPrefix(filepath.Base(dir), SnapshotDirPrefix)
}

// CreateSnapshot pauses compaction and the flush cycle, flushes the memtable
// to disk, hard-links all immutable segment files into a snapshot directory,
// and resumes both cycles on return. Pausing the flush cycle (via Deactivate)
// waits for any in-progress flush to complete — ensuring no new segment files
// appear during the hard-link window.
//
// The snapshot captures all data that was flushed to disk. Concurrent writers
// are NOT blocked: they may continue inserting into the active memtable while
// the hard-link step runs. Those writes land after the flush and are therefore
// excluded from the snapshot. The effective cut-off is the moment the internal
// FlushMemtable call returns.
//
// The snapshot is placed at <snapshotsRoot>/<SnapshotDirPrefix><name>. The
// caller provides snapshotsRoot (typically <indexPath>/.snapshots) and a name
// that identifies this snapshot. The full path is returned so the caller can
// pass it to NewSnapshotBucket.
//
// The hard-linked segments remain valid even after the original bucket compacts,
// because compaction creates new files rather than modifying existing ones.
//
// On error, the snapshot directory is cleaned up and compaction is resumed.
func (b *Bucket) CreateSnapshot(ctx context.Context, snapshotsRoot, name string) (string, error) {
	if name == "" || name == "." || name == ".." || filepath.Base(name) != name {
		return "", fmt.Errorf("invalid snapshot name %q: must be a non-empty single path element", name)
	}

	snapshotDir := filepath.Join(snapshotsRoot, SnapshotDirPrefix+name)

	if err := b.pauseCompaction(ctx); err != nil {
		return "", fmt.Errorf("pause compaction: %w", err)
	}
	defer b.resumeCompaction(ctx)

	// Pause the flush cycle so no concurrent FlushAndSwitch can produce new
	// segment files while we enumerate and hard-link. Deactivate waits for
	// any in-progress flush to complete before returning.
	if err := b.flushCallbackCtrl.Deactivate(ctx); err != nil {
		return "", fmt.Errorf("pause flush cycle: %w", err)
	}
	defer b.flushCallbackCtrl.Activate()

	if err := b.FlushMemtable(); err != nil {
		return "", fmt.Errorf("flush memtable: %w", err)
	}

	if err := hardlinkBucketFiles(b.disk.dir, snapshotDir, false); err != nil {
		os.RemoveAll(snapshotDir)
		return "", fmt.Errorf("hardlink snapshot: %w", err)
	}

	return snapshotDir, nil
}

// hardlinkBucketFiles hard-links bucket files from srcDir into dstDir.
// The dstDir is created if it doesn't exist.
//
// The caller must ensure no concurrent compaction or flush is modifying
// srcDir — for loaded buckets this means pausing both cycles first; for
// unloaded buckets this is inherently safe because no cycles are running.
//
// When includeWAL is false, .wal files are skipped. This is appropriate when
// snapshotting a loaded bucket that has just been flushed (the WAL belongs to
// the new empty memtable). When includeWAL is true, .wal files are included.
// This is necessary when snapshotting an unloaded bucket from disk, where the
// WAL may contain data that was not flushed to a segment (e.g. small tenants
// whose memtable was persisted as a WAL on shutdown).
//
// Temporary (.tmp) files are always skipped.
//
// On error, dstDir may contain a partial set of hard-links. The caller is
// responsible for removing dstDir on failure.
func hardlinkBucketFiles(srcDir, dstDir string, includeWAL bool) error {
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return fmt.Errorf("create snapshot dir: %w", err)
	}

	entries, err := os.ReadDir(srcDir)
	if err != nil {
		return fmt.Errorf("read bucket dir: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		ext := filepath.Ext(entry.Name())
		if ext == ".tmp" {
			continue
		}
		if ext == ".wal" && !includeWAL {
			continue
		}

		src := filepath.Join(srcDir, entry.Name())
		dst := filepath.Join(dstDir, entry.Name())
		if err := os.Link(src, dst); err != nil {
			return fmt.Errorf("hardlink %s: %w", entry.Name(), err)
		}
	}

	return nil
}

// SnapshotBucketFromDisk hard-links bucket files from an unloaded bucket's
// on-disk directory into a new snapshot directory. This is the counterpart to
// CreateSnapshot for shards that are not loaded into memory (e.g. cold
// tenants). The WAL is included because it may contain data that was not
// flushed to a segment on shutdown.
//
// The caller must ensure no concurrent process is modifying srcDir (e.g. by
// holding a shard lock that prevents loading). On error the snapshot directory
// is cleaned up.
func SnapshotBucketFromDisk(srcDir, snapshotsRoot, snapshotName string) (string, error) {
	snapshotDir := filepath.Join(snapshotsRoot, SnapshotDirPrefix+snapshotName)
	if err := hardlinkBucketFiles(srcDir, snapshotDir, true); err != nil {
		os.RemoveAll(snapshotDir)
		return "", err
	}
	return snapshotDir, nil
}

// NewSnapshotBucket opens a read-only bucket backed by hard-linked segment
// files (created by CreateSnapshot). It has no compaction and no flush cycle.
// An empty memtable and WAL are created in snapshotDir but never written to.
//
// The directory base name must start with SnapshotDirPrefix. The bucket is
// opened in immutable mode — all write operations will return ErrImmutable.
//
// The caller must call Shutdown on the returned bucket when done, followed by
// os.RemoveAll on the snapshot directory to clean up hard-links.
func NewSnapshotBucket(
	ctx context.Context, snapshotDir string,
	logger logrus.FieldLogger, opts ...BucketOption,
) (*Bucket, error) {
	if !IsSnapshotDir(snapshotDir) {
		return nil, fmt.Errorf("NewSnapshotBucket: directory must start with %q prefix, got %q",
			SnapshotDirPrefix, filepath.Base(snapshotDir))
	}

	if _, err := os.Stat(snapshotDir); err != nil {
		return nil, fmt.Errorf("NewSnapshotBucket: snapshot directory does not exist: %w", err)
	}

	noopCB := cyclemanager.NewCallbackGroupNoop()
	// Snapshot-enforced options go AFTER caller opts so they cannot be
	// overridden — snapshot buckets must always be read-only and non-compacting.
	// Copy into a new slice to avoid mutating the caller's underlying array.
	allOpts := make([]BucketOption, len(opts), len(opts)+2)
	copy(allOpts, opts)
	allOpts = append(allOpts, WithDisableCompaction(true), WithImmutable(true))
	return NewBucketCreator().NewBucket(ctx, snapshotDir, snapshotDir,
		logger, nil, noopCB, noopCB, allOpts...)
}
