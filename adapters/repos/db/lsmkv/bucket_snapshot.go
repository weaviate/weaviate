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

// Bucket snapshots provide a frozen, read-only view of an LSM bucket's data
// at a specific point in time.
//
// How it works:
//
//  1. CreateSnapshot pauses both compaction and the flush cycle, flushes the
//     active memtable to disk, and hard-links every immutable segment file
//     (.db, .bloom, .cna) into a separate snapshot directory. Both cycles are
//     resumed on return. Pausing the flush cycle ensures no concurrent
//     FlushAndSwitch can produce new segment files during the hard-link
//     window, and Deactivate waits for any in-progress flush to complete
//     before returning — guaranteeing a true point-in-time snapshot.
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
//   - Snapshot buckets are opened with WithReadOnly(true). All write operations
//     (Put, Delete, SetAdd, MapSet, FlushAndSwitch, etc.) return ErrReadOnly.
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

	if err := HardlinkBucketFiles(b.disk.dir, snapshotDir, false); err != nil {
		os.RemoveAll(snapshotDir)
		return "", fmt.Errorf("hardlink snapshot: %w", err)
	}

	return snapshotDir, nil
}

// HardlinkBucketFiles hard-links bucket files from srcDir into dstDir.
// The dstDir is created if it doesn't exist.
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
func HardlinkBucketFiles(srcDir, dstDir string, includeWAL bool) error {
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

// NewSnapshotBucket opens a read-only bucket backed by hard-linked segment
// files (created by CreateSnapshot). It has no compaction and no flush cycle.
// An empty memtable and WAL are created in snapshotDir but never written to.
//
// The directory base name must start with SnapshotDirPrefix. The bucket is
// opened in read-only mode — all write operations will return ErrReadOnly.
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

	noopCB := cyclemanager.NewCallbackGroupNoop()
	allOpts := append([]BucketOption{
		WithDisableCompaction(true),
		WithReadOnly(true),
	}, opts...)
	return NewBucketCreator().NewBucket(ctx, snapshotDir, snapshotDir,
		logger, nil, noopCB, noopCB, allOpts...)
}
