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

//go:build integrationTest

package db

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	entBackup "github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	dynamicent "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
)

func snapshotIno(t *testing.T, path string) uint64 {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	return info.Sys().(*syscall.Stat_t).Ino
}

func containsRelPath(files []string, want string) bool {
	for _, f := range files {
		if f == want {
			return true
		}
	}
	return false
}

// isSealedBackupFile reports whether relPath names a file that PrepareForBackup
// has sealed (or that is otherwise append-only) and so MUST be hard-linked into
// the staging dir, never copied — copying these large files inside the halt would
// regress the compaction pause the snapshot path exists to minimise. The set
// mirrors the kinds an active shard produces: LSM segments and their sidecars,
// condensed HNSW commit logs / snapshots, and async-indexing queue chunks.
func isSealedBackupFile(relPath string) bool {
	base := filepath.Base(relPath)
	switch {
	case strings.HasPrefix(base, "segment-"):
		// segment-*.db / .bloom / .cna / .secondary.N.bloom — all sealed once flushed.
		return true
	case strings.HasSuffix(base, ".condensed"), strings.HasSuffix(base, ".snapshot"):
		return true
	case strings.HasPrefix(base, "chunk-") && strings.HasSuffix(base, ".bin"):
		return true
	default:
		return false
	}
}

// assertSealedFilesStayHardlinked pins the no-pause-regression invariant (AC#3):
// every sealed file that CreateBackupSnapshot staged must share the source inode
// (hard-linked, not copied). It also requires that at least one such file exists,
// so a shard that happened to stage only copies cannot vacuously satisfy it. This
// is the positive counterpart to the meta.db/index.db NotEqual (copy) assertions;
// together they fail if anyone hard-links the bbolt files OR copies the sealed
// files.
func assertSealedFilesStayHardlinked(t *testing.T, rootPath, stagingRoot string, files []string) {
	t.Helper()
	sealed := 0
	for _, relPath := range files {
		if !isSealedBackupFile(relPath) {
			continue
		}
		src := filepath.Join(rootPath, relPath)
		dst := filepath.Join(stagingRoot, relPath)
		srcInfo, err := os.Stat(src)
		require.NoError(t, err)
		dstInfo, err := os.Stat(dst)
		require.NoError(t, err, "sealed file %q must be staged", relPath)
		require.True(t, os.SameFile(srcInfo, dstInfo),
			"sealed file %q must stay hard-linked (share source inode), not be copied", relPath)
		sealed++
	}
	require.Greater(t, sealed, 0,
		"expected at least one sealed (hard-linked) file in the backup set to prove the no-copy invariant")
}

// TestActiveShardSnapshotFlatMetaDB verifies that the active backup path produces a
// torn-free, independent copy of the flat index's meta.db rather than a hardlink:
// a write that mutates the live meta.db after the snapshot must not alter the
// staged copy (the core regression), and the staged file must open as a valid bbolt
// database reflecting the pre-write state.
func TestActiveShardSnapshotFlatMetaDB(t *testing.T) {
	ctx := context.Background()
	className := "ActiveFlatSnapshot"

	shard, index := testShardWithSettings(
		t, ctx,
		&models.Class{Class: className, VectorIndexType: "flat"},
		flatent.NewDefaultUserConfig(),
		false, true, false,
	)
	t.Cleanup(func() { _ = index.drop() })

	objs := make([]*storobj.Object, 10)
	for i := range objs {
		o := testObject(className)
		o.Vector = randVector(3)
		objs[i] = o
	}
	for _, err := range shard.PutObjectBatch(ctx, objs) {
		require.NoError(t, err)
	}

	rootPath := shard.Index().Config.RootPath
	liveMeta := filepath.Join(shard.(*Shard).path(), "meta.db")
	require.FileExists(t, liveMeta)

	stagingRoot := t.TempDir()
	var sd entBackup.ShardDescriptor
	files, err := shard.CreateBackupSnapshot(ctx, &sd, stagingRoot)
	require.NoError(t, err)

	metaRel, err := filepath.Rel(rootPath, liveMeta)
	require.NoError(t, err)
	require.True(t, containsRelPath(files, metaRel), "meta.db must be in the backup file set")

	stagedMeta := filepath.Join(stagingRoot, metaRel)
	require.FileExists(t, stagedMeta)
	require.NotEqual(t, snapshotIno(t, liveMeta), snapshotIno(t, stagedMeta),
		"staged meta.db must be an independent copy, not a hardlink")

	// The flip side of AC#3: the sealed LSM segments produced by the flush during
	// the halt must stay hard-linked (no copy, no pause regression).
	assertSealedFilesStayHardlinked(t, rootPath, stagingRoot, files)

	stagedBefore, err := os.ReadFile(stagedMeta)
	require.NoError(t, err)

	// Mutate the live meta.db in place while the shard is "live" (the long upload
	// window). The flat index uses open-close-per-op, so the file is closed between
	// operations; writing directly mimics a concurrent setDimensions/persistRQData.
	mutateLiveBolt(t, liveMeta)

	stagedAfter, err := os.ReadFile(stagedMeta)
	require.NoError(t, err)
	require.Equal(t, stagedBefore, stagedAfter,
		"staged meta.db must be unchanged by the post-snapshot in-place write")

	// The staged copy must still open as a valid bbolt database.
	staged, err := bolt.Open(stagedMeta, 0o600, &bolt.Options{ReadOnly: true, Timeout: 5 * time.Second})
	require.NoError(t, err)
	require.NoError(t, staged.Close())
}

// TestActiveShardSnapshotDynamicIndexDB verifies the index.db gap is closed on the
// active path: index.db is added to the backup set and snapshotted as an
// independent, torn-free copy, while the underlying flat meta.db is also copied.
func TestActiveShardSnapshotDynamicIndexDB(t *testing.T) {
	t.Setenv("ASYNC_INDEX_INTERVAL", "100ms")
	ctx := context.Background()
	className := "ActiveDynamicSnapshot"

	shard, index := testShardWithSettings(
		t, ctx,
		&models.Class{Class: className, VectorIndexType: "dynamic"},
		dynamicent.NewDefaultUserConfig(),
		false, true, true, /* async indexing required by dynamic */
	)
	t.Cleanup(func() { _ = index.drop() })

	objs := make([]*storobj.Object, 10)
	for i := range objs {
		o := testObject(className)
		o.Vector = randVector(3)
		objs[i] = o
	}
	for _, err := range shard.PutObjectBatch(ctx, objs) {
		require.NoError(t, err)
	}

	rootPath := shard.Index().Config.RootPath
	liveIndexDB := filepath.Join(shard.(*Shard).path(), "index.db")
	require.FileExists(t, liveIndexDB)
	// A below-threshold dynamic index is still backed by flat, so its meta.db
	// exists and must be snapshotted alongside index.db.
	liveMeta := filepath.Join(shard.(*Shard).path(), "meta.db")
	require.FileExists(t, liveMeta)

	stagingRoot := t.TempDir()
	var sd entBackup.ShardDescriptor
	files, err := shard.CreateBackupSnapshot(ctx, &sd, stagingRoot)
	require.NoError(t, err)

	indexDBRel, err := filepath.Rel(rootPath, liveIndexDB)
	require.NoError(t, err)
	require.True(t, containsRelPath(files, indexDBRel),
		"index.db must be added to the active backup file set (gap closed)")
	// index.db is shard-owned and shared across target vectors, so it must be
	// snapshotted exactly once — not once per dynamic index via SnapshotMutableFiles.
	indexDBCount := 0
	for _, f := range files {
		if f == indexDBRel {
			indexDBCount++
		}
	}
	require.Equal(t, 1, indexDBCount, "index.db must appear exactly once (snapshotted once per shard)")

	stagedIndexDB := filepath.Join(stagingRoot, indexDBRel)
	require.FileExists(t, stagedIndexDB)
	require.NotEqual(t, snapshotIno(t, liveIndexDB), snapshotIno(t, stagedIndexDB),
		"staged index.db must be an independent copy, not a hardlink")

	// The underlying flat meta.db must also be snapshotted as an independent copy,
	// not just index.db (the delegated SnapshotMutableFiles must fire for dynamic).
	metaRel, err := filepath.Rel(rootPath, liveMeta)
	require.NoError(t, err)
	require.True(t, containsRelPath(files, metaRel),
		"underlying flat meta.db must be in the backup file set for a flat-phase dynamic index")
	stagedMeta := filepath.Join(stagingRoot, metaRel)
	require.FileExists(t, stagedMeta)
	require.NotEqual(t, snapshotIno(t, liveMeta), snapshotIno(t, stagedMeta),
		"staged meta.db must be an independent copy, not a hardlink")

	// And the sealed files (LSM segments, the async-indexing queue chunk) must stay
	// hard-linked — proving we copy only the small mutable bbolt DBs, not everything.
	assertSealedFilesStayHardlinked(t, rootPath, stagingRoot, files)

	stagedBefore, err := os.ReadFile(stagedIndexDB)
	require.NoError(t, err)

	// Mutate the live index.db in place through the shard's own open handle (the
	// shard holds the exclusive flock for its lifetime, so a second opener would
	// block). This mimics the in-place write the flat->hnsw upgrade commits while
	// the shard is live.
	require.NoError(t, shard.(*Shard).dynamicVectorIndexDB.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("snapshot_test_mutation"))
		if err != nil {
			return err
		}
		return b.Put([]byte("k"), []byte("post-snapshot-write"))
	}))

	stagedAfter, err := os.ReadFile(stagedIndexDB)
	require.NoError(t, err)
	require.Equal(t, stagedBefore, stagedAfter,
		"staged index.db must be unchanged by the post-snapshot in-place write")

	staged, err := bolt.Open(stagedIndexDB, 0o600, &bolt.Options{ReadOnly: true, Timeout: 5 * time.Second})
	require.NoError(t, err)
	require.NoError(t, staged.Close())
}

// mutateLiveBolt opens a bbolt file directly and commits an in-place write,
// simulating the lock-free Update that setDimensions/persistRQData (flat meta.db)
// and the dynamic upgrade (index.db) perform while the shard is live.
func mutateLiveBolt(t *testing.T, path string) {
	t.Helper()
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("snapshot_test_mutation"))
		if err != nil {
			return err
		}
		return b.Put([]byte("k"), []byte("post-snapshot-write"))
	}))
}
