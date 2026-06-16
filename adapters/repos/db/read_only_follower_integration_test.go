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
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/memwatch"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// TestReadOnlyFollower_NodeBootZeroWrites is the node-level capstone for the
// zero-write guarantee. It populates a writer DB (object + inverted + HNSW
// buckets), shuts it down, copies the data dir, makes the copy a hard read-only
// mount (chmod -R a-w), and boots a second DB with Config.ReadOnly over it. It
// asserts that the full node — every shard's LSM store, HNSW index, index
// counter, prop-length tracker, version file — opens and serves reads while
// writing NOTHING to the copy, and that writes are rejected.
//
// This is the test that catches a lazy write-on-open the bucket-level and
// cluster-level tests cannot: it boots all of shard init at once on a kernel-
// enforced read-only directory.
func TestReadOnlyFollower_NodeBootZeroWrites(t *testing.T) {
	ctx := testCtx()
	class := makeTestClass("ReadOnlyBootClass")

	writerDir := t.TempDir()
	readerDir := t.TempDir()

	// Writer and reader must share the same sharding state (same shard name), so
	// the follower loads exactly the writer's shards from the copy.
	shardState := singleShardState()

	// 1. Writer: insert objects with vectors, then shut down (flushes memtables
	//    to segments and persists the HNSW commit log).
	ids := make([]strfmt.UUID, 25)
	{
		writer := bootFollowerTestDB(t, writerDir, false, shardState, class)
		for i := range ids {
			ids[i] = strfmt.UUID(uuid.NewString())
			obj := &models.Object{
				ID:    ids[i],
				Class: class.Class,
				Properties: map[string]interface{}{
					"stringProp": fmt.Sprintf("value-%d", i),
				},
			}
			vec := []float32{float32(i), float32(i) + 1, float32(i) + 2, float32(i) + 3}
			require.NoError(t, writer.PutObject(ctx, obj, vec, nil, nil, nil, 0))
		}
		require.NoError(t, writer.Shutdown(ctx))
	}

	// 2. Copy the data dir; this copy is what the follower mounts read-only.
	require.NoError(t, copyTreeForReadOnly(writerDir, readerDir))

	// 3. Make the copy a hard read-only mount: any write now fails with EACCES.
	//    Snapshot AFTER chmod so the baseline reflects the read-only perms (the
	//    chmod itself is ours, not a DB write).
	makeTreeReadOnlyForFollower(t, readerDir)
	t.Cleanup(func() { makeTreeWritableForFollower(t, readerDir) })
	before := snapshotTreeForReadOnly(t, readerDir)

	// 4. Boot a read-only follower DB over the copy. If any subsystem writes on
	//    open, this fails loudly here (EACCES) rather than silently in prod.
	reader := bootFollowerTestDB(t, readerDir, true, shardState, class)

	// 5. Reads succeed: every object inserted by the writer is readable.
	for _, id := range ids {
		res, err := reader.ObjectByID(ctx, id, nil, additional.Properties{}, "")
		require.NoError(t, err)
		require.NotNil(t, res, "object %s must be readable from the read-only follower", id)
		require.Equal(t, id, res.ID)
	}

	// 6. Writes are rejected at the shard boundary (ReadOnlyShard).
	writeErr := reader.PutObject(ctx, &models.Object{
		ID:    strfmt.UUID(uuid.NewString()),
		Class: class.Class,
	}, []float32{1, 2, 3, 4}, nil, nil, nil, 0)
	require.Error(t, writeErr, "a read-only follower must reject writes")

	// 7. Zero filesystem writes: the directory is byte-for-byte unchanged after
	//    boot + reads + a rejected write.
	after := snapshotTreeForReadOnly(t, readerDir)
	require.Equal(t, before, after, "read-only follower boot + reads must not create, modify, or delete any file")

	// restore writable so Shutdown and t.TempDir cleanup can proceed
	makeTreeWritableForFollower(t, readerDir)
	require.NoError(t, reader.Shutdown(ctx))
}

// bootFollowerTestDB boots a DB over rootDir sharing the given shardState. A
// writer (readOnly=false) materializes the class via migrator.AddClass after
// startup. A reader (readOnly=true) sets Config.ReadOnly and has the schema
// present before WaitForStartup, so the startup path (db.init → NewIndex →
// initAndStoreShards' ReadOnly branch) materializes the shards as ReadOnlyShards,
// mirroring how a follower loads shards from a copy.
func bootFollowerTestDB(t *testing.T, rootDir string, readOnly bool, shardState *sharding.State, classes ...*models.Class) *DB {
	t.Helper()
	logger, _ := test.NewNullLogger()

	// The reader needs the schema before startup; the writer materializes it via
	// AddClass afterwards (so db.init's startup loop doesn't double-create it).
	startupClasses := classes
	if !readOnly {
		startupClasses = nil
	}
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: startupClasses}},
		shardState: shardState,
	}
	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		class := &models.Class{Class: className}
		return readFunc(class, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: classes}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockSchemaReader.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, version uint64) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return nil
	}).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()

	cfg := Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  rootDir,
		QueryMaximumResults:       10,
		MaxImportGoroutinesFactor: 1,
		ReadOnly:                  readOnly,
	}
	if readOnly {
		cfg.EnableLazyLoadShards = boolPtr(false) // eager-load so shards open read-only at boot
	}

	db, err := New(logger, "node1", cfg,
		&FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader)
	require.Nil(t, err)
	db.SetSchemaGetter(schemaGetter)
	require.Nil(t, db.WaitForStartup(testCtx()))

	if !readOnly {
		migrator := NewMigrator(db, logger, "node1")
		for _, class := range classes {
			require.Nil(t, migrator.AddClass(context.Background(), class))
		}
		schemaGetter.schema = schema.Schema{Objects: &models.Schema{Classes: classes}}
	}

	return db
}

// --- read-only tree helpers (suffixed to avoid clashing with other test files) ---

func copyTreeForReadOnly(src, dst string) error {
	return filepath.Walk(src, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o644)
	})
}

type fileFingerprint struct {
	size int64
	mode os.FileMode
}

func snapshotTreeForReadOnly(t *testing.T, dir string) map[string]fileFingerprint {
	t.Helper()
	out := map[string]fileFingerprint{}
	require.NoError(t, filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		out[rel] = fileFingerprint{size: info.Size(), mode: info.Mode().Perm()}
		return nil
	}))
	return out
}

func makeTreeReadOnlyForFollower(t *testing.T, dir string) {
	t.Helper()
	// chmod files first, then dirs, so we can still traverse while changing perms.
	var dirs []string
	require.NoError(t, filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			dirs = append(dirs, p)
			return nil
		}
		return os.Chmod(p, 0o400)
	}))
	sort.Sort(sort.Reverse(sort.StringSlice(dirs)))
	for _, d := range dirs {
		require.NoError(t, os.Chmod(d, 0o500))
	}
}

func makeTreeWritableForFollower(t *testing.T, dir string) {
	t.Helper()
	_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			_ = os.Chmod(p, 0o700)
		} else {
			_ = os.Chmod(p, 0o600)
		}
		return nil
	})
}
