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
	"errors"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/objects"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// openDurabilityDB opens a *DB at rootPath using the same mock wiring as
// TestRestartJourney. shardState must be the SAME instance across all open/close
// cycles so that the mock schema reader returns consistent shard names on reopen.
// The caller is responsible for calling db.Shutdown when done.
func openDurabilityDB(t *testing.T, rootPath string, shardState *sharding.State, schemaGetter *fakeSchemaGetter) *DB {
	t.Helper()

	logger, _ := test.NewNullLogger()

	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
			class := &models.Class{Class: className}
			return readFunc(class, shardState)
		},
	).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()

	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()

	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()

	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  rootPath,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil, nil,
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader)
	require.NoError(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.NoError(t, repo.WaitForStartup(testCtx()))
	return repo
}

// getDurabilityShard retrieves the single shard from the named class index.
// It fails the test if the index or shard is missing.
func getDurabilityShard(t *testing.T, repo *DB, className string) ShardLike {
	t.Helper()

	idx := repo.GetIndex(schema.ClassName(className))
	require.NotNil(t, idx, "index for class %q not found", className)

	var found ShardLike
	err := idx.ForEachShard(func(_ string, shard ShardLike) error {
		found = shard
		return nil
	})
	require.NoError(t, err)
	require.NotNil(t, found, "shard for class %q not found", className)
	return found
}

// newConditionalInsertObj returns a storobj with OnlyIfNotExists=true.
func newConditionalInsertObj(className string, id strfmt.UUID) *storobj.Object {
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:                 id,
			Class:              className,
			LastUpdateTimeUnix: time.Now().UnixMilli(),
		},
		Conditional: storobj.Conditional{
			OnlyIfNotExists: true,
		},
	}
}

// TestConditionalDurabilityRestart validates three durability invariants for
// conditional writes (insert_if_not_exists) across a simulated node restart:
//
//  1. AC1 (existence durable after WAL replay): insert an object via
//     insert_if_not_exists, stop+reopen the shard (WAL replay), re-issue
//     insert_if_not_exists on the same UUID -> must get ErrPreconditionFailed.
//     The object survived the restart and is visible to the condition check.
//
//  2. AC2 (INV-DURABILITY-1, WAL-before-ack): the object is written to WAL
//     only - no explicit flush is called before shutdown. The test proves that
//     WAL replay on startup makes the pre-flush write durable.
//
//  3. AC3 (no torn read after reopen): an unconditional ObjectByID returns the
//     same object that the conditional check detects. Reads and condition checks
//     agree after WAL replay.
//
// Restart mechanism: repo.Shutdown(ctx) flushes the WAL file descriptor, then
// a new *DB is opened on the same directory via New(...) + WaitForStartup(...).
// WaitForStartup -> db.init() -> NewIndex -> initAndStoreShards -> NewShard, which
// opens the lsmkv Store and replays all unflushed WAL segments. This is the exact
// same code path a real node restart takes; it is not a fake.
//
// Critical setup requirement: the SAME *sharding.State instance must be passed to
// both open/close cycles. The mock schema reader's Read callback returns this state
// to NewIndex -> initAndStoreShards so it knows which shard names to open. If a
// fresh state were created for the second open, it would generate a different shard
// name and open an empty shard, missing the WAL data from the first instance.
//
// Causal link:
//   - AC1: without WAL replay, ObjectByID in putObjectLSM sees prevObj==nil on
//     the second insert and returns nil (success), failing require.Error.
//   - AC2: without WAL replay on startup, the object is lost because the memtable
//     was never flushed to SSTable before shutdown, making the second insert succeed.
//   - AC3: without WAL replay or with a torn write, ObjectByID returns nil after
//     reopen, failing require.NotNil.
func TestConditionalDurabilityRestart(t *testing.T) {
	ctx := context.Background()
	const className = "DurabilityRestartCAS"
	dirName := t.TempDir()

	durabilityClass := &models.Class{
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Class:               className,
	}

	// shardState is created once and reused across both open/close cycles.
	// This ensures the mock schema reader returns the same shard name on reopen
	// so NewIndex -> initAndStoreShards opens the shard directory that was written
	// by the first DB instance.
	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}

	// Phase 1: open DB, register class, write object via insert_if_not_exists
	// without any explicit flush - the object lives only in the WAL/memtable.
	repo := openDurabilityDB(t, dirName, shardState, schemaGetter)

	migratorLogger, _ := test.NewNullLogger()
	migrator := NewMigrator(repo, migratorLogger, "node1")
	require.NoError(t, migrator.AddClass(ctx, durabilityClass))

	// Keep the schema getter in sync so the reopened DB finds the class on
	// startup (db.init iterates schemaGetter.GetSchemaSkipAuth().Objects.Classes).
	schemaGetter.schema = schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{durabilityClass},
		},
	}

	id := strfmt.UUID(uuid.NewString())
	shard := getDurabilityShard(t, repo, className)

	// First insert: must succeed (object does not exist yet).
	err := shard.PutObject(ctx, newConditionalInsertObj(className, id))
	require.NoError(t, err, "first insert_if_not_exists must succeed (object did not exist)")

	// Verify the object is readable before shutdown (sanity check).
	preShutdownObj, err := shard.ObjectByID(ctx, id, search.SelectProperties{}, additional.Properties{})
	require.NoError(t, err)
	require.NotNil(t, preShutdownObj, "object must be readable before shutdown")

	// Phase 2: shutdown WITHOUT any explicit flush.
	// This is the WAL-before-ack proof (INV-DURABILITY-1): the object resides
	// only in the WAL/memtable at this point; it was never explicitly flushed
	// to an SSTable segment before shutdown.
	require.NoError(t, repo.Shutdown(ctx))
	repo = nil

	// Phase 3: reopen - WAL replay fires inside WaitForStartup.
	// New DB instance on the same directory; db.init() -> NewIndex ->
	// initAndStoreShards -> NewShard -> lsmkv.NewStore -> WAL replay.
	// This is a real restart, not a fake: the in-memory shard state from the
	// first instance is gone and must be reconstructed from the WAL files on disk.
	reopened := openDurabilityDB(t, dirName, shardState, schemaGetter)
	defer func() { _ = reopened.Shutdown(ctx) }()

	reopenedShard := getDurabilityShard(t, reopened, className)

	// AC3: unconditional GET must find the object after WAL replay.
	// Without WAL replay, the object does not exist in the new shard's LSM store
	// and this call returns nil, failing the assertion.
	postRestartObj, err := reopenedShard.ObjectByID(ctx, id, search.SelectProperties{}, additional.Properties{})
	require.NoError(t, err, "ObjectByID must not error after WAL replay")
	require.NotNil(t, postRestartObj,
		"object must be visible via unconditional GET after WAL replay (AC3: no torn read)")

	// AC1 + AC2: re-issue insert_if_not_exists on the same UUID.
	// The object survived the restart (WAL replay), so the existence check in
	// putObjectLSM must see prevObj!=nil and return ErrPreconditionFailed.
	// If WAL replay had not run, prevObj would be nil and the insert would
	// succeed (nil error), breaking the durability guarantee.
	err = reopenedShard.PutObject(ctx, newConditionalInsertObj(className, id))
	require.Error(t, err,
		"second insert_if_not_exists must fail: object must have survived WAL replay (AC1+AC2)")

	var precondErr *objects.ErrPreconditionFailed
	require.True(t, errors.As(err, &precondErr),
		"error must be ErrPreconditionFailed, got: %v (AC1: condition must see durable state)", err)
}
