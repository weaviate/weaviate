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
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/memwatch"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// setupReplayShard spins up a single-shard repo/index for replay tests.
// Mirrors the bootstrap pattern used by TestOverwriteObjects.
func setupReplayShard(t *testing.T) (repo *DB, idx *Index, shard string, class *models.Class) {
	t.Helper()

	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()
	class = &models.Class{
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Class:               "ReplayClass",
		Properties: []*models.Property{
			{
				Name:         "stringProp",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
	}

	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		return readFunc(class, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()

	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()

	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()

	var err error
	repo, err = New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10,
		MaxImportGoroutinesFactor: 1,
	}, &FakeRemoteClient{}, &FakeNodeResolver{},
		&FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	t.Cleanup(func() { repo.Shutdown(context.Background()) })

	migrator := NewMigrator(repo, logger, "node1")
	require.Nil(t, migrator.AddClass(context.Background(), class))
	schemaGetter.schema = schema.Schema{
		Objects: &models.Schema{Classes: []*models.Class{class}},
	}

	idx = repo.GetIndex(schema.ClassName(class.Class))
	shard, err = idx.shardResolver.ResolveShardByObjectID(context.Background(), strfmt.UUID("981c09f9-67f3-4e6e-a988-c53eaefbd58e"), "")
	require.Nil(t, err)
	return repo, idx, shard, class
}

// mustMarshalPayload produces the raw storobj bytes that the source-side tee writes.
func mustMarshalPayload(t *testing.T, obj *models.Object, vector []float32) []byte {
	t.Helper()
	so := storobj.FromObject(obj, vector, nil, nil)
	payload, err := so.MarshalBinary()
	require.Nil(t, err)
	return payload
}

// TestOverwriteObjectsFromChangeLog_SkipsWhenLocalNewer guards the LWW skip
// gate on both verbs: neither a PUT nor a DELETE with a stale timestamp must
// mutate the local object.
func TestOverwriteObjectsFromChangeLog_SkipsWhenLocalNewer(t *testing.T) {
	repo, idx, shard, class := setupReplayShard(t)

	now := time.Now()
	earlier := now.Add(-time.Hour)
	later := now.Add(time.Hour)
	id := strfmt.UUID("981c09f9-67f3-4e6e-a988-c53eaefbd58e")

	local := &models.Object{
		ID:                 id,
		Class:              class.Class,
		CreationTimeUnix:   later.UnixMilli(),
		LastUpdateTimeUnix: later.UnixMilli(),
		Properties:         map[string]interface{}{"stringProp": "local wins"},
		Vector:             []float32{1, 2, 3},
		VectorWeights:      (map[string]string)(nil),
		Additional:         models.AdditionalProperties{},
	}
	require.Nil(t, repo.PutObject(context.Background(), local, local.Vector, nil, nil, nil, 0))

	stalePut := &models.Object{
		ID:                 id,
		Class:              class.Class,
		CreationTimeUnix:   earlier.UnixMilli(),
		LastUpdateTimeUnix: earlier.UnixMilli(),
		Properties:         map[string]interface{}{"stringProp": "should be ignored"},
		Vector:             []float32{9, 9, 9},
		VectorWeights:      (map[string]string)(nil),
		Additional:         models.AdditionalProperties{},
	}
	putEntry := ChangeLogReplayEntry{
		ID:                      id,
		LastUpdateTimeUnixMilli: earlier.UnixMilli(),
		IsDelete:                false,
		Payload:                 mustMarshalPayload(t, stalePut, stalePut.Vector),
	}
	deleteEntry := ChangeLogReplayEntry{
		ID:                      id,
		LastUpdateTimeUnixMilli: earlier.UnixMilli(),
		IsDelete:                true,
	}

	require.Nil(t, idx.OverwriteObjectsFromChangeLog(context.Background(), shard, []ChangeLogReplayEntry{putEntry, deleteEntry}))

	found, err := repo.Object(context.Background(), class.Class, id, nil, additional.Properties{}, nil, "")
	require.Nil(t, err)
	require.NotNil(t, found, "local object must still exist")
	assert.EqualValues(t, local, found.Object())
}

// TestOverwriteObjectsFromChangeLog_AppliesWhenLocalOlder covers the PUT
// round-trip: obj.MarshalBinary → storobj.FromBinary → PutObjectBatch.
func TestOverwriteObjectsFromChangeLog_AppliesWhenLocalOlder(t *testing.T) {
	repo, idx, shard, class := setupReplayShard(t)

	now := time.Now()
	later := now.Add(time.Hour)
	id := strfmt.UUID("981c09f9-67f3-4e6e-a988-c53eaefbd58e")

	local := &models.Object{
		ID:                 id,
		Class:              class.Class,
		CreationTimeUnix:   now.UnixMilli(),
		LastUpdateTimeUnix: now.UnixMilli(),
		Properties:         map[string]interface{}{"stringProp": "old"},
		Vector:             []float32{1, 2, 3},
		VectorWeights:      (map[string]string)(nil),
		Additional:         models.AdditionalProperties{},
	}
	require.Nil(t, repo.PutObject(context.Background(), local, local.Vector, nil, nil, nil, 0))

	fresh := &models.Object{
		ID:                 id,
		Class:              class.Class,
		CreationTimeUnix:   now.UnixMilli(),
		LastUpdateTimeUnix: later.UnixMilli(),
		Properties:         map[string]interface{}{"stringProp": "new"},
		Vector:             []float32{4, 5, 6},
		VectorWeights:      (map[string]string)(nil),
		Additional:         models.AdditionalProperties{},
	}
	entry := ChangeLogReplayEntry{
		ID:                      id,
		LastUpdateTimeUnixMilli: later.UnixMilli(),
		IsDelete:                false,
		Payload:                 mustMarshalPayload(t, fresh, fresh.Vector),
	}

	require.Nil(t, idx.OverwriteObjectsFromChangeLog(context.Background(), shard, []ChangeLogReplayEntry{entry}))

	found, err := repo.Object(context.Background(), class.Class, id, nil, additional.Properties{}, nil, "")
	require.Nil(t, err)
	require.NotNil(t, found)
	assert.EqualValues(t, fresh, found.Object())
}

// TestOverwriteObjectsFromChangeLog_DeletesAndTombstoneWinsOverOlderPut guards
// the lsmkv.Deleted branch that pulls currUpdateTime from the tombstone's
// deletion time — without this, a subsequent older PUT would resurrect the
// object.
func TestOverwriteObjectsFromChangeLog_DeletesAndTombstoneWinsOverOlderPut(t *testing.T) {
	repo, idx, shard, class := setupReplayShard(t)

	now := time.Now()
	earliest := now.Add(-2 * time.Hour)
	later := now.Add(time.Hour)
	id := strfmt.UUID("981c09f9-67f3-4e6e-a988-c53eaefbd58e")

	local := &models.Object{
		ID:                 id,
		Class:              class.Class,
		CreationTimeUnix:   now.UnixMilli(),
		LastUpdateTimeUnix: now.UnixMilli(),
		Properties:         map[string]interface{}{"stringProp": "to be deleted"},
		Vector:             []float32{1, 2, 3},
	}
	require.Nil(t, repo.PutObject(context.Background(), local, local.Vector, nil, nil, nil, 0))

	deleteEntry := ChangeLogReplayEntry{
		ID:                      id,
		LastUpdateTimeUnixMilli: later.UnixMilli(),
		IsDelete:                true,
	}
	require.Nil(t, idx.OverwriteObjectsFromChangeLog(context.Background(), shard, []ChangeLogReplayEntry{deleteEntry}))

	found, err := repo.Object(context.Background(), class.Class, id, nil, additional.Properties{}, nil, "")
	require.Nil(t, err)
	require.Nil(t, found, "object must be gone after delete replay")

	stalePut := &models.Object{
		ID:                 id,
		Class:              class.Class,
		CreationTimeUnix:   earliest.UnixMilli(),
		LastUpdateTimeUnix: earliest.UnixMilli(),
		Properties:         map[string]interface{}{"stringProp": "should not resurrect"},
		Vector:             []float32{9, 9, 9},
	}
	putEntry := ChangeLogReplayEntry{
		ID:                      id,
		LastUpdateTimeUnixMilli: earliest.UnixMilli(),
		IsDelete:                false,
		Payload:                 mustMarshalPayload(t, stalePut, stalePut.Vector),
	}
	require.Nil(t, idx.OverwriteObjectsFromChangeLog(context.Background(), shard, []ChangeLogReplayEntry{putEntry}))

	found, err = repo.Object(context.Background(), class.Class, id, nil, additional.Properties{}, nil, "")
	require.Nil(t, err)
	assert.Nil(t, found, "tombstone must win over older PUT")
}

// TestOverwriteObjectsFromChangeLog_InOrderPutThenDelete firewalls the
// strict-in-order contract: a future refactor that defers PUTs (the
// OverwriteObjects pattern) would flip the end state from deleted to present.
func TestOverwriteObjectsFromChangeLog_InOrderPutThenDelete(t *testing.T) {
	repo, idx, shard, class := setupReplayShard(t)

	id := strfmt.UUID("981c09f9-67f3-4e6e-a988-c53eaefbd58e")
	t10 := int64(10)
	t15 := int64(15)

	putObj := &models.Object{
		ID:                 id,
		Class:              class.Class,
		CreationTimeUnix:   t10,
		LastUpdateTimeUnix: t10,
		Properties:         map[string]interface{}{"stringProp": "put-then-delete"},
		Vector:             []float32{1, 2, 3},
	}
	batch := []ChangeLogReplayEntry{
		{
			ID:                      id,
			LastUpdateTimeUnixMilli: t10,
			IsDelete:                false,
			Payload:                 mustMarshalPayload(t, putObj, putObj.Vector),
		},
		{
			ID:                      id,
			LastUpdateTimeUnixMilli: t15,
			IsDelete:                true,
		},
	}

	require.Nil(t, idx.OverwriteObjectsFromChangeLog(context.Background(), shard, batch))

	found, err := repo.Object(context.Background(), class.Class, id, nil, additional.Properties{}, nil, "")
	require.Nil(t, err)
	assert.Nil(t, found, "end state after [PUT@10, DELETE@15] must be deleted")
}

// TestOverwriteObjectsFromChangeLog_BatchesContiguousPuts is the correctness
// firewall for the batched PUT path; the perf signal lives in the acceptance
// test wall-clock.
func TestOverwriteObjectsFromChangeLog_BatchesContiguousPuts(t *testing.T) {
	repo, idx, shard, class := setupReplayShard(t)

	now := time.Now()
	const N = 50
	objs := make([]*models.Object, N)
	entries := make([]ChangeLogReplayEntry, N)
	for i := 0; i < N; i++ {
		id := strfmt.UUID(uuid.NewString())
		objs[i] = &models.Object{
			ID:                 id,
			Class:              class.Class,
			CreationTimeUnix:   now.UnixMilli(),
			LastUpdateTimeUnix: now.UnixMilli(),
			Properties:         map[string]interface{}{"stringProp": fmt.Sprintf("contig-%d", i)},
			Vector:             []float32{float32(i), float32(i + 1), float32(i + 2)},
		}
		entries[i] = ChangeLogReplayEntry{
			ID:                      id,
			LastUpdateTimeUnixMilli: now.UnixMilli(),
			IsDelete:                false,
			Payload:                 mustMarshalPayload(t, objs[i], objs[i].Vector),
		}
	}

	require.Nil(t, idx.OverwriteObjectsFromChangeLog(context.Background(), shard, entries))

	for i := 0; i < N; i++ {
		found, err := repo.Object(context.Background(), class.Class, objs[i].ID, nil, additional.Properties{}, nil, "")
		require.Nilf(t, err, "object %d", i)
		require.NotNilf(t, found, "object %d must exist after batched replay", i)
	}
}

// TestOverwriteObjectsFromChangeLog_PutsThenDeleteThenPutsRoundTrip guards
// flush-on-DELETE: a buffered same-UUID PUT must land before the DELETE, and
// a post-DELETE PUT must still flush at end-of-input.
func TestOverwriteObjectsFromChangeLog_PutsThenDeleteThenPutsRoundTrip(t *testing.T) {
	repo, idx, shard, class := setupReplayShard(t)

	t10 := int64(10)
	t15 := int64(15)
	idA := strfmt.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	idB := strfmt.UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
	idC := strfmt.UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")

	mk := func(id strfmt.UUID, ts int64, prop string) (*models.Object, ChangeLogReplayEntry) {
		obj := &models.Object{
			ID:                 id,
			Class:              class.Class,
			CreationTimeUnix:   ts,
			LastUpdateTimeUnix: ts,
			Properties:         map[string]interface{}{"stringProp": prop},
			Vector:             []float32{1, 2, 3},
		}
		return obj, ChangeLogReplayEntry{
			ID:                      id,
			LastUpdateTimeUnixMilli: ts,
			IsDelete:                false,
			Payload:                 mustMarshalPayload(t, obj, obj.Vector),
		}
	}

	_, putA := mk(idA, t10, "A")
	_, putB := mk(idB, t10, "B")
	delA := ChangeLogReplayEntry{ID: idA, LastUpdateTimeUnixMilli: t15, IsDelete: true}
	_, putC := mk(idC, t10, "C")

	batch := []ChangeLogReplayEntry{putA, putB, delA, putC}
	require.Nil(t, idx.OverwriteObjectsFromChangeLog(context.Background(), shard, batch))

	foundA, err := repo.Object(context.Background(), class.Class, idA, nil, additional.Properties{}, nil, "")
	require.Nil(t, err)
	assert.Nil(t, foundA, "A must be deleted (PUT@10 flushed before DELETE@15)")

	foundB, err := repo.Object(context.Background(), class.Class, idB, nil, additional.Properties{}, nil, "")
	require.Nil(t, err)
	require.NotNil(t, foundB, "B must survive the DELETE-flush of A")

	foundC, err := repo.Object(context.Background(), class.Class, idC, nil, additional.Properties{}, nil, "")
	require.Nil(t, err)
	require.NotNil(t, foundC, "C must land in the post-DELETE batch")
}

// TestOverwriteObjectsFromChangeLog_NonMonotonicTimestampSameUUIDKeepsMax
// guards the buffer's max-time dedupe — without it, clock-skewed clients could
// resurrect an older PUT over a newer one in the same drain chunk.
func TestOverwriteObjectsFromChangeLog_NonMonotonicTimestampSameUUIDKeepsMax(t *testing.T) {
	repo, idx, shard, class := setupReplayShard(t)

	id := strfmt.UUID("981c09f9-67f3-4e6e-a988-c53eaefbd58e")
	t10 := int64(10)
	t8 := int64(8)

	objHigh := &models.Object{
		ID:                 id,
		Class:              class.Class,
		CreationTimeUnix:   t10,
		LastUpdateTimeUnix: t10,
		Properties:         map[string]interface{}{"stringProp": "winner-t10"},
		Vector:             []float32{1, 2, 3},
	}
	objLow := &models.Object{
		ID:                 id,
		Class:              class.Class,
		CreationTimeUnix:   t8,
		LastUpdateTimeUnix: t8,
		Properties:         map[string]interface{}{"stringProp": "loser-t8"},
		Vector:             []float32{4, 5, 6},
	}

	batch := []ChangeLogReplayEntry{
		{
			ID:                      id,
			LastUpdateTimeUnixMilli: t10,
			IsDelete:                false,
			Payload:                 mustMarshalPayload(t, objHigh, objHigh.Vector),
		},
		{
			ID:                      id,
			LastUpdateTimeUnixMilli: t8,
			IsDelete:                false,
			Payload:                 mustMarshalPayload(t, objLow, objLow.Vector),
		},
	}

	require.Nil(t, idx.OverwriteObjectsFromChangeLog(context.Background(), shard, batch))

	found, err := repo.Object(context.Background(), class.Class, id, nil, additional.Properties{}, nil, "")
	require.Nil(t, err)
	require.NotNil(t, found)
	assert.EqualValues(t, t10, found.Object().LastUpdateTimeUnix, "max-time entry must win within-buffer dedupe")
	assert.EqualValues(t, "winner-t10", found.Object().Properties.(map[string]interface{})["stringProp"])
}

// TestOverwriteObjectsFromChangeLog_DecodeErrorAborts guards the
// abort-on-first-error contract Phase 5 relies on: a mid-batch decode failure
// must stop replay, and a following well-formed entry must NOT be applied.
func TestOverwriteObjectsFromChangeLog_DecodeErrorAborts(t *testing.T) {
	repo, idx, shard, class := setupReplayShard(t)

	id1 := strfmt.UUID("981c09f9-67f3-4e6e-a988-c53eaefbd58e")
	id2 := strfmt.UUID("11111111-2222-3333-4444-555555555555")
	id3 := strfmt.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	now := time.Now()

	obj1 := &models.Object{
		ID:                 id1,
		Class:              class.Class,
		CreationTimeUnix:   now.UnixMilli(),
		LastUpdateTimeUnix: now.UnixMilli(),
		Properties:         map[string]interface{}{"stringProp": "valid-before-error"},
		Vector:             []float32{1, 2, 3},
	}
	obj3 := &models.Object{
		ID:                 id3,
		Class:              class.Class,
		CreationTimeUnix:   now.UnixMilli(),
		LastUpdateTimeUnix: now.UnixMilli(),
		Properties:         map[string]interface{}{"stringProp": "must-not-be-applied"},
		Vector:             []float32{4, 5, 6},
	}

	batch := []ChangeLogReplayEntry{
		{
			ID:                      id1,
			LastUpdateTimeUnixMilli: now.UnixMilli(),
			IsDelete:                false,
			Payload:                 mustMarshalPayload(t, obj1, obj1.Vector),
		},
		{
			ID:                      id2,
			LastUpdateTimeUnixMilli: now.UnixMilli(),
			IsDelete:                false,
			Payload:                 []byte{0xFF}, // unsupported marshaller version → clean decode error
		},
		{
			ID:                      id3,
			LastUpdateTimeUnixMilli: now.UnixMilli(),
			IsDelete:                false,
			Payload:                 mustMarshalPayload(t, obj3, obj3.Vector),
		},
	}

	err := idx.OverwriteObjectsFromChangeLog(context.Background(), shard, batch)
	require.Error(t, err, "replay must abort on decode error")
	assert.Contains(t, err.Error(), id2.String(), "error must identify the failing entry")

	// Entry 1 landed (before the error), entry 3 must not have.
	found1, err := repo.Object(context.Background(), class.Class, id1, nil, additional.Properties{}, nil, "")
	require.Nil(t, err)
	require.NotNil(t, found1, "entry 1 (before the error) must have been applied")

	found3, err := repo.Object(context.Background(), class.Class, id3, nil, additional.Properties{}, nil, "")
	require.Nil(t, err)
	assert.Nil(t, found3, "entry 3 (after the error) must NOT have been applied")
}
