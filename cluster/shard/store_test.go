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

package shard_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/shard"
	"github.com/weaviate/weaviate/cluster/shard/mocks"
	shardproto "github.com/weaviate/weaviate/cluster/shard/proto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"google.golang.org/protobuf/proto"
)

const (
	testClassName = "TestClass"
	testShardName = "test-shard"
	testNodeID    = "node-1"
)

// newTestStore creates a Store backed by a single-node in-memory RAFT cluster.
// It returns the store and the mock shard that is already wired via SetShard.
func newTestStore(t *testing.T) (*shard.Store, *mocks.Mockshard) {
	t.Helper()

	_, transport := raft.NewInmemTransport("")

	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	cfg := shard.StoreConfig{
		ClassName:          testClassName,
		ShardName:          testShardName,
		NodeID:             testNodeID,
		DataPath:           t.TempDir(),
		Members:            []string{testNodeID},
		Logger:             logger,
		Transport:          transport,
		HeartbeatTimeout:   150 * time.Millisecond,
		ElectionTimeout:    150 * time.Millisecond,
		LeaderLeaseTimeout: 100 * time.Millisecond,
		SnapshotInterval:   10 * time.Second,
		SnapshotThreshold:  1024,
	}

	store, err := shard.NewStore(cfg)
	require.NoError(t, err)

	mockShard := mocks.NewMockshard(t)
	store.SetShard(mockShard)

	return store, mockShard
}

// startAndWaitForLeader starts the store and waits for the single-node
// cluster to elect itself leader. It fails the test if leadership isn't
// acquired within 5 seconds.
func startAndWaitForLeader(t *testing.T, store *shard.Store) {
	t.Helper()

	err := store.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Stop() })

	deadline := time.After(5 * time.Second)
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for leader election")
		case <-ticker.C:
			if store.IsLeader() {
				return
			}
		}
	}
}

// buildPutObjectApplyRequest constructs a protobuf ApplyRequest of type
// TYPE_PUT_OBJECT containing the given storobj.Object.
func buildPutObjectApplyRequest(t *testing.T, className, shardName string, obj *storobj.Object) *shardproto.ApplyRequest {
	t.Helper()

	objBytes, err := obj.MarshalBinary()
	require.NoError(t, err)

	subCmd, err := proto.Marshal(&shardproto.PutObjectRequest{
		Object: objBytes,
	})
	require.NoError(t, err)

	return &shardproto.ApplyRequest{
		Type:       shardproto.ApplyRequest_TYPE_PUT_OBJECT,
		Class:      className,
		Shard:      shardName,
		SubCommand: subCmd,
	}
}

// makeTestObject creates a minimal storobj.Object suitable for testing.
func makeTestObject() *storobj.Object {
	obj := &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:                 strfmt.UUID("12345678-1234-1234-1234-123456789abc"),
			Class:              testClassName,
			CreationTimeUnix:   1000000,
			LastUpdateTimeUnix: 1000000,
		},
		Vector:    []float32{0.1, 0.2, 0.3},
		VectorLen: 3,
	}
	return obj
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestStore_NewStore_DefaultTimeout(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)
	_, transport := raft.NewInmemTransport("")

	cfg := shard.StoreConfig{
		ClassName: testClassName,
		ShardName: testShardName,
		NodeID:    testNodeID,
		DataPath:  t.TempDir(),
		Members:   []string{testNodeID},
		Logger:    logger,
		Transport: transport,
		// ApplyTimeout intentionally left at zero
	}

	store, err := shard.NewStore(cfg)
	require.NoError(t, err)
	assert.NotNil(t, store)
}

func TestStore_Start_BecomesLeader(t *testing.T) {
	store, _ := newTestStore(t)
	startAndWaitForLeader(t, store)

	assert.True(t, store.IsLeader())
}

func TestStore_Start_Idempotent(t *testing.T) {
	store, _ := newTestStore(t)
	startAndWaitForLeader(t, store)

	// Second Start should be a no-op and return nil
	err := store.Start(context.Background())
	assert.NoError(t, err)
}

func TestStore_Start_AfterStop(t *testing.T) {
	store, _ := newTestStore(t)

	err := store.Start(context.Background())
	require.NoError(t, err)

	err = store.Stop()
	require.NoError(t, err)

	err = store.Start(context.Background())
	assert.ErrorIs(t, err, shard.ErrAlreadyClosed)
}

func TestStore_Apply_PutObject(t *testing.T) {
	store, mockShard := newTestStore(t)
	startAndWaitForLeader(t, store)

	mockShard.EXPECT().PutObject(mock.Anything, mock.Anything).Return(nil)

	obj := makeTestObject()
	req := buildPutObjectApplyRequest(t, testClassName, testShardName, obj)

	version, err := store.Apply(context.Background(), req)
	require.NoError(t, err)
	assert.Greater(t, version, uint64(0))

	mockShard.AssertCalled(t, "PutObject", mock.Anything, mock.Anything)
}

func TestStore_Apply_PutObject_ShardError(t *testing.T) {
	store, mockShard := newTestStore(t)
	startAndWaitForLeader(t, store)

	shardErr := fmt.Errorf("disk full")
	mockShard.EXPECT().PutObject(mock.Anything, mock.Anything).Return(shardErr)

	obj := makeTestObject()
	req := buildPutObjectApplyRequest(t, testClassName, testShardName, obj)

	_, err := store.Apply(context.Background(), req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "disk full")
}

func TestStore_Apply_NotStarted(t *testing.T) {
	store, _ := newTestStore(t)

	obj := makeTestObject()
	req := buildPutObjectApplyRequest(t, testClassName, testShardName, obj)

	_, err := store.Apply(context.Background(), req)
	assert.ErrorIs(t, err, shard.ErrNotStarted)
}

func TestStore_Apply_AfterStop(t *testing.T) {
	store, _ := newTestStore(t)
	startAndWaitForLeader(t, store)

	err := store.Stop()
	require.NoError(t, err)

	obj := makeTestObject()
	req := buildPutObjectApplyRequest(t, testClassName, testShardName, obj)

	_, err = store.Apply(context.Background(), req)
	require.Error(t, err)
	// After Stop(), started=false and closed=true. Apply checks started first,
	// so ErrNotStarted is returned. Either sentinel error is acceptable here.
	assert.True(t, errors.Is(err, shard.ErrNotStarted) || errors.Is(err, shard.ErrAlreadyClosed),
		"expected ErrNotStarted or ErrAlreadyClosed, got: %v", err)
}

func TestStore_Apply_IncrementsIndex(t *testing.T) {
	store, mockShard := newTestStore(t)
	startAndWaitForLeader(t, store)

	mockShard.EXPECT().PutObject(mock.Anything, mock.Anything).Return(nil)

	obj := makeTestObject()

	req1 := buildPutObjectApplyRequest(t, testClassName, testShardName, obj)
	v1, err := store.Apply(context.Background(), req1)
	require.NoError(t, err)

	req2 := buildPutObjectApplyRequest(t, testClassName, testShardName, obj)
	v2, err := store.Apply(context.Background(), req2)
	require.NoError(t, err)

	assert.Greater(t, v2, v1)
}

func TestStore_IsLeader_SingleNode(t *testing.T) {
	store, _ := newTestStore(t)
	startAndWaitForLeader(t, store)

	assert.True(t, store.IsLeader())
}

func TestStore_IsLeader_NotStarted(t *testing.T) {
	store, _ := newTestStore(t)

	assert.False(t, store.IsLeader())
}

func TestStore_Leader_ReturnsAddress(t *testing.T) {
	store, _ := newTestStore(t)
	startAndWaitForLeader(t, store)

	leader := store.Leader()
	assert.NotEmpty(t, leader)
}

func TestStore_LeaderID_ReturnsNodeID(t *testing.T) {
	store, _ := newTestStore(t)
	startAndWaitForLeader(t, store)

	leaderID := store.LeaderID()
	assert.Equal(t, testNodeID, leaderID)
}

func TestStore_LastAppliedIndex(t *testing.T) {
	store, mockShard := newTestStore(t)
	startAndWaitForLeader(t, store)

	initialIdx := store.LastAppliedIndex()

	mockShard.EXPECT().PutObject(mock.Anything, mock.Anything).Return(nil)

	obj := makeTestObject()
	req := buildPutObjectApplyRequest(t, testClassName, testShardName, obj)

	_, err := store.Apply(context.Background(), req)
	require.NoError(t, err)

	afterIdx := store.LastAppliedIndex()
	assert.Greater(t, afterIdx, initialIdx)
}

func TestStore_State_Leader(t *testing.T) {
	store, _ := newTestStore(t)
	startAndWaitForLeader(t, store)

	assert.Equal(t, raft.Leader, store.State())
}

func TestStore_Stop_Idempotent(t *testing.T) {
	store, _ := newTestStore(t)

	err := store.Start(context.Background())
	require.NoError(t, err)

	err = store.Stop()
	assert.NoError(t, err)

	err = store.Stop()
	assert.NoError(t, err)
}

func TestStore_Stop_NotStarted(t *testing.T) {
	store, _ := newTestStore(t)

	err := store.Stop()
	assert.NoError(t, err)

	// After Stop (even without Start), Start should fail with ErrAlreadyClosed
	err = store.Start(context.Background())
	assert.ErrorIs(t, err, shard.ErrAlreadyClosed)
}

// ---------------------------------------------------------------------------
// FSM Snapshot-Flush Tests (Phase 2)
// ---------------------------------------------------------------------------

// fakeSnapshotSink implements raft.SnapshotSink for testing Persist().
type fakeSnapshotSink struct {
	bytes.Buffer
	cancelled bool
}

func (f *fakeSnapshotSink) Close() error { return nil }
func (f *fakeSnapshotSink) ID() string   { return "fake-snap-1" }
func (f *fakeSnapshotSink) Cancel() error {
	f.cancelled = true
	return nil
}

func TestFSM_Snapshot_FlushesMemtables(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	fsm := shard.NewFSM(testClassName, testShardName, testNodeID, logger)
	mockShard := mocks.NewMockshard(t)
	fsm.SetShard(mockShard)

	mockShard.EXPECT().FlushMemtables(mock.Anything).Return(nil)

	snap, err := fsm.Snapshot()
	require.NoError(t, err)
	require.NotNil(t, snap)

	// FlushMemtables is called during Persist(), not Snapshot().
	sink := &fakeSnapshotSink{}
	err = snap.Persist(sink)
	require.NoError(t, err)

	mockShard.AssertCalled(t, "FlushMemtables", mock.Anything)
	assert.False(t, sink.cancelled)
}

func TestFSM_Persist_FlushError_CancelsSink(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	fsm := shard.NewFSM(testClassName, testShardName, testNodeID, logger)
	mockShard := mocks.NewMockshard(t)
	fsm.SetShard(mockShard)

	flushErr := errors.New("disk I/O error")
	mockShard.EXPECT().FlushMemtables(mock.Anything).Return(flushErr)

	snap, err := fsm.Snapshot()
	require.NoError(t, err)
	require.NotNil(t, snap)

	sink := &fakeSnapshotSink{}
	err = snap.Persist(sink)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "flush memtables before snapshot persist")
	assert.True(t, sink.cancelled)
}

func TestFSM_Snapshot_NilShard_Succeeds(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	// Do NOT call SetShard — shard remains nil.
	fsm := shard.NewFSM(testClassName, testShardName, testNodeID, logger)

	snap, err := fsm.Snapshot()
	require.NoError(t, err)
	require.NotNil(t, snap)

	// Persist should also succeed with nil shard (no flush attempted).
	sink := &fakeSnapshotSink{}
	err = snap.Persist(sink)
	require.NoError(t, err)
	assert.False(t, sink.cancelled)
}

func TestStore_RaftConfig_TrailingLogs(t *testing.T) {
	t.Run("custom TrailingLogs propagated", func(t *testing.T) {
		_, transport := raft.NewInmemTransport("")
		logger := logrus.New()
		logger.SetLevel(logrus.WarnLevel)

		cfg := shard.StoreConfig{
			ClassName:          testClassName,
			ShardName:          testShardName,
			NodeID:             testNodeID,
			DataPath:           t.TempDir(),
			Members:            []string{testNodeID},
			Logger:             logger,
			Transport:          transport,
			HeartbeatTimeout:   150 * time.Millisecond,
			ElectionTimeout:    150 * time.Millisecond,
			LeaderLeaseTimeout: 100 * time.Millisecond,
			SnapshotInterval:   10 * time.Second,
			SnapshotThreshold:  1024,
			TrailingLogs:       2048,
		}

		store, err := shard.NewStore(cfg)
		require.NoError(t, err)

		mockShard := mocks.NewMockshard(t)
		store.SetShard(mockShard)

		// Store should start successfully with the configured TrailingLogs
		startAndWaitForLeader(t, store)
		assert.True(t, store.IsLeader())
	})

	t.Run("default TrailingLogs when zero", func(t *testing.T) {
		_, transport := raft.NewInmemTransport("")
		logger := logrus.New()
		logger.SetLevel(logrus.WarnLevel)

		cfg := shard.StoreConfig{
			ClassName:          testClassName,
			ShardName:          testShardName,
			NodeID:             testNodeID,
			DataPath:           t.TempDir(),
			Members:            []string{testNodeID},
			Logger:             logger,
			Transport:          transport,
			HeartbeatTimeout:   150 * time.Millisecond,
			ElectionTimeout:    150 * time.Millisecond,
			LeaderLeaseTimeout: 100 * time.Millisecond,
			SnapshotInterval:   10 * time.Second,
			SnapshotThreshold:  1024,
			// TrailingLogs intentionally left at zero -> should default to 0
		}

		store, err := shard.NewStore(cfg)
		require.NoError(t, err)

		mockShard := mocks.NewMockshard(t)
		store.SetShard(mockShard)

		// Store should start successfully with the default TrailingLogs (0)
		startAndWaitForLeader(t, store)
		assert.True(t, store.IsLeader())
	})
}

// ---------------------------------------------------------------------------
// Builder Helpers — Phase 4 Operations
// ---------------------------------------------------------------------------

func buildDeleteObjectApplyRequest(t *testing.T, className, shardName string, id strfmt.UUID, deletionTime time.Time) *shardproto.ApplyRequest {
	t.Helper()

	subCmd, err := proto.Marshal(&shardproto.DeleteObjectRequest{
		Id:               string(id),
		DeletionTimeUnix: deletionTime.UnixNano(),
	})
	require.NoError(t, err)

	return &shardproto.ApplyRequest{
		Type:       shardproto.ApplyRequest_TYPE_DELETE_OBJECT,
		Class:      className,
		Shard:      shardName,
		SubCommand: subCmd,
	}
}

func buildMergeObjectApplyRequest(t *testing.T, className, shardName string, doc objects.MergeDocument) *shardproto.ApplyRequest {
	t.Helper()

	docJSON, err := json.Marshal(doc)
	require.NoError(t, err)

	subCmd, err := proto.Marshal(&shardproto.MergeObjectRequest{
		MergeDocumentJson: docJSON,
	})
	require.NoError(t, err)

	return &shardproto.ApplyRequest{
		Type:       shardproto.ApplyRequest_TYPE_MERGE_OBJECT,
		Class:      className,
		Shard:      shardName,
		SubCommand: subCmd,
	}
}

func buildPutObjectsBatchApplyRequest(t *testing.T, className, shardName string, objs []*storobj.Object) *shardproto.ApplyRequest {
	t.Helper()

	objBytes := make([][]byte, len(objs))
	for i, obj := range objs {
		b, err := obj.MarshalBinary()
		require.NoError(t, err)
		objBytes[i] = b
	}

	subCmd, err := proto.Marshal(&shardproto.PutObjectsBatchRequest{
		Objects: objBytes,
	})
	require.NoError(t, err)

	return &shardproto.ApplyRequest{
		Type:       shardproto.ApplyRequest_TYPE_PUT_OBJECTS_BATCH,
		Class:      className,
		Shard:      shardName,
		SubCommand: subCmd,
	}
}

func buildDeleteObjectsBatchApplyRequest(t *testing.T, className, shardName string, uuids []strfmt.UUID, deletionTime time.Time, dryRun bool) *shardproto.ApplyRequest {
	t.Helper()

	uuidStrs := make([]string, len(uuids))
	for i, id := range uuids {
		uuidStrs[i] = string(id)
	}

	subCmd, err := proto.Marshal(&shardproto.DeleteObjectsBatchRequest{
		Uuids:            uuidStrs,
		DeletionTimeUnix: deletionTime.UnixNano(),
		DryRun:           dryRun,
	})
	require.NoError(t, err)

	return &shardproto.ApplyRequest{
		Type:       shardproto.ApplyRequest_TYPE_DELETE_OBJECTS_BATCH,
		Class:      className,
		Shard:      shardName,
		SubCommand: subCmd,
	}
}

func buildAddReferencesApplyRequest(t *testing.T, className, shardName string, refs objects.BatchReferences) *shardproto.ApplyRequest {
	t.Helper()

	refsJSON, err := json.Marshal(refs)
	require.NoError(t, err)

	subCmd, err := proto.Marshal(&shardproto.AddReferencesRequest{
		ReferencesJson: refsJSON,
	})
	require.NoError(t, err)

	return &shardproto.ApplyRequest{
		Type:       shardproto.ApplyRequest_TYPE_ADD_REFERENCES,
		Class:      className,
		Shard:      shardName,
		SubCommand: subCmd,
	}
}

// ---------------------------------------------------------------------------
// FSM Handler Tests — Phase 4
// ---------------------------------------------------------------------------

func TestStore_Apply_DeleteObject(t *testing.T) {
	store, mockShard := newTestStore(t)
	startAndWaitForLeader(t, store)

	id := strfmt.UUID("12345678-1234-1234-1234-123456789abc")
	deletionTime := time.Now()

	mockShard.EXPECT().DeleteObject(mock.Anything, id, mock.MatchedBy(func(dt time.Time) bool {
		// UnixNano round-trip preserves nanosecond precision
		return dt.UnixNano() == deletionTime.UnixNano()
	})).Return(nil)

	req := buildDeleteObjectApplyRequest(t, testClassName, testShardName, id, deletionTime)
	version, err := store.Apply(context.Background(), req)
	require.NoError(t, err)
	assert.Greater(t, version, uint64(0))
}

func TestStore_Apply_DeleteObject_ShardError(t *testing.T) {
	store, mockShard := newTestStore(t)
	startAndWaitForLeader(t, store)

	id := strfmt.UUID("12345678-1234-1234-1234-123456789abc")
	shardErr := fmt.Errorf("object not found")
	mockShard.EXPECT().DeleteObject(mock.Anything, mock.Anything, mock.Anything).Return(shardErr)

	req := buildDeleteObjectApplyRequest(t, testClassName, testShardName, id, time.Now())
	_, err := store.Apply(context.Background(), req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "object not found")
}

func TestStore_Apply_MergeObject(t *testing.T) {
	store, mockShard := newTestStore(t)
	startAndWaitForLeader(t, store)

	doc := objects.MergeDocument{
		Class:           testClassName,
		ID:              strfmt.UUID("12345678-1234-1234-1234-123456789abc"),
		PrimitiveSchema: map[string]interface{}{"name": "updated"},
		UpdateTime:      time.Now().UnixMilli(),
	}

	mockShard.EXPECT().MergeObject(mock.Anything, mock.MatchedBy(func(m objects.MergeDocument) bool {
		return m.ID == doc.ID && m.Class == doc.Class && m.PrimitiveSchema["name"] == "updated"
	})).Return(nil)

	req := buildMergeObjectApplyRequest(t, testClassName, testShardName, doc)
	version, err := store.Apply(context.Background(), req)
	require.NoError(t, err)
	assert.Greater(t, version, uint64(0))
}

func TestStore_Apply_MergeObject_ShardError(t *testing.T) {
	store, mockShard := newTestStore(t)
	startAndWaitForLeader(t, store)

	doc := objects.MergeDocument{
		Class: testClassName,
		ID:    strfmt.UUID("12345678-1234-1234-1234-123456789abc"),
	}

	shardErr := fmt.Errorf("merge conflict")
	mockShard.EXPECT().MergeObject(mock.Anything, mock.Anything).Return(shardErr)

	req := buildMergeObjectApplyRequest(t, testClassName, testShardName, doc)
	_, err := store.Apply(context.Background(), req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "merge conflict")
}

func TestStore_Apply_PutObjectsBatch(t *testing.T) {
	store, mockShard := newTestStore(t)
	startAndWaitForLeader(t, store)

	obj1 := makeTestObject()
	obj2 := &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:                 strfmt.UUID("22222222-2222-2222-2222-222222222222"),
			Class:              testClassName,
			CreationTimeUnix:   2000000,
			LastUpdateTimeUnix: 2000000,
		},
		Vector:    []float32{0.4, 0.5, 0.6},
		VectorLen: 3,
	}

	mockShard.EXPECT().PutObjectBatch(mock.Anything, mock.MatchedBy(func(objs []*storobj.Object) bool {
		return len(objs) == 2 &&
			objs[0].Object.ID == obj1.Object.ID &&
			objs[1].Object.ID == obj2.Object.ID
	})).Return([]error{nil, nil})

	req := buildPutObjectsBatchApplyRequest(t, testClassName, testShardName, []*storobj.Object{obj1, obj2})
	version, err := store.Apply(context.Background(), req)
	require.NoError(t, err)
	assert.Greater(t, version, uint64(0))
}

func TestStore_Apply_PutObjectsBatch_PartialError(t *testing.T) {
	store, mockShard := newTestStore(t)
	startAndWaitForLeader(t, store)

	obj := makeTestObject()
	batchErr := fmt.Errorf("disk full on object 1")
	mockShard.EXPECT().PutObjectBatch(mock.Anything, mock.Anything).Return([]error{nil, batchErr})

	req := buildPutObjectsBatchApplyRequest(t, testClassName, testShardName, []*storobj.Object{obj, obj})
	_, err := store.Apply(context.Background(), req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "disk full on object 1")
}

func TestStore_Apply_DeleteObjectsBatch(t *testing.T) {
	store, mockShard := newTestStore(t)
	startAndWaitForLeader(t, store)

	uuids := []strfmt.UUID{
		"11111111-1111-1111-1111-111111111111",
		"22222222-2222-2222-2222-222222222222",
	}
	deletionTime := time.Now()

	mockShard.EXPECT().DeleteObjectBatch(mock.Anything, mock.MatchedBy(func(ids []strfmt.UUID) bool {
		return len(ids) == 2 && ids[0] == uuids[0] && ids[1] == uuids[1]
	}), mock.MatchedBy(func(dt time.Time) bool {
		return dt.UnixNano() == deletionTime.UnixNano()
	}), false).Return(objects.BatchSimpleObjects{
		{UUID: uuids[0]},
		{UUID: uuids[1]},
	})

	req := buildDeleteObjectsBatchApplyRequest(t, testClassName, testShardName, uuids, deletionTime, false)
	version, err := store.Apply(context.Background(), req)
	require.NoError(t, err)
	assert.Greater(t, version, uint64(0))
}

func TestStore_Apply_DeleteObjectsBatch_ShardError(t *testing.T) {
	store, mockShard := newTestStore(t)
	startAndWaitForLeader(t, store)

	uuids := []strfmt.UUID{"11111111-1111-1111-1111-111111111111"}
	shardErr := fmt.Errorf("permission denied")

	mockShard.EXPECT().DeleteObjectBatch(mock.Anything, mock.Anything, mock.Anything, false).Return(objects.BatchSimpleObjects{
		{UUID: uuids[0], Err: shardErr},
	})

	req := buildDeleteObjectsBatchApplyRequest(t, testClassName, testShardName, uuids, time.Now(), false)
	_, err := store.Apply(context.Background(), req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "permission denied")
}

func TestStore_Apply_AddReferences(t *testing.T) {
	store, mockShard := newTestStore(t)
	startAndWaitForLeader(t, store)

	refs := objects.BatchReferences{
		{OriginalIndex: 0, Tenant: "tenantA"},
		{OriginalIndex: 1, Tenant: "tenantB"},
	}

	mockShard.EXPECT().AddReferencesBatch(mock.Anything, mock.MatchedBy(func(r objects.BatchReferences) bool {
		return len(r) == 2 && r[0].Tenant == "tenantA" && r[1].Tenant == "tenantB"
	})).Return([]error{nil, nil})

	req := buildAddReferencesApplyRequest(t, testClassName, testShardName, refs)
	version, err := store.Apply(context.Background(), req)
	require.NoError(t, err)
	assert.Greater(t, version, uint64(0))
}

func TestStore_Apply_AddReferences_ShardError(t *testing.T) {
	store, mockShard := newTestStore(t)
	startAndWaitForLeader(t, store)

	refs := objects.BatchReferences{
		{OriginalIndex: 0, Tenant: "tenantA"},
	}

	refErr := fmt.Errorf("reference target not found")
	mockShard.EXPECT().AddReferencesBatch(mock.Anything, mock.Anything).Return([]error{refErr})

	req := buildAddReferencesApplyRequest(t, testClassName, testShardName, refs)
	_, err := store.Apply(context.Background(), req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reference target not found")
}

// ---------------------------------------------------------------------------
// Chunking Tests
// ---------------------------------------------------------------------------

func TestChunkObjectBytes_SingleChunk(t *testing.T) {
	// All objects fit in one chunk
	objectBytes := [][]byte{
		make([]byte, 100),
		make([]byte, 100),
		make([]byte, 100),
	}
	chunks := shard.ChunkObjectBytes(objectBytes, 1000)
	assert.Len(t, chunks, 1)
	assert.Len(t, chunks[0], 3)
}

func TestChunkObjectBytes_MultipleChunks(t *testing.T) {
	// Objects split across chunks (max 250 bytes per chunk)
	objectBytes := [][]byte{
		make([]byte, 100),
		make([]byte, 100),
		make([]byte, 100),
		make([]byte, 100),
	}
	chunks := shard.ChunkObjectBytes(objectBytes, 250)
	assert.Len(t, chunks, 2)
	assert.Len(t, chunks[0], 2) // 100+100 = 200 <= 250
	assert.Len(t, chunks[1], 2) // 100+100 = 200 <= 250
}

func TestChunkObjectBytes_LargeObject(t *testing.T) {
	// Single object exceeds chunk size → own chunk
	objectBytes := [][]byte{
		make([]byte, 50),
		make([]byte, 500), // exceeds maxBytes
		make([]byte, 50),
	}
	chunks := shard.ChunkObjectBytes(objectBytes, 100)
	assert.Len(t, chunks, 3)
	assert.Len(t, chunks[0], 1)
	assert.Len(t, chunks[1], 1)
	assert.Len(t, chunks[2], 1)
}

func TestChunkObjectBytes_Empty(t *testing.T) {
	chunks := shard.ChunkObjectBytes(nil, 1000)
	assert.Nil(t, chunks)

	chunks = shard.ChunkObjectBytes([][]byte{}, 1000)
	assert.Nil(t, chunks)
}
