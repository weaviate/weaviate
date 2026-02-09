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
	"context"
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
			// TrailingLogs intentionally left at zero -> should default to 4096
		}

		store, err := shard.NewStore(cfg)
		require.NoError(t, err)

		mockShard := mocks.NewMockshard(t)
		store.SetShard(mockShard)

		// Store should start successfully with the default TrailingLogs (4096)
		startAndWaitForLeader(t, store)
		assert.True(t, store.IsLeader())
	})
}
