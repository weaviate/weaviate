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

package shard

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	shardproto "github.com/weaviate/weaviate/cluster/shard/proto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// stubShard is a minimal internal shard implementation (the mockery mock
// lives in cluster/shard/mocks, which imports this package — unusable here).
// Only putBatch is routed; everything else is unreachable in these tests.
type stubShard struct {
	putBatch func(objs []*storobj.Object) []error
}

var _ shard = (*stubShard)(nil)

func (s *stubShard) PutObject(context.Context, *storobj.Object) error { panic("unexpected") }
func (s *stubShard) DeleteObject(context.Context, strfmt.UUID, time.Time) error {
	panic("unexpected")
}
func (s *stubShard) MergeObject(context.Context, objects.MergeDocument) error { panic("unexpected") }
func (s *stubShard) PutObjectBatch(_ context.Context, objs []*storobj.Object) []error {
	return s.putBatch(objs)
}

func (s *stubShard) DeleteObjectBatch(context.Context, []strfmt.UUID, time.Time, bool) objects.BatchSimpleObjects {
	panic("unexpected")
}

func (s *stubShard) AddReferencesBatch(context.Context, objects.BatchReferences) []error {
	panic("unexpected")
}
func (s *stubShard) FlushForSnapshot(context.Context) error { panic("unexpected") }
func (s *stubShard) DurableRaftFloor() uint64               { return math.MaxUint64 }
func (s *stubShard) ReadOnlyErr() error                     { return nil }
func (s *stubShard) WaitForSchemaVersion(context.Context, uint64) error {
	return nil
}
func (s *stubShard) ClassPresent() bool { return true }
func (s *stubShard) CreateTransferSnapshot(context.Context) (TransferSnapshot, error) {
	panic("unexpected")
}
func (s *stubShard) ReleaseTransferSnapshot(string) error { return nil }
func (s *stubShard) Name() string                         { return "stub" }

// putBatchEntry builds one committed PUT_OBJECTS_BATCH entry carrying a
// single object with the given UUID.
func putBatchEntry(t *testing.T, index uint64, id strfmt.UUID) raftpb.Entry {
	t.Helper()
	obj := &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:                 id,
			Class:              "C",
			CreationTimeUnix:   1000000,
			LastUpdateTimeUnix: 1000000,
		},
		Vector:    []float32{0.1},
		VectorLen: 1,
	}
	raw, err := obj.MarshalBinary()
	require.NoError(t, err)
	sub, err := proto.Marshal(&shardproto.PutObjectsBatchRequest{Objects: [][]byte{raw}})
	require.NoError(t, err)
	body, err := proto.Marshal(&shardproto.ApplyRequest{
		Type:       shardproto.ApplyRequest_TYPE_PUT_OBJECTS_BATCH,
		Class:      "C",
		Shard:      "S",
		SubCommand: sub,
	})
	require.NoError(t, err)
	return raftpb.Entry{Type: raftpb.EntryNormal, Index: index, Data: encodeCmd(testSaltA|index, body)}
}

// TestApplyItems_ShutdownMidRun_WithholdsUnmaterializedAcks pins the drained
// backlog's Stop/abandon contract: when shutdown lands in the middle of a
// multi-item run, units that materialized before the cut have their items'
// MsgStorageApplyResp delivered, the unit interrupted by shutdown has its
// responses WITHHELD (crash-equivalent: the entries re-deliver on restart and
// re-apply idempotently), and applyItems reports the abort so the worker
// exits. The applied watermark never runs ahead of materialization.
func TestApplyItems_ShutdownMidRun_WithholdsUnmaterializedAcks(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)

	s := newBareStore(true, 1)
	s.config.ClassName, s.config.ShardName = "AbortC", "AbortS"
	s.reqIDSalt = testSaltA
	s.log = logger.WithField("t", "test")
	s.fsm = NewFSM("AbortC", "AbortS", "n1", logger)
	s.localID = 1
	// Capacity 1: item1's ack fills it, so item2's withheld ack cannot race a
	// buffered send against the cancelled context.
	s.localMsgCh = make(chan raftpb.Message, 1)
	s.loopCtx, s.loopCancel = context.WithCancel(context.Background())

	calls := 0
	s.fsm.SetShard(&stubShard{putBatch: func(objs []*storobj.Object) []error {
		calls++
		if calls == 2 {
			// Shutdown lands while the second window is inside the shard.
			s.loopCancel()
		}
		return make([]error, len(objs))
	}})

	// The same UUID across both entries forces two windows (the
	// replica-divergence split rule), giving the run a completed unit and an
	// interrupted one.
	id := strfmt.UUID("00000000-0000-4000-8000-0000000000ab")
	item1 := applyItem{
		entries: []raftpb.Entry{putBatchEntry(t, 5, id)},
		resps:   []raftpb.Message{{Type: raftpb.MsgStorageApplyResp, To: 1, Index: 5}},
	}
	item2 := applyItem{
		entries: []raftpb.Entry{putBatchEntry(t, 6, id)},
		resps:   []raftpb.Message{{Type: raftpb.MsgStorageApplyResp, To: 1, Index: 6}},
	}

	_, ok := s.applyItems([]applyItem{item1, item2})
	require.False(t, ok, "shutdown mid-run must abort the drained run")
	require.Equal(t, 2, calls, "both windows must have reached the shard")

	// Item 1 materialized before the cut: its ack was delivered. Item 2's ack
	// is withheld — crash-equivalent abandon.
	require.Len(t, s.localMsgCh, 1, "exactly the pre-shutdown unit's ack may be delivered")
	ack := <-s.localMsgCh
	require.Equal(t, uint64(5), ack.Index)

	// Both windows really materialized, so the watermark honestly covers
	// them; it advanced per unit, never ahead of the shard write.
	require.Equal(t, uint64(6), s.fsm.LastAppliedIndex())
}

// TestApplyItems_ResponsesFollowUnitCompletionInItemOrder pins ack
// progression across a drained run: each item's MsgStorageApplyResp is
// delivered as soon as the unit covering its last entry materializes — items
// merged into one window ack together at its completion, in item order, and
// an item whose entries span into a later unit acks only at that later
// unit's completion.
func TestApplyItems_ResponsesFollowUnitCompletionInItemOrder(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)

	s := newBareStore(true, 1)
	s.config.ClassName, s.config.ShardName = "OrderC", "OrderS"
	s.reqIDSalt = testSaltA
	s.log = logger.WithField("t", "test")
	s.fsm = NewFSM("OrderC", "OrderS", "n1", logger)
	s.localID = 1
	s.localMsgCh = make(chan raftpb.Message, 8)
	s.loopCtx, s.loopCancel = context.WithCancel(context.Background())
	defer s.loopCancel()

	// acksAtCall[i] = acks visible when shard round i began.
	var acksAtCall []int
	s.fsm.SetShard(&stubShard{putBatch: func(objs []*storobj.Object) []error {
		acksAtCall = append(acksAtCall, len(s.localMsgCh))
		return make([]error, len(objs))
	}})

	dup := strfmt.UUID("00000000-0000-4000-8000-0000000000cd")
	items := []applyItem{
		// Items 1+2 merge into window 1 (distinct UUIDs).
		{
			entries: []raftpb.Entry{putBatchEntry(t, 5, strfmt.UUID("00000000-0000-4000-8000-0000000000a1"))},
			resps:   []raftpb.Message{{Type: raftpb.MsgStorageApplyResp, To: 1, Index: 5}},
		},
		{
			entries: []raftpb.Entry{putBatchEntry(t, 6, dup)},
			resps:   []raftpb.Message{{Type: raftpb.MsgStorageApplyResp, To: 1, Index: 6}},
		},
		// Item 3 repeats a UUID: window 2.
		{
			entries: []raftpb.Entry{putBatchEntry(t, 7, dup)},
			resps:   []raftpb.Message{{Type: raftpb.MsgStorageApplyResp, To: 1, Index: 7}},
		},
	}
	parked, ok := s.applyItems(items)
	require.True(t, ok)
	require.Nil(t, parked, "a healthy run must not park")

	require.Equal(t, []int{0, 2}, acksAtCall,
		"window 1 must begin with no acks out; window 2 must begin with exactly items 1+2 acked")
	require.Len(t, s.localMsgCh, 3)
	for want := uint64(5); want <= 7; want++ {
		ack := <-s.localMsgCh
		require.Equal(t, want, ack.Index, "acks must be delivered in item order")
	}
	require.Equal(t, uint64(7), s.fsm.LastAppliedIndex())
}
