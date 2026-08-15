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
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/shard/sharedlog"
	"github.com/weaviate/weaviate/entities/storobj"
	"go.etcd.io/raft/v3/raftpb"
)

// newParkStore builds the minimal in-package Store wiring the park machinery
// needs: an fsm over the given stub, live loop context, buffered ack channel,
// a serviced worker-request channel, and a real (empty) sharedlog so a
// snapshot install's bookkeeping round-trip and compaction run for real.
// class/shardName individuate the per-shard metrics under test.
func newParkStore(t *testing.T, class, shardName string, stub *stubShard) *Store {
	t.Helper()
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)

	sl, err := sharedlog.Open(sharedlog.Options{
		Path:   filepath.Join(t.TempDir(), sharedRaftLogName),
		Logger: logger,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sl.Close() })

	s := newBareStore(true, 1)
	s.config.ClassName, s.config.ShardName = class, shardName
	s.reqIDSalt = testSaltA
	s.log = logger.WithField("t", "test")
	s.fsm = NewFSM(class, shardName, "n1", logger)
	s.fsm.SetShard(stub)
	s.localID = 1
	s.localMsgCh = make(chan raftpb.Message, 64)
	s.applyCh = make(chan applyItem, 16)
	s.workerReqCh = make(chan workerReq, 8)
	s.sharedLog = sl
	s.loopCtx, s.loopCancel = context.WithCancel(context.Background())
	t.Cleanup(s.loopCancel)

	// Serve worker→loop round-trips (snapshot bookkeeping) like the Ready
	// loop would.
	go func() {
		for {
			select {
			case req := <-s.workerReqCh:
				close(req.done)
			case <-s.loopCtx.Done():
				return
			}
		}
	}()
	return s
}

// applyAck builds an item's MsgStorageApplyResp targeted at the local store.
func applyAck(index uint64) []raftpb.Message {
	return []raftpb.Message{{Type: raftpb.MsgStorageApplyResp, To: 1, Index: index}}
}

// drainAcks returns the indexes of all acks currently buffered, in order.
func drainAcks(ch chan raftpb.Message) []uint64 {
	var out []uint64
	for {
		select {
		case m := <-ch:
			out = append(out, m.Index)
		default:
			return out
		}
	}
}

// counterDelta samples a labeled counter and returns a delta reader.
func counterDelta(read func() float64) func() float64 {
	before := read()
	return func() float64 { return read() - before }
}

// TestApplyItems_ParksAtFailingEntry_PrefixLands pins the prefix-progress
// contract (taxonomy Decision C): in a merged window whose middle entry fails
// environmentally, the entries before it land — the applied index advances
// exactly to the last complete entry and its covering acks deliver — while
// the failing entry and everything after wait in the returned remainder; the
// retry re-runs the parked entry IN FULL and completes the run.
func TestApplyItems_ParksAtFailingEntry_PrefixLands(t *testing.T) {
	idA := strfmt.UUID("00000000-0000-4000-8000-00000000000a")
	idB := strfmt.UUID("00000000-0000-4000-8000-00000000000b")
	idC := strfmt.UUID("00000000-0000-4000-8000-00000000000c")

	failing := true
	seen := map[strfmt.UUID]int{}
	stub := &stubShard{putBatch: func(objs []*storobj.Object) []error {
		errs := make([]error, len(objs))
		for i, o := range objs {
			if failing && o.Object.ID == idB {
				errs[i] = fmt.Errorf("store is read-only due to: resource pressure")
				continue
			}
			seen[o.Object.ID]++
		}
		return errs
	}}
	s := newParkStore(t, "PrefixC", "PrefixS", stub)

	items := []applyItem{
		{entries: []raftpb.Entry{putBatchEntry(t, 5, idA)}, resps: applyAck(5)},
		{entries: []raftpb.Entry{putBatchEntry(t, 6, idB)}, resps: applyAck(6)},
		{entries: []raftpb.Entry{putBatchEntry(t, 7, idC)}, resps: applyAck(7)},
	}

	parked, ok := s.applyItems(items)
	require.True(t, ok)
	require.NotNil(t, parked, "an environmental item failure must park, not swallow")
	require.Equal(t, uint64(6), parked.index, "park point is the first failing entry")
	require.Error(t, parked.err)

	require.Equal(t, uint64(5), s.fsm.LastAppliedIndex(),
		"applied must advance exactly to the last complete entry before the park")
	require.Equal(t, []uint64{5}, drainAcks(s.localMsgCh),
		"only the landed prefix may ack; the parked entry and its successors are withheld")
	require.Len(t, parked.remaining, 2, "the parked entry and everything after remain")
	require.Equal(t, uint64(6), parked.remaining[0].entries[0].Index)
	require.Equal(t, uint64(7), parked.remaining[1].entries[0].Index)

	// The environmental condition clears: the retry re-runs the parked entry
	// in full and the rest of the backlog lands behind it.
	failing = false
	parked2, ok2 := s.applyItems(parked.remaining)
	require.True(t, ok2)
	require.Nil(t, parked2)
	require.Equal(t, uint64(7), s.fsm.LastAppliedIndex())
	require.Equal(t, []uint64{6, 7}, drainAcks(s.localMsgCh), "held acks deliver in item order on resume")
	for _, id := range []strfmt.UUID{idA, idB, idC} {
		require.GreaterOrEqualf(t, seen[id], 1, "object %s must have materialized", id)
	}
}

// TestApplyItemsParking_SnapshotInstallSupersedesParkedBacklog pins the
// follower-catch-up cure: while the worker is parked, a queued snapshot
// install at or above the parked entries supersedes them — the backlog is
// dropped in favour of the restored state, the superseded items' held acks
// deliver AFTER the restore succeeds (they release raft's apply quota), and
// the park ends without the failing entries ever materializing locally.
func TestApplyItemsParking_SnapshotInstallSupersedesParkedBacklog(t *testing.T) {
	stub := &stubShard{putBatch: func(objs []*storobj.Object) []error {
		// The live lossy shape: whole-batch refusal as a 1-element slice.
		return []error{fmt.Errorf("store is read-only due to: resource pressure")}
	}}
	s := newParkStore(t, "SupersedeC", "SupersedeS", stub)

	retries := counterDelta(func() float64 {
		return testutil.ToFloat64(shardRaftApplyParkRetries.WithLabelValues("SupersedeC", "SupersedeS"))
	})

	items := []applyItem{
		{entries: []raftpb.Entry{putBatchEntry(t, 5, strfmt.UUID("00000000-0000-4000-8000-00000000005a"))}, resps: applyAck(5)},
		{entries: []raftpb.Entry{putBatchEntry(t, 6, strfmt.UUID("00000000-0000-4000-8000-00000000006a"))}, resps: applyAck(6)},
	}

	res := make(chan bool, 1)
	go func() { res <- s.applyItemsParking(items, nil) }()

	require.Eventually(t, func() bool { return retries() >= 1 },
		5*time.Second, 10*time.Millisecond, "the run must park on the refusal")

	// A received snapshot covering the parked entries arrives on the apply
	// channel (sequenced there by the append worker in production).
	data, err := json.Marshal(shardSnapshotData{
		ClassName: "SupersedeC", ShardName: "SupersedeS", NodeID: "n1", LastAppliedIndex: 6,
	})
	require.NoError(t, err)
	restored := make(chan struct{})
	snap := &raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 6, Term: 1}, Data: data}
	s.applyCh <- applyItem{snap: snap, resps: applyAck(6), restored: restored}

	select {
	case <-restored:
	case <-time.After(10 * time.Second):
		t.Fatal("snapshot install never completed while parked — the catch-up cure is wedged")
	}
	select {
	case ok := <-res:
		require.True(t, ok, "the parked run must end via the superseding install")
	case <-time.After(10 * time.Second):
		t.Fatal("applyItemsParking did not return after the superseding install")
	}

	require.Equal(t, uint64(6), s.fsm.LastAppliedIndex(), "applied jumps to the snapshot index")
	// Acks: the snapshot's own ack (delivered by the install), then the two
	// superseded items' held acks in item order.
	require.Equal(t, []uint64{6, 5, 6}, drainAcks(s.localMsgCh),
		"superseded items' acks must deliver after the restore — they release raft's apply quota")
	require.Equal(t, float64(0),
		testutil.ToFloat64(shardRaftApplyParkedAge.WithLabelValues("SupersedeC", "SupersedeS")),
		"park gauge must clear once the install supersedes the backlog")
}

// TestApplyItemsParking_IndefiniteTelemetry_DropClears pins the indefinite
// park's observability: under a persistent refusal the retry counter grows
// without a give-up ceiling and the age gauge is live; group teardown (the
// class-drop path cancels the loop context) clears the gauge and abandons the
// backlog deterministically.
func TestApplyItemsParking_IndefiniteTelemetry_DropClears(t *testing.T) {
	stub := &stubShard{putBatch: func(objs []*storobj.Object) []error {
		return []error{fmt.Errorf("store is read-only due to: resource pressure")}
	}}
	s := newParkStore(t, "TelemetryC", "TelemetryS", stub)

	gauge := func() float64 {
		return testutil.ToFloat64(shardRaftApplyParkedAge.WithLabelValues("TelemetryC", "TelemetryS"))
	}
	retries := counterDelta(func() float64 {
		return testutil.ToFloat64(shardRaftApplyParkRetries.WithLabelValues("TelemetryC", "TelemetryS"))
	})

	items := []applyItem{{entries: []raftpb.Entry{putBatchEntry(t, 5,
		strfmt.UUID("00000000-0000-4000-8000-0000000000e5"))}, resps: applyAck(5)}}

	res := make(chan bool, 1)
	go func() { res <- s.applyItemsParking(items, nil) }()

	require.Eventually(t, func() bool { return retries() >= 3 && gauge() > 0 },
		15*time.Second, 20*time.Millisecond,
		"park telemetry must keep growing — parking has no give-up ceiling")
	require.Equal(t, uint64(0), s.fsm.LastAppliedIndex(),
		"applied must never advance over the parked entry")
	require.Empty(t, drainAcks(s.localMsgCh), "no ack may leave for a parked entry")

	// Group teardown (drop/stop) abandons the backlog and clears the gauge.
	s.loopCancel()
	select {
	case ok := <-res:
		require.False(t, ok, "teardown must abandon the parked run")
	case <-time.After(10 * time.Second):
		t.Fatal("parked worker did not exit on loop-context cancellation")
	}
	require.Eventually(t, func() bool { return gauge() == 0 },
		5*time.Second, 10*time.Millisecond, "drop must clear the park gauge")
}

// TestApplyItemsParking_ResumeClearsTelemetry pins the pressure-clear
// resume: once the environmental condition ends, the parked entry
// materializes, applied advances, its ack delivers, and the gauge clears.
func TestApplyItemsParking_ResumeClearsTelemetry(t *testing.T) {
	healed := make(chan struct{})
	stub := &stubShard{putBatch: func(objs []*storobj.Object) []error {
		select {
		case <-healed:
			return make([]error, len(objs))
		default:
			return []error{fmt.Errorf("store is read-only due to: resource pressure")}
		}
	}}
	s := newParkStore(t, "ResumeC", "ResumeS", stub)

	gauge := func() float64 {
		return testutil.ToFloat64(shardRaftApplyParkedAge.WithLabelValues("ResumeC", "ResumeS"))
	}
	retries := counterDelta(func() float64 {
		return testutil.ToFloat64(shardRaftApplyParkRetries.WithLabelValues("ResumeC", "ResumeS"))
	})

	items := []applyItem{{entries: []raftpb.Entry{putBatchEntry(t, 5,
		strfmt.UUID("00000000-0000-4000-8000-0000000000f5"))}, resps: applyAck(5)}}

	res := make(chan bool, 1)
	go func() { res <- s.applyItemsParking(items, nil) }()

	require.Eventually(t, func() bool { return retries() >= 2 && gauge() > 0 },
		15*time.Second, 20*time.Millisecond, "the run must be parked before healing")

	close(healed)
	select {
	case ok := <-res:
		require.True(t, ok, "the parked entry must materialize once the condition clears")
	case <-time.After(15 * time.Second):
		t.Fatal("parked run did not resume after the environmental condition cleared")
	}
	require.Equal(t, uint64(5), s.fsm.LastAppliedIndex())
	require.Equal(t, []uint64{5}, drainAcks(s.localMsgCh))
	require.Equal(t, float64(0), gauge(), "resume must clear the park gauge")
}
