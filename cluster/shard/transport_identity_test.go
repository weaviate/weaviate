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
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

// TestMuxTransport_IdentityUnderStripeChurn pins group-identity integrity
// under concurrent load and stripe lifecycle churn — the property whose
// violation was a suspect for the minor-issues.md #9 collapse (a message
// stepped by the wrong group's Store): 32 groups hammer one peer connection
// across all three lanes (per-group bulk stripes, the shared priority lane,
// and the per-peer coalesced-heartbeat buffers that concatenate frames of
// MANY groups) while each group's bulk stripes are repeatedly retired
// (removeGroup) and legitimately recreated by continued Sends mid-stream.
// Every message stamps its true (groupID, seq) into Context; the receiver
// requires the frame's routing groupID to match the stamp on every delivery.
// Message LOSS is expected and permitted (stripe retirement discards queued
// frames; raft re-sends) — identity corruption never is.
func TestMuxTransport_IdentityUnderStripeChurn(t *testing.T) {
	logger, _ := test.NewNullLogger()
	nodes := setupMuxNodesFlush(t, 2, logger, 5*time.Millisecond)
	sender, receiver := nodes[0], nodes[1]
	to := sender.nodeIDs.register(receiver.id)
	from := sender.nodeIDs.register(sender.id)

	const (
		groups   = 32
		perGroup = 3000
	)
	tag := func(gid uint64, seq int) []byte {
		b := make([]byte, 16)
		binary.BigEndian.PutUint64(b, gid)
		binary.BigEndian.PutUint64(b[8:], uint64(seq))
		return b
	}

	var wg sync.WaitGroup
	for g := 0; g < groups; g++ {
		gid := uint64(100 + g)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seq := 0; seq < perGroup; seq++ {
				var typ raftpb.MessageType
				switch seq % 10 {
				case 0:
					typ = raftpb.MsgHeartbeat // coalescer path
				case 1:
					typ = raftpb.MsgVote // shared priority lane
				default:
					typ = raftpb.MsgApp // per-group bulk stripe
				}
				sender.mux.Send(gid, []raftpb.Message{
					{Type: typ, To: to, From: from, Term: 7, Context: tag(gid, seq)},
				})
				if seq%97 == 96 {
					sender.mux.removeGroup(gid)
				}
			}
		}()
	}
	wg.Wait()

	// Let in-flight deliveries settle: stop once the routed count is stable
	// across an observation window.
	prev := -1
	for i := 0; i < 100; i++ {
		cur := receiver.router.count()
		if cur == prev {
			break
		}
		prev = cur
		time.Sleep(100 * time.Millisecond)
	}

	all := receiver.router.all()
	require.NotEmpty(t, all)
	for _, m := range all {
		require.Len(t, m.msg.Context, 16, "message arrived with foreign/truncated context")
		stamped := binary.BigEndian.Uint64(m.msg.Context)
		require.Equal(t, stamped, m.groupID,
			"cross-group delivery: frame routed as group %d carries payload stamped for group %d (type %s)",
			m.groupID, stamped, m.msg.Type)
	}
	// The path was genuinely exercised across all lanes and groups.
	perType := map[raftpb.MessageType]int{}
	perGid := map[uint64]int{}
	for _, m := range all {
		perType[m.msg.Type]++
		perGid[m.groupID]++
	}
	require.Greater(t, perType[raftpb.MsgApp], 1000)
	require.Greater(t, perType[raftpb.MsgHeartbeat], 100)
	require.Greater(t, perType[raftpb.MsgVote], 100)
	require.Equal(t, groups, len(perGid), "every group should have delivered something")
}
