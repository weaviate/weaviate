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

package replication

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/schema"
)

func newDrainTestManager(t *testing.T, opID uint64, coll, shard string) *Manager {
	t.Helper()
	// schemaReader/nodeSelector are unused on the broadcastNodeReachedState path.
	m := NewManager(schema.SchemaReader{}, nil, prometheus.NewPedanticRegistry())
	m.SetLogger(logrus.New())
	require.NoError(t, m.GetReplicationFSM().Replicate(opID, &cmd.ReplicationReplicateShardRequest{
		Version:          cmd.ReplicationCommandVersionV0,
		Uuid:             strfmt.UUID("00000000-0000-0000-0000-000000000001"),
		SourceNode:       "node1",
		SourceCollection: coll,
		SourceShard:      shard,
		TargetNode:       "node2",
		TransferType:     cmd.COPY.String(),
	}))
	return m
}

// The fix's wiring: a node must not report it reached INTEGRATING until its
// in-flight coordinated writes to the moving shard have drained.
func TestManager_HoldsIntegratingReportUntilDrained(t *testing.T) {
	const opID = uint64(1)
	m := newDrainTestManager(t, opID, "TestClass", "shard1")

	submitted := make(chan cmd.ShardReplicationState, 4)
	m.SetNodeReachedStateSubmitter("node1", func(ctx context.Context, req *cmd.ReplicationNodeReachedStateRequest) error {
		submitted <- req.State
		return nil
	})

	gate := make(chan struct{})
	drainStarted := make(chan struct{}, 1)
	var gotClass, gotShard string
	m.SetInflightDrainer(func(ctx context.Context, class, shard string) error {
		gotClass, gotShard = class, shard
		select {
		case drainStarted <- struct{}{}:
		default:
		}
		select {
		case <-gate:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	m.broadcastNodeReachedState(opID, cmd.INTEGRATING)

	// The drainer is invoked with the op's source class/shard.
	select {
	case <-drainStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("drainer was not invoked for INTEGRATING")
	}
	require.Equal(t, "TestClass", gotClass)
	require.Equal(t, "shard1", gotShard)

	// While the drain is gated, the report must NOT go out.
	select {
	case s := <-submitted:
		t.Fatalf("INTEGRATING reported before the drain completed: %v", s)
	case <-time.After(100 * time.Millisecond):
	}

	close(gate)

	select {
	case s := <-submitted:
		require.Equal(t, cmd.INTEGRATING, s)
	case <-time.After(2 * time.Second):
		t.Fatal("INTEGRATING was not reported after the drain completed")
	}
}

// Only INTEGRATING is gated on the drain; every other state reports promptly
// and never invokes the drainer.
func TestManager_DoesNotDrainForNonIntegratingStates(t *testing.T) {
	const opID = uint64(2)
	m := newDrainTestManager(t, opID, "TestClass", "shard1")

	submitted := make(chan cmd.ShardReplicationState, 4)
	m.SetNodeReachedStateSubmitter("node1", func(ctx context.Context, req *cmd.ReplicationNodeReachedStateRequest) error {
		submitted <- req.State
		return nil
	})

	drained := make(chan struct{}, 1)
	m.SetInflightDrainer(func(ctx context.Context, class, shard string) error {
		select {
		case drained <- struct{}{}:
		default:
		}
		return nil
	})

	for _, st := range []cmd.ShardReplicationState{cmd.HYDRATING, cmd.FINALIZING, cmd.READY} {
		m.broadcastNodeReachedState(opID, st)
		select {
		case s := <-submitted:
			require.Equal(t, st, s)
		case <-time.After(2 * time.Second):
			t.Fatalf("state %v was not reported", st)
		}
	}

	select {
	case <-drained:
		t.Fatal("the drainer must not be called for non-INTEGRATING states")
	default:
	}
}

// A drain that fails or times out must not be papered over by reporting
// INTEGRATING: with writes still in flight, the consumer would seal the source
// change-capture log over them and lose them on the target. The drain is retried
// and the report withheld until it succeeds.
func TestManager_RetriesDrainBeforeReportingIntegrating(t *testing.T) {
	const opID = uint64(3)
	m := newDrainTestManager(t, opID, "TestClass", "shard1")

	// Mirror the real path: a successful submit applies NodeReachedState, so we
	// can assert the op actually reaches the state, not just that it was reported.
	submitted := make(chan cmd.ShardReplicationState, 4)
	m.SetNodeReachedStateSubmitter("node1", func(ctx context.Context, req *cmd.ReplicationNodeReachedStateRequest) error {
		if err := m.GetReplicationFSM().NodeReachedState(req); err != nil {
			return err
		}
		submitted <- req.State
		return nil
	})

	// Fail the drain twice (backstop firing under load), then succeed.
	var attempts atomic.Int32
	m.SetInflightDrainer(func(ctx context.Context, class, shard string) error {
		if attempts.Add(1) < 3 {
			return errors.New("in-flight drain not complete yet")
		}
		return nil
	})

	m.broadcastNodeReachedState(opID, cmd.INTEGRATING)

	select {
	case s := <-submitted:
		require.Equal(t, cmd.INTEGRATING, s)
	case <-time.After(10 * time.Second):
		t.Fatal("INTEGRATING was never reported even though the drain eventually succeeded")
	}
	require.GreaterOrEqual(t, attempts.Load(), int32(3),
		"drain must be retried until it succeeds, not proceeded after the first failure")
	require.True(t, m.GetReplicationFSM().AllPeersAtLeast(opID, cmd.INTEGRATING, []string{"node1"}),
		"node1 should have reached INTEGRATING in the FSM after the drain succeeded")
}
