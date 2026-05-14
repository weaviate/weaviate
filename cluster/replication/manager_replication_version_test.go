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

package replication_test

import (
	"encoding/json"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/fakes"
)

// TestManager_ReplicationVersionBump pins the contract that every
// op-state-mutating apply on the Manager bumps the per-class
// ReplicationVersion to the apply's RAFT index via the wired bumper.
// This is the entry-path defense: a leader-routed schema query (e.g.
// EnsureTenantActiveForWrite) returns version >= bumped RV, and the
// per-write WaitForUpdate fence on the coord side forces every
// coord's local FSM to catch up before routing is built.
func TestManager_ReplicationVersionBump(t *testing.T) {
	type bumpCall struct {
		class string
		index uint64
	}

	seed := func(t *testing.T) (*replication.Manager, *[]bumpCall) {
		t.Helper()
		parser := fakes.NewMockParser()
		parser.On("ParseClass", mock.Anything).Return(nil)
		schemaManager := schema.NewSchemaManager("test-node", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())
		schemaReader := schemaManager.NewSchemaReader()
		mgr := replication.NewManager(schemaReader, mocks.NewMockNodeSelector("localhost"), prometheus.NewPedanticRegistry())

		var calls []bumpCall
		mgr.SetReplicationVersionBumper(func(class string, idx uint64) {
			calls = append(calls, bumpCall{class: class, index: idx})
		})

		// Seed an op directly into the FSM (bypassing Replicate's schema
		// validation which would require a real class).
		require.NoError(t, mgr.GetReplicationFSM().Replicate(1, &api.ReplicationReplicateShardRequest{
			Version:          api.ReplicationCommandVersionV0,
			Uuid:             strfmt.UUID("00000000-0000-0000-0000-000000000001"),
			SourceNode:       "node1",
			SourceCollection: "TestClass",
			SourceShard:      "shard1",
			TargetNode:       "node2",
			TransferType:     api.COPY.String(),
		}))
		return mgr, &calls
	}

	t.Run("UpdateReplicateOpState bumps with op's class + apply index", func(t *testing.T) {
		mgr, calls := seed(t)

		body, err := json.Marshal(&api.ReplicationUpdateOpStateRequest{
			Version: api.ReplicationCommandVersionV0,
			Id:      1,
			State:   api.HYDRATING,
		})
		require.NoError(t, err)
		require.NoError(t, mgr.UpdateReplicateOpState(&api.ApplyRequest{Version: 42, SubCommand: body}))

		require.Len(t, *calls, 1)
		require.Equal(t, "TestClass", (*calls)[0].class)
		require.EqualValues(t, 42, (*calls)[0].index)
	})

	t.Run("CancelReplication bumps with op's class + apply index", func(t *testing.T) {
		mgr, calls := seed(t)

		body, err := json.Marshal(&api.ReplicationCancelRequest{
			Version: api.ReplicationCommandVersionV0,
			Uuid:    strfmt.UUID("00000000-0000-0000-0000-000000000001"),
		})
		require.NoError(t, err)
		require.NoError(t, mgr.CancelReplication(&api.ApplyRequest{Version: 99, SubCommand: body}))

		require.Len(t, *calls, 1)
		require.Equal(t, "TestClass", (*calls)[0].class)
		require.EqualValues(t, 99, (*calls)[0].index)
	})

	t.Run("DeleteReplication bumps with op's class + apply index", func(t *testing.T) {
		mgr, calls := seed(t)

		body, err := json.Marshal(&api.ReplicationDeleteRequest{
			Version: api.ReplicationCommandVersionV0,
			Uuid:    strfmt.UUID("00000000-0000-0000-0000-000000000001"),
		})
		require.NoError(t, err)
		require.NoError(t, mgr.DeleteReplication(&api.ApplyRequest{Version: 7, SubCommand: body}))

		require.Len(t, *calls, 1)
		require.Equal(t, "TestClass", (*calls)[0].class)
		require.EqualValues(t, 7, (*calls)[0].index)
	})

	t.Run("apply on unknown op surfaces the error and skips the bump", func(t *testing.T) {
		mgr, calls := seed(t)

		body, err := json.Marshal(&api.ReplicationCancelRequest{
			Version: api.ReplicationCommandVersionV0,
			Uuid:    strfmt.UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),
		})
		require.NoError(t, err)
		require.Error(t, mgr.CancelReplication(&api.ApplyRequest{Version: 12, SubCommand: body}))
		require.Empty(t, *calls)
	})

	t.Run("nil bumper is tolerated (no panic, no observed bump)", func(t *testing.T) {
		parser := fakes.NewMockParser()
		parser.On("ParseClass", mock.Anything).Return(nil)
		schemaManager := schema.NewSchemaManager("test-node", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())
		mgr := replication.NewManager(schemaManager.NewSchemaReader(), mocks.NewMockNodeSelector("localhost"), prometheus.NewPedanticRegistry())
		require.NoError(t, mgr.GetReplicationFSM().Replicate(1, &api.ReplicationReplicateShardRequest{
			Version:          api.ReplicationCommandVersionV0,
			Uuid:             strfmt.UUID("00000000-0000-0000-0000-000000000001"),
			SourceNode:       "node1",
			SourceCollection: "TestClass",
			SourceShard:      "shard1",
			TargetNode:       "node2",
			TransferType:     api.COPY.String(),
		}))
		// No SetReplicationVersionBumper call → nil callback. Apply must not panic.
		body, err := json.Marshal(&api.ReplicationUpdateOpStateRequest{
			Version: api.ReplicationCommandVersionV0,
			Id:      1,
			State:   api.HYDRATING,
		})
		require.NoError(t, err)
		require.NoError(t, mgr.UpdateReplicateOpState(&api.ApplyRequest{Version: 1, SubCommand: body}))
	})
}
