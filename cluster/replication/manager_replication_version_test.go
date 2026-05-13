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
	"sync"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// recordedBump captures one invocation of the registered bumper so the test
// can assert (class, raftIndex) pairs across multiple op-mutating apply paths.
type recordedBump struct {
	class string
	idx   uint64
}

// TestManager_ReplicationVersionBump pins the contract that every
// op-mutating apply on the replication manager invokes the registered
// version bumper with the op's collection and the apply's RAFT log index.
// Removing or skipping any of these calls would silently regress the
// per-write WaitForUpdate fence that prevents stale-FSM coord routing.
func TestManager_ReplicationVersionBump(t *testing.T) {
	const (
		className = "TestCollection"
		shardName = "shard1"
		srcNode   = "node1"
		tgtNode   = "node2"
	)

	// Build a manager wired to a real SchemaManager (so Replicate's
	// validation finds the class) plus a recording bumper.
	newManager := func(t *testing.T) (*replication.Manager, *[]recordedBump, *sync.Mutex) {
		t.Helper()
		parser := fakes.NewMockParser()
		parser.On("ParseClass", mock.Anything).Return(nil)
		sm := schema.NewSchemaManager("test-node", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())
		require.NoError(t, sm.AddClass(
			buildApplyRequest(className, api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
				Class: &models.Class{Class: className, MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
				State: &sharding.State{
					Physical: map[string]sharding.Physical{shardName: {BelongsToNodes: []string{srcNode}}},
				},
			}), srcNode, true, false))

		mgr := replication.NewManager(sm.NewSchemaReader(), mocks.NewMockNodeSelector("localhost"), prometheus.NewPedanticRegistry())

		var (
			mu      sync.Mutex
			records []recordedBump
		)
		mgr.SetReplicationVersionBumper(func(class string, idx uint64) {
			mu.Lock()
			defer mu.Unlock()
			records = append(records, recordedBump{class: class, idx: idx})
		})
		return mgr, &records, &mu
	}

	// Seed an op so subsequent state mutations have something to act on.
	seedOp := func(t *testing.T, mgr *replication.Manager) strfmt.UUID {
		t.Helper()
		opUUID := strfmt.UUID(uuid.NewString())
		req := &api.ReplicationReplicateShardRequest{
			Uuid:             opUUID,
			SourceCollection: className,
			SourceShard:      shardName,
			SourceNode:       srcNode,
			TargetNode:       tgtNode,
			TransferType:     api.COPY.String(),
		}
		body, err := json.Marshal(req)
		require.NoError(t, err)
		require.NoError(t, mgr.Replicate(1, &api.ApplyRequest{SubCommand: body}))
		return opUUID
	}

	t.Run("UpdateReplicateOpState bumps with op's class + apply index", func(t *testing.T) {
		mgr, records, mu := newManager(t)
		seedOp(t, mgr)

		body, err := json.Marshal(&api.ReplicationUpdateOpStateRequest{
			Version: api.ReplicationCommandVersionV0,
			Id:      1,
			State:   api.HYDRATING,
		})
		require.NoError(t, err)
		require.NoError(t, mgr.UpdateReplicateOpState(&api.ApplyRequest{Version: 42, SubCommand: body}))

		mu.Lock()
		defer mu.Unlock()
		require.Len(t, *records, 1)
		assert.Equal(t, recordedBump{class: className, idx: 42}, (*records)[0])
	})

	t.Run("CancelReplication bumps with op's class + apply index", func(t *testing.T) {
		mgr, records, mu := newManager(t)
		opUUID := seedOp(t, mgr)

		body, err := json.Marshal(&api.ReplicationCancelRequest{
			Version: api.ReplicationCommandVersionV0,
			Uuid:    opUUID,
		})
		require.NoError(t, err)
		require.NoError(t, mgr.CancelReplication(&api.ApplyRequest{Version: 99, SubCommand: body}))

		mu.Lock()
		defer mu.Unlock()
		require.Len(t, *records, 1)
		assert.Equal(t, recordedBump{class: className, idx: 99}, (*records)[0])
	})

	t.Run("DeleteReplication bumps with op's class + apply index", func(t *testing.T) {
		mgr, records, mu := newManager(t)
		opUUID := seedOp(t, mgr)

		body, err := json.Marshal(&api.ReplicationDeleteRequest{
			Version: api.ReplicationCommandVersionV0,
			Uuid:    opUUID,
		})
		require.NoError(t, err)
		require.NoError(t, mgr.DeleteReplication(&api.ApplyRequest{Version: 7, SubCommand: body}))

		mu.Lock()
		defer mu.Unlock()
		require.Len(t, *records, 1)
		assert.Equal(t, recordedBump{class: className, idx: 7}, (*records)[0])
	})

	t.Run("FSM-mutator failure does not bump", func(t *testing.T) {
		mgr, records, mu := newManager(t)
		// Skip seedOp: the cancel will fail with op-not-found, must not bump.

		body, err := json.Marshal(&api.ReplicationCancelRequest{
			Version: api.ReplicationCommandVersionV0,
			Uuid:    strfmt.UUID(uuid.NewString()),
		})
		require.NoError(t, err)
		require.Error(t, mgr.CancelReplication(&api.ApplyRequest{Version: 12, SubCommand: body}))

		mu.Lock()
		defer mu.Unlock()
		assert.Empty(t, *records)
	})

	t.Run("nil bumper is safe", func(t *testing.T) {
		// No SetReplicationVersionBumper call → nil callback. Apply must not panic.
		parser := fakes.NewMockParser()
		parser.On("ParseClass", mock.Anything).Return(nil)
		sm := schema.NewSchemaManager("test-node", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())
		require.NoError(t, sm.AddClass(
			buildApplyRequest(className, api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
				Class: &models.Class{Class: className, MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
				State: &sharding.State{
					Physical: map[string]sharding.Physical{shardName: {BelongsToNodes: []string{srcNode}}},
				},
			}), srcNode, true, false))
		mgr := replication.NewManager(sm.NewSchemaReader(), mocks.NewMockNodeSelector("localhost"), prometheus.NewPedanticRegistry())

		opUUID := seedOp(t, mgr)
		body, err := json.Marshal(&api.ReplicationUpdateOpStateRequest{
			Version: api.ReplicationCommandVersionV0,
			Id:      1,
			State:   api.HYDRATING,
		})
		require.NoError(t, err)
		require.NoError(t, mgr.UpdateReplicateOpState(&api.ApplyRequest{Version: 1, SubCommand: body}))
		_ = opUUID
	})
}
