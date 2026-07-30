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

package cluster

import (
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
)

// TestApplyForceDeleteByIds drives the new command through Store.Apply end to end.
// It is red if the store_apply.go case is missing: the default branch logs and
// no-ops, so the ops would simply survive.
func TestApplyForceDeleteByIds(t *testing.T) {
	ms := NewMockStore(t, "Node-1", 0)
	ms.store.metrics = newStoreMetrics("Node-1", prometheus.NewPedanticRegistry())

	// NewMockStore only replaces the *schema* manager's replication FSM with a
	// mock; store.replicationManager stays real, so seed it directly rather than
	// driving a TYPE_REPLICATION_REPLICATE apply (which would need schema fixtures).
	fsm := ms.store.replicationManager.GetReplicationFSM()
	for id := uint64(1); id <= 3; id++ {
		require.NoError(t, fsm.Replicate(id, &api.ReplicationReplicateShardRequest{
			Version:          api.ReplicationCommandVersionV0,
			Uuid:             strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", id)),
			SourceNode:       fmt.Sprintf("source%d", id),
			SourceCollection: "TestClass",
			SourceShard:      "shard1",
			TargetNode:       "target",
			TransferType:     api.COPY.String(),
		}))
	}

	data := cmdAsBytes("", api.ApplyRequest_TYPE_REPLICATION_REPLICATE_FORCE_DELETE_BY_IDS,
		api.ReplicationForceDeleteByIdsRequest{
			Version: api.ReplicationCommandVersionV0,
			Ids:     []uint64{1, 3},
		}, nil)

	resp, ok := ms.store.Apply(&raft.Log{Index: 1, Type: raft.LogCommand, Data: data}).(Response)
	require.True(t, ok)
	require.NoError(t, resp.Error)

	_, ok = fsm.GetOpById(1)
	require.False(t, ok)
	_, ok = fsm.GetOpById(3)
	require.False(t, ok)
	_, ok = fsm.GetOpById(2)
	require.True(t, ok, "the unlisted op must survive")
}
