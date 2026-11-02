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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestStalledParticipants(t *testing.T) {
	record := schema.TenantTransition{Tenant: "t", Transition: &sharding.Transition{
		Kind:    sharding.TransitionFreeze,
		Attempt: 7,
		Participants: map[string]sharding.TransitionParticipant{
			"member":        {Progress: sharding.ProgressStarted},
			"down":          {Progress: sharding.ProgressStarted},
			"down-reported": {Progress: sharding.ProgressDone},
			"gone":          {Progress: sharding.ProgressStarted},
			"gone-reported": {Progress: sharding.ProgressDone},
		},
	}}
	members := map[string]struct{}{"member": {}, "down": {}, "down-reported": {}}

	removed, down := stalledParticipants(record, members, func(node string) bool { return node == "member" })

	assert.Equal(t, []string{"gone"}, removed, "a node that reported has nothing left to abort")
	assert.Equal(t, []string{"down"}, down)
}

func TestAbortReport(t *testing.T) {
	tests := []struct {
		kind string
		want cmd.TenantProcessRequest_Action
	}{
		{sharding.TransitionFreeze, cmd.TenantProcessRequest_ACTION_FREEZING},
		{sharding.TransitionUnfreeze, cmd.TenantProcessRequest_ACTION_UNFREEZING},
	}
	for _, tt := range tests {
		t.Run(tt.kind, func(t *testing.T) {
			req := abortReport(schema.TenantTransition{Tenant: "t", Transition: &sharding.Transition{Kind: tt.kind, Attempt: 7}}, "gone")

			require.Equal(t, "gone", req.Node)
			require.Equal(t, tt.want, req.Action)
			require.Len(t, req.TenantsProcesses, 1)
			got := req.TenantsProcesses[0]
			assert.Equal(t, cmd.TenantsProcess_OP_ABORT, got.Op)
			assert.Equal(t, uint64(7), got.Attempt)
			assert.Equal(t, "t", got.Tenant.Name)
		})
	}
}

func TestTransitionsCaughtUp(t *testing.T) {
	st := NewMockStore(t, "node1", 0).store

	assert.False(t, st.transitionsCaughtUp(), "the DB has not loaded")

	st.dbLoaded.Store(true)
	assert.False(t, st.transitionsCaughtUp(), "raft is not open")

	st.lastAppliedIndex.Store(10)
	assert.True(t, st.fsmReachedIndex(10))
	assert.False(t, st.fsmReachedIndex(11), "raft is not open, so nothing shows the queue is empty")

	st.joined(11)
	assert.Equal(t, uint64(11), st.joinCommitIndex.Load(), "a node that joined with no state waits for the leader's index too")
}

func TestAbortRemovedParticipants(t *testing.T) {
	ctx := context.Background()
	m := NewMockStore(t, "node-1", utils.MustGetFreeTCPPort())
	m.parser.On("ParseClass", mock.Anything).Return(nil)
	m.indexer.On("Open", mock.Anything).Return(nil)
	m.indexer.On("Close", mock.Anything).Return(nil).Maybe()
	m.indexer.On("AddClass", mock.Anything).Return(nil)
	m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
	m.indexer.On("UpdateTenantsProcess", mock.Anything, mock.Anything).Return(nil)
	m.replicationFSM.EXPECT().HasActiveReplicationForCollection(mock.Anything).Return(false).Maybe()

	r := NewRaft(mocks.NewMockNodeSelector("node-1"), m.store, nil)
	require.NoError(t, r.Open(ctx, m.indexer))
	defer r.Close(ctx)
	require.NoError(t, r.store.Notify(m.cfg.NodeID, fmt.Sprintf("%s:%d", m.cfg.Host, m.cfg.RaftPort)))
	require.True(t, tryNTimesWithWait(40, 200*time.Millisecond, r.store.IsLeader))

	running := func(participants ...string) *sharding.Transition {
		transition := &sharding.Transition{Kind: sharding.TransitionFreeze, Target: models.TenantActivityStatusFROZEN, Attempt: 1, Participants: map[string]sharding.TransitionParticipant{}}
		for _, node := range participants {
			transition.Participants[node] = sharding.TransitionParticipant{Progress: sharding.ProgressStarted}
		}
		return transition
	}
	_, err := r.AddClass(ctx,
		&models.Class{Class: "C", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true}, ReplicationConfig: &models.ReplicationConfig{Factor: 1}},
		&sharding.State{PartitioningEnabled: true, Physical: map[string]sharding.Physical{
			"left":   {Name: "left", Status: models.TenantActivityStatusHOT, BelongsToNodes: []string{"node-1", "gone"}, Transition: running("node-1", "gone")},
			"stayed": {Name: "stayed", Status: models.TenantActivityStatusHOT, BelongsToNodes: []string{"node-1"}, Transition: running("node-1")},
		}},
	)
	require.NoError(t, err)

	r.abortRemovedParticipants(ctx)

	state, err := readShardingState(r.SchemaReader(), "C")
	require.NoError(t, err)
	assert.Nil(t, state.Physical["left"].Transition, "a participant that left the cluster aborts the transition")
	assert.NotNil(t, state.Physical["stayed"].Transition, "a member's transition keeps running")
}
