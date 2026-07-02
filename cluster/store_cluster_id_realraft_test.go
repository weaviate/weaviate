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

	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
)

// TestClusterID_CommittedByRealRaftLeader exercises the real bootstrap path that
// the in-memory unit tests in store_cluster_id_test.go deliberately fake. It
// stands up a self-electing single-node raft leader and lets clusterIDBootstrapLoop
// drive the whole chain end to end:
//
//	loop tick -> IsLeader() -> maybeCommitClusterID() -> uuid.NewV7() -> Execute()
//	-> raft consensus -> FSM.Apply -> applyClusterIDSet -> setClusterIDFields
//	-> close(clusterIDSet)
//
// This is the leadership retry path the reviewer flagged as uncovered: the
// identity is committed through real raft (not poked in-memory), proving that a
// leader with no clusterId set will commit exactly one, and that the loop wiring,
// the proto command, and the apply handler all agree.
//
// A full 3-node leader-crash-before-commit reproduction (kill the leader inside
// the 2s commit window and assert a second leader converges) needs a live
// multi-node consensus harness that does not exist at this layer and is prone to
// memberlist/election flakiness in CI; it is left to acceptance-level testing.
// The two properties that path relies on are both covered deterministically:
// (1) any leader with an unset id commits one via the real loop (this test), and
// (2) a second commit is a no-op under the set-once guard (asserted below and in
// TestClusterID_IdempotentOnReplay).
func TestClusterID_CommittedByRealRaftLeader(t *testing.T) {
	ctx := context.Background()
	m := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
	addr := fmt.Sprintf("%s:%d", m.cfg.Host, m.cfg.RaftPort)
	m.indexer.On("Open", mock.Anything).Return(nil)
	m.indexer.On("Close", mock.Anything).Return(nil)
	m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
	m.replicationFSM.EXPECT().HasActiveReplicationForCollection(mock.Anything).Return(false).Maybe()
	m.replicationFSM.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()

	srv := NewRaft(mocks.NewMockNodeSelector(), m.store, nil)
	defer srv.Close(ctx)
	require.NoError(t, srv.Open(ctx, m.indexer))
	require.NoError(t, srv.store.Notify(m.cfg.NodeID, addr))

	require.True(t, tryNTimesWithWait(25, 200*time.Millisecond, srv.store.IsLeader),
		"node did not become raft leader")
	require.True(t, tryNTimesWithWait(10, 200*time.Millisecond, srv.Ready),
		"node did not become ready")

	// The bootstrap loop ticks every 2s; the id is committed via real raft apply
	// shortly after leadership. 15s is a generous ceiling for a loaded CI runner.
	require.True(t, tryNTimesWithWait(75, 200*time.Millisecond, func() bool {
		return srv.store.ClusterID() != ""
	}), "bootstrap loop did not commit a clusterId via real raft within timeout")

	id := srv.store.ClusterID()
	require.NotEmpty(t, id)

	// WaitForClusterID returns the committed identity without blocking, with a
	// sane inception timestamp committed alongside it.
	waitCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	gotID, createdAt, err := srv.store.WaitForClusterID(waitCtx)
	require.NoError(t, err)
	assert.Equal(t, id, gotID)
	assert.Greater(t, createdAt, int64(0),
		"clusterCreatedAt should be a positive unix-millis timestamp")

	// Set-once under real raft: another leader commit attempt must not change it.
	srv.store.maybeCommitClusterID()
	assert.Equal(t, id, srv.store.ClusterID(),
		"clusterId must be stable once committed (set-once)")
}
