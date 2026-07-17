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

// TestClusterID_CommittedByRealRaftLeader drives cluster-id commit through a
// real single-node raft leader end to end (loop -> Execute -> Apply -> commit).
func TestClusterID_CommittedByRealRaftLeader(t *testing.T) {
	ctx := context.Background()
	m := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
	addr := fmt.Sprintf("%s:%d", m.cfg.Host, m.cfg.RaftPort)
	m.indexer.On("Open", mock.Anything).Return(nil)
	m.indexer.On("Close", mock.Anything).Return(nil)
	m.indexer.On("TriggerSchemaUpdateCallbacks").Return()

	srv := NewRaft(mocks.NewMockNodeSelector(), m.store, nil)
	defer srv.Close(ctx)
	require.NoError(t, srv.Open(ctx, m.indexer))
	require.NoError(t, srv.store.Notify(m.cfg.NodeID, addr))

	require.True(t, tryNTimesWithWait(25, 200*time.Millisecond, srv.store.IsLeader),
		"node did not become raft leader")
	require.True(t, tryNTimesWithWait(10, 200*time.Millisecond, srv.Ready),
		"node did not become ready")

	// generous ceiling for a loaded CI runner; loop ticks every 2s
	require.True(t, tryNTimesWithWait(75, 200*time.Millisecond, func() bool {
		return srv.store.ClusterID() != ""
	}), "bootstrap loop did not commit a clusterId via real raft within timeout")

	id := srv.store.ClusterID()
	require.NotEmpty(t, id)

	waitCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	gotID, err := srv.store.WaitForClusterID(waitCtx)
	require.NoError(t, err)
	assert.Equal(t, id, gotID)

	// Set-once under real raft: another leader commit attempt must not change it.
	srv.store.maybeCommitClusterID()
	assert.Equal(t, id, srv.store.ClusterID(),
		"clusterId must be stable once committed (set-once)")
}
