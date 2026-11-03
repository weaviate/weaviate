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

package namespace

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/cluster"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// leaderName returns the node that reports itself as the RAFT leader.
func leaderName(t *testing.T) string {
	t.Helper()

	var leader string
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		resp, err := helper.Client(t).Cluster.ClusterGetStatistics(
			cluster.NewClusterGetStatisticsParams(), helper.CreateAuth(adminKey))
		if !assert.NoError(c, err) {
			return
		}
		for _, st := range resp.Payload.Statistics {
			if st.Raft != nil && st.Raft.State == "Leader" {
				leader = st.Name
				return
			}
		}
		c.Errorf("no node reports itself as the leader")
	}, 30*time.Second, 200*time.Millisecond, "the cluster never reported a leader")
	return leader
}

// requireDBLoadedEventually waits until node reports its database loaded. Only
// then has its boot built every index, so a class it lists no shard for was
// released rather than not reached yet.
func requireDBLoadedEventually(t *testing.T, node string) {
	t.Helper()

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		resp, err := helper.Client(t).Cluster.ClusterGetStatistics(
			cluster.NewClusterGetStatisticsParams(), helper.CreateAuth(adminKey))
		if !assert.NoError(c, err) {
			return
		}
		for _, st := range resp.Payload.Statistics {
			if st.Name == node {
				assert.True(c, st.DbLoaded)
				return
			}
		}
		c.Errorf("the cluster statistics list no node %q", node)
	}, 30*time.Second, 200*time.Millisecond, "%q never reported its database loaded", node)
}

// requireNoShardsEventually waits until none of the classes lists a shard. live
// is read first in the same poll, because a node that does not answer lists no
// shards either.
func requireNoShardsEventually(t *testing.T, live presentShard, qualifiedClasses ...string) {
	t.Helper()

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		alive, err := shardsForClass(t, live.qualifiedClass)
		if !assert.NoError(c, err) || !assert.Contains(c, alive, live.name) {
			return
		}
		for _, class := range qualifiedClasses {
			got, err := shardsForClass(t, class)
			if assert.NoError(c, err) {
				assert.Empty(c, got, "%q still lists shards", class)
			}
		}
	}, 30*time.Second, 200*time.Millisecond, "%v never listed no shard beside %q", qualifiedClasses, live.name)
}

// A suspended namespace's shards must close on a home node that is a follower,
// and on one that was down when the suspend committed. The test is serial so
// that no parallel test runs while its second subtest stops a node.
func TestNamespaces_SuspendReleasesShardsOnFollowers(t *testing.T) {
	// The home node is neither weaviate-0, which the shared client talks to, nor
	// the leader, so its sweep is a follower's and stopping it keeps the leader.
	home := docker.Weaviate1
	if leaderName(t) == home {
		home = docker.Weaviate2
	}

	anchorNS, followerNS, downNS := uniqueNS(), uniqueNS(), uniqueNS()
	keys := map[string]string{}
	for _, ns := range []string{anchorNS, followerNS, downNS} {
		helper.CreateNamespaceWithHomeNode(t, ns, home, adminKey)
		t.Cleanup(func() { helper.DeleteNamespace(t, ns, adminKey) })
		keys[ns] = createNamespacedUser(t, "u1", ns, adminKey)
		t.Cleanup(func() { helper.DeleteUser(t, ns+":u1", adminKey) })
	}

	const (
		anchorClass       = "Anchor"
		singleTenantClass = "Released"
		multiTenantClass  = "ReleasedTenants"
		tenant            = "warm"
	)
	setupClassInNs1(t, anchorNS, anchorClass, keys[anchorNS])
	anchorShards := requireShardCountEventually(t, anchorNS+":"+anchorClass, 1)
	anchor := presentShard{anchorNS + ":" + anchorClass, anchorShards[0]}

	for _, ns := range []string{followerNS, downNS} {
		setupClassInNs1(t, ns, singleTenantClass, keys[ns])
		setupMTClassInNs1(t, ns, multiTenantClass, keys[ns])
		require.NoError(t, addTenantsAuth(t, multiTenantClass, []*models.Tenant{
			{Name: tenant, ActivityStatus: models.TenantActivityStatusHOT},
		}, keys[ns]))
		requireShardCountEventually(t, ns+":"+singleTenantClass, 1)
		requireShardsEventually(t, ns+":"+multiTenantClass, tenant)
	}

	t.Run("a follower releases a suspended namespace's shards on its own sweep", func(t *testing.T) {
		helper.SuspendNamespace(t, followerNS, adminKey)
		t.Cleanup(func() { helper.ResumeNamespace(t, followerNS, adminKey) })

		requireNoShardsEventually(t, anchor, followerNS+":"+singleTenantClass, followerNS+":"+multiTenantClass)
		require.NotEqual(t, home, leaderName(t),
			"the home node became the leader, so the release was not a follower's")
	})

	t.Run("a node down when the suspend commits releases the shards after it restarts", func(t *testing.T) {
		withHomeNodeStopped(t, home, func(t *testing.T) {
			helper.SuspendNamespace(t, downNS, adminKey)
			t.Cleanup(func() { helper.ResumeNamespace(t, downNS, adminKey) })
		})

		requireDBLoadedEventually(t, home)
		requireNoShardsEventually(t, anchor, downNS+":"+singleTenantClass, downNS+":"+multiTenantClass)
	})
}
