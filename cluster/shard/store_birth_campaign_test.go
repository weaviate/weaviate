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

package shard_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/shard"
)

// The birth-campaign tests separate "designated campaigner fired" from "the
// randomized election race resolved" by timeout separation: with every node's
// election timeout far beyond the assertion window, the ONLY way a leader can
// emerge inside the window is the designated node's immediate birth campaign.
// Without the feature these tests are deterministically red (no leader within
// the window), which is the honest replacement for a statistical distribution
// assertion against a random race.
const birthRaceTimeout = 60 * time.Second

func buildBirthCluster(t *testing.T, shardName, designated string, electionTimeout time.Duration) []*shard.Store {
	t.Helper()
	nodeIDs := []string{"node-a", "node-b", "node-c"}
	specs := make([]shard.TestStoreSpec, len(nodeIDs))
	for i, id := range nodeIDs {
		specs[i] = shard.TestStoreSpec{NodeID: id, PreferredLeader: designated}
	}
	return shard.BuildTestClusterWithOptions(t, "BirthCampaignClass", shardName, specs,
		shard.TestClusterOptions{
			TickInterval:      20 * time.Millisecond,
			HeartbeatTimeout:  40 * time.Millisecond,
			ElectionTimeout:   electionTimeout,
			SnapshotThreshold: 4096,
		})
}

// TestStore_BirthCampaign_DesignatedNodeWinsImmediately pins the core
// mechanism: on a newly bootstrapped group the designated node campaigns as
// soon as the bootstrap conf changes are applied, and wins long before any
// election timer can fire. The designation is deliberately NOT the first
// member, so the test also proves the non-designated nodes held back.
//
// This test doubly pins the deferred fire: etcd's hup() scan-gate refuses a
// campaign issued inside Store.Start (Bootstrap applies the conf changes to
// the raft config in-place, but the log-applied cursor lags at 0, and hup()
// scans (applied, committed] for conf-change entries). Moving the Campaign
// call back into Start silently no-ops it, the 60s race takes over, and this
// test times out.
func TestStore_BirthCampaign_DesignatedNodeWinsImmediately(t *testing.T) {
	stores := buildBirthCluster(t, "shard-birth", "node-b", birthRaceTimeout)
	for _, s := range stores {
		require.NoError(t, s.Start(context.Background()))
	}

	require.Eventually(t, stores[1].IsLeader, 5*time.Second, 20*time.Millisecond,
		"designated node-b must win the birth election immediately; a 5s wait against a 60s election timeout means the birth campaign never fired")
	for i, s := range stores {
		require.Eventuallyf(t, func() bool { return s.LeaderID() == "node-b" },
			5*time.Second, 20*time.Millisecond,
			"store %d must observe node-b as leader", i)
		require.Equal(t, "node-b", s.PreferredLeader(),
			"accessor must expose the birth designation on every voter")
	}
}

// TestStore_BirthCampaign_RotatesAcrossGroups pins the operator-visible
// journey for a single-tenant class: successive groups whose designations
// rotate (as PreferredBirthLeader derives from the sharding-state generator's
// rotated member heads) get leaders on distinct nodes — exactly one
// leadership per node across three groups, instead of the race's occasional
// 3-on-1 concentration.
func TestStore_BirthCampaign_RotatesAcrossGroups(t *testing.T) {
	designations := []string{"node-a", "node-b", "node-c"}
	leaders := make(map[string]int)
	for i, designated := range designations {
		stores := buildBirthCluster(t, fmt.Sprintf("shard-rot-%d", i), designated, birthRaceTimeout)
		for _, s := range stores {
			require.NoError(t, s.Start(context.Background()))
		}
		idx := waitForClusterLeader(t, stores)
		require.Equal(t, designated, stores[idx].LeaderID(),
			"group %d leader must be its designation", i)
		leaders[stores[idx].LeaderID()]++
	}
	require.Equal(t, map[string]int{"node-a": 1, "node-b": 1, "node-c": 1}, leaders,
		"three groups with rotated designations must spread leadership exactly evenly")
}

// TestStore_BirthCampaign_FallsBackWhenDesignatedDown pins the degradation
// guarantee: when the designated node is absent at group birth, the placement
// hint must neither stall the birth nor delay the normal election — the
// remaining voters elect within the ordinary randomized-timeout envelope.
func TestStore_BirthCampaign_FallsBackWhenDesignatedDown(t *testing.T) {
	stores := buildBirthCluster(t, "shard-down", "node-c", 600*time.Millisecond)
	// node-c — the designated campaigner — never starts.
	require.NoError(t, stores[0].Start(context.Background()))
	require.NoError(t, stores[1].Start(context.Background()))

	idx := waitForClusterLeader(t, stores[:2])
	require.NotEqual(t, "node-c", stores[idx].LeaderID(),
		"a never-started node cannot lead")
}

// TestStore_BirthCampaign_RestartDoesNotCampaign pins the birth-only gate: a
// restarting store of an EXISTING group must never fire the campaign, even
// with the designation still configured — failover and recovery elections
// stay on the stock randomized timers. Phase 1 proves the birth campaign
// works on this very store (leader far inside a 30s election timeout); phase
// 2 rebuilds it on the same persisted group state and proves no immediate
// election happens.
func TestStore_BirthCampaign_RestartDoesNotCampaign(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "shared-raft-log")
	snapRoot := t.TempDir()
	opts := shard.TestClusterOptions{
		TickInterval:     10 * time.Millisecond,
		HeartbeatTimeout: 20 * time.Millisecond,
		ElectionTimeout:  30 * time.Second,
		PreferredLeader:  "node-r",
	}

	store, closeInfra := shard.BuildTestStoreAtWithOptions(
		t, "BirthRestartClass", "shard-restart", "node-r", logPath, snapRoot, opts, nil)
	require.NoError(t, store.Start(context.Background()))
	require.Eventually(t, store.IsLeader, 5*time.Second, 20*time.Millisecond,
		"birth: the designated single voter must elect itself via the birth campaign (30s election timeout rules out the race)")
	closeInfra()

	restarted, closeRestarted := shard.BuildTestStoreAtWithOptions(
		t, "BirthRestartClass", "shard-restart", "node-r", logPath, snapRoot, opts, nil)
	defer closeRestarted()
	require.NoError(t, restarted.Start(context.Background()))
	require.Equal(t, "node-r", restarted.PreferredLeader(),
		"designation is still configured on restart — only the birth gate may suppress the campaign")
	require.Never(t, restarted.IsLeader, 2*time.Second, 50*time.Millisecond,
		"restart of an existing group must not birth-campaign; leadership before the 30s election timeout means the hasGroup gate broke")
}
