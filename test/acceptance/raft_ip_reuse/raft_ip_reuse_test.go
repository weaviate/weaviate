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

package raft_ip_reuse

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/cluster"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// stableLeaderWindow outlasts hashicorp/raft's longest replication backoff (10ms*2^10), so the leader has dialed a
// reused IP before the next node starts, and spans many election timeouts, so a leader that steps down shows a new term.
const stableLeaderWindow = 15 * time.Second

type move struct {
	node string
	host int // last octet of the new IP
}

// TestRestartOnReusedIPs restarts nodes of a cluster with one voter (weaviate-0, always the leader) and two non-voters
// on new IPs, one of them another node's old IP. The RAFT configuration still stores every node's old address, and the
// leader dials it while it cannot resolve the node through memberlist. Every node must become ready, the leader must
// keep its term, and later RAFT entries must reach every node.
func TestRestartOnReusedIPs(t *testing.T) {
	tests := []struct {
		name          string
		restartLeader bool
		moves         []move // applied in order: a node vacates an IP before another takes it
		startOrder    []string
	}{
		{
			// 0-weaviate-issues#707: the leader's pipeline for weaviate-1 lands on weaviate-2 and never re-resolves.
			name:          "follower takes another follower's old IP",
			restartLeader: true,
			moves:         []move{{docker.Weaviate1, 21}, {docker.Weaviate2, 11}},
			startOrder:    []string{docker.Weaviate0, docker.Weaviate2, docker.Weaviate1},
		},
		{
			name:       "follower takes another follower's old IP while the leader stays up",
			moves:      []move{{docker.Weaviate1, 21}, {docker.Weaviate2, 11}},
			startOrder: []string{docker.Weaviate2, docker.Weaviate1},
		},
		{
			// The leader's stored address for weaviate-2 is its own; a leader steps down on an AppendEntries of its own term.
			name:          "leader takes a follower's old IP",
			restartLeader: true,
			moves:         []move{{docker.Weaviate2, 22}, {docker.Weaviate1, 21}, {docker.Weaviate0, 12}},
			startOrder:    []string{docker.Weaviate0, docker.Weaviate1, docker.Weaviate2},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			compose, err := docker.New().
				With3NodeCluster().
				WithWeaviateEnv("RAFT_BOOTSTRAP_EXPECT", "1").
				Start(ctx)
			require.NoError(t, err)
			t.Cleanup(func() {
				if err := compose.Terminate(ctx); err != nil {
					t.Errorf("failed to terminate test containers: %v", err)
				}
			})
			t.Cleanup(func() {
				if t.Failed() {
					dumpDiagnostics(ctx, t, compose)
				}
			})

			helper.SetupClient(compose.GetWeaviate().URI())
			helper.CreateClass(t, &models.Class{Class: "BeforeRestart", Vectorizer: "none"})
			requireConverged(t)

			stop := []string{docker.Weaviate1, docker.Weaviate2}
			if tc.restartLeader {
				stop = append([]string{docker.Weaviate0}, stop...)
			}
			for _, node := range stop {
				require.NoError(t, compose.Stop(ctx, node, nil))
			}
			for _, m := range tc.moves {
				ip := compose.SubnetIP(m.host)
				t.Logf("%s: %s -> %s", m.node, compose.IP(m.node), ip)
				require.NoError(t, compose.MoveToIP(ctx, m.node, ip))
			}

			for _, node := range tc.startOrder {
				require.NoErrorf(t, compose.Start(ctx, node), "%s must become ready", node)
				helper.SetupClient(compose.GetWeaviate().URI())
				requireStableLeader(t)
			}

			helper.CreateClass(t, &models.Class{Class: "AfterRestart", Vectorizer: "none"})
			requireConverged(t)
		})
	}
}

// requireStableLeader requires weaviate-0 to lead in the same term at both ends of stableLeaderWindow. With a single
// voter the term changes only if the leader steps down.
func requireStableLeader(t *testing.T) {
	t.Helper()
	before := leaderTerm(t)
	time.Sleep(stableLeaderWindow)
	require.Equal(t, before, leaderTerm(t), "weaviate-0 stepped down and re-elected itself")
}

func leaderTerm(t *testing.T) string {
	t.Helper()
	var term string
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		resp, err := helper.Client(t).Cluster.ClusterGetStatistics(cluster.NewClusterGetStatisticsParams(), nil)
		require.NoError(ct, err)
		for _, s := range resp.Payload.Statistics {
			if s.Name == docker.Weaviate0 && s.Raft != nil {
				require.Equalf(ct, "Leader", s.Raft.State, "weaviate-0 does not stay leader (term %s)", s.Raft.Term)
				term = s.Raft.Term
				return
			}
		}
		require.Fail(ct, "no RAFT statistics for weaviate-0")
	}, 30*time.Second, 500*time.Millisecond)
	return term
}

// requireConverged requires all three nodes to be ready and to have applied the same RAFT index.
func requireConverged(t *testing.T) {
	t.Helper()
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		resp, err := helper.Client(t).Cluster.ClusterGetStatistics(cluster.NewClusterGetStatisticsParams(), nil)
		require.NoError(ct, err)
		require.Len(ct, resp.Payload.Statistics, 3)
		for _, s := range resp.Payload.Statistics {
			require.Truef(ct, s.Ready, "%s is not ready", s.Name)
		}
		require.True(ct, resp.Payload.Synchronized, "nodes applied different RAFT indexes")
	}, 30*time.Second, 500*time.Millisecond)
}

func dumpDiagnostics(ctx context.Context, t *testing.T, compose *docker.DockerCompose) {
	client := http.Client{Timeout: 5 * time.Second}
	if resp, err := client.Get("http://" + compose.GetWeaviate().URI() + "/v1/cluster/statistics"); err != nil {
		t.Logf("cluster statistics unavailable: %v", err)
	} else {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Logf("cluster statistics: %s", body)
	}
	var logs strings.Builder
	compose.DumpWeaviateLogs(ctx, &logs, 300)
	t.Log(logs.String())
}
