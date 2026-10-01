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

package raft_shutdown

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
)

type peerFate int

const (
	stopped peerFate = iota
	partitioned
)

// TestNodeShutsDownPromptlyWithoutPeers shuts a node down after it lost its
// peers. Its raft shutdown must not wait for RPCs to those peers to time out:
// until raft is shut down the node does not close its database, so a stop
// timeout shorter than the transport timeout kills it before the database is
// closed cleanly.
func TestNodeShutsDownPromptlyWithoutPeers(t *testing.T) {
	tests := []struct {
		name       string
		lastLeader bool
		peers      [2]peerFate
	}{
		{name: "last node was a follower, peers stopped", lastLeader: false, peers: [2]peerFate{stopped, stopped}},
		{name: "last node was the leader, peers stopped", lastLeader: true, peers: [2]peerFate{stopped, stopped}},
		{name: "peers partitioned", lastLeader: false, peers: [2]peerFate{partitioned, partitioned}},
		{name: "leader with peers partitioned", lastLeader: true, peers: [2]peerFate{partitioned, partitioned}},
		{name: "one peer stopped, one partitioned", lastLeader: false, peers: [2]peerFate{stopped, partitioned}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			compose, err := docker.New().WithWeaviateCluster(3).Start(ctx)
			require.NoError(t, err)
			defer func() { require.NoError(t, compose.Terminate(ctx)) }()

			names := []string{docker.Weaviate0, docker.Weaviate1, docker.Weaviate2}
			leader := leaderName(t, compose)
			var last string
			var peers []string
			for _, n := range names {
				if (n == leader) == tt.lastLeader && last == "" {
					last = n
				} else {
					peers = append(peers, n)
				}
			}

			stopTimeout := time.Minute
			for i, p := range peers {
				switch tt.peers[i] {
				case stopped:
					require.NoError(t, containerByName(compose, p).Container().Stop(ctx, &stopTimeout))
				case partitioned:
					require.NoError(t, compose.DisconnectFromNetwork(ctx, p))
				}
			}

			c := containerByName(compose, last).Container()
			start := time.Now()
			require.NoError(t, c.Stop(ctx, &stopTimeout))
			took := time.Since(start)

			state, err := c.State(ctx)
			require.NoError(t, err)
			require.Equal(t, 0, state.ExitCode, "node must exit cleanly")

			logs, err := c.Logs(ctx)
			require.NoError(t, err)
			out, err := io.ReadAll(logs)
			require.NoError(t, err)
			require.True(t, strings.Contains(string(out), "closing loaded database"), "node must close its database")

			require.Less(t, took, 5*time.Second, "shutdown waited on RPCs to lost peers")
		})
	}
}

func containerByName(compose *docker.DockerCompose, name string) *docker.DockerContainer {
	for _, c := range compose.Containers() {
		if c.Name() == name {
			return c
		}
	}
	return nil
}

func leaderName(t *testing.T, compose *docker.DockerCompose) string {
	t.Helper()
	var leader string
	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://%s/v1/cluster/statistics", compose.GetWeaviate().URI()))
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		var stats models.ClusterStatisticsResponse
		if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&stats) != nil {
			return false
		}
		for _, s := range stats.Statistics {
			if name, ok := s.LeaderID.(string); ok && name != "" {
				leader = name
				return true
			}
		}
		return false
	}, 30*time.Second, 100*time.Millisecond, "cluster must elect a leader")
	return leader
}
