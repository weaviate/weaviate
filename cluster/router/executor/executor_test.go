//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package executor_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/router/executor"
	"github.com/weaviate/weaviate/cluster/router/types"
)

// mockExecutor creates a mock executor that tracks calls and can simulate errors
func mockExecutor(expectedCalls map[string]int, expectedErrors map[string]bool) executor.Operation {
	return func(replica types.Replica) error {
		expectedCalls[replica.NodeName]++
		if expectedErrors[replica.NodeName] {
			return fmt.Errorf("mock error for node %s", replica.NodeName)
		}
		return nil
	}
}

// createTestPlan creates a test routing plan with the specified replicas
func createTestPlan(replicas []types.Replica) types.ReadRoutingPlan {
	return types.ReadRoutingPlan{
		LocalHostname: "node1", // Default local hostname for tests
		ReplicaSet: types.ReadReplicaSet{
			Replicas: replicas,
		},
	}
}

// createTestReplica creates a test replica
func createTestReplica(nodeName, shardName, hostAddr string) types.Replica {
	return types.Replica{
		NodeName:  nodeName,
		ShardName: shardName,
		HostAddr:  hostAddr,
	}
}

func TestExecuteForEachShard(t *testing.T) {
	type testCase struct {
		name           string
		plan           types.ReadRoutingPlan
		expectedError  string
		expectedCalls  map[string]int  // nodeName -> expected call count
		expectedErrors map[string]bool // nodeName -> whether error was expected
	}

	tests := []testCase{
		{
			name: "single local replica",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node1", "shard1", "node1:8080"),
			}),
			expectedCalls: map[string]int{"node1": 1},
		},
		{
			name: "single remote replica",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node2", "shard1", "node2:8080"),
			}),
			expectedCalls: map[string]int{"node2": 1},
		},
		{
			name: "multiple replicas same shard",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node1", "shard1", "node1:8080"),
				createTestReplica("node2", "shard1", "node2:8080"),
				createTestReplica("node3", "shard1", "node3:8080"),
			}),
			expectedCalls: map[string]int{"node1": 1, "node2": 0, "node3": 0}, // Only first replica of shard should be called
		},
		{
			name: "multiple shards",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node1", "shard1", "node1:8080"),
				createTestReplica("node2", "shard2", "node2:8080"),
				createTestReplica("node3", "shard3", "node3:8080"),
			}),
			expectedCalls: map[string]int{"node1": 1, "node2": 1, "node3": 1},
		},
		{
			name: "local executor error",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node1", "shard1", "node1:8080"),
				createTestReplica("node2", "shard2", "node2:8080"),
			}),
			expectedCalls:  map[string]int{"node1": 1, "node2": 0},
			expectedErrors: map[string]bool{"node1": true},
			expectedError:  "failed to locally execute read plan on replica node1: mock error for node node1",
		},
		{
			name: "remote executor error",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node2", "shard1", "node2:8080"),
				createTestReplica("node3", "shard2", "node3:8080"),
			}),
			expectedCalls:  map[string]int{"node2": 1, "node3": 0},
			expectedErrors: map[string]bool{"node2": true},
			expectedError:  "failed to remotely execute read plan on replica node2 at addr node2:8080: mock error for node node2",
		},
		{
			name: "local executor error",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node1", "shard1", "node1:8080"),
				createTestReplica("node2", "shard2", "node2:8080"),
			}),
			expectedCalls:  map[string]int{"node1": 1, "node2": 0},
			expectedErrors: map[string]bool{"node1": true},
			expectedError:  "failed to locally execute read plan on replica node1: mock error for node node1",
		},
		{
			name:          "empty replica set",
			plan:          createTestPlan([]types.Replica{}),
			expectedCalls: map[string]int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock executors
			localCalls := make(map[string]int)
			remoteCalls := make(map[string]int)

			localExecutor := mockExecutor(localCalls, tt.expectedErrors)
			remoteExecutor := mockExecutor(remoteCalls, tt.expectedErrors)

			// Execute the function
			err := executor.ExecuteForEachShard(tt.plan, localExecutor, remoteExecutor)

			// Verify results
			if tt.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedError)
			} else {
				require.NoError(t, err)
			}

			// Verify call counts
			if tt.expectedCalls != nil {
				for nodeName, expectedCount := range tt.expectedCalls {
					actualCount := localCalls[nodeName] + remoteCalls[nodeName]
					require.Equal(t, expectedCount, actualCount,
						"node %s: expected %d calls, got %d", nodeName, expectedCount, actualCount)
				}
			}
		})
	}
}

func TestExecuteForEachReplicaOfShard(t *testing.T) {
	type testCase struct {
		name           string
		plan           types.ReadRoutingPlan
		shardName      string
		expectedError  string
		expectedCalls  map[string]int  // nodeName -> expected call count
		expectedErrors map[string]bool // nodeName -> whether error was expected
	}
	tests := []testCase{
		{
			name: "single local replica matching shard",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node1", "shard1", "node1:8080"),
			}),
			shardName:     "shard1",
			expectedCalls: map[string]int{"node1": 1},
		},
		{
			name: "single remote replica matching shard",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node2", "shard1", "node2:8080"),
			}),
			shardName:     "shard1",
			expectedCalls: map[string]int{"node2": 1},
		},
		{
			name: "multiple replicas matching shard",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node1", "shard1", "node1:8080"),
				createTestReplica("node2", "shard1", "node2:8080"),
				createTestReplica("node3", "shard1", "node3:8080"),
			}),
			shardName:     "shard1",
			expectedCalls: map[string]int{"node1": 1, "node2": 1, "node3": 1},
		},
		{
			name: "replicas from different shards",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node1", "shard1", "node1:8080"),
				createTestReplica("node2", "shard2", "node2:8080"),
				createTestReplica("node3", "shard3", "node3:8080"),
			}),
			shardName:     "shard1",
			expectedCalls: map[string]int{"node1": 1, "node2": 0, "node3": 0},
		},
		{
			name: "no replicas matching shard",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node1", "shard1", "node1:8080"),
				createTestReplica("node2", "shard2", "node2:8080"),
			}),
			shardName:     "shard3",
			expectedCalls: map[string]int{"node1": 0, "node2": 0},
		},
		{
			name: "remote executor error with stop on error",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node2", "shard1", "node2:8080"),
				createTestReplica("node3", "shard1", "node3:8080"),
			}),
			shardName:      "shard1",
			expectedCalls:  map[string]int{"node2": 1, "node3": 0},
			expectedErrors: map[string]bool{"node2": true},
			expectedError:  "failed to remotely execute read plan on replica node2 at addr node2:8080: mock error for node node2",
		},
		{
			name:          "empty replica set",
			plan:          createTestPlan([]types.Replica{}),
			shardName:     "shard1",
			expectedCalls: map[string]int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock executors
			localCalls := make(map[string]int)
			remoteCalls := make(map[string]int)
			localExecutor := mockExecutor(localCalls, tt.expectedErrors)
			remoteExecutor := mockExecutor(remoteCalls, tt.expectedErrors)

			// Execute the function
			err := executor.ExecuteForEachReplicaOfShard(tt.plan, tt.shardName, localExecutor, remoteExecutor)

			// Verify results
			if tt.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedError)
			} else {
				require.NoError(t, err)
			}

			// Verify call counts
			if tt.expectedCalls != nil {
				for nodeName, expectedCount := range tt.expectedCalls {
					actualCount := localCalls[nodeName] + remoteCalls[nodeName]
					require.Equal(t, expectedCount, actualCount,
						"node %s: expected %d calls, got %d", nodeName, expectedCount, actualCount)
				}
			}
		})
	}
}
