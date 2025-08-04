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

package router_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/router"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
)

// testCase represents a single test case for executor functions

// mockExecutor creates a mock executor that tracks calls and can simulate errors
func mockExecutor(expectedCalls map[string]int, expectedErrors map[string]bool) types.ReadExecutor {
	return func(replica types.Replica) error {
		expectedCalls[replica.NodeName]++
		if expectedErrors[replica.NodeName] {
			return fmt.Errorf("mock error for node %s", replica.NodeName)
		}
		return nil
	}
}

// createTestRouter creates a router for testing based on the router type
func createTestRouter(routerType string) types.Router {
	mockNodeSelector := mocks.NewMockNodeSelector("node1", "node2", "node3")

	// For testing purposes, we'll use the same mock components for both router types
	// In a real scenario, these would be properly mocked based on the router type
	if routerType == "single-tenant" {
		return router.NewBuilder(
			"TestClass",
			false, // single-tenant
			mockNodeSelector,
			nil, // schemaGetter not needed for executor tests
			nil, // schemaReader not needed for executor tests
			nil, // replicationFSM not needed for executor tests
		).Build()
	} else {
		return router.NewBuilder(
			"TestClass",
			true, // multi-tenant
			mockNodeSelector,
			nil, // schemaGetter not needed for executor tests
			nil, // schemaReader not needed for executor tests
			nil, // replicationFSM not needed for executor tests
		).Build()
	}
}

// createTestPlan creates a test routing plan with the specified replicas
func createTestPlan(replicas []types.Replica) types.ReadRoutingPlan {
	return types.ReadRoutingPlan{
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
		routerType     string // "single-tenant" or "multi-tenant"
		plan           types.ReadRoutingPlan
		options        types.ReadRoutingPlanExecutorOptions
		expectedError  string
		expectedCalls  map[string]int  // nodeName -> expected call count
		expectedErrors map[string]bool // nodeName -> whether error was expected
	}

	tests := []testCase{
		{
			name:       "single-tenant: single local replica",
			routerType: "single-tenant",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node1", "shard1", "node1:8080"),
			}),
			options:       types.ReadRoutingPlanExecutorOptions{StopOnError: true},
			expectedCalls: map[string]int{"node1": 1},
		},
		{
			name:       "multi-tenant: single local replica",
			routerType: "multi-tenant",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node1", "shard1", "node1:8080"),
			}),
			options:       types.ReadRoutingPlanExecutorOptions{StopOnError: true},
			expectedCalls: map[string]int{"node1": 1},
		},
		{
			name:       "single-tenant: single remote replica",
			routerType: "single-tenant",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node2", "shard1", "node2:8080"),
			}),
			options:       types.ReadRoutingPlanExecutorOptions{StopOnError: true},
			expectedCalls: map[string]int{"node2": 1},
		},
		{
			name:       "multi-tenant: single remote replica",
			routerType: "multi-tenant",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node2", "shard1", "node2:8080"),
			}),
			options:       types.ReadRoutingPlanExecutorOptions{StopOnError: true},
			expectedCalls: map[string]int{"node2": 1},
		},
		{
			name:       "single-tenant: multiple replicas same shard",
			routerType: "single-tenant",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node1", "shard1", "node1:8080"),
				createTestReplica("node2", "shard1", "node2:8080"),
				createTestReplica("node3", "shard1", "node3:8080"),
			}),
			options:       types.ReadRoutingPlanExecutorOptions{StopOnError: true},
			expectedCalls: map[string]int{"node1": 1, "node2": 0, "node3": 0}, // Only first replica of shard should be called
		},
		{
			name:       "multi-tenant: multiple replicas same shard",
			routerType: "multi-tenant",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node1", "shard1", "node1:8080"),
				createTestReplica("node2", "shard1", "node2:8080"),
				createTestReplica("node3", "shard1", "node3:8080"),
			}),
			options:       types.ReadRoutingPlanExecutorOptions{StopOnError: true},
			expectedCalls: map[string]int{"node1": 1, "node2": 0, "node3": 0}, // Only first replica of shard should be called
		},
		{
			name:       "single-tenant: multiple shards",
			routerType: "single-tenant",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node1", "shard1", "node1:8080"),
				createTestReplica("node2", "shard2", "node2:8080"),
				createTestReplica("node3", "shard3", "node3:8080"),
			}),
			options:       types.ReadRoutingPlanExecutorOptions{StopOnError: true},
			expectedCalls: map[string]int{"node1": 1, "node2": 1, "node3": 1},
		},
		{
			name:       "multi-tenant: multiple shards",
			routerType: "multi-tenant",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node1", "shard1", "node1:8080"),
				createTestReplica("node2", "shard2", "node2:8080"),
				createTestReplica("node3", "shard3", "node3:8080"),
			}),
			options:       types.ReadRoutingPlanExecutorOptions{StopOnError: true},
			expectedCalls: map[string]int{"node1": 1, "node2": 1, "node3": 1},
		},
		{
			name:       "single-tenant: local executor error with stop on error",
			routerType: "single-tenant",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node1", "shard1", "node1:8080"),
				createTestReplica("node2", "shard2", "node2:8080"),
			}),
			options:        types.ReadRoutingPlanExecutorOptions{StopOnError: true},
			expectedCalls:  map[string]int{"node1": 1, "node2": 0},
			expectedErrors: map[string]bool{"node1": true},
			expectedError:  "failed to locally execute read plan on replica node1: mock error for node node1",
		},
		{
			name:       "multi-tenant: local executor error with stop on error",
			routerType: "multi-tenant",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node1", "shard1", "node1:8080"),
				createTestReplica("node2", "shard2", "node2:8080"),
			}),
			options:        types.ReadRoutingPlanExecutorOptions{StopOnError: true},
			expectedCalls:  map[string]int{"node1": 1, "node2": 0},
			expectedErrors: map[string]bool{"node1": true},
			expectedError:  "failed to locally execute read plan on replica node1: mock error for node node1",
		},
		{
			name:       "single-tenant: remote executor error with stop on error",
			routerType: "single-tenant",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node2", "shard1", "node2:8080"),
				createTestReplica("node3", "shard2", "node3:8080"),
			}),
			options:        types.ReadRoutingPlanExecutorOptions{StopOnError: true},
			expectedCalls:  map[string]int{"node2": 1, "node3": 0},
			expectedErrors: map[string]bool{"node2": true},
			expectedError:  "failed to remotely execute read plan on replica node2 at addr node2:8080: mock error for node node2",
		},
		{
			name:       "multi-tenant: remote executor error with stop on error",
			routerType: "multi-tenant",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node2", "shard1", "node2:8080"),
				createTestReplica("node3", "shard2", "node3:8080"),
			}),
			options:        types.ReadRoutingPlanExecutorOptions{StopOnError: true},
			expectedCalls:  map[string]int{"node2": 1, "node3": 0},
			expectedErrors: map[string]bool{"node2": true},
			expectedError:  "failed to remotely execute read plan on replica node2 at addr node2:8080: mock error for node node2",
		},
		{
			name:       "single-tenant: local executor error without stop on error",
			routerType: "single-tenant",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node1", "shard1", "node1:8080"),
				createTestReplica("node2", "shard2", "node2:8080"),
			}),
			options:        types.ReadRoutingPlanExecutorOptions{StopOnError: false},
			expectedCalls:  map[string]int{"node1": 1, "node2": 1},
			expectedErrors: map[string]bool{"node1": true},
		},
		{
			name:       "multi-tenant: local executor error without stop on error",
			routerType: "multi-tenant",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node1", "shard1", "node1:8080"),
				createTestReplica("node2", "shard2", "node2:8080"),
			}),
			options:        types.ReadRoutingPlanExecutorOptions{StopOnError: false},
			expectedCalls:  map[string]int{"node1": 1, "node2": 1},
			expectedErrors: map[string]bool{"node1": true},
		},
		{
			name:          "single-tenant: empty replica set",
			routerType:    "single-tenant",
			plan:          createTestPlan([]types.Replica{}),
			options:       types.ReadRoutingPlanExecutorOptions{StopOnError: true},
			expectedCalls: map[string]int{},
		},
		{
			name:          "multi-tenant: empty replica set",
			routerType:    "multi-tenant",
			plan:          createTestPlan([]types.Replica{}),
			options:       types.ReadRoutingPlanExecutorOptions{StopOnError: true},
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

			// Create router (we'll use a mock since we can't easily create real routers in tests)
			r := createTestRouter(tt.routerType)

			// Execute the function
			err := r.ExecuteForEachShard(tt.plan, tt.options, localExecutor, remoteExecutor)

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
		routerType     string // "single-tenant" or "multi-tenant"
		plan           types.ReadRoutingPlan
		options        types.ReadRoutingPlanExecutorOptions
		shardName      string
		expectedError  string
		expectedCalls  map[string]int  // nodeName -> expected call count
		expectedErrors map[string]bool // nodeName -> whether error was expected
	}
	tests := []testCase{
		{
			name:       "single-tenant: single local replica matching shard",
			routerType: "single-tenant",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node1", "shard1", "node1:8080"),
			}),
			options:       types.ReadRoutingPlanExecutorOptions{StopOnError: true},
			shardName:     "shard1",
			expectedCalls: map[string]int{"node1": 1},
		},
		{
			name:       "multi-tenant: single local replica matching shard",
			routerType: "multi-tenant",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node1", "shard1", "node1:8080"),
			}),
			options:       types.ReadRoutingPlanExecutorOptions{StopOnError: true},
			shardName:     "shard1",
			expectedCalls: map[string]int{"node1": 1},
		},
		{
			name:       "single-tenant: single remote replica matching shard",
			routerType: "single-tenant",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node2", "shard1", "node2:8080"),
			}),
			options:       types.ReadRoutingPlanExecutorOptions{StopOnError: true},
			shardName:     "shard1",
			expectedCalls: map[string]int{"node2": 1},
		},
		{
			name:       "multi-tenant: single remote replica matching shard",
			routerType: "multi-tenant",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node2", "shard1", "node2:8080"),
			}),
			options:       types.ReadRoutingPlanExecutorOptions{StopOnError: true},
			shardName:     "shard1",
			expectedCalls: map[string]int{"node2": 1},
		},
		{
			name:       "single-tenant: multiple replicas matching shard",
			routerType: "single-tenant",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node1", "shard1", "node1:8080"),
				createTestReplica("node2", "shard1", "node2:8080"),
				createTestReplica("node3", "shard1", "node3:8080"),
			}),
			options:       types.ReadRoutingPlanExecutorOptions{StopOnError: true},
			shardName:     "shard1",
			expectedCalls: map[string]int{"node1": 1, "node2": 1, "node3": 1},
		},
		{
			name:       "multi-tenant: multiple replicas matching shard",
			routerType: "multi-tenant",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node1", "shard1", "node1:8080"),
				createTestReplica("node2", "shard1", "node2:8080"),
				createTestReplica("node3", "shard1", "node3:8080"),
			}),
			options:       types.ReadRoutingPlanExecutorOptions{StopOnError: true},
			shardName:     "shard1",
			expectedCalls: map[string]int{"node1": 1, "node2": 1, "node3": 1},
		},
		{
			name:       "single-tenant: replicas from different shards",
			routerType: "single-tenant",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node1", "shard1", "node1:8080"),
				createTestReplica("node2", "shard2", "node2:8080"),
				createTestReplica("node3", "shard3", "node3:8080"),
			}),
			options:       types.ReadRoutingPlanExecutorOptions{StopOnError: true},
			shardName:     "shard1",
			expectedCalls: map[string]int{"node1": 1, "node2": 0, "node3": 0},
		},
		{
			name:       "multi-tenant: replicas from different shards",
			routerType: "multi-tenant",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node1", "shard1", "node1:8080"),
				createTestReplica("node2", "shard2", "node2:8080"),
				createTestReplica("node3", "shard3", "node3:8080"),
			}),
			options:       types.ReadRoutingPlanExecutorOptions{StopOnError: true},
			shardName:     "shard1",
			expectedCalls: map[string]int{"node1": 1, "node2": 0, "node3": 0},
		},
		{
			name:       "single-tenant: no replicas matching shard",
			routerType: "single-tenant",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node1", "shard1", "node1:8080"),
				createTestReplica("node2", "shard2", "node2:8080"),
			}),
			options:       types.ReadRoutingPlanExecutorOptions{StopOnError: true},
			shardName:     "shard3",
			expectedCalls: map[string]int{"node1": 0, "node2": 0},
		},
		{
			name:       "multi-tenant: no replicas matching shard",
			routerType: "multi-tenant",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node1", "shard1", "node1:8080"),
				createTestReplica("node2", "shard2", "node2:8080"),
			}),
			options:       types.ReadRoutingPlanExecutorOptions{StopOnError: true},
			shardName:     "shard3",
			expectedCalls: map[string]int{"node1": 0, "node2": 0},
		},
		{
			name:       "single-tenant: local executor error with stop on error",
			routerType: "single-tenant",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node1", "shard1", "node1:8080"),
				createTestReplica("node2", "shard1", "node2:8080"),
			}),
			options:        types.ReadRoutingPlanExecutorOptions{StopOnError: true},
			shardName:      "shard1",
			expectedCalls:  map[string]int{"node1": 1, "node2": 0},
			expectedErrors: map[string]bool{"node1": true},
			expectedError:  "failed to locally execute read plan on replica node1: mock error for node node1",
		},
		{
			name:       "multi-tenant: local executor error with stop on error",
			routerType: "multi-tenant",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node1", "shard1", "node1:8080"),
				createTestReplica("node2", "shard1", "node2:8080"),
			}),
			options:        types.ReadRoutingPlanExecutorOptions{StopOnError: true},
			shardName:      "shard1",
			expectedCalls:  map[string]int{"node1": 1, "node2": 0},
			expectedErrors: map[string]bool{"node1": true},
			expectedError:  "failed to locally execute read plan on replica node1: mock error for node node1",
		},
		{
			name:       "single-tenant: remote executor error with stop on error",
			routerType: "single-tenant",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node2", "shard1", "node2:8080"),
				createTestReplica("node3", "shard1", "node3:8080"),
			}),
			options:        types.ReadRoutingPlanExecutorOptions{StopOnError: true},
			shardName:      "shard1",
			expectedCalls:  map[string]int{"node2": 1, "node3": 0},
			expectedErrors: map[string]bool{"node2": true},
			expectedError:  "failed to remotely execute read plan on replica node2 at addr node2:8080: mock error for node node2",
		},
		{
			name:       "multi-tenant: remote executor error with stop on error",
			routerType: "multi-tenant",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node2", "shard1", "node2:8080"),
				createTestReplica("node3", "shard1", "node3:8080"),
			}),
			options:        types.ReadRoutingPlanExecutorOptions{StopOnError: true},
			shardName:      "shard1",
			expectedCalls:  map[string]int{"node2": 1, "node3": 0},
			expectedErrors: map[string]bool{"node2": true},
			expectedError:  "failed to remotely execute read plan on replica node2 at addr node2:8080: mock error for node node2",
		},
		{
			name:       "single-tenant: local executor error without stop on error",
			routerType: "single-tenant",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node1", "shard1", "node1:8080"),
				createTestReplica("node2", "shard1", "node2:8080"),
			}),
			options:        types.ReadRoutingPlanExecutorOptions{StopOnError: false},
			shardName:      "shard1",
			expectedCalls:  map[string]int{"node1": 1, "node2": 1},
			expectedErrors: map[string]bool{"node1": true},
		},
		{
			name:       "multi-tenant: local executor error without stop on error",
			routerType: "multi-tenant",
			plan: createTestPlan([]types.Replica{
				createTestReplica("node1", "shard1", "node1:8080"),
				createTestReplica("node2", "shard1", "node2:8080"),
			}),
			options:        types.ReadRoutingPlanExecutorOptions{StopOnError: false},
			shardName:      "shard1",
			expectedCalls:  map[string]int{"node1": 1, "node2": 1},
			expectedErrors: map[string]bool{"node1": true},
		},
		{
			name:          "single-tenant: empty replica set",
			routerType:    "single-tenant",
			plan:          createTestPlan([]types.Replica{}),
			options:       types.ReadRoutingPlanExecutorOptions{StopOnError: true},
			shardName:     "shard1",
			expectedCalls: map[string]int{},
		},
		{
			name:          "multi-tenant: empty replica set",
			routerType:    "multi-tenant",
			plan:          createTestPlan([]types.Replica{}),
			options:       types.ReadRoutingPlanExecutorOptions{StopOnError: true},
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

			// Create router (we'll use a mock since we can't easily create real routers in tests)
			r := createTestRouter(tt.routerType)

			// Execute the function
			err := r.ExecuteForEachReplicaOfShard(tt.plan, tt.options, tt.shardName, localExecutor, remoteExecutor)

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
