//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package replication_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/sharding"
)

var ErrNotFound = errors.New("not found")

func TestManager_Replicate(t *testing.T) {
	tests := []struct {
		name          string
		schemaSetup   func(*testing.T, *schema.SchemaManager) error
		request       *api.ReplicationReplicateShardRequest
		expectedError error
	}{
		{
			name: "valid replication request",
			schemaSetup: func(t *testing.T, s *schema.SchemaManager) error {
				return s.AddClass(
					buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
						Class: &models.Class{Class: "TestCollection", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
						State: &sharding.State{
							Physical: map[string]sharding.Physical{"shard1": {BelongsToNodes: []string{"node1"}}},
						},
					}), "node1", true, false)
			},
			request: &api.ReplicationReplicateShardRequest{
				SourceCollection: "TestCollection",
				SourceShard:      "shard1",
				SourceNode:       "node1",
				TargetNode:       "node2",
			},
			expectedError: nil,
		},
		{
			name: "class not found",
			request: &api.ReplicationReplicateShardRequest{
				SourceCollection: "NonExistentCollection",
				SourceShard:      "shard1",
				SourceNode:       "node1",
				TargetNode:       "node2",
			},
			expectedError: replication.ErrClassNotFound,
		},
		{
			name: "source shard not found",
			schemaSetup: func(t *testing.T, s *schema.SchemaManager) error {
				return s.AddClass(
					buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
						Class: &models.Class{Class: "TestCollection", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
						State: &sharding.State{
							Physical: map[string]sharding.Physical{"shard2": {BelongsToNodes: []string{"node1"}}},
						},
					}), "node1", true, false)
			},
			request: &api.ReplicationReplicateShardRequest{
				SourceCollection: "TestCollection",
				SourceShard:      "NonExistentShard",
				SourceNode:       "node1",
				TargetNode:       "node2",
			},
			expectedError: replication.ErrShardNotFound,
		},
		{
			name: "source node not found",
			schemaSetup: func(t *testing.T, s *schema.SchemaManager) error {
				return s.AddClass(
					buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
						Class: &models.Class{Class: "TestCollection", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
						State: &sharding.State{
							Physical: map[string]sharding.Physical{"shard1": {BelongsToNodes: []string{"node1"}}},
						},
					}), "node1", true, false)
			},
			request: &api.ReplicationReplicateShardRequest{
				SourceCollection: "TestCollection",
				SourceShard:      "shard1",
				SourceNode:       "node4",
				TargetNode:       "node2",
			},
			expectedError: replication.ErrNodeNotFound,
		},
		{
			name: "target node already has shard",
			schemaSetup: func(t *testing.T, s *schema.SchemaManager) error {
				return s.AddClass(
					buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
						Class: &models.Class{Class: "TestCollection", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
						State: &sharding.State{
							Physical: map[string]sharding.Physical{"shard1": {BelongsToNodes: []string{"node1"}}},
						},
					}), "node1", true, false)
			},
			request: &api.ReplicationReplicateShardRequest{
				SourceCollection: "TestCollection",
				SourceShard:      "shard1",
				SourceNode:       "node1",
				TargetNode:       "node1",
			},
			expectedError: replication.ErrAlreadyExists,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			// Setup
			reg := prometheus.NewPedanticRegistry()
			parser := fakes.NewMockParser()
			parser.On("ParseClass", mock.Anything).Return(nil)
			schemaManager := schema.NewSchemaManager("test-node", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())
			schemaReader := schemaManager.NewSchemaReader()
			// TODO mock copier
			manager := replication.NewManager(logrus.New(), schemaReader, nil, reg)
			if tt.schemaSetup != nil {
				tt.schemaSetup(t, schemaManager)
			}

			// Create ApplyRequest
			subCommand, _ := json.Marshal(tt.request)
			applyRequest := &api.ApplyRequest{
				SubCommand: subCommand,
			}

			// Execute
			err := manager.Replicate(0, applyRequest)

			// Assert
			if tt.expectedError != nil {
				assert.ErrorAs(t, err, &tt.expectedError)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestManager_MetricsTracking(t *testing.T) {
	const metricName = "weaviate_replication_operation_fsm_ops_by_state"
	t.Run("one replication operation with two state transitions", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		parser := fakes.NewMockParser()
		parser.On("ParseClass", mock.Anything).Return(nil)
		schemaManager := schema.NewSchemaManager("test-node", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())
		schemaReader := schemaManager.NewSchemaReader()
		manager := replication.NewManager(logrus.New(), schemaReader, nil, reg)
		err := schemaManager.AddClass(buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
			Class: &models.Class{Class: "TestCollection", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
			State: &sharding.State{
				Physical: map[string]sharding.Physical{"shard1": {BelongsToNodes: []string{"node1"}}},
			},
		}), "node1", true, false)
		require.NoError(t, err, "error while adding class: %v", err)

		// Create replication request
		subCommand, err := json.Marshal(&api.ReplicationReplicateShardRequest{
			SourceCollection: "TestCollection",
			SourceShard:      "shard1",
			SourceNode:       "node1",
			TargetNode:       "node2",
		})
		require.NoErrorf(t, err, "error while marshalling a replication request: %v", err)

		err = manager.Replicate(0, &api.ApplyRequest{
			SubCommand: subCommand,
		})
		require.NoErrorf(t, err, "error while starting a replication operation: %v", err)

		assertGaugeValues(t, reg, metricName, map[api.ShardReplicationState]float64{
			api.REGISTERED: 1,
		})

		// Update replication state to 'HYDRATING'
		subCommand, err = json.Marshal(&api.ReplicationUpdateOpStateRequest{
			Version: 0,
			Id:      0,
			State:   api.HYDRATING,
		})
		require.NoErrorf(t, err, "error while marshalling a replication state change operation: %v", err)

		err = manager.UpdateReplicateOpState(&api.ApplyRequest{
			SubCommand: subCommand,
		})
		require.NoErrorf(t, err, "error while updating replication state: %v", err)

		assertGaugeValues(t, reg, metricName, map[api.ShardReplicationState]float64{
			api.REGISTERED: 0,
			api.HYDRATING:  1,
		})

		// Update replication status to 'DEHYDRATING'
		subCommand, err = json.Marshal(&api.ReplicationUpdateOpStateRequest{
			Version: 0,
			Id:      0,
			State:   api.DEHYDRATING,
		})
		require.NoErrorf(t, err, "error while marshalling a replication state change operation: %v", err)

		err = manager.UpdateReplicateOpState(&api.ApplyRequest{
			SubCommand: subCommand,
		})
		require.NoErrorf(t, err, "error while updating replication state: %v", err)

		assertGaugeValues(t, reg, metricName, map[api.ShardReplicationState]float64{
			api.REGISTERED:  0,
			api.HYDRATING:   0,
			api.DEHYDRATING: 1,
		})
	})

	t.Run("two replication operations with different state transitions", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		parser := fakes.NewMockParser()
		parser.On("ParseClass", mock.Anything).Return(nil)
		schemaManager := schema.NewSchemaManager("test-node", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())
		schemaReader := schemaManager.NewSchemaReader()
		manager := replication.NewManager(logrus.New(), schemaReader, nil, reg)
		err := schemaManager.AddClass(buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
			Class: &models.Class{Class: "TestCollection", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
			State: &sharding.State{
				Physical: map[string]sharding.Physical{
					"shard1": {BelongsToNodes: []string{"node1"}},
					"shard2": {BelongsToNodes: []string{"node1"}},
				},
			},
		}), "node1", true, false)
		require.NoError(t, err, "error while adding class: %v", err)

		firstSubCommand, err := json.Marshal(&api.ReplicationReplicateShardRequest{
			SourceCollection: "TestCollection",
			SourceShard:      "shard1",
			SourceNode:       "node1",
			TargetNode:       "node2",
		})
		require.NoErrorf(t, err, "error while marshalling first replication request: %v", err)

		secondSubCommand, err := json.Marshal(&api.ReplicationReplicateShardRequest{
			SourceCollection: "TestCollection",
			SourceShard:      "shard2",
			SourceNode:       "node1",
			TargetNode:       "node3",
		})
		require.NoErrorf(t, err, "error while marshalling second replication request: %v", err)

		err = manager.Replicate(0, &api.ApplyRequest{
			SubCommand: firstSubCommand,
		})
		require.NoErrorf(t, err, "error while starting first replication operation: %v", err)

		err = manager.Replicate(1, &api.ApplyRequest{
			SubCommand: secondSubCommand,
		})
		require.NoErrorf(t, err, "error while starting second replication operation: %v", err)

		assertGaugeValues(t, reg, metricName, map[api.ShardReplicationState]float64{
			api.REGISTERED: 2,
		})

		// Update first operation to 'READY'
		firstStateUpdate, err := json.Marshal(&api.ReplicationUpdateOpStateRequest{
			Version: 0,
			Id:      0,
			State:   api.READY,
		})
		require.NoErrorf(t, err, "error while marshalling first operation state change: %v", err)

		err = manager.UpdateReplicateOpState(&api.ApplyRequest{
			SubCommand: firstStateUpdate,
		})
		require.NoErrorf(t, err, "error while updating first operation state: %v", err)

		// Verify state after first operation state transition
		assertGaugeValues(t, reg, metricName, map[api.ShardReplicationState]float64{
			api.REGISTERED: 1,
			api.READY:      1,
		})

		// Update second operation to 'ABORTED'
		secondStateUpdate, err := json.Marshal(&api.ReplicationUpdateOpStateRequest{
			Version: 0,
			Id:      1,
			State:   api.ABORTED,
		})
		require.NoErrorf(t, err, "error while marshalling second operation state change: %v", err)

		err = manager.UpdateReplicateOpState(&api.ApplyRequest{
			SubCommand: secondStateUpdate,
		})
		require.NoErrorf(t, err, "error while updating second operation state: %v", err)

		// Verify state after second operation state transition
		assertGaugeValues(t, reg, metricName, map[api.ShardReplicationState]float64{
			api.REGISTERED: 0,
			api.READY:      1,
			api.ABORTED:    1,
		})
	})
}

func assertGaugeValues(t *testing.T, reg prometheus.Gatherer, metricName string, expectedMetrics map[api.ShardReplicationState]float64) {
	t.Helper()

	var expectedOutput strings.Builder
	_, _ = fmt.Fprintf(&expectedOutput, "\n# HELP %s Current number of replication operations in each state of the FSM lifecycle\n", metricName)
	_, _ = fmt.Fprintf(&expectedOutput, "# TYPE %s gauge\n", metricName)

	for expectedState, expectedMetricValue := range expectedMetrics {
		_, _ = fmt.Fprintf(&expectedOutput, "%s{state=\"%s\"} %v\n", metricName, expectedState, expectedMetricValue)
	}

	err := testutil.GatherAndCompare(reg, strings.NewReader(expectedOutput.String()), metricName)
	require.NoErrorf(t, err, "error while gathering %s metric: %v", metricName, err)
}

func buildApplyRequest(
	class string,
	cmdType api.ApplyRequest_Type,
	jsonSubCmd interface{},
) *api.ApplyRequest {
	subData, err := json.Marshal(jsonSubCmd)
	if err != nil {
		panic("json.Marshal( " + err.Error())
	}

	cmd := api.ApplyRequest{
		Type:       cmdType,
		Class:      class,
		SubCommand: subData,
	}

	return &cmd
}
