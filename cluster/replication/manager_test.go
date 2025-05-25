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

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
	"github.com/weaviate/weaviate/cluster/replication/types"
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
				Uuid:             uuid4(),
				SourceCollection: "TestCollection",
				SourceShard:      "shard1",
				SourceNode:       "node1",
				TargetNode:       "node2",
				TransferType:     api.COPY.String(),
			},
			expectedError: nil,
		},
		{
			name: "class not found",
			request: &api.ReplicationReplicateShardRequest{
				Uuid:             uuid4(),
				SourceCollection: "NonExistentCollection",
				SourceShard:      "shard1",
				SourceNode:       "node1",
				TargetNode:       "node2",
				TransferType:     api.COPY.String(),
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
				Uuid:             uuid4(),
				SourceCollection: "TestCollection",
				SourceShard:      "NonExistentShard",
				SourceNode:       "node1",
				TargetNode:       "node2",
				TransferType:     api.COPY.String(),
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
				Uuid:             uuid4(),
				SourceCollection: "TestCollection",
				SourceShard:      "shard1",
				SourceNode:       "node4",
				TargetNode:       "node2",
				TransferType:     api.COPY.String(),
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
							Physical: map[string]sharding.Physical{"shard1": {BelongsToNodes: []string{"node1", "node2"}}},
						},
					}), "node1", true, false)
			},
			request: &api.ReplicationReplicateShardRequest{
				Uuid:             uuid4(),
				SourceCollection: "TestCollection",
				SourceShard:      "shard1",
				SourceNode:       "node1",
				TargetNode:       "node2",
				TransferType:     api.COPY.String(),
			},
			expectedError: replication.ErrAlreadyExists,
		},
		{
			name: "source and target are identicals",
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
				Uuid:             uuid4(),
				SourceCollection: "TestCollection",
				SourceShard:      "shard1",
				SourceNode:       "node1",
				TargetNode:       "node1",
				TransferType:     api.COPY.String(),
			},
			expectedError: replication.ErrBadRequest,
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
			manager := replication.NewManager(schemaReader, reg)
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

func TestManager_ReplicateMultipleOps(t *testing.T) {
	tests := []struct {
		name              string
		schemaSetup       func(*testing.T, *schema.SchemaManager) error
		requests          []*api.ReplicationReplicateShardRequest
		expectedLastError error
	}{
		{
			name: "source shard is already moving",
			schemaSetup: func(t *testing.T, s *schema.SchemaManager) error {
				return s.AddClass(
					buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
						Class: &models.Class{Class: "TestCollection", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
						State: &sharding.State{
							Physical: map[string]sharding.Physical{"shard1": {BelongsToNodes: []string{"node1"}}},
						},
					}), "node1", true, false)
			},
			requests: []*api.ReplicationReplicateShardRequest{
				{
					Uuid:             uuid4(),
					SourceCollection: "TestCollection",
					SourceShard:      "shard1",
					SourceNode:       "node1",
					TargetNode:       "node2",
					TransferType:     api.MOVE.String(),
				},
				{
					Uuid:             uuid4(),
					SourceCollection: "TestCollection",
					SourceShard:      "shard1",
					SourceNode:       "node1",
					TargetNode:       "node3",
					TransferType:     api.MOVE.String(),
				},
			},
			expectedLastError: replication.ErrShardAlreadyReplicating,
		},
		{
			name: "source shard can accept multiple copies",
			schemaSetup: func(t *testing.T, s *schema.SchemaManager) error {
				return s.AddClass(
					buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
						Class: &models.Class{Class: "TestCollection", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
						State: &sharding.State{
							Physical: map[string]sharding.Physical{"shard1": {BelongsToNodes: []string{"node1"}}},
						},
					}), "node1", true, false)
			},
			requests: []*api.ReplicationReplicateShardRequest{
				{
					Uuid:             uuid4(),
					SourceCollection: "TestCollection",
					SourceShard:      "shard1",
					SourceNode:       "node1",
					TargetNode:       "node2",
					TransferType:     api.COPY.String(),
				},
				{
					Uuid:             uuid4(),
					SourceCollection: "TestCollection",
					SourceShard:      "shard1",
					SourceNode:       "node1",
					TargetNode:       "node3",
					TransferType:     api.COPY.String(),
				},
			},
			expectedLastError: nil,
		},
		{
			name: "source shard is copying and can't accept a new move",
			schemaSetup: func(t *testing.T, s *schema.SchemaManager) error {
				return s.AddClass(
					buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
						Class: &models.Class{Class: "TestCollection", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
						State: &sharding.State{
							Physical: map[string]sharding.Physical{"shard1": {BelongsToNodes: []string{"node1"}}},
						},
					}), "node1", true, false)
			},
			requests: []*api.ReplicationReplicateShardRequest{
				{
					Uuid:             uuid4(),
					SourceCollection: "TestCollection",
					SourceShard:      "shard1",
					SourceNode:       "node1",
					TargetNode:       "node2",
					TransferType:     api.COPY.String(),
				},
				{
					Uuid:             uuid4(),
					SourceCollection: "TestCollection",
					SourceShard:      "shard1",
					SourceNode:       "node1",
					TargetNode:       "node3",
					TransferType:     api.MOVE.String(),
				},
			},
			expectedLastError: replication.ErrShardAlreadyReplicating,
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
			manager := replication.NewManager(schemaReader, reg)
			if tt.schemaSetup != nil {
				tt.schemaSetup(t, schemaManager)
			}

			for i, request := range tt.requests {
				// Create ApplyRequest
				subCommand, _ := json.Marshal(request)
				applyRequest := &api.ApplyRequest{
					SubCommand: subCommand,
				}

				// Execute
				err := manager.Replicate(uint64(i), applyRequest)
				if i == len(tt.requests)-1 && tt.expectedLastError != nil {
					assert.ErrorAs(t, err, &tt.expectedLastError)
				} else {
					assert.NoError(t, err)
				}
			}
		})
	}
}

func TestManager_UpdateReplicaOpStatusAndRegisterErrors(t *testing.T) {
	type stateChangeAndErrors struct {
		stateChangeRequest       *api.ReplicationUpdateOpStateRequest
		stateChangeExpectedError error

		registerErrorRequests      []*api.ReplicationRegisterErrorRequest
		registerErrorExpectedError []error
	}

	tests := []struct {
		name                 string
		schemaSetup          func(*testing.T, *schema.SchemaManager) error
		replicaRequest       *api.ReplicationReplicateShardRequest
		updateStatusRequests []*stateChangeAndErrors
	}{
		{
			name: "valid state change and no errors",
			schemaSetup: func(t *testing.T, s *schema.SchemaManager) error {
				return s.AddClass(
					buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
						Class: &models.Class{Class: "TestCollection", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
						State: &sharding.State{
							Physical: map[string]sharding.Physical{"shard1": {BelongsToNodes: []string{"node1"}}},
						},
					}), "node1", true, false)
			},
			replicaRequest: &api.ReplicationReplicateShardRequest{
				Uuid:             uuid4(),
				SourceCollection: "TestCollection",
				SourceShard:      "shard1",
				SourceNode:       "node1",
				TargetNode:       "node2",
			},
			updateStatusRequests: []*stateChangeAndErrors{
				{
					stateChangeRequest:    &api.ReplicationUpdateOpStateRequest{Id: 0, State: api.ShardReplicationState(api.HYDRATING)},
					registerErrorRequests: []*api.ReplicationRegisterErrorRequest{},
				},
			},
		},
		{
			name: "valid state change and errors",
			schemaSetup: func(t *testing.T, s *schema.SchemaManager) error {
				return s.AddClass(
					buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
						Class: &models.Class{Class: "TestCollection", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
						State: &sharding.State{
							Physical: map[string]sharding.Physical{"shard1": {BelongsToNodes: []string{"node1"}}},
						},
					}), "node1", true, false)
			},
			replicaRequest: &api.ReplicationReplicateShardRequest{
				Uuid:             uuid4(),
				SourceCollection: "TestCollection",
				SourceShard:      "shard1",
				SourceNode:       "node1",
				TargetNode:       "node2",
			},
			updateStatusRequests: []*stateChangeAndErrors{
				{
					stateChangeRequest: &api.ReplicationUpdateOpStateRequest{Id: 0, State: api.ShardReplicationState(api.HYDRATING)},
					registerErrorRequests: []*api.ReplicationRegisterErrorRequest{
						{Id: 0, Error: "test error"},
						{Id: 0, Error: "test error"},
					},
					registerErrorExpectedError: []error{nil, nil},
				},
			},
		},
		{
			name: "valid state change andinvalid register error",
			schemaSetup: func(t *testing.T, s *schema.SchemaManager) error {
				return s.AddClass(
					buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
						Class: &models.Class{Class: "TestCollection", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
						State: &sharding.State{
							Physical: map[string]sharding.Physical{"shard1": {BelongsToNodes: []string{"node1"}}},
						},
					}), "node1", true, false)
			},
			replicaRequest: &api.ReplicationReplicateShardRequest{
				Uuid:             uuid4(),
				SourceCollection: "TestCollection",
				SourceShard:      "shard1",
				SourceNode:       "node1",
				TargetNode:       "node2",
			},
			updateStatusRequests: []*stateChangeAndErrors{
				{
					stateChangeRequest: &api.ReplicationUpdateOpStateRequest{Id: 0, State: api.ShardReplicationState(api.HYDRATING)},
					registerErrorRequests: []*api.ReplicationRegisterErrorRequest{
						{Id: 1, Error: "test error"},
					},
					registerErrorExpectedError: []error{types.ErrReplicationOperationNotFound, nil},
				},
			},
		},
		{
			name: "multiple state changes and errors",
			schemaSetup: func(t *testing.T, s *schema.SchemaManager) error {
				return s.AddClass(
					buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
						Class: &models.Class{Class: "TestCollection", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
						State: &sharding.State{
							Physical: map[string]sharding.Physical{"shard1": {BelongsToNodes: []string{"node1"}}},
						},
					}), "node1", true, false)
			},
			replicaRequest: &api.ReplicationReplicateShardRequest{
				Uuid:             uuid4(),
				SourceCollection: "TestCollection",
				SourceShard:      "shard1",
				SourceNode:       "node1",
				TargetNode:       "node2",
			},
			updateStatusRequests: []*stateChangeAndErrors{
				{
					stateChangeRequest: &api.ReplicationUpdateOpStateRequest{Id: 0, State: api.ShardReplicationState(api.HYDRATING)},
					registerErrorRequests: []*api.ReplicationRegisterErrorRequest{
						{Id: 0, Error: "test error"},
						{Id: 0, Error: "test error"},
					},
					registerErrorExpectedError: []error{nil, nil},
				},
				{
					stateChangeRequest: &api.ReplicationUpdateOpStateRequest{Id: 0, State: api.ShardReplicationState(api.FINALIZING)},
					registerErrorRequests: []*api.ReplicationRegisterErrorRequest{
						{Id: 0, Error: "test error"},
						{Id: 0, Error: "test error"},
					},
					registerErrorExpectedError: []error{nil, nil},
				},
				{
					stateChangeRequest:    &api.ReplicationUpdateOpStateRequest{Id: 0, State: api.ShardReplicationState(api.REGISTERED)},
					registerErrorRequests: []*api.ReplicationRegisterErrorRequest{},
				},
			},
		},
		{
			name: "invalid state change",
			schemaSetup: func(t *testing.T, s *schema.SchemaManager) error {
				return s.AddClass(
					buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
						Class: &models.Class{Class: "TestCollection", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
						State: &sharding.State{
							Physical: map[string]sharding.Physical{"shard1": {BelongsToNodes: []string{"node1"}}},
						},
					}), "node1", true, false)
			},
			replicaRequest: &api.ReplicationReplicateShardRequest{
				Uuid:             uuid4(),
				SourceCollection: "TestCollection",
				SourceShard:      "shard1",
				SourceNode:       "node1",
				TargetNode:       "node2",
			},
			updateStatusRequests: []*stateChangeAndErrors{
				{
					stateChangeRequest:       &api.ReplicationUpdateOpStateRequest{Id: 1, State: api.ShardReplicationState(api.REGISTERED)},
					stateChangeExpectedError: types.ErrReplicationOperationNotFound,
				},
			},
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
			manager := replication.NewManager(schemaReader, reg)
			if tt.schemaSetup != nil {
				tt.schemaSetup(t, schemaManager)
			}

			// Create ApplyRequest
			subCommand, _ := json.Marshal(tt.replicaRequest)
			applyRequest := &api.ApplyRequest{
				SubCommand: subCommand,
			}

			// Execute
			err := manager.Replicate(0, applyRequest)
			require.NoError(t, err)

			expectedFinalState := replication.NewShardReplicationStatus(api.REGISTERED)

			for _, req := range tt.updateStatusRequests {
				subCommand, _ := json.Marshal(req.stateChangeRequest)
				applyRequest = &api.ApplyRequest{
					SubCommand: subCommand,
				}
				err = manager.UpdateReplicateOpState(applyRequest)
				if req.stateChangeExpectedError != nil {
					assert.ErrorAs(t, err, &req.stateChangeExpectedError)
				} else {
					expectedFinalState.ChangeState(req.stateChangeRequest.State)
					assert.NoError(t, err)
				}

				for i, errReq := range req.registerErrorRequests {
					expectedErr := req.registerErrorExpectedError[i]

					subCommand, _ := json.Marshal(errReq)
					applyRequest = &api.ApplyRequest{
						SubCommand: subCommand,
					}
					err = manager.RegisterError(applyRequest)
					if expectedErr != nil {
						assert.ErrorAs(t, err, &expectedErr)
					} else {
						assert.NoError(t, err)
						expectedFinalState.AddError(errReq.Error)
					}
				}
			}

			subCommand, _ = json.Marshal(&api.ReplicationDetailsRequest{Uuid: tt.replicaRequest.Uuid})
			queryRequest := &api.QueryRequest{
				Type:       api.QueryRequest_TYPE_GET_REPLICATION_DETAILS,
				SubCommand: subCommand,
			}
			resp, err := manager.GetReplicationDetailsByReplicationId(queryRequest)
			assert.NoError(t, err)

			statusResp := api.ReplicationDetailsResponse{}
			err = json.Unmarshal(resp, &statusResp)
			assert.NoError(t, err)
			assert.Equal(t, expectedFinalState.GetCurrent().ToAPIFormat(), statusResp.Status)
			assert.Equal(t, expectedFinalState.GetHistory().ToAPIFormat(), statusResp.StatusHistory)
		})
	}
}

func TestManager_SnapshotRestore(t *testing.T) {
	UUID1 := uuid4()
	UUID2 := uuid4()
	tests := []struct {
		name                   string
		schemaSetup            func(*testing.T, *schema.SchemaManager) error
		uuids                  []strfmt.UUID
		snapshotRequests       []*api.ApplyRequest
		nonSnapshottedRequests []*api.ApplyRequest
	}{
		{
			name: "snapshot and restore data with non snapshotted data",
			schemaSetup: func(t *testing.T, s *schema.SchemaManager) error {
				return s.AddClass(
					buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
						Class: &models.Class{Class: "TestCollection", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
						State: &sharding.State{
							Physical: map[string]sharding.Physical{
								"shard1": {BelongsToNodes: []string{"node1"}},
								"shard2": {BelongsToNodes: []string{"node1"}},
							},
						},
					}), "node1", true, false)
			},
			snapshotRequests: []*api.ApplyRequest{
				buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_REPLICATION_REPLICATE, api.ReplicationReplicateShardRequest{
					Uuid:             UUID1,
					SourceCollection: "TestCollection",
					SourceShard:      "shard1",
					SourceNode:       "node1",
					TargetNode:       "node2",
					TransferType:     api.COPY.String(),
				}),
				buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_REPLICATION_REPLICATE_REGISTER_ERROR, api.ReplicationRegisterErrorRequest{Id: 0, Error: "test error"}),
			},
			nonSnapshottedRequests: []*api.ApplyRequest{
				buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_REPLICATION_REPLICATE, api.ReplicationReplicateShardRequest{
					Uuid:             UUID2,
					SourceCollection: "TestCollection",
					SourceShard:      "shard2",
					SourceNode:       "node1",
					TargetNode:       "node2",
					TransferType:     api.COPY.String(),
				}),
				buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_REPLICATION_REPLICATE_REGISTER_ERROR, api.ReplicationRegisterErrorRequest{Id: 1, Error: "test error"}),
			},
		},
		{
			name: "snapshot and restore no data",
			schemaSetup: func(t *testing.T, s *schema.SchemaManager) error {
				return s.AddClass(
					buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
						Class: &models.Class{Class: "TestCollection", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
						State: &sharding.State{
							Physical: map[string]sharding.Physical{
								"shard1": {BelongsToNodes: []string{"node1"}},
								"shard2": {BelongsToNodes: []string{"node1"}},
							},
						},
					}), "node1", true, false)
			},
			snapshotRequests:       []*api.ApplyRequest{},
			nonSnapshottedRequests: []*api.ApplyRequest{},
		},
		{
			name: "snapshot and restore latest data",
			schemaSetup: func(t *testing.T, s *schema.SchemaManager) error {
				return s.AddClass(
					buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
						Class: &models.Class{Class: "TestCollection", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
						State: &sharding.State{
							Physical: map[string]sharding.Physical{
								"shard1": {BelongsToNodes: []string{"node1"}},
								"shard2": {BelongsToNodes: []string{"node1"}},
							},
						},
					}), "node1", true, false)
			},
			snapshotRequests: []*api.ApplyRequest{
				buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_REPLICATION_REPLICATE, api.ReplicationReplicateShardRequest{
					Uuid:             UUID1,
					SourceCollection: "TestCollection",
					SourceShard:      "shard1",
					SourceNode:       "node1",
					TargetNode:       "node2",
					TransferType:     api.MOVE.String(),
				}),
				buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_REPLICATION_REPLICATE_REGISTER_ERROR, api.ReplicationRegisterErrorRequest{Id: 0, Error: "test error"}),
				buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_REPLICATION_REPLICATE, api.ReplicationReplicateShardRequest{
					Uuid:             UUID2,
					SourceCollection: "TestCollection",
					SourceShard:      "shard2",
					SourceNode:       "node1",
					TargetNode:       "node2",
					TransferType:     api.COPY.String(),
				}),
				buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_REPLICATION_REPLICATE_REGISTER_ERROR, api.ReplicationRegisterErrorRequest{Id: 1, Error: "test error"}),
			},
			nonSnapshottedRequests: []*api.ApplyRequest{},
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
			manager := replication.NewManager(schemaReader, reg)
			if tt.schemaSetup != nil {
				tt.schemaSetup(t, schemaManager)
			}

			var logIndex uint64
			// Write data
			for _, req := range tt.snapshotRequests {
				switch req.Type {
				case api.ApplyRequest_TYPE_REPLICATION_REPLICATE:
					// Execute
					err := manager.Replicate(logIndex, req)
					assert.NoError(t, err)
					logIndex++
				case api.ApplyRequest_TYPE_REPLICATION_REPLICATE_REGISTER_ERROR:
					var originalReq api.ReplicationRegisterErrorRequest
					err := json.Unmarshal(req.SubCommand, &originalReq)
					require.NoError(t, err)
					err = manager.RegisterError(req)
					assert.NoError(t, err)
				default:
					t.Fatalf("unknown apply request type: %v", req.Type)
				}
			}

			// Do the snapshot/restore routine
			bytes, err := manager.Snapshot()
			require.NoError(t, err)
			require.NotNil(t, bytes)

			// Write data that will not be snapshotted
			for _, req := range tt.nonSnapshottedRequests {
				switch req.Type {
				case api.ApplyRequest_TYPE_REPLICATION_REPLICATE:
					// Execute
					err := manager.Replicate(logIndex, req)
					assert.NoError(t, err)
				case api.ApplyRequest_TYPE_REPLICATION_REPLICATE_REGISTER_ERROR:
					var originalReq api.ReplicationRegisterErrorRequest
					err := json.Unmarshal(req.SubCommand, &originalReq)
					require.NoError(t, err)
					err = manager.RegisterError(req)
					assert.NoError(t, err)
				default:
					t.Fatalf("unknown apply request type: %v", req.Type)
				}
				logIndex++
			}

			err = manager.Restore(bytes)
			require.NoError(t, err)

			// Ensure snapshotted data is here
			logIndex = 0
			for _, req := range tt.snapshotRequests {
				switch req.Type {
				case api.ApplyRequest_TYPE_REPLICATION_REPLICATE:
					var originalReq api.ReplicationReplicateShardRequest
					err = json.Unmarshal(req.SubCommand, &originalReq)
					require.NoError(t, err)

					// Create QueryRequest
					subCommand, _ := json.Marshal(&api.ReplicationDetailsRequest{Uuid: originalReq.Uuid})
					queryRequest := &api.QueryRequest{
						Type:       api.QueryRequest_TYPE_GET_REPLICATION_DETAILS,
						SubCommand: subCommand,
					}

					// Execute
					bytes, err := manager.GetReplicationDetailsByReplicationId(queryRequest)
					require.NoError(t, err)
					require.NotNil(t, bytes)

					var resp api.ReplicationDetailsResponse
					err = json.Unmarshal(bytes, &resp)
					require.NoError(t, err)
					require.Equal(t, resp.Uuid, originalReq.Uuid)
					require.Equal(t, resp.Id, logIndex)
					require.Equal(t, originalReq.SourceCollection, resp.Collection)
					require.Equal(t, originalReq.SourceShard, resp.ShardId)
					require.Equal(t, originalReq.SourceNode, resp.SourceNodeId)
					require.Equal(t, originalReq.TargetNode, resp.TargetNodeId)
					require.Equal(t, originalReq.TransferType, resp.TransferType)
					logIndex++
				case api.ApplyRequest_TYPE_REPLICATION_REPLICATE_REGISTER_ERROR:
					originalReq := api.ReplicationRegisterErrorRequest{}
					err = json.Unmarshal(req.SubCommand, &originalReq)
					require.NoError(t, err)

					uuid, err := manager.GetReplicationOpUUIDFromId(originalReq.Id)
					require.NoError(t, err)

					// Create QueryRequest
					subCommand, _ := json.Marshal(&api.ReplicationDetailsRequest{Uuid: uuid})
					queryRequest := &api.QueryRequest{
						Type:       api.QueryRequest_TYPE_GET_REPLICATION_DETAILS,
						SubCommand: subCommand,
					}
					// Execute
					bytes, err := manager.GetReplicationDetailsByReplicationId(queryRequest)
					require.NoError(t, err)
					require.NotNil(t, bytes)

					var resp api.ReplicationDetailsResponse
					err = json.Unmarshal(bytes, &resp)
					require.NoError(t, err)
					require.Equal(t, resp.Uuid, uuid)
					require.Equal(t, resp.Id, originalReq.Id)
					require.Equal(t, api.ShardReplicationState(resp.Status.State), api.REGISTERED)
					require.Equal(t, resp.Status.Errors, []string{originalReq.Error})
				default:
					t.Fatalf("unknown apply request type: %v", req.Type)
				}
			}

			// Ensure non snapshotted data is absent
			for _, req := range tt.nonSnapshottedRequests {
				switch req.Type {
				case api.ApplyRequest_TYPE_REPLICATION_REPLICATE:
					originalReq := api.ReplicationReplicateShardRequest{}
					err = json.Unmarshal(req.SubCommand, &originalReq)
					require.NoError(t, err)

					// Create QueryRequest
					subCommand, _ := json.Marshal(&api.ReplicationDetailsRequest{Uuid: originalReq.Uuid})
					queryRequest := &api.QueryRequest{
						Type:       api.QueryRequest_TYPE_GET_REPLICATION_DETAILS,
						SubCommand: subCommand,
					}

					// Execute
					_, err := manager.GetReplicationDetailsByReplicationId(queryRequest)
					require.Error(t, err)
					logIndex++
				case api.ApplyRequest_TYPE_REPLICATION_REPLICATE_REGISTER_ERROR:
					originalReq := api.ReplicationRegisterErrorRequest{}
					err = json.Unmarshal(req.SubCommand, &originalReq)
					require.NoError(t, err)

					_, err = manager.GetReplicationOpUUIDFromId(originalReq.Id)
					require.Error(t, err)
				default:
					t.Fatalf("unknown apply request type: %v", req.Type)
				}
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
		manager := replication.NewManager(schemaReader, reg)
		err := schemaManager.AddClass(buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
			Class: &models.Class{Class: "TestCollection", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
			State: &sharding.State{
				Physical: map[string]sharding.Physical{"shard1": {BelongsToNodes: []string{"node1"}}},
			},
		}), "node1", true, false)
		require.NoError(t, err, "error while adding class: %v", err)

		// Create replication request
		subCommand, err := json.Marshal(&api.ReplicationReplicateShardRequest{
			Uuid:             uuid4(),
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
		manager := replication.NewManager(schemaReader, reg)
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
			Uuid:             uuid4(),
			SourceCollection: "TestCollection",
			SourceShard:      "shard1",
			SourceNode:       "node1",
			TargetNode:       "node2",
			TransferType:     api.COPY.String(),
		})
		require.NoErrorf(t, err, "error while marshalling first replication request: %v", err)

		secondSubCommand, err := json.Marshal(&api.ReplicationReplicateShardRequest{
			Uuid:             uuid4(),
			SourceCollection: "TestCollection",
			SourceShard:      "shard2",
			SourceNode:       "node1",
			TargetNode:       "node3",
			TransferType:     api.COPY.String(),
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
			State:   api.CANCELLED,
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
			api.CANCELLED:  1,
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

func uuid4() strfmt.UUID {
	id, err := uuid.NewRandom()
	if err != nil {
		panic(fmt.Sprintf("failed to generate Uuid: %v", err))
	}
	return strfmt.UUID(id.String())
}

func TestReplicationFSM_HasOngoingReplication(t *testing.T) {
	type hasOngoingReplicationParams struct {
		collection string
		shard      string
		replica    string
		expected   bool
	}

	tests := []struct {
		name                        string
		status                      api.ShardReplicationState
		hasOngoingReplicationParams []hasOngoingReplicationParams
		expectedError               error
	}{
		{
			name:   "op is REGISTERED",
			status: api.REGISTERED,
			hasOngoingReplicationParams: []hasOngoingReplicationParams{
				{
					collection: "TestCollection",
					shard:      "shard1",
					replica:    "node1",
					expected:   true,
				},
				{
					collection: "non-existing-collection",
					shard:      "shard1",
					replica:    "node1",
					expected:   false,
				},
				{
					collection: "TestCollection",
					shard:      "non-existing-shard",
					replica:    "node1",
					expected:   false,
				},
				{
					collection: "TestCollection",
					shard:      "shard1",
					replica:    "non-existing-replica",
					expected:   false,
				},
			},
			expectedError: nil,
		},
		{
			name:   "op is HYDRATING",
			status: api.HYDRATING,
			hasOngoingReplicationParams: []hasOngoingReplicationParams{
				{
					collection: "TestCollection",
					shard:      "shard1",
					replica:    "node1",
					expected:   true,
				},
				{
					collection: "non-existing-collection",
					shard:      "shard1",
					replica:    "node1",
					expected:   false,
				},
				{
					collection: "TestCollection",
					shard:      "non-existing-shard",
					replica:    "node1",
					expected:   false,
				},
				{
					collection: "TestCollection",
					shard:      "shard1",
					replica:    "non-existing-replica",
					expected:   false,
				},
			},
			expectedError: nil,
		},
		{
			name:   "op is DEHYDRATING",
			status: api.DEHYDRATING,
			hasOngoingReplicationParams: []hasOngoingReplicationParams{
				{
					collection: "TestCollection",
					shard:      "shard1",
					replica:    "node1",
					expected:   true,
				},
				{
					collection: "non-existing-collection",
					shard:      "shard1",
					replica:    "node1",
					expected:   false,
				},
				{
					collection: "TestCollection",
					shard:      "non-existing-shard",
					replica:    "node1",
					expected:   false,
				},
				{
					collection: "TestCollection",
					shard:      "shard1",
					replica:    "non-existing-replica",
					expected:   false,
				},
			},
			expectedError: nil,
		},
		{
			name:   "op is FINALIZING",
			status: api.FINALIZING,
			hasOngoingReplicationParams: []hasOngoingReplicationParams{
				{
					collection: "TestCollection",
					shard:      "shard1",
					replica:    "node1",
					expected:   true,
				},
				{
					collection: "non-existing-collection",
					shard:      "shard1",
					replica:    "node1",
					expected:   false,
				},
				{
					collection: "TestCollection",
					shard:      "non-existing-shard",
					replica:    "node1",
					expected:   false,
				},
				{
					collection: "TestCollection",
					shard:      "shard1",
					replica:    "non-existing-replica",
					expected:   false,
				},
			},
			expectedError: nil,
		},
		{
			name:   "op is CANCELLED",
			status: api.CANCELLED,
			hasOngoingReplicationParams: []hasOngoingReplicationParams{
				{
					collection: "TestCollection",
					shard:      "shard1",
					replica:    "node1",
					expected:   false,
				},
				{
					collection: "non-existing-collection",
					shard:      "shard1",
					replica:    "node1",
					expected:   false,
				},
				{
					collection: "TestCollection",
					shard:      "non-existing-shard",
					replica:    "node1",
					expected:   false,
				},
				{
					collection: "TestCollection",
					shard:      "shard1",
					replica:    "non-existing-replica",
					expected:   false,
				},
			},
			expectedError: nil,
		},
		{
			name:   "op is READY",
			status: api.READY,
			hasOngoingReplicationParams: []hasOngoingReplicationParams{
				{
					collection: "TestCollection",
					shard:      "shard1",
					replica:    "node1",
					expected:   false,
				},
				{
					collection: "non-existing-collection",
					shard:      "shard1",
					replica:    "node1",
					expected:   false,
				},
				{
					collection: "TestCollection",
					shard:      "non-existing-shard",
					replica:    "node1",
					expected:   false,
				},
				{
					collection: "TestCollection",
					shard:      "shard1",
					replica:    "non-existing-replica",
					expected:   false,
				},
			},
			expectedError: nil,
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
			manager := replication.NewManager(schemaReader, reg)
			schemaManager.AddClass(
				buildApplyRequest("TestCollection", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{
					Class: &models.Class{Class: "TestCollection", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false}},
					State: &sharding.State{
						Physical: map[string]sharding.Physical{"shard1": {BelongsToNodes: []string{"node1"}}},
					},
				}), "node1", true, false)

			// Create ApplyRequest
			subCommand, _ := json.Marshal(&api.ReplicationReplicateShardRequest{
				Uuid:             uuid4(),
				SourceCollection: "TestCollection",
				SourceShard:      "shard1",
				SourceNode:       "node1",
				TargetNode:       "node2",
				TransferType:     api.COPY.String(),
			})
			applyRequest := &api.ApplyRequest{
				SubCommand: subCommand,
			}

			// Execute
			err := manager.Replicate(0, applyRequest)
			assert.NoError(t, err)

			manager.GetReplicationFSM().UpdateReplicationOpStatus(&api.ReplicationUpdateOpStateRequest{
				Id:      0,
				Version: 0,
				State:   tt.status,
			})

			for _, param := range tt.hasOngoingReplicationParams {
				actual := manager.GetReplicationFSM().HasOngoingReplication(param.collection, param.shard, param.replica)
				assert.Equal(t, param.expected, actual)
			}
		})
	}
}
