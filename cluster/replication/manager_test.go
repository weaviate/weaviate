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
	"testing"

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
			parser := fakes.NewMockParser()
			parser.On("ParseClass", mock.Anything).Return(nil)
			schemaManager := schema.NewSchemaManager("test-node", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())
			schemaReader := schemaManager.NewSchemaReader()
			// TODO mock copier
			manager := replication.NewManager(logrus.New(), schemaReader, nil)
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
