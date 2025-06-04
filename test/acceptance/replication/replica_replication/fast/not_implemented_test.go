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

package replication

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const UUID = strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168242")

type ReplicationNotImplementedTestSuite struct {
	suite.Suite
	compose *docker.DockerCompose
	down    func()
}

func (suite *ReplicationNotImplementedTestSuite) SetupSuite() {
	t := suite.T()
	// t.Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")

	mainCtx := context.Background()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("REPLICA_MOVEMENT_MINIMUM_ASYNC_WAIT", "5s").
		WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "false").
		Start(mainCtx)
	require.Nil(t, err)
	suite.compose = compose
	suite.down = func() {
		if err := compose.Terminate(mainCtx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}
}

func (suite *ReplicationNotImplementedTestSuite) TearDownSuite() {
	if suite.down != nil {
		suite.down()
	}
}

func TestReplicationNotImplementedTestSuite(t *testing.T) {
	suite.Run(t, new(ReplicationNotImplementedTestSuite))
}

func (suite *ReplicationNotImplementedTestSuite) TestReplicationNotImplemented() {
	t := suite.T()

	helper.SetupClient(suite.compose.GetWeaviate().URI())

	t.Run("POST /replication/replicate", func(t *testing.T) {
		_, err := helper.Client(t).Replication.Replicate(replication.NewReplicateParams().WithBody(&models.ReplicationReplicateReplicaRequest{
			CollectionID:        string_("test-collection"),
			DestinationNodeName: string_("dest-node"),
			ShardID:             string_("test-shard"),
			SourceNodeName:      string_("src-node"),
		}), nil)
		require.IsType(t, &replication.ReplicateNotImplemented{}, err, fmt.Sprintf("Expected NotImplemented error for replicate but got %v", err))
	})

	t.Run("DELETE /replication/replicate", func(t *testing.T) {
		_, err := helper.Client(t).Replication.DeleteAllReplications(replication.NewDeleteAllReplicationsParams(), nil)
		require.IsType(t, &replication.DeleteAllReplicationsNotImplemented{}, err, fmt.Sprintf("Expected NotImplemented error for delete all replications but got %v", err))
	})

	t.Run("GET /replication/replicate/{id}", func(t *testing.T) {
		_, err := helper.Client(t).Replication.ReplicationDetails(replication.NewReplicationDetailsParams().WithID(UUID), nil)
		require.IsType(t, &replication.ReplicationDetailsNotImplemented{}, err, fmt.Sprintf("Expected NotImplemented error for replication details but got %v", err))
	})

	t.Run("DELETE /replication/replicate/{id}", func(t *testing.T) {
		_, err := helper.Client(t).Replication.DeleteReplication(replication.NewDeleteReplicationParams().WithID(UUID), nil)
		require.IsType(t, &replication.DeleteReplicationNotImplemented{}, err, fmt.Sprintf("Expected NotImplemented error for delete replication but got %v", err))
	})

	t.Run("GET /replication/replicate/list", func(t *testing.T) {
		_, err := helper.Client(t).Replication.ListReplication(replication.NewListReplicationParams(), nil)
		require.IsType(t, &replication.ListReplicationNotImplemented{}, err, fmt.Sprintf("Expected NotImplemented error for list replication but got %v", err))
	})

	t.Run("POST /replication/replicate/{id}/cancel", func(t *testing.T) {
		_, err := helper.Client(t).Replication.CancelReplication(replication.NewCancelReplicationParams().WithID(UUID), nil)
		require.IsType(t, &replication.CancelReplicationNotImplemented{}, err, fmt.Sprintf("Expected NotImplemented error for cancel replication but got %v", err))
	})

	t.Run("GET /replication/sharding-state", func(t *testing.T) {
		_, err := helper.Client(t).Replication.GetCollectionShardingState(replication.NewGetCollectionShardingStateParams(), nil)
		require.IsType(t, &replication.GetCollectionShardingStateNotImplemented{}, err, fmt.Sprintf("Expected NotImplemented error for get collection sharding state but got %v", err))
	})
}

func string_(s string) *string {
	return &s
}
