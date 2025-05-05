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

package replica_replication

import (
	"context"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func (suite *ReplicaReplicationTestSuite) TestReplicationReplicateEndpoints() {
	t := suite.T()
	mainCtx := context.Background()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		Start(mainCtx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(mainCtx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	_, cancel := context.WithTimeout(mainCtx, 5*time.Minute)
	defer cancel()

	helper.SetupClient(compose.GetWeaviate().URI())

	paragraphClass := articles.ParagraphsClass()

	t.Run("create schema", func(t *testing.T) {
		helper.DeleteClass(t, paragraphClass.Class)
		helper.CreateClass(t, paragraphClass)
	})

	var id strfmt.UUID

	t.Run("create replication operation", func(t *testing.T) {
		created, err := helper.Client(t).Replication.Replicate(replication.NewReplicateParams().WithBody(getRequest(t, paragraphClass.Class)), nil)
		require.Nil(t, err)
		require.NotNil(t, created)
		require.NotNil(t, created.Payload)
		require.NotNil(t, created.Payload.ID)
		id = *created.Payload.ID
	})

	t.Run("get replication operation", func(t *testing.T) {
		details, err := helper.Client(t).Replication.ReplicationDetails(replication.NewReplicationDetailsParams().WithID(id), nil)
		require.Nil(t, err)
		require.NotNil(t, details)
		require.NotNil(t, details.Payload)
		require.NotNil(t, details.Payload.ID)
		require.Equal(t, id, *details.Payload.ID)
	})

	t.Run("get non-existing replication operation", func(t *testing.T) {
		_, err := helper.Client(t).Replication.ReplicationDetails(replication.NewReplicationDetailsParams().WithID(strfmt.UUID(uuid.New().String())), nil)
		require.NotNil(t, err)
		require.IsType(t, replication.NewReplicationDetailsNotFound(), err)
	})

	t.Run("cancel replication operation", func(t *testing.T) {
		cancelled, err := helper.Client(t).Replication.CancelReplication(replication.NewCancelReplicationParams().WithID(id), nil)
		require.Nil(t, err)
		require.NotNil(t, cancelled)
	})

	t.Run("wait for replication operation to be cancelled", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			details, err := helper.Client(t).Replication.ReplicationDetails(replication.NewReplicationDetailsParams().WithID(id), nil)
			require.Nil(t, err)
			assert.Equal(ct, string(api.CANCELLED), details.Payload.Status.State)
		}, 30*time.Second, 1*time.Second, "replication operation should be cancelled")
	})

	t.Run("delete replication operation", func(t *testing.T) {
		deleted, err := helper.Client(t).Replication.DeleteReplication(replication.NewDeleteReplicationParams().WithID(id), nil)
		require.Nil(t, err)
		require.NotNil(t, deleted)
	})

	t.Run("wait for replication operation to be deleted", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			_, err := helper.Client(t).Replication.ReplicationDetails(replication.NewReplicationDetailsParams().WithID(id), nil)
			require.NotNil(ct, err)
			assert.IsType(ct, replication.NewReplicationDetailsNotFound(), err)
		}, 30*time.Second, 1*time.Second, "replication operation should be deleted")
	})
}

func getRequest(t *testing.T, className string) *models.ReplicationReplicateReplicaRequest {
	verbose := verbosity.OutputVerbose
	nodes, err := helper.Client(t).Nodes.NodesGetClass(nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(className), nil)
	require.Nil(t, err)
	return &models.ReplicationReplicateReplicaRequest{
		CollectionID:        &className,
		SourceNodeName:      &nodes.Payload.Nodes[0].Name,
		DestinationNodeName: &nodes.Payload.Nodes[1].Name,
		ShardID:             &nodes.Payload.Nodes[0].Shards[0].Name,
	}
}
