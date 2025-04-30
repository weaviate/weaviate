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
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func TestCanCreateAndGetAReplicationOperation(t *testing.T) {
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

	var uuid strfmt.UUID

	t.Run("create replication operation", func(t *testing.T) {
		res, err := helper.Client(t).Replication.Replicate(replication.NewReplicateParams().WithBody(getRequest(t, paragraphClass.Class)), nil)
		require.Nil(t, err)
		require.NotNil(t, res)
		require.NotNil(t, res.Payload)
		uuid = *res.Payload.ID
	})

	t.Run("get replication operation", func(t *testing.T) {
		res, err := helper.Client(t).Replication.ReplicationDetails(replication.NewReplicationDetailsParams().WithID(uuid), nil)
		if err != nil {
			parsed, ok := err.(*replication.ReplicationDetailsInternalServerError)
			if ok {
				t.Fatalf("internal server error when getting replication operation: %s", parsed.Payload.Error[0].Message)
			}
		}
		require.Nil(t, err)
		require.NotNil(t, res)
		require.NotNil(t, res.Payload)
		require.Equal(t, uuid, *res.Payload.ID)
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
