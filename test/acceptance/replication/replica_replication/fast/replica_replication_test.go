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
	"net/http"
	"testing"
	"time"

	"github.com/weaviate/weaviate/cluster/proto/api"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

var paragraphIDs = []strfmt.UUID{
	strfmt.UUID("3bf331ac-8c86-4f95-b127-2f8f96bbc093"),
	strfmt.UUID("47b26ba1-6bc9-41f8-a655-8b9a5b60e1a3"),
	strfmt.UUID("5fef6289-28d2-4ea2-82a9-48eb501200cd"),
	strfmt.UUID("34a673b4-8859-4cb4-bb30-27f5622b47e9"),
	strfmt.UUID("9fa362f5-c2dc-4fb8-b5b2-11701adc5f75"),
	strfmt.UUID("63735238-6723-4caf-9eaa-113120968ff4"),
	strfmt.UUID("2236744d-b2d2-40e5-95d8-2574f20a7126"),
	strfmt.UUID("1a54e25d-aaf9-48d2-bc3c-bef00b556297"),
	strfmt.UUID("0b8a0e70-a240-44b2-ac6d-26dda97523b9"),
	strfmt.UUID("50566856-5d0a-4fb1-a390-e099bc236f66"),
}

type ReplicationHappyPathTestSuite struct {
	suite.Suite
}

func (suite *ReplicationHappyPathTestSuite) SetupSuite() {
	t := suite.T()
	t.Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")
}

func TestReplicationHappyPathTestSuite(t *testing.T) {
	suite.Run(t, new(ReplicationHappyPathTestSuite))
}

func (suite *ReplicationHappyPathTestSuite) TestReplicaMovementHappyPath() {
	t := suite.T()
	mainCtx := context.Background()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithText2VecContextionary().
		WithWeaviateEnv("REPLICA_MOVEMENT_MINIMUM_ASYNC_WAIT", "5s").
		WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true").
		Start(mainCtx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(mainCtx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	ctx, cancel := context.WithTimeout(mainCtx, 20*time.Minute)
	defer cancel()

	helper.SetupClient(compose.GetWeaviate().URI())
	paragraphClass := articles.ParagraphsClass()
	articleClass := articles.ArticlesClass()

	t.Run("stop node 3", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 3)
	})

	t.Run("create schema", func(t *testing.T) {
		paragraphClass.ShardingConfig = map[string]interface{}{"desiredCount": 1}
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor:       1,
			AsyncEnabled: false,
		}
		paragraphClass.Vectorizer = "text2vec-contextionary"
		helper.CreateClass(t, paragraphClass)
		articleClass.ShardingConfig = map[string]interface{}{"desiredCount": 1}
		articleClass.ReplicationConfig = &models.ReplicationConfig{
			Factor:       1,
			AsyncEnabled: false,
		}
		helper.CreateClass(t, articleClass)
	})

	t.Run("restart node 3", func(t *testing.T) {
		common.StartNodeAt(ctx, t, compose, 3)
	})

	// Setup client again after restart to avoid HTTP error if client setup to container that now has a changed port
	helper.SetupClient(compose.GetWeaviate().URI())

	t.Run("insert paragraphs", func(t *testing.T) {
		batch := make([]*models.Object, len(paragraphIDs))
		for i, id := range paragraphIDs {
			batch[i] = articles.NewParagraph().
				WithID(id).
				WithContents(fmt.Sprintf("paragraph#%d", i)).
				Object()
		}
		common.CreateObjects(t, compose.GetWeaviate().URI(), batch)
	})

	t.Run("verify that all nodes are running", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			verbose := verbosity.OutputVerbose
			params := nodes.NewNodesGetClassParams().WithOutput(&verbose)
			body, clientErr := helper.Client(t).Nodes.NodesGetClass(params, nil)
			require.NoError(ct, clientErr)
			require.NotNil(ct, body.Payload)

			resp := body.Payload
			require.Len(ct, resp.Nodes, 3)
			for _, n := range resp.Nodes {
				require.NotNil(ct, n.Status)
				assert.Equal(ct, "HEALTHY", *n.Status)
			}
		}, 15*time.Second, 500*time.Millisecond)
	})

	var uuid strfmt.UUID
	sourceNode := -1
	t.Run("start replica replication to node3 for paragraph", func(t *testing.T) {
		verbose := verbosity.OutputVerbose
		params := nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(paragraphClass.Class)
		body, clientErr := helper.Client(t).Nodes.NodesGetClass(params, nil)
		require.NoError(t, clientErr)
		require.NotNil(t, body.Payload)
		targetNode := "node3"
		hasFoundNode := false
		hasFoundShard := false

		for i, node := range body.Payload.Nodes {
			if node.Name == targetNode {
				continue
			}

			if len(node.Shards) >= 1 {
				hasFoundNode = true
			} else {
				continue
			}

			transferType := api.COPY.String()
			for _, shard := range node.Shards {
				if shard.Class != paragraphClass.Class {
					continue
				}
				hasFoundShard = true

				t.Logf("Starting replica replication from %s to %s for shard %s", node.Name, targetNode, shard.Name)
				// i + 1 because stop/start routine are 1 based not 0
				sourceNode = i + 1
				resp, err := helper.Client(t).Replication.Replicate(
					replication.NewReplicateParams().WithBody(
						&models.ReplicationReplicateReplicaRequest{
							CollectionID:        &paragraphClass.Class,
							SourceNodeName:      &node.Name,
							DestinationNodeName: &targetNode,
							ShardID:             &shard.Name,
							TransferType:        &transferType,
						},
					),
					nil,
				)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, resp.Code(), "replication replicate operation didn't return 200 OK")
				require.NotNil(t, resp.Payload)
				require.NotNil(t, resp.Payload.ID)
				require.NotEmpty(t, *resp.Payload.ID)
				uuid = *resp.Payload.ID
			}
		}
		require.True(t, hasFoundShard, "could not find shard for class %s", paragraphClass.Class)
		require.True(t, hasFoundNode, "could not find node with shards for paragraph")
	})

	// If no node was found fail now
	if sourceNode == -1 {
		t.FailNow()
	}

	// Wait for the replication to finish
	t.Run("waiting for replication to finish", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			details, err := helper.Client(t).Replication.ReplicationDetails(
				replication.NewReplicationDetailsParams().WithID(uuid), nil,
			)
			assert.Nil(t, err, "failed to get replication details %s", err)
			assert.NotNil(t, details, "expected replication details to be not nil")
			assert.NotNil(t, details.Payload, "expected replication details payload to be not nil")
			assert.NotNil(t, details.Payload.Status, "expected replication status to be not nil")
			assert.Equal(ct, "READY", details.Payload.Status.State, "expected replication status to be READY")
		}, 360*time.Second, 3*time.Second, "replication operation %s not finished in time", uuid)
	})

	// Kills the original node with the data to ensure we have only one replica available (the new one)
	t.Run(fmt.Sprintf("stop node %d", sourceNode), func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, sourceNode)
	})

	t.Run("assert data is available for paragraph on node3 with consistency level one", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			for _, objId := range paragraphIDs {
				exists, err := common.ObjectExistsCL(t, compose.ContainerURI(3), paragraphClass.Class, objId, types.ConsistencyLevelOne)
				assert.Nil(ct, err)
				assert.True(ct, exists)

				resp, err := common.GetObjectCL(t, compose.ContainerURI(3), paragraphClass.Class, objId, types.ConsistencyLevelOne)
				assert.Nil(ct, err)
				assert.NotNil(ct, resp)
			}
		}, 10*time.Second, 1*time.Second, "node3 doesn't have paragraph data")
	})

	t.Run("assert that async replication is not running in any of the nodes", func(t *testing.T) {
		nodes, err := helper.Client(t).Nodes.
			NodesGetClass(nodes.NewNodesGetClassParams().WithClassName(paragraphClass.Class), nil)
		require.Nil(t, err)
		for _, node := range nodes.Payload.Nodes {
			for _, shard := range node.Shards {
				require.Len(t, shard.AsyncReplicationStatus, 0)
			}
		}
	})
}

func (suite *ReplicationHappyPathTestSuite) TestReplicaMovementTenantHappyPath() {
	t := suite.T()
	mainCtx := context.Background()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithText2VecContextionary().
		WithWeaviateEnv("REPLICA_MOVEMENT_MINIMUM_ASYNC_WAIT", "5s").
		WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true").
		Start(mainCtx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(mainCtx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	ctx, cancel := context.WithTimeout(mainCtx, 20*time.Minute)
	defer cancel()

	helper.SetupClient(compose.GetWeaviate().URI())
	paragraphClass := articles.ParagraphsClass()
	articleClass := articles.ArticlesClass()

	t.Run("create schema", func(t *testing.T) {
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor:       1,
			AsyncEnabled: false,
		}
		paragraphClass.MultiTenancyConfig = &models.MultiTenancyConfig{
			Enabled:              true,
			AutoTenantActivation: true,
			AutoTenantCreation:   true,
		}
		paragraphClass.Vectorizer = "text2vec-contextionary"
		helper.CreateClass(t, paragraphClass)
		articleClass.ReplicationConfig = &models.ReplicationConfig{
			Factor:       1,
			AsyncEnabled: false,
		}
		articleClass.MultiTenancyConfig = &models.MultiTenancyConfig{
			Enabled:              true,
			AutoTenantActivation: true,
			AutoTenantCreation:   true,
		}
		helper.CreateClass(t, articleClass)
	})

	t.Run("insert paragraphs", func(t *testing.T) {
		batch := make([]*models.Object, len(paragraphIDs))
		for i, id := range paragraphIDs {
			batch[i] = articles.NewParagraph().
				WithID(id).
				WithContents(fmt.Sprintf("paragraph#%d", i)).
				WithTenant("tenant0").
				Object()
		}
		common.CreateObjects(t, compose.GetWeaviate().URI(), batch)
	})

	// any node can be chosen as the tenant source node (even if that node is down at the time of creation)
	// so we dynamically find the source and target nodes
	sourceNode := -1
	var targetNode string
	var targetNodeURI string
	var uuid strfmt.UUID
	t.Run("start replica replication to node3 for paragraph", func(t *testing.T) {
		verbose := verbosity.OutputVerbose
		params := nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(paragraphClass.Class)
		body, clientErr := helper.Client(t).Nodes.NodesGetClass(params, nil)
		require.NoError(t, clientErr)
		require.NotNil(t, body.Payload)

		hasFoundNode := false
		hasFoundShard := false

		for i, node := range body.Payload.Nodes {
			if len(node.Shards) >= 1 {
				hasFoundNode = true
			} else {
				continue
			}

			for _, shard := range node.Shards {
				if shard.Class != paragraphClass.Class {
					continue
				}
				hasFoundShard = true

				t.Logf("Starting replica replication from %s to another node for shard %s", node.Name, shard.Name)
				// i + 1 because stop/start routine are 1 based not 0
				sourceNode = i + 1
				break
			}

			if hasFoundShard {
				break
			}
		}

		require.True(t, hasFoundShard, "could not find shard for class %s", paragraphClass.Class)
		require.True(t, hasFoundNode, "could not find node with shards for paragraph")

		// Choose a target node that is not the source node
		for i, node := range body.Payload.Nodes {
			if i+1 == sourceNode {
				continue
			}
			targetNode = node.Name
			targetNodeURI = compose.ContainerURI(i + 1)
			break
		}

		require.NotEmpty(t, targetNode, "could not find a target node different from the source node")

		for _, node := range body.Payload.Nodes {
			if node.Name == targetNode {
				continue
			}

			for _, shard := range node.Shards {
				if shard.Class != paragraphClass.Class {
					continue
				}

				t.Logf("Starting replica replication from %s to %s for shard %s", node.Name, targetNode, shard.Name)
				resp, err := helper.Client(t).Replication.Replicate(
					replication.NewReplicateParams().WithBody(
						&models.ReplicationReplicateReplicaRequest{
							CollectionID:        &paragraphClass.Class,
							SourceNodeName:      &node.Name,
							DestinationNodeName: &targetNode,
							ShardID:             &shard.Name,
						},
					),
					nil,
				)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, resp.Code(), "replication replicate operation didn't return 200 OK")
				require.NotNil(t, resp.Payload)
				require.NotNil(t, resp.Payload.ID)
				require.NotEmpty(t, *resp.Payload.ID)
				uuid = *resp.Payload.ID
			}
		}
	})

	// If didn't find needed info fail now
	if sourceNode == -1 || targetNode == "" || targetNodeURI == "" || uuid.String() == "" {
		t.FailNow()
	}

	// Wait for the replication to finish
	t.Run("waiting for replication to finish", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			details, err := helper.Client(t).Replication.ReplicationDetails(
				replication.NewReplicationDetailsParams().WithID(uuid), nil,
			)
			assert.Nil(t, err, "failed to get replication details %s", err)
			assert.NotNil(t, details, "expected replication details to be not nil")
			assert.NotNil(t, details.Payload, "expected replication details payload to be not nil")
			assert.NotNil(t, details.Payload.Status, "expected replication status to be not nil")
			assert.Equal(ct, "READY", details.Payload.Status.State, "expected replication status to be READY")
		}, 240*time.Second, 1*time.Second, "replication operation %s not finished in time", uuid)
	})

	// Kills the original node with the data to ensure we have only one replica available (the new one)
	t.Run(fmt.Sprintf("stop node %d", sourceNode), func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, sourceNode)
	})

	t.Run("assert data is available for paragraph on node3 with consistency level one", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			for _, objId := range paragraphIDs {
				obj, err := common.GetTenantObjectFromNode(t, targetNodeURI, paragraphClass.Class, objId, targetNode, "tenant0")
				assert.Nil(ct, err)
				assert.NotNil(ct, obj)
			}
		}, 10*time.Second, 1*time.Second, "node3 doesn't have paragraph data")
	})

	t.Run("assert that async replication is not running in any of the nodes", func(t *testing.T) {
		nodes, err := helper.Client(t).Nodes.
			NodesGetClass(nodes.NewNodesGetClassParams().WithClassName(paragraphClass.Class), nil)
		require.Nil(t, err)
		for _, node := range nodes.Payload.Nodes {
			for _, shard := range node.Shards {
				require.Len(t, shard.AsyncReplicationStatus, 0)
			}
		}
	})
}

// TODO fix flake and uncomment
// func (suite *ReplicationTestSuite) TestReplicaMovementTenantParallelWrites() {
// 	t := suite.T()
// 	mainCtx := context.Background()
// 	logger, _ := logrustest.NewNullLogger()

// 	clusterSize := 3
// 	compose, err := docker.New().
// 		WithWeaviateCluster(clusterSize).
// 		WithText2VecContextionary().
// 		WithWeaviateEnv("REPLICA_MOVEMENT_MINIMUM_ASYNC_WAIT", "10s").
// 		Start(mainCtx)
// 	require.Nil(t, err)
// 	defer func() {
// 		if err := compose.Terminate(mainCtx); err != nil {
// 			t.Fatalf("failed to terminate test containers: %s", err.Error())
// 		}
// 	}()

// 	_, cancel := context.WithTimeout(mainCtx, 5*time.Minute)
// 	defer cancel()

// 	helper.SetupClient(compose.GetWeaviate().URI())
// 	paragraphClass := articles.ParagraphsClass()
// 	articleClass := articles.ArticlesClass()

// 	t.Run("create schema", func(t *testing.T) {
// 		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
// 			Factor:       2,
// 			AsyncEnabled: false,
// 		}
// 		paragraphClass.MultiTenancyConfig = &models.MultiTenancyConfig{
// 			Enabled:              true,
// 			AutoTenantActivation: true,
// 			AutoTenantCreation:   true,
// 		}
// 		paragraphClass.Vectorizer = "text2vec-contextionary"
// 		helper.CreateClass(t, paragraphClass)
// 		articleClass.ReplicationConfig = &models.ReplicationConfig{
// 			Factor:       2,
// 			AsyncEnabled: false,
// 		}
// 		articleClass.MultiTenancyConfig = &models.MultiTenancyConfig{
// 			Enabled:              true,
// 			AutoTenantActivation: true,
// 			AutoTenantCreation:   true,
// 		}
// 		helper.CreateClass(t, articleClass)
// 	})

// 	t.Run("insert initial paragraphs", func(t *testing.T) {
// 		batch := make([]*models.Object, len(paragraphIDs))
// 		for i, id := range paragraphIDs {
// 			batch[i] = articles.NewParagraph().
// 				WithID(id).
// 				WithContents(fmt.Sprintf("paragraph#%d", i)).
// 				WithTenant("tenant0").
// 				Object()
// 		}
// 		common.CreateObjects(t, compose.GetWeaviate().URI(), batch)
// 	})

// 	parallelWriteWg := sync.WaitGroup{}
// 	parallelWriteIDs := []string{}
// 	replicationDone := make(chan struct{})
// 	t.Run("start parallel writes", func(t *testing.T) {
// 		parallelWriteWg.Add(1)
// 		enterrors.GoWrapper(func() {
// 			defer parallelWriteWg.Done()
// 			containerId := 1
// 			for {
// 				select {
// 				case <-replicationDone:
// 					return
// 				default:
// 					newWriteId := uuid.New().String()
// 					err = createObjectThreadSafe(
// 						compose.ContainerURI(containerId),
// 						paragraphClass.Class,
// 						map[string]interface{}{
// 							"contents": fmt.Sprintf("paragraph#%d", len(paragraphIDs)+len(parallelWriteIDs)),
// 						},
// 						newWriteId,
// 						"tenant0",
// 					)
// 					assert.NoError(t, err, "error creating object on node with id %d", containerId)
// 					parallelWriteIDs = append(parallelWriteIDs, newWriteId)
// 					containerId++
// 					if containerId >= clusterSize+1 {
// 						containerId = 1
// 					}
// 				}
// 			}
// 		}, logger)
// 	})

// 	// any node can be chosen as the tenant source node (even if that node is down at the time of creation)
// 	// so we dynamically find the source and target nodes
// 	var opUuid strfmt.UUID
// 	var shardName string
// 	type nodeInfo struct {
// 		nodeContainerIndex int
// 		nodeURI            string
// 		nodeName           string
// 	}
// 	sourceNode := nodeInfo{}
// 	targetNode := nodeInfo{}
// 	replicaNode := nodeInfo{}
// 	allNodeInfos := []nodeInfo{}
// 	// TODO test copy as well
// 	transferType := api.MOVE.String()
// 	t.Run("start replica replication to node3 for paragraph", func(t *testing.T) {
// 		verbose := verbosity.OutputVerbose
// 		params := nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(paragraphClass.Class)
// 		body, clientErr := helper.Client(t).Nodes.NodesGetClass(params, nil)
// 		require.NoError(t, clientErr)
// 		require.NotNil(t, body.Payload)

// 		hasFoundNode := false
// 		hasFoundShard := false

// 		// Find two source nodes that have shards
// 		for i, node := range body.Payload.Nodes {
// 			containerIndex := i + 1
// 			allNodeInfos = append(allNodeInfos, nodeInfo{nodeURI: compose.ContainerURI(containerIndex), nodeName: node.Name, nodeContainerIndex: containerIndex})
// 			if len(node.Shards) >= 1 {
// 				hasFoundNode = true
// 				for _, shard := range node.Shards {
// 					if shard.Class != paragraphClass.Class {
// 						continue
// 					}
// 					hasFoundShard = true
// 					shardName = shard.Name
// 					// i + 1 because stop/start routine are 1 based not 0
// 					if sourceNode.nodeName == "" {
// 						sourceNode = nodeInfo{nodeURI: compose.ContainerURI(containerIndex), nodeName: node.Name, nodeContainerIndex: containerIndex}
// 					} else {
// 						replicaNode = nodeInfo{nodeURI: compose.ContainerURI(containerIndex), nodeName: node.Name, nodeContainerIndex: containerIndex}
// 					}
// 				}
// 			}
// 		}

// 		require.True(t, hasFoundShard, "could not find shard for class %s", paragraphClass.Class)
// 		require.True(t, hasFoundNode, "could not find node with shards for paragraph")
// 		require.NotEmpty(t, sourceNode.nodeName, "could not find two source nodes with shards for paragraph")
// 		require.NotEmpty(t, sourceNode.nodeURI, "could not find two source nodes with shards for paragraph")
// 		require.NotEmpty(t, replicaNode.nodeName, "could not find two source nodes with shards for paragraph")
// 		require.NotEmpty(t, replicaNode.nodeURI, "could not find two source nodes with shards for paragraph")
// 		require.NotEqual(t, sourceNode.nodeName, replicaNode.nodeName, "source and replica nodes are the same")
// 		require.NotEqual(t, sourceNode.nodeURI, replicaNode.nodeURI, "source and replica nodes are the same")

// 		// Choose a target node that is not one of the source nodes
// 		for i, node := range body.Payload.Nodes {
// 			if node.Name == sourceNode.nodeName || node.Name == replicaNode.nodeName {
// 				continue
// 			}
// 			targetNode = nodeInfo{nodeURI: compose.ContainerURI(i + 1), nodeName: node.Name}
// 			break
// 		}

// 		require.NotEmpty(t, targetNode, "could not find a target node different from the source nodes")

// 		t.Logf("Starting replica replication from %s to %s for shard %s", sourceNode.nodeName, targetNode.nodeName, shardName)
// 		resp, err := helper.Client(t).Replication.Replicate(
// 			replication.NewReplicateParams().WithBody(
// 				&models.ReplicationReplicateReplicaRequest{
// 					CollectionID:        &paragraphClass.Class,
// 					SourceNodeName:      &sourceNode.nodeName,
// 					DestinationNodeName: &targetNode.nodeName,
// 					ShardID:             &shardName,
// 					TransferType:        &transferType,
// 				},
// 			),
// 			nil,
// 		)
// 		require.NoError(t, err)
// 		require.Equal(t, http.StatusOK, resp.Code(), "replication replicate operation didn't return 200 OK")
// 		require.NotNil(t, resp.Payload)
// 		require.NotNil(t, resp.Payload.ID)
// 		require.NotEmpty(t, *resp.Payload.ID)
// 		opUuid = *resp.Payload.ID
// 	})

// 	require.NotEmpty(t, opUuid, "opUuid is empty")
// 	require.NotEmpty(t, sourceNode.nodeName, "sourceNode is empty")
// 	require.NotEmpty(t, sourceNode.nodeURI, "sourceNode is empty")
// 	require.NotEmpty(t, targetNode.nodeName, "targetNode is empty")
// 	require.NotEmpty(t, targetNode.nodeURI, "targetNode is empty")
// 	require.NotEmpty(t, replicaNode.nodeName, "replicaNode is empty")
// 	require.NotEmpty(t, replicaNode.nodeURI, "replicaNode is empty")
// 	require.NotEqual(t, sourceNode.nodeName, replicaNode.nodeName, "source and replica nodes are the same")
// 	require.NotEqual(t, sourceNode.nodeURI, replicaNode.nodeURI, "source and replica nodes are the same")
// 	require.NotEqual(t, targetNode.nodeName, sourceNode.nodeName, "target and source nodes are the same")
// 	require.NotEqual(t, targetNode.nodeURI, sourceNode.nodeURI, "target and source nodes are the same")
// 	require.NotEqual(t, targetNode.nodeName, replicaNode.nodeName, "target and replica nodes are the same")
// 	require.NotEqual(t, targetNode.nodeURI, replicaNode.nodeURI, "target and replica nodes are the same")

// 	// Wait for the replication to finish
// 	t.Run("waiting for replication to finish", func(t *testing.T) {
// 		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
// 			details, err := helper.Client(t).Replication.ReplicationDetails(
// 				replication.NewReplicationDetailsParams().WithID(opUuid), nil,
// 			)
// 			assert.Nil(t, err, "failed to get replication details %s", err)
// 			assert.NotNil(t, details, "expected replication details to be not nil")
// 			assert.NotNil(t, details.Payload, "expected replication details payload to be not nil")
// 			assert.NotNil(t, details.Payload.Status, "expected replication status to be not nil")
// 			assert.Equal(ct, "READY", details.Payload.Status.State, "expected replication status to be READY")
// 		}, 240*time.Second, 1*time.Second, "replication operation %s not finished in time", opUuid)
// 		// let some writes keep going for a few seconds after the op is ready
// 		time.Sleep(5 * time.Second)
// 		// now stop the writes
// 		close(replicationDone)
// 		parallelWriteWg.Wait()
// 	})

// 	t.Run("all parallel writes are available", func(t *testing.T) {
// 		numParallelWrites := len(parallelWriteIDs)
// 		assert.True(t, numParallelWrites > 1000, "expected at least 1000 parallel writes")
// 		for _, nodeInfo := range allNodeInfos {
// 			// in a move, sourceNode no longer has the shard replica, so we skip it
// 			if transferType == api.MOVE.String() && nodeInfo.nodeName == sourceNode.nodeName {
// 				continue
// 			}

// 			assert.Equal(t, int64(numParallelWrites+len(paragraphIDs)), common.CountTenantObjects(t, nodeInfo.nodeURI, paragraphClass.Class, "tenant0"), fmt.Sprintf("expected %d objects on node %s", numParallelWrites+len(paragraphIDs), nodeInfo.nodeName))
// 		}
// 		for _, nodeInfo := range allNodeInfos {
// 			firstMissingObjectForNode := ""
// 			numMissingObjectsForNode := 0
// 			for _, id := range parallelWriteIDs {
// 				// in a move, sourceNode no longer has the shard replica, so we skip it
// 				if transferType == api.MOVE.String() && nodeInfo.nodeName == sourceNode.nodeName {
// 					continue
// 				}

// 				obj, err := common.GetTenantObjectFromNode(t, nodeInfo.nodeURI, paragraphClass.Class, strfmt.UUID(id), nodeInfo.nodeName, "tenant0")
// 				if err != nil || obj == nil {
// 					numMissingObjectsForNode++
// 					if firstMissingObjectForNode == "" {
// 						assert.Nil(t, err, "error getting object from node %s", nodeInfo.nodeName, id, err)
// 						assert.NotNil(t, obj, "object not found on node", nodeInfo.nodeName, id)
// 						firstMissingObjectForNode = id
// 					}
// 				}
// 			}
// 			assert.Equal(t, 0, numMissingObjectsForNode, "expected no missing objects on node %s", nodeInfo.nodeName)
// 			assert.Empty(t, firstMissingObjectForNode, "expected no missing objects on node %s", nodeInfo.nodeName)
// 		}
// 	})

// 	t.Run("assert data is available for paragraph on node3 with consistency level one", func(t *testing.T) {
// 		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
// 			for _, objId := range paragraphIDs {
// 				obj, err := common.GetTenantObjectFromNode(t, targetNode.nodeURI, paragraphClass.Class, objId, targetNode.nodeName, "tenant0")
// 				assert.Nil(ct, err)
// 				assert.NotNil(ct, obj)
// 			}
// 		}, 10*time.Second, 1*time.Second, "node3 doesn't have paragraph data")
// 	})
// }
