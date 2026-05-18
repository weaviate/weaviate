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

package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/weaviate/weaviate/cluster/proto/api"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

// errObjectNotFound is the sentinel returned by getObjectThreadSafe on
// HTTP 404 so deleted-id assertions can use errors.Is instead of pattern-
// matching on the swagger error type.
var errObjectNotFound = errors.New("object not found")

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

	ctx, cancel := context.WithTimeout(mainCtx, 20*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithText2VecContextionary().
		WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true").
		Start(ctx)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()
	require.Nil(t, err)

	helper.SetupClient(compose.GetWeaviate().URI())
	paragraphClass := articles.ParagraphsClass()
	articleClass := articles.ArticlesClass()

	t.Run("stop node 3", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 3)
	})

	t.Run("create schema", func(t *testing.T) {
		paragraphClass.ShardingConfig = map[string]any{"desiredCount": 1}
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor:       1,
			AsyncEnabled: false,
		}
		paragraphClass.Vectorizer = "text2vec-contextionary"
		helper.CreateClass(t, paragraphClass)
		articleClass.ShardingConfig = map[string]any{"desiredCount": 1}
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
		targetNode := docker.Weaviate2
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

			replicationType := api.COPY.String()
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
							Collection: &paragraphClass.Class,
							SourceNode: &node.Name,
							TargetNode: &targetNode,
							Shard:      &shard.Name,
							Type:       &replicationType,
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

	ctx, cancel := context.WithTimeout(mainCtx, 20*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithText2VecContextionary().
		WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true").
		Start(ctx)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()
	require.Nil(t, err)

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
							Collection: &paragraphClass.Class,
							SourceNode: &node.Name,
							TargetNode: &targetNode,
							Shard:      &shard.Name,
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

func (suite *ReplicationHappyPathTestSuite) TestReplicaMovementTenantParallelWrites() {
	t := suite.T()
	mainCtx := context.Background()

	clusterSize := 3
	compose, err := docker.New().
		WithWeaviateCluster(clusterSize).
		WithText2VecContextionary().
		WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true").
		WithWeaviateEnv("REPLICATION_ENGINE_MAX_WORKERS", "10").
		Start(mainCtx)
	defer func() {
		if err := compose.Terminate(mainCtx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()
	require.Nil(t, err)

	_, cancel := context.WithTimeout(mainCtx, 5*time.Minute)
	defer cancel()

	helper.SetupClient(compose.GetWeaviate().URI())
	paragraphClass := articles.ParagraphsClass()
	articleClass := articles.ArticlesClass()
	tenant := "tenant0"

	t.Run("create schema", func(t *testing.T) {
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor:       2,
			AsyncEnabled: false,
		}
		paragraphClass.MultiTenancyConfig = &models.MultiTenancyConfig{
			Enabled: true,
		}
		paragraphClass.Vectorizer = "text2vec-contextionary"
		helper.CreateClass(t, paragraphClass)
		helper.CreateTenants(t, paragraphClass.Class, []*models.Tenant{{Name: tenant}})
		articleClass.ReplicationConfig = &models.ReplicationConfig{
			Factor:       2,
			AsyncEnabled: false,
		}
		articleClass.MultiTenancyConfig = &models.MultiTenancyConfig{
			Enabled: true,
		}
		helper.CreateClass(t, articleClass)
	})

	t.Run("wait for eventual consistency of schema", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			for i := 0; i < clusterSize; i++ {
				helper.SetupClient(compose.ContainerURI(i + 1))

				consistency := false
				respSchema, err := helper.Client(t).Schema.SchemaDump(
					&schema.SchemaDumpParams{Consistency: &consistency},
					nil,
				)
				assert.Nil(ct, err)
				if respSchema == nil {
					continue
				}
				assert.Len(ct, respSchema.Payload.Classes, 2, "expected 2 classes in schema dump from node %d, got %d", i, len(respSchema.Payload.Classes))

				respTenants, err := helper.Client(t).Schema.TenantExists(
					&schema.TenantExistsParams{ClassName: paragraphClass.Class, TenantName: tenant, Consistency: &consistency},
					nil,
				)
				assert.Nil(ct, err)
				if respTenants == nil {
					continue
				}
				assert.True(ct, respTenants.IsSuccess(), 1, "expected tenant to exist in tenant exists response from node %d", i)
			}
		}, 10*time.Second, 1*time.Second, "schema not consistent across all nodes")
	})
	helper.SetupClient(compose.GetWeaviate().URI())

	t.Run("insert initial paragraphs", func(t *testing.T) {
		objs := make([]*models.Object, len(paragraphIDs))
		for i, id := range paragraphIDs {
			objs[i] = articles.NewParagraph().
				WithID(id).
				WithContents(fmt.Sprintf("paragraph#%d", i)).
				WithTenant(tenant).
				Object()
		}
		all := "ALL"
		params := batch.NewBatchObjectsCreateParams().WithConsistencyLevel(&all).WithBody(batch.BatchObjectsCreateBody{Objects: objs})
		resp, err := helper.Client(t).Batch.BatchObjectsCreate(params, nil)
		require.NoError(t, err, "failed to create initial batch of paragraphs: %s", err)
		for _, r := range resp.Payload {
			require.Nil(t, r.Result.Errors, "expected no errors in batch create response for paragraph %s: %+v", r.ID, r.Result.Errors)
		}
	})

	// Seed the parallel writer with the original paragraphs so the workload is
	// free to UPDATE/DELETE pre-HYDRATING data too. writes is populated once
	// the writer is stopped, below, and is the test-side source of truth for
	// what the move target must hold.
	seed := map[strfmt.UUID]string{}
	for i, id := range paragraphIDs {
		seed[id] = fmt.Sprintf("paragraph#%d", i)
	}
	stopWrites := startParallelWrites(t, mainCtx, newComposeNodeSource(compose, clusterSize), paragraphClass.Class, tenant, seed)
	// Guard against the orphan-writer race: if any require.XXX between
	// here and the explicit stopWrites() call below Goexits the test,
	// the writer goroutine would otherwise outlive the test's *testing.T
	// and race against tRunner.func1's deferred cleanup. stopWrites is
	// idempotent so the explicit call later is still effective.
	defer stopWrites()
	var writes parallelWriteResult

	// any node can be chosen as the tenant source node (even if that node is down at the time of creation)
	// so we dynamically find the source and target nodes
	var opUuid strfmt.UUID
	var shardName string
	type nodeInfo struct {
		nodeContainerIndex int
		nodeURI            string
		nodeName           string
	}
	sourceNode := nodeInfo{}
	targetNode := nodeInfo{}
	replicaNode := nodeInfo{}
	// TODO test copy as well
	transferType := api.MOVE.String()
	t.Run(fmt.Sprintf("start replica replication to %s for paragraph", targetNode.nodeName), func(t *testing.T) {
		verbose := verbosity.OutputVerbose
		params := nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(paragraphClass.Class)
		body, clientErr := helper.Client(t).Nodes.NodesGetClass(params, nil)
		require.NoError(t, clientErr)
		require.NotNil(t, body.Payload)

		hasFoundNode := false
		hasFoundShard := false

		// Find two source nodes that have shards
		for i, node := range body.Payload.Nodes {
			containerIndex := i + 1
			if len(node.Shards) >= 1 {
				hasFoundNode = true
				for _, shard := range node.Shards {
					if shard.Class != paragraphClass.Class {
						continue
					}
					hasFoundShard = true
					shardName = shard.Name
					// i + 1 because stop/start routine are 1 based not 0
					if sourceNode.nodeName == "" {
						sourceNode = nodeInfo{nodeURI: compose.ContainerURI(containerIndex), nodeName: node.Name, nodeContainerIndex: containerIndex}
					} else {
						replicaNode = nodeInfo{nodeURI: compose.ContainerURI(containerIndex), nodeName: node.Name, nodeContainerIndex: containerIndex}
					}
				}
			}
		}

		require.True(t, hasFoundShard, "could not find shard for class %s", paragraphClass.Class)
		require.True(t, hasFoundNode, "could not find node with shards for paragraph")
		require.NotEmpty(t, sourceNode.nodeName, "could not find two source nodes with shards for paragraph")
		require.NotEmpty(t, sourceNode.nodeURI, "could not find two source nodes with shards for paragraph")
		require.NotEmpty(t, replicaNode.nodeName, "could not find two source nodes with shards for paragraph")
		require.NotEmpty(t, replicaNode.nodeURI, "could not find two source nodes with shards for paragraph")
		require.NotEqual(t, sourceNode.nodeName, replicaNode.nodeName, "source and replica nodes are the same")
		require.NotEqual(t, sourceNode.nodeURI, replicaNode.nodeURI, "source and replica nodes are the same")

		// Choose a target node that is not one of the source nodes
		for i, node := range body.Payload.Nodes {
			if node.Name == sourceNode.nodeName || node.Name == replicaNode.nodeName {
				continue
			}
			targetNode = nodeInfo{nodeURI: compose.ContainerURI(i + 1), nodeName: node.Name}
			break
		}

		require.NotEmpty(t, targetNode, "could not find a target node different from the source nodes")

		t.Logf("Starting replica replication from %s to %s for shard %s", sourceNode.nodeName, targetNode.nodeName, shardName)
		resp, err := helper.Client(t).Replication.Replicate(
			replication.NewReplicateParams().WithBody(
				&models.ReplicationReplicateReplicaRequest{
					Collection: &paragraphClass.Class,
					SourceNode: &sourceNode.nodeName,
					TargetNode: &targetNode.nodeName,
					Shard:      &shardName,
					Type:       &transferType,
				},
			),
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.Code(), "replication replicate operation didn't return 200 OK")
		require.NotNil(t, resp.Payload)
		require.NotNil(t, resp.Payload.ID)
		require.NotEmpty(t, *resp.Payload.ID)
		opUuid = *resp.Payload.ID
	})

	require.NotEmpty(t, opUuid, "opUuid is empty")
	require.NotEmpty(t, sourceNode.nodeName, "sourceNode is empty")
	require.NotEmpty(t, sourceNode.nodeURI, "sourceNode is empty")
	require.NotEmpty(t, targetNode.nodeName, "targetNode is empty")
	require.NotEmpty(t, targetNode.nodeURI, "targetNode is empty")
	require.NotEmpty(t, replicaNode.nodeName, "replicaNode is empty")
	require.NotEmpty(t, replicaNode.nodeURI, "replicaNode is empty")
	require.NotEqual(t, sourceNode.nodeName, replicaNode.nodeName, "source and replica nodes are the same")
	require.NotEqual(t, sourceNode.nodeURI, replicaNode.nodeURI, "source and replica nodes are the same")
	require.NotEqual(t, targetNode.nodeName, sourceNode.nodeName, "target and source nodes are the same")
	require.NotEqual(t, targetNode.nodeURI, sourceNode.nodeURI, "target and source nodes are the same")
	require.NotEqual(t, targetNode.nodeName, replicaNode.nodeName, "target and replica nodes are the same")
	require.NotEqual(t, targetNode.nodeURI, replicaNode.nodeURI, "target and replica nodes are the same")

	// Wait for the replication to finish
	t.Run("waiting for replication to finish", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			details, err := helper.Client(t).Replication.ReplicationDetails(
				replication.NewReplicationDetailsParams().WithID(opUuid), nil,
			)
			assert.Nil(t, err, "failed to get replication details %s", err)
			assert.NotNil(t, details, "expected replication details to be not nil")
			assert.NotNil(t, details.Payload, "expected replication details payload to be not nil")
			assert.NotNil(t, details.Payload.Status, "expected replication status to be not nil")
			assert.Equal(ct, "READY", details.Payload.Status.State, "expected replication status to be READY")
		}, 240*time.Second, 1*time.Second, "replication operation %s not finished in time", opUuid)
		// now stop the writes and capture the writer's final tracked state
		writes = stopWrites()
	})

	// Only target is asserted: replicaNode can validly diverge under
	// multi-coordinator LWW races; async repl converges that.
	t.Run("post-move object set matches the writer's tracked state on target", func(t *testing.T) {
		// give time for any pending replication to finish so that all parallel writes are replicated to the new node before we check for their existence
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			n, err := countObjectsThreadSafe(targetNode.nodeURI, paragraphClass.Class, tenant)
			if !assert.NoError(ct, err, "count aggregate failed against target %s", targetNode.nodeName) {
				return
			}
			assert.Equal(ct, int64(len(writes.liveIDs)), n)
		}, 30*time.Second, 1*time.Second, "not all parallel writes are available on target %s", targetNode.nodeName)

		for id, expectedContents := range writes.liveIDs {
			var obj *models.Object
			assert.EventuallyWithT(t, func(ct *assert.CollectT) {
				o, err := getObjectThreadSafe(targetNode.nodeURI, paragraphClass.Class, id, targetNode.nodeName, tenant)
				assert.NoError(ct, err, "error getting live id %s from target %s", id, targetNode.nodeName)
				assert.NotNil(ct, o, "live id %s not yet present on target %s", id, targetNode.nodeName)
				obj = o
			}, 10*time.Second, 1*time.Second, "live id %s missing on target %s", id, targetNode.nodeName)
			if !assert.NotNil(t, obj, "live id %s missing on target %s", id, targetNode.nodeName) {
				continue
			}
			props, ok := obj.Properties.(map[string]any)
			if !assert.True(t, ok, "object %s on target %s has unexpected properties shape", id, targetNode.nodeName) {
				continue
			}
			assert.Equal(t, expectedContents, props["contents"],
				"contents mismatch for id %s on target %s (LWW failure?)", id, targetNode.nodeName)
		}

		for id := range writes.deletedIDs {
			assert.EventuallyWithT(t, func(ct *assert.CollectT) {
				_, err := getObjectThreadSafe(targetNode.nodeURI, paragraphClass.Class, id, targetNode.nodeName, tenant)
				assert.ErrorIs(ct, err, errObjectNotFound, "deleted id %s unexpectedly present on target %s", id, targetNode.nodeName)
			}, 10*time.Second, 1*time.Second, "deleted id %s still present on target %s", id, targetNode.nodeName)
		}
	})
}

// getObjectThreadSafe issues GET /v1/objects/{class}/{id} with a node_name
// query param (and tenant, when non-empty), bypassing the helper.Client global
// so it can be called concurrently with other helper calls without racing on
// the SetupClient/Client globals. tenant is "" for single-tenant collections.
//
// Returns (nil, errObjectNotFound) on 404 so callers can distinguish "not
// here yet" from network/server errors.
func getObjectThreadSafe(uri, class string, id strfmt.UUID, nodename, tenant string) (*models.Object, error) {
	q := url.Values{}
	q.Set("node_name", nodename)
	if tenant != "" {
		q.Set("tenant", tenant)
	}
	target := fmt.Sprintf("http://%s/v1/objects/%s/%s?%s", uri, class, id, q.Encode())
	req, err := http.NewRequest("GET", target, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error sending request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, errObjectNotFound
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("status %s, body: %s", resp.Status, string(body))
	}
	var obj models.Object
	if err := json.NewDecoder(resp.Body).Decode(&obj); err != nil {
		return nil, fmt.Errorf("decode object: %w", err)
	}
	return &obj, nil
}

// countObjectsThreadSafe runs an Aggregate{Class{meta{count}}} GraphQL query
// against the given node URI, bypassing the helper.Client global. When tenant
// is non-empty it scopes the aggregate to that tenant; "" aggregates a
// single-tenant collection. Same race-free rationale as getObjectThreadSafe.
func countObjectsThreadSafe(uri, class, tenant string) (int64, error) {
	var query string
	if tenant != "" {
		query = fmt.Sprintf(`{Aggregate{%s(tenant:%q){meta{count}}}}`, class, tenant)
	} else {
		query = fmt.Sprintf(`{Aggregate{%s{meta{count}}}}`, class)
	}
	body, err := json.Marshal(map[string]string{"query": query})
	if err != nil {
		return 0, fmt.Errorf("marshal graphql query: %w", err)
	}
	req, err := http.NewRequest("POST", "http://"+uri+"/v1/graphql", bytes.NewBuffer(body))
	if err != nil {
		return 0, fmt.Errorf("create graphql request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("send graphql request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("graphql status %s, body: %s", resp.Status, string(respBody))
	}
	var raw struct {
		Data struct {
			Aggregate map[string][]struct {
				Meta struct {
					Count json.Number `json:"count"`
				} `json:"meta"`
			} `json:"Aggregate"`
		} `json:"data"`
		Errors []map[string]any `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return 0, fmt.Errorf("decode graphql response: %w", err)
	}
	if len(raw.Errors) > 0 {
		return 0, fmt.Errorf("graphql errors: %v", raw.Errors)
	}
	arr, ok := raw.Data.Aggregate[class]
	if !ok || len(arr) == 0 {
		return 0, fmt.Errorf("missing aggregate result for class %s", class)
	}
	n, err := arr[0].Meta.Count.Int64()
	if err != nil {
		return 0, fmt.Errorf("parse count %q: %w", arr[0].Meta.Count.String(), err)
	}
	return n, nil
}

func createObjectThreadSafe(uri string, class string, properties map[string]any, id string, tenant string) error {
	type Object struct {
		Class      string         `json:"class"`
		Properties map[string]any `json:"properties"`
		ID         string         `json:"id"`
		Tenant     string         `json:"tenant,omitempty"`
	}
	obj := Object{Class: class, Properties: properties, ID: id, Tenant: tenant}
	jsonData, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("error marshalling JSON: %w", err)
	}
	req, err := http.NewRequest("POST", "http://"+uri+"/v1/objects?consistency_level=ALL", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("error sending request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		res, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("request failed with status: %s, and error reading body: %w", resp.Status, err)
		}
		return fmt.Errorf("request failed with status: %s, and body: %s", resp.Status, string(res))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// patchObjectThreadSafe issues a PATCH against /v1/objects/{class}/{id}.
// Mirrors createObjectThreadSafe (no helper.SetupClient) so it's safe to
// call from the writer goroutine in parallel with the main test thread.
func patchObjectThreadSafe(uri, class, id, tenant string, properties map[string]any) error {
	type Object struct {
		Class      string         `json:"class"`
		Properties map[string]any `json:"properties"`
		ID         string         `json:"id"`
		Tenant     string         `json:"tenant,omitempty"`
	}
	obj := Object{Class: class, Properties: properties, ID: id, Tenant: tenant}
	jsonData, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("error marshalling JSON: %w", err)
	}
	url := fmt.Sprintf("http://%s/v1/objects/%s/%s?consistency_level=ALL", uri, class, id)
	req, err := http.NewRequest("PATCH", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("error sending request: %w", err)
	}
	defer resp.Body.Close()
	// 204 No Content is the documented success for PATCH; accept 200 too.
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		res, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("request failed with status: %s, and error reading body: %w", resp.Status, readErr)
		}
		return fmt.Errorf("request failed with status: %s, and body: %s", resp.Status, string(res))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// deleteObjectThreadSafe issues a DELETE against /v1/objects/{class}/{id}.
// tenant is sent as a query param when non-empty (the REST API requires it for
// tenant-scoped objects); "" deletes from a single-tenant collection.
func deleteObjectThreadSafe(uri, class, id, tenant string) error {
	url := fmt.Sprintf("http://%s/v1/objects/%s/%s?consistency_level=ALL", uri, class, id)
	if tenant != "" {
		url += "&tenant=" + tenant
	}
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("error sending request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		res, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("request failed with status: %s, and error reading body: %w", resp.Status, readErr)
		}
		return fmt.Errorf("request failed with status: %s, and body: %s", resp.Status, string(res))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// composeNodeSource adapts a testcontainers DockerCompose into the
// nodeSourcer interface that startParallelWrites / dumpReplica*Once consume.
// Used by the tenant-MOVE tests which still spin up their own cluster.
type composeNodeSource struct {
	compose     *docker.DockerCompose
	clusterSize int
}

func newComposeNodeSource(compose *docker.DockerCompose, clusterSize int) composeNodeSource {
	return composeNodeSource{compose: compose, clusterSize: clusterSize}
}

func (c composeNodeSource) Size() int           { return c.clusterSize }
func (c composeNodeSource) URIFor(i int) string { return c.compose.ContainerURI(i) }
func (c composeNodeSource) FetchLogs(ctx context.Context, i int) (io.ReadCloser, error) {
	node := c.compose.GetWeaviateNode(i)
	if node == nil {
		return nil, fmt.Errorf("weaviate node %d not found in compose", i)
	}
	return node.Container().Logs(ctx)
}

// dumpReplicaLogsOnce dumps each node's container logs (filtered to
// route-stale / DEHYDRATING / retry-related lines) the first time it
// is invoked; subsequent calls are no-ops via the supplied sync.Once
// so a flapping parallel-write loop doesn't drown the test output.
// Filter is intentionally broad: it pulls anything that helps
// distinguish whether the coord-side retry engaged, whether
// waitForFSMCatchUp logged its no-applied-index warning, and whether
// the source-side fence saw DEHYDRATING.
func dumpReplicaLogsOnce(t *testing.T, ctx context.Context, nodes nodeSourcer, once *sync.Once) {
	once.Do(func() {
		for i := 1; i <= nodes.Size(); i++ {
			logs, err := nodes.FetchLogs(ctx, i)
			if err != nil {
				t.Logf("weaviate-%d: failed to get logs: %v", i-1, err)
				continue
			}
			buf, _ := io.ReadAll(logs)
			logs.Close()
			for _, line := range strings.Split(string(buf), "\n") {
				if matchesRouteStaleDiagnostics(line) {
					t.Logf("weaviate-%d: %s", i-1, line)
				}
			}
		}
	})
}

// matchesRouteStaleDiagnostics matches log lines worth dumping when a
// parallel-write goroutine sees a real write error — global lifecycle and
// routing decisions that explain why a CL=ALL write failed.
func matchesRouteStaleDiagnostics(line string) bool {
	keywords := []string{
		"push.retry_route_stale",
		"without an applied index",
		"waiting for local FSM to catch up",
		"source_applied",
		"is not a current write target",
		"route stale",
		"RouteStale",
		"DEHYDRATING",
		"FINALIZING",
		"WaitForUpdateAllNodes",
		"replicate insertion",
	}
	for _, k := range keywords {
		if strings.Contains(line, k) {
			return true
		}
	}
	return false
}
