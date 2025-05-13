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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/weaviate/weaviate/cluster/proto/api"

	"github.com/go-openapi/strfmt"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/cluster/router/types"
	enterrors "github.com/weaviate/weaviate/entities/errors"
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

type ReplicaReplicationTestSuite struct {
	suite.Suite
}

func (suite *ReplicaReplicationTestSuite) SetupTest() {
	suite.T().Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")
}

func TestReplicaReplicationTestSuite(t *testing.T) {
	suite.Run(t, new(ReplicaReplicationTestSuite))
}

func (suite *ReplicaReplicationTestSuite) TestReplicaMovementHappyPath() {
	t := suite.T()
	mainCtx := context.Background()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithText2VecContextionary().
		Start(mainCtx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(mainCtx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	ctx, cancel := context.WithTimeout(mainCtx, 10*time.Minute)
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
		}, 240*time.Second, 1*time.Second, "replication operation %s not finished in time", uuid)
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
}

func (suite *ReplicaReplicationTestSuite) TestReplicaMovementTenantHappyPath() {
	t := suite.T()
	mainCtx := context.Background()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithText2VecContextionary().
		Start(mainCtx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(mainCtx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	ctx, cancel := context.WithTimeout(mainCtx, 5*time.Minute)
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
}

func (suite *ReplicaReplicationTestSuite) TestReplicaMovementOneWriteExtraSlowFileCopy() {
	t := suite.T()
	mainCtx := context.Background()
	logger, _ := logrustest.NewNullLogger()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithText2VecContextionary().
		WithWeaviateEnv("WEAVIATE_TEST_COPY_REPLICA_SLEEP", "20s").
		Start(mainCtx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(mainCtx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	ctx, cancel := context.WithTimeout(mainCtx, 10*time.Minute)
	defer cancel()

	helper.SetupClient(compose.GetWeaviate().URI())
	paragraphClass := articles.ParagraphsClass()

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
	})

	t.Run("restart node 3", func(t *testing.T) {
		common.StartNodeAt(ctx, t, compose, 3)
	})

	// Setup client again after restart to avoid HTTP error if client setup to container that now has a changed port
	helper.SetupClient(compose.GetWeaviate().URI())

	numParagraphsInsertedBeforeStart := 10
	t.Run("insert paragraphs", func(t *testing.T) {
		// Create and insert each object individually
		for i := 0; i < numParagraphsInsertedBeforeStart; i++ {
			obj := articles.NewParagraph().
				WithID(strfmt.UUID(uuid.New().String())).
				WithContents(fmt.Sprintf("paragraph#%d", i)).
				Object()

			// Insert the object individually
			common.CreateObjects(t, compose.GetWeaviate().URI(), []*models.Object{obj})
		}
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

	var opId strfmt.UUID
	sourceNode := -1
	numParagraphsInsertedWhileStarting := 1
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

			for _, shard := range node.Shards {
				if shard.Class != paragraphClass.Class {
					continue
				}
				hasFoundShard = true

				t.Logf("Starting replica replication from %s to %s for shard %s", node.Name, targetNode, shard.Name)
				// i + 1 because stop/start routine are 1 based not 0
				sourceNode = i + 1
				// start replication in parallel with inserting new paragraphs
				wg := sync.WaitGroup{}
				wg.Add(1)
				enterrors.GoWrapper(func() {
					defer wg.Done()
					// TODO replace/remove this sleep once we have a test that constantly inserts in parallel
					// during shard replica movement
					// sleep 20s so that the source node has paused compaction but not resumed yet
					time.Sleep(20 * time.Second)
					for i := 0; i < numParagraphsInsertedWhileStarting; i++ {
						err := createObjectThreadSafe(
							compose.ContainerURI(sourceNode),
							paragraphClass.Class,
							map[string]interface{}{
								"contents": fmt.Sprintf("paragraph#%d", numParagraphsInsertedBeforeStart+i),
							},
							uuid.New().String(),
							// TODO handle
							"",
						)
						require.NoError(t, err)
						time.Sleep(time.Millisecond)

					}
				}, logger)

				wg.Add(1)
				enterrors.GoWrapper(func() {
					defer wg.Done()
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
					opId = *resp.Payload.ID
				}, logger)
				wg.Wait()
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
				replication.NewReplicationDetailsParams().WithID(opId), nil,
			)
			assert.Nil(t, err, "failed to get replication details %s", err)
			assert.NotNil(t, details, "expected replication details to be not nil")
			assert.NotNil(t, details.Payload, "expected replication details payload to be not nil")
			assert.NotNil(t, details.Payload.Status, "expected replication status to be not nil")
			assert.Equal(ct, "READY", details.Payload.Status.State, "expected replication status to be READY")
		}, 240*time.Second, 1*time.Second, "replication operation %s not finished in time", opId)
	})

	// Kills the original node with the data to ensure we have only one replica available (the new one)
	t.Run(fmt.Sprintf("stop node %d", sourceNode), func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, sourceNode)
	})

	t.Run("assert correct number of objects on node3", func(t *testing.T) {
		numObjectsFound := common.CountObjects(t, compose.ContainerURI(3), paragraphClass.Class)
		assert.Equal(t, numParagraphsInsertedBeforeStart+numParagraphsInsertedWhileStarting, int(numObjectsFound))
	})
}

func createObjectThreadSafe(uri string, class string, properties map[string]interface{}, id string, tenant string) error {
	// Define the data structure for the request body
	type Object struct {
		Class      string                 `json:"class"`
		Properties map[string]interface{} `json:"properties"`
		ID         string                 `json:"id"`
		Tenant     string                 `json:"tenant"`
	}

	// Create an instance of the object with sample data
	obj := Object{
		Class:      class,
		Properties: properties,
		ID:         id,
		Tenant:     tenant,
	}

	// Marshal the object to JSON
	jsonData, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("error marshalling JSON: %w", err)
	}

	// Create a new POST request
	req, err := http.NewRequest("POST", "http://"+uri+"/v1/objects", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}

	// Set the appropriate headers
	req.Header.Set("Content-Type", "application/json")

	// Send the request using http.DefaultClient
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error sending request: %w", err)
	}
	defer resp.Body.Close()

	// Check the response status
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("request failed with status: %s", resp.Status)
	}

	return nil
}

func (suite *ReplicaReplicationTestSuite) TestReplicaMovementTenantConstantWrites() {
	t := suite.T()
	mainCtx := context.Background()
	logger, _ := logrustest.NewNullLogger()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithText2VecContextionary().
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
	articleClass := articles.ArticlesClass()

	t.Run("create schema", func(t *testing.T) {
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor:       2,
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
			Factor:       2,
			AsyncEnabled: false,
		}
		articleClass.MultiTenancyConfig = &models.MultiTenancyConfig{
			Enabled:              true,
			AutoTenantActivation: true,
			AutoTenantCreation:   true,
		}
		helper.CreateClass(t, articleClass)
	})

	t.Run("insert initial paragraphs", func(t *testing.T) {
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

	constantWriteWg := sync.WaitGroup{}
	constantWriteIDs := []string{}
	replicationDone := make(chan struct{})
	t.Run("start constant writes", func(t *testing.T) {
		constantWriteWg.Add(1)
		enterrors.GoWrapper(func() {
			defer constantWriteWg.Done()
			containerId := 1
			for {
				select {
				case <-replicationDone:
					return
				default:
					newWriteId := uuid.New().String()
					err = createObjectThreadSafe(
						compose.ContainerURI(containerId),
						paragraphClass.Class,
						map[string]interface{}{
							"contents": fmt.Sprintf("paragraph#%d", len(paragraphIDs)+len(constantWriteIDs)),
						},
						newWriteId,
						"tenant0",
					)
					// fmt.Println("NATEE newWriteId", newWriteId, err)
					require.NoError(t, err)
					constantWriteIDs = append(constantWriteIDs, newWriteId)
					containerId++
					if containerId >= 4 {
						containerId = 1
					}
					time.Sleep(1 * time.Millisecond)
				}
			}
		}, logger)
	})

	// any node can be chosen as the tenant source node (even if that node is down at the time of creation)
	// so we dynamically find the source and target nodes
	sourceNodes := []int{}
	sourceNodeURIs := []string{}
	var targetNodeIndex int
	var targetNode string
	var targetNodeURI string
	var opUuid strfmt.UUID
	t.Run("start replica replication to node3 for paragraph", func(t *testing.T) {
		verbose := verbosity.OutputVerbose
		params := nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(paragraphClass.Class)
		body, clientErr := helper.Client(t).Nodes.NodesGetClass(params, nil)
		require.NoError(t, clientErr)
		require.NotNil(t, body.Payload)

		hasFoundNode := false
		hasFoundShard := false

		// Find two source nodes that have shards
		for i, node := range body.Payload.Nodes {
			if len(node.Shards) >= 1 {
				hasFoundNode = true
				for _, shard := range node.Shards {
					if shard.Class != paragraphClass.Class {
						continue
					}
					hasFoundShard = true
					// i + 1 because stop/start routine are 1 based not 0
					sourceNodes = append(sourceNodes, i+1)
					sourceNodeURIs = append(sourceNodeURIs, compose.ContainerURI(i+1))
					if len(sourceNodes) == 2 {
						break
					}
				}
			}
			if len(sourceNodes) == 2 {
				break
			}
		}

		require.True(t, hasFoundShard, "could not find shard for class %s", paragraphClass.Class)
		require.True(t, hasFoundNode, "could not find node with shards for paragraph")
		require.Len(t, sourceNodes, 2, "could not find two source nodes with shards for paragraph")

		// Choose a target node that is not one of the source nodes
		for i, node := range body.Payload.Nodes {
			if i+1 == sourceNodes[0] || i+1 == sourceNodes[1] {
				continue
			}
			targetNode = node.Name
			targetNodeURI = compose.ContainerURI(i + 1)
			targetNodeIndex = i + 1
			break
		}

		require.NotEmpty(t, targetNode, "could not find a target node different from the source nodes")

		for i, node := range body.Payload.Nodes {
			// use sourceNodes[0] for the source node of the replica movement
			if node.Name == targetNode || i+1 == sourceNodes[1] {
				continue
			}

			for _, shard := range node.Shards {
				if shard.Class != paragraphClass.Class {
					continue
				}

				// TODO move vs copy?
				transferType := api.MOVE.String()
				t.Logf("Starting replica replication from %s to %s for shard %s", node.Name, targetNode, shard.Name)
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
				opUuid = *resp.Payload.ID
			}
		}
	})

	// If didn't find needed info fail now
	if len(sourceNodes) != 2 || targetNode == "" || targetNodeURI == "" || opUuid.String() == "" {
		t.FailNow()
	}

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
			fmt.Println("NATEE replication details status state", details.Payload.Status.State)
		}, 240*time.Second, 1*time.Second, "replication operation %s not finished in time", opUuid)
		time.Sleep(30 * time.Second)
		close(replicationDone) // Signal that replication is complete
		constantWriteWg.Wait()
	})

	// Kills the original node with the data to ensure we have only one replica available (the new one)
	// t.Run(fmt.Sprintf("stop node %d", sourceNodes[0]), func(t *testing.T) {
	// 	common.StopNodeAt(ctx, t, compose, sourceNodes[0])
	// })
	t.Run("all constant writes are available", func(t *testing.T) {
		verbose := verbosity.OutputVerbose
		params := nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(paragraphClass.Class)
		body, clientErr := helper.Client(t).Nodes.NodesGetClass(params, nil)
		require.NoError(t, clientErr)
		require.NotNil(t, body.Payload)
		numConstantWrites := len(constantWriteIDs)
		require.True(t, numConstantWrites > 1000, "expected at least 1000 constant writes")
		// TODO flaky
		require.Equal(t, int64(numConstantWrites+len(paragraphIDs)), common.CountTenantObjects(t, compose.ContainerURI(sourceNodes[0]), paragraphClass.Class, "tenant0"), fmt.Sprintf("expected %d objects on node %d", numConstantWrites+len(paragraphIDs), sourceNodes[0]))
		require.Equal(t, int64(numConstantWrites+len(paragraphIDs)), common.CountTenantObjects(t, compose.ContainerURI(sourceNodes[1]), paragraphClass.Class, "tenant0"), fmt.Sprintf("expected %d objects on node %d", numConstantWrites+len(paragraphIDs), sourceNodes[1]))
		require.Equal(t, int64(numConstantWrites+len(paragraphIDs)), common.CountTenantObjects(t, compose.ContainerURI(targetNodeIndex), paragraphClass.Class, "tenant0"), fmt.Sprintf("expected %d objects on node %d", numConstantWrites+len(paragraphIDs), targetNodeIndex))
		for _, id := range constantWriteIDs {
			// TODO not sure if nodeIndex+1 in container URIs and node name actually match?
			for nodeIndex, node := range body.Payload.Nodes {
				// TODO this is a hack to skip the source node for now
				if nodeIndex == sourceNodes[0]+1 {
					continue
				}
				obj, err := common.GetTenantObjectFromNode(t, compose.ContainerURI(nodeIndex+1), paragraphClass.Class, strfmt.UUID(id), node.Name, "tenant0")
				require.Nil(t, err, "error getting object from node %s", node.Name)
				require.NotNil(t, obj, "object not found on node %s", node.Name)
			}
		}
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
	// fmt.Println("NATEE sleeping")
	// time.Sleep(9999 * time.Second)
}
