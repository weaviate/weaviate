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

package slow_file_copy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/replication"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

type ReplicationTestSuite struct {
	suite.Suite
}

func (suite *ReplicationTestSuite) SetupTest() {
	suite.T().Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")
}

func TestReplicationTestSuite(t *testing.T) {
	suite.Run(t, new(ReplicationTestSuite))
}

func (suite *ReplicationTestSuite) TestReplicaMovementOneWriteExtraSlowFileCopy() {
	t := suite.T()
	mainCtx := context.Background()
	logger, _ := logrustest.NewNullLogger()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithText2VecContextionary().
		WithWeaviateEnv("WEAVIATE_TEST_COPY_REPLICA_SLEEP", "20s").
		WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true").
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
		}, 360*time.Second, 3*time.Second, "replication operation %s not finished in time", opId)
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
		Tenant     string                 `json:"tenant,omitempty"`
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
