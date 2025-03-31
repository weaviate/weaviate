package replica_replication

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

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

var (
	paragraphIDs = []strfmt.UUID{
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

	articleIDs = []strfmt.UUID{
		strfmt.UUID("aeaf8743-5a8f-4149-b960-444181d3131a"),
		strfmt.UUID("2a1e9834-064e-4ca8-9efc-35707c6bae6d"),
		strfmt.UUID("8d101c0c-4deb-48d0-805c-d9c691042a1a"),
		strfmt.UUID("b9715fec-ef6c-4e8d-a89e-55e2eebee3f6"),
		strfmt.UUID("faf520f2-f6c3-4cdf-9c16-0348ffd0f8ac"),
		strfmt.UUID("d4c695dd-4dc7-4e49-bc73-089ef5f90fc8"),
		strfmt.UUID("c7949324-e07f-4ffc-8be0-194f0470d375"),
		strfmt.UUID("9c112e01-7759-43ed-a6e8-5defb267c8ee"),
		strfmt.UUID("9bf847f3-3a1a-45a5-b656-311163e536b5"),
		strfmt.UUID("c1975388-d67c-404a-ae77-5983fbaea4bb"),
	}
)

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

	t.Run("insert articles", func(t *testing.T) {
		batch := make([]*models.Object, len(articleIDs))
		for i, id := range articleIDs {
			batch[i] = articles.NewArticle().
				WithID(id).
				WithTitle(fmt.Sprintf("Article#%d", i)).
				Object()
		}
		common.CreateObjects(t, compose.GetWeaviateNode(2).URI(), batch)
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

			for _, shard := range node.Shards {
				if shard.Class != paragraphClass.Class {
					continue
				}
				hasFoundShard = true

				t.Logf("Starting replica replication from %s to %s for shard %s", node.Name, targetNode, shard.Name)
				sourceNode = i
				helper.SetupClient(compose.ContainerURI(i))
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
			}
		}
		require.True(t, hasFoundShard, "could not find shard for class %s", paragraphClass.Class)
		require.True(t, hasFoundNode, "could not find node with shards for paragraph")
	})

	if sourceNode == -1 {
		t.FailNow()
	}

	t.Run(fmt.Sprintf("stop node %d", sourceNode), func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, sourceNode+1)
	})

	// TODO: Start watch status until completion
	// For now we sleep, remove the sleep and instead poll status
	time.Sleep(20 * time.Second)

	t.Run("assert data is available for paragraph on node3 with consistency level one", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			resp := common.GQLGet(t, compose.ContainerURI(2), "Paragraph", types.ConsistencyLevelOne)
			assert.Len(ct, resp, len(paragraphIDs))
		}, 10*time.Second, 1*time.Second, "node3 doesn't have paragraph data")
	})

	t.Run("assert data is not available for paragraph on node3 with consistency level one", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			resp := common.GQLGet(t, compose.ContainerURI(2), "Article", types.ConsistencyLevelOne)
			assert.Len(ct, resp, 0)
		}, 10*time.Second, 1*time.Second, "node3 does have articles data")
	})

	time.Sleep(10 * time.Minute)
}
