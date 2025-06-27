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
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/weaviate/weaviate/client/nodes"
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

type AsyncReplicationTestSuite struct {
	suite.Suite
}

func (suite *AsyncReplicationTestSuite) SetupTest() {
	suite.T().Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")
}

func TestAsyncReplicationTestSuite(t *testing.T) {
	suite.Run(t, new(AsyncReplicationTestSuite))
}

func (suite *AsyncReplicationTestSuite) TestAsyncRepairSimpleScenario() {
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

	t.Run("create schema", func(t *testing.T) {
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor:       3,
			AsyncEnabled: true,
		}
		paragraphClass.Vectorizer = "text2vec-contextionary"
		helper.CreateClass(t, paragraphClass)
		articleClass.ReplicationConfig = &models.ReplicationConfig{
			Factor:       3,
			AsyncEnabled: true,
		}
		helper.CreateClass(t, articleClass)
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

	t.Run("stop node 3", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 3)
	})

	repairObj := models.Object{
		ID:    "e5390693-5a22-44b8-997d-2a213aaf5884",
		Class: "Paragraph",
		Properties: map[string]interface{}{
			"contents": "a new paragraph",
		},
	}

	t.Run("add new object to node one", func(t *testing.T) {
		common.CreateObjectCL(t, compose.GetWeaviate().URI(), &repairObj, types.ConsistencyLevelOne)
	})

	t.Run("restart node 3", func(t *testing.T) {
		common.StartNodeAt(ctx, t, compose, 3)
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

	t.Run("assert new object read repair was made", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			resp, err := common.GetObjectCL(t, compose.GetWeaviateNode(3).URI(),
				repairObj.Class, repairObj.ID, types.ConsistencyLevelOne)
			assert.Nil(ct, err)
			assert.NotNil(ct, resp)
			if resp == nil {
				return
			}
			assert.Equal(ct, repairObj.ID, resp.ID)
			assert.Equal(ct, repairObj.Class, resp.Class)
			assert.EqualValues(ct, repairObj.Properties, resp.Properties)
			assert.EqualValues(ct, repairObj.Vector, resp.Vector)
		}, 60*time.Second, 1*time.Second, "not all the objects have been asynchronously replicated")
	})

	replaceObj := repairObj
	replaceObj.Properties = map[string]interface{}{
		"contents": "this paragraph was replaced",
	}

	t.Run("stop node 2", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 2)
	})

	t.Run("replace object", func(t *testing.T) {
		common.UpdateObjectCL(t, compose.GetWeaviateNode(3).URI(), &replaceObj, types.ConsistencyLevelOne)
	})

	t.Run("restart node 2", func(t *testing.T) {
		common.StartNodeAt(ctx, t, compose, 2)
	})

	t.Run("verify that all nodes are running", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			verbose := verbosity.OutputVerbose
			params := nodes.NewNodesGetClassParams().WithOutput(&verbose)
			body, clientErr := helper.Client(t).Nodes.NodesGetClass(params, nil)
			require.NoError(ct, clientErr)
			require.NotNil(ct, body.Payload)

			resp := body.Payload
			require.Len(ct, resp.Nodes, 3)
			for _, n := range resp.Nodes {
				require.NotNil(ct, n.Status)
				require.Equal(ct, "HEALTHY", *n.Status)
			}
		}, 15*time.Second, 500*time.Millisecond)
	})

	t.Run("assert updated object read repair was made", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			exists, err := common.ObjectExistsCL(t, compose.GetWeaviateNode(2).URI(),
				replaceObj.Class, replaceObj.ID, types.ConsistencyLevelOne)
			require.Nil(ct, err)
			require.True(ct, exists)

			resp, err := common.GetObjectCL(t, compose.GetWeaviate().URI(),
				repairObj.Class, repairObj.ID, types.ConsistencyLevelOne)
			require.Nil(ct, err)
			require.NotNil(ct, resp)

			if resp == nil {
				return
			}

			require.Equal(ct, replaceObj.ID, resp.ID)
			require.Equal(ct, replaceObj.Class, resp.Class)
			require.EqualValues(ct, replaceObj.Properties, resp.Properties)
			require.EqualValues(ct, replaceObj.Vector, resp.Vector)
		}, 120*time.Second, 5*time.Second, "not all the objects have been asynchronously replicated")
	})
}
