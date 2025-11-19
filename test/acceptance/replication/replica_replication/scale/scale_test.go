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

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/entities/models"
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

type ScaleTestSuite struct {
	suite.Suite
}

func (suite *ScaleTestSuite) SetupSuite() {
	t := suite.T()
	t.Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")
}

func TestScaleTestSuite(t *testing.T) {
	suite.Run(t, new(ScaleTestSuite))
}

func (suite *ScaleTestSuite) TestScalingSingleTenant() {
	t := suite.T()
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithText2VecContextionary().
		WithWeaviateEnv("REPLICA_MOVEMENT_MINIMUM_ASYNC_WAIT", "5s").
		WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true").
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	paragraphClass := articles.ParagraphsClass()

	shardsCount := 3

	t.Run("create schema", func(t *testing.T) {
		paragraphClass.ShardingConfig = map[string]interface{}{"desiredCount": shardsCount}
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor:       1,
			AsyncEnabled: false,
		}
		paragraphClass.Vectorizer = "text2vec-contextionary"
		helper.CreateClass(t, paragraphClass)
	})

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

	var scalePlan *models.ReplicationScalePlan

	t.Run("get scale plan to increase replication factor to 3", func(t *testing.T) {
		resp, err := helper.Client(t).Replication.
			GetReplicationScalePlan(replication.NewGetReplicationScalePlanParams().
				WithCollection(paragraphClass.Class).WithReplicationFactor(3), nil)
		require.Nil(t, err)
		require.Equal(t, http.StatusOK, resp.Code())
		require.NotNil(t, resp.Payload)

		scalePlan = resp.Payload
	})

	var operationIds []strfmt.UUID

	t.Run("apply scale plan", func(t *testing.T) {
		resp, err := helper.Client(t).Replication.
			ApplyReplicationScalePlan(replication.NewApplyReplicationScalePlanParams().
				WithBody(scalePlan), nil)
		require.Nil(t, err)
		require.Equal(t, http.StatusOK, resp.Code())
		require.NotNil(t, resp.Payload)

		require.Equal(t, scalePlan.PlanID, resp.Payload.PlanID)
		require.Equal(t, scalePlan.Collection, resp.Payload.Collection)

		operationIds = resp.Payload.OperationIds
	})

	t.Run("wait for replication operations to be READY", func(t *testing.T) {
		for _, opId := range operationIds {
			require.EventuallyWithT(t, func(ct *assert.CollectT) {
				details, err := helper.Client(t).Replication.ReplicationDetails(
					replication.NewReplicationDetailsParams().WithID(opId), nil,
				)
				require.Nil(ct, err, "failed to get replication details %s", err)
				require.NotNil(ct, details, "expected replication details to be not nil")
				require.NotNil(ct, details.Payload, "expected replication details payload to be not nil")
				require.NotNil(ct, details.Payload.Status, "expected replication status to be not nil")
				require.Equal(ct, "READY", details.Payload.Status.State, "expected replication status to be READY")
			}, 240*time.Second, 1*time.Second, "replication operation %s not finished in time", opId)
		}
	})

	t.Run("verify sharding state reflects new replication factor", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			resp, err := helper.Client(t).Replication.
				GetCollectionShardingState(replication.NewGetCollectionShardingStateParams().
					WithCollection(&paragraphClass.Class), nil)
			require.Nil(ct, err)
			require.Equal(ct, http.StatusOK, resp.Code())
			require.NotNil(ct, resp.Payload)
			require.Equal(t, paragraphClass.Class, resp.Payload.ShardingState.Collection)
			require.Equal(t, shardsCount, len(resp.Payload.ShardingState.Shards))

			for _, shard := range resp.Payload.ShardingState.Shards {
				require.Equal(ct, 3, len(shard.Replicas), "expected shard to have 3 replicas")
			}
		}, 30*time.Second, 1*time.Second, "sharding state does not reflect new replication factor")
	})

	t.Run("verify data is available on all nodes", func(t *testing.T) {
		for i := 1; i <= 3; i++ {
			nodeURI := compose.ContainerURI(i)
			require.EventuallyWithT(t, func(ct *assert.CollectT) {
				for _, objId := range paragraphIDs {
					obj, err := common.GetObjectFromNode(t, nodeURI, paragraphClass.Class, objId, fmt.Sprintf("node%d", i))
					require.Nil(ct, err)
					require.NotNil(ct, obj)
				}
			}, 30*time.Second, 1*time.Second, "node%d doesn't have paragraph data", i)
		}
	})

	t.Run("get scale plan to decrease replication factor to 1", func(t *testing.T) {
		resp, err := helper.Client(t).Replication.
			GetReplicationScalePlan(replication.NewGetReplicationScalePlanParams().
				WithCollection(paragraphClass.Class).WithReplicationFactor(1), nil)
		require.Nil(t, err)
		require.Equal(t, http.StatusOK, resp.Code())
		require.NotNil(t, resp.Payload)
		scalePlan = resp.Payload
	})

	t.Run("apply scale down plan", func(t *testing.T) {
		resp, err := helper.Client(t).Replication.
			ApplyReplicationScalePlan(replication.NewApplyReplicationScalePlanParams().
				WithBody(scalePlan), nil)
		require.Nil(t, err)
		require.Equal(t, http.StatusOK, resp.Code())
		require.NotNil(t, resp.Payload)
		require.Equal(t, scalePlan.PlanID, resp.Payload.PlanID)
		require.Equal(t, scalePlan.Collection, resp.Payload.Collection)
		operationIds = resp.Payload.OperationIds
	})

	t.Run("wait for replication operations to be READY after scale down", func(t *testing.T) {
		for _, opId := range operationIds {
			require.EventuallyWithT(t, func(ct *assert.CollectT) {
				details, err := helper.Client(t).Replication.ReplicationDetails(
					replication.NewReplicationDetailsParams().WithID(opId), nil,
				)
				require.Nil(ct, err)
				require.NotNil(ct, details)
				require.NotNil(ct, details.Payload)
				require.Equal(ct, "READY", details.Payload.Status.State)
			}, 240*time.Second, 1*time.Second)
		}
	})

	var shardingState *models.ReplicationShardingState

	t.Run("verify sharding state reflects new replication factor after scale down", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			resp, err := helper.Client(t).Replication.
				GetCollectionShardingState(replication.NewGetCollectionShardingStateParams().
					WithCollection(&paragraphClass.Class), nil)
			require.Nil(ct, err)
			require.Equal(ct, http.StatusOK, resp.Code())
			require.NotNil(ct, resp.Payload)
			require.Equal(t, shardsCount, len(resp.Payload.ShardingState.Shards))

			for _, shard := range resp.Payload.ShardingState.Shards {
				require.Equal(ct, 1, len(shard.Replicas), "expected shard to have 1 replica")
			}

			shardingState = resp.Payload.ShardingState
		}, 30*time.Second, 1*time.Second, "sharding state does not reflect new replication factor")
	})

	t.Run("verify data is available on remaining nodes after scale down", func(t *testing.T) {
		remainingNodes := make(map[string]struct{})
		for _, shard := range shardingState.Shards {
			for _, replica := range shard.Replicas {
				remainingNodes[replica] = struct{}{}
			}
		}

		for nodeName := range remainingNodes {
			require.EventuallyWithT(t, func(ct *assert.CollectT) {
				for _, objId := range paragraphIDs {
					obj, err := common.GetObjectFromNode(t, compose.GetWeaviate().URI(), paragraphClass.Class, objId, nodeName)
					require.Nil(ct, err)
					require.NotNil(ct, obj)
				}
			}, 30*time.Second, 1*time.Second, "node %s doesn't have paragraph data", nodeName)
		}
	})
}

func (suite *ScaleTestSuite) TestScalingMultiTenant() {
	t := suite.T()
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithText2VecContextionary().
		WithWeaviateEnv("REPLICA_MOVEMENT_MINIMUM_ASYNC_WAIT", "5s").
		WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true").
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	paragraphClass := articles.ParagraphsClass()

	t.Run("create schema (multi-tenant)", func(t *testing.T) {
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
	})

	helper.SetupClient(compose.GetWeaviate().URI())

	tenants := []string{"tenantA", "tenantB", "tenantC"}

	t.Run("insert paragraphs for each tenant", func(t *testing.T) {
		for _, tenant := range tenants {
			batch := make([]*models.Object, len(paragraphIDs))
			for i, id := range paragraphIDs {
				batch[i] = articles.NewParagraph().
					WithID(id).
					WithContents(fmt.Sprintf("paragraph#%d", i)).
					WithTenant(tenant).
					Object()
			}
			common.CreateObjects(t, compose.GetWeaviate().URI(), batch)
		}
	})

	var scalePlan *models.ReplicationScalePlan

	t.Run("get scale plan to increase replication factor to 3", func(t *testing.T) {
		resp, err := helper.Client(t).Replication.
			GetReplicationScalePlan(replication.NewGetReplicationScalePlanParams().
				WithCollection(paragraphClass.Class).WithReplicationFactor(3), nil)
		require.Nil(t, err)
		require.Equal(t, http.StatusOK, resp.Code())
		require.NotNil(t, resp.Payload)

		scalePlan = resp.Payload
	})

	var operationIds []strfmt.UUID

	t.Run("apply scale plan", func(t *testing.T) {
		resp, err := helper.Client(t).Replication.
			ApplyReplicationScalePlan(replication.NewApplyReplicationScalePlanParams().
				WithBody(scalePlan), nil)
		require.Nil(t, err)
		require.Equal(t, http.StatusOK, resp.Code())
		require.NotNil(t, resp.Payload)

		require.Equal(t, scalePlan.PlanID, resp.Payload.PlanID)
		require.Equal(t, scalePlan.Collection, resp.Payload.Collection)

		operationIds = resp.Payload.OperationIds
	})

	t.Run("wait for replication operations to be READY", func(t *testing.T) {
		for _, opId := range operationIds {
			require.EventuallyWithT(t, func(ct *assert.CollectT) {
				details, err := helper.Client(t).Replication.ReplicationDetails(
					replication.NewReplicationDetailsParams().WithID(opId), nil,
				)
				require.Nil(ct, err, "failed to get replication details %s", err)
				require.NotNil(ct, details, "expected replication details to be not nil")
				require.NotNil(ct, details.Payload, "expected replication details payload to be not nil")
				require.NotNil(ct, details.Payload.Status, "expected replication status to be not nil")
				require.Equal(ct, "READY", details.Payload.Status.State, "expected replication status to be READY")
			}, 240*time.Second, 1*time.Second, "replication operation %s not finished in time", opId)
		}
	})

	t.Run("verify sharding state reflects new replication factor", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			resp, err := helper.Client(t).Replication.
				GetCollectionShardingState(replication.NewGetCollectionShardingStateParams().
					WithCollection(&paragraphClass.Class), nil)
			require.Nil(ct, err)
			require.Equal(ct, http.StatusOK, resp.Code())
			require.NotNil(t, resp.Payload)
			require.Equal(t, paragraphClass.Class, resp.Payload.ShardingState.Collection)
			require.Equal(t, len(tenants), len(resp.Payload.ShardingState.Shards))

			for _, shard := range resp.Payload.ShardingState.Shards {
				require.Equal(ct, 3, len(shard.Replicas), "expected shard to have 3 replicas")
			}
		}, 30*time.Second, 1*time.Second, "sharding state does not reflect new replication factor")
	})

	t.Run("verify data is available on all nodes for each tenant", func(t *testing.T) {
		for i := 1; i <= 3; i++ {
			nodeURI := compose.ContainerURI(i)
			for _, tenant := range tenants {
				require.EventuallyWithT(t, func(ct *assert.CollectT) {
					for _, objId := range paragraphIDs {
						obj, err := common.GetTenantObjectFromNode(t, nodeURI, paragraphClass.Class, objId, fmt.Sprintf("node%d", i), tenant)
						require.Nil(ct, err)
						require.NotNil(ct, obj)
					}
				}, 30*time.Second, 1*time.Second, "node%d doesn't have paragraph data for tenant %s", i, tenant)
			}
		}
	})

	t.Run("get scale plan to decrease replication factor to 1", func(t *testing.T) {
		resp, err := helper.Client(t).Replication.
			GetReplicationScalePlan(replication.NewGetReplicationScalePlanParams().
				WithCollection(paragraphClass.Class).WithReplicationFactor(1), nil)
		require.Nil(t, err)
		require.Equal(t, http.StatusOK, resp.Code())
		require.NotNil(t, resp.Payload)
		scalePlan = resp.Payload
	})

	t.Run("apply scale down plan", func(t *testing.T) {
		resp, err := helper.Client(t).Replication.
			ApplyReplicationScalePlan(replication.NewApplyReplicationScalePlanParams().
				WithBody(scalePlan), nil)
		require.Nil(t, err)
		require.Equal(t, http.StatusOK, resp.Code())
		require.NotNil(t, resp.Payload)
		require.Equal(t, scalePlan.PlanID, resp.Payload.PlanID)
		require.Equal(t, scalePlan.Collection, resp.Payload.Collection)
		operationIds = resp.Payload.OperationIds
	})

	t.Run("wait for replication operations to be READY after scale down", func(t *testing.T) {
		for _, opId := range operationIds {
			require.EventuallyWithT(t, func(ct *assert.CollectT) {
				details, err := helper.Client(t).Replication.ReplicationDetails(
					replication.NewReplicationDetailsParams().WithID(opId), nil,
				)
				require.Nil(ct, err)
				require.NotNil(ct, details)
				require.NotNil(ct, details.Payload)
				require.Equal(ct, "READY", details.Payload.Status.State)
			}, 240*time.Second, 1*time.Second)
		}
	})

	var shardingState *models.ReplicationShardingState

	t.Run("verify sharding state reflects new replication factor after scale down", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			resp, err := helper.Client(t).Replication.
				GetCollectionShardingState(replication.NewGetCollectionShardingStateParams().
					WithCollection(&paragraphClass.Class), nil)
			require.Nil(ct, err)
			require.Equal(ct, http.StatusOK, resp.Code())
			require.NotNil(t, resp.Payload)
			require.Equal(t, len(tenants), len(resp.Payload.ShardingState.Shards))

			for _, shard := range resp.Payload.ShardingState.Shards {
				require.Equal(ct, 1, len(shard.Replicas), "expected shard to have 1 replica")
			}

			shardingState = resp.Payload.ShardingState
		}, 30*time.Second, 1*time.Second, "sharding state does not reflect new replication factor")
	})

	t.Run("verify data is available on remaining nodes after scale down for each tenant", func(t *testing.T) {
		remainingNodes := make(map[string]struct{})
		for _, shard := range shardingState.Shards {
			for _, replica := range shard.Replicas {
				remainingNodes[replica] = struct{}{}
			}
		}

		for nodeName := range remainingNodes {
			for _, tenant := range tenants {
				require.EventuallyWithT(t, func(ct *assert.CollectT) {
					for _, objId := range paragraphIDs {
						obj, err := common.GetTenantObjectFromNode(t, compose.GetWeaviate().URI(), paragraphClass.Class, objId, nodeName, tenant)
						require.Nil(ct, err)
						require.NotNil(ct, obj)
					}
				}, 30*time.Second, 1*time.Second, "node %s doesn't have paragraph data for tenant %s", nodeName, tenant)
			}
		}
	})
}
