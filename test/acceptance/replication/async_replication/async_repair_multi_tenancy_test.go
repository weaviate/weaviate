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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

// In this scenario, we are testing two things:
//
//  1. The invocation of shard.UpdateAsyncReplication on an index with inactive tenants,
//     using ForEachLoadedShard to avoid force loading shards for the purpose of enabling
//     async replication, ensuring that if it is enabled when some tenants are inactive,
//     that the change is correctly applied to the tenants if they are later activated
//
//  2. That once (1) occurs, the hashBeat process is correctly initiated at the time that
//     the tenant is activated, and any missing objects are successfully propagated to the
//     nodes which are missing them.
//
// The actual scenario is as follows:
//   - Create a class with multi-tenancy enabled, replicated by a factor of 3
//   - Create a tenant, and set it to inactive
//   - Stop the second node
//   - Activate the tenant, and insert 1000 objects into the 2 surviving nodes
//   - Restart the second node and ensure it has an object count of 0
//   - Update the class to enabled async replication
//   - Wait a few seconds for objects to propagate
//   - Verify that the resurrected node contains all objects
func (suite *AsyncReplicationTestSuite) TestAsyncRepairMultiTenancyScenario() {
	t := suite.T()
	mainCtx := context.Background()

	var (
		clusterSize = 3
		tenantName  = "tenant-0"
		objectCount = 100
	)

	compose, err := docker.New().
		WithWeaviateCluster(clusterSize).
		WithText2VecContextionary().
		Start(mainCtx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(mainCtx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	ctx, cancel := context.WithTimeout(mainCtx, 15*time.Minute)
	defer cancel()

	paragraphClass := articles.ParagraphsClass()

	t.Run("create schema", func(t *testing.T) {
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor:       int64(clusterSize),
			AsyncEnabled: true,
		}
		paragraphClass.Vectorizer = "text2vec-contextionary"
		paragraphClass.MultiTenancyConfig = &models.MultiTenancyConfig{
			AutoTenantActivation: true,
			Enabled:              true,
		}

		helper.SetupClient(compose.GetWeaviate().URI())
		helper.CreateClass(t, paragraphClass)
	})

	t.Run("add inactive tenant", func(t *testing.T) {
		tenants := []*models.Tenant{{Name: tenantName, ActivityStatus: "COLD"}}
		helper.CreateTenants(t, paragraphClass.Class, tenants)
	})

	t.Run("stop node 2", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 2)
	})

	// Activate/insert tenants while node 2 is down
	t.Run("activate tenant and insert paragraphs", func(t *testing.T) {
		batch := make([]*models.Object, objectCount)
		for i := 0; i < objectCount; i++ {
			batch[i] = articles.NewParagraph().
				WithContents(fmt.Sprintf("paragraph#%d", i)).
				WithTenant(tenantName).
				Object()
		}
		common.CreateObjectsCL(t, compose.GetWeaviate().URI(), batch, types.ConsistencyLevelOne)
	})

	t.Run("start node 2", func(t *testing.T) {
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
			require.Len(ct, resp.Nodes, clusterSize)
			for _, n := range resp.Nodes {
				require.NotNil(ct, n.Status)
				require.Equal(ct, "HEALTHY", *n.Status)
			}
		}, 15*time.Second, 500*time.Millisecond)
	})

	t.Run("validate async object propagation", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			resp := common.GQLTenantGet(t, compose.GetWeaviateNode(2).URI(), paragraphClass.Class, types.ConsistencyLevelOne, tenantName)
			require.Len(ct, resp, objectCount)
		}, 120*time.Second, 5*time.Second, "not all the objects have been asynchronously replicated")
	})
}
