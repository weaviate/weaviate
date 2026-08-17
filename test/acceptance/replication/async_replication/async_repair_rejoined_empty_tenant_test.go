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

// TestAsyncRepairRejoinedNodeEmptyTenantReplica: a node is down for a tenant's
// first writes and rejoins with empty, unloaded tenant shards. Async replication
// must repair them without anything touching the tenants on that node (#12526);
// the /nodes probe used here reports unloaded shards without loading them.
func (suite *AsyncReplicationTestSuite) TestAsyncRepairRejoinedNodeEmptyTenantReplica() {
	t := suite.T()
	mainCtx := context.Background()

	var (
		clusterSize      = 3
		tenantCount      = 3
		objectsPerTenant = 20
		node             = 2
		nodeName         = docker.Weaviate1
	)

	ctx, cancel := context.WithTimeout(mainCtx, 15*time.Minute)
	defer cancel()

	compose := suite.compose

	paragraphClass := articles.ParagraphsClass()

	tenantNames := make([]string, tenantCount)
	for i := range tenantNames {
		tenantNames[i] = fmt.Sprintf("tenant-%d", i)
	}

	t.Run("create multi-tenant schema replicated on every node", func(t *testing.T) {
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor: int64(clusterSize),
		}
		paragraphClass.Vectorizer = "text2vec-contextionary"
		paragraphClass.MultiTenancyConfig = &models.MultiTenancyConfig{
			Enabled: true,
		}

		helper.SetupClient(compose.GetWeaviate().URI())
		helper.CreateClass(t, paragraphClass)
	})

	t.Run("create hot tenants while every node is up", func(t *testing.T) {
		tenants := make([]*models.Tenant, tenantCount)
		for i, name := range tenantNames {
			tenants[i] = &models.Tenant{Name: name, ActivityStatus: models.TenantActivityStatusHOT}
		}
		helper.CreateTenants(t, paragraphClass.Class, tenants)
	})

	t.Run(fmt.Sprintf("stop node %d", node), func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, node)
	})

	t.Run("insert the tenants' first objects on the surviving nodes", func(t *testing.T) {
		for _, tenant := range tenantNames {
			batch := make([]*models.Object, objectsPerTenant)
			for i := range batch {
				batch[i] = articles.NewParagraph().
					WithContents(fmt.Sprintf("%s#%d", tenant, i)).
					WithTenant(tenant).
					Object()
			}
			common.CreateObjectsCL(t, compose.GetWeaviate().URI(), batch, types.ConsistencyLevelOne)
		}
	})

	t.Run(fmt.Sprintf("restart node %d", node), func(t *testing.T) {
		common.StartNodeAt(ctx, t, compose, node)
	})

	t.Run("all nodes healthy", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			verbose := verbosity.OutputVerbose
			params := nodes.NewNodesGetClassParams().WithOutput(&verbose)
			body, clientErr := helper.Client(t).Nodes.NodesGetClass(params, nil)
			require.NoError(ct, clientErr)
			require.NotNil(ct, body.Payload)
			require.Len(ct, body.Payload.Nodes, clusterSize)
			for _, n := range body.Payload.Nodes {
				require.NotNil(ct, n.Status)
				require.Equal(ct, "HEALTHY", *n.Status)
			}
		}, 30*time.Second, 500*time.Millisecond)
	})

	t.Run("every tenant is repaired on the restarted node without being touched there", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			verbose := verbosity.OutputVerbose
			params := nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(paragraphClass.Class)
			body, clientErr := helper.Client(t).Nodes.NodesGetClass(params, nil)
			require.NoError(ct, clientErr)
			require.NotNil(ct, body.Payload)

			counts := map[string]int64{}
			for _, n := range body.Payload.Nodes {
				if n.Name != nodeName {
					continue
				}
				for _, shard := range n.Shards {
					if shard.Class == paragraphClass.Class {
						counts[shard.Name] = shard.ObjectCount
					}
				}
			}
			for _, tenant := range tenantNames {
				require.EqualValues(ct, objectsPerTenant, counts[tenant],
					"tenant %s on %s not repaired: %v", tenant, nodeName, counts)
			}
		}, 240*time.Second, 5*time.Second, "the restarted node did not converge")
	})

	t.Run("the repaired tenants are readable on the restarted node", func(t *testing.T) {
		for _, tenant := range tenantNames {
			resp := common.GQLTenantGet(t, compose.GetWeaviateNode(node).URI(),
				paragraphClass.Class, types.ConsistencyLevelOne, tenant)
			require.Len(t, resp, objectsPerTenant, "tenant %s", tenant)
		}
	})
}
