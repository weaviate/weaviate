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

// TestAsyncRepairRootPrefilterManyTenants exercises the batched hashtree-root
// pre-filter end-to-end: a node restarts and every MT tenant reconciles via it.
func (suite *AsyncReplicationTestSuite) TestAsyncRepairRootPrefilterManyTenants() {
	t := suite.T()
	mainCtx := context.Background()

	var (
		clusterSize      = 3
		tenantCount      = 12
		objectsPerTenant = 20
	)

	ctx, cancel := context.WithTimeout(mainCtx, 15*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateCluster(clusterSize).
		WithText2VecContextionary().
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	paragraphClass := articles.ParagraphsClass()

	tenantNames := make([]string, tenantCount)
	for i := range tenantNames {
		tenantNames[i] = fmt.Sprintf("tenant-%d", i)
	}

	t.Run("create multi-tenant schema replicated with async enabled", func(t *testing.T) {
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor: int64(clusterSize),
		}
		paragraphClass.Vectorizer = "text2vec-contextionary"
		paragraphClass.MultiTenancyConfig = &models.MultiTenancyConfig{
			AutoTenantActivation: true,
			Enabled:              true,
		}

		helper.SetupClient(compose.GetWeaviate().URI())
		helper.CreateClass(t, paragraphClass)
	})

	t.Run("create tenants", func(t *testing.T) {
		tenants := make([]*models.Tenant, tenantCount)
		for i, name := range tenantNames {
			tenants[i] = &models.Tenant{Name: name, ActivityStatus: "HOT"}
		}
		helper.CreateTenants(t, paragraphClass.Class, tenants)
	})

	node := 3

	t.Run(fmt.Sprintf("stop node %d", node), func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, node)
	})

	t.Run("insert objects into every tenant on the surviving nodes", func(t *testing.T) {
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

	t.Run("every tenant reconciles on the restarted node via the batched pre-filter", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			for _, tenant := range tenantNames {
				resp := common.GQLTenantGet(t, compose.GetWeaviateNode(node).URI(),
					paragraphClass.Class, types.ConsistencyLevelOne, tenant)
				require.Len(ct, resp, objectsPerTenant, "tenant %s not fully reconciled", tenant)
			}
		}, 180*time.Second, 5*time.Second, "not all tenants were asynchronously reconciled")
	})
}
