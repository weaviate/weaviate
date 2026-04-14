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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

// In this scenario, we are testing that async replication correctly propagates
// objects to a node that missed writes while it was stopped, when multi-tenancy
// is involved and the tenant was in COLD state at the time of the writes.
//
// The actual scenario is as follows:
//   - Create a class with multi-tenancy enabled, replicated by a factor of 3
//     (async replication is always on for RF > 1)
//   - Create a tenant, and set it to inactive (COLD)
//   - Stop the second node
//   - Activate the tenant, and insert objects into the 2 surviving nodes
//   - Restart the second node
//   - Verify that the resurrected node eventually receives all objects via
//     async replication
func (suite *AsyncReplicationTestSuite) TestAsyncRepairMultiTenancyScenario() {
	t := suite.T()

	var (
		tenantName  = "tenant-0"
		objectCount = 100
	)

	paragraphClass := articles.ParagraphsClass()

	t.Cleanup(func() {
		// best-effort: ignore error if class was never created
		helper.Client(t).Schema.SchemaObjectsDelete(
			schema.NewSchemaObjectsDeleteParams().WithClassName(paragraphClass.Class),
			nil,
		)
	})

	t.Run("create schema", func(t *testing.T) {
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor: 3,
		}
		paragraphClass.Vectorizer = "text2vec-contextionary"
		paragraphClass.MultiTenancyConfig = &models.MultiTenancyConfig{
			AutoTenantActivation: true,
			Enabled:              true,
		}

		helper.SetupClient(suite.compose.GetWeaviate().URI())
		helper.CreateClass(t, paragraphClass)
	})

	t.Run("add inactive tenant", func(t *testing.T) {
		tenants := []*models.Tenant{{Name: tenantName, ActivityStatus: "COLD"}}
		helper.CreateTenants(t, paragraphClass.Class, tenants)
	})

	t.Run("stop node 2", func(t *testing.T) {
		common.StopNodeAt(suite.suiteCtx, t, suite.compose, 2)
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
		common.CreateObjectsCL(t, suite.compose.GetWeaviate().URI(), batch, types.ConsistencyLevelOne)
	})

	t.Run("start node 2", func(t *testing.T) {
		common.StartNodeAt(suite.suiteCtx, t, suite.compose, 2)
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

	t.Run("validate async object propagation", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			resp := common.GQLTenantGet(t, suite.compose.GetWeaviateNode(2).URI(), paragraphClass.Class, types.ConsistencyLevelOne, tenantName)
			require.Len(ct, resp, objectCount)
		}, 120*time.Second, 5*time.Second, "not all the objects have been asynchronously replicated")
	})
}

// In this scenario, we are testing that a tenant in COLD state correctly
// initialises async replication when it is lazily loaded (activated). The
// scenario is:
//
//   - Create a class with multi-tenancy enabled, replicated by a factor of 3
//     (async replication is always on for RF > 1)
//   - Create a tenant in COLD state
//   - Stop one node
//   - Insert data into the surviving nodes (this will activate the tenant)
//   - Restart the stopped node
//   - Verify that the restarted node eventually receives all the data,
//     demonstrating that the tenant correctly picked up async replication
//     when it was lazily loaded/activated
func (suite *AsyncReplicationTestSuite) TestAsyncRepairMultiTenancyColdTenantConfigUpdate() {
	t := suite.T()

	var (
		tenantName  = "tenant-0"
		objectCount = 100
	)

	paragraphClass := articles.ParagraphsClass()

	t.Cleanup(func() {
		// best-effort: ignore error if class was never created
		helper.Client(t).Schema.SchemaObjectsDelete(
			schema.NewSchemaObjectsDeleteParams().WithClassName(paragraphClass.Class),
			nil,
		)
	})

	t.Run("create schema", func(t *testing.T) {
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor: 3,
		}
		paragraphClass.Vectorizer = "text2vec-contextionary"
		paragraphClass.MultiTenancyConfig = &models.MultiTenancyConfig{
			AutoTenantActivation: true,
			Enabled:              true,
		}

		helper.SetupClient(suite.compose.GetWeaviate().URI())
		helper.CreateClass(t, paragraphClass)
	})

	t.Run("add tenant in cold state", func(t *testing.T) {
		tenants := []*models.Tenant{{Name: tenantName, ActivityStatus: "COLD"}}
		helper.CreateTenants(t, paragraphClass.Class, tenants)
	})

	t.Run("stop node 2", func(t *testing.T) {
		common.StopNodeAt(suite.suiteCtx, t, suite.compose, 2)
	})

	t.Run("insert paragraphs (this will activate the tenant)", func(t *testing.T) {
		batch := make([]*models.Object, objectCount)
		for i := 0; i < objectCount; i++ {
			batch[i] = articles.NewParagraph().
				WithContents(fmt.Sprintf("paragraph#%d", i)).
				WithTenant(tenantName).
				Object()
		}
		common.CreateObjectsCL(t, suite.compose.GetWeaviate().URI(), batch, types.ConsistencyLevelOne)
	})

	t.Run("start node 2", func(t *testing.T) {
		common.StartNodeAt(suite.suiteCtx, t, suite.compose, 2)
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

	t.Run("validate async object propagation to restarted node", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			resp := common.GQLTenantGet(t, suite.compose.GetWeaviateNode(2).URI(), paragraphClass.Class, types.ConsistencyLevelOne, tenantName)
			require.Len(ct, resp, objectCount)
		}, 120*time.Second, 5*time.Second, "not all the objects have been asynchronously replicated to the restarted node")
	})
}
