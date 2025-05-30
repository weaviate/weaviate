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
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func (suite *ReplicationTestSuite) TestReplicationDeletingTenantCleansUpOperations() {
	t := suite.T()

	helper.SetupClient(suite.compose.GetWeaviate().URI())
	paragraphClass := articles.ParagraphsClass()
	paragraphClass.MultiTenancyConfig = &models.MultiTenancyConfig{
		AutoTenantCreation: true,
		Enabled:            true,
	}

	tenant1 := "tenant1"
	tenant2 := "tenant2"

	stateToDeleteIn := []api.ShardReplicationState{
		api.REGISTERED,
		api.HYDRATING,
		api.FINALIZING,
	}

	for _, state := range stateToDeleteIn {
		t.Run(fmt.Sprintf("delete tenant when op is %s", state), func(t *testing.T) {
			helper.DeleteClass(t, paragraphClass.Class)
			helper.CreateClass(t, paragraphClass)

			t.Run(fmt.Sprintf("insert paragraphs into %s", tenant1), func(t *testing.T) {
				batch := make([]*models.Object, 5000)
				for i := 0; i < 5000; i++ {
					batch[i] = (*models.Object)(articles.NewParagraph().
						WithContents(fmt.Sprintf("paragraph#%d", i)).
						WithTenant(tenant1).
						Object())
				}
				helper.CreateObjectsBatch(t, batch)
			})

			t.Run(fmt.Sprintf("insert paragraphs into %s", tenant2), func(t *testing.T) {
				batch := make([]*models.Object, 5000)
				for i := 0; i < 5000; i++ {
					batch[i] = (*models.Object)(articles.NewParagraph().
						WithContents(fmt.Sprintf("paragraph#%d", i)).
						WithTenant(tenant2).
						Object())
				}
				helper.CreateObjectsBatch(t, batch)
			})

			var id1 strfmt.UUID
			t.Run(fmt.Sprintf("create replication operation for %s", tenant1), func(t *testing.T) {
				created, err := helper.Client(t).Replication.Replicate(replication.NewReplicateParams().WithBody(getMTRequest(t, paragraphClass.Class, tenant1)), nil)
				require.Nil(t, err)
				require.NotNil(t, created)
				require.NotNil(t, created.Payload)
				require.NotNil(t, created.Payload.ID)
				id1 = *created.Payload.ID
			})

			var id2 strfmt.UUID
			t.Run(fmt.Sprintf("create replication operation for %s", tenant2), func(t *testing.T) {
				created, err := helper.Client(t).Replication.Replicate(replication.NewReplicateParams().WithBody(getMTRequest(t, paragraphClass.Class, tenant2)), nil)
				require.Nil(t, err)
				require.NotNil(t, created)
				require.NotNil(t, created.Payload)
				require.NotNil(t, created.Payload.ID)
				id2 = *created.Payload.ID
			})

			if state != api.REGISTERED {
				t.Run(fmt.Sprintf("wait until op for %s is in %s state", tenant1, state), func(t *testing.T) {
					assert.EventuallyWithT(t, func(ct *assert.CollectT) {
						details, err := helper.Client(t).Replication.ReplicationDetails(replication.NewReplicationDetailsParams().WithID(id1), nil)
						require.Nil(ct, err)
						require.Equal(ct, state.String(), details.Payload.Status.State)
					}, 30*time.Second, 100*time.Millisecond, "replication operation should be in %s state", state)
				})
			}

			t.Run(fmt.Sprintf("delete %s", tenant1), func(t *testing.T) {
				helper.DeleteTenants(t, paragraphClass.Class, []string{tenant1})
			})

			t.Run("wait for replication operation to be deleted", func(t *testing.T) {
				assert.EventuallyWithT(t, func(ct *assert.CollectT) {
					_, err := helper.Client(t).Replication.ReplicationDetails(replication.NewReplicationDetailsParams().WithID(id1), nil)
					require.NotNil(ct, err)
					assert.IsType(ct, replication.NewReplicationDetailsNotFound(), err)
				}, 30*time.Second, 1*time.Second, "replication operation should be deleted")
			})

			t.Run(fmt.Sprintf("ensure that the replication operation for %s is still there", tenant2), func(t *testing.T) {
				details, err := helper.Client(t).Replication.ReplicationDetails(replication.NewReplicationDetailsParams().WithID(id2), nil)
				require.Nil(t, err)
				require.NotNil(t, details)
				require.NotNil(t, details.Payload)
				require.NotNil(t, details.Payload.ID)
				require.Equal(t, id2, *details.Payload.ID)
			})

			// Required so we can loop again for other replication operation stages without waiting for it to complete
			// and potentially colliding with replication ops on a per-shard basis, which is a 500

			t.Run(fmt.Sprintf("delete %s", tenant2), func(t *testing.T) {
				helper.DeleteTenants(t, paragraphClass.Class, []string{tenant2})
			})

			t.Run("wait for replication operation to be deleted", func(t *testing.T) {
				assert.EventuallyWithT(t, func(ct *assert.CollectT) {
					_, err := helper.Client(t).Replication.ReplicationDetails(replication.NewReplicationDetailsParams().WithID(id2), nil)
					require.NotNil(ct, err)
					assert.IsType(ct, replication.NewReplicationDetailsNotFound(), err)
				}, 30*time.Second, 1*time.Second, "replication operation should be deleted")
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
		})
	}
}

func getMTRequest(t *testing.T, className, tenant string) *models.ReplicationReplicateReplicaRequest {
	verbosity := verbosity.OutputVerbose
	nodes, err := helper.Client(t).Nodes.NodesGetClass(nodes.NewNodesGetClassParams().WithOutput(&verbosity).WithClassName(className), nil)
	require.Nil(t, err)

	// find node with tenant
	sourceNode := ""
	for _, node := range nodes.Payload.Nodes {
		for _, shard := range node.Shards {
			if shard.Name == tenant {
				sourceNode = node.Name
				break
			}
		}
	}

	// find node without tenant
	destinationNode := ""
	for _, node := range nodes.Payload.Nodes {
		shards := make([]string, 0)
		for _, shard := range node.Shards {
			shards = append(shards, shard.Name)
		}
		if sourceNode != node.Name && !slices.Contains(shards, tenant) {
			destinationNode = node.Name
			break
		}
	}

	return &models.ReplicationReplicateReplicaRequest{
		CollectionID:        &className,
		SourceNodeName:      &sourceNode,
		DestinationNodeName: &destinationNode,
		ShardID:             &tenant,
	}
}
