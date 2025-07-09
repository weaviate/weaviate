//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package replication

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func (suite *ReplicationTestSuite) TestReplicationReplicateWithLazyShardLoading() {
	t := suite.T()
	helper.SetupClient(suite.compose.GetWeaviate().URI())

	cls := articles.ParagraphsClass()
	cls.MultiTenancyConfig = &models.MultiTenancyConfig{
		Enabled:              true,
		AutoTenantActivation: true,
		AutoTenantCreation:   true,
	}

	// Create the class
	t.Log("Creating class", cls.Class)
	helper.DeleteClass(t, cls.Class)
	helper.CreateClass(t, cls)

	// Load data
	t.Log("Loading data...")
	tenantNames := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		t.Logf("into tenant %d", i)
		tenantName := fmt.Sprintf("tenant-%d", i)
		tenantNames = append(tenantNames, tenantName)
		batch := make([]*models.Object, 1000)
		for j := 0; j < 1000; j++ {
			batch[j] = (*models.Object)(articles.NewParagraph().
				WithContents(fmt.Sprintf("paragraph#%d", j)).
				WithTenant(tenantName).
				Object())
		}
		helper.CreateObjectsBatch(t, batch)
	}

	// Get a random subset of tenants to replicate
	t.Log("Selecting random tenants for replication")
	randomTenants := randomTenants(tenantNames, len(tenantNames)/4)

	// Find the nodes on which the tenants are located
	t.Log("Finding nodes for tenants")
	verbose := verbosity.OutputVerbose
	nodes, err := helper.Client(t).Nodes.NodesGetClass(
		nodes.NewNodesGetClassParams().WithClassName(cls.Class).WithOutput(&verbose),
		nil,
	)
	require.Nil(t, err)

	replicaByTenant := make(map[string]string, len(randomTenants))
	nodeNames := make([]string, 0, len(nodes.Payload.Nodes))
	for _, node := range nodes.Payload.Nodes {
		nodeNames = append(nodeNames, node.Name)
		for _, shard := range node.Shards {
			for _, tenantName := range randomTenants {
				if shard.Name == tenantName {
					replicaByTenant[tenantName] = node.Name
				}
			}
		}
	}
	t.Log(replicaByTenant, nodeNames)

	// Deactivate all the tenants
	t.Log("Deactivating all tenants")
	tenants := make([]*models.Tenant, 0, 1000)
	for _, tenantName := range tenantNames {
		tenants = append(tenants, &models.Tenant{Name: tenantName, ActivityStatus: models.TenantActivityStatusINACTIVE})
	}
	helper.UpdateTenants(t, cls.Class, tenants)

	opIds := make([]strfmt.UUID, 0, len(replicaByTenant))
	for tenantName, sourceNode := range replicaByTenant {
		t.Run(fmt.Sprintf("replicate tenant %s on node %s", tenantName, sourceNode), func(t *testing.T) {
			// Choose random node as the target node
			var targetNode string
			rand.Shuffle(len(nodeNames), func(i, j int) {
				nodeNames[i], nodeNames[j] = nodeNames[j], nodeNames[i]
			})
			for _, tgtNode := range nodeNames {
				if sourceNode != tgtNode {
					targetNode = tgtNode
					t.Logf("Selected target node %s for tenant %s", targetNode, tenantName)
					break
				}
			}

			// Start replication
			t.Logf("Starting replication for tenant %s from node %s to target node %s", tenantName, sourceNode, targetNode)
			move := "MOVE"
			res, err := helper.Client(t).Replication.Replicate(
				replication.NewReplicateParams().WithBody(&models.ReplicationReplicateReplicaRequest{
					SourceNode: &sourceNode,
					TargetNode: &targetNode,
					Collection: &cls.Class,
					Shard:      &tenantName,
					Type:       &move,
				}),
				nil,
			)
			require.Nil(t, err, "failed to start replication for tenant %s on node %s", tenantName, sourceNode)
			opIds = append(opIds, *res.Payload.ID)
		})
	}

	// Wait for all replication operations to complete
	t.Log("Waiting for replication operations to complete")
	assert.EventuallyWithT(t, func(ct *assert.CollectT) {
		completed := make(map[strfmt.UUID]bool, len(opIds))
		for _, opId := range opIds {
			res, err := helper.Client(t).Replication.ReplicationDetails(
				replication.NewReplicationDetailsParams().WithID(opId),
				nil,
			)
			require.Nil(t, err, "failed to get replication operation %s", opId)
			if res.Payload.Status.State == models.ReplicationReplicateDetailsReplicaStatusStateREADY {
				completed[opId] = true
			} else {
				completed[opId] = false
			}
		}
		howManyDone := 0
		for _, done := range completed {
			if done {
				howManyDone++
			}
		}
		t.Log("Replication operations completed:", howManyDone, "of", len(completed))
		assert.True(ct, howManyDone == len(completed), "not all replication operations completed yet")
	}, 300*time.Second, 5*time.Second, "replication operations did not complete in time")
}

func randomTenants(s []string, k int) []string {
	if k > len(s) {
		k = len(s)
	}
	indices := rand.Perm(len(s))[:k]
	result := make([]string, k)
	for i, idx := range indices {
		result[i] = s[idx]
	}
	return result
}
