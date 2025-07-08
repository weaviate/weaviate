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
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/graphql"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

type movement struct {
	source string
	target string
	shard  string
}

// func (suite *ReplicationTestSuite) TestReplicationReplicateScaleOut() {
// 	t := suite.T()

// helper.SetupClient(suite.compose.GetWeaviate().URI())
func TestReplicationReplicateScaleOut(t *testing.T) {
	helper.SetupClient("localhost:8080")

	cls := articles.ParagraphsClass()
	cls.MultiTenancyConfig = &models.MultiTenancyConfig{
		Enabled:              true,
		AutoTenantActivation: true,
		AutoTenantCreation:   true,
	}
	cls.ReplicationConfig = &models.ReplicationConfig{
		Factor: 1,
	}

	// Create the class
	helper.DeleteClass(t, cls.Class)
	helper.CreateClass(t, cls)

	// Load data
	batch := make([]*models.Object, 0, 10000)
	tenantNames := make([]string, 0, 1000)
	t.Log("Loading data into tenants...")
	for i := 0; i < 1000; i++ {
		tenantName := fmt.Sprintf("tenant-%d", i)
		tenantNames = append(tenantNames, tenantName)
		for j := 0; j < 100; j++ {
			batch = append(batch, (*models.Object)(articles.NewParagraph().
				WithContents(fmt.Sprintf("paragraph#%d", j)).
				WithTenant(tenantName).
				Object()))
		}
		if len(batch) == 10000 {
			helper.CreateObjectsBatch(t, batch)
			batch = batch[:0] // reset batch for next iteration
		}
	}
	if len(batch) > 0 {
		helper.CreateObjectsBatch(t, batch)
	}

	ns, err := helper.Client(t).Nodes.NodesGet(
		nodes.NewNodesGetParams(), nil,
	)
	require.Nil(t, err)
	nodeNames := make([]string, 0, len(ns.Payload.Nodes))
	for _, node := range ns.Payload.Nodes {
		nodeNames = append(nodeNames, node.Name)
	}
	require.Len(t, nodeNames, 3)

	shardingState, err := helper.Client(t).Replication.GetCollectionShardingState(
		replication.NewGetCollectionShardingStateParams().WithCollection(&cls.Class), nil,
	)
	require.Nil(t, err)
	require.Len(t, shardingState.Payload.ShardingState.Shards, 1000)

	movements := []movement{}
	for _, state := range shardingState.Payload.ShardingState.Shards {
		replica := state.Replicas[0]
		for _, node := range nodeNames {
			if node == replica {
				continue
			}
			movements = append(movements, movement{
				source: replica,
				target: node,
				shard:  state.Shard,
			})
		}
	}

	t.Logf("Running %d scale out operations", len(movements))

	req := []*models.ReplicationReplicateReplicaRequest{}
	for _, movement := range movements {
		req = append(req, &models.ReplicationReplicateReplicaRequest{
			SourceNode: &movement.source,
			TargetNode: &movement.target,
			Shard:      &movement.shard,
			Collection: &cls.Class,
		})
	}
	_, err = helper.Client(t).Replication.ReplicateMany(
		replication.NewReplicateManyParams().WithBody(req),
		nil,
	)
	if err != nil {
		parsed, ok := err.(*replication.ReplicateInternalServerError)
		if ok {
			t.Logf("Replication error: %s", parsed.Payload.Error[0].Message)
		} else {
			t.Logf("Replication error: %s", err.Error())
		}
	}
	require.Nil(t, err, "failed to start batch replications")

	// Wait until all ops are in the READY state
	assert.EventuallyWithT(t, func(ct *assert.CollectT) {
		ops, err := helper.Client(t).Replication.ListReplication(
			replication.NewListReplicationParams().WithCollection(&cls.Class), nil,
		)
		require.Nil(ct, err, "failed to list replication operations")
		for _, op := range ops.Payload {
			assert.Equal(ct, "READY", op.Status, "replication operation should be in READY state")
		}
	}, 10*time.Minute, 1*time.Second, "not all replication operations are in READY state")

	// Assert that data is the same on each node
	// nodeToAddress := map[string]string{}
	// for idx, node := range ns.Payload.Nodes {
	// 	nodeToAddress[node.Name] = suite.compose.GetWeaviateNode(idx + 1).URI()
	// }
	nodeToAddress := map[string]string{
		"weaviate-voter-0": "localhost:8080",
		"weaviate-voter-1": "localhost:8081",
		"weaviate-1":       "localhost:8082",
	}

	objectCountByReplica := map[string]map[string]int64{}
	for _, node := range nodeNames {
		objectCountByReplica[node] = make(map[string]int64)
	}
	for _, tenantName := range tenantNames {
		for node, address := range nodeToAddress {
			helper.SetupClient(address)
			res, err := helper.Client(t).Graphql.GraphqlPost(graphql.NewGraphqlPostParams().WithBody(&models.GraphQLQuery{
				Query: fmt.Sprintf(`{ Aggregate { %s(tenant: "%s") { meta { count } } } }`, cls.Class, tenantName),
			}), nil)
			require.Nil(t, err, "failed to get object count for tenant %s on node %s", tenantName, node)
			val, err := res.Payload.Data["Aggregate"].(map[string]any)["Paragraph"].([]any)[0].(map[string]any)["meta"].(map[string]any)["count"].(json.Number).Int64()
			require.Nil(t, err, "failed to parse object count for tenant %s on node %s", tenantName, node)
			objectCountByReplica[node][tenantName] = val
		}
	}

	// Verify that all replicas have the same number of objects
	t.Log("Verifying object counts across replicas")
	var expectedCount int64
	var comparisonReplica string
	for _, tenantName := range tenantNames {
		for replica, count := range objectCountByReplica {
			if expectedCount == 0 {
				expectedCount = count[tenantName]
				comparisonReplica = replica
			} else {
				require.Equal(t, expectedCount, count, "object counts across replicas do not match. Expected %d as on %s but got %d for replica %s instead", expectedCount, comparisonReplica, count, replica)
			}
		}
	}
}
