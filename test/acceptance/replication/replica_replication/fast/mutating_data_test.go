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
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client"
	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/client/graphql"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func (suite *ReplicationTestSuite) TestReplicationReplicateWhileMutatingDataWithNoAutomatedResolution() {
	test(suite, models.ReplicationConfigDeletionStrategyNoAutomatedResolution)
}

func (suite *ReplicationTestSuite) TestReplicationReplicateWhileMutatingDataWithDeleteOnConflict() {
	test(suite, models.ReplicationConfigDeletionStrategyDeleteOnConflict)
}

func (suite *ReplicationTestSuite) TestReplicationReplicateWhileMutatingDataWithTimeBasedResolution() {
	test(suite, models.ReplicationConfigDeletionStrategyTimeBasedResolution)
}

func test(suite *ReplicationTestSuite, strategy string) {
	t := suite.T()
	helper.SetupClient(suite.compose.GetWeaviate().URI())

	cls := articles.ParagraphsClass()
	cls.MultiTenancyConfig = &models.MultiTenancyConfig{
		Enabled:              true,
		AutoTenantActivation: true,
		AutoTenantCreation:   true,
	}

	cls.ReplicationConfig = &models.ReplicationConfig{
		Factor:           int64(2),
		DeletionStrategy: strategy,
	}

	// Create the class
	t.Log("Creating class", cls.Class)
	helper.DeleteClass(t, cls.Class)
	helper.CreateClass(t, cls)

	// Wait for all replication ops to be deleted
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		res, err := helper.Client(t).Replication.ListReplication(
			replication.NewListReplicationParams().WithCollection(&cls.Class),
			nil,
		)
		require.Nil(ct, err, "failed to list replication operations for class %s", cls.Class)
		assert.Empty(ct, res.Payload, "there are still replication operations for class %s", cls.Class)
	}, 30*time.Second, 5*time.Second, "replication operations for class %s did not finish in time", cls.Class)

	// Load data
	t.Log("Loading data into tenant")
	tenantName := "tenant"
	batch := make([]*models.Object, 1000)
	for j := 0; j < 1000; j++ {
		batch[j] = (*models.Object)(articles.NewParagraph().
			WithContents(fmt.Sprintf("paragraph#%d", j)).
			WithTenant(tenantName).
			Object())
	}
	helper.CreateObjectsBatch(t, batch)

	// Find the nodes on which the tenants are located
	t.Log("Finding nodes and tenant replicas")
	ns, err := helper.Client(t).Nodes.NodesGetClass(
		nodes.NewNodesGetClassParams().WithClassName(cls.Class),
		nil,
	)
	require.Nil(t, err)
	nodeNames := make([]string, 0, len(ns.Payload.Nodes))
	for _, node := range ns.Payload.Nodes {
		nodeNames = append(nodeNames, node.Name)
	}

	shardingState, err := helper.Client(t).Replication.GetCollectionShardingState(
		replication.NewGetCollectionShardingStateParams().WithCollection(&cls.Class),
		nil,
	)
	require.Nil(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Log("Starting data mutation in background")
	go mutateData(t, ctx, helper.Client(t), cls.Class, tenantName, 100)

	// Choose other node node as the target node
	var targetNode string
	var sourceNode string
	for _, shard := range shardingState.Payload.ShardingState.Shards {
		if shard.Shard != tenantName {
			continue
		}
		sourceNode = shard.Replicas[0]                                 // Take the first (of two) replica as the source node
		targetNode = symmetricDifference(nodeNames, shard.Replicas)[0] // Choose the other node as the target
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
	require.Nil(t, err, "failed to start replication for tenant %s from node %s to node %s", tenantName, sourceNode, targetNode)
	opId := *res.Payload.ID

	t.Log("Waiting for replication operation to complete")
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		res, err := helper.Client(t).Replication.ReplicationDetails(
			replication.NewReplicationDetailsParams().WithID(opId),
			nil,
		)
		require.Nil(t, err, "failed to get replication operation %s", opId)
		assert.True(ct, res.Payload.Status.State == models.ReplicationReplicateDetailsReplicaStatusStateREADY, "replication operation not completed yet")
	}, 300*time.Second, 5*time.Second, "replication operations did not complete in time")

	t.Log("Replication operation completed successfully, cancelling data mutation")
	cancel() // stop mutating to allow the verification to proceed

	t.Log("Waiting for a while to ensure all data is replicated")
	time.Sleep(time.Minute) // Wait a bit to ensure all data is replicated

	// Verify that shards all have consistent data
	t.Log("Verifying data consistency of tenant")

	verbose := verbosity.OutputVerbose
	ns, err = helper.Client(t).Nodes.NodesGetClass(
		nodes.NewNodesGetClassParams().WithClassName(cls.Class).WithOutput(&verbose),
		nil,
	)
	require.Nil(t, err)

	nodeToAddress := map[string]string{}
	for idx, node := range ns.Payload.Nodes {
		nodeToAddress[node.Name] = suite.compose.GetWeaviateNode(idx + 1).URI()
	}

	objectCountByReplica := make(map[string]int64)
	for node, address := range nodeToAddress {
		helper.SetupClient(address)
		res, err := helper.Client(t).Graphql.GraphqlPost(graphql.NewGraphqlPostParams().WithBody(&models.GraphQLQuery{
			Query: fmt.Sprintf(`{ Aggregate { %s(tenant: "%s") { meta { count } } } }`, cls.Class, tenantName),
		}), nil)
		require.Nil(t, err, "failed to get object count for tenant %s on node %s", tenantName, node)
		val, err := res.Payload.Data["Aggregate"].(map[string]any)["Paragraph"].([]any)[0].(map[string]any)["meta"].(map[string]any)["count"].(json.Number).Int64()
		require.Nil(t, err, "failed to parse object count for tenant %s on node %s", tenantName, node)
		objectCountByReplica[node] = val
	}

	// Verify that all replicas have the same number of objects
	t.Log("Verifying object counts across replicas")
	for node, count := range objectCountByReplica {
		t.Logf("Node %s has %d objects for tenant %s", node, count, tenantName)
		assert.Equal(t, objectCountByReplica[nodeNames[0]], count, "object count mismatch for tenant %s on node %s", tenantName, node)
	}
}

func mutateData(t *testing.T, ctx context.Context, client *client.Weaviate, className string, tenantName string, wait int) {
	for {
		select {
		case <-ctx.Done():
			t.Log("Mutation context done, stopping data mutation")
			return
		default:
			// Add some new objects
			randAdd := rand.Intn(20) + 1
			btch := make([]*models.Object, randAdd)
			for i := 0; i < randAdd; i++ {
				btch[i] = (*models.Object)(articles.NewParagraph().
					WithContents(fmt.Sprintf("new-paragraph#%d", i)).
					WithTenant(tenantName).
					Object())
			}
			all := "ALL"
			params := batch.NewBatchObjectsCreateParams().
				WithBody(batch.BatchObjectsCreateBody{
					Objects: btch,
				}).WithConsistencyLevel(&all)
			client.Batch.BatchObjectsCreate(params, nil)

			time.Sleep(time.Duration(wait) * time.Millisecond) // Sleep to simulate some delay between mutations

			// Get the existing objects
			limit := int64(10000)
			res, err := client.Objects.ObjectsList(
				objects.NewObjectsListParams().WithClass(&className).WithTenant(&tenantName).WithLimit(&limit),
				nil,
			)
			if err != nil {
				t.Logf("Error listing objects for tenant %s: %v", tenantName, err)
				continue
			}
			randUpdate := rand.Intn(20) + 1
			toUpdate := random(res.Payload.Objects, randUpdate)
			randDelete := rand.Intn(20) + 1
			toDelete := random(symmetricDifference(res.Payload.Objects, toUpdate), randDelete)

			time.Sleep(time.Duration(wait) * time.Millisecond) // Sleep to simulate some delay between mutations

			// Update some existing objects
			for _, obj := range toUpdate {
				updated := (*models.Object)(articles.NewParagraph().
					WithContents(fmt.Sprintf("updated-%s", obj.Properties.(map[string]any)["contents"])).
					WithTenant(tenantName).
					WithID(obj.ID).
					Object())
				client.Objects.ObjectsClassPut(
					objects.NewObjectsClassPutParams().WithID(obj.ID).WithBody(updated).WithConsistencyLevel(&all),
					nil,
				)
			}

			time.Sleep(time.Duration(wait) * time.Millisecond) // Sleep to simulate some delay between mutations

			// Delete some existing objects
			for _, obj := range toDelete {
				client.Objects.ObjectsClassDelete(
					objects.NewObjectsClassDeleteParams().WithClassName(className).WithID(obj.ID).WithTenant(&tenantName).WithConsistencyLevel(&all),
					nil,
				)
			}
		}
	}
}

// symmetricDifference returns the symmetric difference of two slices.
// It returns a slice containing elements that are in either a or b, but not in both
func symmetricDifference[T comparable](a, b []T) []T {
	count := make(map[T]int)

	for _, v := range a {
		count[v]++
	}
	for _, v := range b {
		count[v]++
	}

	var result []T
	for k, v := range count {
		if v == 1 {
			result = append(result, k)
		}
	}
	return result
}
