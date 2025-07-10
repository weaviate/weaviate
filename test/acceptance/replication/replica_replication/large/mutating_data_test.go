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

package large

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	httptransport "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/weaviate/weaviate/client"
	apiclient "github.com/weaviate/weaviate/client"
	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/client/graphql"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

type ReplicationTestSuite struct {
	suite.Suite
}

func (suite *ReplicationTestSuite) SetupTest() {
	suite.T().Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")
}

func TestReplicationTestSuite(t *testing.T) {
	suite.Run(t, new(ReplicationTestSuite))
}

func (suite *ReplicationTestSuite) TestReplicationReplicateWhileMutatingData() {
	t := suite.T()
	mainCtx := context.Background()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		Start(mainCtx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(mainCtx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	// move := "MOVE"
	copy := "COPY"

	// t.Run("MOVE, rf=2, no automated resolution", func(t *testing.T) {
	// 	test(t, compose, move, 2, models.ReplicationConfigDeletionStrategyNoAutomatedResolution)
	// })
	// t.Run("MOVE, rf=2, delete on conflict", func(t *testing.T) {
	// 	test(t, compose, move, 2, models.ReplicationConfigDeletionStrategyDeleteOnConflict)
	// })
	// t.Run("MOVE, rf=2, time-based resolution", func(t *testing.T) {
	// 	test(t, compose, move, 2, models.ReplicationConfigDeletionStrategyTimeBasedResolution)
	// })

	t.Run("COPY, rf=1, no automated resolution", func(t *testing.T) {
		test(t, compose, copy, 1, models.ReplicationConfigDeletionStrategyNoAutomatedResolution)
	})
	// t.Run("COPY, rf=1, delete on conflict", func(t *testing.T) {
	// 	test(t, compose, copy, 1, models.ReplicationConfigDeletionStrategyDeleteOnConflict)
	// })
	// t.Run("COPY, rf=1, time-based resolution", func(t *testing.T) {
	// 	test(t, compose, copy, 1, models.ReplicationConfigDeletionStrategyTimeBasedResolution)
	// })
}

func test(t *testing.T, compose *docker.DockerCompose, replicationType string, factor int, strategy string) {
	helper.SetupClient(compose.GetWeaviate().URI())

	cls := articles.ParagraphsClass()
	cls.MultiTenancyConfig = &models.MultiTenancyConfig{
		Enabled:              true,
		AutoTenantActivation: true,
		AutoTenantCreation:   true,
	}

	cls.ReplicationConfig = &models.ReplicationConfig{
		Factor:           int64(factor),
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

	verbose := verbosity.OutputVerbose
	ns, err = helper.Client(t).Nodes.NodesGetClass(
		nodes.NewNodesGetClassParams().WithClassName(cls.Class).WithOutput(&verbose),
		nil,
	)
	require.Nil(t, err)

	nodeToAddress := map[string]string{}
	for idx, node := range ns.Payload.Nodes {
		nodeToAddress[node.Name] = compose.GetWeaviateNode(idx + 1).URI()
	}

	// Choose other node as the target node
	var sourceNode string
	var targetNode string
	for _, shard := range shardingState.Payload.ShardingState.Shards {
		if shard.Shard != tenantName {
			continue
		}
		sourceNode = shard.Replicas[0]                                 // Take the first (of two) replica as the source node
		targetNode = symmetricDifference(nodeNames, shard.Replicas)[0] // Choose the other node as the target
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Logf("Starting data mutation in background targeting ")
	go mutateData(t, ctx, cls.Class, tenantName, 100, nodeToAddress)

	// Start replication
	t.Logf("Starting %s replication for tenant %s from node %s to target node %s", replicationType, tenantName, sourceNode, targetNode)
	res, err := helper.Client(t).Replication.Replicate(
		replication.NewReplicateParams().WithBody(&models.ReplicationReplicateReplicaRequest{
			SourceNode: &sourceNode,
			TargetNode: &targetNode,
			Collection: &cls.Class,
			Shard:      &tenantName,
			Type:       &replicationType,
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

func mutateData(t *testing.T, ctx context.Context, className string, tenantName string, wait int, nodeToAddress map[string]string) {
	for {
		select {
		case <-ctx.Done():
			t.Log("Mutation context done, stopping data mutation")
			return
		default:
			// Select a random node to mutate data to on this iteration
			nodeNames := make([]string, 0, len(nodeToAddress))
			for node := range nodeToAddress {
				nodeNames = append(nodeNames, node)
			}
			client := newClient(nodeToAddress[random(nodeNames, 1)[0]])

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

func random[T any](s []T, k int) []T {
	if k > len(s) {
		k = len(s)
	}
	indices := rand.Perm(len(s))[:k]
	result := make([]T, k)
	for i, idx := range indices {
		result[i] = s[idx]
	}
	return result
}

func newClient(address string) *client.Weaviate {
	transport := httptransport.New(address, "/v1", []string{"http"})
	return apiclient.New(transport, strfmt.Default)
}
