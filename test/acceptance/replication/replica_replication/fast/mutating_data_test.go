//	_       _
//
// __      _____  __ ___   ___  __ _| |_ ___
//
//	\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//	 \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//	  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//	 Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//	 CONTACT: hello@weaviate.io

package replication

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func TestReplicationReplicateWhileMutatingData(t *testing.T) {
	helper.SetupClient("localhost:8080")

	cls := articles.ParagraphsClass()
	cls.MultiTenancyConfig = &models.MultiTenancyConfig{
		Enabled:              true,
		AutoTenantActivation: true,
		AutoTenantCreation:   true,
	}
	cls.ReplicationConfig = &models.ReplicationConfig{
		Factor: int64(2),
	}

	// Create the class
	t.Log("Creating class", cls.Class)
	helper.DeleteClass(t, cls.Class)
	helper.CreateClass(t, cls)

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

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	t.Log("Starting data mutation in background")
	go mutateData(t, ctx, cls.Class, tenantName)

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
	assert.EventuallyWithT(t, func(ct *assert.CollectT) {
		res, err := helper.Client(t).Replication.ReplicationDetails(
			replication.NewReplicationDetailsParams().WithID(opId),
			nil,
		)
		require.Nil(t, err, "failed to get replication operation %s", opId)
		assert.True(ct, res.Payload.Status.State == models.ReplicationReplicateDetailsReplicaStatusStateREADY, "replication operation not completed yet")
	}, 300*time.Second, 5*time.Second, "replication operations did not complete in time")

	// Verify that shards all have consistent data
	t.Log("Verifying data consistency of tenant")

	verbose := verbosity.OutputVerbose
	ns, err = helper.Client(t).Nodes.NodesGetClass(
		nodes.NewNodesGetClassParams().WithClassName(cls.Class).WithOutput(&verbose),
		nil,
	)
	require.Nil(t, err)

	objectCountByReplica := make(map[string]int64)
	for _, node := range ns.Payload.Nodes {
		for _, shard := range node.Shards {
			if shard.Name != tenantName {
				continue
			}
			objectCountByReplica[node.Name] = shard.ObjectCount
		}
	}

	// Verify that all replicas have the same number of objects
	t.Log("Verifying object counts across replicas")
	var expectedCount int64
	var comparisonReplica string
	for replica, count := range objectCountByReplica {
		if expectedCount == 0 {
			expectedCount = count
			comparisonReplica = replica
		} else {
			assert.Equal(t, expectedCount, count, "object counts across replicas do not match. Expected %d as on %s but got %d for replica %s instead", expectedCount, comparisonReplica, count, replica)
		}
	}
}

func mutateData(t *testing.T, ctx context.Context, className string, tenantName string) {
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
			_, err := helper.Client(t).Batch.BatchObjectsCreate(params, nil)
			if err != nil {
				t.Logf("Error creating batch objects for tenant %s: %v", tenantName, err)
			}

			time.Sleep(100 * time.Millisecond) // Sleep to simulate some delay between mutations

			// Get the existing objects
			limit := int64(10000)
			res, err := helper.Client(t).Objects.ObjectsList(
				objects.NewObjectsListParams().WithClass(&className).WithTenant(&tenantName).WithLimit(&limit),
				nil,
			)
			require.Nil(t, err)
			randUpdate := rand.Intn(20) + 1
			toUpdate := random(res.Payload.Objects, randUpdate)
			randDelete := rand.Intn(20) + 1
			toDelete := random(symmetricDifference(res.Payload.Objects, toUpdate), randDelete)

			time.Sleep(100 * time.Millisecond) // Sleep to simulate some delay between mutations

			// Update some existing objects
			for _, obj := range toUpdate {
				updated := (*models.Object)(articles.NewParagraph().
					WithContents(fmt.Sprintf("updated-%s", obj.Properties.(map[string]any)["contents"])).
					WithTenant(tenantName).
					WithID(obj.ID).
					Object())
				_, err := helper.Client(t).Objects.ObjectsClassPut(
					objects.NewObjectsClassPutParams().WithID(obj.ID).WithBody(updated).WithConsistencyLevel(&all),
					nil,
				)
				if err != nil {
					parsed, ok := err.(*objects.ObjectsClassPutInternalServerError)
					if ok {
						t.Logf("Internal Server error Error updating object %s for tenant %s: %v", obj.ID, tenantName, parsed.Payload.Error[0].Message)
					} else {
						t.Logf("Error updating object %s for tenant %s: %v", obj.ID, tenantName, err)
					}
				}
			}

			time.Sleep(100 * time.Millisecond) // Sleep to simulate some delay between mutations

			// Delete some existing objects
			for _, obj := range toDelete {
				_, err := helper.Client(t).Objects.ObjectsClassDelete(
					objects.NewObjectsClassDeleteParams().WithClassName(className).WithID(obj.ID).WithTenant(&tenantName).WithConsistencyLevel(&all),
					nil,
				)
				if err != nil {
					t.Logf("Error deleting object %s for tenant %s: %v", obj.ID, tenantName, err)
				}
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
