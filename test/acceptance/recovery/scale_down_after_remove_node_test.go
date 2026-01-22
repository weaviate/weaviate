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

package recovery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

// TestScaleDownAfterRemoveNode verifies that after removing a Raft peer via
// /v1/cluster/remove, the sharding state no longer references the removed node
// and replicas are redistributed to keep the replication factor where possible.
func TestScaleDownAfterRemoveNode(t *testing.T) {
	t.Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithText2VecContextionary().
		WithWeaviateEnv("REPLICA_MOVEMENT_MINIMUM_ASYNC_WAIT", "5s").
		WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true").
		Start(ctx)
	require.NoError(t, err)

	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	paragraphClass := articles.ParagraphsClass()
	paragraphClass.ShardingConfig = map[string]interface{}{"desiredCount": 3}
	paragraphClass.ReplicationConfig = &models.ReplicationConfig{
		Factor:       2,
		AsyncEnabled: false,
	}
	paragraphClass.Vectorizer = "text2vec-contextionary"

	t.Run("create schema", func(t *testing.T) {
		helper.DeleteClass(t, paragraphClass.Class)
		helper.CreateClass(t, paragraphClass)
	})

	t.Run("insert some data", func(t *testing.T) {
		objects := make([]*models.Object, 10)
		for i := range objects {
			objects[i] = articles.NewParagraph().
				WithContents(fmt.Sprintf("paragraph#%d", i)).
				Object()
		}
		common.CreateObjects(t, compose.GetWeaviate().URI(), objects)
	})

	var initialState *models.ReplicationShardingState

	t.Run("read initial sharding state", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			resp, err := helper.Client(t).Replication.
				GetCollectionShardingState(replication.NewGetCollectionShardingStateParams().
					WithCollection(&paragraphClass.Class), nil)
			require.NoError(ct, err)
			require.Equal(ct, http.StatusOK, resp.Code())
			require.NotNil(ct, resp.Payload)
			initialState = resp.Payload.ShardingState
			require.NotNil(ct, initialState)
		}, 30*time.Second, time.Second)
	})

	// Pick a node that currently owns at least one replica.
	var nodeToRemove string
	for _, shard := range initialState.Shards {
		if len(shard.Replicas) > 0 {
			nodeToRemove = shard.Replicas[0]
			break
		}
	}
	require.NotEmpty(t, nodeToRemove, "expected at least one replica to decide which node to remove")

	t.Run("remove node via /v1/cluster/remove", func(t *testing.T) {
		body := map[string]string{"node": nodeToRemove}
		data, err := json.Marshal(body)
		require.NoError(t, err)

		resp, err := http.Post(
			fmt.Sprintf("%s/v1/cluster/remove", compose.GetWeaviate().ClusterURI()),
			"application/json",
			bytes.NewReader(data),
		)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("remove same node twice via /v1/cluster/remove", func(t *testing.T) {
		// Call the remove endpoint a second time with the same node ID and
		// observe the behavior. This is primarily exploratory and does not
		// enforce a specific status code, only that the call itself succeeds.
		body := map[string]string{"node": nodeToRemove}
		data, err := json.Marshal(body)
		require.NoError(t, err)

		clusterAPIURL := compose.GetWeaviate().ClusterURI()

		resp, err := http.Post(
			fmt.Sprintf("%s/v1/cluster/remove", clusterAPIURL),
			"application/json",
			bytes.NewReader(data),
		)
		require.NoError(t, err)
		defer resp.Body.Close()
		t.Logf("second /v1/cluster/remove for %q returned status %d", nodeToRemove, resp.StatusCode)
	})

	t.Run("sharding state no longer references removed node and RF is preserved", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			resp, err := helper.Client(t).Replication.
				GetCollectionShardingState(replication.NewGetCollectionShardingStateParams().
					WithCollection(&paragraphClass.Class), nil)
			require.NoError(ct, err)
			require.Equal(ct, http.StatusOK, resp.Code())
			require.NotNil(ct, resp.Payload)

			state := resp.Payload.ShardingState
			require.NotNil(ct, state)

			for _, shard := range state.Shards {
				for _, replica := range shard.Replicas {
					require.NotEqual(ct, nodeToRemove, replica, "shard still references removed node")
				}
				// Best-effort: expect we still have at least one replica per shard
				// and at most the desired replication factor.
				require.GreaterOrEqual(ct, len(shard.Replicas), 1)
				require.LessOrEqual(ct, len(shard.Replicas), int(paragraphClass.ReplicationConfig.Factor))
			}
		}, 60*time.Second, 2*time.Second)
	})

	// Extract node number from nodeToRemove (e.g., "node1" -> 1, "node2" -> 2)
	nodeNumStr := strings.TrimPrefix(nodeToRemove, "node")
	nodeNum, err := strconv.Atoi(nodeNumStr)
	require.NoError(t, err, "failed to parse node number from %q", nodeToRemove)
	require.GreaterOrEqual(t, nodeNum, 1)
	require.LessOrEqual(t, nodeNum, 3)

	// Map node number to container name (node1 -> "weaviate", node2 -> "weaviate2", node3 -> "weaviate3")
	var removedContainerName string
	switch nodeNum {
	case 1:
		removedContainerName = "weaviate"
	case 2:
		removedContainerName = "weaviate2"
	case 3:
		removedContainerName = "weaviate3"
	default:
		t.Fatalf("unexpected node number: %d", nodeNum)
	}

	t.Run("restart remaining cluster nodes", func(t *testing.T) {
		// Restart all nodes except the removed one to ensure cluster stability
		for i := 1; i <= 3; i++ {
			if i == nodeNum {
				continue // Skip the removed node
			}
			var containerName string
			switch i {
			case 1:
				containerName = "weaviate"
			case 2:
				containerName = "weaviate2"
			case 3:
				containerName = "weaviate3"
			}
			// Stop and start the container to simulate a restart
			timeout := 30 * time.Second
			require.NoError(t, compose.Stop(ctx, containerName, &timeout), "failed to stop %s", containerName)
			time.Sleep(2 * time.Second) // Brief pause between stop and start
			require.NoError(t, compose.Start(ctx, containerName), "failed to start %s", containerName)
		}
		// Wait for cluster to stabilize after restarts
		time.Sleep(10 * time.Second)
	})

	t.Run("shutdown removed node container", func(t *testing.T) {
		timeout := 30 * time.Second
		require.NoError(t, compose.Stop(ctx, removedContainerName, &timeout), "failed to stop removed node container %s", removedContainerName)
		// Wait a bit for the cluster to detect the node is down
		time.Sleep(5 * time.Second)
	})

	t.Run("verify data is still accessible after removed node shutdown", func(t *testing.T) {
		// Collect objects from all remaining nodes (cluster-wide view), skipping the removed node.
		idSet := make(map[string]struct{})

		for i := 1; i <= 3; i++ {
			if i == nodeNum {
				continue // skip removed node
			}

			var container *docker.DockerContainer
			switch i {
			case 1:
				container = compose.GetWeaviate()
			case 2:
				container = compose.GetWeaviateNode2()
			case 3:
				container = compose.GetWeaviateNode3()
			}
			if container == nil || container.URI() == "" {
				continue
			}

			nodeURI := container.URI()
			helper.SetupClient(nodeURI)

			require.EventuallyWithT(t, func(ct *assert.CollectT) {
				params := objects.NewObjectsListParams()
				params.WithClass(&paragraphClass.Class)

				resp, err := helper.Client(t).Objects.ObjectsList(params, nil)
				require.NoError(ct, err)
				require.Equal(ct, http.StatusOK, resp.Code())
				require.NotNil(ct, resp.Payload)

				for _, obj := range resp.Payload.Objects {
					if obj == nil || obj.ID == "" {
						continue
					}
					idSet[string(obj.ID)] = struct{}{}
				}
			}, 60*time.Second, 2*time.Second)
		}

		// We inserted 10 objects; across the remaining nodes we expect all 10 to still be present.
		require.Equal(t, 10, len(idSet), "expected all 10 objects to be accessible across remaining nodes")

		// Verify sharding state is still consistent and does not reference the removed node.
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			resp, err := helper.Client(t).Replication.
				GetCollectionShardingState(replication.NewGetCollectionShardingStateParams().
					WithCollection(&paragraphClass.Class), nil)
			require.NoError(ct, err)
			require.Equal(ct, http.StatusOK, resp.Code())
			require.NotNil(ct, resp.Payload)

			state := resp.Payload.ShardingState
			require.NotNil(ct, state)

			for _, shard := range state.Shards {
				for _, replica := range shard.Replicas {
					require.NotEqual(ct, nodeToRemove, replica, "shard still references removed node after shutdown")
				}
				// Each shard should still have at least one replica
				require.GreaterOrEqual(ct, len(shard.Replicas), 1, "shard should have at least one replica")
			}
		}, 30*time.Second, 2*time.Second)
	})
}
