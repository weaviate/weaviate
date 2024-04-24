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
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/replica"
)

func asyncRepairObjectInsertionScenario(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	clusterSize := 3

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

	t.Run("create schema", func(t *testing.T) {
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor:       int64(clusterSize),
			AsyncEnabled: true,
		}
		paragraphClass.Vectorizer = "text2vec-contextionary"

		helper.SetupClient(compose.GetWeaviate().URI())
		helper.CreateClass(t, paragraphClass)
	})

	itCount := 3

	for it := 0; it < itCount; it++ {
		// pick one node to be down during upserts
		node := 1 + rand.Intn(clusterSize)

		t.Run(fmt.Sprintf("stop node %d", node), func(t *testing.T) {
			stopNodeAt(ctx, t, compose, node)
		})

		t.Run("insert paragraphs", func(t *testing.T) {
			batch := make([]*models.Object, len(paragraphIDs))
			for i := range paragraphIDs {
				batch[i] = articles.NewParagraph().
					WithContents(fmt.Sprintf("paragraph#%d_%d", it, i)).
					Object()
			}

			// choose one more node to insert the objects into
			var targetNode int
			for {
				targetNode = 1 + rand.Intn(clusterSize)
				if targetNode != node {
					break
				}
			}

			createObjectsCL(t, compose.GetWeaviateNode(targetNode).URI(), batch, replica.One)
		})

		t.Run(fmt.Sprintf("restart node %d", node), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()

			restartNode(ctx, t, compose, clusterSize, node)
		})
	}

	time.Sleep(1 * time.Second)

	for n := 1; n <= clusterSize; n++ {
		t.Run(fmt.Sprintf("assert node %d has all the objects", n), func(t *testing.T) {
			count := countObjects(t, compose.GetWeaviateNode(n).URI(), paragraphClass.Class)
			require.EqualValues(t, itCount*len(paragraphIDs), count)
		})
	}
}

func restartNode(ctx context.Context, t *testing.T, compose *docker.DockerCompose, clusterSize, node int) {
	if node != 1 {
		stopNodeAt(ctx, t, compose, node)
		time.Sleep(10 * time.Second)

		require.NoError(t, compose.Start(ctx, compose.GetWeaviateNode(node).Name()))
		time.Sleep(5 * time.Second) // wait for initialization
	}

	// since node1 is the gossip "leader", the other nodes must be stopped and restarted
	// after node1 to re-facilitate internode communication

	for n := clusterSize; n >= 1; n-- {
		stopNodeAt(ctx, t, compose, n)
		time.Sleep(10 * time.Second)
	}

	for n := 1; n <= clusterSize; n++ {
		require.NoError(t, compose.Start(ctx, compose.GetWeaviateNode(n).Name()))
		time.Sleep(5 * time.Second) // wait for initialization
	}
}
