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

func asyncRepairUpsertScenario(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	clusterSize := 3

	compose, err := docker.New().
		WithWeaviate().
		WithWeaviateClusterSize(clusterSize).
		WithText2VecContextionary().
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	paragraphClass := articles.ParagraphsClass()

	t.Run("create schema", func(t *testing.T) {
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor: int64(clusterSize),
		}
		paragraphClass.Vectorizer = "text2vec-contextionary"
		helper.CreateClass(t, paragraphClass)
	})

	for it := 0; it < 5; it++ {
		// pick one node to be down during upserts
		node := 1 + rand.Intn(clusterSize)

		t.Run(fmt.Sprintf("stop node %d", node), func(t *testing.T) {
			stopNodeAt(ctx, t, compose, 1)
		})

		t.Run("upsert paragraphs", func(t *testing.T) {
			batch := make([]*models.Object, len(paragraphIDs))
			for i, id := range paragraphIDs {
				batch[i] = articles.NewParagraph().
					WithID(id).
					WithContents(fmt.Sprintf("paragraph#%d_%d", it, i)).
					Object()
			}
			createObjectsCL(t, compose.GetWeaviate().URI(), batch, replica.One)
		})

		t.Run(fmt.Sprintf("restart node %d", node), func(t *testing.T) {
			restartNode(ctx, t, compose, clusterSize, node)
		})
	}

	time.Sleep(1 * time.Second)

	for n := 1; n <= clusterSize; n++ {
		t.Run(fmt.Sprintf("assert node %d has all the objects", n), func(t *testing.T) {
			count := countObjects(t, compose.GetWeaviateNode(n).URI(), paragraphClass.Class)
			require.EqualValues(t, len(paragraphIDs), count)
		})
	}
}

func restartNode(ctx context.Context, t *testing.T, compose *docker.DockerCompose, clusterSize, node int) {
	if node != 1 {
		stopNode(ctx, t, compose, compose.GetWeaviateNode(node).Name())
		time.Sleep(3 * time.Second)

		require.NoError(t, compose.Start(ctx, compose.GetWeaviateNode(node).Name()))
		time.Sleep(5 * time.Second) // wait for initialization
	}

	// since node1 is the gossip "leader", the other nodes must be stopped and restarted
	// after node1 to re-facilitate internode communication

	for n := clusterSize; n >= 1; n-- {
		stopNode(ctx, t, compose, compose.GetWeaviateNode(n).Name())
		time.Sleep(3 * time.Second)
	}

	for n := 1; n <= clusterSize; n++ {
		require.NoError(t, compose.Start(ctx, compose.GetWeaviateNode(n).Name()))
		time.Sleep(5 * time.Second) // wait for initialization
	}
}
