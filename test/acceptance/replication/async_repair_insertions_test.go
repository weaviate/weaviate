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
	t.Skip()

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

	itCount := 1

	for it := 0; it < itCount; it++ {
		// pick one node to be down during upserts
		node := 2 + rand.Intn(clusterSize-1)

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
			startNodeAt(ctx, t, compose, node)
			time.Sleep(time.Second)
		})
	}

	// wait for some time for async replication to repair missing object
	time.Sleep(3 * time.Second)

	for n := 1; n <= clusterSize; n++ {
		t.Run(fmt.Sprintf("assert node %d has all the objects", n), func(t *testing.T) {
			count := countObjects(t, compose.GetWeaviateNode(n).URI(), paragraphClass.Class)
			require.EqualValues(t, itCount*len(paragraphIDs), count)
		})
	}
}
