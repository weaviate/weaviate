//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package replication

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/replica"
)

func consistencyLevel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateCluster().
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
			Factor: 2,
		}
		helper.CreateClass(t, paragraphClass)
	})

	t.Run("stop node 2", func(t *testing.T) {
		stopNode(ctx, t, compose, compose.GetWeaviateNode2().Name())
		time.Sleep(10 * time.Second)
	})

	t.Run("insert paragraphs", func(t *testing.T) {
		batch := make([]*models.Object, len(paragraphIDs))
		for i, id := range paragraphIDs {
			batch[i] = articles.NewParagraph().
				WithID(id).
				WithContents(fmt.Sprintf("paragraph#%d", i)).
				Object()
		}
		createObjectsCL(t, compose.GetWeaviate().URI(), batch, replica.One)
	})

	t.Run("restart node 2", func(t *testing.T) {
		err = compose.Start(ctx, compose.GetWeaviateNode2().Name())
		require.Nil(t, err)
	})

	// With consistencyLevel == ONE, node2 should return empty right away,
	// instead of timing out trying to contact node1
	t.Run("search to show that node2 wont attempt to contact node1 with cl ONE", func(t *testing.T) {
		resp := gqlGet(t, compose.GetWeaviateNode2().URI(), paragraphClass.Class, replica.One)
		// Because all paragraphs were added to node1 when node2 was down, the
		// response here should be empty since node2 does not contain any data
		assert.Empty(t, resp)
	})
}
