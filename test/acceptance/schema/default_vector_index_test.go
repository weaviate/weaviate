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

package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func TestDefaultVectorIndexEmpty(t *testing.T) {
	mainCtx := context.Background()

	compose, err := docker.New().
		WithWeaviate().
		Start(mainCtx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(mainCtx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	t.Run("no env defaults to hnsw", func(t *testing.T) {
		cls := articles.ParagraphsClass()
		cls.ReplicationConfig = &models.ReplicationConfig{Factor: 1}

		helper.DeleteClass(t, cls.Class)
		helper.CreateClass(t, cls)

		got := helper.GetClass(t, cls.Class)
		require.Equal(t, "hnsw", got.VectorIndexType)
	})
}

func TestDefaultVectorIndexHfresh(t *testing.T) {
	mainCtx := context.Background()

	compose, err := docker.New().
		WithWeaviate().
		WithWeaviateEnv("DEFAULT_VECTOR_INDEX", "hfresh").
		Start(mainCtx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(mainCtx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	t.Run("env set to hfresh defaults to hfresh", func(t *testing.T) {
		cls := articles.ParagraphsClass()
		cls.ReplicationConfig = &models.ReplicationConfig{Factor: 1}

		helper.DeleteClass(t, cls.Class)
		helper.CreateClass(t, cls)

		got := helper.GetClass(t, cls.Class)
		require.Equal(t, "hfresh", got.VectorIndexType)
	})
}
