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

package alterschema

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func TestProperties_SingleNode(t *testing.T) {
	var compose *docker.DockerCompose
	var err error
	if os.Getenv("TEST_WEAVIATE_IMAGE") != "" {
		ctx := context.Background()
		compose, err = docker.New().
			WithWeaviate().
			WithText2VecModel2Vec().
			Start(ctx)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, compose.Terminate(ctx))
		}()

		helper.SetupClient(compose.GetWeaviate().URI())
		defer helper.ResetClient()
	}

	t.Run("delete property's index empty collection", testDeletePropertyIndexEmpty())
	t.Run("delete property's index multi-tenant", testDeletePropertyIndexMultiTenant(compose))
	t.Run("drop vector index", testDropVectorIndex(compose, true))
	t.Run("drop vector index multi-tenant", testDropVectorIndexMultiTenant(compose, true))
	// NOTE: "delete property's index" must run last because it destabilises the
	// Weaviate container (the class is not cleaned up and the text2vec-model2vec
	// module may leave the server in a bad state for subsequent tests).
	t.Run("delete property's index", testDeletePropertyIndex(compose))
}

func TestProperties_Cluster(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithText2VecModel2Vec().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	t.Run("delete property's index empty collection", testDeletePropertyIndexEmpty())
	t.Run("delete property's index multi-tenant", testDeletePropertyIndexMultiTenant(nil))
	t.Run("drop vector index", testDropVectorIndex(nil, false))
	t.Run("drop vector index multi-tenant", testDropVectorIndexMultiTenant(nil, false))
	t.Run("delete property's index", testDeletePropertyIndex(nil))
}
