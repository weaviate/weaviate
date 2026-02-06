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

package properties

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

	t.Run("delete property's index multi-tenant", testDeletePropertyIndexMultiTenant())
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

	t.Run("delete property's index multi-tenant", testDeletePropertyIndexMultiTenant())
	t.Run("delete property's index", testDeletePropertyIndex(nil))
}
