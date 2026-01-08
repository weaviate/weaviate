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
	if useLocalWeaviate := os.Getenv("TEST_USE_LOCAL_WEAVIATE"); useLocalWeaviate == "" {
		ctx := context.Background()
		compose, err := docker.New().
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

	t.Run("update property", testDeletePropertyIndex())
}
