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
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper/sample-schema/multimodal"
)

func TestMulti2VecTwelveLabs_SingleNode(t *testing.T) {
	apiKey := os.Getenv("TWELVELABS_APIKEY")
	if apiKey == "" {
		t.Skip("skipping, TWELVELABS_APIKEY environment variable not present")
	}
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviate().
		WithMulti2VecTwelveLabs(apiKey).
		WithWeaviateEnv("MODULES_CLIENT_TIMEOUT", fmt.Sprintf("%.0fs", multimodal.DefaultTimeout.Seconds())).
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()
	endpoint := compose.GetWeaviate().URI()

	t.Run("multi2vec-twelvelabs", testMulti2VecTwelveLabs(endpoint))
}
