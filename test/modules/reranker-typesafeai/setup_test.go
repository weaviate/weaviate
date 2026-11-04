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
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/test/docker"
)

func TestRerankerTypeSafeAI(t *testing.T) {
	apiKey := os.Getenv("TYPESAFEAI_APIKEY")
	if apiKey == "" {
		t.Skip("skipping, TYPESAFEAI_APIKEY environment variable not present")
	}
	ctx := context.Background()
	compose, err := docker.New().
		WithRerankerTypeSafeAI(apiKey).
		WithWeaviateWithGRPC().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	t.Run("reranker-typesafeai", testRerankerTypeSafeAI(compose.GetWeaviate().URI(), compose.GetWeaviate().GrpcURI()))
}

func TestRerankerTypeSafeAIAgainstAFakeProvider(t *testing.T) {
	fake := newFakeProvider(t)
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviateWithGRPC().
		WithWeaviateHostGateway().
		WithRerankerTypeSafeAI(fakeAPIKey).
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	t.Run("reranker-typesafeai", testRerankerTypeSafeAIAgainstAFakeProvider(fake,
		compose.GetWeaviate().URI(), compose.GetWeaviate().GrpcURI()))
}
