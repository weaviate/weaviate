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

package collection_config_tests

import (
	"context"
	"net"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate-go-client/v6"
	"github.com/weaviate/weaviate-go-client/v6/collections"
	"github.com/weaviate/weaviate-go-client/v6/collections/vectorindex"
	"github.com/weaviate/weaviate-go-client/v6/modules/selfprovided"
	"github.com/weaviate/weaviate/test/docker"
)

func TestUpdateNamedVectorIndexType_SingleNode(t *testing.T) {
	c := newClient(t, docker.New().WithWeaviateWithGRPC())
	testUpdateNamedVectorIndexType(t, c)
}

func TestUpdateNamedVectorIndexType_Cluster(t *testing.T) {
	c := newClient(t, docker.New().WithWeaviateCluster(3).WithWeaviateExposeGRPCPort())
	testUpdateNamedVectorIndexType(t, c)
}

// testUpdateNamedVectorIndexType checks that changing the vector index type of
// a named vector is rejected with 422. The server used to panic on this update
// and answer 500.
func testUpdateNamedVectorIndexType(t *testing.T, c *weaviate.Client) {
	ctx := context.Background()
	indexes := map[string]collections.VectorIndex{
		"hnsw": vectorindex.HNSW{},
		"flat": vectorindex.Flat{},
	}
	for _, tt := range []struct{ from, to string }{
		{from: "hnsw", to: "flat"},
		{from: "flat", to: "hnsw"},
	} {
		t.Run(tt.from+" to "+tt.to, func(t *testing.T) {
			name := "UpdateNamedVectorIndexType"
			require.NoError(t, c.Collections.Delete(ctx, name))
			t.Cleanup(func() { require.NoError(t, c.Collections.Delete(ctx, name)) })

			h, err := c.Collections.Create(ctx, collections.Collection{
				Name:       name,
				Properties: []collections.Property{{Name: "name", DataType: collections.DataTypeText}},
				Vectors: map[string]collections.VectorConfig{
					"v": {Vectorizer: selfprovided.Vectorizer, Index: indexes[tt.from]},
				},
			})
			require.NoError(t, err)

			// The stored compression settings belong to the old index type. Without
			// clearing them the server rejects them before it checks the type change.
			err = h.Config.UpdateVectorConfig(ctx, "v", func(vc *collections.VectorConfig) {
				vc.Index = indexes[tt.to]
				vc.Compression = nil
			})
			require.Error(t, err, "changing the index type of a named vector must be rejected")
			var httpErr *weaviate.HTTPError
			require.ErrorAs(t, err, &httpErr, "expected an HTTP error, got: %v", err)
			assert.Equal(t, http.StatusUnprocessableEntity, httpErr.Code, "body: %s", httpErr.Body)
			assert.Contains(t, httpErr.Body, `vector index type of vector \"v\" is immutable`)

			got, err := c.Collections.GetConfig(ctx, name)
			require.NoError(t, err)
			require.NotNil(t, got.Vectors["v"].Index)
			assert.Equal(t, vectorindex.Type(tt.from), got.Vectors["v"].Index.Name())
		})
	}
}

func newClient(t *testing.T, compose *docker.Compose) *weaviate.Client {
	t.Helper()
	ctx := context.Background()
	env, err := compose.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Terminate(ctx)) })

	host, httpPort, err := net.SplitHostPort(env.GetWeaviate().URI())
	require.NoError(t, err)
	_, grpcPort, err := net.SplitHostPort(env.GetWeaviate().GrpcURI())
	require.NoError(t, err)

	c, err := weaviate.NewLocal(ctx,
		weaviate.WithHost(host),
		weaviate.WithHTTPPort(httpPort),
		weaviate.WithGRPCPort(grpcPort),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	return c
}
