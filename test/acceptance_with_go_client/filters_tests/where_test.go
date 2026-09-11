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

package filters_tests

import (
	"acceptance_tests_with_client/internal/wvhost"
	"context"
	"log"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate-go-client/v6"
	"github.com/weaviate/weaviate/test/docker"
)

func TestWhereFilter_SingleNode_Contains(t *testing.T) {
	c := wvhost.NewClient(t)
	t.Run("Contains", testContains(c))
	t.Run("Contains text", testContainsText(c))
	t.Run("Contains movies", testContainsMovies(c))
}

func TestWhereFilter_SingleNode_Numerical(t *testing.T) {
	ctx := context.Background()

	t.Run("with rangeable on disk", func(t *testing.T) {
		compose, err := docker.New().
			WithWeaviate().
			Start(ctx)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, compose.Terminate(ctx))
		}()

		c := newClusterClient(t, compose.GetWeaviate())
		t.Run("numerical filters", testNumericalFilters(c))
	})

	t.Run("with rangeable in memory", func(t *testing.T) {
		compose, err := docker.New().
			WithWeaviate().
			WithWeaviateEnv("INDEX_RANGEABLE_IN_MEMORY", "true").
			Start(ctx)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, compose.Terminate(ctx))
		}()

		c := newClusterClient(t, compose.GetWeaviate())
		t.Run("numerical filters", testNumericalFilters(c))
	})
}

func TestWhereFilter_Cluster(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithText2VecContextionary().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	c := newClusterClient(t, compose.GetWeaviate())
	t.Run("Contains", testContains(c))
	t.Run("Contains text", testContainsText(c))
	t.Run("Contains movies", testContainsMovies(c))
	t.Run("Numerical filters", testNumericalFilters(c))
}

func newClusterClient(t *testing.T, dc *docker.DockerContainer) *weaviate.Client {
	t.Helper()

	var err error
	restHost, restPort, err := net.SplitHostPort(dc.URI())
	require.NoError(t, err)

	grpcHost, grpcPort, err := net.SplitHostPort(dc.GrpcURI())
	require.NoError(t, err)

	log.Printf("\t>>>>>>>>>>>>>>>>>>>>>>>>>>URI: %q", dc.URI())
	log.Printf("\t>>>>>>>>>>>>>>>>>>>>>>>>>>GrpcURI: %q", dc.GrpcURI())

	return wvhost.NewClient(t,
		weaviate.WithHTTPHost(restHost),
		weaviate.WithHTTPPort(restPort),
		weaviate.WithGRPCHost(grpcHost),
		weaviate.WithGRPCPort(grpcPort),
	)
}
