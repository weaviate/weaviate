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
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"

	"acceptance_tests_with_client/internal/wvhost"
)

func TestWhereFilter_SingleNode_Contains(t *testing.T) {
	t.Run("Contains", testContains(wvhost.REST()))
	t.Run("Contains text", testContainsText(wvhost.REST()))
	t.Run("Contains movies", testContainsMovies(wvhost.REST()))
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

		endpoint := compose.GetWeaviate().URI()

		t.Run("numerical filters", testNumericalFilters(endpoint))
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

		endpoint := compose.GetWeaviate().URI()

		t.Run("numerical filters", testNumericalFilters(endpoint))
	})
}

func TestWhereFilter_SingleNode_RoaringSetInMemory(t *testing.T) {
	ctx := context.Background()

	// testContainsMovies equality-filters (ContainsAny/ContainsAll/ContainsNone)
	// exclusively on "languages", a text array of the fixed "Movies" fixture
	// class, whose filterable index is a roaring set bucket. The allow-list
	// holds exactly that bucket in the always-merged in-memory form.
	compose, err := docker.New().
		WithWeaviate().
		WithWeaviateEnv("INDEX_ROARINGSET_IN_MEMORY", "Movies.languages").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	// inMemoryBuilds counts the debug build lines naming the allow-listed
	// bucket, proving the in-memory structure was actually built — the filter
	// run alone would also pass if the bucket silently stayed on the disk read
	// path. LOG_LEVEL defaults to debug in the test container.
	inMemoryBuilds := func() int {
		rc, err := compose.GetWeaviate().Container().Logs(ctx)
		require.NoError(t, err)
		defer rc.Close()
		logs, err := io.ReadAll(rc)
		require.NoError(t, err)
		count := 0
		for line := range strings.Lines(string(logs)) {
			if strings.Contains(line, "roaringset segment-in-memory built") &&
				strings.Contains(line, "property_languages") {
				count++
			}
		}
		return count
	}

	endpoint := compose.GetWeaviate().URI()

	t.Run("Contains movies", testContainsMovies(endpoint))

	buildsAfterImport := inMemoryBuilds()
	require.Positive(t, buildsAfterImport,
		"the allow-listed bucket must have built its in-memory structure")

	t.Run("after restart", func(t *testing.T) {
		require.NoError(t, compose.StopAt(ctx, 0, nil))
		// StartAt blocks until /v1/.well-known/ready responds 200.
		require.NoError(t, compose.StartAt(ctx, 0))

		// the restart may remap the published port
		endpoint := compose.GetWeaviate().URI()

		t.Run("Contains movies", testContainsMovies(endpoint))

		require.Greater(t, inMemoryBuilds(), buildsAfterImport,
			"the in-memory structure must be built again after the restart")
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

	endpoint := compose.GetWeaviate().URI()

	t.Run("Contains", testContains(endpoint))
	t.Run("Contains text", testContainsText(endpoint))
	t.Run("Contains movies", testContainsMovies(endpoint))
	t.Run("Numerical filters", testNumericalFilters(endpoint))
}
