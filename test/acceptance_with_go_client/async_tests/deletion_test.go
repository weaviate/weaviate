package asynctests

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate/test/docker"
)

func TestAsync_Deletion(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviate().
		WithWeaviateEnv("ASYNC_INDEXING", "true").
		WithWeaviateEnv("ASYNC_THROTTLING", "true").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	st := time.Now()

	client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: compose.GetWeaviate().URI()})
	require.NoError(t, err)

	className := "DeletionTest"
	createClassLegacyVector(t, client, className)

	objects := randomObjects(className, 5000, 768)
	batchObjects(t, client, objects)

	for i := 0; i < 30; i++ {
		start := rand.Intn(4000)
		end := min(start+2000, 5000)
		fmt.Println(i, "Updating range:", start, ":", end)
		rng := updateRange(objects, start, end)
		batchObjects(t, client, rng)
	}

	checkEventuallyIndexed(t, client, className)

	got := getAllVectors(t, client, className)
	require.Equal(t, len(objects), len(got))
	require.ElementsMatch(t, objectsToVectors(objects), got)

	fmt.Println("Took:", time.Since(st))
}
