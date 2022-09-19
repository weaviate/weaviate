package test

import (
	"context"
	"os"
	"testing"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/test/docker"
)

const weaviateEndpoint = "WEAVIATE_ENDPOINT"

func TestMain(m *testing.M) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviate().WithRef2VecCentroid().
		Start(ctx)
	if err != nil {
		panic(errors.Wrapf(err, "cannot start"))
	}

	os.Setenv(weaviateEndpoint, compose.GetWeaviate().URI())
	code := m.Run()

	if err := compose.Terminate(ctx); err != nil {
		panic(errors.Wrapf(err, "cannot terminate"))
	}

	os.Exit(code)
}
