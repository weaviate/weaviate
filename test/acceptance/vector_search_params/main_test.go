//	_       _
//
// __      _____  __ ___   ___  __ _| |_ ___
//
//	\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//	 \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//	  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//	 Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//	 CONTACT: hello@weaviate.io
package vector_search_params

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/weaviate/weaviate/test/docker"
)

// singleNode is the shared single-node Weaviate (REST + gRPC) used by all
// tests except the replicated one, which starts its own cluster.
var singleNode *docker.DockerCompose

func TestMain(m *testing.M) {
	ctx := context.Background()
	compose, err := docker.New().WithWeaviateWithGRPC().Start(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start weaviate: %v\n", err)
		os.Exit(1)
	}
	singleNode = compose

	code := m.Run()

	if err := compose.Terminate(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "failed to terminate weaviate: %v\n", err)
	}
	os.Exit(code)
}
