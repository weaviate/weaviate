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

package vector_index_params

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/weaviate/weaviate/test/docker"
)

// Operator caps of the shared node. They are well above every value the tests
// send (ef <= 1000, search_probe <= 256) and differ from each other and from
// the built-in default (10000), so the caps test can tell which cap each
// surface read without starting a node of its own.
const (
	maxSearchEF    = 2000
	maxSearchProbe = 500
)

// singleNode is the shared single-node Weaviate (REST + gRPC) used by every
// test except the sharded one, which needs a cluster.
var singleNode *docker.DockerCompose

func TestMain(m *testing.M) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviateWithGRPC().
		WithWeaviateEnv("HNSW_MAX_SEARCH_EF", fmt.Sprint(maxSearchEF)).
		WithWeaviateEnv("HFRESH_MAX_SEARCH_PROBE", fmt.Sprint(maxSearchProbe)).
		Start(ctx)
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
