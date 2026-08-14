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

// Package vector_index_restrictions covers the configuration-restriction
// guardrails (ALLOWED_VECTOR_INDEX_TYPES / ALLOWED_COMPRESSION_TYPES /
// RESTRICTIONS_ERROR_MESSAGE) via testcontainers.
package vector_index_restrictions

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/test/docker"
)

const suiteTestTimeout = 10 * time.Minute

func suiteContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), suiteTestTimeout)
}

// startCluster boots a 3-node Weaviate cluster with env. gRPC is on for
// parity with usage_limits and future gRPC schema-API tests.
func startCluster(t *testing.T, ctx context.Context, env map[string]string) (*docker.DockerCompose, func()) {
	t.Helper()
	c := docker.New().WithWeaviateClusterWithGRPC()
	for k, v := range env {
		c = c.WithWeaviateEnv(k, v)
	}
	compose, err := c.Start(ctx)
	require.NoError(t, err, "failed to start 3-node Weaviate testcontainer")
	return compose, terminator(t, compose)
}

func terminator(t *testing.T, compose *docker.DockerCompose) func() {
	return func() {
		if err := compose.Terminate(context.Background()); err != nil {
			t.Logf("failed to terminate testcontainer: %v", err)
		}
	}
}
