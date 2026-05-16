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

// Package vector_index_restrictions is the testcontainer-based
// end-to-end suite for the configuration-restriction guardrails
// (ALLOWED_VECTOR_INDEX_TYPES, ALLOWED_COMPRESSION_TYPES,
// RESTRICTIONS_ERROR_MESSAGE). Follow-up to the usage_limits suite —
// same testcontainer shape, but exercising the 422 / CONFIG_NOT_ALLOWED
// contract surfaced by class create/update against the allow-lists.
//
// Suite layout mirrors usage_limits to minimize testcontainer churn:
// the bulk of scenarios share a single container booted with the
// `hfresh,hnsw` mixed-allow-list shape so each sub-test only exercises
// what the previous one couldn't; scenarios that need a different
// startup config (fail-to-boot, runtime override) spin up their own.
package vector_index_restrictions

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/test/docker"
)

// suiteTestTimeout caps total wall-clock for the suite. 10 minutes
// mirrors other acceptance suites; meaningful work is 1-2 minutes
// once the container is built.
const suiteTestTimeout = 10 * time.Minute

func suiteContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), suiteTestTimeout)
}

// startContainer boots a single-node Weaviate with the given env vars.
// WithWeaviateWithGRPC keeps parity with usage_limits even though the
// current restriction code path is REST-only — having gRPC up makes
// future gRPC schema-API additions cheaper to test.
func startContainer(t *testing.T, ctx context.Context, env map[string]string) (*docker.DockerCompose, func()) {
	t.Helper()
	c := docker.New().WithWeaviateWithGRPC()
	for k, v := range env {
		c = c.WithWeaviateEnv(k, v)
	}
	compose, err := c.Start(ctx)
	require.NoError(t, err, "failed to start Weaviate testcontainer")
	return compose, func() {
		if err := compose.Terminate(context.Background()); err != nil {
			t.Logf("failed to terminate testcontainer: %v", err)
		}
	}
}
