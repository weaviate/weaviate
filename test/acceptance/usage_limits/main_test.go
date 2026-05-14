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

// Package usage_limits is the testcontainer-based end-to-end suite for
// the Free-Tier guardrails RFC. The suite is deliberately shaped to
// minimize testcontainer restarts (per the RFC plan's inline feedback):
// the bulk of scenarios share a single container booted with all four
// limits set + a custom error-message template; only the scenarios that
// need a different startup config (unsupported scope → fail-to-boot)
// spin up a separate container.
package usage_limits

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/test/docker"
)

// usageLimitsTestTimeout caps the total wall-clock for the suite.
// 10 minutes mirrors other acceptance suites; the meaningful work fits
// in 1-2 minutes once the testcontainer is built.
const usageLimitsTestTimeout = 10 * time.Minute

// suiteContext returns a context tied to the test suite's overall budget.
// Used as the parent for compose.Start so a hung container never strands
// the test runner.
func suiteContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), usageLimitsTestTimeout)
}

// startContainer boots a single-node Weaviate with the given env vars set.
// Uses WithWeaviateWithGRPC so the gRPC port is exposed for the
// gRPC-batch acceptance scenarios (the plain WithWeaviate variant only
// exposes the REST port). Returns the compose handle (for URI access +
// cleanup) plus a terminate function the caller defers.
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
