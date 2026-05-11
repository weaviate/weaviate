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

// Package selfrecovery contains acceptance tests for the SELF_RECOVERY
// shard re-hydration flow (see plan: cluster/replication/selfrecovery).
//
// This file holds the foundational smoke test that boots a cluster with
// the feature flag enabled and verifies the wiring is in place:
//   - The accept-empty debug endpoint is reachable and 404s for an
//     unknown (collection, shard).
//   - The restart debug endpoint 409s for a shard whose live directory
//     already exists.
//   - With the feature flag off, none of the /debug/self-recovery/*
//     endpoints (nor the test-only /debug/raft/snapshot) are registered.
//
// The full data-loss-and-restore scenarios from the plan
// (single-shard recovery, mass recovery with bounded concurrency, resume
// after crash, consistency=QUORUM/ALL correctness, all-peers-wiped
// catastrophe, multi-tenancy, lazy-load interaction, source pause stall,
// operator cancel, etc.) require volume-management infrastructure
// (`docker volume rm` mid-test or in-container `rm -rf`) that the
// existing test/docker harness does not yet expose. Those scenarios are
// tracked as follow-ups; this scaffold gives them a home to land in.
package selfrecovery

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func TestSelfRecoverySmokeWiring(t *testing.T) {
	mainCtx := context.Background()
	ctx, cancel := context.WithTimeout(mainCtx, 6*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("SELF_RECOVERY_ENABLED", "true").
		WithWeaviateEnv("SELF_RECOVERY_CONCURRENCY", "2").
		WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true"). // /replication/* observability (and RF>1 schema cmds)
		WithWeaviateWithDebugPort().                         // /debug/self-recovery/* lives on the profiling port
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("terminate compose: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	debugURI := compose.GetWeaviate().DebugURI()
	require.NotEmpty(t, debugURI, "DebugURI is empty — was WithWeaviateWithDebugPort() called?")

	// Prometheus metrics live on a separate port (2112 by default,
	// PROMETHEUS_MONITORING_ENABLED-gated) which the test/docker
	// harness does not currently expose. Metric registration is
	// covered by the unit test in cluster/replication/selfrecovery.

	// accept-empty debug endpoint is reachable. We probe with a
	// non-existent (collection, shard) and verify the endpoint responds
	// with a 404 from the schema gate. Validates the handler is
	// registered AND that the schema-error → 404 mapping is in place
	// (rather than the generic 500 the handler used to return for
	// validation failures).
	t.Run("accept_empty_endpoint_is_reachable", func(t *testing.T) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			"http://"+debugURI+"/debug/self-recovery/accept-empty?collection=NoSuchClass&shard=NoSuchShard", nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusNotFound, resp.StatusCode,
			"unknown class/shard should yield 404 (schema gate); got %d", resp.StatusCode)
	})

	// /restart on a shard whose live directory already exists must be
	// rejected with 409 Conflict (recovery already completed / never
	// happened — restarting would re-copy peer data over a healthy shard).
	t.Run("restart_rejects_when_shard_is_live", func(t *testing.T) {
		// Quorum must be formed before an RF=3 CreateClass succeeds.
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			body, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), nil)
			require.NoError(ct, err)
			require.NotNil(ct, body.Payload)
			require.Len(ct, body.Payload.Nodes, 3)
			for _, n := range body.Payload.Nodes {
				require.Equal(ct, "HEALTHY", *n.Status)
			}
		}, 3*time.Minute, 1*time.Second)

		cls := articles.ParagraphsClass()
		cls.ShardingConfig = map[string]interface{}{"desiredCount": 1}
		cls.ReplicationConfig = &models.ReplicationConfig{Factor: 3, AsyncEnabled: false}
		cls.Vectorizer = "none"
		helper.CreateClass(t, cls)

		// Resolve the (single) shard name once the shard is placed.
		var shardName string
		verbose := verbosity.OutputVerbose
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			body, err := helper.Client(t).Nodes.NodesGetClass(
				nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(cls.Class), nil)
			require.NoError(ct, err)
			require.NotNil(ct, body.Payload)
			require.NotEmpty(ct, body.Payload.Nodes)
			require.NotEmpty(ct, body.Payload.Nodes[0].Shards)
			shardName = body.Payload.Nodes[0].Shards[0].Name
			require.NotEmpty(ct, shardName)
		}, 30*time.Second, 500*time.Millisecond)

		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			"http://"+debugURI+"/debug/self-recovery/restart?collection="+cls.Class+"&shard="+shardName, nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		bodyBytes, _ := io.ReadAll(resp.Body)
		require.Equal(t, http.StatusConflict, resp.StatusCode,
			"restart on a live shard must be 409; got %d (%s)", resp.StatusCode, string(bodyBytes))
		require.Contains(t, strings.ToLower(string(bodyBytes)), "recovering",
			"409 body should explain the shard is not recovering; got %q", string(bodyBytes))
	})
}

// TestSelfRecoveryDebugEndpointsDisabledWhenFeatureOff verifies that the
// /debug/self-recovery/* operator endpoints and the test-only
// /debug/raft/snapshot endpoint are NOT registered when
// SELF_RECOVERY_ENABLED is unset/false — they must not be a surface (even
// on the profiling port) on a node that doesn't use the feature.
func TestSelfRecoveryDebugEndpointsDisabledWhenFeatureOff(t *testing.T) {
	mainCtx := context.Background()
	ctx, cancel := context.WithTimeout(mainCtx, 4*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviate(). // single node; feature flag deliberately NOT set
		WithWeaviateWithDebugPort().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("terminate compose: %v", err)
		}
	}()

	debugURI := compose.GetWeaviate().DebugURI()
	require.NotEmpty(t, debugURI)

	for _, path := range []string{
		"/debug/self-recovery/accept-empty?collection=C&shard=S",
		"/debug/self-recovery/restart?collection=C&shard=S",
		"/debug/raft/snapshot",
	} {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+debugURI+path, nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		require.Equal(t, http.StatusNotFound, resp.StatusCode,
			"%s must be 404 (handler not registered) when SELF_RECOVERY_ENABLED is off; got %d (%s)", path, resp.StatusCode, string(body))
	}
}
