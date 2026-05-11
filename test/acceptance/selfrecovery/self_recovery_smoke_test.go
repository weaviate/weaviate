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

// Package selfrecovery holds acceptance tests for SELF_RECOVERY shard
// re-hydration. Data-loss cases use tmpfs-backed /data so a container stop
// wipes the shard dir without root or external volume tooling.
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

	compose := startSelfRecoveryCluster(ctx, t, srClusterCfg{asyncDisabled: true, debugPort: true})
	debugURI := compose.GetWeaviate().DebugURI()
	require.NotEmpty(t, debugURI, "DebugURI is empty — was WithWeaviateWithDebugPort() called?")

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

	t.Run("restart_rejects_when_shard_is_live", func(t *testing.T) {
		waitClusterHealthy(t)

		cls := articles.ParagraphsClass()
		cls.ShardingConfig = map[string]interface{}{"desiredCount": 1}
		cls.ReplicationConfig = &models.ReplicationConfig{Factor: 3}
		cls.Vectorizer = "none"
		helper.CreateClass(t, cls)

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

func TestSelfRecoveryDebugEndpointsDisabledWhenFeatureOff(t *testing.T) {
	mainCtx := context.Background()
	ctx, cancel := context.WithTimeout(mainCtx, 4*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviate().
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
