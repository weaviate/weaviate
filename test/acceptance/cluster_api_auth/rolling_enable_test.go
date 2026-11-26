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

package test

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// TestClusterAPIAuthRollingEnable turns on cluster API basic auth in a running
// cluster, one node at a time, in two rollouts. Rollout 1 adds credentials
// with send-only on. Rollout 2 turns send-only off. After each node changes,
// a round of requests at consistency ALL goes through every node. This proves
// that no mix of old and new configs rejects traffic.
func TestClusterAPIAuthRollingEnable(t *testing.T) {
	// a local run takes about 85s, mostly in the six node replacements
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Minute)
	defer cancel()

	const (
		user      = "cluster-user"
		pass      = "cluster-pass"
		className = "RollingEnableAuth"
	)

	compose, err := docker.New().With3NodeCluster().
		WithWeaviateEnv("REPLICATION_GRPC_ENABLED", "true").
		WithWeaviateDataVolumes().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
	}()

	node := func(n int) *docker.DockerContainer { return compose.GetWeaviateNode(n + 1) }

	helper.SetupClient(node(0).URI())
	helper.CreateClass(t, &models.Class{
		Class:             className,
		Vectorizer:        "none",
		ReplicationConfig: &models.ReplicationConfig{Factor: 3},
		Properties:        []*models.Property{{Name: "name", DataType: schema.DataTypeText.PropString()}},
	})

	var ids []strfmt.UUID
	round := func(t *testing.T) {
		t.Helper()
		for n := range 3 {
			uri := node(n).URI()
			id := strfmt.UUID(uuid.NewString())
			require.NoError(t, common.CreateObjectCL(t, uri, &models.Object{
				ID: id, Class: className, Properties: map[string]any{"name": fmt.Sprintf("via weaviate-%d", n)},
			}, types.ConsistencyLevelAll))
			ids = append(ids, id)

			for _, id := range ids {
				_, err := common.GetObjectCL(t, uri, className, id, types.ConsistencyLevelAll)
				require.NoErrorf(t, err, "read %s at ALL via weaviate-%d", id, n)
			}

			statuses := common.GetNodes(t, uri).Nodes
			require.Len(t, statuses, 3)
			for _, ns := range statuses {
				require.NotNil(t, ns.Status)
				require.Equalf(t, models.NodeStatusStatusHEALTHY, *ns.Status, "%s seen via weaviate-%d", ns.Name, n)
			}
		}
	}

	replace := func(t *testing.T, n int, set map[string]string) {
		t.Helper()
		require.NoError(t, compose.ReplaceNodeWithEnv(ctx, n, set))

		info, err := node(n).Container().Inspect(ctx)
		require.NoError(t, err)
		require.Contains(t, info.Config.Env, "REPLICATION_GRPC_ENABLED=true")
		for k, v := range set {
			require.Contains(t, info.Config.Env, k+"="+v)
		}

		// /ready can be up while the shard is still loading. Wait for the
		// shard with a read that the node serves itself. That read makes no
		// call to another node, so a peer's auth config cannot affect it.
		// ids[0] was written before the replacement.
		name := node(n).Name()
		require.NotEmpty(t, ids, "no object to probe the replaced node with")
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			_, err := common.GetObjectFromNode(t, node(n).URI(), className, ids[0], name)
			require.NoError(ct, err)
		}, 60*time.Second, 500*time.Millisecond, "shard on replaced %s not serving", name)

		// /ready also does not wait for memberlist to reconnect the node to
		// its peers, or for its health score to settle. This wait checks only
		// the member count and each node's entry about itself. Neither uses
		// the cluster API, so retrying them cannot hide a rejected request.
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			for m := range 3 {
				helper.SetupClient(node(m).URI())
				resp, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), nil)
				require.NoError(ct, err)
				require.Lenf(ct, resp.Payload.Nodes, 3, "members seen by weaviate-%d", m)
				for _, ns := range resp.Payload.Nodes {
					if ns.Name == node(m).Name() {
						require.NotNil(ct, ns.Status)
						require.Equalf(ct, models.NodeStatusStatusHEALTHY, *ns.Status, "weaviate-%d about itself", m)
					}
				}
			}
		}, 60*time.Second, 500*time.Millisecond, "memberlist did not settle after replacing %s", name)
	}

	probeJoin := func(t *testing.T, n int, setAuth bool, u, p string) int {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			fmt.Sprintf("http://%s/v1/cluster/join", node(n).ClusterURI()), http.NoBody)
		require.NoError(t, err)
		if setAuth {
			req.SetBasicAuth(u, p)
		}
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		return resp.StatusCode
	}

	t.Run("baseline", func(t *testing.T) {
		round(t)
	})

	t.Run("rollout 1", func(t *testing.T) {
		for n := range 3 {
			replace(t, n, map[string]string{
				"CLUSTER_BASIC_AUTH_USERNAME":  user,
				"CLUSTER_BASIC_AUTH_PASSWORD":  pass,
				"CLUSTER_BASIC_AUTH_SEND_ONLY": "true",
			})
			round(t)
		}
	})

	t.Run("send-only does not verify", func(t *testing.T) {
		for n := range 3 {
			assert.Equalf(t, http.StatusBadRequest, probeJoin(t, n, false, "", ""), "weaviate-%d", n)
		}
	})

	t.Run("rollout 2", func(t *testing.T) {
		for n := range 3 {
			replace(t, n, map[string]string{"CLUSTER_BASIC_AUTH_SEND_ONLY": "false"})
			round(t)
		}
	})

	t.Run("verification on", func(t *testing.T) {
		for n := range 3 {
			assert.Equalf(t, http.StatusUnauthorized, probeJoin(t, n, false, "", ""), "no creds, weaviate-%d", n)
			assert.Equalf(t, http.StatusUnauthorized, probeJoin(t, n, true, user, "wrong"), "wrong creds, weaviate-%d", n)
			assert.Equalf(t, http.StatusBadRequest, probeJoin(t, n, true, user, pass), "right creds, weaviate-%d", n)
		}
	})
}
