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

package asyncrep_battle

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
)

// nodeIDSet reads the id set on one node via a raw GraphQL POST at CL=ONE,
// bypassing the process-global test client so it is safe anywhere.
func nodeIDSet(uri, class, tenant string, limit int) (map[string]struct{}, error) {
	tenantArg := ""
	if tenant != "" {
		tenantArg = fmt.Sprintf("tenant: %q, ", tenant)
	}
	query := fmt.Sprintf(`{Get{%s(%sconsistencyLevel: ONE, limit: %d){_additional{id}}}}`, class, tenantArg, limit)
	body, _ := json.Marshal(map[string]string{"query": query})
	resp, err := http.Post(fmt.Sprintf("http://%s/v1/graphql", uri), "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var generic struct {
		Data   map[string]json.RawMessage `json:"data"`
		Errors []struct{ Message string } `json:"errors"`
	}
	if err := json.Unmarshal(raw, &generic); err != nil {
		return nil, fmt.Errorf("parse graphql response: %w (body %.200s)", err, raw)
	}
	if len(generic.Errors) > 0 {
		return nil, fmt.Errorf("graphql error: %s", generic.Errors[0].Message)
	}
	var get map[string][]struct {
		Additional struct {
			ID string `json:"id"`
		} `json:"_additional"`
	}
	if err := json.Unmarshal(generic.Data["Get"], &get); err != nil {
		return nil, fmt.Errorf("parse Get payload: %w", err)
	}
	out := make(map[string]struct{}, len(get[class]))
	for _, o := range get[class] {
		out[o.Additional.ID] = struct{}{}
	}
	return out, nil
}

// requireConverged retries until the per-node id sets match across all three
// nodes AND the async-checkpoint roots are bit-identical per shard. Call only
// with all three nodes up and no writers running.
func requireConverged(ctx context.Context, t *testing.T, compose *docker.DockerCompose, class string, limit int, timeout time.Duration) {
	t.Helper()
	uris := nodeURIs(compose)
	clusters := clusterURIs(compose)
	shards := discoverShards(t, uris[0], class)
	require.NotEmpty(t, shards)

	createdAt := time.Now().UTC()
	cutoffMs := createdAt.Add(10 * time.Second).UnixMilli()
	for _, cluster := range clusters {
		asyncCheckpointCreate(t, cluster, class, shards, cutoffMs, createdAt.UnixMilli())
	}
	defer func() {
		for _, cluster := range clusters {
			asyncCheckpointDelete(t, cluster, class, shards)
		}
	}()

	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		var sets []map[string]struct{}
		for _, uri := range uris {
			s, err := nodeIDSet(uri, class, "", limit)
			require.NoError(ct, err)
			sets = append(sets, s)
		}
		require.Equal(ct, len(sets[0]), len(sets[1]), "node1 vs node2 id-set size")
		require.Equal(ct, len(sets[0]), len(sets[2]), "node1 vs node3 id-set size")
		require.Equal(ct, sets[0], sets[1], "node1 vs node2 id sets diverge")
		require.Equal(ct, sets[0], sets[2], "node1 vs node3 id sets diverge")

		for _, shard := range shards {
			var roots []string
			for i, cluster := range clusters {
				statuses := asyncCheckpointStatus(t, cluster, class, []string{shard})
				entry, ok := statuses[shard]
				require.True(ct, ok, "node %d hosts no checkpoint for shard %s", i+1, shard)
				require.NotZero(ct, entry.CutoffMs, "node %d checkpoint inactive for shard %s", i+1, shard)
				roots = append(roots, base64.StdEncoding.EncodeToString(entry.Root))
			}
			require.Equal(ct, roots[0], roots[1], "shard %s root node1 vs node2", shard)
			require.Equal(ct, roots[0], roots[2], "shard %s root node1 vs node3", shard)
		}
	}, timeout, 2*time.Second, "replicas did not converge for class %s", class)
}

// requireSampleState probes sampled ids at CL=ONE on one node against the
// writer's acked model: live ids present, deleted ids absent. Main goroutine only.
func requireSampleState(t *testing.T, uri, class string, live, deleted []strfmt.UUID) {
	t.Helper()
	for _, id := range live {
		exists, err := common.ObjectExistsCL(t, uri, class, id, types.ConsistencyLevelOne)
		require.NoError(t, err, "probe live id %s on %s", id, uri)
		require.True(t, exists, "acked live id %s missing on %s", id, uri)
	}
	for _, id := range deleted {
		exists, err := common.ObjectExistsCL(t, uri, class, id, types.ConsistencyLevelOne)
		require.NoError(t, err, "probe deleted id %s on %s", id, uri)
		require.False(t, exists, "acked-deleted id %s resurrected on %s", id, uri)
	}
}

// ── async-checkpoint wire helpers (wire-coupled copies of the convergence test) ──

type asyncCheckpointStatusEntry struct {
	Root        []byte `json:"root"`
	CutoffMs    int64  `json:"cutoff_ms"`
	CreatedAtMs int64  `json:"created_at_ms"`
}

func asyncCheckpointCreate(t *testing.T, clusterURI, className string, shards []string, cutoffMs, createdAtMs int64) {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"shards":        shards,
		"cutoff_ms":     cutoffMs,
		"created_at_ms": createdAtMs,
	})
	require.NoError(t, err)
	resp, err := http.Post(asyncCheckpointURL(clusterURI, className), "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("create checkpoint returned %d: %s", resp.StatusCode, respBody)
	}
}

func asyncCheckpointDelete(t *testing.T, clusterURI, className string, shards []string) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"shards": shards})
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodDelete, asyncCheckpointURL(clusterURI, className), bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("delete checkpoint returned %d: %s", resp.StatusCode, respBody)
	}
}

func asyncCheckpointStatus(t *testing.T, clusterURI, className string, shards []string) map[string]asyncCheckpointStatusEntry {
	t.Helper()
	u, err := url.Parse(asyncCheckpointURL(clusterURI, className))
	require.NoError(t, err)
	if len(shards) > 0 {
		q := u.Query()
		for _, s := range shards {
			q.Add("shards", s)
		}
		u.RawQuery = q.Encode()
	}
	resp, err := http.Get(u.String())
	require.NoError(t, err)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status returned %d: %s", resp.StatusCode, body)
	}
	var out map[string]asyncCheckpointStatusEntry
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return out
}

func asyncCheckpointURL(clusterURI, className string) string {
	// docker's clusterURI is host:port (no scheme); the cluster API speaks plain HTTP.
	uri := clusterURI
	if !strings.HasPrefix(uri, "http://") && !strings.HasPrefix(uri, "https://") {
		uri = "http://" + uri
	}
	return fmt.Sprintf("%s/replicas/indices/%s/async-checkpoint", uri, className)
}

// discoverShards uses the public REST API: the cluster API has no shard-enumeration endpoint.
func discoverShards(t *testing.T, restURI, className string) []string {
	t.Helper()
	uri := restURI
	if !strings.HasPrefix(uri, "http://") && !strings.HasPrefix(uri, "https://") {
		uri = "http://" + uri
	}
	resp, err := http.Get(fmt.Sprintf("%s/v1/schema/%s/shards", uri, className))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var shards []struct {
		Name string `json:"name"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&shards))
	out := make([]string, len(shards))
	for i, s := range shards {
		out[i] = s.Name
	}
	return out
}
