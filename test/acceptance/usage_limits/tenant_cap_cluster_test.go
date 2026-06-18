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

package usage_limits

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// TestTenantCap_HardUnderClusterConcurrency proves the cap is a hard limit under
// cross-node concurrency: with attempts >> maxTenants fired at all nodes, exactly
// maxTenants are created (surplus gets a typed 429) and every node converges to
// that count.
func TestTenantCap_HardUnderClusterConcurrency(t *testing.T) {
	ctx, cancel := suiteContext(t)
	defer cancel()

	const (
		className  = "TenantCapCluster"
		maxTenants = 10
		attempts   = 45 // >> maxTenants so the surplus must be rejected
	)

	// The tenant cap requires REPLICATION_MAXIMUM_FACTOR=1 to boot (usage-limits ↔
	// replication linkage); RF=1 also keeps each tenant a single shard on one node.
	compose, err := docker.New().With3NodeCluster().
		WithWeaviateEnv("MAXIMUM_ALLOWED_TENANTS_PER_COLLECTION", fmt.Sprintf("%d", maxTenants)).
		WithWeaviateEnv("REPLICATION_MAXIMUM_FACTOR", "1").
		Start(ctx)
	require.NoError(t, err, "failed to start 3-node Weaviate cluster")
	defer func() {
		if err := compose.Terminate(context.Background()); err != nil {
			t.Logf("failed to terminate cluster: %v", err)
		}
	}()

	nodes := []string{
		"http://" + compose.GetWeaviate().URI(),
		"http://" + compose.GetWeaviateNode2().URI(),
		"http://" + compose.GetWeaviateNode3().URI(),
	}

	createMTCollection(t, ctx, nodes[0], className)

	// Barrier: the class is created on nodes[0], but the burst hits all three.
	// Wait until every node can serve the class before firing
	waitForClassOnNodes(t, nodes, className)

	// Fire attempts distinct adds concurrently across all nodes. Outcomes are
	// collected off the test goroutine; assertions run on the main one.
	type outcome struct {
		status int
		body   []byte
		err    error
	}
	results := make([]outcome, attempts)
	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			uri := nodes[i%len(nodes)]
			status, body, err := postOneTenant(ctx, uri, className, fmt.Sprintf("T%03d", i))
			results[i] = outcome{status: status, body: body, err: err}
		}(i)
	}
	wg.Wait()

	var created, rejected, typedRejects, other int
	for _, r := range results {
		switch {
		case r.err != nil:
			other++
		case r.status >= 200 && r.status < 300:
			created++
		case r.status == http.StatusTooManyRequests:
			rejected++
			if isTenantLimitBody(r.body, maxTenants) {
				typedRejects++
			}
		default:
			other++
		}
	}
	t.Logf("created=%d rejected(429)=%d typed=%d other=%d (cap=%d, attempts=%d)",
		created, rejected, typedRejects, other, maxTenants, attempts)

	assert.Equal(t, maxTenants, created, "exactly cap tenants must be accepted, no overshoot")
	// Every surplus add must come back as a typed 429 — including the ones a
	// follower forwarded to the leader, which must not degrade to a 5xx/error.
	assert.Equal(t, attempts-maxTenants, rejected, "every surplus add must be a 429, not a 5xx/error")
	assert.Equal(t, rejected, typedRejects,
		"every 429 must carry the canonical USAGE_LIMIT_EXCEEDED/tenants/%d body", maxTenants)

	// Hard invariant: every node converges to exactly maxTenants. Overshoot would
	// be permanent, so reaching exactly maxTenants proves the cap held.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		for _, uri := range nodes {
			helper.SetupClient(strings.TrimPrefix(uri, "http://"))
			resp, err := helper.GetTenants(t, className)
			if assert.NoError(c, err, "list tenants on %s", uri) {
				assert.Len(c, resp.Payload, maxTenants, "node %s must hold exactly maxTenants", uri)
			}
		}
	}, 30*time.Second, 500*time.Millisecond, "all nodes must converge to exactly maxTenants")
}

// waitForClassOnNodes blocks until the freshly-created class is visible on every
// node, i.e. the schema has propagated cluster-wide.
func waitForClassOnNodes(t *testing.T, nodes []string, class string) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		for _, uri := range nodes {
			helper.SetupClient(strings.TrimPrefix(uri, "http://"))
			_, err := helper.GetClassWithoutAssert(t, class, "")
			assert.NoError(c, err, "class not yet visible on %s", uri)
		}
	}, 30*time.Second, 200*time.Millisecond, "class must propagate to all nodes before the burst")
}

// postOneTenant adds a single tenant and returns the HTTP status + body without
// touching *testing.T, so it is safe to call from many goroutines.
func postOneTenant(ctx context.Context, httpURI, class, tenant string) (int, []byte, error) {
	body := []byte(fmt.Sprintf(`[{"name":%q}]`, tenant))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		httpURI+"/v1/schema/"+class+"/tenants", bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, b, err
}

// isTenantLimitBody reports whether body is the canonical tenant-cap 429 payload.
func isTenantLimitBody(body []byte, wantValue int) bool {
	var parsed struct {
		ErrorCode string `json:"errorCode"`
		Limit     string `json:"limit"`
		Value     int64  `json:"value"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return false
	}
	return parsed.ErrorCode == "USAGE_LIMIT_EXCEEDED" &&
		parsed.Limit == "tenants" &&
		parsed.Value == int64(wantValue)
}
