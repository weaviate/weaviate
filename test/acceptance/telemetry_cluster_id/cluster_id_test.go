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

package telemetry_cluster_id

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/usecases/telemetry"
)

// telemetrySink records telemetry payloads POSTed by the Weaviate containers.
// The clusterId is exposed nowhere else, so the payload is the only way to
// observe it.
type telemetrySink struct {
	server *httptest.Server
	port   int

	mu     sync.Mutex
	latest map[string]telemetry.Payload
	total  int
}

func newTelemetrySink(t *testing.T) *telemetrySink {
	t.Helper()
	s := &telemetrySink{latest: make(map[string]telemetry.Payload)}

	// Bind all interfaces (not httptest's default loopback) so containers can
	// reach the sink through the host-gateway IP.
	ln, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)
	s.port = ln.Addr().(*net.TCPAddr).Port

	s.server = &httptest.Server{
		Listener: ln,
		Config: &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			var p telemetry.Payload
			_ = json.Unmarshal(body, &p)
			s.mu.Lock()
			s.total++
			if p.NodeID != "" {
				s.latest[p.NodeID] = p
			}
			s.mu.Unlock()
			w.WriteHeader(http.StatusOK) // any non-200 is an error to the pusher
		})},
	}
	s.server.Start()
	t.Cleanup(s.server.Close)
	return s
}

// telemetryURL points Weaviate at this sink; being non-empty, it also stops
// telemetry.New from ever falling back to the production endpoint.
func (s *telemetrySink) telemetryURL() string {
	return fmt.Sprintf("http://host.docker.internal:%d", s.port)
}

func (s *telemetrySink) received() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.total
}

func (s *telemetrySink) clusterIDFor(nodeID string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.latest[nodeID]
	if !ok || p.ClusterID == "" {
		return "", false
	}
	return p.ClusterID, true
}

func (s *telemetrySink) dump(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	t.Logf("SINK total=%d nodes=%d", s.total, len(s.latest))
	for id, p := range s.latest {
		t.Logf("SINK nodeId=%q type=%s clusterId=%q", id, p.Type, p.ClusterID)
	}
}

func (s *telemetrySink) forget(nodeIDs ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range nodeIDs {
		delete(s.latest, id)
	}
}

func (s *telemetrySink) waitClusterID(t *testing.T, nodeID string, timeout time.Duration) string {
	t.Helper()
	var id string
	require.Eventuallyf(t, func() bool {
		got, ok := s.clusterIDFor(nodeID)
		if ok {
			id = got
		}
		return ok
	}, timeout, 500*time.Millisecond,
		"timed out waiting for a non-empty clusterId from node %q", nodeID)
	return id
}

func nodeNames(size int) []string {
	names := make([]string, size)
	for i := range names {
		names[i] = fmt.Sprintf("node%d", i+1)
	}
	return names
}

// waitTimeout covers leader election plus a few push intervals: the first push
// can fire before the leader commits the clusterId, so a later push backfills it.
const waitTimeout = 90 * time.Second

func TestTelemetryClusterID_SingleNode(t *testing.T) {
	ctx := context.Background()
	sink := newTelemetrySink(t)

	compose, err := docker.New().
		WithWeaviate().
		WithWeaviateHostGateway().
		WithWeaviateEnv("TELEMETRY_URL", sink.telemetryURL()).
		WithWeaviateEnv("TELEMETRY_PUSH_INTERVAL", "2s").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	t.Cleanup(func() { sink.dump(t) })

	const node = "node1"

	var before string
	t.Run("clusterId is created", func(t *testing.T) {
		before = sink.waitClusterID(t, node, waitTimeout)
		require.Positive(t, sink.received(), "redirect took effect; nothing went to the real endpoint")
		require.NotEmpty(t, before)
		t.Logf("clusterId before restart: %s", before)
	})

	t.Run("clusterId is stable across a restart", func(t *testing.T) {
		timeout := 30 * time.Second
		require.NoError(t, compose.StopNode(ctx, 1, &timeout))
		sink.forget(node) // drop the shutdown TERMINATE push; assert only post-restart payloads
		require.NoError(t, compose.StartNode(ctx, 1))

		after := sink.waitClusterID(t, node, waitTimeout)
		t.Logf("clusterId after restart:  %s", after)
		assert.Equal(t, before, after, "clusterId must survive a restart on the same volume")
	})
}

func TestTelemetryClusterID_ThreeNodes(t *testing.T) {
	ctx := context.Background()
	sink := newTelemetrySink(t)

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateHostGateway().
		WithWeaviateEnv("TELEMETRY_URL", sink.telemetryURL()).
		WithWeaviateEnv("TELEMETRY_PUSH_INTERVAL", "2s").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	t.Cleanup(func() { sink.dump(t) })

	nodes := nodeNames(3)

	var clusterID string
	t.Run("all nodes share one clusterId with distinct nodeIds", func(t *testing.T) {
		ids := make(map[string]string, len(nodes))
		for _, n := range nodes {
			ids[n] = sink.waitClusterID(t, n, waitTimeout)
			t.Logf("clusterId for %s: %s", n, ids[n])
		}
		require.Positive(t, sink.received(), "redirect took effect; nothing went to the real endpoint")
		require.Len(t, ids, 3)

		clusterID = ids[nodes[0]]
		for _, n := range nodes {
			assert.Equal(t, clusterID, ids[n], "all nodes must share the same cluster-wide clusterId")
		}
	})

	t.Run("clusterId is unchanged after restarting a node", func(t *testing.T) {
		const restarted = "node2"
		timeout := 30 * time.Second
		require.NoError(t, compose.StopNode(ctx, 2, &timeout))
		sink.forget(restarted)
		require.NoError(t, compose.StartNode(ctx, 2))

		after := sink.waitClusterID(t, restarted, waitTimeout)
		t.Logf("clusterId after restarting %s: %s", restarted, after)
		assert.Equal(t, clusterID, after, "clusterId must be unchanged after a node restart")
	})
}
