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

package clients

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/replica"
)

// nodeNotReadyBody is what a booting node answers on the replica endpoints.
const nodeNotReadyBody = "503 Node not ready"

// notReadyServer counts the attempts a client burned on an unready replica.
type notReadyServer struct {
	*httptest.Server
	requests atomic.Int64
}

func newNotReadyServer(t *testing.T) *notReadyServer {
	t.Helper()
	s := &notReadyServer{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.requests.Add(1)
		http.Error(w, nodeNotReadyBody, http.StatusServiceUnavailable)
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *notReadyServer) hostPort() string {
	return s.URL[len("http://"):]
}

func testObject() *storobj.Object {
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			Class:      "C1",
			ID:         UUID1,
			Properties: map[string]interface{}{"stringProp": "abc"},
		},
		Vector:    []float32{1, 2, 3},
		VectorLen: 3,
	}
}

// A node-level readiness gate is not transient: it must not be retried.
func TestShouldRetry_NodeNotReadyIsNotRetryable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		code int
		want bool
	}{
		{
			name: "503 service unavailable (node not ready) must NOT be retried",
			code: http.StatusServiceUnavailable,
			want: false,
		},
		{
			name: "429 too many requests stays retryable",
			code: http.StatusTooManyRequests,
			want: true,
		},
		{
			name: "500 internal server error stays retryable",
			code: http.StatusInternalServerError,
			want: true,
		},
		{
			name: "412 precondition failed is not retryable",
			code: http.StatusPreconditionFailed,
			want: false,
		},
		{
			name: "404 not found is not retryable",
			code: http.StatusNotFound,
			want: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, shouldRetry(test.code),
				"shouldRetry(%d) classification", test.code)
		})
	}
}

// Per call: one attempt, then fail fast with the node-not-ready error.
func TestReplicaClient_DoesNotRetryNodeNotReady(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		call func(t *testing.T, c *replicationClient, host string) error
	}{
		{
			name: "FetchObject",
			call: func(t *testing.T, c *replicationClient, host string) error {
				t.Helper()
				_, err := c.FetchObject(context.Background(), host, "C1", "S1",
					UUID1, nil, additional.Properties{}, MAX_RETRIES)
				return err
			},
		},
		{
			name: "DigestObjects",
			call: func(t *testing.T, c *replicationClient, host string) error {
				t.Helper()
				_, err := c.DigestObjects(context.Background(), host, "C1", "S1",
					[]strfmt.UUID{UUID1}, MAX_RETRIES)
				return err
			},
		},
		{
			name: "PutObject",
			call: func(t *testing.T, c *replicationClient, host string) error {
				t.Helper()
				_, err := c.PutObject(context.Background(), host, "C1", "S1",
					"RID1", testObject(), 0)
				return err
			},
		},
		{
			name: "Commit",
			call: func(t *testing.T, c *replicationClient, host string) error {
				t.Helper()
				var resp replica.SimpleResponse
				return c.Commit(context.Background(), host, "C1", "S1", "RID1", &resp)
			},
		},
		{
			name: "Abort",
			call: func(t *testing.T, c *replicationClient, host string) error {
				t.Helper()
				_, err := c.Abort(context.Background(), host, "C1", "S1", "RID1")
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			server := newNotReadyServer(t)
			c := newReplicationClient(t, server.Client())

			err := test.call(t, c, server.hostPort())
			require.Error(t, err, "a 503 from a booting node must surface as an error")

			assert.EqualValues(t, 1, server.requests.Load(),
				"%s must fail fast on %q: exactly one attempt, no retry budget burned "+
					"against a node that cannot recover within the retry window",
				test.name, nodeNotReadyBody)
		})
	}
}

// After a few consecutive failures the host is skipped until a probe lets it back in.
func TestReplicaClient_CircuitBreakerSkipsUnreadyHost(t *testing.T) {
	t.Parallel()

	// once the budget is spent, further calls must not reach the host
	const consecutiveFailureBudget = 3

	const callsAfterBreakerShouldOpen = 5

	unready := newNotReadyServer(t)
	c := newReplicationClient(t, unready.Client())

	total := consecutiveFailureBudget + callsAfterBreakerShouldOpen
	for i := 0; i < total; i++ {
		_, err := c.DigestObjects(context.Background(), unready.hostPort(),
			"C1", "S1", []strfmt.UUID{UUID1}, MAX_RETRIES)
		require.Error(t, err, "call %d against an unready host must fail", i+1)
	}

	assert.LessOrEqual(t, unready.requests.Load(), int64(consecutiveFailureBudget),
		"after %d consecutive failures the client must mark the host unhealthy and "+
			"stop attempting it; %d further calls must not reach it (got %d total requests)",
		consecutiveFailureBudget, callsAfterBreakerShouldOpen, unready.requests.Load())

	// the breaker must be per-host: a global one would be a cluster-wide outage
	var healthyRequests atomic.Int64
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		healthyRequests.Add(1)
		w.Write([]byte(`[]`)) //nolint:errcheck
	}))
	defer healthy.Close()

	_, err := c.DigestObjects(context.Background(), healthy.URL[len("http://"):],
		"C1", "S1", []strfmt.UUID{UUID1}, MAX_RETRIES)
	require.NoError(t, err, "a healthy host must still be served while another host's breaker is open")
	assert.EqualValues(t, 1, healthyRequests.Load(),
		"the breaker must be scoped per host, not global")
}

// Commit and Abort must honour a cancelled context.
func TestReplicaClient_CommitAbortHonorCanceledContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		call func(t *testing.T, ctx context.Context, c *replicationClient, host string) error
	}{
		{
			name: "Commit",
			call: func(t *testing.T, ctx context.Context, c *replicationClient, host string) error {
				t.Helper()
				var resp replica.SimpleResponse
				return c.Commit(ctx, host, "C1", "S1", "RID1", &resp)
			},
		},
		{
			name: "Abort",
			call: func(t *testing.T, ctx context.Context, c *replicationClient, host string) error {
				t.Helper()
				_, err := c.Abort(ctx, host, "C1", "S1", "RID1")
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			server := newNotReadyServer(t)
			c := newReplicationClient(t, server.Client())

			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			err := test.call(t, ctx, c, server.hostPort())
			require.Error(t, err, "%s on a canceled context must fail", test.name)
			assert.ErrorIs(t, err, context.Canceled,
				"%s must propagate the caller's cancellation", test.name)
			assert.EqualValues(t, 0, server.requests.Load(),
				"%s must not contact the replica after the caller canceled; "+
					"newHttpReplicaCMD drops the ctx, so cancellation never reaches the transport",
				test.name)
		})
	}
}
