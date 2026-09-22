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
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Only node-level failures may take a replica out of rotation.
func TestHostBreakerClassification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		err       error
		wantOpens bool
	}{
		{name: "503 node not ready", err: &HTTPError{Code: http.StatusServiceUnavailable}, wantOpens: true},
		{name: "502 bad gateway", err: &HTTPError{Code: http.StatusBadGateway}, wantOpens: true},
		{name: "504 gateway timeout", err: &HTTPError{Code: http.StatusGatewayTimeout}, wantOpens: true},
		{name: "429 shed", err: &HTTPError{Code: http.StatusTooManyRequests}, wantOpens: true},
		{name: "500 internal", err: &HTTPError{Code: http.StatusInternalServerError}, wantOpens: false},
		{name: "404 not found", err: &HTTPError{Code: http.StatusNotFound}, wantOpens: false},
		{name: "412 precondition failed", err: &HTTPError{Code: http.StatusPreconditionFailed}, wantOpens: false},
		{name: "wrapped 503", err: fmt.Errorf("digest: %w", &HTTPError{Code: http.StatusServiceUnavailable}), wantOpens: true},
		{name: "transport failure", err: errors.New("connect: connection refused"), wantOpens: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			b := newHostBreakers()
			for i := 0; i < hostBreakerThreshold; i++ {
				b.observe(context.Background(), "h1:7001", test.err)
			}
			if test.wantOpens {
				require.ErrorIs(t, b.allow("h1:7001"), ErrHostCircuitOpen)
			} else {
				require.NoError(t, b.allow("h1:7001"))
			}
		})
	}
}

// The breaker must let a recovered node back in on its own.
func TestHostBreakerRecovery(t *testing.T) {
	t.Parallel()

	const host = "h1:7001"
	b := newHostBreakers()
	b.coolOff = 20 * time.Millisecond

	unavailable := &HTTPError{Code: http.StatusServiceUnavailable}
	for i := 0; i < hostBreakerThreshold; i++ {
		b.observe(context.Background(), host, unavailable)
	}
	require.ErrorIs(t, b.allow(host), ErrHostCircuitOpen, "breaker must be open right after the budget is spent")

	time.Sleep(b.coolOff + 10*time.Millisecond)
	require.NoError(t, b.allow(host), "after the cool-off a probe must be let through")

	// still down: one failed probe re-opens without spending the budget again
	b.observe(context.Background(), host, unavailable)
	require.ErrorIs(t, b.allow(host), ErrHostCircuitOpen, "a failed probe must re-open the breaker at once")

	// recovered: the probe succeeds and the host is back in rotation
	time.Sleep(b.coolOff + 10*time.Millisecond)
	require.NoError(t, b.allow(host))
	b.observe(context.Background(), host, nil)
	for i := 0; i < 5; i++ {
		require.NoError(t, b.allow(host), "a recovered host must be served normally")
	}
}

// A recovered host must leave no entry behind: host:port rotates on restart.
func TestHostBreakerEvictsRecoveredHosts(t *testing.T) {
	t.Parallel()

	const host = "h1:7001"
	unavailable := &HTTPError{Code: http.StatusServiceUnavailable}

	tracked := func(b *hostBreakers) int {
		b.mu.RLock()
		defer b.mu.RUnlock()
		return len(b.hosts)
	}

	tests := []struct {
		name string
		run  func(t *testing.T, b *hostBreakers)
	}{
		{
			name: "host that never failed",
			run:  func(t *testing.T, b *hostBreakers) { b.observe(context.Background(), host, nil) },
		},
		{
			name: "request-specific errors",
			run: func(t *testing.T, b *hostBreakers) {
				for i := 0; i < hostBreakerThreshold*2; i++ {
					b.observe(context.Background(), host, &HTTPError{Code: http.StatusNotFound})
				}
			},
		},
		{
			name: "caller cancellation",
			run: func(t *testing.T, b *hostBreakers) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				for i := 0; i < hostBreakerThreshold*2; i++ {
					b.observe(ctx, host, context.Canceled)
				}
			},
		},
		{
			name: "blip below the threshold, then a success",
			run: func(t *testing.T, b *hostBreakers) {
				b.observe(context.Background(), host, unavailable)
				require.Equal(t, 1, tracked(b), "a failing host must be tracked")
				b.observe(context.Background(), host, nil)
			},
		},
		{
			name: "open breaker, then recovery",
			run: func(t *testing.T, b *hostBreakers) {
				for i := 0; i < hostBreakerThreshold; i++ {
					b.observe(context.Background(), host, unavailable)
				}
				require.ErrorIs(t, b.allow(host), ErrHostCircuitOpen)
				require.Equal(t, 1, tracked(b), "an open breaker must be tracked")

				time.Sleep(b.coolOff + 10*time.Millisecond)
				require.NoError(t, b.allow(host))
				b.observe(context.Background(), host, nil)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			b := newHostBreakers()
			b.coolOff = 20 * time.Millisecond
			test.run(t, b)

			require.Zero(t, tracked(b), "a healthy host must leave no entry behind")
			require.NoError(t, b.allow(host))
		})
	}
}

// A host that vanished mid-outage must be evicted even though it never recovered.
func TestHostBreakerEvictsIdleHosts(t *testing.T) {
	t.Parallel()

	b := newHostBreakers()
	unavailable := &HTTPError{Code: http.StatusServiceUnavailable}

	for i := 0; i < hostBreakerThreshold; i++ {
		b.observe(context.Background(), "gone:7001", unavailable)
	}
	require.ErrorIs(t, b.allow("gone:7001"), ErrHostCircuitOpen)

	// backdate the entry past the idle TTL
	b.mu.Lock()
	b.hosts["gone:7001"].lastSeen.Store(time.Now().Add(-2 * hostBreakerIdleTTL).UnixNano())
	b.mu.Unlock()

	b.observe(context.Background(), "live:7001", unavailable)

	b.mu.RLock()
	defer b.mu.RUnlock()
	require.NotContains(t, b.hosts, "gone:7001", "an idle host must be evicted")
	require.Contains(t, b.hosts, "live:7001", "the host that just failed must stay")
}

// A caller that gives up says nothing about the replica's health.
func TestHostBreakerIgnoresCallerCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	b := newHostBreakers()
	for i := 0; i < hostBreakerThreshold*3; i++ {
		b.observe(ctx, "h1:7001", context.Canceled)
	}
	require.NoError(t, b.allow("h1:7001"))
}

// The breaker counts consecutive failures, so blips must never accumulate.
func TestHostBreakerSuccessResetsStreak(t *testing.T) {
	t.Parallel()

	const host = "h1:7001"
	b := newHostBreakers()
	unavailable := &HTTPError{Code: http.StatusServiceUnavailable}
	for i := 0; i < hostBreakerThreshold*5; i++ {
		b.observe(context.Background(), host, unavailable)
		b.observe(context.Background(), host, nil)
		require.NoError(t, b.allow(host))
	}
}

// Race-detector exercise: the breaker is shared by every replica call.
func TestHostBreakerConcurrentUse(t *testing.T) {
	t.Parallel()

	b := newHostBreakers()
	b.coolOff = time.Millisecond

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		host := fmt.Sprintf("h%d:7001", i%4)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_ = b.allow(host)
				if j%3 == 0 {
					b.observe(context.Background(), host, &HTTPError{Code: http.StatusServiceUnavailable})
				} else {
					b.observe(context.Background(), host, nil)
				}
			}
		}()
	}
	wg.Wait()
}

// A shedding node gets at most SHED_ATTEMPTS attempts, not the MAX_RETRIES ladder.
func TestReplicaClient_ShedAttemptsAreCapped(t *testing.T) {
	t.Parallel()

	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "node overloaded, request shed", http.StatusTooManyRequests)
	}))
	defer server.Close()

	c := newReplicationClient(t, server.Client())
	_, err := c.DigestObjects(context.Background(), server.URL[len("http://"):],
		"C1", "S1", []strfmt.UUID{UUID1}, MAX_RETRIES)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "429")
	assert.EqualValues(t, SHED_ATTEMPTS, requests.Load(),
		"a shedding node must get at most %d attempts, not the full retry budget", SHED_ATTEMPTS)
}

// Once the node serves again, traffic resumes on its own.
func TestReplicaClient_BreakerLetsRecoveredHostBackIn(t *testing.T) {
	t.Parallel()

	var ready atomic.Bool
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if !ready.Load() {
			http.Error(w, nodeNotReadyBody, http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte(`[]`)) //nolint:errcheck
	}))
	defer server.Close()

	c := newReplicationClient(t, server.Client())
	c.breakers.coolOff = 20 * time.Millisecond
	host := server.URL[len("http://"):]

	for i := 0; i < hostBreakerThreshold; i++ {
		_, err := c.DigestObjects(context.Background(), host, "C1", "S1", []strfmt.UUID{UUID1}, MAX_RETRIES)
		require.Error(t, err)
	}
	require.EqualValues(t, hostBreakerThreshold, requests.Load())

	_, err := c.DigestObjects(context.Background(), host, "C1", "S1", []strfmt.UUID{UUID1}, MAX_RETRIES)
	require.ErrorIs(t, err, ErrHostCircuitOpen)
	require.EqualValues(t, hostBreakerThreshold, requests.Load(), "an open breaker must not reach the host")

	ready.Store(true)
	time.Sleep(c.breakers.coolOff + 10*time.Millisecond)

	_, err = c.DigestObjects(context.Background(), host, "C1", "S1", []strfmt.UUID{UUID1}, MAX_RETRIES)
	require.NoError(t, err, "a recovered host must be picked up again after the cool-off")
	assert.EqualValues(t, hostBreakerThreshold+1, requests.Load())
}
