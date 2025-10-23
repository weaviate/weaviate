//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package clusterapi_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
	"github.com/weaviate/weaviate/usecases/cluster"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/replica"
)

func TestMaintenanceModeReplicatedIndices(t *testing.T) {
	noopAuth := clusterapi.NewNoopAuthHandler()
	fakeReplicator := newFakeReplicator(false)
	logger, _ := test.NewNullLogger()
	indices := clusterapi.NewReplicatedIndices(fakeReplicator, nil, noopAuth, func() bool { return true }, cluster.RequestQueueConfig{}, logger)
	mux := http.NewServeMux()
	mux.Handle("/replicas/indices/", indices.Indices())
	server := httptest.NewServer(mux)

	defer server.Close()

	maintenanceModeExpectedHTTPStatus := http.StatusTeapot
	requestURL := func(suffix string) string {
		return fmt.Sprintf("%s/replicas/indices/MyClass/shards/myshard%s", server.URL, suffix)
	}
	indicesTestRequests := []indicesTestRequest{
		{"GET", "/objects/_digest"},
		{"PUT", "/objects/_overwrite"},
		{"DELETE", "/objects/deadbeef"},
		{"PATCH", "/objects/deadbeef"},
		{"GET", "/objects/deadbeef"},
		{"POST", "/objects/references"},
		{"GET", "/objects"},
		{"POST", "/objects"},
		{"DELETE", "/objects"},
		{"PUT", "/replication-factor:increase"},
		{"POST", ":commit"},
		{"POST", ":abort"},
	}
	for _, testRequest := range indicesTestRequests {
		t.Run(fmt.Sprintf("%s on %s returns maintenance mode status", testRequest.method, testRequest.suffix), func(t *testing.T) {
			req, err := http.NewRequest(testRequest.method, requestURL(testRequest.suffix), nil)
			assert.Nil(t, err)
			res, err := http.DefaultClient.Do(req)
			assert.Nil(t, err)
			defer res.Body.Close()
			assert.True(t, res.StatusCode == maintenanceModeExpectedHTTPStatus, "expected %d, got %d", maintenanceModeExpectedHTTPStatus, res.StatusCode)
		})
	}
}

func TestReplicatedIndicesWorkQueue(t *testing.T) {
	testCases := []struct {
		name               string
		requestQueueConfig cluster.RequestQueueConfig
		numRequests        int
		expectedAccepted   int
		expectedRejected   int
	}{
		{
			name:               "empty_config",
			requestQueueConfig: cluster.RequestQueueConfig{},
			numRequests:        10,
			expectedAccepted:   10,
			expectedRejected:   0,
		},
		{
			name: "disabled_10reqs",
			requestQueueConfig: cluster.RequestQueueConfig{
				IsEnabled: configRuntime.NewDynamicValue(false),
			},
			numRequests:      10,
			expectedAccepted: 10,
			expectedRejected: 0,
		},
		{
			name: "disabled_10reqs_0config",
			requestQueueConfig: cluster.RequestQueueConfig{
				IsEnabled:           configRuntime.NewDynamicValue(false),
				NumWorkers:          0,
				QueueSize:           0,
				QueueFullHttpStatus: 0,
			},
			numRequests:      10,
			expectedAccepted: 10,
			expectedRejected: 0,
		},
		{
			name: "disabled_10reqs_1config",
			requestQueueConfig: cluster.RequestQueueConfig{
				IsEnabled:           configRuntime.NewDynamicValue(false),
				NumWorkers:          1,
				QueueSize:           1,
				QueueFullHttpStatus: 1,
			},
			numRequests:      10,
			expectedAccepted: 10,
			expectedRejected: 0,
		},
		{
			// the implementation ensures that at least one worker is running
			name: "enabled_10reqs_0workers_1buffer_429",
			requestQueueConfig: cluster.RequestQueueConfig{
				IsEnabled:           configRuntime.NewDynamicValue(true),
				NumWorkers:          0,
				QueueSize:           1,
				QueueFullHttpStatus: http.StatusTooManyRequests,
			},
			numRequests:      10,
			expectedAccepted: 2,
			expectedRejected: 8,
		},
		{
			name: "enabled_10reqs_2workers_3buffer_429",
			requestQueueConfig: cluster.RequestQueueConfig{
				IsEnabled:           configRuntime.NewDynamicValue(true),
				NumWorkers:          2,
				QueueSize:           3,
				QueueFullHttpStatus: http.StatusTooManyRequests,
			},
			numRequests:      10,
			expectedAccepted: 5,
			expectedRejected: 5,
		},
		{
			name:        "enabled_10reqs_32workers_1024buffer_429",
			numRequests: 10,
			requestQueueConfig: cluster.RequestQueueConfig{
				IsEnabled:           configRuntime.NewDynamicValue(true),
				NumWorkers:          32,
				QueueSize:           1024,
				QueueFullHttpStatus: http.StatusTooManyRequests,
			},
			expectedAccepted: 10,
			expectedRejected: 0,
		},
		{
			name: "enabled_10reqs_5workers_0buffer_429",
			requestQueueConfig: cluster.RequestQueueConfig{
				IsEnabled:           configRuntime.NewDynamicValue(true),
				NumWorkers:          5,
				QueueSize:           0,
				QueueFullHttpStatus: http.StatusTooManyRequests,
			},
			numRequests:      10,
			expectedAccepted: 5,
			expectedRejected: 5,
		},
		{
			name: "enabled_10reqs_1workers_1buffer_504",
			requestQueueConfig: cluster.RequestQueueConfig{
				IsEnabled:           configRuntime.NewDynamicValue(true),
				NumWorkers:          1,
				QueueSize:           1,
				QueueFullHttpStatus: http.StatusGatewayTimeout,
			},
			numRequests:      10,
			expectedAccepted: 2,
			expectedRejected: 8,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			noopAuth := clusterapi.NewNoopAuthHandler()
			fakeReplicator := newFakeReplicator(true)
			logger, _ := test.NewNullLogger()
			indices := clusterapi.NewReplicatedIndices(fakeReplicator, nil, noopAuth, func() bool { return false }, tc.requestQueueConfig, logger)
			mux := http.NewServeMux()
			mux.Handle("/replicas/indices/", indices.Indices())
			server := httptest.NewServer(mux)
			defer server.Close()

			requestKey := fmt.Sprintf("%s=%s", replica.RequestKey, "test_request_id")
			req, err := http.NewRequest("POST", fmt.Sprintf("%s/replicas/indices/MyClass/shards/myshard:commit?%s", server.URL, requestKey), nil)
			assert.Nil(t, err)
			wgAccepted := sync.WaitGroup{}
			wgRejected := sync.WaitGroup{}
			wgAccepted.Add(tc.expectedAccepted)
			wgRejected.Add(tc.expectedRejected)
			httpStatuses := make(chan int, tc.numRequests)
			for i := 0; i < tc.numRequests; i++ {
				go func() {
					res, err := http.DefaultClient.Do(req)
					assert.Nil(t, err)
					defer res.Body.Close()
					httpStatuses <- res.StatusCode
					if res.StatusCode == http.StatusOK {
						wgAccepted.Done()
					} else if res.StatusCode == tc.requestQueueConfig.QueueFullHttpStatus {
						wgRejected.Done()
					} else {
						// unexpected status code received
						fmt.Println("unexpected status code: ", res.StatusCode)
						t.Fail()
					}
				}()
			}
			wgRejected.Wait()
			for i := 0; i < tc.expectedAccepted; i++ {
				fakeReplicator.commitBlock <- struct{}{}
			}
			wgAccepted.Wait()
			close(httpStatuses)

			actualAccepted := 0
			actualRejected := 0
			for httpStatus := range httpStatuses {
				if httpStatus == http.StatusOK {
					actualAccepted++
				} else if httpStatus == tc.requestQueueConfig.QueueFullHttpStatus {
					actualRejected++
				} else {
					fmt.Println("unexpected status code: ", httpStatus)
					t.Fail()
				}
			}
			assert.Equal(t, tc.expectedAccepted, actualAccepted)
			assert.Equal(t, tc.expectedRejected, actualRejected)
		})
	}
}

func TestReplicatedIndicesShutdown(t *testing.T) {
	testCases := []struct {
		name               string
		requestQueueConfig cluster.RequestQueueConfig
		numRequests        int
		shutdownTimeout    time.Duration
	}{
		{
			name: "shutdown_with_no_queue",
			requestQueueConfig: cluster.RequestQueueConfig{
				IsEnabled: configRuntime.NewDynamicValue(false),
			},
			numRequests:     0,
			shutdownTimeout: 1 * time.Second,
		},
		{
			name: "shutdown_with_empty_queue",
			requestQueueConfig: cluster.RequestQueueConfig{
				IsEnabled:  configRuntime.NewDynamicValue(true),
				NumWorkers: 2,
				QueueSize:  10,
			},
			numRequests:     0,
			shutdownTimeout: 1 * time.Second,
		},
		{
			name: "shutdown_with_pending_requests",
			requestQueueConfig: cluster.RequestQueueConfig{
				IsEnabled:                   configRuntime.NewDynamicValue(true),
				NumWorkers:                  1,
				QueueSize:                   5,
				QueueShutdownTimeoutSeconds: 1,
			},
			numRequests:     3,
			shutdownTimeout: 2 * time.Second,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			noopAuth := clusterapi.NewNoopAuthHandler()
			fakeReplicator := newFakeReplicator(false)
			logger, _ := test.NewNullLogger()

			indices := clusterapi.NewReplicatedIndices(
				fakeReplicator,
				nil,
				noopAuth,
				func() bool { return false },
				tc.requestQueueConfig,
				logger,
			)

			mux := http.NewServeMux()
			mux.Handle("/replicas/indices/", indices.Indices())
			server := httptest.NewServer(mux)
			defer server.Close()

			// Send requests if needed
			wg := sync.WaitGroup{}
			requestKey := fmt.Sprintf("%s=%s", replica.RequestKey, "test_request_id")

			for i := 0; i < tc.numRequests; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					req, err := http.NewRequest("POST",
						fmt.Sprintf("%s/replicas/indices/MyClass/shards/myshard:commit?%s",
							server.URL, requestKey), nil)
					assert.Nil(t, err)

					res, err := http.DefaultClient.Do(req)
					if err == nil {
						res.Body.Close()
					}
				}()
			}

			// Test shutdown
			ctx, cancel := context.WithTimeout(t.Context(), tc.shutdownTimeout)
			defer cancel()

			start := time.Now()
			err := indices.Close(ctx)
			shutdownDuration := time.Since(start)
			// Should shutdown gracefully
			assert.NoError(t, err)
			// Shutdown should be reasonably fast
			assert.True(t, shutdownDuration < 1*time.Second)

			// Wait for any remaining requests to complete
			wg.Wait()

			// Test that new requests are rejected after shutdown
			req, err := http.NewRequest("POST",
				fmt.Sprintf("%s/replicas/indices/MyClass/shards/myshard:commit?%s",
					server.URL, requestKey), nil)
			assert.Nil(t, err)

			res, err := http.DefaultClient.Do(req)
			assert.Nil(t, err)
			defer res.Body.Close()

			// Should get 503 Service Unavailable after shutdown
			assert.Equal(t, http.StatusServiceUnavailable, res.StatusCode)
		})
	}
}

// TestReplicatedIndicesRejectsRequestsDuringShutdown verifies that the request queue
// properly rejects new requests with HTTP 503 during shutdown, and that there's no
// data race between closing the requestQueue channel and handlers attempting to send on it.
//
// This test uses an aggressive stress strategy to maximize the likelihood of triggering
// the race condition that was detected in CI.
func TestReplicatedIndicesRejectsRequestsDuringShutdown(t *testing.T) {
	noopAuth := clusterapi.NewNoopAuthHandler()
	fakeReplicator := newFakeReplicator(true)
	logger, _ := test.NewNullLogger()

	// Configure with enough workers and queue size to handle concurrent load
	cfg := cluster.RequestQueueConfig{
		IsEnabled:  configRuntime.NewDynamicValue(true),
		NumWorkers: 10,
		QueueSize:  100,
	}

	indices := clusterapi.NewReplicatedIndices(
		fakeReplicator,
		nil,
		noopAuth,
		func() bool { return false },
		cfg,
		logger,
	)

	mux := http.NewServeMux()
	mux.Handle("/replicas/indices/", indices.Indices())
	server := httptest.NewServer(mux)
	defer server.Close()

	requestKey := fmt.Sprintf("%s=%s", replica.RequestKey, "shutdown_race")
	reqURL := fmt.Sprintf("%s/replicas/indices/MyClass/shards/myshard:commit?%s", server.URL, requestKey)

	// Send one request that will block in a worker to simulate active processing during shutdown.
	// This ensures workers are busy when Close() is called, mimicking real production scenarios.
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		req, err := http.NewRequest("POST", reqURL, nil)
		require.NoError(t, err)

		res, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer res.Body.Close()
	}()

	// Wait for the first request to start processing before proceeding with shutdown test
	select {
	case <-fakeReplicator.WaitForStart():
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for first request to start")
	}

	// Stress test strategy to trigger the race between Close() and handler goroutines:
	// Without the RWMutex fix, this test reliably reproduces the data race where:
	// - Close() tries to close(requestQueue) at indices_replicas.go:1029
	// - Handlers try to send on requestQueue at indices_replicas.go:207
	// The fix uses RWMutex to ensure these operations cannot happen concurrently.

	const numConcurrentRequests = 100
	var wg sync.WaitGroup
	got503 := atomic.Bool{}

	// Helper to launch a wave of concurrent requests
	sendRequests := func() {
		for i := 0; i < numConcurrentRequests; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				req, err := http.NewRequest("POST", reqURL, nil)
				if err != nil {
					return
				}

				res, err := http.DefaultClient.Do(req)
				if err != nil {
					return
				}
				defer func() { _ = res.Body.Close() }()

				// Track if we got 503 (shutdown response)
				if res.StatusCode == http.StatusServiceUnavailable {
					got503.Store(true)
				}
			}()
		}
	}

	// Phase 1: Launch first wave of 100 concurrent requests to create queue pressure
	sendRequests()

	// Phase 2: Give requests time to queue up, then initiate shutdown
	time.Sleep(10 * time.Millisecond)

	closeErr := make(chan error, 1)
	go func() {
		closeErr <- indices.Close(context.Background())
	}()

	// Phase 3: Launch second wave immediately during shutdown
	// This creates maximum contention - new requests racing with Close() closing the channel.
	// Without the RWMutex fix, this reliably triggers the race detector.
	time.Sleep(1 * time.Millisecond) // Brief delay to let Close() start
	sendRequests()

	// Phase 4: Unblock the worker processing the first request so shutdown can complete
	time.Sleep(5 * time.Millisecond)
	fakeReplicator.Done()

	// Wait for all concurrent requests to finish
	wg.Wait()

	// Verify that at least some requests during shutdown received proper 503 rejection
	require.True(t, got503.Load(), "expected at least one request to receive 503 during shutdown")

	require.NoError(t, <-closeErr)
	<-firstDone
}

func TestReplicatedIndicesShutdownMultipleCalls(t *testing.T) {
	noopAuth := clusterapi.NewNoopAuthHandler()
	fakeReplicator := newFakeReplicator(false)
	logger, _ := test.NewNullLogger()

	indices := clusterapi.NewReplicatedIndices(
		fakeReplicator,
		nil,
		noopAuth,
		func() bool { return false },
		cluster.RequestQueueConfig{
			IsEnabled:  configRuntime.NewDynamicValue(true),
			NumWorkers: 1,
			QueueSize:  5,
		},
		logger,
	)

	// First shutdown should succeed
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := indices.Close(ctx)
	assert.NoError(t, err)

	// Second shutdown should also succeed (no error) due to sync.Once
	err = indices.Close(ctx)
	assert.NoError(t, err)

	// Third shutdown should also succeed (no error) due to sync.Once
	err = indices.Close(ctx)
	assert.NoError(t, err)
}

func TestReplicatedIndicesShutdownWithStuckRequests(t *testing.T) {
	noopAuth := clusterapi.NewNoopAuthHandler()
	// Create a fake replicator that blocks on commit operations
	fakeReplicator := newFakeReplicator(true) // This will block on commit
	logger, _ := test.NewNullLogger()

	indices := clusterapi.NewReplicatedIndices(
		fakeReplicator,
		nil,
		noopAuth,
		func() bool { return false },
		cluster.RequestQueueConfig{
			IsEnabled:                   configRuntime.NewDynamicValue(true),
			NumWorkers:                  1,
			QueueSize:                   5,
			QueueShutdownTimeoutSeconds: 1,
		},
		logger,
	)

	mux := http.NewServeMux()
	mux.Handle("/replicas/indices/", indices.Indices())
	server := httptest.NewServer(mux)
	defer server.Close()

	// Send a request that will get stuck
	requestKey := fmt.Sprintf("%s=%s", replica.RequestKey, "stuck_request")
	req, err := http.NewRequest("POST",
		fmt.Sprintf("%s/replicas/indices/MyClass/shards/myshard:commit?%s",
			server.URL, requestKey), nil)
	assert.Nil(t, err)

	// Start the request in a goroutine (it will get stuck)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		res, err := http.DefaultClient.Do(req)
		if err == nil {
			res.Body.Close()
		}
	}()

	// Wait for the operation to actually start (using this to avoid sleep)
	select {
	case <-fakeReplicator.WaitForStart():
		// Operation has started, we can proceed with shutdown test
	case <-time.After(1 * time.Second):
		t.Fatal("operation did not start within timeout")
	}

	// Now try to shutdown - this should timeout because the request is stuck
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	start := time.Now()
	err = indices.Close(ctx)
	shutdownDuration := time.Since(start)

	// Should get a timeout error because the worker is stuck
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "shutdown timeout reached")

	// Shutdown should have taken at least the configured timeout
	assert.True(t, shutdownDuration >= 500*time.Millisecond)

	fakeReplicator.Done()

	// Wait for the stuck request to complete (it should eventually timeout)
	wg.Wait()
}
