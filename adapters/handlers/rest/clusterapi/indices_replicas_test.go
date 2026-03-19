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

package clusterapi_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/cluster"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	replicaTypes "github.com/weaviate/weaviate/usecases/replica/types"
)

func TestMaintenanceModeReplicatedIndices(t *testing.T) {
	noopAuth := clusterapi.NewNoopAuthHandler()
	fakeReplicator := replicaTypes.NewMockReplicator(t)
	logger, _ := test.NewNullLogger()
	indices := clusterapi.NewReplicatedIndices(fakeReplicator, noopAuth, func() bool { return true }, cluster.RequestQueueConfig{}, logger, func() bool { return true })
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
		{"POST", "/objects/digestsInRange"},
		{"POST", "/objects/compareDigests"},
		{"POST", "/objects/hashtree/level/0"},
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
			fakeReplicator := replicaTypes.NewMockReplicator(t)
			commitBlock := make(chan struct{})

			//  Configure CommitReplication to block until signaled
			fakeReplicator.EXPECT().CommitReplication(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Run(func(_ context.Context, _ string, _ string, _ string) {
				<-commitBlock
			}).Return(replica.SimpleResponse{})

			logger, _ := test.NewNullLogger()
			indices := clusterapi.NewReplicatedIndices(fakeReplicator, noopAuth, func() bool { return false }, tc.requestQueueConfig, logger, func() bool { return true })
			mux := http.NewServeMux()
			mux.Handle("/replicas/indices/", indices.Indices())
			server := httptest.NewServer(mux)
			defer server.Close()

			requestKey := fmt.Sprintf("%s=%s", replica.RequestKey, "test_request_id")
			req, err := http.NewRequest("POST", fmt.Sprintf("%s/replicas/indices/MyClass/shards/myshard:commit?%s", server.URL, requestKey), nil)
			assert.Nil(t, err)
			var wgAll sync.WaitGroup
			wgAll.Add(tc.numRequests)
			rejectedCh := make(chan struct{}, tc.numRequests)
			httpStatuses := make(chan int, tc.numRequests)
			for i := 0; i < tc.numRequests; i++ {
				go func() {
					defer wgAll.Done()
					res, err := http.DefaultClient.Do(req.Clone(req.Context()))
					assert.Nil(t, err)
					defer res.Body.Close()
					httpStatuses <- res.StatusCode
					if res.StatusCode == http.StatusOK {
						// accepted — will be unblocked by close(commitBlock) below
					} else if res.StatusCode == tc.requestQueueConfig.QueueFullHttpStatus {
						rejectedCh <- struct{}{}
					} else {
						// unexpected status code received
						fmt.Println("unexpected status code: ", res.StatusCode)
						t.Fail()
					}
				}()
			}
			// Wait until we have seen enough rejections, then unblock all accepted requests
			for i := 0; i < tc.expectedRejected; i++ {
				<-rejectedCh
			}
			close(commitBlock)
			wgAll.Wait()
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
			fakeReplicator := replicaTypes.NewMockReplicator(t)
			fakeReplicator.EXPECT().
				CommitReplication(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(replica.SimpleResponse{}).
				Maybe()
			logger, _ := test.NewNullLogger()

			indices := clusterapi.NewReplicatedIndices(
				fakeReplicator,
				noopAuth,
				func() bool { return false },
				tc.requestQueueConfig,
				logger,
				func() bool { return true },
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

// TestReplicatedIndicesRejectsRequestsDuringShutdown verifies that requests arriving
// during shutdown receive HTTP 503 responses instead of being enqueued or causing errors.
func TestReplicatedIndicesRejectsRequestsDuringShutdown(t *testing.T) {
	noopAuth := clusterapi.NewNoopAuthHandler()
	fakeReplicator := replicaTypes.NewMockReplicator(t)
	startSignal := make(chan struct{})
	doneSignal := make(chan struct{})

	// Configure CommitReplication to signal start and block until done
	fakeReplicator.EXPECT().CommitReplication(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Run(func(_ context.Context, _ string, _ string, _ string) {
		select {
		case startSignal <- struct{}{}:
		default:
		}
		<-doneSignal
	}).Return(replica.SimpleResponse{})

	logger, _ := test.NewNullLogger()

	cfg := cluster.RequestQueueConfig{
		IsEnabled:  configRuntime.NewDynamicValue(true),
		NumWorkers: 2,
		QueueSize:  10,
	}

	indices := clusterapi.NewReplicatedIndices(
		fakeReplicator,
		noopAuth,
		func() bool { return false },
		cfg,
		logger,
		func() bool { return true },
	)

	mux := http.NewServeMux()
	mux.Handle("/replicas/indices/", indices.Indices())
	server := httptest.NewServer(mux)
	defer server.Close()

	requestKey := fmt.Sprintf("%s=%s", replica.RequestKey, "shutdown_test")
	reqURL := fmt.Sprintf("%s/replicas/indices/MyClass/shards/myshard:commit?%s", server.URL, requestKey)

	// Send one request that will block in a worker to simulate active processing during shutdown
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		req, err := http.NewRequest("POST", reqURL, nil)
		require.NoError(t, err)

		res, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		_ = res.Body.Close()
	}()

	// Wait for the first request to start processing
	select {
	case <-startSignal:
	case <-t.Context().Done():
		t.Fatalf("timed out waiting for first request to start")
	}

	// Start shutdown
	closeErr := make(chan error, 1)
	go func() {
		closeErr <- indices.Close(context.Background())
	}()

	// Wait until Close() has set isShutdown = true before sending requests,
	// so we have a happens-before guarantee and the test is not racy.
	select {
	case <-indices.ShuttingDown():
	case <-t.Context().Done():
		t.Fatalf("timed out waiting for shutdown to start")
	}

	// Send a few concurrent requests while shutdown is in progress
	const numConcurrentRequests = 10
	var wg sync.WaitGroup
	got503 := atomic.Bool{}

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
			if res.StatusCode == http.StatusServiceUnavailable {
				got503.Store(true)
			}
		}()
	}

	// Unblock all workers blocked on the mock. close() unblocks every
	// receiver, not just one, so even if multiple workers picked up
	// requests before isShutdown was set, they all get released.
	time.Sleep(10 * time.Millisecond)
	close(doneSignal)

	// Wait for all requests to finish
	wg.Wait()

	// Verify that at least some requests during shutdown received 503
	require.True(t, got503.Load(), "expected requests during shutdown to receive 503")

	require.NoError(t, <-closeErr)
	<-firstDone
}

func TestReplicatedIndicesShutdownMultipleCalls(t *testing.T) {
	noopAuth := clusterapi.NewNoopAuthHandler()
	fakeReplicator := replicaTypes.NewMockReplicator(t)
	logger, _ := test.NewNullLogger()

	indices := clusterapi.NewReplicatedIndices(
		fakeReplicator,
		noopAuth,
		func() bool { return false },
		cluster.RequestQueueConfig{
			IsEnabled:  configRuntime.NewDynamicValue(true),
			NumWorkers: 1,
			QueueSize:  5,
		},
		logger,
		func() bool { return true },
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
	fakeReplicator := replicaTypes.NewMockReplicator(t)
	startSignal := make(chan struct{})
	doneSignal := make(chan struct{})

	// Configure CommitReplication to signal start and block until done
	fakeReplicator.EXPECT().CommitReplication(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Run(func(_ context.Context, _ string, _ string, _ string) {
		select {
		case startSignal <- struct{}{}:
		default:
		}
		<-doneSignal
	}).Return(replica.SimpleResponse{})

	logger, _ := test.NewNullLogger()

	indices := clusterapi.NewReplicatedIndices(
		fakeReplicator,
		noopAuth,
		func() bool { return false },
		cluster.RequestQueueConfig{
			IsEnabled:                   configRuntime.NewDynamicValue(true),
			NumWorkers:                  1,
			QueueSize:                   5,
			QueueShutdownTimeoutSeconds: 1,
		},
		logger,
		func() bool { return true },
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
	case <-startSignal:
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

	doneSignal <- struct{}{}

	// Wait for the stuck request to complete (it should eventually timeout)
	wg.Wait()
}

func newOverwriteServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	logger, _ := test.NewNullLogger()
	indices := clusterapi.NewReplicatedIndices(
		newFakeReplicator(false),
		clusterapi.NewNoopAuthHandler(),
		func() bool { return false },
		cluster.RequestQueueConfig{},
		logger,
		func() bool { return true },
	)
	mux := http.NewServeMux()
	mux.Handle("/replicas/indices/", indices.Indices())
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	url := server.URL + "/replicas/indices/C1/shards/S1/objects/_overwrite"
	return server, url
}

func vobjectPayload(t *testing.T) []byte {
	t.Helper()
	now := time.Now()
	vobjs := []*objects.VObject{
		{
			LatestObject: &models.Object{
				ID:                 strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168241"),
				Class:              "C1",
				LastUpdateTimeUnix: now.UnixMilli(),
			},
			StaleUpdateTime: now.UnixMilli(),
		},
	}
	body, err := clusterapi.IndicesPayloads.VersionedObjectList.MarshalV2(vobjs)
	require.NoError(t, err)
	return body
}

func TestPutOverwriteObjectsCompression(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name           string
		compressionHdr string
		encodingHdr    string
		compressBody   bool
		useBinaryBody  bool
		wantStatus     int
	}{
		{
			name:           "ZstdCompressedBinaryRequest",
			compressionHdr: "zstd",
			encodingHdr:    "binary",
			compressBody:   true,
			useBinaryBody:  true,
			wantStatus:     http.StatusOK,
		},

		{
			name:           "UncompressedJSONFallback",
			compressionHdr: "",
			encodingHdr:    "",
			compressBody:   false,
			useBinaryBody:  false,
			wantStatus:     http.StatusOK,
		},

		{
			name:           "UnsupportedCompressionAlgorithm",
			compressionHdr: "gzip",
			compressBody:   false,
			useBinaryBody:  false,
			wantStatus:     http.StatusBadRequest,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, url := newOverwriteServer(t)

			var body []byte
			if tc.useBinaryBody {
				body = vobjectPayload(t) // MarshalV2 binary format
			} else {
				now := time.Now()
				vobjs := []*objects.VObject{{
					LatestObject: &models.Object{
						ID:                 strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168241"),
						Class:              "C1",
						LastUpdateTimeUnix: now.UnixMilli(),
					},
					StaleUpdateTime: now.UnixMilli(),
				}}
				var err error
				body, err = clusterapi.IndicesPayloads.VersionedObjectList.Marshal(vobjs)
				require.NoError(t, err)
			}

			if tc.compressBody {
				enc, err := zstd.NewWriter(nil)
				require.NoError(t, err)
				body = enc.EncodeAll(body, make([]byte, 0, len(body)))
				enc.Close()
			}

			req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
			require.NoError(t, err)
			if tc.compressionHdr != "" {
				req.Header.Set("X-Request-Compression", tc.compressionHdr)
			}
			if tc.encodingHdr != "" {
				req.Header.Set("X-Request-Encoding", tc.encodingHdr)
			}

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, tc.wantStatus, resp.StatusCode)

			if tc.wantStatus == http.StatusOK {
				var result []interface{}
				require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
			}
		})
	}
}

// newCompareDigestsServer builds a test HTTP server that serves the replicated
// indices handler backed by the supplied replicator.
func newCompareDigestsServer(t *testing.T, replicator replicaTypes.Replicator) (*httptest.Server, string) {
	t.Helper()
	logger, _ := test.NewNullLogger()
	indices := clusterapi.NewReplicatedIndices(
		replicator,
		clusterapi.NewNoopAuthHandler(),
		func() bool { return false },
		cluster.RequestQueueConfig{},
		logger,
		func() bool { return true },
	)
	mux := http.NewServeMux()
	mux.Handle("/replicas/indices/", indices.Indices())
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	url := server.URL + "/replicas/indices/C1/shards/S1/objects/compareDigests"
	return server, url
}

// buildBinaryDigests encodes a slice of RepairResponse records into the 25-byte
// binary wire format used by postCompareDigests (UUID + UpdateTime + flags byte).
func buildBinaryDigests(t *testing.T, records []types.RepairResponse) []byte {
	t.Helper()
	buf := make([]byte, len(records)*replica.CompareDigestsRecordLength)
	for i, r := range records {
		id, err := uuid.Parse(r.ID)
		require.NoError(t, err)
		off := i * replica.CompareDigestsRecordLength
		copy(buf[off:], id[:])
		binary.BigEndian.PutUint64(buf[off+16:], uint64(r.UpdateTime))
		if r.Deleted {
			buf[off+24] = replica.CompareDigestsFlagDeleted
		}
	}
	return buf
}

func TestPostCompareDigests(t *testing.T) {
	t.Parallel()

	const (
		testUUID1 = "73f2eb5f-5abf-447a-81ca-74b1dd168241"
		testUUID2 = "73f2eb5f-5abf-447a-81ca-74b1dd168242"
	)

	now := time.Now().UnixMilli()

	sourceDigests := []types.RepairResponse{
		{ID: testUUID1, UpdateTime: now},
		{ID: testUUID2, UpdateTime: now + 1000},
	}
	// The target reports uuid1 as missing and uuid2 as stale.
	staleDigests := []types.RepairResponse{
		{ID: testUUID1, UpdateTime: 0},
		{ID: testUUID2, UpdateTime: now},
	}

	t.Run("HappyPath", func(t *testing.T) {
		t.Parallel()
		rep := replicaTypes.NewMockReplicator(t)
		rep.EXPECT().
			CompareDigests(mock.Anything, "C1", "S1", mock.MatchedBy(func(d []types.RepairResponse) bool {
				return len(d) == 2
			})).
			Return(staleDigests, nil)

		_, url := newCompareDigestsServer(t, rep)

		body := buildBinaryDigests(t, sourceDigests)
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/octet-stream")
		// compareDigests is binary-only; no X-Accept-Response-Encoding needed.

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "application/octet-stream", resp.Header.Get("Content-Type"))
		// No X-Response-Encoding header — the protocol is unconditionally binary.
		assert.Empty(t, resp.Header.Get("X-Response-Encoding"))

		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, len(staleDigests)*replica.CompareDigestsRecordLength, len(respBody))

		// Decode and verify each returned stale record.
		for i, want := range staleDigests {
			off := i * replica.CompareDigestsRecordLength
			rec := respBody[off : off+replica.CompareDigestsRecordLength]
			gotID, err := uuid.FromBytes(rec[:16])
			require.NoError(t, err)
			assert.Equal(t, want.ID, gotID.String())
			assert.Equal(t, uint64(want.UpdateTime), binary.BigEndian.Uint64(rec[16:24]))
			assert.Equal(t, want.Deleted, rec[24]&replica.CompareDigestsFlagDeleted != 0)
		}
	})

	t.Run("EmptyBody", func(t *testing.T) {
		t.Parallel()
		rep := replicaTypes.NewMockReplicator(t)
		rep.EXPECT().
			CompareDigests(mock.Anything, "C1", "S1", []types.RepairResponse(nil)).
			Return(nil, nil)

		_, url := newCompareDigestsServer(t, rep)

		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, http.NoBody)
		require.NoError(t, err)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Empty(t, respBody)
	})

	t.Run("InvalidPayloadLength", func(t *testing.T) {
		t.Parallel()
		// A payload of 10 bytes is not a multiple of CompareDigestsRecordLength (25).
		rep := newFakeReplicator(false)
		_, url := newCompareDigestsServer(t, rep)

		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url,
			bytes.NewReader([]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a}))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/octet-stream")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})

	t.Run("ReplicatorError", func(t *testing.T) {
		t.Parallel()
		rep := replicaTypes.NewMockReplicator(t)
		rep.EXPECT().
			CompareDigests(mock.Anything, "C1", "S1", mock.Anything).
			Return(nil, fmt.Errorf("storage unavailable"))

		_, url := newCompareDigestsServer(t, rep)

		body := buildBinaryDigests(t, sourceDigests)
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/octet-stream")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	})

	t.Run("RequestPayloadRoundTrip", func(t *testing.T) {
		t.Parallel()
		// Verify that the handler decodes the binary request faithfully:
		// each UUID and UpdateTime must match what the client encoded.
		var gotDigests []types.RepairResponse
		rep := replicaTypes.NewMockReplicator(t)
		rep.EXPECT().
			CompareDigests(mock.Anything, "C1", "S1", mock.MatchedBy(func(d []types.RepairResponse) bool {
				gotDigests = d
				return true
			})).
			Return(nil, nil)

		_, url := newCompareDigestsServer(t, rep)

		body := buildBinaryDigests(t, sourceDigests)
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(body))
		require.NoError(t, err)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)

		require.Len(t, gotDigests, len(sourceDigests))
		for i, want := range sourceDigests {
			assert.Equal(t, want.ID, gotDigests[i].ID)
			assert.Equal(t, want.UpdateTime, gotDigests[i].UpdateTime)
		}
	})

	t.Run("DeletedFlagRoundTrip", func(t *testing.T) {
		t.Parallel()
		// When the replicator returns Deleted=true for an object, the response
		// flags byte must have CompareDigestsFlagDeleted set, and the client
		// must decode it back to Deleted=true.
		deletedDigests := []types.RepairResponse{
			{ID: testUUID1, UpdateTime: now - 1000, Deleted: true},
			{ID: testUUID2, UpdateTime: now, Deleted: false},
		}
		rep := replicaTypes.NewMockReplicator(t)
		rep.EXPECT().
			CompareDigests(mock.Anything, "C1", "S1", mock.Anything).
			Return(deletedDigests, nil)

		_, url := newCompareDigestsServer(t, rep)

		body := buildBinaryDigests(t, sourceDigests)
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/octet-stream")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, len(deletedDigests)*replica.CompareDigestsRecordLength, len(respBody))

		for i, want := range deletedDigests {
			off := i * replica.CompareDigestsRecordLength
			rec := respBody[off : off+replica.CompareDigestsRecordLength]
			gotID, err := uuid.FromBytes(rec[:16])
			require.NoError(t, err)
			assert.Equal(t, want.ID, gotID.String())
			assert.Equal(t, uint64(want.UpdateTime), binary.BigEndian.Uint64(rec[16:24]))
			assert.Equal(t, want.Deleted, rec[24]&replica.CompareDigestsFlagDeleted != 0,
				"record %d: Deleted flag mismatch", i)
		}
	})
}
