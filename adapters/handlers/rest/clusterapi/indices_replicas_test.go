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
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/replica"
)

func TestMaintenanceModeReplicatedIndices(t *testing.T) {
	noopAuth := clusterapi.NewNoopAuthHandler()
	fakeReplicator := newFakeReplicator()
	indices := clusterapi.NewReplicatedIndices(fakeReplicator, nil, noopAuth, func() bool { return true }, clusterapi.WorkQueueConfig{})
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
		name             string
		workQueueConfig  clusterapi.WorkQueueConfig
		numRequests      int
		expectedAccepted int
		expectedRejected int
	}{
		{
			name:             "empty_config",
			workQueueConfig:  clusterapi.WorkQueueConfig{},
			numRequests:      10,
			expectedAccepted: 10,
			expectedRejected: 0,
		},
		{
			name: "disabled_10reqs",
			workQueueConfig: clusterapi.WorkQueueConfig{
				IsEnabled: configRuntime.NewDynamicValue(false),
			},
			numRequests:      10,
			expectedAccepted: 10,
			expectedRejected: 0,
		},
		{
			name: "disabled_10reqs_0config",
			workQueueConfig: clusterapi.WorkQueueConfig{
				IsEnabled:            configRuntime.NewDynamicValue(false),
				NumWorkers:           0,
				BufferSize:           0,
				BufferFullHttpStatus: 0,
			},
			numRequests:      10,
			expectedAccepted: 10,
			expectedRejected: 0,
		},
		{
			name: "disabled_10reqs_1config",
			workQueueConfig: clusterapi.WorkQueueConfig{
				IsEnabled:            configRuntime.NewDynamicValue(false),
				NumWorkers:           1,
				BufferSize:           1,
				BufferFullHttpStatus: 1,
			},
			numRequests:      10,
			expectedAccepted: 10,
			expectedRejected: 0,
		},
		{
			// the implementation ensures that at least one worker is running
			name: "enabled_10reqs_0workers_1buffer_429",
			workQueueConfig: clusterapi.WorkQueueConfig{
				IsEnabled:            configRuntime.NewDynamicValue(true),
				NumWorkers:           0,
				BufferSize:           1,
				BufferFullHttpStatus: http.StatusTooManyRequests,
			},
			numRequests:      10,
			expectedAccepted: 2,
			expectedRejected: 8,
		},
		{
			name: "enabled_10reqs_2workers_3buffer_429",
			workQueueConfig: clusterapi.WorkQueueConfig{
				IsEnabled:            configRuntime.NewDynamicValue(true),
				NumWorkers:           2,
				BufferSize:           3,
				BufferFullHttpStatus: http.StatusTooManyRequests,
			},
			numRequests:      10,
			expectedAccepted: 5,
			expectedRejected: 5,
		},
		{
			name:        "enabled_10reqs_32workers_1024buffer_429",
			numRequests: 10,
			workQueueConfig: clusterapi.WorkQueueConfig{
				IsEnabled:            configRuntime.NewDynamicValue(true),
				NumWorkers:           32,
				BufferSize:           1024,
				BufferFullHttpStatus: http.StatusTooManyRequests,
			},
			expectedAccepted: 10,
			expectedRejected: 0,
		},
		{
			name: "enabled_10reqs_5workers_0buffer_429",
			workQueueConfig: clusterapi.WorkQueueConfig{
				IsEnabled:            configRuntime.NewDynamicValue(true),
				NumWorkers:           5,
				BufferSize:           0,
				BufferFullHttpStatus: http.StatusTooManyRequests,
			},
			numRequests:      10,
			expectedAccepted: 5,
			expectedRejected: 5,
		},
		{
			name: "enabled_10reqs_1workers_1buffer_504",
			workQueueConfig: clusterapi.WorkQueueConfig{
				IsEnabled:            configRuntime.NewDynamicValue(true),
				NumWorkers:           1,
				BufferSize:           1,
				BufferFullHttpStatus: http.StatusGatewayTimeout,
			},
			numRequests:      10,
			expectedAccepted: 2,
			expectedRejected: 8,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			noopAuth := clusterapi.NewNoopAuthHandler()
			fakeReplicator := newFakeReplicator()
			indices := clusterapi.NewReplicatedIndices(fakeReplicator, nil, noopAuth, func() bool { return false }, tc.workQueueConfig)
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
					} else if res.StatusCode == tc.workQueueConfig.BufferFullHttpStatus {
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
				} else if httpStatus == tc.workQueueConfig.BufferFullHttpStatus {
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
