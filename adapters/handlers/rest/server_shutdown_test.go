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

package rest

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
)

// A request still running when GracefulTimeout expires must not stop
// handleShutdown from calling ServerShutdown, which leaves the cluster and
// closes the database.
func TestHandleShutdownCallsServerShutdown(t *testing.T) {
	tests := []struct {
		name string
		// whether each HTTP server holds a request past GracefulTimeout
		requestOutlivesDrain []bool
	}{
		{name: "one server, drained in time", requestOutlivesDrain: []bool{false}},
		{name: "one server, request outlives the drain", requestOutlivesDrain: []bool{true}},
		{name: "two servers, one request outlives the drain", requestOutlivesDrain: []bool{false, true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var serverShutdownCalls atomic.Int32
			s := NewServer(&operations.WeaviateAPI{
				PreServerShutdown: func() {},
				ServerShutdown:    func() { serverShutdownCalls.Add(1) },
			})
			s.GracefulTimeout = 50 * time.Millisecond

			var servers []*http.Server
			for _, outlives := range tt.requestOutlivesDrain {
				servers = append(servers, startServer(t, outlives))
			}

			require.NoError(t, s.Shutdown())
			wg := new(sync.WaitGroup)
			wg.Add(1)
			s.handleShutdown(wg, &servers)

			require.Equal(t, int32(1), serverShutdownCalls.Load())
		})
	}
}

// startServer serves one HTTP server. With holdRequest, it returns once a
// request is in flight that runs until the test ends.
func startServer(t *testing.T, holdRequest bool) *http.Server {
	t.Helper()
	release := make(chan struct{})
	entered := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(entered)
		<-release
	}))
	t.Cleanup(func() {
		close(release)
		ts.Close()
	})
	if !holdRequest {
		return ts.Config
	}

	go func() {
		resp, err := ts.Client().Get(ts.URL)
		if err == nil {
			resp.Body.Close()
		}
	}()
	<-entered
	return ts.Config
}
