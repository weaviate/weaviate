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

package grpcweb

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestTracker(t *testing.T) {
	tests := []struct {
		name     string
		inFlight int  // requests held inside the wrapped handler when Drain starts
		stop     bool // close Drain's stop channel instead of releasing the requests
		want     bool
	}{
		// Drain is parked in its select before the 8 requests are released, so
		// this row does not race Drain's setup against leave. Under -race it
		// covers the 8 concurrent leave calls and the last one's close of zero.
		{name: "drain waits for an in-flight request", inFlight: 8, want: true},
		{name: "stop releases drain", inFlight: 1, stop: true, want: false},
		{name: "idle drain returns at once", inFlight: 0, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var tracker Tracker
			entered := make(chan struct{}, tt.inFlight)
			release := make(chan struct{})
			handler := tracker.wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				entered <- struct{}{}
				<-release
				w.WriteHeader(http.StatusOK)
			}))

			var served sync.WaitGroup
			codes := make(chan int, tt.inFlight)
			for range tt.inFlight {
				served.Add(1)
				go func() {
					defer served.Done()
					rec := httptest.NewRecorder()
					handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", nil))
					codes <- rec.Code
				}()
			}
			for range tt.inFlight {
				<-entered
			}

			stop := make(chan struct{})
			drained := make(chan bool, 1)
			go func() { drained <- tracker.Drain(stop) }()

			if tt.inFlight > 0 {
				select {
				case <-drained:
					t.Fatal("Drain returned while requests were in flight")
				case <-time.After(50 * time.Millisecond):
				}
				if tt.stop {
					close(stop)
				} else {
					close(release)
				}
			}

			select {
			case got := <-drained:
				require.Equal(t, tt.want, got)
			case <-time.After(5 * time.Second):
				t.Fatal("Drain did not return")
			}

			if tt.stop {
				close(release)
			}
			served.Wait()
			close(codes)
			for code := range codes {
				require.Equal(t, http.StatusOK, code, "admitted request must be served")
			}
		})
	}

	t.Run("idle drain with stop already closed reports drained", func(t *testing.T) {
		// Both channels are ready the moment Drain selects. Without the
		// state recheck on the stop branch a bare select picks at random and
		// can report an idle tracker as not drained. 100 iterations make a
		// regression to the random select fail with near-certainty.
		stop := make(chan struct{})
		close(stop)
		for range 100 {
			var tracker Tracker
			require.True(t, tracker.Drain(stop))
		}
	})

	t.Run("refused request gets a CORS-readable 503", func(t *testing.T) {
		var tracker Tracker
		require.True(t, tracker.Drain(nil))
		handler, err := NewHandler(grpc.NewServer(), &state.State{
			ServerConfig: &config.WeaviateConfig{Config: config.Config{
				CORS: config.CORS{
					AllowOrigin:  config.DefaultCORSAllowOrigin,
					AllowMethods: config.DefaultCORSAllowMethods,
					AllowHeaders: config.DefaultCORSAllowHeaders,
				},
			}},
		}, &tracker)
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodPost, "/weaviate.v1.Weaviate/Search", nil)
		req.Header.Set("Origin", "http://localhost:3000")
		req.Header.Set("Content-Type", "application/grpc-web+proto")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusServiceUnavailable, rec.Code)
		require.NotEmpty(t, rec.Header().Get("Access-Control-Allow-Origin"))
		require.Contains(t, rec.Header().Get("Access-Control-Expose-Headers"), "Grpc-Status")
	})
}
