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
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	entsentry "github.com/weaviate/weaviate/entities/sentry"
	"github.com/weaviate/weaviate/usecases/config"
)

// Handlers blocked on their ctx must not hold http.Server.Shutdown past its deadline.
func TestInFlightCancelUnblocksServerShutdown(t *testing.T) {
	const requests = 2
	logger, _ := logrustest.NewNullLogger()
	appState := &state.State{Logger: logger, ServerConfig: &config.WeaviateConfig{
		Config: config.Config{Sentry: &entsentry.ConfigOpts{}},
	}}
	inFlight := newInFlightCancel(50 * time.Millisecond)
	entered := make(chan struct{}, requests)
	blocked := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
		w.WriteHeader(http.StatusOK)
	})

	srv := httptest.NewUnstartedServer(makeSetupGlobalMiddleware(appState, nil, nil, inFlight)(blocked))
	makeConfigureServer(appState, inFlight.requestsCtx)(srv.Config, "http", srv.Listener.Addr().String())
	srv.Start()
	t.Cleanup(srv.Close)

	type result struct {
		status int
		err    error
	}
	results := make(chan result, requests)
	for range requests {
		enterrors.GoWrapper(func() {
			resp, err := http.Post(srv.URL+"/v1/batch/objects", "application/json", nil)
			if err != nil {
				results <- result{err: err}
				return
			}
			resp.Body.Close()
			results <- result{status: resp.StatusCode}
		}, logger)
	}
	for range requests {
		<-entered
	}

	inFlight.cancelRequestsAfterDelay()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, srv.Config.Shutdown(ctx))

	for range requests {
		res := <-results
		require.NoError(t, res.err)
		assert.Equal(t, http.StatusServiceUnavailable, res.status)
	}
	assert.Equal(t, int64(requests), inFlight.unavailableResponses.Load())
}

func TestInFlightCancelMiddleware(t *testing.T) {
	cases := []struct {
		name            string
		handler         func(w http.ResponseWriter, cancel func())
		wantStatus      int
		wantBody        string
		wantUnavailable int64
		wantFlushed     bool
	}{
		{
			name: "response finishing before cancel passes through",
			handler: func(w http.ResponseWriter, _ func()) {
				w.WriteHeader(http.StatusCreated)
				w.Write([]byte("created"))
			},
			wantStatus: http.StatusCreated,
			wantBody:   "created",
		},
		{
			name: "response starting after cancel becomes 503",
			handler: func(w http.ResponseWriter, cancel func()) {
				w.Header().Set("Content-Length", "13")
				cancel()
				w.WriteHeader(http.StatusOK)
				_, err := w.Write([]byte("partial batch"))
				assert.NoError(t, err, "go-swagger responders panic on a write error")
			},
			wantStatus:      http.StatusServiceUnavailable,
			wantUnavailable: 1,
		},
		{
			name: "handler writing nothing after cancel gets 503",
			handler: func(_ http.ResponseWriter, cancel func()) {
				cancel()
			},
			wantStatus:      http.StatusServiceUnavailable,
			wantUnavailable: 1,
		},
		{
			name: "response started before cancel is untouched",
			handler: func(w http.ResponseWriter, cancel func()) {
				w.Write([]byte("a"))
				cancel()
				w.Write([]byte("b"))
			},
			wantStatus: http.StatusOK,
			wantBody:   "ab",
		},
		{
			name: "flush reaches the underlying writer",
			handler: func(w http.ResponseWriter, _ func()) {
				w.Write([]byte("chunk"))
				f, ok := w.(http.Flusher)
				require.True(t, ok, "wrapped writer must implement http.Flusher")
				f.Flush()
			},
			wantStatus:  http.StatusOK,
			wantBody:    "chunk",
			wantFlushed: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inFlight := newInFlightCancel(time.Hour)
			t.Cleanup(inFlight.cancelRequests)
			rec := httptest.NewRecorder()
			inFlight.unavailableAfterCancel(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				tc.handler(w, inFlight.cancelRequests)
			})).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/batch/objects", nil))

			assert.Equal(t, tc.wantStatus, rec.Code)
			assert.Equal(t, tc.wantUnavailable, inFlight.unavailableResponses.Load())
			assert.Equal(t, tc.wantFlushed, rec.Flushed)
			body, err := io.ReadAll(rec.Body)
			require.NoError(t, err)
			if tc.wantStatus != http.StatusServiceUnavailable {
				assert.Equal(t, tc.wantBody, string(body))
				return
			}
			assert.Empty(t, rec.Header().Get("Content-Length"), "handler-declared length leaked onto the 503")
			var errResp models.ErrorResponse
			require.NoError(t, json.Unmarshal(body, &errResp), "body: %s", body)
			require.Len(t, errResp.Error, 1)
			assert.Equal(t, errServerShuttingDown.Error(), errResp.Error[0].Message)
		})
	}
}
