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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/loads"
	"github.com/go-openapi/runtime/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	grpcHandler "github.com/weaviate/weaviate/adapters/handlers/grpc"
	"github.com/weaviate/weaviate/adapters/handlers/grpc/grpcweb"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	entsentry "github.com/weaviate/weaviate/entities/sentry"
	pbv1 "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// One cancel of the shared ctx cuts blocked REST and gRPC handlers together and
// refuses later arrivals on both, so http.Server.Shutdown meets its deadline.
func TestInFlightCancelCutsRESTAndGRPCTogether(t *testing.T) {
	const restRequests = 2
	logger, _ := logrustest.NewNullLogger()
	appState := newMiddlewareTestState(logger)
	callsCtx, cancelCalls := context.WithCancel(context.Background())
	t.Cleanup(cancelCalls)

	restInFlight := newInFlightCancel(callsCtx)
	restEntered := make(chan struct{}, restRequests+1)
	releaseREST := make(chan struct{})
	blocked := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		restEntered <- struct{}{}
		select {
		case <-r.Context().Done():
		case <-releaseREST:
		}
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewUnstartedServer(makeSetupGlobalMiddleware(appState, nil, nil, restInFlight)(blocked))
	makeConfigureServer(appState, restInFlight.requestsCtx)(srv.Config, "http", srv.Listener.Addr().String())
	srv.Start()
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(releaseREST) })

	grpcInFlight := grpcHandler.NewInFlightCancel(callsCtx)
	health := &blockingHealthServer{respectCtx: true, entered: make(chan struct{}, 2), release: make(chan struct{})}
	t.Cleanup(func() { close(health.release) })
	grpcServer := grpc.NewServer(grpc.ChainUnaryInterceptor(grpcInFlight.UnavailableAfterCancel()))
	grpc_health_v1.RegisterHealthServer(grpcServer, health)
	t.Cleanup(grpcServer.Stop)
	healthClient := grpc_health_v1.NewHealthClient(serveBufconn(t, grpcServer))

	type result struct {
		status int
		err    error
	}
	postBatch := func() result {
		resp, err := http.Post(srv.URL+"/v1/batch/objects", "application/json", nil)
		if err != nil {
			return result{err: err}
		}
		resp.Body.Close()
		return result{status: resp.StatusCode}
	}
	checkHealth := func() error {
		_, err := healthClient.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
		return err
	}
	await := func(ch <-chan struct{}, what string) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			require.FailNow(t, "timed out waiting for "+what)
		}
	}

	restResults := make(chan result, restRequests)
	for range restRequests {
		enterrors.GoWrapper(func() { restResults <- postBatch() }, logger)
		await(restEntered, "a REST request to reach its handler")
	}
	grpcResult := make(chan error, 1)
	enterrors.GoWrapper(func() { grpcResult <- checkHealth() }, logger)
	await(health.entered, "the gRPC call to reach its handler")

	cancelCalls()

	for range restRequests {
		select {
		case res := <-restResults:
			require.NoError(t, res.err)
			assert.Equal(t, http.StatusServiceUnavailable, res.status, "REST request in flight at the cancel")
		case <-time.After(5 * time.Second):
			require.FailNow(t, "REST request still blocked after the cancel")
		}
	}
	select {
	case err := <-grpcResult:
		assert.Equal(t, codes.Unavailable, status.Code(err), "gRPC call in flight at the cancel")
	case <-time.After(5 * time.Second):
		require.FailNow(t, "gRPC call still blocked after the cancel")
	}

	res := postBatch()
	require.NoError(t, res.err)
	assert.Equal(t, http.StatusServiceUnavailable, res.status, "REST request arriving after the cancel")
	assert.Equal(t, codes.Unavailable, status.Code(checkHealth()), "gRPC call arriving after the cancel")
	assert.Empty(t, restEntered, "REST request arriving after the cancel reached its handler")
	assert.Empty(t, health.entered, "gRPC call arriving after the cancel reached its handler")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, srv.Config.Shutdown(ctx))
	assert.Equal(t, int64(restRequests+1), restInFlight.unavailableResponses.Load())
	assert.Equal(t, int64(2), grpcInFlight.UnavailableResponses())
}

// newTestInFlightCancel returns an inFlightCancel and the cancel of its requestsCtx.
func newTestInFlightCancel(t *testing.T) (*inFlightCancel, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return newInFlightCancel(ctx), cancel
}

func newMiddlewareTestState(logger *logrus.Logger) *state.State {
	return &state.State{Logger: logger, ServerConfig: &config.WeaviateConfig{
		Config: config.Config{Sentry: &entsentry.ConfigOpts{}},
	}}
}

// Readiness answers 503 from the start of shutdown, while liveness answers 200 throughout.
func TestInFlightCancelProbes(t *testing.T) {
	cases := []struct {
		name       string
		path       string
		prepare    func(c *inFlightCancel, cancel context.CancelFunc)
		wantStatus int
	}{
		{
			name:       "readiness once shutdown started, before the cancel",
			path:       "/v1/.well-known/ready",
			prepare:    func(c *inFlightCancel, _ context.CancelFunc) { c.startShutdown() },
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name:       "liveness after the cancel",
			path:       "/v1/.well-known/live",
			prepare:    func(c *inFlightCancel, cancel context.CancelFunc) { c.startShutdown(); cancel() },
			wantStatus: http.StatusOK,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := logrustest.NewNullLogger()
			inFlight, cancel := newTestInFlightCancel(t)
			tc.prepare(inFlight, cancel)
			handler := makeSetupGlobalMiddleware(newMiddlewareTestState(logger), nil, nil, inFlight)(
				http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
					t.Errorf("probe %s reached the API handler", tc.path)
				}))

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))
			assert.Equal(t, tc.wantStatus, rec.Code)
		})
	}
}

// A 503 after the cancel still carries CORS headers and reaches the batch metrics.
func TestInFlightCancelUnavailablePassesThroughCORSAndMetrics(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	appState := newMiddlewareTestState(logger)
	appState.ServerConfig.Config.CORS = config.CORS{AllowOrigin: "https://example.com", AllowMethods: "POST"}
	appState.ServerConfig.Config.Monitoring.Enabled = true
	appState.Metrics = newBatchMetrics(false)
	appState.HTTPServerMetrics = monitoring.NewHTTPServerMetrics("test", prometheus.NewRegistry())

	spec, err := loads.Embedded(SwaggerJSON, FlatSwaggerJSON)
	require.NoError(t, err)
	api := operations.NewWeaviateAPI(spec)
	api.Init()
	routes := middleware.NewRoutableContext(spec, api, middleware.DefaultRouter(spec, api))

	inFlight, cancel := newTestInFlightCancel(t)
	cancel()
	handler := makeSetupGlobalMiddleware(appState, routes, nil, inFlight)(
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			t.Error("request after the cancel reached the API handler")
		}))

	r := httptest.NewRequest(http.MethodPost, "/v1/batch/objects", strings.NewReader(`{"objects":[]}`))
	r.Header.Set("Origin", "https://example.com")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, r)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, "https://example.com", rec.Header().Get("Access-Control-Allow-Origin"))
	count, _ := batchSizeSamples(t, appState.Metrics, "")
	assert.Equal(t, uint64(1), count, "batch metrics middleware did not observe the 503")
}

// writeHeaderCounter counts WriteHeader calls, which httptest.ResponseRecorder
// silently ignores after the first.
type writeHeaderCounter struct {
	*httptest.ResponseRecorder
	calls int
}

func (c *writeHeaderCounter) WriteHeader(status int) {
	c.calls++
	c.ResponseRecorder.WriteHeader(status)
}

// A handler panicking after the cancel still answers 503 and is counted.
func TestInFlightCancelPanicAfterCancel(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	inFlight, cancel := newTestInFlightCancel(t)
	handler := makeSetupGlobalMiddleware(newMiddlewareTestState(logger), nil, nil, inFlight)(
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			cancel()
			panic(errors.New("handler failed"))
		}))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/batch/objects", nil))

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, int64(1), inFlight.unavailableResponses.Load())
}

// blockingSearch holds every Search call until its ctx is cancelled.
type blockingSearch struct {
	pbv1.UnimplementedWeaviateServer
	entered chan struct{}
}

func (b *blockingSearch) Search(ctx context.Context, _ *pbv1.SearchRequest) (*pbv1.SearchReply, error) {
	b.entered <- struct{}{}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(5 * time.Second):
		return &pbv1.SearchReply{}, nil
	}
}

// From the shutdown cancel, or from the moment the gRPC server begins stopping,
// grpc-web gets Grpc-Status 14 and Connect a JSON 503, both with CORS headers.
// Each refusal counts on the grpc-web counter, never the REST one.
func TestInFlightCancelGrpcWebRefusal(t *testing.T) {
	const (
		origin      = "https://example.com"
		grpcWebType = "application/grpc-web+proto"
	)
	triggers := []struct {
		name  string
		start func(cancelCalls func(), startStop func())
	}{
		{name: "shutdown cancel", start: func(cancelCalls, _ func()) { cancelCalls() }},
		{name: "gRPC stop starting", start: func(_, startStop func()) { startStop() }},
	}

	for _, trigger := range triggers {
		t.Run(trigger.name, func(t *testing.T) {
			logger, _ := logrustest.NewNullLogger()
			appState := newMiddlewareTestState(logger)
			appState.ServerConfig.Config.CORS = config.CORS{AllowOrigin: origin}
			grpcServer := grpc.NewServer()
			t.Cleanup(grpcServer.Stop)
			search := &blockingSearch{entered: make(chan struct{}, 1)}
			pbv1.RegisterWeaviateServer(grpcServer, search)
			restInFlight, cancelCalls := newTestInFlightCancel(t)
			grpcWebCtx, refuseGrpcWeb := context.WithCancel(restInFlight.requestsCtx)
			t.Cleanup(refuseGrpcWeb)
			grpcWebInFlight := newInFlightCancel(grpcWebCtx)
			grpcWebHandler, err := grpcweb.NewHandler(grpcServer, appState, grpcWebInFlight.unavailableAfterCancel)
			require.NoError(t, err)
			restHandler := makeSetupGlobalMiddleware(appState, nil, nil, restInFlight)(
				http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
					t.Error("grpc-web call reached the REST handler")
				}))
			handler := grpcweb.Mount("/v1/grpc-web", grpcWebHandler, restHandler, func() bool { return true })

			serve := func(method string, body []byte, header http.Header) *httptest.ResponseRecorder {
				r := httptest.NewRequest(method, "/v1/grpc-web"+pbv1.Weaviate_Search_FullMethodName, bytes.NewReader(body))
				r.Header.Set("Origin", origin)
				for k, v := range header {
					r.Header[k] = v
				}
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, r.WithContext(restInFlight.requestsCtx))
				return rec
			}
			// An empty grpc-web data frame is a flag byte followed by a zero length.
			grpcWebCall := func() *httptest.ResponseRecorder {
				return serve(http.MethodPost, make([]byte, 5), http.Header{"Content-Type": {grpcWebType}})
			}

			inFlightDone := make(chan *httptest.ResponseRecorder, 1)
			enterrors.GoWrapper(func() { inFlightDone <- grpcWebCall() }, logger)
			select {
			case <-search.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("grpc-web call never reached Search")
			}
			trigger.start(cancelCalls, func() {
				t.Cleanup(startGrpcStop(grpcServer, refuseGrpcWeb, time.Hour, logger))
			})

			connect := http.Header{"Content-Type": {"application/proto"}, "Connect-Protocol-Version": {"1"}}
			preflight := http.Header{"Access-Control-Request-Method": {http.MethodPost}}
			cases := []struct {
				name            string
				rec             *httptest.ResponseRecorder
				wantStatus      int
				wantContentType string
				wantGrpcStatus  string
				wantEmptyBody   bool
			}{
				{
					name: "grpc-web in flight at the trigger", rec: <-inFlightDone,
					wantStatus: http.StatusOK, wantContentType: grpcWebType, wantGrpcStatus: "14", wantEmptyBody: true,
				},
				{
					name: "grpc-web arriving after the trigger", rec: grpcWebCall(),
					wantStatus: http.StatusOK, wantContentType: grpcWebType, wantGrpcStatus: "14", wantEmptyBody: true,
				},
				{
					name: "connect arriving after the trigger", rec: serve(http.MethodPost, nil, connect),
					wantStatus: http.StatusServiceUnavailable, wantContentType: "application/json",
				},
				{
					name: "preflight after the trigger", rec: serve(http.MethodOptions, nil, preflight),
					wantStatus: http.StatusOK, wantEmptyBody: true,
				},
			}
			for _, tc := range cases {
				assert.Equal(t, tc.wantStatus, tc.rec.Code, tc.name)
				assert.Equal(t, tc.wantContentType, tc.rec.Header().Get("Content-Type"), tc.name)
				assert.Equal(t, tc.wantGrpcStatus, tc.rec.Header().Get("Grpc-Status"), tc.name)
				assert.Equal(t, tc.wantEmptyBody, tc.rec.Body.Len() == 0, tc.name)
				assert.Equal(t, origin, tc.rec.Header().Get("Access-Control-Allow-Origin"), tc.name)
			}
			assert.Equal(t, int64(3), grpcWebInFlight.unavailableResponses.Load(), "grpc-web refusals")
			assert.Equal(t, int64(0), restInFlight.unavailableResponses.Load(), "REST refusals")
		})
	}
}

func TestInFlightCancelMiddleware(t *testing.T) {
	cases := []struct {
		name                string
		cancelBeforeArrival bool
		handler             func(w http.ResponseWriter, cancel func())
		wantStatus          int
		wantBody            string
		wantUnavailable     int64
		wantFlushed         bool
		wantHandlerSkipped  bool
	}{
		{
			name:                "request arriving after cancel gets 503 without running its handler",
			cancelBeforeArrival: true,
			handler: func(w http.ResponseWriter, _ func()) {
				w.Write([]byte("ran"))
			},
			wantStatus:         http.StatusServiceUnavailable,
			wantUnavailable:    1,
			wantHandlerSkipped: true,
		},
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
			inFlight, cancel := newTestInFlightCancel(t)
			if tc.cancelBeforeArrival {
				cancel()
			}
			handlerRan := false
			rec := httptest.NewRecorder()
			counted := &writeHeaderCounter{ResponseRecorder: rec}
			inFlight.unavailableAfterCancel(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				handlerRan = true
				tc.handler(w, cancel)
			})).ServeHTTP(counted, httptest.NewRequest(http.MethodPost, "/v1/batch/objects", nil))

			assert.Equal(t, !tc.wantHandlerSkipped, handlerRan, "handler ran")
			assert.LessOrEqual(t, counted.calls, 1, "WriteHeader reached the underlying writer more than once")
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
