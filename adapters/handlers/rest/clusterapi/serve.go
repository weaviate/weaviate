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

package clusterapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	sentryhttp "github.com/getsentry/sentry-go/http"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc"
	"github.com/weaviate/weaviate/adapters/handlers/rest/raft"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/handlers/rest/types"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

const (
	MAX_CONCURRENT_STREAMS = 250
	MAX_READ_FRAME_SIZE    = (16 * 1024 * 1024) // 16 MB
)

// Server represents the cluster API server
type Server struct {
	server            *http.Server
	appState          *state.State
	replicatedIndices *replicatedIndices
	grpc              *grpc.Server
}

// Ensure Server implements interfaces.ClusterServer
var _ types.ClusterServer = (*Server)(nil)

// NewServer creates a new cluster API server instance
func NewServer(appState *state.State) *Server {
	port := appState.ServerConfig.Config.Cluster.DataBindPort
	auth := NewBasicAuthHandler(appState.ServerConfig.Config.Cluster.AuthConfig)

	appState.Logger.WithField("port", port).
		WithField("action", "cluster_api_startup").
		Debugf("serving cluster api on port %d", port)

	indices := NewIndices(appState.RemoteIndexIncoming, appState.DB, auth, appState.Cluster.MaintenanceModeEnabledForLocalhost, appState.Logger)
	replicatedIndices := NewReplicatedIndices(
		appState.DB,
		auth,
		appState.Cluster.MaintenanceModeEnabledForLocalhost,
		appState.ServerConfig.Config.Cluster.RequestQueueConfig,
		appState.Logger,
		appState.ClusterService.Ready)

	classifications := NewClassifications(appState.ClassificationRepo.TxManager(), auth)
	nodes := NewNodes(appState.RemoteNodeIncoming, auth)
	backups := NewBackups(appState.BackupManager, auth)
	dbUsers := NewDbUsers(appState.APIKeyRemote, auth)
	objectTTL := NewObjectTTL(appState.RemoteIndexIncoming, auth, appState.Logger)

	mux := http.NewServeMux()
	mux.Handle("/classifications/transactions/",
		http.StripPrefix("/classifications/transactions/",
			classifications.Transactions()))

	mux.Handle("/cluster/users/db/", dbUsers.Users())
	mux.Handle("/cluster/object_ttl/", objectTTL.Expired())
	mux.Handle("/nodes/", monitoring.AddTracingToHTTPMiddleware(nodes.Nodes(), appState.Logger))
	mux.Handle("/indices/", monitoring.AddTracingToHTTPMiddleware(indices.Indices(), appState.Logger))
	mux.Handle("/replicas/indices/", monitoring.AddTracingToHTTPMiddleware(replicatedIndices.Indices(), appState.Logger))

	mux.Handle("/backups/can-commit", backups.CanCommit())
	mux.Handle("/backups/commit", backups.Commit())
	mux.Handle("/backups/abort", backups.Abort())
	mux.Handle("/backups/status", backups.Status())

	mux.Handle("/", index())

	grpcServer := grpc.NewServer(appState)

	var handler http.Handler
	// Multiplexing handler: Routes gRPC vs. REST (HTTP)
	handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("content-type"), "application/grpc") {
			grpcServer.ServeHTTP(w, r) // Route to gRPC
			return
		}
		mux.ServeHTTP(w, r) // Route to REST mux (handles HTTP/1.1 or plain HTTP/2)
	})

	handler = addClusterHandlerMiddleware(handler, appState)
	if appState.ServerConfig.Config.Sentry.Enabled {
		// Wrap the default mux with Sentry to capture panics, report errors and
		// measure performance.
		//
		// Alternatively, you can also wrap individual handlers if you need to
		// use different options for different parts of your app.
		handler = sentryhttp.New(sentryhttp.Options{}).Handle(mux)
	}

	if appState.ServerConfig.Config.Monitoring.Enabled {
		handler = monitoring.InstrumentHTTP(
			handler,
			staticRoute(mux),
			appState.HTTPServerMetrics.InflightRequests,
			appState.HTTPServerMetrics.RequestDuration,
			appState.HTTPServerMetrics.RequestBodySize,
			appState.HTTPServerMetrics.ResponseBodySize,
		)
	}

	protocols := http.Protocols{}
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)

	return &Server{
		server: &http.Server{
			Addr:      fmt.Sprintf(":%d", port),
			Handler:   handler,
			Protocols: &protocols,
		},
		appState:          appState,
		replicatedIndices: replicatedIndices,
		grpc:              grpcServer,
	}
}

// Serve starts the server and blocks until an error occurs
func (s *Server) Serve() error {
	s.appState.Logger.WithField("action", "cluster_api_startup").
		Infof("cluster api server is ready to handle requests on %s", s.server.Addr)
	return s.server.ListenAndServe()
}

// Close gracefully shuts down the server
func (s *Server) Close(ctx context.Context) error {
	s.appState.Logger.WithField("action", "cluster_api_shutdown").
		Info("server is shutting down")

	// Close the replicatedIndices first to drain the queue and wait for workers
	// This ensures all pending replication requests are processed before stopping the server
	if s.replicatedIndices != nil {
		s.appState.Logger.WithField("action", "cluster_api_shutdown").
			Info("shutting down replicated indices")
		if err := s.replicatedIndices.Close(ctx); err != nil {
			s.appState.Logger.WithField("action", "cluster_api_shutdown").
				WithError(err).
				Warn("error shutting down replicated indices")
		}
	}

	// Now shutdown the servers after the replicated indices have been closed
	eg := enterrors.NewErrorGroupWrapper(s.appState.Logger)
	eg.Go(func() error {
		if err := s.server.Shutdown(ctx); err != nil {
			s.appState.Logger.WithField("action", "cluster_api_shutdown").
				WithError(err).
				Error("could not stop server gracefully")
			return s.server.Close()
		}
		return nil
	})
	eg.Go(func() error {
		if err := s.grpc.Close(ctx); err != nil {
			s.appState.Logger.WithField("action", "cluster_api_shutdown").
				WithError(err).
				Error("could not stop grpc server gracefully")
			return err
		}
		return nil
	})

	return eg.Wait()
}

// Serve is kept for backward compatibility
func Serve(appState *state.State) (*Server, error) {
	server := NewServer(appState)
	if err := server.Serve(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		appState.Logger.WithField("action", "cluster_api_shutdown").
			WithError(err).
			Error("server error")
	}
	return server, nil
}

func index() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.String() != "" && r.URL.String() != "/" {
			http.NotFound(w, r)
			return
		}

		payload := map[string]string{
			"description": "Weaviate's cluster-internal API for cross-node communication",
		}

		json.NewEncoder(w).Encode(payload)
	})
}

// staticRoute is used to convert routes in our internal http server into static routes
// by removing all the dynamic variables in the route. Useful for instrumentation
// where "route cardinality" matters.

// Example: `/replicas/indices/Movies/shards/hello0/objects` -> `/replicas/indices`
func staticRoute(mux *http.ServeMux) monitoring.StaticRouteLabel {
	return func(r *http.Request) (*http.Request, string) {
		route := r.URL.String()

		_, pattern := mux.Handler(r)
		if pattern != "" {
			route = pattern
		}
		return r, route
	}
}

// clusterv1Regexp is used to intercept requests and redirect them to a dedicated http server independent of swagger
var clusterv1Regexp = regexp.MustCompile("/v1/cluster/*")

// addClusterHandlerMiddleware will inject a middleware that will catch all requests matching clusterv1Regexp.
// If the request match, it will route it to a dedicated http.Handler and skip the next middleware.
// If the request doesn't match, it will continue to the next handler.
func addClusterHandlerMiddleware(next http.Handler, appState *state.State) http.Handler {
	// Instantiate the router outside the returned lambda to avoid re-allocating everytime a new request comes in
	raftRouter := raft.ClusterRouter(appState.SchemaManager.Handler)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case clusterv1Regexp.MatchString(r.URL.Path):
			raftRouter.ServeHTTP(w, r)
		default:
			next.ServeHTTP(w, r)
		}
	})
}
