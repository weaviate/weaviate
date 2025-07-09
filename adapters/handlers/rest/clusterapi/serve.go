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

package clusterapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	sentryhttp "github.com/getsentry/sentry-go/http"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/handlers/rest/types"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// Server represents the cluster API server
type Server struct {
	server   *http.Server
	appState *state.State
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
	replicatedIndices := NewReplicatedIndices(appState.RemoteReplicaIncoming, appState.Scaler, auth, appState.Cluster.MaintenanceModeEnabledForLocalhost)
	classifications := NewClassifications(appState.ClassificationRepo.TxManager(), auth)
	nodes := NewNodes(appState.RemoteNodeIncoming, auth)
	backups := NewBackups(appState.BackupManager, auth)
	dbUsers := NewDbUsers(appState.APIKeyRemote, auth)

	mux := http.NewServeMux()
	mux.Handle("/classifications/transactions/",
		http.StripPrefix("/classifications/transactions/",
			classifications.Transactions()))

	mux.Handle("/cluster/users/db/", dbUsers.Users())
	mux.Handle("/nodes/", nodes.Nodes())
	mux.Handle("/indices/", indices.Indices())
	mux.Handle("/replicas/indices/", replicatedIndices.Indices())

	mux.Handle("/backups/can-commit", backups.CanCommit())
	mux.Handle("/backups/commit", backups.Commit())
	mux.Handle("/backups/abort", backups.Abort())
	mux.Handle("/backups/status", backups.Status())

	mux.Handle("/", index())

	var handler http.Handler
	handler = mux
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

	return &Server{
		server: &http.Server{
			Addr:    fmt.Sprintf(":%d", port),
			Handler: handler,
		},
		appState: appState,
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

	if err := s.server.Shutdown(ctx); err != nil {
		s.appState.Logger.WithField("action", "cluster_api_shutdown").
			WithError(err).
			Error("could not stop server gracefully")
		return s.server.Close()
	}
	return nil
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
