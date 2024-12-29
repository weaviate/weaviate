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
	"encoding/json"
	"fmt"
	"net/http"

	sentryhttp "github.com/getsentry/sentry-go/http"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
)

func Serve(appState *state.State) {
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

	mux := http.NewServeMux()
	mux.Handle("/classifications/transactions/",
		http.StripPrefix("/classifications/transactions/",
			classifications.Transactions()))

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
	http.ListenAndServe(fmt.Sprintf(":%d", port), handler)
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
