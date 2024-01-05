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

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
)

func Serve(appState *state.State) {
	port := appState.ServerConfig.Config.Cluster.DataBindPort
	auth := NewBasicAuthHandler(appState.ServerConfig.Config.Cluster.AuthConfig)

	appState.Logger.WithField("port", port).
		WithField("action", "cluster_api_startup").
		Debugf("serving cluster api on port %d", port)

	schema := NewSchema(appState.SchemaManager.TxManager(), auth)
	indices := NewIndices(appState.RemoteIndexIncoming, appState.DB, auth)
	replicatedIndices := NewReplicatedIndices(appState.RemoteReplicaIncoming, appState.Scaler, auth)
	classifications := NewClassifications(appState.ClassificationRepo.TxManager(), auth)
	nodes := NewNodes(appState.RemoteNodeIncoming, auth)
	backups := NewBackups(appState.BackupManager, auth)

	mux := http.NewServeMux()
	mux.Handle("/schema/transactions/",
		http.StripPrefix("/schema/transactions/", schema.Transactions()))
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
	http.ListenAndServe(fmt.Sprintf(":%d", port), mux)
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
