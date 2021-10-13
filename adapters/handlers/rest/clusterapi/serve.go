//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package clusterapi

import (
	"fmt"
	"net/http"

	"github.com/semi-technologies/weaviate/adapters/handlers/rest/state"
)

func Serve(appState *state.State) {
	port := appState.ServerConfig.Config.Cluster.DataBindPort
	if port <= 0 {
		port = 7946
	}

	appState.Logger.WithField("port", port).
		WithField("action", "cluster_api_startup").
		Debugf("serving cluster api on port %d", port)

	schema := NewSchema(appState.SchemaManager.TxManager())
	indices := NewIndices(appState.RemoteIncoming)
	classifications := NewClassifications(appState.ClassificationRepo.TxManager())

	mux := http.NewServeMux()
	mux.Handle("/schema/transactions/",
		http.StripPrefix("/schema/transactions/", schema.Transactions()))
	mux.Handle("/classifications/transactions/",
		http.StripPrefix("/classifications/transactions/",
			classifications.Transactions()))

	mux.Handle("/indices/", indices.Indices())
	mux.Handle("/", schema.index())
	http.ListenAndServe(fmt.Sprintf(":%d", port), mux)
}
