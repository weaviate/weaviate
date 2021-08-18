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

	schema := newSchema(nil)

	mux := http.NewServeMux()
	mux.Handle("/", schema.index())
	http.ListenAndServe(fmt.Sprintf(":%d", port), mux)
}
