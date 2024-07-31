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

package rest

import (
	"context"
	"net/http"
	"os"
	"strings"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/entities/schema"
)

func setupDebugHandlers(appState *state.State) {
	logger := appState.Logger.WithField("handler", "debug")

	http.HandleFunc("/debug/reindex/collection/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !config.Enabled(os.Getenv("ASYNC_INDEXING")) {
			http.Error(w, "async indexing is not enabled", http.StatusNotImplemented)
			return
		}

		path := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, "/debug/reindex/collection/"))
		parts := strings.Split(path, "/")
		if len(parts) < 3 || len(parts) > 5 || parts[1] != "shards" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		colName, shardName := parts[0], parts[2]
		targetVector := ""
		if len(parts) == 4 {
			targetVector = parts[3]
		}

		idx := appState.DB.GetIndex(schema.ClassName(colName))
		if idx == nil {
			logger.WithField("collection", colName).Error("collection not found")
			http.Error(w, "collection not found", http.StatusNotFound)
			return
		}

		err := idx.DebugResetVectorIndex(context.Background(), shardName, targetVector)
		if err != nil {
			logger.
				WithField("shard", shardName).
				WithField("targetVector", targetVector).
				WithError(err).
				Error("failed to reset vector index")
			return
		}

		logger.WithField("shard", shardName).Info("reindexing started")

		w.WriteHeader(http.StatusAccepted)
	}))
}
