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
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/configbase"
)

func setupDebugHandlers(appState *state.State) {
	logger := appState.Logger

	http.HandleFunc("/debug/maintenance", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		maintenanceFilePath := filepath.Join(appState.ServerConfig.Config.Persistence.DataPath, "MAINTENANCE")
		if _, err := os.Stat(maintenanceFilePath); err == nil {
			logger.WithField("file", maintenanceFilePath).Error("maintenance mode already enabled")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		err := os.WriteFile(maintenanceFilePath, []byte(`Delete this file to disable maintenance mode`), 0o600)
		if err != nil {
			logger.WithError(err).Error("failed to enable maintenance mode")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		logger.WithField("file", maintenanceFilePath).Warn("server will go into maintenance mode on next restart. Delete the file to disable maintenance mode")
	}))

	http.HandleFunc("/debug/reindex/collection/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !appState.DB.IsInMaintenanceMode() {
			http.Error(w, "server is not in maintenance mode", http.StatusServiceUnavailable)
			return
		}

		if !configbase.Enabled(os.Getenv("ASYNC_INDEXING")) {
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
		vecIdxID := "main"
		if len(parts) == 4 {
			vecIdxID = parts[3]
		}

		idx := appState.DB.GetIndex(schema.ClassName(colName))
		if idx == nil {
			logger.WithField("collection", colName).Error("collection not found")
			http.Error(w, "collection not found", http.StatusNotFound)
			return
		}

		shard := idx.GetShard(shardName)
		if shard == nil {
			logger.WithField("shard", shardName).Error("shard not found")
			http.Error(w, "shard not found", http.StatusNotFound)
			return
		}

		if size := shard.Queue().Size(); size > 0 {
			logger.WithField("queue_size", size).Warn("queue is not empty")
		}

		q := shard.Queue()
		q.PauseIndexing()

		commitLogPath := fmt.Sprintf("%s/%s/%s/%s.hnsw.commitlog.d", idx.Config.RootPath, idx.ID(), shardName, vecIdxID)

		if _, err := os.Stat(commitLogPath); os.IsNotExist(err) {
			logger.WithField("commitLogPath", commitLogPath).Error("commit log not found")
			http.Error(w, "commit log not found", http.StatusNotFound)
			return
		}

		err := os.RemoveAll(commitLogPath)
		if err != nil {
			logger.WithField("commitLogPath", commitLogPath).WithError(err).Error("failed to remove commit log")
			http.Error(w, "failed to remove commit log", http.StatusInternalServerError)
			return
		}

		err = q.Checkpoints.Update(shard.ID(), "", 0)
		if err != nil {
			logger.WithField("shard", shardName).WithError(err).Error("failed to update checkpoint")
			http.Error(w, "failed to update checkpoint", http.StatusInternalServerError)
			return
		}

		logger.WithField("shard", shardName).Info("index commit log removed successfully. Reindexing will start on next restart.")

		w.WriteHeader(http.StatusAccepted)
	}))
}
