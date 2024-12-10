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
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/entities/schema"
)

func setupDebugHandlers(appState *state.State) {
	logger := appState.Logger.WithField("handler", "debug")

	http.HandleFunc("/debug/index/rebuild/inverted", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		colName := r.URL.Query().Get("collection")
		if colName == "" {
			http.Error(w, "collection is required", http.StatusBadRequest)
			return
		}
		propertyNamesStr := r.URL.Query().Get("propertyNames")
		if propertyNamesStr == "" {
			http.Error(w, "propertyNames is required", http.StatusBadRequest)
			return
		}
		propertyNames := strings.Split(propertyNamesStr, ",")
		if len(propertyNames) == 0 {
			http.Error(w, "propertyNames len > 0 is required", http.StatusBadRequest)
			return
		}
		timeoutStr := r.URL.Query().Get("timeout")
		timeoutDuration := time.Hour
		var err error
		if timeoutStr != "" {
			timeoutDuration, err = time.ParseDuration(timeoutStr)
			if err != nil {
				http.Error(w, "timeout duration has invalid format", http.StatusBadRequest)
				return
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
		defer cancel()

		err = appState.Migrator.InvertedReindex(ctx, map[string]any{
			"ShardInvertedReindexTask_SpecifiedIndex": map[string][]string{colName: propertyNames},
		})
		if err != nil {
			logger.WithError(err).Error("failed to rebuild inverted index")
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))

	// newLogLevel can be one of: panic, fatal, error, warn, info, debug, trace (defaults to info)
	http.HandleFunc("/debug/config/logger/level", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		newLogLevel := r.URL.Query().Get("newLogLevel")
		if newLogLevel == "" {
			http.Error(w, "newLogLevel is required", http.StatusBadRequest)
			return
		}
		level, err := logLevelFromString(newLogLevel)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		appState.Logger.SetLevel(level)
		w.WriteHeader(http.StatusOK)
	}))

	http.HandleFunc("/debug/index/rebuild/vector", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !config.Enabled(os.Getenv("ASYNC_INDEXING")) {
			http.Error(w, "async indexing is not enabled", http.StatusNotImplemented)
			return
		}

		colName := r.URL.Query().Get("collection")
		shardName := r.URL.Query().Get("shard")
		targetVector := r.URL.Query().Get("vector")

		if colName == "" || shardName == "" {
			http.Error(w, "collection and shard are required", http.StatusBadRequest)
			return
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
			if errTxt := err.Error(); strings.Contains(errTxt, "not found") {
				http.Error(w, "shard not found", http.StatusNotFound)
			}

			http.Error(w, "failed to reset vector index", http.StatusInternalServerError)
			return
		}

		logger.WithField("shard", shardName).Info("reindexing started")

		w.WriteHeader(http.StatusAccepted)
	}))

	http.HandleFunc("/debug/index/repair/vector", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !config.Enabled(os.Getenv("ASYNC_INDEXING")) {
			http.Error(w, "async indexing is not enabled", http.StatusNotImplemented)
			return
		}

		colName := r.URL.Query().Get("collection")
		shardName := r.URL.Query().Get("shard")
		targetVector := r.URL.Query().Get("vector")

		if colName == "" || shardName == "" {
			http.Error(w, "collection and shard are required", http.StatusBadRequest)
			return
		}

		idx := appState.DB.GetIndex(schema.ClassName(colName))
		if idx == nil {
			logger.WithField("collection", colName).Error("collection not found")
			http.Error(w, "collection not found", http.StatusNotFound)
			return
		}

		err := idx.DebugRepairIndex(context.Background(), shardName, targetVector)
		if err != nil {
			logger.
				WithField("shard", shardName).
				WithField("targetVector", targetVector).
				WithError(err).
				Error("failed to repair vector index")
			if errTxt := err.Error(); strings.Contains(errTxt, "not found") {
				http.Error(w, "shard not found", http.StatusNotFound)
			}

			http.Error(w, "failed to repair vector index", http.StatusInternalServerError)
			return
		}

		logger.
			WithField("shard", shardName).
			WithField("targetVector", targetVector).
			Info("repair started")

		w.WriteHeader(http.StatusAccepted)
	}))

	http.HandleFunc("/debug/stats/collection/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, "/debug/stats/collection/"))
		parts := strings.Split(path, "/")
		if len(parts) < 3 || len(parts) > 5 || parts[1] != "shards" {
			logger.WithField("parts", parts).Info("invalid path")
			http.Error(w, "invalid path", http.StatusNotFound)
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

		shard, release, err := idx.GetShard(context.Background(), shardName)
		if err != nil {
			logger.WithField("shard", shardName).Error(err)
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		if shard == nil {
			logger.WithField("shard", shardName).Error("shard not found")
			http.Error(w, "shard not found", http.StatusNotFound)
			return
		}
		defer release()

		// Get the vector index
		var vidx db.VectorIndex
		if vecIdxID == "main" {
			vidx = shard.VectorIndex()
		} else {
			vidx = shard.VectorIndexes()[vecIdxID]
		}

		if vidx == nil {
			logger.WithField("shard", shardName).Error("vector index not found")
			http.Error(w, "vector index not found", http.StatusNotFound)
			return
		}

		stats, err := vidx.Stats()
		if err != nil {
			logger.Error(err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		jsonBytes, err := json.Marshal(stats)
		if err != nil {
			logger.WithError(err).Error("marshal failed on stats")
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		logger.Info("Stats on HNSW started")

		w.WriteHeader(http.StatusOK)
		w.Write(jsonBytes)
	}))

	// Call via something like: curl -X GET localhost:6060/debug/config/maintenance_mode (can replace GET w/ POST or DELETE)
	// The port is Weaviate's configured Go profiling port (defaults to 6060)
	http.HandleFunc("/debug/config/maintenance_mode", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var bytesToWrite []byte = nil
		switch r.Method {
		case http.MethodGet:
			jsonBytes, err := json.Marshal(MaintenanceMode{Enabled: appState.Cluster.MaintenanceModeEnabledForLocalhost()})
			if err != nil {
				logger.WithError(err).Error("marshal failed on stats")
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			bytesToWrite = jsonBytes
		case http.MethodPost:
			appState.Cluster.SetMaintenanceModeForLocalhost(true)
		case http.MethodDelete:
			appState.Cluster.SetMaintenanceModeForLocalhost(false)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
		w.WriteHeader(http.StatusOK)
		if bytesToWrite != nil {
			w.Write(bytesToWrite)
		}
	}))
}

type MaintenanceMode struct {
	Enabled bool `json:"enabled"`
}
