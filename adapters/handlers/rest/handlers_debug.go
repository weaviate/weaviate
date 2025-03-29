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
	"fmt"
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

	http.HandleFunc("/debug/index/rebuild/inverted/abort", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		appState.ReindexCtxCancel(fmt.Errorf("abort endpoint"))
		w.WriteHeader(http.StatusAccepted)
	}))

	http.HandleFunc("/debug/index/rebuild/inverted/status", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		colName := r.URL.Query().Get("collection")
		rootPath := appState.DB.GetConfig().RootPath

		if colName == "" {
			http.Error(w, "collection and shard are required", http.StatusBadRequest)
			return
		}
		className := schema.ClassName(colName)

		classNameString := strings.ToLower(className.String())
		idx := appState.DB.GetIndex(className)
		if idx == nil {
			logger.WithField("collection", colName).Error("collection not found or not ready")
			http.Error(w, "collection not found or not ready", http.StatusNotFound)
			return
		}

		// get all shards

		paths := []string{}
		err := idx.ForEachLoadedShard(
			func(shardName string, shard db.ShardLike) error {
				shardPath := rootPath + "/" + classNameString + "/" + shardName + "/lsm/"
				paths = append(paths, shardPath)
				return nil
			},
		)
		if err != nil {
			logger.WithField("collection", colName).Error("failed to get shard names")
			http.Error(w, "failed to get shard names", http.StatusInternalServerError)
			return
		}

		if len(paths) == 0 {
			http.Error(w, "no shards found", http.StatusNotFound)
			return
		}

		output := make([]map[string]string, len(paths))

		for i, path := range paths {
			output[i] = map[string]string{
				"shard":  path,
				"status": "unknown",
			}
			// check if a .migrations/searchable_map_to_blockmax exists
			_, err := os.Stat(path + ".migrations/searchable_map_to_blockmax")
			if err != nil {
				output[i]["status"] = "not_started"
				output[i]["message"] = "no searchable_map_to_blockmax found"
				continue
			}

			path += ".migrations/searchable_map_to_blockmax/"
			// check if started.mig exists
			_, err = os.Stat(path + "started.mig")
			if err != nil {
				output[i]["status"] = "not_started"
				output[i]["message"] = "no started.mig found"
				continue
			}
			// load the started.mig file and add it's value to the output
			startedFile, err := os.ReadFile(path + "started.mig")
			if err != nil {
				output[i]["status"] = "error"
				output[i]["message"] = "failed to read started.mig"
				continue
			}
			output[i]["status"] = "started"
			output[i]["start_time"] = string(startedFile)

			// check if the properties.mig file exists
			_, err = os.Stat(path + "properties.mig")
			if err != nil {
				output[i]["message"] = "computing properties to reindex"
				continue
			}

			// load the properties.mig file and add it's value to the output
			propertiesFile, err := os.ReadFile(path + "properties.mig")
			if err != nil {
				output[i]["status"] = "error"
				output[i]["message"] = "failed to read properties.mig"
				continue
			}

			output[i]["properties"] = string(propertiesFile)

			// count the progress.mig.* files in the shard directory
			files, err := os.ReadDir(path)
			if err != nil {
				output[i]["status"] = "error"
				output[i]["message"] = "failed to read shard directory"
				continue
			}
			progressCount := 0

			// files sorted by filename

			for _, file := range files {
				if strings.HasPrefix(file.Name(), "progress.mig.") {
					progressCount++
				}
			}
			if progressCount == 0 {
				output[i]["message"] = "no progress.mig.* files found, no snapshots created yet"
				continue
			}

			// load latest progress.mig.* file and add it's value to the output
			progressFile, err := os.ReadFile(path + fmt.Sprintf("progress.mig.%09d", progressCount))
			if err != nil {
				output[i]["status"] = "error"
				output[i]["message"] = fmt.Sprintf("failed to read progress.mig.%09d", progressCount)
				continue
			}

			output[i]["latest_snapshot"] = strings.Join(strings.Split(string(progressFile), "\n"), ", ")

			output[i]["status"] = "in progress"
			output[i]["snapshot_count"] = fmt.Sprintf("%d", progressCount)

			// check if reindexed.mig exists
			_, err = os.Stat(path + "reindexed.mig")
			if err == nil {
				output[i]["status"] = "reindexed"
				output[i]["message"] = "reindexing done"
			}

			// load the reindexed.mig file and add it's value to the output
			reindexedFile, err := os.ReadFile(path + "reindexed.mig")
			if err != nil {
				output[i]["status"] = "error"
				output[i]["message"] = "failed to read reindexed.mig"
				continue
			}
			output[i]["reindexed"] = strings.Join(strings.Split(string(reindexedFile), "\n"), ", ")

			// check if merged.mig exists
			_, err = os.Stat(path + "merged.mig")
			if err != nil {
				output[i]["message"] = "reindexing done, merging buckets"
				continue
			}

			output[i]["status"] = "merged"
			output[i]["message"] = "merged reindex and ingest buckets"

			// load the merged.mig file and add it's value to the output
			mergedFile, err := os.ReadFile(path + "merged.mig")
			if err != nil {
				output[i]["status"] = "error"
				output[i]["message"] = "failed to read merged.mig"
				continue
			}
			output[i]["merged"] = string(mergedFile)

			// check if swapped.mig.* exists
			swappedFiles, err := os.ReadDir(path)
			if err != nil {
				output[i]["status"] = "error"
				output[i]["message"] = "failed to read shard directory"
				continue
			}
			swappedCount := 0
			for _, file := range swappedFiles {
				if strings.HasPrefix(file.Name(), "swapped.mig.") {
					swappedCount++
				}
			}

			if swappedCount > 0 {
				output[i]["status"] = "swapped"
				output[i]["message"] = fmt.Sprintf("swapped %d files", swappedCount)
			}

			// check if swapped.mig exists
			_, err = os.Stat(path + "swapped.mig")
			if err != nil {
				output[i]["message"] = "reindexing done, buckets not swapped yet"
				continue
			}

			output[i]["status"] = "swapped"
			output[i]["message"] = "swapped buckets"

			// load the swapped.mig file and add it's value to the output
			swappedFile, err := os.ReadFile(path + "swapped.mig")
			if err != nil {
				output[i]["status"] = "error"
				output[i]["message"] = "failed to read swapped.mig"
				continue
			}
			output[i]["swapped"] = string(swappedFile)
			output[i]["message"] = "reindexing done"
			output[i]["status"] = "done"

		}

		response := map[string]interface{}{
			"shards": output,
		}
		json.NewEncoder(w).Encode(response)

		w.WriteHeader(http.StatusOK)
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
		var targetVector string
		if len(parts) == 4 {
			targetVector = parts[3]
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

		vidx, ok := shard.GetVectorIndex(targetVector)
		if !ok {
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
