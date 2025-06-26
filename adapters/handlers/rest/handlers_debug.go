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
	"slices"
	"strings"
	"time"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/entities/models"
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

	http.HandleFunc("/debug/index/rebuild/inverted/cancelReindexContext", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		appState.ReindexCtxCancel(fmt.Errorf("cancelReindexContext endpoint"))
		w.WriteHeader(http.StatusAccepted)
	}))

	http.HandleFunc("/debug/index/rebuild/inverted/suspend", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		colName := r.URL.Query().Get("collection")
		if colName == "" {
			http.Error(w, "collection is required", http.StatusBadRequest)
			return
		}
		shardsToMigrateString := r.URL.Query().Get("shards")
		shardsToMigrate := strings.Split(shardsToMigrateString, ",")[1:]

		className := schema.ClassName(colName)
		classNameString := strings.ToLower(className.String())
		idx := appState.DB.GetIndex(className)

		rootPath := appState.DB.GetConfig().RootPath

		if idx == nil {
			logger.WithField("collection", colName).Error("collection not found or not ready")
			http.Error(w, "collection not found or not ready", http.StatusNotFound)
			return
		}
		err := idx.ForEachShard(
			func(shardName string, shard db.ShardLike) error {
				if len(shardsToMigrate) == 0 || slices.Contains(shardsToMigrate, shardName) {
					shardPath := rootPath + "/" + classNameString + "/" + shardName + "/lsm/"
					_, err := os.Stat(shardPath + ".migrations/searchable_map_to_blockmax")
					if err != nil {
						logger.WithField("shard", shardName).Error("shard not found or not ready")
						http.Error(w, "shard not found or not ready", http.StatusNotFound)
						return fmt.Errorf("shard not found or not ready")
					}
					// create paused.mig file
					pauseFile, err := os.Create(shardPath + ".migrations/searchable_map_to_blockmax/paused.mig")
					if err != nil && !os.IsExist(err) {
						logger.WithField("shard", shardName).Error("failed to create paused.mig file")
						http.Error(w, "failed to create paused.mig file", http.StatusInternalServerError)
						return fmt.Errorf("failed to create paused.mig file")
					}
					defer pauseFile.Close()

				}
				return nil
			},
		)
		if err != nil {
			logger.WithField("collection", colName).Error("failed to get shard names")
			http.Error(w, "failed to get shard names", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusAccepted)
	}))

	http.HandleFunc("/debug/index/rebuild/inverted/resume", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		colName := r.URL.Query().Get("collection")
		if colName == "" {
			http.Error(w, "collection is required", http.StatusBadRequest)
			return
		}
		shardsToMigrateString := r.URL.Query().Get("shards")
		shardsToMigrate := strings.Split(shardsToMigrateString, ",")[1:]

		className := schema.ClassName(colName)
		classNameString := strings.ToLower(className.String())
		idx := appState.DB.GetIndex(className)

		rootPath := appState.DB.GetConfig().RootPath

		if idx == nil {
			logger.WithField("collection", colName).Error("collection not found or not ready")
			http.Error(w, "collection not found or not ready", http.StatusNotFound)
			return
		}
		err := idx.ForEachShard(
			func(shardName string, shard db.ShardLike) error {
				if len(shardsToMigrate) == 0 || slices.Contains(shardsToMigrate, shardName) {
					shardPath := rootPath + "/" + classNameString + "/" + shardName + "/lsm/"
					_, err := os.Stat(shardPath + ".migrations/searchable_map_to_blockmax")
					if err != nil {
						logger.WithField("shard", shardName).Error("shard not found or not ready")
						http.Error(w, "shard not found or not ready", http.StatusNotFound)
						return fmt.Errorf("shard not found or not ready")
					}
					// create paused.mig file
					err = os.Remove(shardPath + ".migrations/searchable_map_to_blockmax/paused.mig")
					if err != nil && !os.IsNotExist(err) {
						logger.WithField("shard", shardName).Error("failed to delete paused.mig file")
						http.Error(w, "failed to delete paused.mig file", http.StatusInternalServerError)
						return err
					}

				}
				return nil
			},
		)
		if err != nil {
			logger.WithField("collection", colName).Error(err, "failed to get shard names")
			http.Error(w, "failed to get shard names "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusAccepted)
	}))

	http.HandleFunc("/debug/index/rebuild/inverted/rollback", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		colName := r.URL.Query().Get("collection")
		if colName == "" {
			http.Error(w, "collection is required", http.StatusBadRequest)
			return
		}
		shardsToMigrateString := r.URL.Query().Get("shards")
		shardsToMigrate := strings.Split(shardsToMigrateString, ",")[1:]

		className := schema.ClassName(colName)
		classNameString := strings.ToLower(className.String())
		idx := appState.DB.GetIndex(className)

		rootPath := appState.DB.GetConfig().RootPath

		if idx == nil {
			logger.WithField("collection", colName).Error("collection not found or not ready")
			http.Error(w, "collection not found or not ready", http.StatusNotFound)
			return
		}
		err := idx.ForEachShard(
			func(shardName string, shard db.ShardLike) error {
				if len(shardsToMigrate) == 0 || slices.Contains(shardsToMigrate, shardName) {
					shardPath := rootPath + "/" + classNameString + "/" + shardName + "/lsm/"
					_, err := os.Stat(shardPath + ".migrations/searchable_map_to_blockmax")
					if err != nil {
						logger.WithField("shard", shardName).Error("shard not found or not ready")
						http.Error(w, "shard not found or not ready", http.StatusNotFound)
						return fmt.Errorf("shard not found or not ready")
					}
					// create rollback.mig file
					rollbackFile, err := os.Create(shardPath + ".migrations/searchable_map_to_blockmax/rollback.mig")
					if err != nil && !os.IsExist(err) {
						logger.WithField("shard", shardName).Error("failed to create rollback.mig file")
						http.Error(w, "failed to create rollback.mig file", http.StatusInternalServerError)
						return fmt.Errorf("failed to create rollback.mig file")
					}
					defer rollbackFile.Close()

				}
				return nil
			},
		)
		if err != nil {
			logger.WithField("collection", colName).Error("failed to get shard names")
			http.Error(w, "failed to get shard names", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusAccepted)
	}))

	http.HandleFunc("/debug/index/rebuild/inverted/start", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		colName := r.URL.Query().Get("collection")
		shardsToMigrateString := r.URL.Query().Get("shards")
		delayStartString := r.URL.Query().Get("delayStart")
		perObjectDelayString := r.URL.Query().Get("delayPerObject")

		if delayStartString != "" {
			os.Setenv("DEBUG_SHARD_INIT_DELAY", delayStartString)
		}

		if perObjectDelayString != "" {
			os.Setenv("DEBUG_SHARD_PER_OBJECT_DELAY", perObjectDelayString)
		}

		rootPath := appState.DB.GetConfig().RootPath

		if colName == "" {
			http.Error(w, "collection name is required", http.StatusBadRequest)
			return
		}

		shardsToMigrate := strings.Split(shardsToMigrateString, ",")[1:]
		// if len(shardsToMigrate) == 0, migrate all shards

		className := schema.ClassName(colName)
		classNameString := strings.ToLower(className.String())
		idx := appState.DB.GetIndex(className)

		if idx == nil {
			logger.WithField("collection", colName).Error("collection not found or not ready")
			http.Error(w, "collection not found or not ready", http.StatusNotFound)
			return
		}
		// shard map: shardName -> shardPath
		paths := make(map[string]string)
		shards := make(map[string]db.ShardLike)

		output := make(map[string]map[string]string, len(paths))
		// shards will not be force loaded, as we are only getting the name
		err := idx.ForEachShard(
			func(shardName string, shard db.ShardLike) error {
				if len(shardsToMigrate) == 0 || slices.Contains(shardsToMigrate, shardName) {
					shardPath := rootPath + "/" + classNameString + "/" + shardName + "/lsm/"
					paths[shardName] = shardPath
					shards[shardName] = shard
					output[shardName] = map[string]string{
						"shard":       shardName,
						"shardStatus": shard.GetStatusNoLoad().String(),
						"status":      "unknown",
					}

				}
				return nil
			},
		)
		if err != nil {
			logger.WithField("collection", colName).Error("failed to get shard names")
			http.Error(w, "failed to get shard names", http.StatusInternalServerError)
			return
		}

		for i, path := range paths {
			if path == "" {
				output[i]["status"] = "shard_not_loaded"
				output[i]["message"] = "shard not loaded"
				continue
			}

			// check if the shard directory exists
			_, err := os.Stat(path)
			if err != nil {
				output[i]["status"] = "shard_not_loaded"
				output[i]["message"] = "shard directory does not exist"
				continue
			}

			// check if a .migrations/searchable_map_to_blockmax exists
			_, err = os.Stat(path + ".migrations/searchable_map_to_blockmax")
			if err != nil {
				output[i]["status"] = "not_started"
				output[i]["message"] = "no searchable_map_to_blockmax folder found"
				continue
			}

			path += ".migrations/searchable_map_to_blockmax/"
			// check if started.mig exists
			_, err = os.Stat(path + "started.mig")
			if err == nil {
				output[i]["status"] = "started"
				output[i]["message"] = "reindexing started"
				continue
			}

			// migration is not started yet, create start file as start.mig
			// check if start.mig exists
			_, err = os.Stat(path + "start.mig")
			if err == nil {
				output[i]["status"] = "started"
				output[i]["message"] = "start.mig already exists, reindexing will start at next restart"
				continue
			}
			// create start.mig file
			startedFile, err := os.Create(path + "start.mig")
			if err != nil {
				output[i]["status"] = "error"
				output[i]["message"] = "failed to create start.mig"
				continue
			}
			startedFile.Close()

			output[i]["status"] = "will_start"
			output[i]["message"] = "migration will start at next restart"

		}

		response := map[string]interface{}{
			"shards": output,
		}
		jsonBytes, err := json.Marshal(response)
		if err != nil {
			logger.WithError(err).Error("marshal failed on stats")
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonBytes)
	}))

	http.HandleFunc("/debug/index/rebuild/inverted/reload", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		colName := r.URL.Query().Get("collection")
		shardsToMigrateString := r.URL.Query().Get("shards")

		rootPath := appState.DB.GetConfig().RootPath

		if colName == "" {
			http.Error(w, "collection name is required", http.StatusBadRequest)
			return
		}

		shardsToMigrate := strings.Split(shardsToMigrateString, ",")[1:]
		// if len(shardsToMigrate) == 0, migrate all shards

		className := schema.ClassName(colName)
		classNameString := strings.ToLower(className.String())
		idx := appState.DB.GetIndex(className)

		if idx == nil {
			logger.WithField("collection", colName).Error("collection not found or not ready")
			http.Error(w, "collection not found or not ready", http.StatusNotFound)
			return
		}
		// shard map: shardName -> shardPath
		paths := make(map[string]string)
		shards := make(map[string]db.ShardLike)

		output := make(map[string]map[string]string, len(paths))
		// shards will not be force loaded, as we are only getting the name
		err := idx.ForEachShard(
			func(shardName string, shard db.ShardLike) error {
				if len(shardsToMigrate) == 0 || slices.Contains(shardsToMigrate, shardName) {
					err := idx.IncomingReinitShard(
						context.Background(),
						shardName,
					)
					if err != nil {
						logger.WithField("shard", shardName).Error("failed to reinit shard " + err.Error())
						output[shardName] = map[string]string{
							"shard":       shardName,
							"shardStatus": shard.GetStatusNoLoad().String(),
							"status":      "error",
							"message":     "failed to reinit shard",
							"error":       err.Error(),
						}
						return nil
					}
					shardPath := rootPath + "/" + classNameString + "/" + shardName + "/lsm/"
					paths[shardName] = shardPath
					shards[shardName] = shard
					output[shardName] = map[string]string{
						"shard":       shardName,
						"shardStatus": shard.GetStatusNoLoad().String(),
						"status":      "reinit",
						"message":     "reinit shard started",
					}
				}
				return nil
			},
		)

		response := map[string]interface{}{
			"shards": output,
		}
		jsonBytes, err := json.Marshal(response)
		if err != nil {
			logger.WithError(err).Error("marshal failed on stats")
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonBytes)
	}))

	http.HandleFunc("/debug/index/rebuild/inverted/status", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		colName := r.URL.Query().Get("collection")
		rootPath := appState.DB.GetConfig().RootPath

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if colName == "" {
			http.Error(w, "collection and shard are required", http.StatusBadRequest)
			return
		}
		className := schema.ClassName(colName)
		info := appState.SchemaManager.SchemaReader.ClassInfo(className.String())

		classNameString := strings.ToLower(className.String())
		idx := appState.DB.GetIndex(className)

		if idx == nil {
			logger.WithField("collection", colName).Error("collection not found or not ready")
			http.Error(w, "collection not found or not ready", http.StatusNotFound)
			return
		}

		blockMaxStatus := "not_ready"
		blockMaxEnabled := idx.GetInvertedIndexConfig().UsingBlockMaxWAND
		if blockMaxEnabled {
			blockMaxStatus = "enabled"
		}
		// shard map: shardName -> shardPath
		paths := make(map[string]string)
		output := make(map[string]map[string]interface{}, len(paths))

		// shards will not be force loaded, as we are only getting the name
		err := idx.ForEachShard(
			func(shardName string, shard db.ShardLike) error {
				shardPath := rootPath + "/" + classNameString + "/" + shardName + "/lsm/"
				paths[shardName] = shardPath
				output[shardName] = map[string]interface{}{
					"shard":       shardName,
					"shardStatus": shard.GetStatusNoLoad().String(),
					"status":      "unknown",
				}

				return nil
			},
		)
		if err != nil {
			logger.WithField("collection", colName).Error("failed to get shard names")
			http.Error(w, "failed to get shard names", http.StatusInternalServerError)
			return
		}

		// tenant map: tenantName -> *models.TenantResponse
		tenantMap := make(map[string]*models.Tenant)

		if info.MultiTenancy.Enabled {

			tenantResponses, err := appState.SchemaManager.GetConsistentTenants(ctx, nil, colName, true, []string{})
			if err != nil {
				logger.WithField("collection", colName).Error("failed to get tenant responses")
				http.Error(w, "failed to get tenant responses", http.StatusInternalServerError)
				return
			}

			for _, tenant := range tenantResponses {
				tenantMap[tenant.Name] = tenant
			}
		} else {
			for name := range paths {
				tenantMap[name] = &models.Tenant{
					Name: name,
				}
			}
		}

		if len(tenantMap) == 0 {
			http.Error(w, "no shards or tenants found", http.StatusNotFound)
			return
		}

		for i, tenant := range tenantMap {
			path := paths[tenant.Name]

			if path == "" {
				output[i]["status"] = "shard_not_loaded"
				output[i]["message"] = "shard not loaded"
				continue
			}
			// check if the shard directory exists
			_, err := os.Stat(path)
			if err != nil {
				output[i]["status"] = "shard_not_loaded"
				output[i]["message"] = "shard directory does not exist"
				continue
			}

			// check if a .migrations/searchable_map_to_blockmax exists
			_, err = os.Stat(path + ".migrations/searchable_map_to_blockmax")
			if err != nil {
				output[i]["status"] = "not_started"
				output[i]["message"] = "no searchable_map_to_blockmax folder found"
				continue
			}

			keyParser := &db.UuidKeyParser{}
			rt := db.NewFileMapToBlockmaxReindexTracker(path, keyParser)

			status, message := rt.GetStatusStrings()
			output[i]["status"] = status
			output[i]["message"] = message

			properties, err := rt.GetProps()
			if err != nil {
				output[i]["properties"] = fmt.Sprintf("error: %v", err)
			} else {
				output[i]["properties"] = properties
			}
			output[i]["times"] = rt.GetTimes()

			migratedCount, snapshotCount, _ := rt.GetMigratedCount()

			output[i]["migratedCount"] = fmt.Sprintf("%d", migratedCount)
			output[i]["snapshotCount"] = fmt.Sprintf("%d", snapshotCount)

		}

		response := map[string]interface{}{
			"shards":        output,
			"BlockMax WAND": blockMaxStatus,
		}

		jsonBytes, err := json.Marshal(response)
		if err != nil {
			logger.WithError(err).Error("marshal failed on stats")
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonBytes)
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
