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

	http.HandleFunc(
		"/debug/async-replication/remove-target-overrides",
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			collectionName := r.URL.Query().Get("collection")
			if collectionName == "" {
				http.Error(w, "collection is required", http.StatusBadRequest)
				return
			}
			shardNamesStr := r.URL.Query().Get("shardNames")
			if shardNamesStr == "" {
				http.Error(w, "shardNames is required", http.StatusBadRequest)
				return
			}
			shardNames := strings.Split(shardNamesStr, ",")
			if len(shardNames) == 0 {
				http.Error(w, "shardNames len > 0 is required", http.StatusBadRequest)
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

			idx := appState.DB.GetIndex(schema.ClassName(collectionName))
			if idx == nil {
				logger.WithField("collection", collectionName).Error("collection not found")
				http.Error(w, "collection not found", http.StatusNotFound)
				return
			}
			for _, shardName := range shardNames {
				err = idx.IncomingRemoveAllAsyncReplicationTargetNodes(ctx, shardName)
				if err != nil {
					logger.WithError(err).WithField("collection", collectionName).WithField("shard", shardName).
						Warn("debug endpoint failed to remove all async replication target nodes")
					http.Error(w, "failed to remove all async replication target nodes", http.StatusInternalServerError)
					return
				}
				logger.WithField("collection", collectionName).WithField("shard", shardName).
					Info("debug endpoint removed all async replication target nodes")
			}
			w.WriteHeader(http.StatusAccepted)
		}))

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
		changeFile("paused.mig", false, logger, appState, r, w)
	}))

	http.HandleFunc("/debug/index/rebuild/inverted/resume", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		changeFile("paused.mig", true, logger, appState, r, w)
	}))

	http.HandleFunc("/debug/index/rebuild/inverted/rollback", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		changeFile("rollback.mig", false, logger, appState, r, w)
	}))

	http.HandleFunc("/debug/index/rebuild/inverted/unrollback", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		changeFile("rollback.mig", true, logger, appState, r, w)
	}))

	http.HandleFunc("/debug/index/rebuild/inverted/start", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		changeFile("start.mig", false, logger, appState, r, w)
	}))

	http.HandleFunc("/debug/index/rebuild/inverted/unstart", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		changeFile("start.mig", true, logger, appState, r, w)
	}))

	http.HandleFunc("/debug/index/rebuild/inverted/reload", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		colName := r.URL.Query().Get("collection")

		if colName == "" {
			http.Error(w, "collection name is required", http.StatusBadRequest)
			return
		}

		shardsToMigrateString := strings.TrimSpace(r.URL.Query().Get("shards"))

		shardsToMigrate := []string{}
		if shardsToMigrateString != "" {
			shardsToMigrate = strings.Split(shardsToMigrateString, ",")
		}

		className := schema.ClassName(colName)
		idx := appState.DB.GetIndex(className)

		if idx == nil {
			logger.WithField("collection", colName).Error("collection not found or not ready")
			http.Error(w, "collection not found or not ready", http.StatusNotFound)
			return
		}

		output := make(map[string]map[string]string)
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
					output[shardName] = map[string]string{
						"shard":       shardName,
						"shardStatus": shard.GetStatusNoLoad().String(),
						"status":      "reinit",
						"message":     "reinit shard started",
					}
				} else {
					output[shardName] = map[string]string{
						"shard":       shardName,
						"shardStatus": shard.GetStatusNoLoad().String(),
						"status":      "skipped",
						"message":     fmt.Sprintf("shard %s not selected", shardName),
					}
				}
				return nil
			},
		)

		response := map[string]interface{}{
			"shards": output,
		}

		if err != nil {
			logger.WithField("collection", colName).Error("failed to get shard names")
			http.Error(w, "failed to get shard names", http.StatusInternalServerError)
			response["error"] = "failed to get shard names: " + err.Error()
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
		response := map[string]interface{}{
			"BlockMax WAND": "unknown",
			"status":        "unknown",
		}
		output := make(map[string]map[string]interface{})
		func() {
			rootPath := appState.DB.GetConfig().RootPath
			colName := r.URL.Query().Get("collection")
			className := schema.ClassName(colName)
			classNameString, _, idx, err := parseIndexAndShards(appState, r)
			if err != nil {
				logger.WithError(err).Error("failed to parse index and shards")
				response["status"] = "error"
				response["error"] = err.Error()
				return
			}

			response["BlockMax WAND"] = "not_ready"
			blockMaxEnabled := idx.GetInvertedIndexConfig().UsingBlockMaxWAND
			if blockMaxEnabled {
				response["BlockMax WAND"] = "enabled"
			}
			// shard map: shardName -> shardPath
			paths := make(map[string]string)

			// shards will not be force loaded, as we are only getting the name
			err = idx.ForEachShard(
				func(shardName string, shard db.ShardLike) error {
					shardPath := rootPath + "/" + classNameString + "/" + shardName + "/lsm/"
					paths[shardName] = shardPath
					output[shardName] = map[string]interface{}{
						"shardStatus": shard.GetStatusNoLoad().String(),
						"status":      "unknown",
					}
					return nil
				},
			)
			if err != nil {
				logger.WithField("collection", colName).Error("failed to get shard names")
				response["status"] = "error"
				response["error"] = err.Error()
				return
			}

			// tenant map: tenantName -> *models.TenantResponse
			tenantMap := make(map[string]*models.Tenant)

			info := appState.SchemaManager.SchemaReader.ClassInfo(className.String())
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if info.MultiTenancy.Enabled {

				tenantResponses, err := appState.SchemaManager.GetConsistentTenants(ctx, nil, colName, true, []string{})
				if err != nil {
					logger.WithField("collection", colName).Error("failed to get tenant responses")
					response["status"] = "error"
					response["error"] = err.Error()
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
				logger.WithField("collection", colName).Error("no tenants found")
				response["status"] = "error"
				response["error"] = "no tenants found"
				return
			}

			for i, tenant := range tenantMap {
				path := paths[tenant.Name]

				if path == "" {
					output[i]["status"] = "shard_not_loaded"
					output[i]["message"] = "shard not loaded"
					output[i]["action"] = "load shard or activate tenant"
					continue
				}
				// check if the shard directory exists
				_, err := os.Stat(path)
				if err != nil {
					output[i]["status"] = "shard_not_loaded"
					output[i]["message"] = "shard directory does not exist"
					output[i]["action"] = "load shard or activate tenant"
					continue
				}

				// check if a .migrations/searchable_map_to_blockmax exists
				_, err = os.Stat(path + ".migrations/searchable_map_to_blockmax")
				if err != nil {
					output[i]["status"] = "not_started"
					output[i]["message"] = "no searchable_map_to_blockmax folder found"
					output[i]["action"] = "enable relevant REINDEX_MAP_TO_BLOCKMAX_* env vars"
					continue
				}

				keyParser := &db.UuidKeyParser{}
				rt := db.NewFileMapToBlockmaxReindexTracker(path, keyParser)

				status, message, action := rt.GetStatusStrings()

				if appState.ServerConfig.Config.ReindexMapToBlockmaxConfig.ConditionalStart && !rt.HasStartCondition() {
					message = "reindexing not started, no start condition file found"
					status = "not_started"
					action = "call /start?collection=<> endpoint to start reindexing"
				}

				output[i]["status"] = status
				output[i]["message"] = message
				output[i]["action"] = action

				properties, _ := rt.GetProps()
				if properties != nil {
					output[i]["properties"] = properties
				}
				output[i]["times"] = rt.GetTimes()

				migratedCount, snapshots, _ := rt.GetMigratedCount()

				output[i]["migratedCount"] = fmt.Sprintf("%d", migratedCount)
				output[i]["snapshotCount"] = fmt.Sprintf("%d", len(snapshots))
				output[i]["snapshots"] = snapshots

			}
			response["status"] = "success"
		}()

		response["shards"] = output
		jsonBytes, err := json.Marshal(response)
		if err != nil {
			logger.WithError(err).Error("marshal failed on stats")
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonBytes)
	}))

	http.HandleFunc("/debug/index/rebuild/inverted/overrides", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{}

		func() {
			classNameString, shardsToMigrate, idx, err := parseIndexAndShards(appState, r)
			if err != nil {
				logger.WithError(err).Error("failed to parse index and shards")
				response["error"] = err.Error()
				return
			}

			rootPath := appState.DB.GetConfig().RootPath
			err = idx.ForEachShard(
				func(shardName string, shard db.ShardLike) error {
					if len(shardsToMigrate) == 0 || slices.Contains(shardsToMigrate, shardName) {
						shardPath := rootPath + "/" + classNameString + "/" + shardName + "/lsm/"
						_, err := os.Stat(shardPath + ".migrations/searchable_map_to_blockmax")
						if err != nil {
							return fmt.Errorf("shard not found or not ready")
						}
						filename := "overrides.mig"
						_, err = os.Stat(shardPath + ".migrations/searchable_map_to_blockmax/" + filename)
						if err != nil {
							response[shardName] = map[string]string{
								"status":    "not found",
								"overrides": "no overrides.mig file found",
							}
							return nil
						}
						// read the overrides.mig file
						file, err := os.ReadFile(shardPath + ".migrations/searchable_map_to_blockmax/" + filename)
						if err != nil {
							return fmt.Errorf("failed to read %s in shard %s: %w", filename, shardName, err)
						}
						// parse the file content
						lines := strings.Split(string(file), "\n")
						overrides := make(map[string]string)
						for _, line := range lines {
							line = strings.TrimSpace(line)
							if line == "" || strings.HasPrefix(line, "#") {
								continue // skip empty lines and comments
							}
							parts := strings.SplitN(line, "=", 2)
							if len(parts) != 2 {
								return fmt.Errorf("invalid override format in %s: %s", filename, line)
							}
							key := strings.TrimSpace(parts[0])
							value := strings.TrimSpace(parts[1])
							overrides[key] = value
						}

						response[shardName] = map[string]interface{}{
							"status":    "success",
							"overrides": overrides,
						}
					} else {
						response[shardName] = map[string]string{
							"status":  "skipped",
							"message": fmt.Sprintf("shard %s not selected", shardName),
						}
					}
					return nil
				},
			)
			if err != nil {
				logger.WithField("collection", classNameString).WithField("shards", shardsToMigrate).WithError(err).Error("failed to iterate over shards")
				response["error"] = err.Error()
				return
			}
		}()

		jsonBytes, err := json.Marshal(response)
		if err != nil {
			logger.WithError(err).Error("marshal failed on stats")
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonBytes)
	}))

	http.HandleFunc("/debug/index/rebuild/inverted/set_overrides", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{}

		clear := config.Enabled(r.URL.Query().Get("clear"))
		func() {
			classNameString, shardsToMigrate, idx, err := parseIndexAndShards(appState, r)
			if err != nil {
				logger.WithError(err).Error("failed to parse index and shards")
				response["error"] = err.Error()
				return
			}

			rootPath := appState.DB.GetConfig().RootPath
			err = idx.ForEachShard(
				func(shardName string, shard db.ShardLike) error {
					if len(shardsToMigrate) == 0 || slices.Contains(shardsToMigrate, shardName) {
						shardPath := rootPath + "/" + classNameString + "/" + shardName + "/lsm/"
						_, err := os.Stat(shardPath + ".migrations/searchable_map_to_blockmax")
						if err != nil {
							return fmt.Errorf("shard not found or not ready")
						}
						filename := "overrides.mig"
						// open file for writing
						filePath := shardPath + ".migrations/searchable_map_to_blockmax/" + filename
						if clear {
							err := os.Remove(filePath)
							if err != nil && !os.IsNotExist(err) {
								return fmt.Errorf("failed to clear %s in shard %s: %w", filename, shardName, err)
							}
						}
						file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
						if err != nil {
							return fmt.Errorf("failed to open %s in shard %s: %w", filename, shardName, err)
						}
						defer file.Close()
						// get overrides from query params
						overridesURL := r.URL.Query()
						overrides := make(map[string][]string)
						for key, values := range overridesURL {
							if key == "clear" || key == "collection" || key == "shards" {
								continue
							}
							for _, value := range values {
								overrides[key] = append(overrides[key], value)
								_, err := file.WriteString(fmt.Sprintf("%s=%s\n", key, value))
								if err != nil {
									return fmt.Errorf("failed to write to %s in shard %s: %w", filename, shardName, err)
								}
							}
						}

						response[shardName] = map[string]interface{}{
							"status": "success",
							"wrote":  overrides,
						}
					} else {
						response[shardName] = map[string]string{
							"status":  "skipped",
							"message": fmt.Sprintf("shard %s not selected", shardName),
						}
					}
					return nil
				},
			)
			if err != nil {
				logger.WithField("collection", classNameString).WithField("shards", shardsToMigrate).WithError(err).Error("failed to iterate over shards")
				response["error"] = err.Error()
				return
			}
		}()

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
