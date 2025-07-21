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
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/entities/schema"
)

func parseIndexAndShards(appState *state.State, r *http.Request) (string, []string, *db.Index, error) {
	colName := strings.TrimSpace(r.URL.Query().Get("collection"))
	if colName == "" {
		return "", nil, nil, fmt.Errorf("collection is required")
	}

	shardsToMigrateString := strings.TrimSpace(r.URL.Query().Get("shards"))

	shardsToMigrate := []string{}
	if shardsToMigrateString != "" {
		shardsToMigrate = strings.Split(shardsToMigrateString, ",")
	}

	className := schema.ClassName(colName)
	classNameString := strings.ToLower(className.String())
	idx := appState.DB.GetIndex(className)

	if idx == nil {
		return "", nil, nil, fmt.Errorf("collection not found or not ready")
	}

	return classNameString, shardsToMigrate, idx, nil
}

func changeFile(filename string, delete bool, logger *logrus.Entry, appState *state.State, r *http.Request, w http.ResponseWriter) {
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
				alreadyDid := false
				if len(shardsToMigrate) == 0 || slices.Contains(shardsToMigrate, shardName) {
					shardPath := rootPath + "/" + classNameString + "/" + shardName + "/lsm/.migrations/searchable_map_to_blockmax/"
					_, err := os.Stat(shardPath)
					if err != nil {
						return fmt.Errorf("shard not found or not ready")
					}
					if delete {
						err = os.Remove(shardPath + filename)
						if os.IsNotExist(err) {
							alreadyDid = true
						} else if err != nil {
							return fmt.Errorf("failed to delete %s: %w", filename, err)
						}
					} else {
						// check if the file already exists
						_, err = os.Stat(shardPath + filename)
						if err == nil {
							alreadyDid = true
						} else {
							file, err := os.Create(shardPath + filename)
							if os.IsExist(err) {
								alreadyDid = true
							} else if err != nil {
								return fmt.Errorf("failed to create %s: %w", filename, err)
							}
							defer file.Close()
						}
					}
					response[shardName] = map[string]string{
						"status": "success",
						"message": fmt.Sprintf("file %s %s in shard %s", filename,
							func() string {
								if delete {
									if alreadyDid {
										return "already deleted"
									}
									return "deleted"
								} else {
									if alreadyDid {
										return "already created"
									}
								}
								return "created"
							}(), shardName),
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
}
