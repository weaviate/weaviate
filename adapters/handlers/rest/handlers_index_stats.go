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
	"net/http"
	"strings"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/entities/schema"
)

func setupIndexStatsHandlers(appState *state.State) {
	logger := appState.Logger.WithField("handler", "debug")

	http.HandleFunc("/debug/stats/collection/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, "/debug/stats/collection/"))
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
			logger.WithField("shard", shardName).Error(err)
			http.Error(w, "stats method failed", http.StatusMethodNotAllowed)
			return
		}

		logger.Info("Stats on HNSW started")
		logger.WithField("Dimensions:", stats.Dimensions).Info("Dimensions")
		logger.WithField("EntryPointID:", stats.EntryPointID).Info("EntryPointID")
		logger.WithField("Distribution Layers:", stats.DistributionLayers).Info("Distribution Layers")
		logger.WithField("Unreachable Points:", stats.UnreachablePoints).Info("Unreachable Points")
		logger.WithField("Number of Tombstones:", stats.NumTombstones).Info("Number of Tombstones")
		logger.WithField("Cache Size:", stats.CacheSize).Info("Cache Size")
		if stats.PQConfiguration.Enabled {
			logger.WithField("PQ Configuration:", stats.PQConfiguration).Info("Quantization")
		} else if stats.SQConfiguration.Enabled {
			logger.WithField("SQ Configuration:", stats.SQConfiguration).Info("Quantization")
		} else if stats.BQConfiguration.Enabled {
			logger.WithField("BQ Configuration:", stats.BQConfiguration).Info("Quantization")
		} else {
			logger.WithField("Quantization:", "None").Info("Quantization")
		}

		w.WriteHeader(http.StatusAccepted)
	}))
}
