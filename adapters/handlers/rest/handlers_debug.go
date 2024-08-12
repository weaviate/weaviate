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

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/schema"
)

type DebugGraph struct {
	Nodes      []DebugGraphNode `json:"nodes"`
	Tombstones []DebugGraphNode `json:"tombstones"`
}

type DebugGraphNode struct {
	DocID uint64       `json:"docID"`
	ObjID *strfmt.UUID `json:"objID"`
}

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

		// Reset the queue
		var q *db.IndexQueue
		if vecIdxID == "main" {
			q = shard.Queue()
		} else {
			q = shard.Queues()[vecIdxID]
		}
		if q == nil {
			logger.WithField("shard", shardName).Error("index queue not found")
			http.Error(w, "index queue not found", http.StatusNotFound)
			return
		}

		// Reset the vector index
		err := shard.DebugResetVectorIndex(context.Background(), vecIdxID)
		if err != nil {
			logger.WithField("shard", shardName).WithError(err).Error("failed to reset vector index")
			http.Error(w, "failed to reset vector index", http.StatusInternalServerError)
			return
		}

		// Reindex in the background
		errors.GoWrapper(func() {
			err = q.PreloadShard(shard)
			if err != nil {
				logger.WithField("shard", shardName).WithError(err).Error("failed to reindex vector index")
				return
			}
		}, logger)

		logger.WithField("shard", shardName).Info("reindexing started")

		w.WriteHeader(http.StatusAccepted)
	}))

	http.HandleFunc("/debug/graph/collection/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			w.Write([]byte("GET is allowed only"))
			return
		}
		path := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, "/debug/graph/collection/"))
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
			http.Error(w, "index ", http.StatusNotFound)
			return
		}

		shard := idx.GetShard(shardName)
		if shard == nil {
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

		graph, err := hnsw.GetGraph(vidx, "/debug/graph/collection/"+colName+"/shards/"+shardName)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var nodes []DebugGraphNode
		for _, node := range graph.Nodes {
			docID := node.ID
			var objID *strfmt.UUID
			obj, _ := shard.ObjectByIndexID(r.Context(), docID, true) // Ignore error, object will be nil in response if cannot be found
			if obj != nil {
				id := obj.ID()
				objID = &id
			}
			nodes = append(nodes, DebugGraphNode{
				DocID: docID,
				ObjID: objID,
			})
		}
		var tombstones []DebugGraphNode
		for docID := range graph.Tombstones {
			var objID *strfmt.UUID
			obj, _ := shard.ObjectByIndexID(r.Context(), docID, true) // Ignore error, object will be nil in response if cannot be found
			if obj != nil {
				id := obj.ID()
				objID = &id
			}
			tombstones = append(tombstones, DebugGraphNode{
				DocID: docID,
				ObjID: objID,
			})
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(DebugGraph{Nodes: nodes, Tombstones: tombstones})
	}))
}
