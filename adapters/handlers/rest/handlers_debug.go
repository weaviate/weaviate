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
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/schema"
)

type DebugGraph struct {
	Ghosts     []DebugGraphVertex `json:"ghosts"`     // Nodes without objects in the LSM store
	Orphans    []DebugGraphVertex `json:"orphans"`    // Objects in the LSM store without nodes in the graph
	Tombstones []DebugGraphVertex `json:"tombstones"` // Nodes that have been deleted, objID should be nil for all
	Vertices   []DebugGraphVertex `json:"nodes"`      // Nodes in the graph
}

type DebugGraphVertex struct {
	DocID       uint64       `json:"docID"`
	ObjID       *strfmt.UUID `json:"objID"`
	Maintenance bool         `json:"maintenance"`
	Connections [][]uint64   `json:"connections"`
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

		includeConnections := r.URL.Query().Get("include") == "connections"

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

		var ghosts []DebugGraphVertex
		var orphans []DebugGraphVertex
		var tombstones []DebugGraphVertex
		var vertices []DebugGraphVertex
		for _, node := range graph.Nodes {
			docID := node.ID
			obj, _ := shard.ObjectByIndexID(r.Context(), docID, true) // Ignore error, object will be nil in response if cannot be found
			vertex := DebugGraphVertex{
				Connections: node.Connections,
				DocID:       docID,
				Maintenance: node.Maintenance,
			}
			if includeConnections {
				vertex.Connections = node.Connections
			}
			if obj != nil {
				id := obj.ID()
				vertex.ObjID = &id
				vertices = append(vertices, vertex)
			} else {
				ghosts = append(ghosts, vertex)
			}
		}
		for docID := range graph.Tombstones {
			var objID *strfmt.UUID
			obj, _ := shard.ObjectByIndexID(r.Context(), docID, true) // Ignore error, object will be nil in response if cannot be found
			if obj != nil {
				id := obj.ID()
				objID = &id
			}
			tombstones = append(tombstones, DebugGraphVertex{
				DocID: docID,
				ObjID: objID,
			})
		}

		count := shard.ObjectCount()
		limit := 10000
		for count > 0 {
			objs, err := shard.ObjectList(r.Context(), limit, nil, nil, additional.Properties{}, schema.ClassName(shardName))
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			for _, obj := range objs {
				docID := obj.DocID
				if !vidx.ContainsNode(docID) {
					id := obj.ID()
					orphans = append(orphans, DebugGraphVertex{
						DocID: docID,
						ObjID: &id,
					})
				}
			}
			count -= limit
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(DebugGraph{
			Ghosts:     ghosts,
			Orphans:    orphans,
			Tombstones: tombstones,
			Vertices:   vertices,
		})
	}))
}
