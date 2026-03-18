//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rest

import (
	"encoding/json"
	"net/http"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

func setupShardNoopDebugHandler(appState *state.State, provider *distributedtask.ShardNoopProvider) {
	// GET /debug/distributed-tasks/status?id=<taskID>
	http.HandleFunc("/debug/distributed-tasks/status", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		taskID := r.URL.Query().Get("id")
		if taskID == "" {
			http.Error(w, `{"error":"missing id parameter"}`, http.StatusBadRequest)
			return
		}

		// Find the task version by listing tasks
		tasks, err := appState.ClusterService.ListDistributedTasks(r.Context())
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		desc := distributedtask.TaskDescriptor{ID: taskID}
		for _, task := range tasks[distributedtask.ShardNoopProviderNamespace] {
			if task.ID == taskID {
				desc.Version = task.Version
				break
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"taskCompleted":     provider.IsTaskCompleted(desc),
			"finalizedSubUnits": provider.GetFinalizedUnits(desc),
		})
	}))

	// POST /debug/distributed-tasks/add — JSON body with task configuration.
	http.HandleFunc("/debug/distributed-tasks/add", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			ID                string            `json:"id"`
			Units             []string          `json:"subUnits,omitempty"`
			UnitGroups        map[string]string `json:"subUnitGroups,omitempty"` // unitID → groupID
			FailSubUnit       string            `json:"failSubUnit,omitempty"`
			Collection        string            `json:"collection,omitempty"`
			UnitToShard       map[string]string `json:"subUnitToShard,omitempty"`
			UnitToNode        map[string]string `json:"subUnitToNode,omitempty"`
			SlowSubUnit       string            `json:"slowSubUnit,omitempty"`
			SlowDelayMs       int               `json:"slowDelayMs,omitempty"`
			ProcessingDelayMs int               `json:"processingDelayMs,omitempty"`
			MaxConcurrency    int               `json:"maxConcurrency,omitempty"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid JSON body"}`, http.StatusBadRequest)
			return
		}

		if req.ID == "" {
			http.Error(w, `{"error":"missing id field"}`, http.StatusBadRequest)
			return
		}

		payload := distributedtask.ShardNoopProviderPayload{
			FailUnitID:         req.FailSubUnit,
			Collection:         req.Collection,
			UnitToShard:        req.UnitToShard,
			UnitToNode:         req.UnitToNode,
			SlowUnitID:         req.SlowSubUnit,
			SlowSubUnitDelayMs: req.SlowDelayMs,
			ProcessingDelayMs:  req.ProcessingDelayMs,
			MaxConcurrency:     req.MaxConcurrency,
		}

		var err error
		if len(req.UnitGroups) > 0 {
			specs := make([]distributedtask.UnitSpec, 0, len(req.Units))
			for _, suID := range req.Units {
				specs = append(specs, distributedtask.UnitSpec{
					ID:      suID,
					GroupID: req.UnitGroups[suID],
				})
			}
			err = appState.ClusterService.AddDistributedTaskWithGroups(
				r.Context(),
				distributedtask.ShardNoopProviderNamespace,
				req.ID,
				payload,
				specs,
			)
		} else {
			err = appState.ClusterService.AddDistributedTask(
				r.Context(),
				distributedtask.ShardNoopProviderNamespace,
				req.ID,
				payload,
				req.Units,
			)
		}
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{"status": "accepted"})
	}))
}
