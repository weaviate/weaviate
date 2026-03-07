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
	"strings"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

func setupDummyTaskDebugHandler(appState *state.State, provider *distributedtask.DummyProvider) {
	// POST /debug/distributed-tasks/add?id=<taskID>&sub_units=su-1,su-2&fail_sub_unit=su-2
	http.HandleFunc("/debug/distributed-tasks/add", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		taskID := r.URL.Query().Get("id")
		if taskID == "" {
			http.Error(w, `{"error":"missing id parameter"}`, http.StatusBadRequest)
			return
		}

		subUnitsParam := r.URL.Query().Get("sub_units")
		var subUnitIDs []string
		if subUnitsParam != "" {
			subUnitIDs = strings.Split(subUnitsParam, ",")
		}

		payload := distributedtask.DummyProviderPayload{
			FailSubUnitID: r.URL.Query().Get("fail_sub_unit"),
		}

		err := appState.ClusterService.AddDistributedTask(
			r.Context(),
			distributedtask.DummyProviderNamespace,
			taskID,
			payload,
			subUnitIDs,
		)
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
