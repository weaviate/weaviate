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
	"net/http"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/cluster"
)

// setupRaftDebugHandlers registers test-only admin endpoints that make RAFT scriptable.
// Force-snapshot lets SELF_RECOVERY tests pin a wiped node's rejoin to the InstallSnapshot path.
func setupRaftDebugHandlers(appState *state.State, raft *cluster.Raft) {
	logger := appState.Logger.WithField("handler", "raft_debug")

	// POST /debug/raft/snapshot — blocks until the local snapshot completes.
	http.HandleFunc("/debug/raft/snapshot", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed; use POST", http.StatusMethodNotAllowed)
			return
		}
		if raft == nil {
			http.Error(w, "raft is not configured on this node", http.StatusServiceUnavailable)
			return
		}
		if err := raft.ForceSnapshot(); err != nil {
			logger.WithError(err).Error("force snapshot failed")
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		if _, err := w.Write([]byte(`{"status":"snapshot_taken"}` + "\n")); err != nil {
			logger.WithError(err).Debug("raft snapshot: response write failed (client disconnect?)")
		}
	}))
}
