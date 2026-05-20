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

// setupRaftDebugHandlers registers admin endpoints used to make RAFT
// behaviour observable / scriptable from tests. Currently a single
// endpoint that forces an immediate snapshot — used by the
// SELF_RECOVERY acceptance tests so a wiped node's rejoin is
// guaranteed to go through InstallSnapshot → Restore. Both the
// snapshot/Restore and log-replay rejoin paths can trigger
// self-recovery; this endpoint just lets tests pick which path
// deterministically rather than waiting on snapshot-threshold timing.
func setupRaftDebugHandlers(appState *state.State, raft *cluster.Raft) {
	logger := appState.Logger.WithField("handler", "raft_debug")

	// POST /debug/raft/snapshot — triggers raft.Snapshot() on the
	// local node and blocks until it completes. Returns 202 on
	// success, 500 on error.
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
