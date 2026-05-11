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
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/cluster/replication/selfrecovery"
)

// validShardOrCollection rejects values that could escape the data root
// when concatenated into a path (orchestrator builds <root>/<class>/<shard>).
// The schema already restricts class/shard names to a safe charset; this
// is defense in depth on the operator-facing endpoints.
func validShardOrCollection(name string) error {
	if name == "" {
		return errors.New("must not be empty")
	}
	if strings.ContainsAny(name, `/\`+"\x00") {
		return errors.New("must not contain path separators or null bytes")
	}
	if name == "." || name == ".." || strings.HasPrefix(name, "..") {
		return errors.New("must not be a relative path component")
	}
	return nil
}

// setupSelfRecoveryHandlers registers the SELF_RECOVERY operator
// endpoints under /debug/self-recovery (accept-empty and restart),
// sibling of the other node-local admin handlers in handlers_debug.go.
func setupSelfRecoveryHandlers(appState *state.State, orch *selfrecovery.Orchestrator) {
	logger := appState.Logger.WithField("handler", "self_recovery")

	http.HandleFunc("/debug/self-recovery/accept-empty", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed; use POST", http.StatusMethodNotAllowed)
			return
		}
		collection := r.URL.Query().Get("collection")
		shard := r.URL.Query().Get("shard")
		if err := validShardOrCollection(collection); err != nil {
			http.Error(w, "collection: "+err.Error(), http.StatusBadRequest)
			return
		}
		if err := validShardOrCollection(shard); err != nil {
			http.Error(w, "shard: "+err.Error(), http.StatusBadRequest)
			return
		}
		if orch == nil {
			http.Error(w, "self-recovery is not configured on this node", http.StatusServiceUnavailable)
			return
		}
		// WithoutCancel: AcceptEmpty's promotion step (LoadLocalShard)
		// can outlive the HTTP request; preserve values for tracing.
		path, err := orch.AcceptEmpty(context.WithoutCancel(r.Context()), selfrecovery.ShardRef{Collection: collection, Shard: shard})
		if err != nil {
			logger.WithError(err).WithField("collection", collection).WithField("shard", shard).
				Error("self-recovery accept-empty failed")
			// Schema-gate failure (unknown collection/shard) is a
			// client-side mistake, not a 500 — surface as 404.
			if errors.Is(err, selfrecovery.ErrSelfRecoveryShardNotInSchema) {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		if err := json.NewEncoder(w).Encode(map[string]string{
			"status": "accepted",
			"path":   path,
		}); err != nil {
			logger.WithError(err).Debug("self-recovery accept-empty: response write failed (client disconnect?)")
		}
	}))

	// POST /debug/self-recovery/restart?collection=X&shard=Y
	// Cancels in-flight ops, erases ".recovering/", submits afresh.
	http.HandleFunc("/debug/self-recovery/restart", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed; use POST", http.StatusMethodNotAllowed)
			return
		}
		collection := r.URL.Query().Get("collection")
		shard := r.URL.Query().Get("shard")
		if err := validShardOrCollection(collection); err != nil {
			http.Error(w, "collection: "+err.Error(), http.StatusBadRequest)
			return
		}
		if err := validShardOrCollection(shard); err != nil {
			http.Error(w, "shard: "+err.Error(), http.StatusBadRequest)
			return
		}
		if orch == nil {
			http.Error(w, "self-recovery is not configured on this node", http.StatusServiceUnavailable)
			return
		}
		// WithoutCancel: Restart re-submits the recovery using the
		// passed ctx; if we used r.Context() the resubmit would be
		// canceled the moment the handler returned. Values (tracing)
		// are still inherited.
		if err := orch.RestartRecovery(context.WithoutCancel(r.Context()), collection, shard); err != nil {
			logger.WithError(err).WithField("collection", collection).WithField("shard", shard).
				Error("self-recovery restart failed")
			// The shard already has a live local dir — nothing to restart.
			// That's an operator-side mistake, not a 500.
			if errors.Is(err, selfrecovery.ErrSelfRecoveryShardAlreadyLive) {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		if err := json.NewEncoder(w).Encode(map[string]string{
			"status": "restarted",
		}); err != nil {
			logger.WithError(err).Debug("self-recovery restart: response write failed (client disconnect?)")
		}
	}))
}
