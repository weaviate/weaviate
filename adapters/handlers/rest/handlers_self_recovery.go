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

// validShardOrCollection rejects values that could escape the data root when joined into <root>/<class>/<shard>.
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

// setupSelfRecoveryHandlers registers the SELF_RECOVERY operator endpoints under /debug/self-recovery.
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
		// WithoutCancel: promotion (LoadLocalShard) can outlive the request.
		path, err := orch.AcceptEmpty(context.WithoutCancel(r.Context()), selfrecovery.ShardRef{Collection: collection, Shard: shard})
		if err != nil {
			logger.WithError(err).WithField("collection", collection).WithField("shard", shard).
				Error("self-recovery accept-empty failed")
			// Unknown collection/shard is a client mistake, not a 500.
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
		// WithoutCancel: the resubmit outlives the handler.
		if err := orch.RestartRecovery(context.WithoutCancel(r.Context()), collection, shard); err != nil {
			logger.WithError(err).WithField("collection", collection).WithField("shard", shard).
				Error("self-recovery restart failed")
			// Unknown collection/shard is a client mistake, not a 500.
			if errors.Is(err, selfrecovery.ErrSelfRecoveryShardNotInSchema) {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			// Shard already has a live local dir — nothing to restart.
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
