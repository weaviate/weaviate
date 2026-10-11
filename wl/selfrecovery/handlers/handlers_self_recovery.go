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

// Package handlers serves the /debug/self-recovery operator endpoints of a licensed node.
// It is Weaviate-licensed (wl/LICENSE-WEAVIATE), unlike the BSD-3-Clause code outside wl/.
package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/sirupsen/logrus"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/wl/selfrecovery"
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

const (
	// AcceptEmptyPath is the accept-empty escape hatch.
	AcceptEmptyPath = "/debug/self-recovery/accept-empty"
	// RestartPath cancels in-flight ops, erases ".recovering/" and submits afresh.
	RestartPath = "/debug/self-recovery/restart"
)

// SetupHandlers registers the SELF_RECOVERY operator endpoints on mux. The handlers check neither the flag nor the license key, so call it only in FeatureLicensed.
func SetupHandlers(mux *http.ServeMux, logger logrus.FieldLogger, orch *selfrecovery.Orchestrator) {
	logger = logger.WithField("handler", "self_recovery")
	mux.HandleFunc(AcceptEmptyPath, newAcceptEmptyHandler(logger, orch))
	mux.HandleFunc(RestartPath, newRestartHandler(logger, orch))
}

func newAcceptEmptyHandler(logger logrus.FieldLogger, orch *selfrecovery.Orchestrator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
			logger.WithField("collection", collection).WithField("shard", shard).
				Errorf("self-recovery accept-empty failed: %v", err)
			// Unknown collection/shard is a client mistake, not a 500.
			if errors.Is(err, selfrecovery.ErrSelfRecoveryShardNotInSchema) {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			if errors.Is(err, selfrecovery.ErrSelfRecoveryOpInFlight) {
				http.Error(w, err.Error(), http.StatusConflict)
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
			logger.Debugf("self-recovery accept-empty: response write failed (client disconnect?): %v", err)
		}
	}
}

func newRestartHandler(logger logrus.FieldLogger, orch *selfrecovery.Orchestrator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
			logger.WithField("collection", collection).WithField("shard", shard).
				Errorf("self-recovery restart failed: %v", err)
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
			// An op past FINALIZING refuses the cancel and resumes on its own.
			if errors.Is(err, replicationTypes.ErrCancellationImpossible) {
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
			logger.Debugf("self-recovery restart: response write failed (client disconnect?): %v", err)
		}
	}
}
