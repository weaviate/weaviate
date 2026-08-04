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

package clusterapi

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// reindexCleanupProber answers whether this node is still tearing down reindex
// sidecars for a collection.
type reindexCleanupProber interface {
	AnyCleanupInProgressForCollection(collection string) bool
}

// ReindexCleanupActivity is the answer to "have you raised your gate yet".
type ReindexCleanupActivity struct {
	CleaningUp bool `json:"cleaningUp"`
}

type ReindexCleanup struct {
	prober reindexCleanupProber
	auth   auth
}

func NewReindexCleanup(prober reindexCleanupProber, auth auth) *ReindexCleanup {
	return &ReindexCleanup{prober: prober, auth: auth}
}

// Activity handles GET /reindex/cleanup-activity?collection=<name>.
//
// Deliberately its own route rather than a mode on /backups/node-activity: a
// node running an older build has to be distinguishable from one that answers
// "no cleanup". A new query parameter on an existing route would be ignored by
// the old build, which would then return a perfectly valid backup-activity
// answer that the caller would misread. A separate path 404s instead, and the
// caller can treat that as "cannot ask" rather than as "nothing running".
func (rc *ReindexCleanup) Activity() http.Handler {
	return rc.auth.handleFunc(rc.activityHandler())
}

func (rc *ReindexCleanup) activityHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		collection := r.URL.Query().Get("collection")
		if collection == "" {
			http.Error(w, "collection query parameter is required", http.StatusBadRequest)
			return
		}

		// Never silently report "not cleaning up": a cancel's answer depends
		// on this, and a wrong "no" reopens the window it exists to close.
		if rc.prober == nil {
			http.Error(w, "reindex cleanup probe is not wired on this node", http.StatusServiceUnavailable)
			return
		}

		data, err := json.Marshal(ReindexCleanupActivity{
			CleaningUp: rc.prober.AnyCleanupInProgressForCollection(collection),
		})
		if err != nil {
			http.Error(w, fmt.Errorf("marshal response: %w", err).Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	}
}
