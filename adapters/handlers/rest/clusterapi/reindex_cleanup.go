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
	"strconv"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/clusterprobe"
)

// ReindexCleanupProber answers whether this node has seen a cancel for the
// collection or is still tearing down its reindex sidecars. This is the
// confirmation signal a cancelling node waits on; it blocks nothing itself,
// unlike the cleanup gate the backup and restore admission checks read.
type ReindexCleanupProber interface {
	AnyCleanupInProgressForCollection(collection string) bool
}

type ReindexCleanup struct {
	// resolve is called per request, not once at construction: the internal
	// server is built before the reindex provider exists, so capturing the
	// value here would freeze a nil that never becomes real.
	resolve func() ReindexCleanupProber
	auth    auth
	logger  logrus.FieldLogger
}

func NewReindexCleanup(resolve func() ReindexCleanupProber, auth auth, logger logrus.FieldLogger) *ReindexCleanup {
	return &ReindexCleanup{resolve: resolve, auth: auth, logger: logger}
}

// NewReindexCleanupFromState is the wiring the internal server uses; see the
// resolve field for why it binds late.
func NewReindexCleanupFromState(appState *state.State, auth auth) *ReindexCleanup {
	var logger logrus.FieldLogger
	if appState != nil && appState.Logger != nil {
		logger = appState.Logger
	}
	return NewReindexCleanup(func() ReindexCleanupProber {
		if appState == nil {
			return nil
		}
		// Load gives a concrete pointer, which must be compared as one here:
		// returning it unconditionally would box a nil into the interface,
		// where it reads as non-nil to the handler's == nil check.
		provider := appState.ReindexProvider.Load()
		if provider == nil {
			return nil
		}
		return provider
	}, auth, logger)
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

// loggedCollectionLimit caps the query-string value this handler logs. A
// collection name is far shorter; the cap exists for the value an unauthorized
// caller can send, not for the one a peer sends.
const loggedCollectionLimit = 128

// loggableCollection makes an attacker-supplied query value safe to put in a
// logrus field: quoting escapes the newline that would otherwise split one log
// line into two forgeable ones, and the cap stops a megabyte of query string
// from being written per request.
func loggableCollection(collection string) string {
	if len(collection) > loggedCollectionLimit {
		collection = collection[:loggedCollectionLimit] + "…(truncated)"
	}
	return strconv.Quote(collection)
}

func (rc *ReindexCleanup) activityHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		if r.Method != http.MethodGet {
			http.Error(w, "/reindex/cleanup-activity only serves GET", http.StatusMethodNotAllowed)
			return
		}

		collection := r.URL.Query().Get("collection")
		if collection == "" {
			http.Error(w, "collection query parameter is required", http.StatusBadRequest)
			return
		}

		// Never silently report "not cleaning up": a cancel's answer depends
		// on this, and a wrong "no" reopens the window it exists to close.
		var prober ReindexCleanupProber
		if rc.resolve != nil {
			prober = rc.resolve()
		}
		if prober == nil {
			http.Error(w, "reindex cleanup probe is not wired on this node", http.StatusServiceUnavailable)
			return
		}

		cleaningUp := prober.AnyCleanupInProgressForCollection(collection)
		if rc.logger != nil {
			// The cancelling node waits on this answer, so an operator tracing a
			// slow cancel needs to see that the question arrived and what it got.
			rc.logger.WithField("action", "reindex_cleanup_probe").
				WithField("collection", loggableCollection(collection)).
				WithField("cleaning_up", cleaningUp).
				Debug("reindex cleanup probe answered")
		}
		data, err := json.Marshal(clusterprobe.NewReindexCleanupActivity(cleaningUp))
		if err != nil {
			http.Error(w, fmt.Errorf("marshal response: %w", err).Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	}
}
