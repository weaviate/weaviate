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
	"reflect"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
)

// ReindexCleanupProber answers whether this node is still tearing down reindex
// sidecars for a collection.
type ReindexCleanupProber interface {
	AnyCleanupInProgressForCollection(collection string) bool
}

// ReindexCleanupActivity is the answer to "have you raised your gate yet".
type ReindexCleanupActivity struct {
	CleaningUp bool `json:"cleaningUp"`
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
	// Both fields below are concrete pointers and must be compared as such
	// before they are boxed; see isNilProber.
	var logger logrus.FieldLogger
	if appState != nil && appState.Logger != nil {
		logger = appState.Logger
	}
	return NewReindexCleanup(func() ReindexCleanupProber {
		if appState == nil || appState.ReindexProvider == nil {
			return nil
		}
		return appState.ReindexProvider
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

// isNilProber reports whether the interface holds nothing callable, including
// a nil pointer boxed into it — which a plain == nil misses, and which reaches
// this route whenever a caller passes an unset field straight through.
func isNilProber(prober ReindexCleanupProber) bool {
	if prober == nil {
		return true
	}
	v := reflect.ValueOf(prober)
	switch v.Kind() {
	case reflect.Ptr, reflect.Interface, reflect.Map, reflect.Slice, reflect.Func:
		return v.IsNil()
	default:
		return false
	}
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
		var prober ReindexCleanupProber
		if rc.resolve != nil {
			prober = rc.resolve()
		}
		if isNilProber(prober) {
			http.Error(w, "reindex cleanup probe is not wired on this node", http.StatusServiceUnavailable)
			return
		}

		cleaningUp := prober.AnyCleanupInProgressForCollection(collection)
		if rc.logger != nil {
			// The cancelling node waits on this answer, so an operator tracing a
			// slow cancel needs to see that the question arrived and what it got.
			rc.logger.WithField("action", "reindex_cleanup_probe").
				WithField("collection", collection).
				WithField("cleaning_up", cleaningUp).
				Debug("reindex cleanup probe answered")
		}
		data, err := json.Marshal(ReindexCleanupActivity{CleaningUp: cleaningUp})
		if err != nil {
			http.Error(w, fmt.Errorf("marshal response: %w", err).Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	}
}
