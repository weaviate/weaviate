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
	"unicode/utf8"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/clusterprobe"
)

// ReindexCleanupProber answers whether this node has seen a cancel for the
// collection or is still tearing down its reindex sidecars. This is the
// confirmation signal a cancelling node waits on. It blocks nothing itself.
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

// Activity handles GET /reindex/cleanup-activity?collection=<name>.
//
// Its own route rather than a query param on /backups/node-activity: an older
// build would ignore an unknown param and answer a misleading "not busy"
// instead of 404ing.
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
		// Cut on a rune boundary so the kept part doesn't end in an escaped half rune.
		cut := loggedCollectionLimit
		for cut > 0 && !utf8.RuneStart(collection[cut]) {
			cut--
		}
		collection = collection[:cut] + "…(truncated)"
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

		// An operator tracing a slow cancel needs this logged on every path
		// the request can leave by, not just the success path.
		log := rc.logger.WithField("action", "reindex_cleanup_probe").
			WithField("collection", loggableCollection(collection))

		// Never silently report "not cleaning up": a cancel's answer depends
		// on this, and a wrong "no" reopens the window it exists to close.
		var prober ReindexCleanupProber
		if rc.resolve != nil {
			prober = rc.resolve()
		}
		if prober == nil {
			// The sentinel body lets the caller tell this permanent 503 apart
			// from a transient one; see [clusterprobe.ProbeNotWiredMarker].
			log.Debug("reindex cleanup probe answered: not wired on this node")
			http.Error(w, clusterprobe.ProbeNotWiredMarker, http.StatusServiceUnavailable)
			return
		}

		cleaningUp := prober.AnyCleanupInProgressForCollection(collection)
		log.WithField("cleaning_up", cleaningUp).Debug("reindex cleanup probe answered")
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
