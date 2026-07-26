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

package helpers

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/weaviate/weaviate/usecases/monitoring"
)

// Token states, counted on the coordinator, so an operator can confirm the
// kill switch engaged instead of inferring it from a latency curve.
const (
	QueryDedupeTokenMinted   = "minted"
	QueryDedupeTokenDisabled = "disabled"
)

// Per-leg outcomes, counted on the shard. Kept distinct so "legs never
// overlapped" can't be confused with "legs overlapped but sharing failed".
const (
	// AllowListDedupeShared means this leg was handed a reference to a build
	// another leg was already running.
	AllowListDedupeShared = "shared"
	// AllowListDedupeUnshared means this leg led the build and nobody joined it
	// before it published: dedupe was on, it just did not trigger.
	AllowListDedupeUnshared = "unshared"
	// AllowListDedupeFilterMismatch means a leg arrived under a token already in
	// flight for a different filter, so sharing was refused.
	AllowListDedupeFilterMismatch = "filter_mismatch"
	// AllowListDedupeLeaderFailed means the leg joined but the leader published
	// nothing shareable, so it fell back to its own build.
	AllowListDedupeLeaderFailed = "leader_failed"
	// AllowListDedupeCancelled means the leg's own context expired while it was
	// waiting on the leader.
	AllowListDedupeCancelled = "cancelled"
)

var (
	hybridDedupeTokens = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "weaviate",
		Name:      "hybrid_filter_dedupe_tokens_total",
		Help: "Filtered multi-leg hybrid queries by whether the coordinator minted a " +
			"dedupe token or the kill switch suppressed it. Counted once per query.",
	}, []string{"state"})

	allowListDedupe = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "weaviate",
		Name:      "filter_allow_list_dedupe_total",
		Help: "Filter allow-list builds carrying a dedupe token, by outcome. Counted " +
			"once per leg per shard.",
	}, []string{"outcome"})
)

func init() {
	// Create every child eagerly. Without this, "the feature was never
	// exercised" and "the feature is off" both render as a missing series.
	monitoring.InitCounterVec(hybridDedupeTokens, [][]string{
		{QueryDedupeTokenMinted},
		{QueryDedupeTokenDisabled},
	})
	monitoring.InitCounterVec(allowListDedupe, [][]string{
		{AllowListDedupeShared},
		{AllowListDedupeUnshared},
		{AllowListDedupeFilterMismatch},
		{AllowListDedupeLeaderFailed},
		{AllowListDedupeCancelled},
	})
}

// RecordQueryDedupeToken counts one coordinator decision about minting a dedupe
// token.
func RecordQueryDedupeToken(state string) {
	hybridDedupeTokens.WithLabelValues(state).Inc()
}

// RecordAllowListDedupe counts one tokenised allow-list build on a shard.
func RecordAllowListDedupe(outcome string) {
	allowListDedupe.WithLabelValues(outcome).Inc()
}
