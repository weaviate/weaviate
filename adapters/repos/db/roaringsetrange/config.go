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

package roaringsetrange

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// leafCacheConfig states, one-hot. Ordered by what an operator should look at
// first: whether the segment exists at all, then whether its budget was
// understood, then whether it was deliberately switched off.
const (
	leafCacheStateFeatureOff  = "disabled_feature_off"
	leafCacheStateUnparseable = "unparseable"
	leafCacheStateBudgetZero  = "disabled_budget_zero"
	leafCacheStateEnabled     = "enabled"
)

var leafCacheStates = []string{
	leafCacheStateFeatureOff,
	leafCacheStateUnparseable,
	leafCacheStateBudgetZero,
	leafCacheStateEnabled,
}

var leafCacheConfig = promauto.With(monitoring.GetMetrics().Registerer).NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "lsm_roaringsetrange_leaf_cache_config",
		Help: "Effective state of the range-filter leaf cache, one child set to 1. " +
			"disabled_feature_off means INDEX_RANGEABLE_IN_MEMORY is off, so no in-memory " +
			"segment and therefore no cache is ever built — this is the default; " +
			"unparseable means " + LeafCacheMaxMemoryEnv + " was not understood and the " +
			"default budget is standing in; disabled_budget_zero means it resolved to 0; " +
			"enabled means the cache is live. Read this before the _ops_total counters: " +
			"on the default path they stay flat for the same reason an idle cache does.",
	}, []string{"state"})

// PublishConfig records how this process is configured for the in-memory range
// segment, and warns about any value it could not parse.
//
// It takes the feature flag and runs at startup rather than from
// NewSegmentInMemory, because the default configuration never constructs an
// in-memory segment. On that path every per-segment counter stays flat for the
// same reason an idle cache does, and a mistyped budget produced no log line at
// all — so the state most deployments are actually in was the one state that
// could not be read.
func PublishConfig(featureEnabled bool, logger logrus.FieldLogger) {
	for _, state := range leafCacheStates {
		leafCacheConfig.WithLabelValues(state).Set(0)
	}

	switch {
	case !featureEnabled:
		leafCacheConfig.WithLabelValues(leafCacheStateFeatureOff).Set(1)
	case leafCacheEnvErr != nil:
		leafCacheConfig.WithLabelValues(leafCacheStateUnparseable).Set(1)
	case leafCacheMaxMemory <= 0:
		leafCacheConfig.WithLabelValues(leafCacheStateBudgetZero).Set(1)
	default:
		leafCacheConfig.WithLabelValues(leafCacheStateEnabled).Set(1)
	}

	// Both warnings fire regardless of the feature flag: a value this build
	// cannot parse is an operator mistake worth naming even where it is inert,
	// and an operator who sets one of these has already decided the feature
	// matters to them.
	logLeafCacheConfig(logger)
	logCascadeSeedConfig(logger)
}
