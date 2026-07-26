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
	"os"
	"strings"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

const (
	// metricsNamespace prefixes every series this package emits. It matches
	// adapters/repos/db/lsmkv, where the current lsm_* convention lives and which
	// is prefixed throughout. The bare lsm_bitmap_buffers_usage in the same
	// subsystem belongs to the legacy PrometheusMetrics monolith, which carries
	// its own instruction not to add metrics to it.
	metricsNamespace = "weaviate"

	leafCacheConfigName   = "lsm_roaringsetrange_leaf_cache_config"
	leafCacheOpsName      = "lsm_roaringsetrange_leaf_cache_ops_total"
	cascadeSeedConfigName = "lsm_roaringsetrange_cascade_seed_config"
	cascadeSeedOpsName    = "lsm_roaringsetrange_cascade_seed_total"

	// Help text cross-references have to name the emitted series, which is what
	// a dashboard greps for, not the Name field the namespace is prepended to.
	leafCacheConfigSeries   = metricsNamespace + "_" + leafCacheConfigName
	cascadeSeedConfigSeries = metricsNamespace + "_" + cascadeSeedConfigName
)

// IndexRangeableInMemoryEnv gates the in-memory range segment, and with it
// everything else in this file. Parsed into config by usecases/config; read
// here only to report a value that parse silently dropped.
const IndexRangeableInMemoryEnv = "INDEX_RANGEABLE_IN_MEMORY"

var (
	indexRangeableEnvValue = os.Getenv(IndexRangeableInMemoryEnv)
	indexRangeableLogged   atomic.Bool
)

// parseBoolEnv classifies a boolean env value three ways where entcfg.Enabled
// classifies it two: it tells "the operator asked for off" apart from "this
// build did not understand the value". Collapsing those two makes a typo
// indistinguishable from a deliberate off, with nothing logged either way.
func parseBoolEnv(v string) (value, recognised bool) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "on", "enabled", "1", "true":
		return true, true
	case "off", "disabled", "0", "false":
		return false, true
	default:
		return false, false
	}
}

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
		Namespace: metricsNamespace,
		Name:      leafCacheConfigName,
		Help: "How this process is configured for the range-filter leaf cache, one child " +
			"set to 1. disabled_feature_off means " + IndexRangeableInMemoryEnv + " is off, so " +
			"no in-memory segment and therefore no cache is ever built — this is the default; " +
			"unparseable means " + LeafCacheMaxMemoryEnv + " was not understood and the " +
			"default budget is standing in; disabled_budget_zero means it resolved to 0; " +
			"enabled means nothing is switched off, so a cache is built for each in-memory " +
			"segment as one appears. This reports the switches, not whether any cache is " +
			"live: it reads enabled from boot, before a collection exists to hold a segment. " +
			"Read it before the _ops_total counters, which are the liveness reading: on the " +
			"default path they stay flat for the same reason an idle cache does.",
	}, []string{"state"})

// Children exist from process start, so a scrape landing before db.New reads
// all-zero rather than an absent series — the same reason every counter in this
// subsystem is created eagerly.
func init() {
	for _, state := range leafCacheStates {
		leafCacheConfig.WithLabelValues(state).Set(0)
	}
}

// PublishConfig records how this process is configured for the in-memory range
// segment, and warns about any value it could not parse.
//
// It takes the feature flag and runs at startup rather than from
// NewSegmentInMemory, because the default configuration never constructs an
// in-memory segment. On that path every per-segment counter stays flat for the
// same reason an idle cache does, so without this the state most deployments
// are in is the one state nothing reports.
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

	// All three warnings fire regardless of the feature flag: a value this build
	// cannot parse is an operator mistake worth naming even where it is inert,
	// and an operator who sets one of these has already decided the feature
	// matters to them.
	logIndexRangeableConfig(logger)
	logLeafCacheConfig(logger)
	logCascadeSeedConfig(logger)
}

// logIndexRangeableConfig names a value entcfg.Enabled dropped. It recognises
// only truthy words, so INDEX_RANGEABLE_IN_MEMORY=yes is indistinguishable from
// unset and from false: feature off, gauge disabled_feature_off, no line saying
// the value was why. The feature's other two knobs both report an unparsed
// value, so the one gating them should too.
//
// Scoped to naming the value here. Correcting entcfg.Enabled would change
// behaviour for every caller in the repo, which is a separate decision.
func logIndexRangeableConfig(logger logrus.FieldLogger) {
	if logger == nil || indexRangeableEnvValue == "" ||
		!indexRangeableLogged.CompareAndSwap(false, true) {
		return
	}

	if _, recognised := parseBoolEnv(indexRangeableEnvValue); !recognised {
		logger.WithField("action", "roaringsetrange_index_rangeable_in_memory").
			Warnf("%s=%q is not a recognised boolean, the in-memory range segment stays off",
				IndexRangeableInMemoryEnv, indexRangeableEnvValue)
	}
}
