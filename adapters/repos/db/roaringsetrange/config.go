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
	// metricsNamespace matches the lsm_* convention in adapters/repos/db/lsmkv.
	// The bare lsm_bitmap_buffers_usage neighbour belongs to the legacy,
	// frozen PrometheusMetrics monolith and isn't the convention to follow.
	metricsNamespace = "weaviate"

	leafCacheConfigName   = "lsm_roaringsetrange_leaf_cache_config"
	leafCacheOpsName      = "lsm_roaringsetrange_leaf_cache_ops_total"
	cascadeSeedConfigName = "lsm_roaringsetrange_cascade_seed_config"
	cascadeSeedName       = "lsm_roaringsetrange_cascade_seed_total"
	// Named for the resolution, not the delete: it increments once the filter
	// resolves, before anything is removed, even if it matched nothing.
	deleteFilterResolutionsName = "lsm_roaringsetrange_delete_filter_resolutions_total"

	// Help text cross-references have to name the emitted series, which is what
	// a dashboard greps for, not the Name field the namespace is prepended to.
	leafCacheConfigSeries   = metricsNamespace + "_" + leafCacheConfigName
	leafCacheOpsSeries      = metricsNamespace + "_" + leafCacheOpsName
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

// parseBoolEnv classifies a value three ways, not entcfg.Enabled's two: it
// keeps a deliberate off distinct from an unrecognised value, so a typo isn't
// silently read as intentional.
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
			"Read it before " + leafCacheOpsSeries + " and " + metricsNamespace + "_" +
			cascadeSeedName + ", which are the liveness reading: on the default path they " +
			"stay flat for the same reason an idle cache does.",
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
// segment, and warns about any value it could not parse. It takes the feature
// flag rather than reading it off a segment, because on the default path no
// segment is ever constructed.
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
	logIndexRangeableConfig(featureEnabled, logger)
	logLeafCacheConfig(logger)
	logCascadeSeedConfig(logger)
}

// logIndexRangeableConfig names a value entcfg.Enabled silently dropped:
// entcfg.Enabled recognises only truthy words, so an unparseable value reads
// as unset with nothing logged. Scoped to naming it here rather than fixing
// entcfg.Enabled, which every caller in the repo shares.
//
// The outcome comes from featureEnabled, never from the value: the config file
// is parsed before FromEnv and FromEnv only ever switches the flag on, so the
// string cannot say where the feature ended up.
func logIndexRangeableConfig(featureEnabled bool, logger logrus.FieldLogger) {
	if logger == nil || indexRangeableEnvValue == "" ||
		!indexRangeableLogged.CompareAndSwap(false, true) {
		return
	}

	state := "off"
	if featureEnabled {
		state = "on"
	}
	entry := logger.WithField("action", "roaringsetrange_index_rangeable_in_memory")

	// parseBoolEnv trims and entcfg.Enabled does not, so " true" reads as an
	// intent this build understands against a feature that stayed off. Comparing
	// the intent to the resolved state surfaces that gap instead of hiding it,
	// which is why the two parsers are allowed to keep disagreeing.
	intent, recognised := parseBoolEnv(indexRangeableEnvValue)
	switch {
	case !recognised:
		entry.Warnf("%s=%q is not a recognised boolean and was dropped, the in-memory range segment is %s",
			IndexRangeableInMemoryEnv, indexRangeableEnvValue, state)
	case intent != featureEnabled:
		entry.Warnf("%s=%q did not take effect, the in-memory range segment is %s",
			IndexRangeableInMemoryEnv, indexRangeableEnvValue, state)
	}
}
