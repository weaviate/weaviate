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

// CascadeSeedEnabledEnv switches the seeded cascade off and restores the
// plane-0 cascade. Defaults to on, and is read once at startup, so it needs a
// restart to take effect.
//
// Named positively so the value an operator reaches for to switch seeding off
// means that. A *_DISABLED name cannot: its own trailing word, as a value,
// parses as falsy and leaves the feature on.
const CascadeSeedEnabledEnv = "QUERY_RANGEABLE_CASCADE_SEED_ENABLED"

var (
	cascadeSeedEnabled    bool
	cascadeSeedEnvValue   = os.Getenv(CascadeSeedEnabledEnv)
	cascadeSeedEnvUnknown bool
	cascadeSeedLogged     atomic.Bool
)

func init() {
	enabled, recognised := parseCascadeSeedEnabled(cascadeSeedEnvValue)
	cascadeSeedEnabled = enabled
	cascadeSeedEnvUnknown = !recognised
	publishCascadeSeedConfig()
}

// publishCascadeSeedConfig makes the switch state readable at boot, before any
// query exercises the cascade.
func publishCascadeSeedConfig() {
	for _, state := range []string{"enabled", "disabled", "unrecognised"} {
		cascadeSeedConfig.WithLabelValues(state).Set(0)
	}

	switch {
	case cascadeSeedEnvUnknown:
		cascadeSeedConfig.WithLabelValues("unrecognised").Set(1)
	case cascadeSeedEnabled:
		cascadeSeedConfig.WithLabelValues("enabled").Set(1)
	default:
		cascadeSeedConfig.WithLabelValues("disabled").Set(1)
	}
}

// parseCascadeSeedEnabled reports whether seeding stays on. Unset means on, and
// an unrecognised value leaves it on rather than failing to boot: a typo in a
// perf knob shouldn't take a node out of a cluster.
func parseCascadeSeedEnabled(v string) (enabled, recognised bool) {
	if strings.TrimSpace(v) == "" {
		return true, true
	}
	enabled, recognised = parseBoolEnv(v)
	if !recognised {
		return true, false
	}
	return enabled, true
}

var (
	cascadeSeedConfig = promauto.With(monitoring.GetMetrics().Registerer).NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      cascadeSeedConfigName,
			Help: "Which state " + CascadeSeedEnabledEnv + " left the seeded range cascade in, " +
				"readable at boot rather than only once a query exercises it. " +
				"unrecognised means the variable was set to something this build does not parse " +
				"and seeding stayed on; see the warning logged alongside it. " +
				"This reports the switch, not whether the cascade runs at all: " +
				leafCacheConfigSeries + "{state=\"disabled_feature_off\"} tells you " +
				"whether an in-memory segment exists for it to run in.",
		}, []string{"state"})

	cascadeSeedOps = promauto.With(monitoring.GetMetrics().Registerer).NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      cascadeSeedOpsName,
			Help: "Range-filter cascades by where they started, summed over every rangeable " +
				"bucket in the process: there is no class, shard or property dimension. " +
				"seeded means the cascade started from the lowest set bit's plane; " +
				"disabled means " + CascadeSeedEnabledEnv + " switched seeding off and it " +
				"started from plane 0; no_set_bit means the value had none and the cascade " +
				"was a no-op. Only the in-memory range segment reaches these counters, so all " +
				"three read zero both when " + IndexRangeableInMemoryEnv + " is off and when " +
				"it is on with no range filters running: read " + cascadeSeedConfigSeries +
				" for the switch, and these for the traffic.",
		}, []string{"outcome"})

	// Created eagerly so a disabled-and-exercised process is a non-zero counter
	// rather than an absent series, which is indistinguishable from an idle one.
	cascadeSeedSeeded   = cascadeSeedOps.WithLabelValues("seeded")
	cascadeSeedDisabled = cascadeSeedOps.WithLabelValues("disabled")
	cascadeSeedNoSetBit = cascadeSeedOps.WithLabelValues("no_set_bit")
)

// observeCascadeSeed records where a cascade started. Cache hits aren't
// counted: no cascade runs for them.
func observeCascadeSeed(start cascadeStart) {
	switch {
	case start.narrowed:
		cascadeSeedSeeded.Inc()
	case !cascadeSeedEnabled:
		cascadeSeedDisabled.Inc()
	default:
		cascadeSeedNoSetBit.Inc()
	}
}

// logCascadeSeedConfig logs the seeding configuration once per process, so an
// operator can confirm the kill switch engaged or spot an unrecognised value.
func logCascadeSeedConfig(logger logrus.FieldLogger) {
	if logger == nil || !cascadeSeedLogged.CompareAndSwap(false, true) {
		return
	}

	switch {
	case cascadeSeedEnvUnknown:
		logger.WithField("action", "roaringsetrange_cascade_seed").
			Warnf("%s=%q is not a recognised boolean, keeping the seeded range cascade enabled",
				CascadeSeedEnabledEnv, cascadeSeedEnvValue)
	case !cascadeSeedEnabled:
		logger.WithField("action", "roaringsetrange_cascade_seed").
			Infof("%s=%q, the range cascade starts from plane 0",
				CascadeSeedEnabledEnv, cascadeSeedEnvValue)
	}
}
