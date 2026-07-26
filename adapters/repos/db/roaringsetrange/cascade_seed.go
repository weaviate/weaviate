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

// CascadeSeedDisabledEnv turns the seeded cascade off and restores the plane-0
// cascade. Read once at startup, so it needs a restart to take effect.
const CascadeSeedDisabledEnv = "QUERY_RANGEABLE_CASCADE_SEED_DISABLED"

var (
	cascadeSeedEnabled    bool
	cascadeSeedEnvValue   = os.Getenv(CascadeSeedDisabledEnv)
	cascadeSeedEnvUnknown bool
	cascadeSeedLogged     atomic.Bool
)

func init() {
	disabled, recognised := parseCascadeSeedDisabled(cascadeSeedEnvValue)
	cascadeSeedEnabled = !disabled
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

// parseCascadeSeedDisabled reports whether the kill switch is engaged. An
// unrecognised value leaves seeding on rather than failing to boot: a typo in
// a perf knob shouldn't take a node out of a cluster.
func parseCascadeSeedDisabled(v string) (disabled, recognised bool) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "":
		return false, true
	case "on", "enabled", "1", "true":
		return true, true
	case "off", "disabled", "0", "false":
		return false, true
	default:
		return false, false
	}
}

var (
	cascadeSeedConfig = promauto.With(monitoring.GetMetrics().Registerer).NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "lsm_roaringsetrange_cascade_seed_config",
			Help: "Which state " + CascadeSeedDisabledEnv + " left the seeded range cascade in, " +
				"readable at boot rather than only once a query exercises it. " +
				"unrecognised means the variable was set to something this build does not parse " +
				"and seeding stayed on; see the warning logged alongside it. " +
				"This reports the switch, not whether the cascade runs at all: " +
				"lsm_roaringsetrange_leaf_cache_config{state=\"disabled_feature_off\"} tells you " +
				"whether an in-memory segment exists for it to run in.",
		}, []string{"state"})

	cascadeSeedOps = promauto.With(monitoring.GetMetrics().Registerer).NewCounterVec(
		prometheus.CounterOpts{
			Name: "lsm_roaringsetrange_cascade_seed_total",
			Help: "Range-filter cascades by where they started. " +
				"seeded means the cascade started from the lowest set bit's plane; " +
				"disabled means " + CascadeSeedDisabledEnv + " engaged and it started from plane 0; " +
				"no_set_bit means the value had none and the cascade was a no-op. " +
				"All three read zero when the range cascade has not been exercised at all, " +
				"which is what distinguishes a disabled feature from an unused one.",
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
				CascadeSeedDisabledEnv, cascadeSeedEnvValue)
	case !cascadeSeedEnabled:
		logger.WithField("action", "roaringsetrange_cascade_seed").
			Infof("%s is set, the range cascade starts from plane 0", CascadeSeedDisabledEnv)
	}
}
