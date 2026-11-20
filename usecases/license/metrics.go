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

package license

import "github.com/prometheus/client_golang/prometheus"

// allStatuses lists every reachable status so the gauge exposes a series for
// each: 1 for the current status, 0 for the others.
var allStatuses = []Status{
	StatusUnlicensed, StatusValid, StatusExpired, StatusRevoked,
	StatusUnknown, StatusUnreachable, StatusDegraded,
}

type metricsCollector struct {
	provider  StateProvider
	status    *prometheus.Desc
	expiresAt *prometheus.Desc
}

func newMetricsCollector(provider StateProvider) *metricsCollector {
	return &metricsCollector{
		provider: provider,
		status: prometheus.NewDesc(
			"weaviate_license_status",
			"License status of this instance (1 for the current status, 0 for the others).",
			[]string{"edition", "status"}, nil,
		),
		expiresAt: prometheus.NewDesc(
			"weaviate_license_expires_at_seconds",
			"License expiry as Unix epoch seconds (0 if unknown).",
			nil, nil,
		),
	}
}

func (c *metricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.status
	ch <- c.expiresAt
}

func (c *metricsCollector) Collect(ch chan<- prometheus.Metric) {
	state := c.provider.Current()
	edition := string(state.Edition())
	for _, status := range allStatuses {
		var value float64
		if status == state.Status {
			value = 1
		}
		ch <- prometheus.MustNewConstMetric(c.status, prometheus.GaugeValue, value, edition, string(status))
	}

	var expiresAt float64
	if !state.ExpiresAt.IsZero() {
		expiresAt = float64(state.ExpiresAt.Unix())
	}
	ch <- prometheus.MustNewConstMetric(c.expiresAt, prometheus.GaugeValue, expiresAt)
}

// RegisterMetrics registers a collector that reads the current license state
// once per scrape. With monitoring disabled, reg is a no-op registerer and
// this silently does nothing.
func RegisterMetrics(reg prometheus.Registerer, provider StateProvider) {
	reg.MustRegister(newMetricsCollector(provider))
}
