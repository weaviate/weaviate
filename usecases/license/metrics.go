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

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// allStatuses lists every reachable status so the gauge exposes a series for
// each: 1 for the current status, 0 for the others.
var allStatuses = []Status{StatusUnlicensed, StatusValid}

// RegisterMetrics registers the license metrics with reg and sets them to the
// given state. With monitoring disabled, reg is a no-op registerer and this
// silently does nothing.
func RegisterMetrics(reg prometheus.Registerer, state State) {
	status := promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "weaviate_license_status",
		Help: "License status of this instance (1 for the current status, 0 for the others).",
	}, []string{"status"})
	for _, s := range allStatuses {
		var v float64
		if s == state.Status {
			v = 1
		}
		status.WithLabelValues(string(s)).Set(v)
	}
}
