//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type Metrics struct {
	collectionsCount prometheus.Gauge
	enabled          bool
}

func NewMetrics(prom *monitoring.PrometheusMetrics) *Metrics {
	if prom == nil {
		return &Metrics{}
	}

	return &Metrics{
		collectionsCount: prom.CollectionsCount,
		enabled:          true,
	}
}

func (m *Metrics) collectionsCountInc() {
	if m.enabled {
		m.collectionsCount.Inc()
	}
}

func (m *Metrics) collectionsCountDec() {
	if m.enabled {
		m.collectionsCount.Dec()
	}
}
