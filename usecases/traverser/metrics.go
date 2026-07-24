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

package traverser

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type Metrics struct {
	queriesAggregateCount *prometheus.GaugeVec
	queriesGetCount       *prometheus.GaugeVec
	queriesGetDurations   prometheus.ObserverVec
	dimensions            *prometheus.CounterVec
	dimensionsCombined    prometheus.Counter
	groupClasses          bool
}

func NewMetrics(prom *monitoring.PrometheusMetrics) *Metrics {
	if prom == nil {
		return nil
	}

	return &Metrics{
		queriesAggregateCount: prom.QueriesCount.MustCurryWith(prometheus.Labels{"query_type": "aggregate"}),
		queriesGetCount:       prom.QueriesCount.MustCurryWith(prometheus.Labels{"query_type": "get_graphql"}),
		queriesGetDurations:   prom.QueriesDurations.MustCurryWith(prometheus.Labels{"query_type": "get_graphql"}),
		dimensions:            prom.QueryDimensions,
		dimensionsCombined:    prom.QueryDimensionsCombined,
		groupClasses:          prom.Group,
	}
}

func (m *Metrics) QueriesAggregateInc(className string) {
	if m == nil {
		return
	}

	if m.groupClasses {
		className = "n/a"
	}

	m.queriesAggregateCount.WithLabelValues(className).Inc()
}

func (m *Metrics) QueriesAggregateDec(className string) {
	if m == nil {
		return
	}

	if m.groupClasses {
		className = "n/a"
	}

	m.queriesAggregateCount.WithLabelValues(className).Dec()
}

func (m *Metrics) QueriesGetInc(className string) {
	if m == nil {
		return
	}

	if m.groupClasses {
		className = "n/a"
	}

	m.queriesGetCount.WithLabelValues(className).Inc()
}

func (m *Metrics) QueriesObserveDuration(className string, startMs int64) {
	if m == nil {
		return
	}

	if m.groupClasses {
		className = "n/a"
	}

	took := float64(time.Now().UnixMilli() - startMs)

	m.queriesGetDurations.WithLabelValues(className).Observe(took)
}

func (m *Metrics) QueriesGetDec(className string) {
	if m == nil {
		return
	}

	if m.groupClasses {
		className = "n/a"
	}

	m.queriesGetCount.WithLabelValues(className).Dec()
}

func (m *Metrics) AddUsageDimensions(className, queryType, operation string, dims int) {
	if m == nil {
		return
	}

	if m.groupClasses {
		className = "n/a"
	}

	m.dimensions.WithLabelValues(queryType, operation, className).Add(float64(dims))
	m.dimensionsCombined.Add(float64(dims))
}
