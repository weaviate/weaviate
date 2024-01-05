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

package traverser

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type Metrics struct {
	queriesCount       *prometheus.GaugeVec
	queriesDurations   *prometheus.HistogramVec
	dimensions         *prometheus.CounterVec
	dimensionsCombined prometheus.Counter
	groupClasses       bool
}

func NewMetrics(prom *monitoring.PrometheusMetrics) *Metrics {
	if prom == nil {
		return nil
	}

	return &Metrics{
		queriesCount:       prom.QueriesCount,
		queriesDurations:   prom.QueriesDurations,
		dimensions:         prom.QueryDimensions,
		dimensionsCombined: prom.QueryDimensionsCombined,
		groupClasses:       prom.Group,
	}
}

func (m *Metrics) QueriesAggregateInc(className string) {
	if m == nil {
		return
	}

	if m.groupClasses {
		className = "n/a"
	}

	m.queriesCount.With(prometheus.Labels{
		"class_name": className,
		"query_type": "aggregate",
	}).Inc()
}

func (m *Metrics) QueriesAggregateDec(className string) {
	if m == nil {
		return
	}

	if m.groupClasses {
		className = "n/a"
	}

	m.queriesCount.With(prometheus.Labels{
		"class_name": className,
		"query_type": "aggregate",
	}).Dec()
}

func (m *Metrics) QueriesGetInc(className string) {
	if m == nil {
		return
	}

	if m.groupClasses {
		className = "n/a"
	}

	m.queriesCount.With(prometheus.Labels{
		"class_name": className,
		"query_type": "get_graphql",
	}).Inc()
}

func (m *Metrics) QueriesObserveDuration(className string, startMs int64) {
	if m == nil {
		return
	}

	if m.groupClasses {
		className = "n/a"
	}

	took := float64(time.Now().UnixMilli() - startMs)

	m.queriesDurations.With(prometheus.Labels{
		"class_name": className,
		"query_type": "get_graphql",
	}).Observe(float64(took))
}

func (m *Metrics) QueriesGetDec(className string) {
	if m == nil {
		return
	}

	if m.groupClasses {
		className = "n/a"
	}

	m.queriesCount.With(prometheus.Labels{
		"class_name": className,
		"query_type": "get_graphql",
	}).Dec()
}

func (m *Metrics) AddUsageDimensions(className, queryType, operation string, dims int) {
	if m == nil {
		return
	}

	if m.groupClasses {
		className = "n/a"
	}

	m.dimensions.With(prometheus.Labels{
		"class_name": className,
		"operation":  operation,
		"query_type": queryType,
	}).Add(float64(dims))
	m.dimensionsCombined.Add(float64(dims))
}
