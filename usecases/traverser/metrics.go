//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package traverser

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/semi-technologies/weaviate/usecases/monitoring"
)

type Metrics struct {
	queriesCount *prometheus.GaugeVec
	dimensions   *prometheus.CounterVec
}

func NewMetrics(prom *monitoring.PrometheusMetrics) *Metrics {
	if prom == nil {
		return nil
	}

	return &Metrics{
		queriesCount: prom.QueriesCount,
		dimensions:   prom.QueryDimensions,
	}
}

func (m *Metrics) QueriesAggregateInc(className string) {
	if m == nil {
		return
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

	m.queriesCount.With(prometheus.Labels{
		"class_name": className,
		"query_type": "aggregate",
	}).Dec()
}

func (m *Metrics) QueriesGetInc(className string) {
	if m == nil {
		return
	}

	m.queriesCount.With(prometheus.Labels{
		"class_name": className,
		"query_type": "get_graphql",
	}).Inc()
}

func (m *Metrics) QueriesGetDec(className string) {
	if m == nil {
		return
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

	m.dimensions.With(prometheus.Labels{
		"class_name": className,
		"operation":  operation,
		"query_type": queryType,
	}).Add(float64(dims))
}
