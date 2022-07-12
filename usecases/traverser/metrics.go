package traverser

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/semi-technologies/weaviate/usecases/monitoring"
)

type Metrics struct {
	queriesCount *prometheus.GaugeVec
}

func NewMetrics(prom *monitoring.PrometheusMetrics) *Metrics {
	if prom == nil {
		return nil
	}

	return &Metrics{
		queriesCount: prom.QueriesCount,
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
