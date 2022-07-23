package objects

import (
	"time"

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

func (m *Metrics) BatchInc(className string) {
	if m == nil {
		return
	}

	m.queriesCount.With(prometheus.Labels{
		"class_name": className,
		"query_type": "batch",
	}).Inc()
}

func (m *Metrics) BatchDec(className string) {
	if m == nil {
		return
	}

	m.queriesCount.With(prometheus.Labels{
		"class_name": className,
		"query_type": "batch",
	}).Dec()
}

func (m *Metrics) BatchRefInc(className string) {
	if m == nil {
		return
	}

	m.queriesCount.With(prometheus.Labels{
		"class_name": className,
		"query_type": "batch_references",
	}).Inc()
}

func (m *Metrics) BatchRefDec(className string) {
	if m == nil {
		return
	}

	m.queriesCount.With(prometheus.Labels{
		"class_name": className,
		"query_type": "batch_references",
	}).Dec()
}

func (m *Metrics) BatchOp(op string, startNs int64) {
	if m == nil {
		return
	}

	took := float64(time.Now().UnixNano()-startNs) / float64(time.Millisecond)

	b.metrics.BatchTime.With(prometheus.Labels{
		"operation":  op,
		"class_name": "n/a",
		"shard_name": "n/a",
	}).Observe(float64(tookMs))
}
