package objects

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/semi-technologies/weaviate/usecases/monitoring"
)

type Metrics struct {
	queriesCount *prometheus.GaugeVec
	batchTime    *prometheus.HistogramVec
}

func NewMetrics(prom *monitoring.PrometheusMetrics) *Metrics {
	if prom == nil {
		return nil
	}

	return &Metrics{
		queriesCount: prom.QueriesCount,
		batchTime:    prom.BatchTime,
	}
}

func (m *Metrics) BatchInc() {
	if m == nil {
		return
	}

	m.queriesCount.With(prometheus.Labels{
		"class_name": "n/a",
		"query_type": "batch",
	}).Inc()
}

func (m *Metrics) BatchDec() {
	if m == nil {
		return
	}

	m.queriesCount.With(prometheus.Labels{
		"class_name": "n/a",
		"query_type": "batch",
	}).Dec()
}

func (m *Metrics) BatchRefInc() {
	if m == nil {
		return
	}

	m.queriesCount.With(prometheus.Labels{
		"class_name": "n/a",
		"query_type": "batch_references",
	}).Inc()
}

func (m *Metrics) BatchRefDec() {
	if m == nil {
		return
	}

	m.queriesCount.With(prometheus.Labels{
		"class_name": "n/a",
		"query_type": "batch_references",
	}).Dec()
}

func (m *Metrics) BatchOp(op string, startNs int64) {
	if m == nil {
		return
	}

	took := float64(time.Now().UnixNano()-startNs) / float64(time.Millisecond)

	m.batchTime.With(prometheus.Labels{
		"operation":  op,
		"class_name": "n/a",
		"shard_name": "n/a",
	}).Observe(float64(took))
}
