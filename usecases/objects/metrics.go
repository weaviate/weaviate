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

func (m *Metrics) makeQueriesIncFunc(queryType string) func() {
	return func() {
		if m == nil {
			return
		}

		m.queriesCount.With(prometheus.Labels{
			"class_name": "n/a",
			"query_type": queryType,
		}).Inc()
	}
}

func (m *Metrics) makeQueriesDecFunc(queryType string) func() {
	return func() {
		if m == nil {
			return
		}

		m.queriesCount.With(prometheus.Labels{
			"class_name": "n/a",
			"query_type": queryType,
		}).Dec()
	}
}

func (m *Metrics) BatchInc() {
	m.makeQueriesIncFunc("batch")
}

func (m *Metrics) BatchDec() {
	m.makeQueriesDecFunc("batch")
}

func (m *Metrics) BatchRefInc() {
	m.makeQueriesIncFunc("batch_references")
}

func (m *Metrics) BatchRefDec() {
	m.makeQueriesDecFunc("batch_references")
}

func (m *Metrics) BatchDeleteInc() {
	m.makeQueriesIncFunc("batch_delete")
}

func (m *Metrics) BatchDeleteDec() {
	m.makeQueriesDecFunc("batch_delete")
}

func (m *Metrics) AddObjectInc() {
	m.makeQueriesIncFunc("add_object")
}

func (m *Metrics) AddObjectDec() {
	m.makeQueriesDecFunc("add_object")
}

func (m *Metrics) UpdateObjectInc() {
	m.makeQueriesIncFunc("update_object")
}

func (m *Metrics) UpdateObjectDec() {
	m.makeQueriesDecFunc("update_object")
}

func (m *Metrics) MergeObjectInc() {
	m.makeQueriesIncFunc("merge_object")
}

func (m *Metrics) MergeObjectDec() {
	m.makeQueriesDecFunc("merge_object")
}

func (m *Metrics) DeleteObjectInc() {
	m.makeQueriesIncFunc("delete_object")
}

func (m *Metrics) DeleteObjectDec() {
	m.makeQueriesDecFunc("delete_object")
}

func (m *Metrics) GetObjectInc() {
	m.makeQueriesIncFunc("get_object")
}

func (m *Metrics) GetObjectDec() {
	m.makeQueriesDecFunc("get_object")
}

func (m *Metrics) HeadObjectInc() {
	m.makeQueriesIncFunc("head_object")
}

func (m *Metrics) HeadObjectDec() {
	m.makeQueriesDecFunc("head_object")
}

func (m *Metrics) AddReferenceInc() {
	m.makeQueriesIncFunc("add_reference")
}

func (m *Metrics) AddReferenceDec() {
	m.makeQueriesDecFunc("add_reference")
}

func (m *Metrics) UpdateReferenceInc() {
	m.makeQueriesIncFunc("update_reference")
}

func (m *Metrics) UpdateReferenceDec() {
	m.makeQueriesDecFunc("update_reference")
}

func (m *Metrics) DeleteReferenceInc() {
	m.makeQueriesIncFunc("delete_reference")
}

func (m *Metrics) DeleteReferenceDec() {
	m.makeQueriesDecFunc("delete_reference")
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
