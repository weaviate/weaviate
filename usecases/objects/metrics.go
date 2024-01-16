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

package objects

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type Metrics struct {
	queriesCount       *prometheus.GaugeVec
	batchTime          *prometheus.HistogramVec
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
		batchTime:          prom.BatchTime,
		dimensions:         prom.QueryDimensions,
		dimensionsCombined: prom.QueryDimensionsCombined,
		groupClasses:       prom.Group,
	}
}

func (m *Metrics) queriesInc(queryType string) {
	if m == nil {
		return
	}

	m.queriesCount.With(prometheus.Labels{
		"class_name": "n/a",
		"query_type": queryType,
	}).Inc()
}

func (m *Metrics) queriesDec(queryType string) {
	if m == nil {
		return
	}

	m.queriesCount.With(prometheus.Labels{
		"class_name": "n/a",
		"query_type": queryType,
	}).Dec()
}

func (m *Metrics) BatchInc() {
	m.queriesInc("batch")
}

func (m *Metrics) BatchDec() {
	m.queriesDec("batch")
}

func (m *Metrics) BatchRefInc() {
	m.queriesInc("batch_references")
}

func (m *Metrics) BatchRefDec() {
	m.queriesDec("batch_references")
}

func (m *Metrics) BatchDeleteInc() {
	m.queriesInc("batch_delete")
}

func (m *Metrics) BatchDeleteDec() {
	m.queriesDec("batch_delete")
}

func (m *Metrics) AddObjectInc() {
	m.queriesInc("add_object")
}

func (m *Metrics) AddObjectDec() {
	m.queriesDec("add_object")
}

func (m *Metrics) UpdateObjectInc() {
	m.queriesInc("update_object")
}

func (m *Metrics) UpdateObjectDec() {
	m.queriesDec("update_object")
}

func (m *Metrics) MergeObjectInc() {
	m.queriesInc("merge_object")
}

func (m *Metrics) MergeObjectDec() {
	m.queriesDec("merge_object")
}

func (m *Metrics) DeleteObjectInc() {
	m.queriesInc("delete_object")
}

func (m *Metrics) DeleteObjectDec() {
	m.queriesDec("delete_object")
}

func (m *Metrics) GetObjectInc() {
	m.queriesInc("get_object")
}

func (m *Metrics) GetObjectDec() {
	m.queriesDec("get_object")
}

func (m *Metrics) HeadObjectInc() {
	m.queriesInc("head_object")
}

func (m *Metrics) HeadObjectDec() {
	m.queriesDec("head_object")
}

func (m *Metrics) AddReferenceInc() {
	m.queriesInc("add_reference")
}

func (m *Metrics) AddReferenceDec() {
	m.queriesDec("add_reference")
}

func (m *Metrics) UpdateReferenceInc() {
	m.queriesInc("update_reference")
}

func (m *Metrics) UpdateReferenceDec() {
	m.queriesDec("update_reference")
}

func (m *Metrics) DeleteReferenceInc() {
	m.queriesInc("delete_reference")
}

func (m *Metrics) DeleteReferenceDec() {
	m.queriesDec("delete_reference")
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
