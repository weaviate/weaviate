//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package moduletools

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/weaviate/weaviate/usecases/monitoring"
)

type Metrics interface {
	VectorizeRequestDurations(moduleName, operation, className string, startMs int64)
}

type moduleMetrics struct {
	metrics *monitoring.PrometheusMetrics
}

func newModuleMetrics(metrics *monitoring.PrometheusMetrics) *moduleMetrics {
	return &moduleMetrics{metrics}
}

func (m *moduleMetrics) VectorizeRequestDurations(moduleName, operation, className string, startMs int64) {
	if m.metrics == nil {
		return
	}

	took := float64(time.Now().UnixMilli() - startMs)

	m.metrics.VectorizeRequestDurations.With(prometheus.Labels{
		"module_name": moduleName,
		"operation":   operation,
		"class_name":  className,
	}).Observe(float64(took))
}
