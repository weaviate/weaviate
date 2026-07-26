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

package roaringsetrange

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

var (
	batchDeleteOps = promauto.With(monitoring.GetMetrics().Registerer).NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      batchDeleteOpsName,
			Help: "Filtered batch deletes by how their victims were resolved, summed over " +
				"every shard in the process. cascade means the range cascade answered, so the " +
				"leaf cache may have served this delete an entry another operation built; " +
				"other means the filter resolved without it. Both children are conserved, one " +
				"increment per completed resolution, so a rate is meaningful. This is the only " +
				"reading of that routing available by default: the matching slow-query record " +
				"carries the per-operation detail but exists only once " +
				"QUERY_SLOW_LOG_ENABLED is on, and is sampled after that.",
		}, []string{"routed"})

	// Created eagerly so both read zero on a process that has never batch-deleted,
	// rather than one being absent and the other a reading.
	batchDeleteViaCascade = batchDeleteOps.WithLabelValues("cascade")
	batchDeleteOther      = batchDeleteOps.WithLabelValues("other")
)

// ObserveBatchDeleteRouting records how one delete's filter resolved. Exported
// because the delete path lives in package db, for the same reason
// DocBitmapAnnotation is: a non-query caller has no other way to say it went
// through the cascade.
func ObserveBatchDeleteRouting(viaCascade bool) {
	if viaCascade {
		batchDeleteViaCascade.Inc()
		return
	}
	batchDeleteOther.Inc()
}
