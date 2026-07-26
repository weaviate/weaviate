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
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// Label values for deleteFilterResolutions. Each names the backing that
// answered the filter, which is a fact about the read that happened rather
// than about how the process is configured, so none of them can tick on a
// deployment where the thing it names does not exist.
const (
	routedRangeableInMemory = "rangeable_in_memory"
	routedRangeableOnDisk   = "rangeable_on_disk"
	routedNonRangeable      = "non_rangeable"
)

var (
	deleteFilterResolutions = promauto.With(monitoring.GetMetrics().Registerer).NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      deleteFilterResolutionsName,
			Help: "Filters resolved to find the victims of a delete, by the backing that " +
				"answered, summed over every shard in the process: there is no class, shard or " +
				"property dimension. " + routedRangeableInMemory + " means the in-memory range " +
				"segment answered, which is the only place the leaf cache and the seeded " +
				"cascade live; " + routedRangeableOnDisk + " means the range index answered " +
				"from disk segments, which reach the same reader but hold no leaf cache — this " +
				"is what " + IndexRangeableInMemoryEnv + " off, the default, produces; " +
				routedNonRangeable + " means no range read ran at all. " +
				routedRangeableInMemory + " says the memoisable path was traversed, not that a " +
				"memoised leaf was served: " + leafCacheOpsSeries + "{operation=\"hit\"} is " +
				"that reading, and " + leafCacheConfigSeries + " says whether a cache existed " +
				"to serve one — state=\"disabled_feature_off\" is why " +
				routedRangeableInMemory + " is flat on the default path rather than the process " +
				"being idle, and state=\"disabled_budget_zero\" is an in-memory segment with no " +
				"cache in it, which this counter cannot tell from a live one. " +
				"It counts resolutions, not deletes. The increment fires once the filter has " +
				"resolved, before anything is removed, so a resolution matching zero objects " +
				"counts and so does one whose delete is then cancelled having removed nothing. " +
				"Both producers reach it, a batch delete and the object-TTL sweep, and a delete " +
				"fanned out across replicas resolves once per shard on every node that serves " +
				"it. No count of deleted objects and no count of user requests can be derived " +
				"from it. A filter that failed to resolve is not counted at all. The matching " +
				"slow-query record carries the per-operation detail but exists only once " +
				"QUERY_SLOW_LOG_ENABLED is on, and is sampled after that.",
		}, []string{"routed"})

	// Created eagerly so every child reads zero on a process that has never
	// deleted, rather than being absent and indistinguishable from idle.
	resolvedRangeableInMemory = deleteFilterResolutions.WithLabelValues(routedRangeableInMemory)
	resolvedRangeableOnDisk   = deleteFilterResolutions.WithLabelValues(routedRangeableOnDisk)
	resolvedNonRangeable      = deleteFilterResolutions.WithLabelValues(routedNonRangeable)
)

// ObserveDeleteFilterResolution records how one delete's filter resolved,
// reading the annotations the range readers left in ctx. Exported because the
// delete path lives in package db, for the same reason DocBitmapAnnotation is:
// a non-query caller has no other way to ask.
func ObserveDeleteFilterResolution(ctx context.Context) {
	switch readSourceFromContext(ctx) {
	case sourceInMemorySegment:
		resolvedRangeableInMemory.Inc()
	case sourceDiskSegments:
		resolvedRangeableOnDisk.Inc()
	default:
		resolvedNonRangeable.Inc()
	}
}
