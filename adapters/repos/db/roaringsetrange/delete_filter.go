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

// Label values for deleteFilterResolutions, named for the readers that
// answered rather than for how the process is configured. The middle one names
// the absence of the in-memory segment rather than a backing: the readers it
// stands for are the bucket's memtable plus however many range segments happen
// to be on disk, and that count is zero until the first flush.
const (
	routedRangeableInMemory          = "rangeable_in_memory"
	routedRangeableNoInMemorySegment = "rangeable_no_in_memory_segment"
	routedNonRangeable               = "non_rangeable"
)

var (
	deleteFilterResolutions = promauto.With(monitoring.GetMetrics().Registerer).NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      deleteFilterResolutionsName,
			Help: "Filters resolved to find the victims of a delete, by the readers that " +
				"answered, summed over every shard in the process: there is no class, shard or " +
				"property dimension. " + routedRangeableInMemory + " means the in-memory range " +
				"segment answered, which is the only place the leaf cache and the seeded " +
				"cascade live; " + routedRangeableNoInMemorySegment + " means the range index " +
				"answered without one, from the bucket's memtable and whatever range segments " +
				"it has flushed, which reach the same reader but hold no leaf cache — this " +
				"is what " + IndexRangeableInMemoryEnv + " off, the default, produces; " +
				routedNonRangeable + " means no range read ran at all. Those first two are " +
				"mutually exclusive and that variable picks which one a process can move; " +
				routedNonRangeable + " moves on either setting. " +
				routedRangeableInMemory + " says the memoisable path was traversed, not that a " +
				"memoised leaf was served: " + leafCacheOpsSeries + "{operation=\"hit\"} is " +
				"that reading, and " + leafCacheConfigSeries + " says whether a cache existed " +
				"to serve one — state=\"disabled_feature_off\" is why " +
				routedRangeableInMemory + " is flat on the default path rather than the process " +
				"being idle, and state=\"disabled_budget_zero\" is an in-memory segment with no " +
				"cache in it, which this counter cannot tell from a live one. " +
				routedRangeableNoInMemorySegment + " says that segment was absent, not that a " +
				"disk segment was read: until a collection first flushes it has no range " +
				"segment on disk and the memtable answers alone, and that counts here too. " +
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
	resolvedRangeableInMemory          = deleteFilterResolutions.WithLabelValues(routedRangeableInMemory)
	resolvedRangeableNoInMemorySegment = deleteFilterResolutions.WithLabelValues(routedRangeableNoInMemorySegment)
	resolvedNonRangeable               = deleteFilterResolutions.WithLabelValues(routedNonRangeable)
)

// ObserveDeleteFilterResolution records how one delete's filter resolved.
// Exported because the delete path lives in package db.
func ObserveDeleteFilterResolution(ctx context.Context) {
	switch readSourceFromContext(ctx) {
	case sourceInMemorySegment:
		resolvedRangeableInMemory.Inc()
	case sourceNoInMemorySegment:
		resolvedRangeableNoInMemorySegment.Inc()
	default:
		resolvedNonRangeable.Inc()
	}
}
