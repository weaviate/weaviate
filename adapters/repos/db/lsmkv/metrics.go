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

package lsmkv

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type (
	NsObserver   func(ns int64)
	Setter       func(val uint64)
	TimeObserver func(start time.Time)
)

type Metrics struct {
	CompactionReplace    *prometheus.GaugeVec
	CompactionSet        *prometheus.GaugeVec
	CompactionMap        *prometheus.GaugeVec
	CompactionRoaringSet *prometheus.GaugeVec
	ActiveSegments       *prometheus.GaugeVec
	bloomFilters         prometheus.ObserverVec
	SegmentObjects       *prometheus.GaugeVec
	SegmentSize          *prometheus.GaugeVec
	SegmentCount         *prometheus.GaugeVec
	startupDurations     prometheus.ObserverVec
	startupDiskIO        prometheus.ObserverVec
	objectCount          prometheus.Gauge
	memtableDurations    prometheus.ObserverVec
	memtableSize         *prometheus.GaugeVec
	DimensionSum         *prometheus.GaugeVec

	groupClasses bool
}

func NewMetrics(promMetrics *monitoring.PrometheusMetrics, className,
	shardName string,
) *Metrics {
	if promMetrics.Group {
		className = "n/a"
		shardName = "n/a"
	}

	replace := promMetrics.AsyncOperations.MustCurryWith(prometheus.Labels{
		"operation":  "compact_lsm_segments_stratreplace",
		"class_name": className,
		"shard_name": shardName,
	})

	set := promMetrics.AsyncOperations.MustCurryWith(prometheus.Labels{
		"operation":  "compact_lsm_segments_stratset",
		"class_name": className,
		"shard_name": shardName,
	})

	roaringSet := promMetrics.AsyncOperations.MustCurryWith(prometheus.Labels{
		"operation":  "compact_lsm_segments_stratroaringset",
		"class_name": className,
		"shard_name": shardName,
	})

	stratMap := promMetrics.AsyncOperations.MustCurryWith(prometheus.Labels{
		"operation":  "compact_lsm_segments_stratmap",
		"class_name": className,
		"shard_name": shardName,
	})

	return &Metrics{
		groupClasses:         promMetrics.Group,
		CompactionReplace:    replace,
		CompactionSet:        set,
		CompactionMap:        stratMap,
		CompactionRoaringSet: roaringSet,
		ActiveSegments: promMetrics.LSMSegmentCount.MustCurryWith(prometheus.Labels{
			"class_name": className,
			"shard_name": shardName,
		}),
		bloomFilters: promMetrics.LSMBloomFilters.MustCurryWith(prometheus.Labels{
			"class_name": className,
			"shard_name": shardName,
		}),
		SegmentObjects: promMetrics.LSMSegmentObjects.MustCurryWith(prometheus.Labels{
			"class_name": className,
			"shard_name": shardName,
		}),
		SegmentSize: promMetrics.LSMSegmentSize.MustCurryWith(prometheus.Labels{
			"class_name": className,
			"shard_name": shardName,
		}),
		SegmentCount: promMetrics.LSMSegmentCountByLevel.MustCurryWith(prometheus.Labels{
			"class_name": className,
			"shard_name": shardName,
		}),
		startupDiskIO: promMetrics.StartupDiskIO.MustCurryWith(prometheus.Labels{
			"class_name": className,
			"shard_name": shardName,
		}),
		startupDurations: promMetrics.StartupDurations.MustCurryWith(prometheus.Labels{
			"class_name": className,
			"shard_name": shardName,
		}),
		objectCount: promMetrics.ObjectCount.With(prometheus.Labels{
			"class_name": className,
			"shard_name": shardName,
		}),
		memtableDurations: promMetrics.LSMMemtableDurations.MustCurryWith(prometheus.Labels{
			"class_name": className,
			"shard_name": shardName,
		}),
		memtableSize: promMetrics.LSMMemtableSize.MustCurryWith(prometheus.Labels{
			"class_name": className,
			"shard_name": shardName,
		}),
		DimensionSum: promMetrics.VectorDimensionsSum.MustCurryWith(prometheus.Labels{
			"class_name": className,
			"shard_name": shardName,
		}),
	}
}

func noOpTimeObserver(start time.Time) {
	// do nothing
}

func noOpNsObserver(startNs int64) {
	// do nothing
}

func noOpSetter(val uint64) {
	// do nothing
}

func (m *Metrics) MemtableOpObserver(path, strategy, op string) NsObserver {
	if m == nil {
		return noOpNsObserver
	}

	if m.groupClasses {
		path = "n/a"
	}

	curried := m.memtableDurations.With(prometheus.Labels{
		"operation": op,
		"path":      path,
		"strategy":  strategy,
	})

	return func(startNs int64) {
		took := float64(time.Now().UnixNano()-startNs) / float64(time.Millisecond)
		curried.Observe(took)
	}
}

func (m *Metrics) MemtableSizeSetter(path, strategy string) Setter {
	if m == nil || m.groupClasses {
		// this metric would set absolute values, that's not possible in
		// grouped mode, each call would essentially overwrite the last
		return noOpSetter
	}

	curried := m.memtableSize.With(prometheus.Labels{
		"path":     path,
		"strategy": strategy,
	})

	return func(size uint64) {
		curried.Set(float64(size))
	}
}

func (m *Metrics) BloomFilterObserver(strategy, operation string) TimeObserver {
	if m == nil {
		return noOpTimeObserver
	}

	curried := m.bloomFilters.With(prometheus.Labels{
		"strategy":  strategy,
		"operation": operation,
	})

	return func(before time.Time) {
		curried.Observe(float64(time.Since(before)) / float64(time.Millisecond))
	}
}

func (m *Metrics) TrackStartupReadWALDiskIO(read int64, nanoseconds int64) {
	if m == nil {
		return
	}

	seconds := float64(nanoseconds) / float64(time.Second)
	throughput := float64(read) / float64(seconds)
	m.startupDiskIO.With(prometheus.Labels{"operation": "lsm_recover_wal"}).Observe(throughput)
}

func (m *Metrics) TrackStartupBucket(start time.Time) {
	if m == nil {
		return
	}

	took := float64(time.Since(start)) / float64(time.Millisecond)
	m.startupDurations.With(prometheus.Labels{"operation": "lsm_startup_bucket"}).Observe(took)
}

func (m *Metrics) TrackStartupBucketRecovery(start time.Time) {
	if m == nil {
		return
	}

	took := float64(time.Since(start)) / float64(time.Millisecond)
	m.startupDurations.With(prometheus.Labels{"operation": "lsm_startup_bucket_recovery"}).Observe(took)
}

func (m *Metrics) ObjectCount(count int) {
	if m == nil {
		return
	}

	m.objectCount.Set(float64(count))
}
