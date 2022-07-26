//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package lsmkv

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/semi-technologies/weaviate/usecases/monitoring"
)

type NsObserver func(ns int64)

type Metrics struct {
	CompactionReplace *prometheus.GaugeVec
	CompactionSet     *prometheus.GaugeVec
	CompactionMap     *prometheus.GaugeVec
	ActiveSegments    *prometheus.GaugeVec
	BloomFilters      prometheus.ObserverVec
	SegmentObjects    *prometheus.GaugeVec
	SegmentSize       *prometheus.GaugeVec
	SegmentCount      *prometheus.GaugeVec
	startupDurations  prometheus.ObserverVec
	startupDiskIO     prometheus.ObserverVec
	objectCount       prometheus.Gauge
	memtableDurations prometheus.ObserverVec
	memtableSize      *prometheus.GaugeVec
}

func NewMetrics(promMetrics *monitoring.PrometheusMetrics, className,
	shardName string) *Metrics {
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

	stratMap := promMetrics.AsyncOperations.MustCurryWith(prometheus.Labels{
		"operation":  "compact_lsm_segments_stratmap",
		"class_name": className,
		"shard_name": shardName,
	})

	return &Metrics{
		CompactionReplace: replace,
		CompactionSet:     set,
		CompactionMap:     stratMap,
		ActiveSegments: promMetrics.LSMSegmentCount.MustCurryWith(prometheus.Labels{
			"class_name": className,
			"shard_name": shardName,
		}),
		BloomFilters: promMetrics.LSMBloomFilters.MustCurryWith(prometheus.Labels{
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
	}
}

func (m *Metrics) MemtableSize(path, strategy string, size uint64) {
	if m == nil {
		return
	}

	m.memtableSize.With(prometheus.Labels{
		"path":     path,
		"strategy": strategy,
	}).Set(float64(size))
}

// func (m *Metrics) MemtableOp(path, strategy, op string, startNs int64) {
// 	if m == nil {
// 		return
// 	}

// 	took := float64(time.Now().UnixNano()-startNs) / float64(time.Millisecond)

// 	m.memtableDurations.With(prometheus.Labels{
// 		"operation": op,
// 		"path":      path,
// 		"strategy":  strategy,
// 	}).Observe(took)
// }

func noOpNsObserver(startNs int64) {
	return
}

func (m *Metrics) MemtableOpObserver(path, strategy, op string) NsObserver {
	if m == nil {
		return noOpNsObserver
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
