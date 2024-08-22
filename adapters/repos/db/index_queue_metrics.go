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

package db

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type IndexQueueMetrics struct {
	logger          *logrus.Entry
	monitoring      bool
	pushDuration    prometheus.Observer
	deleteDuration  prometheus.Observer
	preloadDuration prometheus.Observer
	preloadCount    prometheus.Gauge
	searchDuration  prometheus.Observer
	paused          prometheus.Gauge
	size            prometheus.Gauge
	staleCount      prometheus.Counter
	vectorsDequeued prometheus.Gauge
	waitDuration    prometheus.Observer
	grouped         bool
	baseMetrics     *monitoring.PrometheusMetrics
}

func NewIndexQueueMetrics(
	logger logrus.FieldLogger, prom *monitoring.PrometheusMetrics,
	className, shardName string, targetVector string,
) *IndexQueueMetrics {
	m := &IndexQueueMetrics{
		logger: logger.WithField("monitoring", "index_queue"),
	}

	if prom == nil {
		return m
	}

	m.baseMetrics = prom

	if prom.Group {
		className = "n/a"
		shardName = "n/a"
		targetVector = "n/a"
		m.grouped = true
	}

	m.monitoring = true

	m.pushDuration = prom.IndexQueuePushDuration.With(prometheus.Labels{
		"class_name":    className,
		"shard_name":    shardName,
		"target_vector": targetVector,
	})
	m.deleteDuration = prom.IndexQueueDeleteDuration.With(prometheus.Labels{
		"class_name":    className,
		"shard_name":    shardName,
		"target_vector": targetVector,
	})
	m.preloadDuration = prom.IndexQueuePreloadDuration.With(prometheus.Labels{
		"class_name":    className,
		"shard_name":    shardName,
		"target_vector": targetVector,
	})
	m.preloadCount = prom.IndexQueuePreloadCount.With(prometheus.Labels{
		"class_name":    className,
		"shard_name":    shardName,
		"target_vector": targetVector,
	})
	m.searchDuration = prom.IndexQueueSearchDuration.With(prometheus.Labels{
		"class_name":    className,
		"shard_name":    shardName,
		"target_vector": targetVector,
	})
	m.paused = prom.IndexQueuePaused.With(prometheus.Labels{
		"class_name":    className,
		"shard_name":    shardName,
		"target_vector": targetVector,
	})
	m.size = prom.IndexQueueSize.With(prometheus.Labels{
		"class_name":    className,
		"shard_name":    shardName,
		"target_vector": targetVector,
	})
	m.staleCount = prom.IndexQueueStaleCount.With(prometheus.Labels{
		"class_name":    className,
		"shard_name":    shardName,
		"target_vector": targetVector,
	})
	m.vectorsDequeued = prom.IndexQueueVectorsDequeued.With(prometheus.Labels{
		"class_name":    className,
		"shard_name":    shardName,
		"target_vector": targetVector,
	})
	m.waitDuration = prom.IndexQueueWaitDuration.With(prometheus.Labels{
		"class_name":    className,
		"shard_name":    shardName,
		"target_vector": targetVector,
	})

	return m
}

func (m *IndexQueueMetrics) DeleteShardLabels(class, shard string) {
	if m.grouped {
		// never delete the shared label, only individual ones
		return
	}

	m.baseMetrics.DeleteShard(class, shard)
}

func (m *IndexQueueMetrics) Push(start time.Time, size int) {
	took := time.Since(start)
	m.logger.WithField("action", "push").
		WithField("batch_size", size).
		WithField("took", took).
		Tracef("add batch to index queue took %s", took)

	if !m.monitoring {
		return
	}

	m.pushDuration.Observe(float64(took / time.Millisecond))
}

func (m *IndexQueueMetrics) Delete(start time.Time, count int) {
	took := time.Since(start)
	m.logger.WithField("action", "delete").
		WithField("took", took).
		WithField("count", count).
		Tracef("deleting vectors took %s", took)

	if !m.monitoring {
		return
	}

	m.deleteDuration.Observe(float64(took / time.Millisecond))
}

func (m *IndexQueueMetrics) Preload(start time.Time, count int) {
	took := time.Since(start)
	m.logger.WithField("action", "preload").
		WithField("took", took).
		Tracef("preloading vectors took %s", took)

	if !m.monitoring {
		return
	}

	m.preloadDuration.Observe(float64(took / time.Millisecond))
	m.preloadCount.Set(float64(count))
}

func (m *IndexQueueMetrics) Search(start time.Time) {
	took := time.Since(start)
	m.logger.WithField("action", "search").
		WithField("took", took).
		Tracef("search took %s", took)

	if !m.monitoring {
		return
	}

	m.searchDuration.Observe(float64(took / time.Millisecond))
}

func (m *IndexQueueMetrics) Paused() {
	m.logger.WithField("action", "pause").
		Trace("index queue paused")

	if !m.monitoring {
		return
	}

	m.paused.Set(1)
}

func (m *IndexQueueMetrics) Resumed() {
	m.logger.WithField("action", "resume").
		Trace("index queue resumed")

	if !m.monitoring {
		return
	}

	m.paused.Set(0)
}

func (m *IndexQueueMetrics) Size(size int64) {
	m.logger.WithField("size", size).Tracef("queue size %d", size)

	if !m.monitoring {
		return
	}

	m.size.Set(float64(size))
}

func (m *IndexQueueMetrics) Stale() {
	m.logger.Trace("queue stale")

	if !m.monitoring {
		return
	}

	m.staleCount.Inc()
}

func (m *IndexQueueMetrics) VectorsDequeued(count int64) {
	m.logger.WithField("vectors_dequeued", count).
		Tracef("number of vectors sent to the workers per tick")

	if !m.monitoring {
		return
	}

	m.vectorsDequeued.Set(float64(count))
}

func (m *IndexQueueMetrics) Wait(start time.Time) {
	took := time.Since(start)
	m.logger.WithField("action", "wait").
		WithField("took", took).
		Tracef("waiting for workers to index chunks took %s", took)

	if !m.monitoring {
		return
	}

	m.waitDuration.Observe(float64(took / time.Millisecond))
}
