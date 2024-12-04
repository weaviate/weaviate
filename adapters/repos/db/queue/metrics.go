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

package queue

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type Metrics struct {
	logger                      *logrus.Entry
	baseMetrics                 *monitoring.PrometheusMetrics
	monitoring                  bool
	queueSize                   prometheus.Gauge
	queueDiskUsage              prometheus.Gauge
	queuesPaused                prometheus.Gauge
	queuesCount                 prometheus.Gauge
	partitionProcessingDuration prometheus.Observer
}

func NewMetrics(
	logger logrus.FieldLogger,
	prom *monitoring.PrometheusMetrics,
	labels prometheus.Labels,
) *Metrics {
	m := Metrics{
		logger: logger.WithField("monitoring", "queue"),
	}

	if prom == nil {
		return &m
	}

	m.baseMetrics = prom

	m.monitoring = true

	m.queueSize = prom.QueueSize.With(labels)
	m.queueDiskUsage = prom.QueueDiskUsage.With(labels)
	m.queuesPaused = prom.QueuePaused.With(labels)
	m.queuesCount = prom.QueueCount.With(labels)
	m.partitionProcessingDuration = prom.QueuePartitionProcessingDuration.With(labels)

	return &m
}

func (m *Metrics) Paused(id string) {
	m.logger.WithField("action", "queue_pause").
		WithField("queue_id", id).
		Trace("index queue paused")

	if !m.monitoring {
		return
	}

	m.queuesPaused.Inc()
}

func (m *Metrics) Resumed(id string) {
	m.logger.WithField("action", "queue_resume").
		WithField("queue_id", id).
		Trace("index queue resumed")

	if !m.monitoring {
		return
	}

	m.queuesPaused.Dec()
}

func (m *Metrics) Registered(id string) {
	m.logger.WithField("action", "queue_register").
		WithField("queue_id", id).
		Trace("queue registered")

	if !m.monitoring {
		return
	}

	m.queuesCount.Inc()
}

func (m *Metrics) Unregistered(id string) {
	m.logger.WithField("action", "queue_unregister").
		WithField("queue_id", id).
		Trace("queue unregistered")

	if !m.monitoring {
		return
	}

	m.queuesCount.Dec()
}

func (m *Metrics) TasksProcessed(start time.Time, count int) {
	took := time.Since(start)
	m.logger.WithField("action", "dispatch_queue").
		WithField("partition_size", count).
		WithField("took", took).
		Tracef("partition processed by worker in %s", took)

	if !m.monitoring {
		return
	}

	m.partitionProcessingDuration.Observe(float64(took.Milliseconds()))
}

func (m *Metrics) Size(size uint64) {
	m.logger.WithField("size", size).Tracef("queue size %d", size)

	if !m.monitoring {
		return
	}

	m.queueSize.Set(float64(size))
}

func (m *Metrics) DiskUsage(size int64) {
	m.logger.WithField("disk_usage", size).Tracef("disk usage of queue %d", size)

	if !m.monitoring {
		return
	}

	m.queueDiskUsage.Set(float64(size))
}
