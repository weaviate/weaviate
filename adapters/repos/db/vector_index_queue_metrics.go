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
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type VectorIndexQueueMetrics struct {
	logger       *logrus.Entry
	baseMetrics  *monitoring.PrometheusMetrics
	monitoring   bool
	insertCount  prometheus.Counter
	deleteCount  prometheus.Counter
	grouped      bool
	className    string
	shardName    string
	targetVector string
}

func NewVectorIndexQueueMetrics(
	logger logrus.FieldLogger, prom *monitoring.PrometheusMetrics,
	className, shardName string, targetVector string,
) *VectorIndexQueueMetrics {
	m := &VectorIndexQueueMetrics{
		logger:       logger.WithField("monitoring", "index_queue"),
		className:    className,
		shardName:    shardName,
		targetVector: targetVector,
	}

	if prom == nil {
		return m
	}

	m.baseMetrics = prom

	if prom.Group {
		m.className = "n/a"
		m.shardName = "n/a"
		m.targetVector = "n/a"
		m.grouped = true
	}

	m.monitoring = true

	m.insertCount = prom.VectorIndexQueueInsertCount.With(prometheus.Labels{
		"class_name":    m.className,
		"shard_name":    m.shardName,
		"target_vector": m.targetVector,
	})
	m.deleteCount = prom.VectorIndexQueueDeleteCount.With(prometheus.Labels{
		"class_name":    m.className,
		"shard_name":    m.shardName,
		"target_vector": m.targetVector,
	})

	return m
}

func (m *VectorIndexQueueMetrics) QueueMetrics() *queue.Metrics {
	return queue.NewMetrics(
		m.logger,
		m.baseMetrics,
		prometheus.Labels{
			"class_name": m.className,
			"shard_name": m.shardName,
		},
	)
}

func (m *VectorIndexQueueMetrics) DeleteShardLabels(class, shard string) {
	if !m.monitoring {
		return
	}

	if m.grouped {
		// never delete the shared label, only individual ones
		return
	}

	m.baseMetrics.DeleteShard(class, shard)
}

func (m *VectorIndexQueueMetrics) Insert(start time.Time, count int) {
	took := time.Since(start)
	m.logger.WithField("action", "insert").
		WithField("batch_size", count).
		WithField("took", took).
		Tracef("push insert operations to vector index queue took %s", took)

	if !m.monitoring {
		return
	}

	m.insertCount.Add(float64(count))
}

func (m *VectorIndexQueueMetrics) Delete(start time.Time, count int) {
	took := time.Since(start)
	m.logger.WithField("action", "delete").
		WithField("batch_size", count).
		WithField("took", took).
		Tracef("push delete operations to vector index queue took %s", took)

	if !m.monitoring {
		return
	}

	m.deleteCount.Add(float64(count))
}
