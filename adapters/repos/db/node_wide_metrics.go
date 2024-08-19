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
)

type nodeWideMetricsObserver struct {
	db       *DB
	shutdown chan struct{}
}

func newNodeWideMetricsObserver(db *DB) *nodeWideMetricsObserver {
	return &nodeWideMetricsObserver{db: db, shutdown: make(chan struct{})}
}

func (o *nodeWideMetricsObserver) Start() {
	t := time.NewTicker(30 * time.Second)

	defer t.Stop()

	for {
		select {
		case <-o.shutdown:
			return
		case <-t.C:
			o.observeIfShardsReady()
		}
	}
}

func (o *nodeWideMetricsObserver) observeIfShardsReady() {
	o.db.indexLock.RLock()
	defer o.db.indexLock.RUnlock()

	allShardsReady := true

	for _, index := range o.db.indices {
		if !index.allShardsReady.Load() {
			allShardsReady = false
			break
		}
	}

	if !allShardsReady {
		o.db.logger.WithFields(logrus.Fields{
			"action": "skip_observe_node_wide_metrics",
		}).Debugf("skip node-wide metrics, not all shards ready")
		return
	}

	o.observeUnlocked()
}

// assumes that the caller already holds a db.indexLock.Rlock()
func (o *nodeWideMetricsObserver) observeUnlocked() {
	start := time.Now()

	totalObjectCount := 0
	for _, index := range o.db.indices {
		index.ForEachShard(func(name string, shard ShardLike) error {
			totalObjectCount += shard.ObjectCountAsync()
			return nil
		})
	}

	o.db.promMetrics.ObjectCount.With(prometheus.Labels{
		"class_name": "n/a",
		"shard_name": "n/a",
	}).Set(float64(totalObjectCount))

	took := time.Since(start)
	o.db.logger.WithFields(logrus.Fields{
		"action":       "observe_node_wide_metrics",
		"took":         took,
		"object_count": totalObjectCount,
	}).Debug("observed node wide metrics")
}

func (o *nodeWideMetricsObserver) Shutdown() {
	o.shutdown <- struct{}{}
}
