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
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

type nodeWideMetricsObserver struct {
	db       *DB
	shutdown chan struct{}

	activityLock    sync.Mutex
	activityTracker map[string]map[string]int32
	lastTenantUsage map[string]map[string]time.Time
}

func newNodeWideMetricsObserver(db *DB) *nodeWideMetricsObserver {
	return &nodeWideMetricsObserver{db: db, shutdown: make(chan struct{})}
}

func (o *nodeWideMetricsObserver) Start() {
	t := time.NewTicker(30 * time.Second)

	// make sure we start with a warm state, otherwise we delay the initial
	// update by 30s. This only applies to tenant activity, other metrics wait
	// for shard-readiness anyway.
	o.observeActivity()

	defer t.Stop()

	for {
		select {
		case <-o.shutdown:
			return
		case <-t.C:
			o.observeActivity()
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

func (o *nodeWideMetricsObserver) observeActivity() {
	start := time.Now()
	current := o.getCurrentActivity()

	o.activityLock.Lock()
	defer o.activityLock.Unlock()

	o.lastTenantUsage = o.analyzeActivityDelta(current)
	o.activityTracker = current

	took := time.Since(start)
	o.db.logger.WithFields(logrus.Fields{
		"action": "observe_tenantactivity",
		"took":   took,
	}).Debug("observed tenant activity stats")
}

func (o *nodeWideMetricsObserver) analyzeActivityDelta(currentActivity map[string]map[string]int32) map[string]map[string]time.Time {
	previousActivity := o.activityTracker
	if previousActivity == nil {
		previousActivity = map[string]map[string]int32{}
	}

	now := time.Now()

	// create a new map, this way we will automatically drop anything that
	// doesn't appear in the new list anymore
	newUsage := map[string]map[string]time.Time{}

	for class, current := range currentActivity {
		// fmt.Printf("%s: %v\n", class, current)
		newUsage[class] = map[string]time.Time{}

		for tenant, act := range current {
			if _, ok := previousActivity[class]; !ok {
				previousActivity[class] = map[string]int32{}
			}

			previous, ok := previousActivity[class][tenant]
			if !ok {
				// this tenant didn't appear on the previous list, so we need to consider
				// it recently active
				newUsage[class][tenant] = now
				continue
			}

			if act == previous {
				// unchanged, we can copy the current state
				newUsage[class][tenant] = o.lastTenantUsage[class][tenant]
			} else {
				// activity changed we need to update it
				newUsage[class][tenant] = now
			}
		}
	}

	return newUsage
}

func (o *nodeWideMetricsObserver) getCurrentActivity() map[string]map[string]int32 {
	o.db.indexLock.RLock()
	defer o.db.indexLock.RUnlock()

	current := map[string]map[string]int32{}
	for _, index := range o.db.indices {
		if !index.partitioningEnabled {
			continue
		}
		cn := index.Config.ClassName.String()
		current[cn] = map[string]int32{}
		index.ForEachShard(func(name string, shard ShardLike) error {
			current[cn][name] = shard.Activity()
			return nil
		})
	}

	return current
}

func (o *nodeWideMetricsObserver) Usage() map[string]map[string]time.Time {
	o.activityLock.Lock()
	defer o.activityLock.Unlock()

	return o.lastTenantUsage
}
