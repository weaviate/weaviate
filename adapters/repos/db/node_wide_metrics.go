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
	"github.com/weaviate/weaviate/entities/tenantactivity"
)

type nodeWideMetricsObserver struct {
	db       *DB
	shutdown chan struct{}

	activityLock    sync.Mutex
	activityTracker activityByCollection
	lastTenantUsage tenantactivity.ByCollection
}

// internal types used for tenant activity aggregation, not exposed to the user
type (
	activityByCollection map[string]activityByTenant
	activityByTenant     map[string]int32
)

func newNodeWideMetricsObserver(db *DB) *nodeWideMetricsObserver {
	return &nodeWideMetricsObserver{db: db, shutdown: make(chan struct{})}
}

func (o *nodeWideMetricsObserver) Start() {
	t30 := time.NewTicker(30 * time.Second)
	t10 := time.NewTicker(10 * time.Second)

	// make sure we start with a warm state, otherwise we delay the initial
	// update. This only applies to tenant activity, other metrics wait
	// for shard-readiness anyway.
	o.observeActivity()

	defer t30.Stop()
	defer t10.Stop()

	for {
		select {
		case <-o.shutdown:
			return
		case <-t10.C:
			o.observeActivity()
		case <-t30.C:
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

func (o *nodeWideMetricsObserver) analyzeActivityDelta(currentActivity activityByCollection) tenantactivity.ByCollection {
	previousActivity := o.activityTracker
	if previousActivity == nil {
		previousActivity = make(activityByCollection)
	}

	now := time.Now()

	// create a new map, this way we will automatically drop anything that
	// doesn't appear in the new list anymore
	newUsage := make(tenantactivity.ByCollection)

	for class, current := range currentActivity {
		newUsage[class] = make(tenantactivity.ByTenant)

		for tenant, act := range current {
			if _, ok := previousActivity[class]; !ok {
				previousActivity[class] = make(activityByTenant)
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

func (o *nodeWideMetricsObserver) getCurrentActivity() activityByCollection {
	o.db.indexLock.RLock()
	defer o.db.indexLock.RUnlock()

	current := make(activityByCollection)
	for _, index := range o.db.indices {
		if !index.partitioningEnabled {
			continue
		}
		cn := index.Config.ClassName.String()
		current[cn] = make(activityByTenant)
		index.ForEachShard(func(name string, shard ShardLike) error {
			current[cn][name] = shard.Activity()
			return nil
		})
	}

	return current
}

func (o *nodeWideMetricsObserver) Usage() tenantactivity.ByCollection {
	if o == nil {
		// not loaded yet, requests could come in before the db is initialized yet
		// don't attempt to lock, as that would lead to a nil-pointer issue
		return tenantactivity.ByCollection{}
	}

	o.activityLock.Lock()
	defer o.activityLock.Unlock()

	return o.lastTenantUsage
}
