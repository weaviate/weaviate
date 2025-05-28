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
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/tenantactivity"
)

type nodeWideMetricsObserver struct {
	db       *DB
	shutdown chan struct{}

	activityLock          sync.Mutex
	activityTracker       activityByCollection
	lastTenantUsage       tenantactivity.ByCollection
	lastTenantUsageReads  tenantactivity.ByCollection
	lastTenantUsageWrites tenantactivity.ByCollection
}

// internal types used for tenant activity aggregation, not exposed to the user
type (
	activityByCollection map[string]activityByTenant
	activityByTenant     map[string]activity
	activity             struct {
		read  int32
		write int32
	}
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

	o.lastTenantUsage, o.lastTenantUsageReads, o.lastTenantUsageWrites = o.analyzeActivityDelta(current)
	o.activityTracker = current

	took := time.Since(start)
	o.db.logger.WithFields(logrus.Fields{
		"action": "observe_tenantactivity",
		"took":   took,
	}).Debug("observed tenant activity stats")
}

func (o *nodeWideMetricsObserver) logActivity(col, tenant, activityType string, value int32) {
	logBase := o.db.logger.WithFields(logrus.Fields{
		"action":             "tenant_activity_change",
		"collection":         col,
		"tenant":             tenant,
		"activity_type":      activityType,
		"last_counter_value": value,
	})

	var lvlStr string
	switch activityType {
	case "read":
		lvlStr = o.db.config.TenantActivityReadLogLevel.Get()
	case "write":
		lvlStr = o.db.config.TenantActivityWriteLogLevel.Get()
	default:
		lvlStr = "debug" // fall-back for any unknown activityType
	}

	level, err := logrus.ParseLevel(strings.ToLower(lvlStr))
	if err != nil {
		level = logrus.DebugLevel
		logBase.WithField("invalid_level", lvlStr).
			Warn("unknown tenant activity log level, defaulting to debug")
	}

	logBase.Logf(level, "tenant %s activity change: %s", tenant, activityType)
}

func (o *nodeWideMetricsObserver) analyzeActivityDelta(currentActivity activityByCollection) (total, reads, writes tenantactivity.ByCollection) {
	previousActivity := o.activityTracker
	if previousActivity == nil {
		previousActivity = make(activityByCollection)
	}

	now := time.Now()

	// create a new map, this way we will automatically drop anything that
	// doesn't appear in the new list anymore
	newUsageTotal := make(tenantactivity.ByCollection)
	newUsageReads := make(tenantactivity.ByCollection)
	newUsageWrites := make(tenantactivity.ByCollection)

	for class, current := range currentActivity {
		newUsageTotal[class] = make(tenantactivity.ByTenant)
		newUsageReads[class] = make(tenantactivity.ByTenant)
		newUsageWrites[class] = make(tenantactivity.ByTenant)

		for tenant, act := range current {
			if _, ok := previousActivity[class]; !ok {
				previousActivity[class] = make(activityByTenant)
			}

			previous, ok := previousActivity[class][tenant]
			if !ok {
				// this tenant didn't appear on the previous list, so we need to consider
				// it recently active
				newUsageTotal[class][tenant] = now

				// only track detailed value if the value is greater than the initial
				// value, otherwise we consider it just an activation without any user
				// activity
				if act.read > 1 {
					newUsageReads[class][tenant] = now
					o.logActivity(class, tenant, "read", act.read)
				}
				if act.write > 1 {
					newUsageWrites[class][tenant] = now
					o.logActivity(class, tenant, "write", act.write)
				}

				if act.read == 1 && act.write == 1 {
					// no specific activity, just an activation
					o.logActivity(class, tenant, "activation", 1)
				}
				continue
			}

			if act.read == previous.read && act.write == previous.write {
				// unchanged, we can copy the current state
				newUsageTotal[class][tenant] = o.lastTenantUsage[class][tenant]

				// only copy previous reads+writes if they existed before
				if lastRead, ok := o.lastTenantUsageReads[class][tenant]; ok {
					newUsageReads[class][tenant] = lastRead
				}
				if lastWrite, ok := o.lastTenantUsageWrites[class][tenant]; ok {
					newUsageWrites[class][tenant] = lastWrite
				}
			} else {
				// activity changed we need to update it
				newUsageTotal[class][tenant] = now
				if act.read > previous.read {
					newUsageReads[class][tenant] = now
					o.logActivity(class, tenant, "read", act.read)
				} else if lastRead, ok := o.lastTenantUsageReads[class][tenant]; ok {
					newUsageReads[class][tenant] = lastRead
				}

				if act.write > previous.write {
					newUsageWrites[class][tenant] = now
					o.logActivity(class, tenant, "write", act.write)
				} else if lastWrite, ok := o.lastTenantUsageWrites[class][tenant]; ok {
					newUsageWrites[class][tenant] = lastWrite
				}

			}
		}
	}

	return newUsageTotal, newUsageReads, newUsageWrites
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
			act := activity{}
			act.read, act.write = shard.Activity()
			current[cn][name] = act
			return nil
		})
	}

	return current
}

func (o *nodeWideMetricsObserver) Usage(filter tenantactivity.UsageFilter) tenantactivity.ByCollection {
	if o == nil {
		// not loaded yet, requests could come in before the db is initialized yet
		// don't attempt to lock, as that would lead to a nil-pointer issue
		return tenantactivity.ByCollection{}
	}

	o.activityLock.Lock()
	defer o.activityLock.Unlock()

	switch filter {

	case tenantactivity.UsageFilterOnlyReads:
		return o.lastTenantUsageReads
	case tenantactivity.UsageFilterOnlyWrites:
		return o.lastTenantUsageWrites
	case tenantactivity.UsageFilterAll:
		return o.lastTenantUsage
	default:
		return o.lastTenantUsage
	}
}
