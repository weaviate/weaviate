//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/tenantactivity"
	"github.com/weaviate/weaviate/usecases/config"
)

type nodeWideMetricsObserver struct {
	db *DB

	// Goroutines spawned by nodeWideMetricsObserver must exit after receiving on this channel.
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

// Start goroutines for periodically polling node-wide metrics.
// Shard read/write activity and objects_count are only collected
// if metric aggregation (PROMETHEUS_MONITORING_GROUP) is enabled.
// Only start this service if DB has Prometheus enabled.
func (o *nodeWideMetricsObserver) Start() {
	// Prometheus metrics are redundant with Usage Module is enabled
	if o.db.config.TrackVectorDimensions && !o.db.config.UsageEnabled {
		o.observeDimensionMetrics()
	}

	if o.db.promMetrics.Group {
		o.observeShards()
	}
}

func (o *nodeWideMetricsObserver) Shutdown() {
	close(o.shutdown)
}

func (o *nodeWideMetricsObserver) observeShards() {
	enterrors.GoWrapper(func() {
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
	}, o.db.logger)
}

// Call observeUnlocked iff all shards in the local indices are loaded.
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

// Collect object_count metric for every shard.
// Assumes that all shards are loaded and that
// the caller already holds a db.indexLock.Rlock().
func (o *nodeWideMetricsObserver) observeUnlocked() {
	start := time.Now()

	totalObjectCount := int64(0)
	for _, index := range o.db.indices {
		index.ForEachShard(func(name string, shard ShardLike) error {
			objectCount, err := shard.ObjectCountAsync(context.Background())
			if err != nil {
				o.db.logger.Warnf("error while getting object count for shard %s: %w", shard.Name(), err)
			}
			totalObjectCount += objectCount
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

// NOTE(dyma): should this also chech that all indices report allShardsReady == true?
// Otherwise getCurrentActivity may end up loading lazy-loaded shards just to check
// their activity, which is redundant on a cold shard?
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

// ----------------------------------------------------------------------------
// Vector dimensions tracking
// ----------------------------------------------------------------------------

// Start a goroutine to collect vector dimension/segment metrics from the shards,
// and publish them at a regular interval. Only call this method in the constructor,
// as it does not guard access with locks.
// If vector dimension tracking is disabled, this method is a no-op: no goroutine will
// be started and the "done" channel stays nil.
func (o *nodeWideMetricsObserver) observeDimensionMetrics() {
	enterrors.GoWrapper(func() {
		interval := config.DefaultTrackVectorDimensionsInterval
		if o.db.config.TrackVectorDimensionsInterval > 0 { // duration must be > 0, or time.Timer will panic
			interval = o.db.config.TrackVectorDimensionsInterval
		}

		// This is a low-priority background process, which is not time-sensitive.
		// Some downstream calls require a context, so we create one, but we needn't
		// manage it beyond making sure it doesn't leak.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		o.publishVectorMetrics(ctx)

		tick := time.NewTicker(interval)
		defer tick.Stop()

		for {
			select {
			case <-o.shutdown:
				return
			case <-tick.C:
				o.publishVectorMetrics(ctx)
			}
		}
	}, o.db.logger)
}

func (o *nodeWideMetricsObserver) publishVectorMetrics(ctx context.Context) {
	// NOTE(dyma): can we copy the index map and release lock immediately?
	// No new indices can be added while we're holding the lock anyways,
	// there are no metrics to loose.
	//
	// If in case some indices are shut down / dropped after copy, we can
	// always check something like:
	//
	// i.closeLock.Lock()
	// defer i.closeLock.Unlock()
	// if i.closed {
	// 	continue // skip
	// }
	// maybe even TryLock(), because locked means it's about to be closed.

	o.db.indexLock.RLock()
	defer o.db.indexLock.RUnlock()

	var total DimensionMetrics
	for _, index := range o.db.indices {
		className := index.Config.ClassName.String()

		// Avoid loading cold shards, as it may create I/O spikes.
		index.ForEachLoadedShard(func(shardName string, sl ShardLike) error {
			var shard *Shard
			switch s := sl.(type) {
			case *Shard:
				shard = s
			case *LazyLoadShard:
				shard = s.shard // this shard
			default:
				// Actually we could calculate dimensions on any ShardLike
				// but as Shard is currently the only implementation that
				// reports vector metrics, we will pretend that we can't.
				return nil
			}

			dim := shard.getTotalDimensionMetrics(ctx)

			// Aggregate metrics instead of reporting them per-shard if grouping is enabled.
			if o.db.promMetrics.Group {
				total = total.Add(dim)
			} else {
				o.sendVectorDimensions(className, shardName, dim)
			}
			return nil
		})
	}

	// Report aggregate metrics for the node if grouping is enabled.
	if o.db.promMetrics.Group {
		o.sendVectorDimensions("n/a", "n/a", total)
	}
}

// Set vector_dimensions=DimensionMetrics.Uncompressed and vector_segments=DimensionMetrics.Compressed gauges.
func (o *nodeWideMetricsObserver) sendVectorDimensions(className, shardName string, dm DimensionMetrics) {
	if g, err := o.db.promMetrics.VectorDimensionsSum.GetMetricWithLabelValues(className, shardName); err == nil {
		g.Set(float64(dm.Uncompressed))
	}

	if g, err := o.db.promMetrics.VectorSegmentsSum.GetMetricWithLabelValues(className, shardName); err == nil {
		g.Set(float64(dm.Compressed))
	}
}
