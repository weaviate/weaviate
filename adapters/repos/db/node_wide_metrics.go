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
	"maps"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
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
	if o.db.config.TrackVectorDimensions {
		enterrors.GoWrapper(o.observeDimensionMetrics, o.db.logger)
	}

	if o.db.promMetrics.Group {
		enterrors.GoWrapper(o.observeShards, o.db.logger)
	}
}

func (o *nodeWideMetricsObserver) Shutdown() {
	close(o.shutdown)
}

func (o *nodeWideMetricsObserver) observeShards() {
	// make sure we start with a warm state, otherwise we delay the initial
	// update. This only applies to tenant activity, other metrics wait
	// for shard-readiness anyway.
	o.observeActivity()

	t30 := time.NewTicker(30 * time.Second)
	defer t30.Stop()

	t10 := time.NewTicker(10 * time.Second)
	defer t10.Stop()

	for {
		select {
		case <-o.shutdown:
			return
		case <-t10.C:
			o.observeActivity()
		case <-t30.C:
			o.observeObjectCount()
		}
	}
}

// Collect and publish aggregated object_count metric iff all indices report allShardsReady=true.
func (o *nodeWideMetricsObserver) observeObjectCount() {
	o.db.indexLock.RLock()
	defer o.db.indexLock.RUnlock()

	for _, index := range o.db.indices {
		if !index.allShardsReady.Load() {
			o.db.logger.WithFields(logrus.Fields{
				"action": "skip_observe_node_wide_metrics",
			}).Debugf("skip node-wide metrics, not all shards ready")
			return
		}
	}

	start := time.Now()

	totalObjectCount := int64(0)
	for _, index := range o.db.indices {
		index.ForEachShard(func(name string, shard ShardLike) error {
			index.shardCreateLocks.RLock(name)
			defer index.shardCreateLocks.RUnlock(name)
			exists, err := index.tenantDirExists(name)
			if err != nil {
				o.db.logger.
					WithField("action", "observe_node_wide_metrics").
					WithField("shard", name).
					WithField("class", index.Config.ClassName).
					Warnf("error while checking if shard exists: %v", err)
				return nil
			}
			if !exists {
				// shard was deleted in the meantime or is newly created and hasn't been written to disk, skip
				return nil
			}
			objectCount, err := shard.ObjectCountAsync(context.Background())
			if err != nil {
				o.db.logger.WithField("action", "observe_node_wide_metrics").
					WithField("shard", name).
					WithField("class", index.Config.ClassName).
					Warnf("error while getting object count for shard: %v", err)
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
			index.shardCreateLocks.RLock(name)
			defer index.shardCreateLocks.RUnlock(name)

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
}

func (o *nodeWideMetricsObserver) publishVectorMetrics(ctx context.Context) {
	var indices map[string]*Index
	// We're a low-priority process, copy the index map to avoid blocking others.
	// No new indices can be added while we're holding the lock anyways.
	func() {
		o.db.indexLock.RLock()
		defer o.db.indexLock.RUnlock()
		indices = make(map[string]*Index, len(o.db.indices))
		maps.Copy(indices, o.db.indices)
	}()

	var total DimensionMetrics

	start := time.Now()
	defer func() {
		took := time.Since(start)
		o.db.logger.WithFields(logrus.Fields{
			"action":           "observe_node_wide_metrics",
			"took":             took,
			"total_dimensions": total.Uncompressed,
			"total_segments":   total.Compressed,
			"publish_grouped":  o.db.promMetrics.Group,
		}).Debug("published vector metrics")
	}()

	for _, index := range indices {
		func() {
			index.dropIndex.RLock()
			defer index.dropIndex.RUnlock()

			index.closeLock.RLock()
			closed := index.closed
			index.closeLock.RUnlock()
			if !closed {
				className := index.Config.ClassName.String()

				// Avoid loading cold shards, as it may create I/O spikes.
				index.ForEachLoadedShard(func(shardName string, sl *Shard) error {
					index.shardCreateLocks.RLock(shardName)
					defer index.shardCreateLocks.RUnlock(shardName)

					dim := calculateShardDimensionMetrics(ctx, sl)
					total = total.Add(dim)

					// Report metrics per-shard if grouping is disabled.
					if !o.db.promMetrics.Group {
						o.sendVectorDimensions(className, shardName, dim)
					}
					return nil
				})
			}
		}()
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

// Calculate total vector dimensions for all vector indices in the shard's parent Index.
func calculateShardDimensionMetrics(ctx context.Context, sl ShardLike) DimensionMetrics {
	var total DimensionMetrics
	for name, config := range sl.Index().GetVectorIndexConfigs() {
		dim := calcVectorDimensionMetrics(ctx, sl, name, config)
		total = total.Add(dim)
	}
	return total
}

// Calculate vector dimensions for a vector index in a shard.
func calcVectorDimensionMetrics(ctx context.Context, sl ShardLike, vecName string, vecCfg schemaConfig.VectorIndexConfig) DimensionMetrics {
	switch dimInfo := GetDimensionCategoryLegacy(vecCfg); dimInfo.category {
	case DimensionCategoryPQ:
		count, _ := sl.QuantizedDimensions(ctx, vecName, dimInfo.segments)
		return DimensionMetrics{Uncompressed: 0, Compressed: count}
	case DimensionCategoryBQ:
		// BQ: 1 bit per dimension, packed into uint64 blocks (8 bytes per 64 dimensions)
		// [1..64] dimensions -> 8 bytes, [65..128] dimensions -> 16 bytes, etc.
		// Roundup is required because BQ packs bits into uint64 blocks - you can't have
		// a partial uint64 block. Even 1 dimension needs a full 8-byte uint64 block.
		count, _ := sl.Dimensions(ctx, vecName)
		bytes := (count + 63) / 64 * 8 // Round up to next uint64 block, then multiply by 8 bytes
		return DimensionMetrics{Uncompressed: 0, Compressed: bytes}
	case DimensionCategoryRQ:
		// RQ: bits per dimension, where bits can be 1 or 8
		// For bits=1: equivalent to BQ (1 bit per dimension, packed in uint64 blocks)
		// For bits=8: 8 bits per dimension (1 byte per dimension)
		count, _ := sl.Dimensions(ctx, vecName)
		bits := dimInfo.bits
		// RQ 8 Bit : DimensionMetrics{Uncompressed: bytes, Compressed: 0}
		// RQ 1 Bit : DimensionMetrics{Uncompressed: 0, Compressed: bytes}
		// this because of legacy vector_dimensions_sum is uncompressed and vector_segments_sum is compressed
		if bits == 1 {
			// bits=1: same as BQ - 1 bit per dimension, packed in uint64 blocks
			return DimensionMetrics{Uncompressed: 0, Compressed: (count + 63) / 64 * 8}
		}

		// bits=8: 8 bits per dimension (1 byte per dimension)
		return DimensionMetrics{Uncompressed: count, Compressed: 0}
	default:
		count, _ := sl.Dimensions(ctx, vecName)
		return DimensionMetrics{Uncompressed: count, Compressed: 0}
	}
}
