//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
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
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/tenantactivity"
	"github.com/weaviate/weaviate/usecases/config"
)

type nodeWideMetricsObserver struct {
	db *DB

	// Goroutines spawned by nodeWideMetricsObserver must exit after receiving on this channel.
	shutdown chan struct{}

	// The tenant maps that the most recent cycle filled, kept so the next cycle
	// can refill them instead of allocating new ones. The counters a cycle
	// compares against are kept in usage. Only the observeShards goroutine
	// touches it.
	activitySnapshot activityByCollection

	// The most tenants each collection has held since its counters map was last
	// replaced. Cleared and pruned maps keep their buckets, so a collection down
	// to less than a quarter of this gets fresh maps: the usage map on the cycle
	// the drop shows, the counters map on the next. Only the observeShards
	// goroutine touches it.
	tenantPeaks map[string]int

	// Guards usage only. The two fields above are written outside it.
	activityLock sync.RWMutex
	// Tenant usage as of the most recent cycle. Each cycle updates it in place
	// instead of building a new one, so repeated observations do not allocate.
	usage usageByCollection
}

// internal types used for tenant activity aggregation, not exposed to the user
type (
	activityByCollection map[string]activityByTenant
	activityByTenant     map[string]activity
	activity             struct {
		read  int32
		write int32
	}

	usageByCollection map[string]usageByTenant
	usageByTenant     map[string]tenantUsage
	// tenantUsage holds what the next cycle needs to compute a delta: the counter
	// values last seen, and the timestamps derived from them.
	tenantUsage struct {
		read         int32
		write        int32
		lastActivity time.Time
		lastRead     time.Time
		lastWrite    time.Time
	}
)

func newNodeWideMetricsObserver(db *DB) *nodeWideMetricsObserver {
	return &nodeWideMetricsObserver{
		db:          db,
		shutdown:    make(chan struct{}),
		tenantPeaks: map[string]int{},
	}
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

	o.updateUsage(current)

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

// Update the usage state from the newly observed counters. Collections and
// tenants that no longer appear are deleted, the rest are updated in place, so
// repeated observations do not allocate. A collection that lost most of its
// tenants gets a new map, carrying over the records of the tenants that are
// left.
func (o *nodeWideMetricsObserver) updateUsage(currentActivity activityByCollection) {
	now := time.Now()

	if o.usage == nil {
		o.usage = make(usageByCollection, len(currentActivity))
	}

	// drop whatever doesn't appear in the new list anymore
	for class := range o.usage {
		if _, ok := currentActivity[class]; !ok {
			delete(o.usage, class)
			delete(o.tenantPeaks, class)
		}
	}

	for class, current := range currentActivity {
		previous := o.usage[class]

		byTenant := previous
		if byTenant == nil || o.lostMostTenants(class, len(current)) {
			byTenant = make(usageByTenant, len(current))
			o.usage[class] = byTenant
		} else {
			for tenant := range byTenant {
				if _, ok := current[tenant]; !ok {
					delete(byTenant, tenant)
				}
			}
		}
		o.tenantPeaks[class] = max(o.tenantPeaks[class], len(current))

		for tenant, act := range current {
			// each record is read before it is overwritten, so previous can be the
			// map being written to
			byTenant[tenant] = o.tenantUsageDelta(class, tenant, act, previous, now)
		}
	}
}

// Whether a collection is down to less than a quarter of the tenants it peaked
// at, so that reusing its maps would keep far more buckets than it needs.
func (o *nodeWideMetricsObserver) lostMostTenants(class string, live int) bool {
	return live*4 < o.tenantPeaks[class]
}

// Derive a tenant's usage from its current counters and its record from the
// previous cycle. lastActivity moves when either counter changes, lastRead and
// lastWrite only when their own counter increases.
func (o *nodeWideMetricsObserver) tenantUsageDelta(class, tenant string, act activity, previous usageByTenant, now time.Time) tenantUsage {
	prev, ok := previous[tenant]
	if !ok {
		// this tenant didn't appear on the previous list, so we need to consider
		// it recently active
		usage := tenantUsage{read: act.read, write: act.write, lastActivity: now}

		// only track detailed value if the value is greater than the initial
		// value, otherwise we consider it just an activation without any user
		// activity
		if act.read > 1 {
			usage.lastRead = now
			o.logActivity(class, tenant, "read", act.read)
		}
		if act.write > 1 {
			usage.lastWrite = now
			o.logActivity(class, tenant, "write", act.write)
		}

		if act.read == 1 && act.write == 1 {
			// no specific activity, just an activation
			o.logActivity(class, tenant, "activation", 1)
		}
		return usage
	}

	usage := prev
	usage.read, usage.write = act.read, act.write
	if act.read == prev.read && act.write == prev.write {
		// unchanged, keep the previous timestamps
		return usage
	}

	// activity changed we need to update it
	usage.lastActivity = now
	if act.read > prev.read {
		usage.lastRead = now
		o.logActivity(class, tenant, "read", act.read)
	}
	if act.write > prev.write {
		usage.lastWrite = now
		o.logActivity(class, tenant, "write", act.write)
	}

	return usage
}

func (o *nodeWideMetricsObserver) getCurrentActivity() activityByCollection {
	o.db.indexLock.RLock()
	defer o.db.indexLock.RUnlock()

	previous := o.activitySnapshot
	current := make(activityByCollection, len(o.db.indices))
	for _, index := range o.db.indices {
		if !index.partitioningEnabled {
			continue
		}
		cn := index.Config.ClassName.String()

		// the previous counters are already kept in o.usage, so the map they were
		// read from can be refilled instead of allocated again
		tenants := previous[cn]
		switch {
		case tenants == nil:
			tenants = make(activityByTenant)
		case o.lostMostTenants(cn, len(tenants)):
			// len is last cycle's tenant count, as this cycle's is only known once
			// the map has been filled. The replacement's size becomes the peak the
			// next drop is measured against, so a collection that keeps draining
			// keeps shrinking.
			size := len(tenants)
			tenants = make(activityByTenant, size)
			o.tenantPeaks[cn] = size
		default:
			clear(tenants)
		}
		current[cn] = tenants

		index.ForEachShard(func(name string, shard ShardLike) error {
			index.shardCreateLocks.RLock(name)
			defer index.shardCreateLocks.RUnlock(name)

			act := activity{}
			act.read, act.write = shard.Activity()
			tenants[name] = act
			return nil
		})
	}
	o.activitySnapshot = current

	return current
}

// A zero time means the tenant saw no activity of that kind.
func (u tenantUsage) timestampFor(filter tenantactivity.UsageFilter) time.Time {
	switch filter {
	case tenantactivity.UsageFilterOnlyReads:
		return u.lastRead
	case tenantactivity.UsageFilterOnlyWrites:
		return u.lastWrite
	default:
		return u.lastActivity
	}
}

// How many tenants a filter reports. Every tenant has a total-activity
// timestamp, but a read or write filter typically matches only a few of them, so
// counting keeps Usage's result map from reserving space for the rest.
func (tenants usageByTenant) countMatching(filter tenantactivity.UsageFilter) int {
	if filter == tenantactivity.UsageFilterAll {
		return len(tenants)
	}

	count := 0
	for _, u := range tenants {
		if !u.timestampFor(filter).IsZero() {
			count++
		}
	}
	return count
}

// Usage returns a copy: every cycle rewrites the observer's own maps in place,
// so handing those out would let a caller range over a map while a cycle writes
// to it, aborting the process with "concurrent map iteration and map write".
func (o *nodeWideMetricsObserver) Usage(filter tenantactivity.UsageFilter) tenantactivity.ByCollection {
	if o == nil {
		// not loaded yet, requests could come in before the db is initialized yet
		// don't attempt to lock, as that would lead to a nil-pointer issue
		return tenantactivity.ByCollection{}
	}

	o.activityLock.RLock()
	defer o.activityLock.RUnlock()

	usage := make(tenantactivity.ByCollection, len(o.usage))
	for class, tenants := range o.usage {
		byTenant := make(tenantactivity.ByTenant, tenants.countMatching(filter))
		for tenant, u := range tenants {
			if ts := u.timestampFor(filter); !ts.IsZero() {
				byTenant[tenant] = ts
			}
		}
		usage[class] = byTenant
	}

	return usage
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
	if o.db.config.DisableDimensionMetrics.Get() {
		return
	}
	// We're a low-priority process, copy the index map to avoid blocking others.
	indices := o.db.copyIndices()

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
				index.ForEachLoadedShard(func(shardName string, sl ShardLike) error {
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
