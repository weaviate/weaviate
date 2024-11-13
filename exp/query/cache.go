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

package query

import (
	"container/list"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

var (
	ErrTenantNotFound       = errors.New("cache: tenant not found")
	ErrTenantDirectoryFound = errors.New("cache: tenant directory not found")
)

const (
	evictFraction = 0.1 // When evicting, remove 10% of max disk capacity.
	CachePrefix   = "weaviate-cache"
)

// DiskCache is LRU disk based cache for tenants on-disk data.
type DiskCache struct {
	// key: <collection>/<tenant>
	tenants map[string]*list.Element
	tmu     sync.RWMutex

	evictList *list.List
	emu       sync.Mutex

	// maxCap in bytes.
	// more than this will start evicting the tenants
	maxCap int64

	basePath string

	metrics *CacheMetrics
}

func NewDiskCache(basePath string, maxCap int64, metrics *CacheMetrics) *DiskCache {
	return &DiskCache{
		tenants:   make(map[string]*list.Element),
		evictList: list.New(),
		basePath:  path.Join(basePath, CachePrefix),
		maxCap:    maxCap,
		metrics:   metrics,
	}
}

// Tenant returns cached tenant info for given collection and tenantID.
// Caller can use returned data to get abosolute path to access tenant data.
func (d *DiskCache) Tenant(collection, tenantID string) (*TenantCache, error) {
	timer := prometheus.NewTimer(d.metrics.OpsDuration.WithLabelValues(collection, tenantID, "get")) // "get" is the operation type, meaning the read path of the cache.
	defer timer.ObserveDuration()

	d.tmu.RLock()
	c, exists := d.tenants[d.TenantKey(collection, tenantID)]
	d.tmu.RUnlock()

	if exists {
		d.emu.Lock()
		d.evictList.MoveToFront(c)
		c.Value.(*TenantCache).LastAccessed = time.Now()
		d.emu.Unlock()

		return c.Value.(*TenantCache), nil
	}

	d.metrics.CacheMiss.WithLabelValues(collection, tenantID).Inc()
	return nil, ErrTenantNotFound
}

// AddTenant is called after having the files in right directory
// AddTenant checks if file is present on that directory
// deterministically generated from `collection` & `tenant` with basePath
func (d *DiskCache) AddTenant(collection, tenantID string, version uint64) error {
	timer := prometheus.NewTimer(d.metrics.OpsDuration.WithLabelValues(collection, tenantID, "put")) // "put" is the operation type, meaning the write path of the cache.
	defer timer.ObserveDuration()

	tc := &TenantCache{
		TenantID:     tenantID,
		Collection:   collection,
		Version:      version,
		LastAccessed: time.Now(),
		basePath:     d.basePath,
	}

	_, err := os.Stat(tc.AbsolutePath())
	if err != nil && errors.Is(err, os.ErrNotExist) {
		return errors.Join(ErrTenantDirectoryFound, err)
	}

	if err != nil {
		return fmt.Errorf("failed to get local path of the tenant: %w", err)
	}

	d.emu.Lock()
	c := d.evictList.PushFront(tc)
	d.emu.Unlock()

	d.tmu.Lock()
	d.tenants[d.TenantKey(tc.Collection, tc.TenantID)] = c
	d.tmu.Unlock()

	// evit if the size is getting filled up
	// TODO(kavi): Worth doing it on different go routine? But will get complex with correctness
	if d.usage() > d.maxCap {
		if err := d.evict(); err != nil {
			return err
		}
	}
	return nil
}

// BasePath returns base directory of cache data.
// Useful for client to know where to copy the files from upstream when using
// the cache.
func (d *DiskCache) BasePath() string {
	return d.basePath
}

// return total current usage of disk cache in bytes
func (d *DiskCache) usage() int64 {
	timer := prometheus.NewTimer(d.metrics.UsageCalcDuration)
	defer timer.ObserveDuration()

	var sizeBytes int64

	filepath.Walk(d.basePath, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			sizeBytes += info.Size()
		}
		return nil
	})
	return sizeBytes
}

func (d *DiskCache) evict() error {
	evictSize := int64(float64(d.maxCap) * evictFraction)

	for d.usage() > (d.maxCap - evictSize) {
		e := d.evictList.Back()
		if e == nil {
			// NOTE: Shouldn't happen.
			// This mean, we still have disk space filling up, but tenant eviction list is empty.
			panic("cache: max capacity reached. But couldn't find any tenant to evict")
		}
		t := e.Value.(*TenantCache)
		if err := os.RemoveAll(t.AbsolutePath()); err != nil {
			return err
		}
		delete(d.tenants, d.TenantKey(t.Collection, t.TenantID))
		d.evictList.Remove(e)
	}
	return nil
}

func (d *DiskCache) TenantKey(collection, tenantID string) string {
	return path.Join(collection, tenantID)
}

type TenantCache struct {
	Collection   string
	TenantID     string
	Version      uint64
	LastAccessed time.Time

	basePath string
}

func (tc *TenantCache) AbsolutePath() string {
	return path.Join(tc.basePath, tc.Collection, tc.TenantID, fmt.Sprintf("%d", tc.Version))
}

// CacheMetrics exposes some insights about how cache operations.
type CacheMetrics struct {
	// OpsDuration tracks overall duration of each cache operation.
	OpsDuration *prometheus.HistogramVec
	CacheMiss   *prometheus.CounterVec

	// UsageCalcDuration tracks `usage()` calls that sits in both read and write path.
	UsageCalcDuration prometheus.Histogram
}

func NewCacheMetrics(namespace string, reg prometheus.Registerer) *CacheMetrics {
	r := promauto.With(reg)

	return &CacheMetrics{
		OpsDuration: r.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "query_cache_duration_seconds",
			Buckets:   monitoring.LatencyBuckets,
		}, []string{"collection", "tenant", "operation"}), // operation is either "get" or "put"
		CacheMiss: r.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "query_cache_miss_total",
		}, []string{"collection", "tenant"}),

		UsageCalcDuration: r.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "query_cache_usage_calc_duration_seconds",
			Buckets:   monitoring.LatencyBuckets,
		}),
	}
}
