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
	tenants   map[string]*list.Element
	evictList *list.List
	mu        sync.Mutex // TODO(kavi): Make it more fine-grained than global mutex

	// more than this will start evicting the tenants
	// maxCap in bytes.
	maxCap int64

	basePath string
}

func NewDiskCache(basePath string, maxCap int64) *DiskCache {
	return &DiskCache{
		tenants:   make(map[string]*list.Element),
		evictList: list.New(),
		basePath:  path.Join(basePath, CachePrefix),
		maxCap:    maxCap,
	}
}

// Assumes id all small-case
func (d *DiskCache) Tenant(collection, tenantID string) (*TenantCache, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if c, exists := d.tenants[d.TenantKey(collection, tenantID)]; exists {
		d.evictList.MoveToFront(c)
		c.Value.(*TenantCache).LastAccessed = time.Now()
		return c.Value.(*TenantCache), nil
	}
	return nil, ErrTenantNotFound
}

// AddTenant is called after having the files in right directory
// AddTenant checks if file is present on that directory
// deterministically generated from `collection` & `tenant` with basePath
func (d *DiskCache) AddTenant(collection, tenantID string, version uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

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

	c := d.evictList.PushFront(tc)
	d.tenants[d.TenantKey(tc.Collection, tc.TenantID)] = c

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
