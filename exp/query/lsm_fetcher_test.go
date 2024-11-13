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
	"context"
	"fmt"
	"os"
	"path"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	fixedSize = 1 << 10 // 1KB
)

func TestLSMFetcher_withoutCache(t *testing.T) {
	root := path.Join(os.TempDir(), t.Name())
	defer func() {
		os.RemoveAll(root)
	}()

	ctx := context.Background()
	downloader := newMockDownloader(t)
	f := NewLSMFetcher(root, downloader, testLogger())

	// used without cache, so every `Fetch()` should download tenant data from upstream
	kv, gotFullpath, err := f.Fetch(ctx, "test-collection", "test-tenant", 0)
	expectedFullpath := path.Join(root, "test-collection", "test-tenant", "0", defaultLSMRoot)
	require.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, expectedFullpath, gotFullpath)
	assert.Equal(t, 1, downloader.count)

	// Any version number is irrelevant if cache is disabled and still should download from upstream
	kv, gotFullpath, err = f.Fetch(ctx, "test-collection", "test-tenant", 111) // non-zero version number
	expectedFullpath = path.Join(root, "test-collection", "test-tenant", "111", defaultLSMRoot)
	require.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, expectedFullpath, gotFullpath)
	assert.Equal(t, 2, downloader.count)

	// Fetching different tenant should also download from upstream
	kv, gotFullpath, err = f.Fetch(ctx, "test-collection2", "test-tenant2", 111) // non-zero version number
	expectedFullpath = path.Join(root, "test-collection2", "test-tenant2", "111", defaultLSMRoot)
	require.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, expectedFullpath, gotFullpath)
	assert.Equal(t, 3, downloader.count)
}

func TestLSMFetcher_withCache(t *testing.T) {
	testCollection := "test-collection"
	testTenant := "test-tenant"

	cases := []struct {
		name                string
		version             uint64
		expectedCacheHit    int
		expectedUpstreamHit int
		addTenant           bool
		addTenantVersion    uint64
	}{
		{
			name:                "version-0 should always download from upstream",
			version:             0,
			expectedCacheHit:    0,
			expectedUpstreamHit: 1,
		},
		{
			name:                "cache-miss should download from upstream, independent of version",
			version:             127,
			expectedCacheHit:    0,
			expectedUpstreamHit: 1,
		},
		{
			name:                "cache hit and version is outdated, download from upstream",
			version:             2,
			expectedCacheHit:    1,
			expectedUpstreamHit: 1,
			addTenant:           true,
			addTenantVersion:    1, // outdated version
		},
		{
			name:                "cache hit and version is matched, return from cache",
			version:             2,
			expectedCacheHit:    1,
			expectedUpstreamHit: 0, // no upstream call
			addTenant:           true,
			addTenantVersion:    2, // version matched
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := path.Join(os.TempDir(), t.Name())
			defer func() {
				os.RemoveAll(root)
			}()

			ctx := context.Background()
			upstream := newMockDownloader(t)
			cache := newMockCache(t, root)
			f := NewLSMFetcherWithCache(root, upstream, cache, testLogger())
			versionStr := fmt.Sprintf("%d", tc.version)

			if tc.addTenant {
				err := cache.AddTenant(testCollection, testTenant, tc.addTenantVersion)
				require.NoError(t, err)
			}

			kv, gotFullpath, err := f.Fetch(ctx, testCollection, testTenant, tc.version)
			expectedFullpath := path.Join(root, testCollection, testTenant, versionStr, defaultLSMRoot)
			require.NoError(t, err)
			assert.NotNil(t, kv)
			assert.Equal(t, expectedFullpath, gotFullpath)
			assert.Equal(t, tc.expectedUpstreamHit, upstream.count)   // upstream called?
			assert.Equal(t, tc.expectedCacheHit, cache.cacheHitCount) // cache hit?

			// after first `Fetch` next `Fetch` should always hit the cache
			// Fetcher should have populated the cache after it got from the upstream (except if version==0)
			if tc.version != 0 {
				previous := cache.cacheHitCount
				kv, gotFullpath, err = f.Fetch(ctx, testCollection, testTenant, tc.version)
				expectedFullpath = path.Join(root, testCollection, testTenant, versionStr, defaultLSMRoot)
				require.NoError(t, err)
				assert.NotNil(t, kv)
				assert.Equal(t, expectedFullpath, gotFullpath)
				assert.Equal(t, tc.expectedUpstreamHit, upstream.count) // shouldn't change upstream count
				assert.Equal(t, previous+1, cache.cacheHitCount)        // cacheHit should be previous + 1
			}
		})
	}
}

func TestLSMFetcher_concurrentInflights(t *testing.T) {
	// when `n` concurrent requests done for `(collection,tenant, version)` that is not in the cache.
	// It shouldn't span `n` upstream calls. Instead just `1` upstream calls

	root := path.Join(os.TempDir(), t.Name())
	ctx := context.Background()

	upstream := newMockDownloader(t)
	cache := newMockCache(t, root)
	fetcher := NewLSMFetcherWithCache(root, upstream, cache, testLogger())

	testCollection := "test-collection"
	testTenant := "test-tenant"
	testVersion := uint64(23)

	var (
		wg   sync.WaitGroup
		wait = make(chan struct{})
		n    = 100 // `n` concurrent clients calling `Fetch`
	)

	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			<-wait // wait till all groutines ready to fetch
			_, _, err := fetcher.Fetch(ctx, testCollection, testTenant, testVersion)
			require.NoError(t, err)
		}()
	}

	close(wait) // trigger fetch from all go-routines at the same time
	wg.Wait()   // wait till all goroutines done.

	assert.Equal(t, 1, upstream.count) // Should hit the upstream only once.
}

type mockDownloader struct {
	t     *testing.T
	count int
}

func newMockDownloader(t *testing.T) *mockDownloader {
	return &mockDownloader{
		t: t,
	}
}

func (m *mockDownloader) DownloadToPath(ctx context.Context, collection, tenant, nodeName, path string) error {
	createTempFileWithSize(m.t, path, "mock-lsm-downloader", fixedSize)
	m.count++
	return nil
}

type mockCache struct {
	t             *testing.T
	tenants       map[string]*TenantCache
	basePath      string
	cacheHitCount int
	addCount      int
}

func newMockCache(t *testing.T, basePath string) *mockCache {
	return &mockCache{
		t:        t,
		tenants:  make(map[string]*TenantCache),
		basePath: basePath,
	}
}

func (m *mockCache) Tenant(collection, tenantID string) (*TenantCache, error) {
	tc, ok := m.tenants[collection+tenantID]
	if !ok {
		return nil, ErrTenantNotFound
	}
	m.cacheHitCount++
	return tc, nil
}

func (m *mockCache) AddTenant(collection, tenantID string, version uint64) error {
	tc := &TenantCache{
		Collection: collection,
		TenantID:   tenantID,
		Version:    version,
		basePath:   m.basePath,
	}
	m.tenants[collection+tenantID] = tc
	m.addCount++
	return nil
}

func (m *mockCache) BasePath() string {
	return m.basePath
}
