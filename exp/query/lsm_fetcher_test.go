package query

import (
	"context"
	"os"
	"path"
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
	expectedFullpath := path.Join(root, "test-collection", "test-tenant", defaultLSMRoot)
	require.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, expectedFullpath, gotFullpath)
	assert.Equal(t, 1, downloader.count)

	// Any version number is irrelavent if cache is disabled and still should download from upstream
	kv, gotFullpath, err = f.Fetch(ctx, "test-collection", "test-tenant", 111) // non-zero version number
	expectedFullpath = path.Join(root, "test-collection", "test-tenant", defaultLSMRoot)
	require.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, expectedFullpath, gotFullpath)
	assert.Equal(t, 2, downloader.count)

	// Fetching different tenant should also download from upstream
	kv, gotFullpath, err = f.Fetch(ctx, "test-collection2", "test-tenant2", 111) // non-zero version number
	expectedFullpath = path.Join(root, "test-collection2", "test-tenant2", defaultLSMRoot)
	require.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, expectedFullpath, gotFullpath)
	assert.Equal(t, 3, downloader.count)
}

func TestLSMFetcher_withCache(t *testing.T) {
	root := path.Join(os.TempDir(), t.Name())
	defer func() {
		os.RemoveAll(root)
	}()

	ctx := context.Background()
	upstream := newMockDownloader(t)
	cache := newMockCache(t, root)
	f := NewLSMFetcherWithCache(root, upstream, cache, testLogger())

	// passing version 0, should always download from upstream
	kv, gotFullpath, err := f.Fetch(ctx, "test-collection", "test-tenant", 0)
	expectedFullpath := path.Join(root, "test-collection", "test-tenant", defaultLSMRoot)
	require.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, expectedFullpath, gotFullpath)
	assert.Equal(t, 1, upstream.count) // upstream called.

	// if cache miss, download from upstream
	kv, gotFullpath, err = f.Fetch(ctx, "test-collection", "test-tenant", 127)
	expectedFullpath = path.Join(root, "test-collection", "test-tenant", defaultLSMRoot)
	require.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, expectedFullpath, gotFullpath)
	assert.Equal(t, 2, upstream.count)      // upstream called.
	assert.Equal(t, 0, cache.cacheHitCount) // shouldn't hit the cache

	// if cache hit and version is not matched, download from upstream
	err = cache.AddTenant("test-collection", "test-tenant", 1)
	require.NoError(t, err)
	kv, gotFullpath, err = f.Fetch(ctx, "test-collection", "test-tenant", 2) // version mismatch
	expectedFullpath = path.Join(root, "test-collection", "test-tenant", defaultLSMRoot)
	require.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, expectedFullpath, gotFullpath)
	assert.Equal(t, 3, upstream.count)      // upstream called.
	assert.Equal(t, 1, cache.cacheHitCount) // hit the cache, but still called upstream because of version mismatch

	// if cache hit and version is matched, return from the cache
	kv, gotFullpath, err = f.Fetch(ctx, "test-collection", "test-tenant", 1) // version match
	expectedFullpath = path.Join(root, "test-collection", "test-tenant", defaultLSMRoot)
	require.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, expectedFullpath, gotFullpath)
	assert.Equal(t, 3, upstream.count)      // upstream not called.
	assert.Equal(t, 2, cache.cacheHitCount) // hit the cache,
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
