package query

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

const (
	defaultLSMRoot = "lsm"

	// NOTE(kavi): using fixed nodeName that offloaded tenant under that prefix on object storage.
	// TODO(kavi): Make it dynamic.
	nodeName = "weaviate-0"
)

// LSMFetcher fetches the tenant's LSM data using given `upstream` without or without local cache.
type LSMFetcher struct {
	cache    LSMCache
	upstream LSMDownloader
	log      logrus.FieldLogger

	basePath string
}

// LSMDownloader is usually some component that used to download tenant's data.
type LSMDownloader interface {
	DownloadToPath(ctx context.Context, collection, tenant, nodeName, path string) error
}

// LSMCache provides caching of tenant's LSM data.
type LSMCache interface {
	Tenant(collection, tenantID string) (*TenantCache, error)
	AddTenant(collection, tenantID string, version uint64) error
}

// NewLSMFetcher creates LSMFetcher without cache.
// Every call is forwarded to given `upstream` to fetch the LSM data for the tenant.
func NewLSMFetcher(basePath string, upstream LSMDownloader, log logrus.FieldLogger) *LSMFetcher {
	return NewLSMFetcherWithCache(basePath, upstream, nil, log)
}

// NewLSMFetcherWithCache creates LSMFetcher with cache.
func NewLSMFetcherWithCache(basePath string, upstream LSMDownloader, cache LSMCache, log logrus.FieldLogger) *LSMFetcher {
	return &LSMFetcher{
		basePath: basePath,
		cache:    cache,
		log:      log,
		upstream: upstream,
	}
}

// Fetch try to fetch LSM data for the given (collection,tenant).
// It works in following modes
// 1. If cache is nil, fetch from upstream.
// 2. If cache is enabled and not found in cache, fetch from upstream.
// 3. If cache is enabled and found in the cache but local version is outdated, fetch from the upstream
// 4. if cache is enabled, found in the cache and local version is up to date, return from the cache.
func (l *LSMFetcher) Fetch(ctx context.Context, collection, tenant string, version uint64) (*lsmkv.Store, string, error) {
	var (
		tenantPath string
		download   bool = true
	)

	if l.cache != nil {
		c, err := l.cache.Tenant(collection, tenant)
		if err == nil && version != 0 && c.Version >= version {
			tenantPath = c.AbsolutePath()
			download = false
		}
		if err != nil && !errors.Is(err, ErrTenantNotFound) {
			return nil, "", err
		}
	}

	if download {
		tenantPath = path.Join(
			l.basePath,
			strings.ToLower(collection),
			strings.ToLower(tenant),
		)
		// src - s3://<collection>/<tenant>/<node>/
		// dst (local) - <data-path/<collection>/<tenant>
		l.log.WithFields(logrus.Fields{
			"collection": collection,
			"tenant":     tenant,
			"nodeName":   nodeName,
			"dst":        tenantPath,
		}).Debug("starting download to path")
		if err := l.upstream.DownloadToPath(ctx, collection, tenant, nodeName, tenantPath); err != nil {
			return nil, "", err
		}

	}

	// populate the cache before returning
	if l.cache != nil {
		if err := l.cache.AddTenant(collection, tenant, version); err != nil {
			return nil, "", fmt.Errorf("failed populate the cache: %w", err)
		}
	}

	lsmPath := path.Join(tenantPath, defaultLSMRoot)

	store, err := lsmkv.New(lsmPath, lsmPath, l.log, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	if err != nil {
		return nil, "", fmt.Errorf("failed to create store to read offloaded tenant data: %w", err)
	}

	return store, lsmPath, nil
}
