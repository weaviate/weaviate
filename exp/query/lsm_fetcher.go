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
	modsloads3 "github.com/weaviate/weaviate/modules/offload-s3"
)

const (
	DefaulLocaltBasePath = "/tmp/tenant-cache"
	defaultLSMRoot       = "lsm"

	// NOTE(kavi): using fixed nodeName that offloaded tenant under that prefix on object storage.
	// TODO(kavi): Make it dynamic.
	nodeName = "weaviate-0"
)

// LSMFetcher fetches the tenant's LSM data using given `upstream` without or without local cache.
type LSMFetcher struct {
	cache    *DiskCache
	upstream *modsloads3.Module
	log      logrus.FieldLogger

	basePath string
}

// NewLSMFetcher creates LSMFetcher without cache.
// Every call is forwarded to given `upstream` to fetch the LSM data for the tenant.
func NewLSMFetcher(basePath string, upstream *modsloads3.Module, log logrus.FieldLogger) *LSMFetcher {
	return NewLSMFetcherWithCache(basePath, upstream, nil, log)
}

// NewLSMFetcherWithCache creates LSMFetcher with cache.
func NewLSMFetcherWithCache(basePath string, upstream *modsloads3.Module, cache *DiskCache, log logrus.FieldLogger) *LSMFetcher {
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
		lsmPath  string
		download bool = true
	)

	if l.cache != nil {
		c, err := l.cache.Tenant(collection, tenant)
		if err == nil && c.Version >= version {
			lsmPath = c.AbsolutePath()
			download = false
		}
		if err != nil && !errors.Is(err, ErrTenantNotFound) {
			return nil, "", err
		}
	}

	if download {
		dst := path.Join(
			l.upstream.DataPath,
			strings.ToLower(collection),
			strings.ToLower(tenant),
		)
		if download {
			// src - s3://<collection>/<tenant>/<node>/
			// dst (local) - <data-path/<collection>/<tenant>
			l.log.WithFields(logrus.Fields{
				"collection": collection,
				"tenant":     tenant,
				"nodeName":   nodeName,
				"dst":        dst,
			}).Debug("starting download to path")
			if err := l.upstream.DownloadToPath(ctx, collection, tenant, nodeName, dst); err != nil {
				return nil, "", err
			}
		}

		lsmPath = path.Join(dst, defaultLSMRoot)
	}

	store, err := lsmkv.New(lsmPath, lsmPath, l.log, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	if err != nil {
		return nil, "", fmt.Errorf("failed to create store to read offloaded tenant data: %w", err)
	}

	return store, lsmPath, nil
}
