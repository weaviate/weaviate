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
	"errors"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"golang.org/x/sync/singleflight"
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

	// sg makes sure there is at most one in-flight request is done to upstream
	// for the unique combination of (collection, tenant, version), say when multiple
	// concurrent in-flight requests were made for the same tenant data.
	sg singleflight.Group
}

// LSMDownloader is usually some component that used to download tenant's data.
type LSMDownloader interface {
	DownloadToPath(ctx context.Context, collection, tenant, nodeName, path string) error
}

// LSMCache provides caching of tenant's LSM data.
type LSMCache interface {
	// get tenant
	Tenant(collection, tenantID string) (*TenantCache, error)

	// put tenant
	AddTenant(collection, tenantID string, version uint64) error

	BasePath() string
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
// 3. If cache is enabled and found in the cache but local version is outdated, fetch from the upstream and remove old version.
// 4. if cache is enabled, found in the cache and local version is up to date, return from the cache.
func (l *LSMFetcher) Fetch(ctx context.Context, collection, tenant string, version uint64) (*lsmkv.Store, string, error) {
	// unique key for (collectin, tenant, version). this key make sure only one in-flight requests hitting upstream.
	key := fmt.Sprintf("%s-%s-%d", collection, tenant, version)
	val, err, _ := l.sg.Do(key, func() (any, error) {
		return l.fetch(ctx, collection, tenant, version)
	})

	if err != nil {
		return nil, "", fmt.Errorf("failed to fetch tenant data: %w", err)
	}

	tenantPath := val.(string)

	lsmPath := path.Join(tenantPath, defaultLSMRoot)
	store, err := lsmkv.New(lsmPath, lsmPath, l.log, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	if err != nil {
		return nil, "", fmt.Errorf("failed to create store to read offloaded tenant data: %w", err)
	}

	return store, lsmPath, nil
}

func (l *LSMFetcher) fetch(ctx context.Context, collection, tenant string, version uint64) (string, error) {
	var (
		basePath          = l.basePath
		tenantPath string = ""
		download   bool   = true
		oldVersion uint64
	)

	if l.cache != nil {
		basePath = l.cache.BasePath()
		c, err := l.cache.Tenant(collection, tenant)
		if err != nil && !errors.Is(err, ErrTenantNotFound) {
			return "", fmt.Errorf("failed to get tenant from cache: %w", err)
		}

		if err == nil && version != 0 && c.Version >= version {
			tenantPath = c.AbsolutePath()
			download = false
		}

		if err == nil && version != 0 && c.Version < version {
			// oldVersion is used to cleanup previous version of tenant data.
			oldVersion = c.Version
		}
	}

	if download {
		var err error
		tenantPath, err = l.download(ctx, basePath, collection, tenant, version)
		if err != nil {
			return "", err
		}

		// populate the cache before returning
		if l.cache != nil {
			if err := l.cache.AddTenant(collection, tenant, version); err != nil {
				return "", fmt.Errorf("failed populate the cache: %w", err)
			}
		}

		// remove old version
		if oldVersion > 0 {
			// NOTE: Removing old version can be tricky, even though all new lookup returns new version path.
			// There can be some clients still using old versions. We can leave without removing and delete only if eviction is needed.
			// But still that doesn't remove above stated problem.
			if err := l.remove(basePath, collection, tenant, oldVersion); err != nil {
				return "", fmt.Errorf("failed to remove old version of tenant data: %w", err)
			}
		}

	}
	return tenantPath, nil
}

func (l *LSMFetcher) download(ctx context.Context, basePath string, collection, tenant string, version uint64) (string, error) {
	versionStr := fmt.Sprintf("%d", version)

	tenantPath := path.Join(
		basePath,
		strings.ToLower(collection),
		strings.ToLower(tenant),
		versionStr,
	)

	// src - s3://<collection>/<tenant>/<node>/
	// dst (local) - <data-path/<collection>/<tenant>/<version>
	l.log.WithFields(logrus.Fields{
		"collection": collection,
		"tenant":     tenant,
		"nodeName":   nodeName,
		"dst":        tenantPath,
	}).Debug("starting download to path")
	if err := l.upstream.DownloadToPath(ctx, collection, tenant, nodeName, tenantPath); err != nil {
		return "", err
	}

	return tenantPath, nil
}

func (l *LSMFetcher) remove(basePath string, collection, tenant string, version uint64) error {
	dst := path.Join(
		basePath,
		strings.ToLower(collection),
		strings.ToLower(tenant),
		fmt.Sprintf("%d", version),
	)

	if err := os.RemoveAll(dst); err != nil {
		return fmt.Errorf("failed to remove old version(%d) of tenant(%s/%s): %w", version, collection, tenant, err)
	}
	return nil
}
