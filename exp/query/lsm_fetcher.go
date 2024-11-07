package query

import (
	"context"
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

type LSMFetcher struct {
	cache    *DiskCache
	upstream *modsloads3.Module
	log      logrus.FieldLogger

	basePath string
}

func NewLSMFetcher(basePath string, upstream *modsloads3.Module, log logrus.FieldLogger) *LSMFetcher {
	return NewLSMFetcherWithCache(basePath, upstream, nil, log)
}

func NewLSMFetcherWithCache(basePath string, upstream *modsloads3.Module, cache *DiskCache, log logrus.FieldLogger) *LSMFetcher {
	return &LSMFetcher{
		basePath: basePath,
		cache:    cache,
		log:      log,
		upstream: upstream,
	}
}

func (l *LSMFetcher) Fetch(ctx context.Context, collection, tenant string, version uint64) (*lsmkv.Store, string, error) {
	download := (l.cache == nil)
	// TODO(kavi): Will update download flag based on data version from schema later to decide to get from local cache or upstream storage.

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

	lsmPath := path.Join(dst, defaultLSMRoot)

	store, err := lsmkv.New(lsmPath, lsmPath, l.log, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	if err != nil {
		return nil, "", fmt.Errorf("failed to create store to read offloaded tenant data: %w", err)
	}

	return store, dst, nil
}
