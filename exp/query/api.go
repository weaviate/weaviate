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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/flat"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
	modsloads3 "github.com/weaviate/weaviate/modules/offload-s3"
	"github.com/weaviate/weaviate/usecases/modulecomponents/text2vecbase"
	"github.com/weaviate/weaviate/usecases/modules"
)

const (
	TenantOffLoadingStatus = "FROZEN"

	// NOTE(kavi): using fixed nodeName that offloaded tenant under that prefix on object storage.
	// TODO(kavi): Make it dynamic.
	nodeName                          = "weaviate-0"
	defaultLSMObjectsBucket           = "objects"
	defaultLSMVectorsCompressedBucket = "vectors_compressed"
	defaultLSMRoot                    = "lsm"

	maxQueryObjectsLimit = 10
)

var (
	ErrInvalidTenant = errors.New("invalid tenant status")

	// Sentinel error to mark if limit is reached when iterating on objects bucket. not user facing.
	errLimitReached = errors.New("limit reached")
)

// API is the core query API that is transport agnostic (http, grpc, etc).
type API struct {
	log    logrus.FieldLogger
	config *Config

	tenant TenantInfo
	// svc provides the underlying search API via v1.WeaviateServer
	// TODO(kavi): Split `v1.WeaviateServer` into composable `v1.Searcher` and everything else.
	// svc    protocol.WeaviateServer
	// schema *schema.Manager
	// batch  *objects.BatchManager
	offload *modsloads3.Module
	// cachedTenantLsmkvStores: tenantName -> tenantLsmkvStore (lsm object and path to tenant dir on disk)
	// the tenantLsmkvStore value is updated if new tenant data versions are downloaded.
	// Note that tenantLsmkvStore.localTenantPath points to the root of the tenant dir,
	// not the lsm subdir.
	cachedTenantLsmkvStores sync.Map

	vectorizer text2vecbase.TextVectorizer
}

type TenantInfo interface {
	TenantStatus(ctx context.Context, collection, tenant string) (string, error)
}

func NewAPI(
	tenant TenantInfo,
	offload *modsloads3.Module,
	vectorizer text2vecbase.TextVectorizer,
	config *Config,
	log logrus.FieldLogger,
) *API {
	api := &API{
		log:        log,
		config:     config,
		tenant:     tenant,
		offload:    offload,
		vectorizer: vectorizer,
	}
	return api
}

// Search serves vector search over the offloaded tenant on object storage.
func (a *API) Search(ctx context.Context, req *SearchRequest) (*SearchResponse, error) {
	// Expectations
	// 0. Check if tenant status == FROZEN
	// 1. Build local path `/<datapath>/<collection>/<tenant>` from search request
	// 2. If it doesn't exist, fetch from object store to same path as above
	// 3. If it doesn't exist on object store, return 404
	// 4. Once in the local disk, load the index
	// 5. Searve the search.

	info, err := a.tenant.TenantStatus(ctx, req.Collection, req.Tenant)
	if err != nil {
		return nil, err
	}

	if info != TenantOffLoadingStatus {
		return nil, fmt.Errorf("tenant %q is not offloaded, %w", req.Tenant, ErrInvalidTenant)
	}

	store, localPath, err := a.EnsureLSM(ctx, req.Collection, req.Tenant, false)
	if err != nil {
		return nil, err
	}

	var resp SearchResponse

	limit := req.Limit
	if limit == 0 || limit > maxQueryObjectsLimit {
		limit = maxQueryObjectsLimit
	}

	if len(req.NearText) != 0 {
		// do vector search
		resObjects, distances, err := a.vectorSearch(ctx, store, localPath, req.NearText, float32(req.Certainty), limit)
		if err != nil {
			return nil, err
		}

		if len(resObjects) != len(distances) {
			return nil, fmt.Errorf("vectorsearch returned distance in-complete")
		}

		for i := 0; i < len(resObjects); i++ {
			resp.Results = append(resp.Results, Result{
				Obj:       resObjects[i],
				Certainty: float64(distances[i]),
			})
		}
		return &resp, nil
	}

	// return all objects upto `limit`
	if err := store.CreateOrLoadBucket(ctx, defaultLSMObjectsBucket); err != nil {
		return nil, fmt.Errorf("failed to load objects bucket in store: %w", err)
	}
	bkt := store.Bucket(defaultLSMObjectsBucket)
	if err := bkt.IterateObjects(ctx, func(object *storobj.Object) error {
		resp.Results = append(resp.Results, Result{Obj: object})
		if len(resp.Results) >= limit {
			return errLimitReached
		}
		return nil
	}); err != nil && !errors.Is(errLimitReached, err) {
		return nil, fmt.Errorf("failed to iterate objects in store: %w", err)
	}

	return &resp, nil
}

func (a *API) vectorSearch(
	ctx context.Context,
	store *lsmkv.Store,
	localPath string,
	nearText []string,
	threshold float32,
	limit int,
) ([]*storobj.Object, []float32, error) {
	vectors, err := a.vectorizer.Texts(ctx, nearText, &modules.ClassBasedModuleConfig{})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to vectorize the nearText query: %w", err)
	}

	// TODO(kavi): Assuming BQ compression is enabled. Make it generic.
	bq := flatent.CompressionUserConfig{
		Enabled: true,
	}

	// NOTE(kavi): Accept distance provider as dependency?
	dis := distancer.NewCosineDistanceProvider()
	index, err := flat.New(flat.Config{
		ID:               defaultLSMVectorsCompressedBucket,
		DistanceProvider: dis,
		RootPath:         localPath,
	}, flatent.UserConfig{
		BQ: bq,
	}, store)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize index: %w", err)
	}
	defer index.Shutdown(ctx)

	// TODO(kavi): Here `limit` is not what you expect. It's the maxLimit.
	// Currently `SearchByVectorDistance` api takes limit via `newSearchByDistParams(maxLimit)` which caller
	// don't have control too.
	certainty := 1 - threshold
	matched_ids, distance, err := index.SearchByVectorDistance(vectors, certainty, int64(limit), nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to search by vector: %w", err)
	}

	opts := []lsmkv.BucketOption{
		// lsmkv.WithStrategy(lsmkv.StrategyReplace),
		lsmkv.WithSecondaryIndices(2),
	}

	if err := store.CreateOrLoadBucket(ctx, defaultLSMObjectsBucket, opts...); err != nil {
		return nil, nil, fmt.Errorf("failed to vectorize query text: %w", err)
	}

	bkt := store.Bucket(defaultLSMObjectsBucket)

	objs := make([]*storobj.Object, 0)

	for _, id := range matched_ids {
		key, err := indexDocIDToLSMKey(id)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to serialize ids returned from flat index: %w", err)
		}
		objB, err := bkt.GetBySecondary(0, key)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get object from store: %w", err)
		}

		obj, err := storobj.FromBinary(objB)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to decode object from store: %w", err)
		}

		objs = append(objs, obj)
	}

	return objs, distance, nil
}

type LSMEnsurer interface {
	// EnsureLSM returns the store for this collection/tenant and the local path for the lsm store
	EnsureLSM(ctx context.Context, collection, tenant string, doUpdateTenantIfExistsLocally bool) (*lsmkv.Store, string, error)
}

var tenantDoesNotExistLocally = fmt.Errorf("tenant does not exist locally")

type tenantLsmkvStore struct {
	s *lsmkv.Store
	// localTenantPath is the path on disk to the tenant dir
	localTenantPath string
}

// EnsureLSM returns a cached lsmkv store or downloads a new one and returns that. If
// doUpdateTenantIfExistsLocally is true then the tenant will only be downloaded if it exists
// locally and if the tenant does not exist locally then this method will return nil. If
// doUpdateTenantIfExistsLocally is false then the cached tenant will be returned immediately if
// it exists locally, otherwise it will download and return it.
func (a *API) EnsureLSM(
	ctx context.Context,
	collection,
	tenant string,
	doUpdateTenantIfExistsLocally bool,
) (*lsmkv.Store, string, error) {
	// TODO move EnsureLSM into its own type separate from API

	// TODO if multiple calls to download the same tenant at the same time or get same microseconds
	// from the system clock, things will probably break below
	currentLocalTime := time.Now().UnixMicro()

	// TODO background cleanup of old disk path? we don't want to delete the old disk path until
	// all the searches reading from it have completed. Also should replace sync.Map with an LRU
	// and/or buffer pool type abstraction
	cachedTenantLsmkvStore, tenantIsCachedLocally := a.cachedTenantLsmkvStores.Load(tenant)
	proceedWithDownload := false
	if doUpdateTenantIfExistsLocally {
		if tenantIsCachedLocally {
			// we should download because we've been told there is a new tenant version
			// for a tenant we have locally
			proceedWithDownload = true
		} else {
			// TODO we only care about tenants we have for new versions, make diff func instead of
			// using bool or return error or other?
			return nil, "", errors.New("tenant does not exist locally, you can ignore this error, the tenant will be downloaded later")

		}
	}
	if !doUpdateTenantIfExistsLocally {
		if tenantIsCachedLocally {
			// tenant is cached, we can just return it because this is not a new version call
			cachedLsmkvStore := cachedTenantLsmkvStore.(tenantLsmkvStore)
			return cachedLsmkvStore.s, path.Join(cachedLsmkvStore.localTenantPath, defaultLSMRoot), nil
		} else {
			// we should download because the caller wants this tenant to exist locally but it does not
			proceedWithDownload = true
		}
	}

	// base collection path
	localBaseCollectionPath := path.Join(
		a.offload.DataPath,
		strings.ToLower(collection),
	)
	// the path the offload module downloads to
	localTenantOffloadPath := path.Join(
		localBaseCollectionPath,
		strings.ToLower(tenant),
	)
	// the new dir we will create to move this download into
	localClassTimePath := path.Join(
		a.offload.DataPath,
		strings.ToLower(collection),
		fmt.Sprintf("%d", currentLocalTime),
	)
	// the path we will rename this download to
	localTenantTimePath := path.Join(
		a.offload.DataPath,
		strings.ToLower(collection),
		fmt.Sprintf("%d", currentLocalTime),
		strings.ToLower(tenant),
	)
	// the path we will return
	localLsmPath := path.Join(
		localTenantTimePath,
		defaultLSMRoot,
	)
	if proceedWithDownload {
		// src - s3://<collection>/<tenant>/<node>/
		// dst (local) - <data-path/<collection>/<tenant>
		if err := a.offload.Download(ctx, collection, tenant, nodeName); err != nil {
			return nil, "", err
		}
		if err := os.MkdirAll(localClassTimePath, 0o755); err != nil {
			return nil, "", err
		}
		if err := os.Rename(localTenantOffloadPath, localTenantTimePath); err != nil {
			return nil, "", err
		}
	}

	store, err := lsmkv.New(localLsmPath, localLsmPath, a.log, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	if err != nil {
		return nil, "", fmt.Errorf("failed to create store to read offloaded tenant data: %w", err)
	}
	// remember to store the tenant root dir in tenantLsmkvStore, not the lsm path
	if oldTenantLsmkvStoreAny, ok := a.cachedTenantLsmkvStores.Swap(tenant, tenantLsmkvStore{s: store, localTenantPath: localTenantTimePath}); ok {
		// when we replace or evict an lsmkvStore, we need to shut it down.
		// I'm assuming lsmkvStore has internal locks which prevent this shutdown call
		// from cancelling searches running at the same time.
		// I'm also assuming once Shutdown returns we don't need the old files on disk anymore.
		oldTenantLsmkvStore := oldTenantLsmkvStoreAny.(tenantLsmkvStore)
		oldTenantLsmkvStore.s.Shutdown(ctx)

		// to be extra safe, only remove if the path looks reasonably like what we expect
		re := regexp.MustCompile(fmt.Sprintf(`%s/\d+/%s`, localBaseCollectionPath, tenant))
		if re.MatchString(oldTenantLsmkvStore.localTenantPath) {
			os.RemoveAll(strings.TrimSuffix(oldTenantLsmkvStore.localTenantPath, tenant))
		}
	}
	return store, localLsmPath, nil
}

type SearchRequest struct {
	Collection string
	Tenant     string
	NearText   []string // vector search
	Certainty  float64  // threshold to match with certainty of vectors match
	Limit      int
	// TODO(kavi): Add fields to do filter based search
}

type SearchResponse struct {
	Results []Result
}

type Result struct {
	Obj       *storobj.Object
	Certainty float64
}

func indexDocIDToLSMKey(x uint64) ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	err := binary.Write(buf, binary.LittleEndian, x)
	if err != nil {
		return nil, fmt.Errorf("serialize int value as big endian: %w", err)
	}

	return buf.Bytes(), nil
}
