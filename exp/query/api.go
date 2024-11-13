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
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/flat"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
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
	nodeName       = "weaviate-0"
	defaultLSMRoot = "lsm"

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

	schema SchemaQuerier

	offload *modsloads3.Module
	// cachedTenantLsmkvStores: tenantName -> tenantLsmkvStore (lsm object and path to tenant dir on disk)
	// the tenantLsmkvStore value is updated if new tenant data versions are downloaded.
	// Note that tenantLsmkvStore.localTenantPath points to the root of the tenant dir,
	// not the lsm subdir.
	cachedTenantLsmkvStores sync.Map

	vectorizer text2vecbase.TextVectorizer[[]float32]
	stopwords  *stopwords.Detector
}

type SchemaQuerier interface {
	// TenantStatus returns (STATUS, VERSION) tuple
	TenantStatus(ctx context.Context, collection, tenant string) (string, uint64, error)
	Collection(ctx context.Context, collection string) (*models.Class, error)
}

func NewAPI(
	schema SchemaQuerier,
	offload *modsloads3.Module,
	vectorizer text2vecbase.TextVectorizer[[]float32],
	stopwords *stopwords.Detector,
	config *Config,
	log logrus.FieldLogger,
) *API {
	api := &API{
		log:        log,
		config:     config,
		schema:     schema,
		offload:    offload,
		vectorizer: vectorizer,
		stopwords:  stopwords,
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

	info, tenantVersion, err := a.schema.TenantStatus(ctx, req.Collection, req.Tenant)
	if err != nil {
		return nil, err
	}

	if info != TenantOffLoadingStatus {
		return nil, fmt.Errorf("tenant %q is not offloaded, %w", req.Tenant, ErrInvalidTenant)
	}

	store, localPath, err := a.FetchLSM(ctx, req.Collection, req.Tenant, tenantVersion)
	if err != nil {
		return nil, err
	}
	defer store.Shutdown(ctx)

	var resp SearchResponse

	limit := req.Limit
	if limit == 0 || limit > maxQueryObjectsLimit {
		limit = maxQueryObjectsLimit
	}

	// TODO(kavi): Hanle where we support both nearText && Filters in the query
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

	if req.Filters != nil {
		resObjs, err := a.propertyFilters(ctx, store, req.Collection, req.Class, req.Tenant, req.Filters, limit)
		if err != nil {
			return nil, fmt.Errorf("failed to do filter search: %w", err)
		}
		for i := 0; i < len(resObjs); i++ {
			resp.Results = append(resp.Results, Result{
				Obj: resObjs[i],
			})
		}
		return &resp, nil
	}

	// return all objects upto `limit`
	if err := store.CreateOrLoadBucket(ctx, helpers.ObjectsBucketLSM); err != nil {
		return nil, fmt.Errorf("failed to load objects bucket in store: %w", err)
	}
	bkt := store.Bucket(helpers.ObjectsBucketLSM)
	if err := bkt.IterateObjects(ctx, func(object *storobj.Object) error {
		resp.Results = append(resp.Results, Result{Obj: object})
		if len(resp.Results) >= limit {
			return errLimitReached
		}
		return nil
	}); err != nil && !errors.Is(err, errLimitReached) {
		return nil, fmt.Errorf("failed to iterate objects in store: %w", err)
	}

	return &resp, nil
}

func (a *API) propertyFilters(
	ctx context.Context,
	store *lsmkv.Store,
	collection string,
	class *models.Class,
	tenant string,
	filter *filters.LocalFilter,
	limit int,
) ([]*storobj.Object, error) {
	// TODO(kavi): make it dynamic
	fallbackToSearchable := func() bool {
		return false
	}

	opts := []lsmkv.BucketOption{
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
	}

	var err error
	if class == nil {
		class, err = a.schema.Collection(ctx, collection)
		if err != nil {
			return nil, fmt.Errorf("failed to get class info from schema: %w", err)
		}
	}

	props := make([]string, 0)

	// Made sure all the properties of the class have been loaded for inverted index search
	for _, prop := range class.Properties {
		if err := store.CreateOrLoadBucket(ctx, helpers.BucketFromPropNameLSM(prop.Name), opts...); err != nil {
			return nil, fmt.Errorf("failed to open property lsmkv bucket: %w", err)
		}
		props = append(props, prop.Name)
	}

	getClass := func(className string) *models.Class {
		return class
	}

	// TODO(kavi): Handle cases where we pass `nil` to `propIndices`(for geo-indices) and `classSearcher`
	searcher := inverted.NewSearcher(a.log, store, getClass, nil, nil, a.stopwords, 0, fallbackToSearchable, tenant, maxQueryObjectsLimit, nil)

	opts = []lsmkv.BucketOption{
		lsmkv.WithSecondaryIndices(2),
	}

	// TOD(kavi): remove `opts` may be not necessary
	if err := store.CreateOrLoadBucket(ctx, helpers.ObjectsBucketLSM, opts...); err != nil {
		return nil, fmt.Errorf("failed to open objects bucket: %w", err)
	}

	objs, err := searcher.Objects(ctx, limit, filter, nil, additional.Properties{}, schema.ClassName(collection), props)
	if err != nil {
		return nil, fmt.Errorf("failed to search for objects:%w", err)
	}

	return objs, nil
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
		ID:               helpers.VectorsCompressedBucketLSM,
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
	matched_ids, distance, err := index.SearchByVectorDistance(ctx, vectors, certainty, int64(limit), nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to search by vector: %w", err)
	}

	opts := []lsmkv.BucketOption{
		lsmkv.WithSecondaryIndices(2),
	}

	if err := store.CreateOrLoadBucket(ctx, helpers.ObjectsBucketLSM, opts...); err != nil {
		return nil, nil, fmt.Errorf("failed to vectorize query text: %w", err)
	}

	bkt := store.Bucket(helpers.ObjectsBucketLSM)

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

type tenantLsmkvStore struct {
	s *lsmkv.Store
	// localTenantPath is the path on disk to the tenant dir
	localTenantPath string
}

func (a *API) FetchLSM(ctx context.Context, collection, tenant string, tenatVersion uint64) (*lsmkv.Store, string, error) {
	download := a.config.AlwaysFetchObjectStore
	// TODO(kavi): Will update download flag based on data version from schema later to decide to get from local cache or upstream storage.

	dst := path.Join(
		a.offload.DataPath,
		strings.ToLower(collection),
		strings.ToLower(tenant),
	)
	if download {
		// src - s3://<collection>/<tenant>/<node>/
		// dst (local) - <data-path/<collection>/<tenant>
		a.log.WithFields(logrus.Fields{
			"collection": collection,
			"tenant":     tenant,
			"nodeName":   nodeName,
			"dst":        dst,
		}).Debug("starting download to path")
		if err := a.offload.DownloadToPath(ctx, collection, tenant, nodeName, dst); err != nil {
			return nil, "", err
		}
	}

	// TODO(kavi): Avoid creating store every time?
	lsmPath := path.Join(dst, defaultLSMRoot)
	store, err := lsmkv.New(lsmPath, lsmPath, a.log, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	if err != nil {
		return nil, "", fmt.Errorf("failed to create store to read offloaded tenant data: %w", err)
	}

	return store, dst, nil
}

// NOTE(kavi): Not using it anywhere for now as it is bit unsable.
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

	// TODO if multiple calls to download the same tenant at nearly the same time happen
	// we'll unecessarily download the same tenant multiple times. If multiple calls to download
	// get the same microseconds value from the system, there could be issues.
	// I think the easiest way to fix this would be to serialize downloads per tenant.
	currentLocalTime := time.Now().UnixMicro()

	// TODO replace sync.Map with an LRU and/or buffer pool type abstraction
	cachedTenantLsmkvStore, tenantIsCachedLocally := a.cachedTenantLsmkvStores.Load(tenant)
	proceedWithDownload := false
	if doUpdateTenantIfExistsLocally {
		if tenantIsCachedLocally {
			// we should download because we've been told there is a new tenant version
			// for a tenant we have locally
			proceedWithDownload = true
			a.log.WithField("tenant", tenant).Debug("downloading tenant because it exists locally and there is a new version")
		} else {
			// TODO we only care about tenants we have for new versions, make diff func instead of
			// using bool or return error or other?
			a.log.WithField("tenant", tenant).Debug("ignoring new tenant data version because tenant does not exist locally")
			return nil, "", errors.New("tenant does not exist locally, ignore this error, will download later if needed")
		}
	}
	if !doUpdateTenantIfExistsLocally {
		if tenantIsCachedLocally {
			// tenant is cached, we can just return it because this is not a new version call
			cachedLsmkvStore := cachedTenantLsmkvStore.(tenantLsmkvStore)
			a.log.WithField("tenant", tenant).Debug("returning cached tenant")
			return cachedLsmkvStore.s, path.Join(cachedLsmkvStore.localTenantPath, defaultLSMRoot), nil
		} else {
			// we should download because the caller wants this tenant to exist locally but it does not
			proceedWithDownload = true
			a.log.WithField("tenant", tenant).Debug("downloading tenant because it does not exist locally and is requested")
		}
	}

	// base collection path
	localBaseCollectionPath := path.Join(
		a.offload.DataPath,
		strings.ToLower(collection),
	)
	// the path we will download to
	localTenantTimePath := path.Join(
		localBaseCollectionPath,
		strings.ToLower(tenant),
		fmt.Sprintf("%d", currentLocalTime),
	)
	// the path we will return
	localLsmPath := path.Join(
		localTenantTimePath,
		defaultLSMRoot,
	)
	if proceedWithDownload {
		_, err := os.Stat(localTenantTimePath)
		if os.IsNotExist(err) || a.config.AlwaysFetchObjectStore {
			// src - s3://<collection>/<tenant>/<node>/
			// dst (local) - <data-path/<collection>/<tenant>/timestamp
			a.log.WithFields(logrus.Fields{
				"collection":          collection,
				"tenant":              tenant,
				"nodeName":            nodeName,
				"localTenantTimePath": localTenantTimePath,
			}).Debug("starting download to path")
			if err := a.offload.DownloadToPath(ctx, collection, tenant, nodeName, localTenantTimePath); err != nil {
				return nil, "", err
			}
		}
	}

	store, err := lsmkv.New(localLsmPath, localLsmPath, a.log, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
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

		// to be extra safe, only remove if the path looks reasonably like what we expect (datapath/collection/tenant/timestamp/*)
		// NOTE if MatchString is too slow, we could use a simpler check like strings.HasPrefix without the timestamp
		if matched, err := regexp.MatchString(fmt.Sprintf(`%s/%s/\d+`, localBaseCollectionPath, tenant),
			oldTenantLsmkvStore.localTenantPath); matched && err == nil {

			dirToRemove := strings.TrimSuffix(oldTenantLsmkvStore.localTenantPath, tenant)
			a.log.WithField("dirToRemove", dirToRemove).Debugf("removing old tenant dir")
			os.RemoveAll(dirToRemove)
		}
	}
	return store, localLsmPath, nil
}

type SearchRequest struct {
	Collection string
	// if class is not nil, try avoid getting the class again from schema
	Class     *models.Class
	Tenant    string
	NearText  []string // vector search
	Certainty float64  // threshold to match with certainty of vectors match
	Limit     int
	Filters   *filters.LocalFilter
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
