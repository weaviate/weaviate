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
	// NOTE: This lock may be in-efficient if downloaded needed from multiple tenants
	// TODO(kavi): Optimize based on some data later.
	omu sync.Mutex

	// tenantsIHave sync.Map // tenantName -> diskPath
	// tenant/../lsm/0
	// tenant/../lsm/1
	//

	vectorizer text2vecbase.TextVectorizer

	startMetadataSubscription func()
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
	metadataSubscription := NewMetadataSubscription(api, config.MetadataGRPCHost, config.MetadataGRPCPort, log)
	api.startMetadataSubscription = metadataSubscription.EnsureStarted
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

	// TODO come up with a better way to ensure this doesn't run until the gRPC serving is active
	a.startMetadataSubscription()

	// TODO explore locking within this func and MetadataSubcription downloads as i expect
	// vectorSearch/IterateObjects could fail if the files they operate on are being modified
	// while they are running
	store, localPath, err := a.EnsureLSM(ctx, req.Collection, req.Tenant, false)
	if err != nil {
		return nil, err
	}
	defer store.Shutdown(ctx)

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
	// EnsureLSM returns the store for this collection/tenant and the local path for the store
	EnsureLSM(ctx context.Context, collection, tenant string, updateTenantIfExistsLocally bool) (*lsmkv.Store, string, error)
}

func (a *API) EnsureLSM(ctx context.Context, collection, tenant string, updateTenantIfExistsLocally bool) (*lsmkv.Store, string, error) {
	currentLocalTime := time.Now()
	localPath := path.Join(a.offload.DataPath, strings.ToLower(collection), strings.ToLower(tenant), defaultLSMRoot, string(currentLocalTime.UnixMilli()))

	// NOTE: Download only if path doesn't exist.
	// Assumes, whatever in the path is latest.
	// We will add a another way to keep this path upto date via some data versioning.
	a.omu.Lock()
	defer a.omu.Unlock()

	_, err := os.Stat(localPath)

	// TODO remove old?
	tenantExistsLocally := !os.IsNotExist(err)
	proceedWithDownload := false
	// Technically, we want !xor, but leaving these if's because it's easy to read
	if updateTenantIfExistsLocally && tenantExistsLocally {
		// we should download because we've been told there is a new tenant version
		// for a tenant we have locally
		proceedWithDownload = true
	}
	if updateTenantIfExistsLocally && !tenantExistsLocally {
		// we only care about tenants we have for new versions, error? or diff func?
		return nil, "", errors.New("tenant does not exist locally")
	}
	if !updateTenantIfExistsLocally && tenantExistsLocally {
		// cached
	}
	if !updateTenantIfExistsLocally && !tenantExistsLocally {
		// we should download because the caller wants
		proceedWithDownload = true
	}
	if proceedWithDownload {
		// src - s3://<collection>/<tenant>/<node>/
		// dst (local) - <data-path/<collection>/<tenant>
		if err := a.offload.Download(ctx, collection, tenant, nodeName); err != nil {
			return nil, "", err
		}
	}

	// TODO(kavi): Avoid creating store every time?
	store, err := lsmkv.New(localPath, localPath, a.log, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	if err != nil {
		return nil, "", fmt.Errorf("failed to create store to read offloaded tenant data: %w", err)
	}
	return store, localPath, nil
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
