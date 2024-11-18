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

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/flat"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
	"github.com/weaviate/weaviate/usecases/modulecomponents/text2vecbase"
	"github.com/weaviate/weaviate/usecases/modules"
)

const (
	TenantOffLoadingStatus = "FROZEN"

	defaultLSMRoot = "lsm"

	maxQueryObjectsLimit = 10
)

var (
	ErrInvalidTenant          = errors.New("invalid tenant status")
	ErrTenantBelongsToNoNodes = errors.New("tenant belongs to no nodes")

	// Sentinel error to mark if limit is reached when iterating on objects bucket. not user facing.
	errLimitReached = errors.New("limit reached")
)

// API is the core query API that is transport agnostic (http, grpc, etc).
type API struct {
	log    logrus.FieldLogger
	config *Config

	schema SchemaQuerier
	lsm    *LSMFetcher

	vectorizer text2vecbase.TextVectorizer[[]float32]
	stopwords  *stopwords.Detector
}

type SchemaQuerier interface {
	// TenantStatus returns (STATUS, BELONGSTONODES, VERSION) tuple
	TenantStatus(ctx context.Context, collection, tenant string) (string, []string, int64, error)
	Collection(ctx context.Context, collection string) (*models.Class, error)
}

func NewAPI(
	schema SchemaQuerier,
	lsm *LSMFetcher,
	vectorizer text2vecbase.TextVectorizer[[]float32],
	stopwords *stopwords.Detector,
	config *Config,
	log logrus.FieldLogger,
) *API {
	api := &API{
		log:        log,
		config:     config,
		schema:     schema,
		vectorizer: vectorizer,
		stopwords:  stopwords,
		lsm:        lsm,
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

	info, belongsToNodes, tenantVersion, err := a.schema.TenantStatus(ctx, req.Collection, req.Tenant)
	if err != nil {
		return nil, err
	}

	if info != TenantOffLoadingStatus {
		return nil, fmt.Errorf("tenant %q is not offloaded, %w", req.Tenant, ErrInvalidTenant)
	}

	// We will always pick the first node in the belongsToNodes list. We recommend not using replication if tenants are
	// offloaded to S3 to avoid write discrepancy issues.
	// TODO(lre): This is not ideal as it breaks HA guarantuess on ONE/QUORUM if clients are not using replication.
	// Think of a better way to support this.
	if len(belongsToNodes) == 0 {
		return nil, fmt.Errorf("tenant %q belongs to no nodes, impossible to locate in s3: %w", req.Tenant, ErrTenantBelongsToNoNodes)
	}

	store, localPath, err := a.lsm.Fetch(ctx, belongsToNodes[0], req.Collection, req.Tenant, tenantVersion)
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
