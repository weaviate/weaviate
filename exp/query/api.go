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
	"path"
	"strings"

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

	// TODO(kavi): Accept `limit` arg from user.
	maxQueryObjectsLimit = 10
)

var ErrInvalidTenant = errors.New("invalid tenant status")

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
	offload    *modsloads3.Module
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
	return &API{
		log:        log,
		config:     config,
		tenant:     tenant,
		offload:    offload,
		vectorizer: vectorizer,
	}
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

	// src - s3://<collection>/<tenant>/<node>/
	// dst (local) - <data-path/<collection>/<tenant>
	if err := a.offload.Download(ctx, req.Collection, req.Tenant, nodeName); err != nil {
		return nil, err
	}

	localPath := path.Join(a.offload.DataPath, strings.ToLower(req.Collection), strings.ToLower(req.Tenant), defaultLSMRoot)

	// TODO(kavi): Avoid creating store every time?
	store, err := lsmkv.New(localPath, localPath, a.log, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	if err != nil {
		return nil, fmt.Errorf("failed to create store to read offloaded tenant data: %w", err)
	}

	var resp SearchResponse

	if len(req.NearText) != 0 {
		// do vector search
		resObjects, err := a.vectorSearch(ctx, store, localPath, req.NearText, float32(req.Certainty), req.Limit)
		if err != nil {
			return nil, err
		}
		resp.objects = append(resp.objects, resObjects...)
	} else {
		// return all objects
		if err := store.CreateOrLoadBucket(ctx, defaultLSMObjectsBucket); err != nil {
			return nil, fmt.Errorf("failed to load objects bucket in store: %w", err)
		}
		bkt := store.Bucket(defaultLSMObjectsBucket)
		if err := bkt.IterateObjects(ctx, func(object *storobj.Object) error {
			resp.objects = append(resp.objects, object)
			return nil
		}); err != nil {
			return nil, fmt.Errorf("failed to iterate objects in store: %w", err)
		}

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
) ([]*storobj.Object, error) {
	vectors, err := a.vectorizer.Texts(ctx, nearText, &modules.ClassBasedModuleConfig{})
	if err != nil {
		return nil, fmt.Errorf("failed to vectorize the nearText query: %w", err)
	}

	fmt.Println("vectors", vectors)

	// TODO(kavi): Assuming BQ compression is enabled. Make it generic.
	bq := flatent.CompressionUserConfig{
		Enabled: true,
	}
	// if err := store.CreateOrLoadBucket(ctx, defaultLSMVectorsCompressedBucket); err != nil {
	// 	return nil, fmt.Errorf("failed to load vectors bucket in store: %w", err)
	// }

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
		return nil, fmt.Errorf("failed to initialize index: %w", err)
	}

	matched_ids, _, err := index.SearchByVector(vectors, limit, nil)

	fmt.Println("matchd_ids", matched_ids)

	opts := []lsmkv.BucketOption{
		lsmkv.WithSecondaryIndices(2),
	}

	if err := store.CreateOrLoadBucket(ctx, defaultLSMObjectsBucket, opts...); err != nil {
		return nil, fmt.Errorf("failed to vectorize query text: %w", err)
	}

	bkt := store.Bucket(defaultLSMObjectsBucket)

	objs := make([]*storobj.Object, 0)

	for _, id := range matched_ids {
		key, err := indexDocIDToLSMKey(id)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize ids returned from flat index: %w", err)
		}
		objB, err := bkt.GetBySecondary(0, key)
		if err != nil {
			return nil, fmt.Errorf("failed to get object from store: %w", err)
		}

		obj, err := storobj.FromBinary(objB)
		if err != nil {
			return nil, fmt.Errorf("failed to decode object from store: %w", err)
		}

		objs = append(objs, obj)
	}

	return objs, nil
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
	objects []*storobj.Object
}

func indexDocIDToLSMKey(x uint64) ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	err := binary.Write(buf, binary.LittleEndian, x)
	if err != nil {
		return nil, fmt.Errorf("serialize int value as big endian: %w", err)
	}

	return buf.Bytes(), nil
}

func normalizeNearText(t string) string {
	return strings.TrimSpace(t)
}
