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
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	modsloads3 "github.com/weaviate/weaviate/modules/offload-s3"
)

const (
	TenantOffLoadingStatus = "FROZEN"

	// NOTE(kavi): using fixed nodeName that offloaded tenant under that prefix on object storage.
	// TODO(kavi): Make it dynamic.
	nodeName                = "weaviate-0"
	defaultLSMObjectsBucket = "objects"
	defaultLSMRoot          = "lsm"
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
	offload *modsloads3.Module
}

type TenantInfo interface {
	TenantStatus(ctx context.Context, collection, tenant string) (string, error)
}

func NewAPI(
	tenant TenantInfo,
	offload *modsloads3.Module,
	config *Config,
	log logrus.FieldLogger,
) *API {
	return &API{
		log:     log,
		config:  config,
		tenant:  tenant,
		offload: offload,
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

	// TODO(kavi): Use proper `path.Join`
	localPath := fmt.Sprintf("%s/%s/%s/%s", a.offload.DataPath, strings.ToLower(req.Collection), strings.ToLower(req.Tenant), defaultLSMRoot)
	store, err := lsmkv.New(localPath, localPath, a.log, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	if err != nil {
		return nil, fmt.Errorf("failed to create store to read offloaded tenant data: %w", err)
	}

	if err := store.CreateOrLoadBucket(ctx, defaultLSMObjectsBucket); err != nil {
		return nil, fmt.Errorf("failed to load objects bucket in store: %w", err)
	}

	bkt := store.Bucket(defaultLSMObjectsBucket)

	var resp SearchResponse

	if err := bkt.IterateObjects(ctx, func(object *storobj.Object) error {
		resp.objects = append(resp.objects, object)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to iterate objects in store: %w", err)
	}

	return &resp, nil
}

type SearchRequest struct {
	Collection string
	Tenant     string
}

type SearchResponse struct {
	objects []*storobj.Object
}
