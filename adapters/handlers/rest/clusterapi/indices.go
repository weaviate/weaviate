//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package clusterapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/entities/searchparams"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/semi-technologies/weaviate/usecases/objects"
)

type indices struct {
	shards                    shards
	regexpObjects             *regexp.Regexp
	regexpObjectsSearch       *regexp.Regexp
	regexpObjectsFind         *regexp.Regexp
	regexpObjectsAggregations *regexp.Regexp
	regexpObject              *regexp.Regexp
	regexpReferences          *regexp.Regexp
	regexpShardsStatus        *regexp.Regexp
	regexpShardFiles          *regexp.Regexp
	regexpShard               *regexp.Regexp
	regexpShardReinit         *regexp.Regexp
}

const (
	urlPatternObjects = `\/indices\/([A-Za-z0-9_+-]+)` +
		`\/shards\/([A-Za-z0-9]+)\/objects`
	urlPatternObjectsSearch = `\/indices\/([A-Za-z0-9_+-]+)` +
		`\/shards\/([A-Za-z0-9]+)\/objects\/_search`
	urlPatternObjectsFind = `\/indices\/([A-Za-z0-9_+-]+)` +
		`\/shards\/([A-Za-z0-9]+)\/objects\/_find`
	urlPatternObjectsAggregations = `\/indices\/([A-Za-z0-9_+-]+)` +
		`\/shards\/([A-Za-z0-9]+)\/objects\/_aggregations`
	urlPatternObject = `\/indices\/([A-Za-z0-9_+-]+)` +
		`\/shards\/([A-Za-z0-9]+)\/objects\/([A-Za-z0-9_+-]+)`
	urlPatternReferences = `\/indices\/([A-Za-z0-9_+-]+)` +
		`\/shards\/([A-Za-z0-9]+)\/references`
	urlPatternShardsStatus = `\/indices\/([A-Za-z0-9_+-]+)` +
		`\/shards\/([A-Za-z0-9]+)\/_status`
	urlPatternShardFiles = `\/indices\/([A-Za-z0-9_+-]+)` +
		`\/shards\/([A-Za-z0-9]+)\/files/(.*)`
	urlPatternShard = `\/indices\/([A-Za-z0-9_+-]+)` +
		`\/shards\/([A-Za-z0-9]+)$`
	urlPatternShardReinit = `\/indices\/([A-Za-z0-9_+-]+)` +
		`\/shards\/([A-Za-z0-9]+)/_reinit`
)

type shards interface {
	PutObject(ctx context.Context, indexName, shardName string,
		obj *storobj.Object) error
	BatchPutObjects(ctx context.Context, indexName, shardName string,
		objs []*storobj.Object) []error
	BatchAddReferences(ctx context.Context, indexName, shardName string,
		refs objects.BatchReferences) []error
	GetObject(ctx context.Context, indexName, shardName string,
		id strfmt.UUID, selectProperties search.SelectProperties,
		additional additional.Properties) (*storobj.Object, error)
	Exists(ctx context.Context, indexName, shardName string,
		id strfmt.UUID) (bool, error)
	DeleteObject(ctx context.Context, indexName, shardName string,
		id strfmt.UUID) error
	MergeObject(ctx context.Context, indexName, shardName string,
		mergeDoc objects.MergeDocument) error
	MultiGetObjects(ctx context.Context, indexName, shardName string,
		id []strfmt.UUID) ([]*storobj.Object, error)
	Search(ctx context.Context, indexName, shardName string,
		vector []float32, distance float32, limit int, filters *filters.LocalFilter,
		keywordRanking *searchparams.KeywordRanking, sort []filters.Sort,
		additional additional.Properties) ([]*storobj.Object, []float32, error)
	Aggregate(ctx context.Context, indexName, shardName string,
		params aggregation.Params) (*aggregation.Result, error)
	FindDocIDs(ctx context.Context, indexName, shardName string,
		filters *filters.LocalFilter) ([]uint64, error)
	DeleteObjectBatch(ctx context.Context, indexName, shardName string,
		docIDs []uint64, dryRun bool) objects.BatchSimpleObjects
	GetShardStatus(ctx context.Context, indexName, shardName string) (string, error)
	UpdateShardStatus(ctx context.Context, indexName, shardName,
		targetStatus string) error

	// Scale-out Replication POC
	FilePutter(ctx context.Context, indexName, shardName,
		filePath string) (io.WriteCloser, error)
	CreateShard(ctx context.Context, indexName, shardName string) error
	ReinitShard(ctx context.Context, indexName, shardName string) error
}

func NewIndices(shards shards) *indices {
	return &indices{
		regexpObjects:             regexp.MustCompile(urlPatternObjects),
		regexpObjectsSearch:       regexp.MustCompile(urlPatternObjectsSearch),
		regexpObjectsFind:         regexp.MustCompile(urlPatternObjectsFind),
		regexpObjectsAggregations: regexp.MustCompile(urlPatternObjectsAggregations),
		regexpObject:              regexp.MustCompile(urlPatternObject),
		regexpReferences:          regexp.MustCompile(urlPatternReferences),
		regexpShardsStatus:        regexp.MustCompile(urlPatternShardsStatus),
		regexpShardFiles:          regexp.MustCompile(urlPatternShardFiles),
		regexpShard:               regexp.MustCompile(urlPatternShard),
		regexpShardReinit:         regexp.MustCompile(urlPatternShardReinit),
		shards:                    shards,
	}
}

func (i *indices) Indices() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case i.regexpObjectsSearch.MatchString(path):
			if r.Method != http.MethodPost {
				http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
				return
			}

			i.postSearchObjects().ServeHTTP(w, r)
			return
		case i.regexpObjectsFind.MatchString(path):
			if r.Method != http.MethodPost {
				http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
				return
			}

			i.postFindDocIDs().ServeHTTP(w, r)
			return
		case i.regexpObjectsAggregations.MatchString(path):
			if r.Method != http.MethodPost {
				http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
				return
			}

			i.postAggregateObjects().ServeHTTP(w, r)
			return
		case i.regexpObject.MatchString(path):
			if r.Method == http.MethodGet {
				i.getObject().ServeHTTP(w, r)
				return
			}
			if r.Method == http.MethodDelete {
				i.deleteObject().ServeHTTP(w, r)
				return
			}
			if r.Method == http.MethodPatch {
				i.mergeObject().ServeHTTP(w, r)
				return
			}

			http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
			return

		case i.regexpObjects.MatchString(path):
			if r.Method == http.MethodGet {
				i.getObjectsMulti().ServeHTTP(w, r)
				return
			}
			if r.Method == http.MethodPost {
				i.postObject().ServeHTTP(w, r)
				return
			}
			if r.Method == http.MethodDelete {
				i.deleteObjects().ServeHTTP(w, r)
				return
			}
			http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
			return

		case i.regexpReferences.MatchString(path):
			if r.Method != http.MethodPost {
				http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
				return
			}

			i.postReferences().ServeHTTP(w, r)
			return

		case i.regexpShardsStatus.MatchString(path):
			if r.Method == http.MethodGet {
				i.getGetShardStatus().ServeHTTP(w, r)
				return
			}
			if r.Method == http.MethodPost {
				i.postUpdateShardStatus().ServeHTTP(w, r)
				return
			}
			http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
			return

		case i.regexpShardFiles.MatchString(path):
			if r.Method == http.MethodPost {
				i.postShardFile().ServeHTTP(w, r)
				return
			}
			http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
			return

		case i.regexpShard.MatchString(path):
			if r.Method == http.MethodPost {
				i.postShard().ServeHTTP(w, r)
				return
			}
			http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
			return
		case i.regexpShardReinit.MatchString(path):
			if r.Method == http.MethodPut {
				i.putShardReinit().ServeHTTP(w, r)
				return
			}
			http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
			return

		default:
			http.NotFound(w, r)
			return
		}
	})
}

func (i *indices) postObject() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpObjects.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		defer r.Body.Close()

		ct := r.Header.Get("content-type")

		switch ct {
		case IndicesPayloads.ObjectList.MIME():
			i.postObjectBatch(w, r, index, shard)
			return

		case IndicesPayloads.SingleObject.MIME():
			i.postObjectSingle(w, r, index, shard)
			return

		default:
			http.Error(w, "415 Unsupported Media Type", http.StatusUnsupportedMediaType)
			return
		}
	})
}

func (i *indices) postObjectSingle(w http.ResponseWriter, r *http.Request,
	index, shard string,
) {
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	obj, err := IndicesPayloads.SingleObject.Unmarshal(bodyBytes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := i.shards.PutObject(r.Context(), index, shard, obj); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (i *indices) postObjectBatch(w http.ResponseWriter, r *http.Request,
	index, shard string,
) {
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	objs, err := IndicesPayloads.ObjectList.Unmarshal(bodyBytes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	errs := i.shards.BatchPutObjects(r.Context(), index, shard, objs)
	errsJSON, err := IndicesPayloads.ErrorList.Marshal(errs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	IndicesPayloads.ErrorList.SetContentTypeHeader(w)
	w.Write(errsJSON)
}

func (i *indices) getObject() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpObject.FindStringSubmatch(r.URL.Path)
		if len(args) != 4 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard, id := args[1], args[2], args[3]

		defer r.Body.Close()

		if r.URL.Query().Get("check_exists") != "" {
			i.checkExists(w, r, index, shard, id)
			return
		}

		additionalEncoded := r.URL.Query().Get("additional")
		if additionalEncoded == "" {
			http.Error(w, "missing required url param 'additional'",
				http.StatusBadRequest)
			return
		}

		additionalBytes, err := base64.StdEncoding.DecodeString(additionalEncoded)
		if err != nil {
			http.Error(w, "base64 decode 'additional' param: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		selectPropertiesEncoded := r.URL.Query().Get("selectProperties")
		if selectPropertiesEncoded == "" {
			http.Error(w, "missing required url param 'selectProperties'",
				http.StatusBadRequest)
			return
		}

		selectPropertiesBytes, err := base64.StdEncoding.
			DecodeString(selectPropertiesEncoded)
		if err != nil {
			http.Error(w, "base64 decode 'selectProperties' param: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		var additional additional.Properties
		if err := json.Unmarshal(additionalBytes, &additional); err != nil {
			http.Error(w, "unmarshal 'additional' param from json: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		var selectProperties search.SelectProperties
		if err := json.Unmarshal(selectPropertiesBytes, &selectProperties); err != nil {
			http.Error(w, "unmarshal 'selectProperties' param from json: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		obj, err := i.shards.GetObject(r.Context(), index, shard, strfmt.UUID(id),
			selectProperties, additional)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		if obj == nil {
			// this is a legitimate case - the requested ID doesn't exist, don't try
			// to marshal anything
			w.WriteHeader(http.StatusNotFound)
			return
		}

		objBytes, err := IndicesPayloads.SingleObject.Marshal(obj)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		IndicesPayloads.SingleObject.SetContentTypeHeader(w)
		w.Write(objBytes)
	})
}

func (i *indices) checkExists(w http.ResponseWriter, r *http.Request,
	index, shard, id string,
) {
	ok, err := i.shards.Exists(r.Context(), index, shard, strfmt.UUID(id))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	if ok {
		w.WriteHeader(http.StatusNoContent)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

func (i *indices) deleteObject() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpObject.FindStringSubmatch(r.URL.Path)
		if len(args) != 4 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard, id := args[1], args[2], args[3]

		defer r.Body.Close()

		err := i.shards.DeleteObject(r.Context(), index, shard, strfmt.UUID(id))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		w.WriteHeader(http.StatusNoContent)
	})
}

func (i *indices) mergeObject() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpObject.FindStringSubmatch(r.URL.Path)
		if len(args) != 4 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard, _ := args[1], args[2], args[3]

		defer r.Body.Close()
		ct, ok := IndicesPayloads.MergeDoc.CheckContentTypeHeaderReq(r)
		if !ok {
			http.Error(w, errors.Errorf("unexpected content type: %s", ct).Error(),
				http.StatusUnsupportedMediaType)
			return
		}

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		mergeDoc, err := IndicesPayloads.MergeDoc.Unmarshal(bodyBytes)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if err := i.shards.MergeObject(r.Context(), index, shard, mergeDoc); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})
}

func (i *indices) getObjectsMulti() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpObjects.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, fmt.Sprintf("invalid URI: %s", r.URL.Path),
				http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		defer r.Body.Close()

		idsEncoded := r.URL.Query().Get("ids")
		if idsEncoded == "" {
			http.Error(w, "missing required url param 'ids'",
				http.StatusBadRequest)
			return
		}

		idsBytes, err := base64.StdEncoding.DecodeString(idsEncoded)
		if err != nil {
			http.Error(w, "base64 decode 'ids' param: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		var ids []strfmt.UUID
		if err := json.Unmarshal(idsBytes, &ids); err != nil {
			http.Error(w, "unmarshal 'ids' param from json: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		objs, err := i.shards.MultiGetObjects(r.Context(), index, shard, ids)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		objsBytes, err := IndicesPayloads.ObjectList.Marshal(objs)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		IndicesPayloads.ObjectList.SetContentTypeHeader(w)
		w.Write(objsBytes)
	})
}

func (i *indices) postSearchObjects() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpObjectsSearch.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		defer r.Body.Close()
		reqPayload, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read request body: "+err.Error(), http.StatusInternalServerError)
			return
		}

		ct, ok := IndicesPayloads.SearchParams.CheckContentTypeHeaderReq(r)
		if !ok {
			http.Error(w, errors.Errorf("unexpected content type: %s", ct).Error(),
				http.StatusUnsupportedMediaType)
			return
		}

		vector, certainty, limit, filters, keywordRanking, sort, additional, err := IndicesPayloads.SearchParams.
			Unmarshal(reqPayload)
		if err != nil {
			http.Error(w, "unmarshal search params from json: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		results, dists, err := i.shards.Search(r.Context(), index, shard,
			vector, certainty, limit, filters, keywordRanking, sort, additional)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resBytes, err := IndicesPayloads.SearchResults.Marshal(results, dists)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		IndicesPayloads.SearchResults.SetContentTypeHeader(w)
		w.Write(resBytes)
	})
}

func (i *indices) postReferences() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpReferences.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		defer r.Body.Close()
		reqPayload, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read request body: "+err.Error(),
				http.StatusInternalServerError)
			return
		}

		ct, ok := IndicesPayloads.ReferenceList.CheckContentTypeHeaderReq(r)
		if !ok {
			http.Error(w, errors.Errorf("unexpected content type: %s", ct).Error(),
				http.StatusUnsupportedMediaType)
			return
		}

		refs, err := IndicesPayloads.ReferenceList.Unmarshal(reqPayload)
		if err != nil {
			http.Error(w, "read request body: "+err.Error(),
				http.StatusInternalServerError)
			return
		}

		errs := i.shards.BatchAddReferences(r.Context(), index, shard, refs)
		errsJSON, err := IndicesPayloads.ErrorList.Marshal(errs)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		IndicesPayloads.ErrorList.SetContentTypeHeader(w)
		w.Write(errsJSON)
	})
}

func (i *indices) postAggregateObjects() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpObjectsAggregations.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		defer r.Body.Close()
		reqPayload, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read request body: "+err.Error(),
				http.StatusInternalServerError)
			return
		}

		ct, ok := IndicesPayloads.AggregationParams.CheckContentTypeHeaderReq(r)
		if !ok {
			http.Error(w, errors.Errorf("unexpected content type: %s", ct).Error(),
				http.StatusUnsupportedMediaType)
			return
		}

		params, err := IndicesPayloads.AggregationParams.Unmarshal(reqPayload)
		if err != nil {
			http.Error(w, "read request body: "+err.Error(),
				http.StatusInternalServerError)
			return
		}

		aggRes, err := i.shards.Aggregate(r.Context(), index, shard, params)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		aggResBytes, err := IndicesPayloads.AggregationResult.Marshal(aggRes)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		IndicesPayloads.AggregationResult.SetContentTypeHeader(w)
		w.Write(aggResBytes)
	})
}

func (i *indices) postFindDocIDs() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpObjectsFind.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		defer r.Body.Close()
		reqPayload, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read request body: "+err.Error(), http.StatusInternalServerError)
			return
		}

		ct, ok := IndicesPayloads.FindDocIDsParams.CheckContentTypeHeaderReq(r)
		if !ok {
			http.Error(w, errors.Errorf("unexpected content type: %s", ct).Error(),
				http.StatusUnsupportedMediaType)
			return
		}

		filters, err := IndicesPayloads.FindDocIDsParams.
			Unmarshal(reqPayload)
		if err != nil {
			http.Error(w, "unmarshal find doc ids params from json: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		results, err := i.shards.FindDocIDs(r.Context(), index, shard, filters)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resBytes, err := IndicesPayloads.FindDocIDsResults.Marshal(results)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		IndicesPayloads.FindDocIDsResults.SetContentTypeHeader(w)
		w.Write(resBytes)
	})
}

func (i *indices) deleteObjects() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpObjects.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		defer r.Body.Close()
		reqPayload, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read request body: "+err.Error(), http.StatusInternalServerError)
			return
		}

		ct, ok := IndicesPayloads.BatchDeleteParams.CheckContentTypeHeaderReq(r)
		if !ok {
			http.Error(w, errors.Errorf("unexpected content type: %s", ct).Error(),
				http.StatusUnsupportedMediaType)
			return
		}

		docIDs, dryRun, err := IndicesPayloads.BatchDeleteParams.
			Unmarshal(reqPayload)
		if err != nil {
			http.Error(w, "unmarshal find doc ids params from json: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		results := i.shards.DeleteObjectBatch(r.Context(), index, shard, docIDs, dryRun)

		resBytes, err := IndicesPayloads.BatchDeleteResults.Marshal(results)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		IndicesPayloads.BatchDeleteResults.SetContentTypeHeader(w)
		w.Write(resBytes)
	})
}

func (i *indices) getGetShardStatus() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpShardsStatus.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		defer r.Body.Close()

		status, err := i.shards.GetShardStatus(r.Context(), index, shard)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		statusBytes, err := IndicesPayloads.GetShardStatusResults.Marshal(status)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		IndicesPayloads.GetShardStatusResults.SetContentTypeHeader(w)
		w.Write(statusBytes)
	})
}

func (i *indices) postUpdateShardStatus() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpShardsStatus.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		defer r.Body.Close()
		reqPayload, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read request body: "+err.Error(), http.StatusInternalServerError)
			return
		}

		ct, ok := IndicesPayloads.UpdateShardStatusParams.CheckContentTypeHeaderReq(r)
		if !ok {
			http.Error(w, errors.Errorf("unexpected content type: %s", ct).Error(),
				http.StatusUnsupportedMediaType)
			return
		}

		targetStatus, err := IndicesPayloads.UpdateShardStatusParams.
			Unmarshal(reqPayload)
		if err != nil {
			http.Error(w, "unmarshal find doc ids params from json: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		IndicesPayloads.UpdateShardsStatusResults.SetContentTypeHeader(w)

		err = i.shards.UpdateShardStatus(r.Context(), index, shard, targetStatus)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
}

func (i *indices) postShardFile() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpShardFiles.FindStringSubmatch(r.URL.Path)
		if len(args) != 4 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard, filename := args[1], args[2], args[3]

		ct, ok := IndicesPayloads.ShardFiles.CheckContentTypeHeaderReq(r)
		if !ok {
			http.Error(w, errors.Errorf("unexpected content type: %s", ct).Error(),
				http.StatusUnsupportedMediaType)
			return
		}

		fp, err := i.shards.FilePutter(r.Context(), index, shard, filename)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		defer fp.Close()
		n, err := io.Copy(fp, r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		fmt.Printf("%s/%s/%s n=%d\n", index, shard, filename, n)

		w.WriteHeader(http.StatusNoContent)
	})
}

func (i *indices) postShard() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpShard.FindStringSubmatch(r.URL.Path)
		fmt.Println(args)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		err := i.shards.CreateShard(r.Context(), index, shard)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
	})
}

func (i *indices) putShardReinit() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpShardReinit.FindStringSubmatch(r.URL.Path)
		fmt.Println(args)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		err := i.shards.ReinitShard(r.Context(), index, shard)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})
}
