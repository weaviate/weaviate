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

package clients

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/scaler"
)

type RemoteIndex struct {
	retryClient
}

func NewRemoteIndex(httpClient *http.Client) *RemoteIndex {
	return &RemoteIndex{retryClient: retryClient{
		client:  httpClient,
		retryer: newRetryer(),
	}}
}

func (c *RemoteIndex) PutObject(ctx context.Context, hostName, indexName,
	shardName string, obj *storobj.Object,
) error {
	path := fmt.Sprintf("/indices/%s/shards/%s/objects", indexName, shardName)
	method := http.MethodPost
	url := url.URL{Scheme: "http", Host: hostName, Path: path}

	marshalled, err := clusterapi.IndicesPayloads.SingleObject.Marshal(obj)
	if err != nil {
		return errors.Wrap(err, "marshal payload")
	}

	req, err := http.NewRequestWithContext(ctx, method, url.String(),
		bytes.NewReader(marshalled))
	if err != nil {
		return errors.Wrap(err, "open http request")
	}

	clusterapi.IndicesPayloads.SingleObject.SetContentTypeHeaderReq(req)
	res, err := c.client.Do(req)
	if err != nil {
		return errors.Wrap(err, "send http request")
	}

	defer res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(res.Body)
		return errors.Errorf("unexpected status code %d (%s)", res.StatusCode,
			body)
	}

	return nil
}

func duplicateErr(in error, count int) []error {
	out := make([]error, count)
	for i := range out {
		out[i] = in
	}
	return out
}

func (c *RemoteIndex) BatchPutObjects(ctx context.Context, hostName, indexName,
	shardName string, objs []*storobj.Object, _ *additional.ReplicationProperties,
) []error {
	path := fmt.Sprintf("/indices/%s/shards/%s/objects", indexName, shardName)
	method := http.MethodPost
	url := url.URL{Scheme: "http", Host: hostName, Path: path}

	marshalled, err := clusterapi.IndicesPayloads.ObjectList.Marshal(objs)
	if err != nil {
		return duplicateErr(errors.Wrap(err, "marshal payload"), len(objs))
	}

	req, err := http.NewRequestWithContext(ctx, method, url.String(),
		bytes.NewReader(marshalled))
	if err != nil {
		return duplicateErr(errors.Wrap(err, "open http request"), len(objs))
	}

	clusterapi.IndicesPayloads.ObjectList.SetContentTypeHeaderReq(req)

	res, err := c.client.Do(req)
	if err != nil {
		return duplicateErr(errors.Wrap(err, "send http request"), len(objs))
	}

	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(res.Body)
		return duplicateErr(errors.Errorf("unexpected status code %d (%s)",
			res.StatusCode, body), len(objs))
	}

	if ct, ok := clusterapi.IndicesPayloads.ErrorList.
		CheckContentTypeHeader(res); !ok {
		return duplicateErr(errors.Errorf("unexpected content type: %s",
			ct), len(objs))
	}

	resBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return duplicateErr(errors.Wrap(err, "ready body"), len(objs))
	}

	return clusterapi.IndicesPayloads.ErrorList.Unmarshal(resBytes)
}

func (c *RemoteIndex) BatchAddReferences(ctx context.Context, hostName, indexName,
	shardName string, refs objects.BatchReferences,
) []error {
	path := fmt.Sprintf("/indices/%s/shards/%s/references", indexName, shardName)
	method := http.MethodPost
	url := url.URL{Scheme: "http", Host: hostName, Path: path}

	marshalled, err := clusterapi.IndicesPayloads.ReferenceList.Marshal(refs)
	if err != nil {
		return duplicateErr(errors.Wrap(err, "marshal payload"), len(refs))
	}

	req, err := http.NewRequestWithContext(ctx, method, url.String(),
		bytes.NewReader(marshalled))
	if err != nil {
		return duplicateErr(errors.Wrap(err, "open http request"), len(refs))
	}

	clusterapi.IndicesPayloads.ReferenceList.SetContentTypeHeaderReq(req)

	res, err := c.client.Do(req)
	if err != nil {
		return duplicateErr(errors.Wrap(err, "send http request"), len(refs))
	}

	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(res.Body)
		return duplicateErr(errors.Errorf("unexpected status code %d (%s)",
			res.StatusCode, body), len(refs))
	}

	if ct, ok := clusterapi.IndicesPayloads.ErrorList.
		CheckContentTypeHeader(res); !ok {
		return duplicateErr(errors.Errorf("unexpected content type: %s",
			ct), len(refs))
	}

	resBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return duplicateErr(errors.Wrap(err, "ready body"), len(refs))
	}

	return clusterapi.IndicesPayloads.ErrorList.Unmarshal(resBytes)
}

func (c *RemoteIndex) GetObject(ctx context.Context, hostName, indexName,
	shardName string, id strfmt.UUID, selectProps search.SelectProperties,
	additional additional.Properties,
) (*storobj.Object, error) {
	selectPropsBytes, err := json.Marshal(selectProps)
	if err != nil {
		return nil, errors.Wrap(err, "marshal selectProps props")
	}

	additionalBytes, err := json.Marshal(additional)
	if err != nil {
		return nil, errors.Wrap(err, "marshal additional props")
	}

	selectPropsEncoded := base64.StdEncoding.EncodeToString(selectPropsBytes)
	additionalEncoded := base64.StdEncoding.EncodeToString(additionalBytes)

	path := fmt.Sprintf("/indices/%s/shards/%s/objects/%s", indexName, shardName, id)
	method := http.MethodGet
	url := url.URL{Scheme: "http", Host: hostName, Path: path}
	q := url.Query()
	q.Set("additional", additionalEncoded)
	q.Set("selectProperties", selectPropsEncoded)
	url.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, method, url.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "open http request")
	}

	res, err := c.client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send http request")
	}

	defer res.Body.Close()
	if res.StatusCode == http.StatusNotFound {
		// this is a legitimate case - the requested ID doesn't exist, don't try
		// to unmarshal anything
		return nil, nil
	}

	if res.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(res.Body)
		return nil, errors.Errorf("unexpected status code %d (%s)", res.StatusCode,
			body)
	}

	ct, ok := clusterapi.IndicesPayloads.SingleObject.CheckContentTypeHeader(res)
	if !ok {
		return nil, errors.Errorf("unknown content type %s", ct)
	}

	objBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read body")
	}

	obj, err := clusterapi.IndicesPayloads.SingleObject.Unmarshal(objBytes)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal body")
	}

	return obj, nil
}

func (c *RemoteIndex) Exists(ctx context.Context, hostName, indexName,
	shardName string, id strfmt.UUID,
) (bool, error) {
	path := fmt.Sprintf("/indices/%s/shards/%s/objects/%s", indexName, shardName, id)
	method := http.MethodGet
	url := url.URL{Scheme: "http", Host: hostName, Path: path}
	q := url.Query()
	q.Set("check_exists", "true")
	url.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, method, url.String(), nil)
	if err != nil {
		return false, errors.Wrap(err, "open http request")
	}

	res, err := c.client.Do(req)
	if err != nil {
		return false, errors.Wrap(err, "send http request")
	}

	defer res.Body.Close()
	if res.StatusCode == http.StatusNotFound {
		// this is a legitimate case - the requested ID doesn't exist, don't try
		// to unmarshal anything
		return false, nil
	}

	if res.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(res.Body)
		return false, errors.Errorf("unexpected status code %d (%s)", res.StatusCode,
			body)
	}

	return true, nil
}

func (c *RemoteIndex) DeleteObject(ctx context.Context, hostName, indexName,
	shardName string, id strfmt.UUID,
) error {
	path := fmt.Sprintf("/indices/%s/shards/%s/objects/%s", indexName, shardName, id)
	method := http.MethodDelete
	url := url.URL{Scheme: "http", Host: hostName, Path: path}

	req, err := http.NewRequestWithContext(ctx, method, url.String(), nil)
	if err != nil {
		return errors.Wrap(err, "open http request")
	}

	res, err := c.client.Do(req)
	if err != nil {
		return errors.Wrap(err, "send http request")
	}

	defer res.Body.Close()
	if res.StatusCode == http.StatusNotFound {
		// this is a legitimate case - the requested ID doesn't exist, don't try
		// to unmarshal anything, we can assume it was already deleted
		return nil
	}

	if res.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(res.Body)
		return errors.Errorf("unexpected status code %d (%s)", res.StatusCode,
			body)
	}

	return nil
}

func (c *RemoteIndex) MergeObject(ctx context.Context, hostName, indexName,
	shardName string, mergeDoc objects.MergeDocument,
) error {
	path := fmt.Sprintf("/indices/%s/shards/%s/objects/%s", indexName, shardName,
		mergeDoc.ID)
	method := http.MethodPatch
	url := url.URL{Scheme: "http", Host: hostName, Path: path}

	marshalled, err := clusterapi.IndicesPayloads.MergeDoc.Marshal(mergeDoc)
	if err != nil {
		return errors.Wrap(err, "marshal payload")
	}

	req, err := http.NewRequestWithContext(ctx, method, url.String(),
		bytes.NewReader(marshalled))
	if err != nil {
		return errors.Wrap(err, "open http request")
	}

	clusterapi.IndicesPayloads.MergeDoc.SetContentTypeHeaderReq(req)
	res, err := c.client.Do(req)
	if err != nil {
		return errors.Wrap(err, "send http request")
	}

	defer res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(res.Body)
		return errors.Errorf("unexpected status code %d (%s)", res.StatusCode,
			body)
	}

	return nil
}

func (c *RemoteIndex) MultiGetObjects(ctx context.Context, hostName, indexName,
	shardName string, ids []strfmt.UUID,
) ([]*storobj.Object, error) {
	idsBytes, err := json.Marshal(ids)
	if err != nil {
		return nil, errors.Wrap(err, "marshal selectProps props")
	}

	idsEncoded := base64.StdEncoding.EncodeToString(idsBytes)

	path := fmt.Sprintf("/indices/%s/shards/%s/objects", indexName, shardName)
	method := http.MethodGet
	url := url.URL{Scheme: "http", Host: hostName, Path: path}
	q := url.Query()
	q.Set("ids", idsEncoded)
	url.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, method, url.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "open http request")
	}

	res, err := c.client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send http request")
	}

	defer res.Body.Close()
	if res.StatusCode == http.StatusNotFound {
		// this is a legitimate case - the requested ID doesn't exist, don't try
		// to unmarshal anything
		return nil, nil
	}

	if res.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(res.Body)
		return nil, errors.Errorf("unexpected status code %d (%s)", res.StatusCode,
			body)
	}

	ct, ok := clusterapi.IndicesPayloads.ObjectList.CheckContentTypeHeader(res)
	if !ok {
		return nil, errors.Errorf("unexpected content type: %s", ct)
	}

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	objs, err := clusterapi.IndicesPayloads.ObjectList.Unmarshal(bodyBytes)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal objects")
	}

	return objs, nil
}

func (c *RemoteIndex) SearchShard(ctx context.Context, host, index, shard string,
	vector []float32, limit int,
	filters *filters.LocalFilter,
	keywordRanking *searchparams.KeywordRanking,
	sort []filters.Sort,
	cursor *filters.Cursor,
	groupBy *searchparams.GroupBy,
	additional additional.Properties,
) ([]*storobj.Object, []float32, error) {
	// new request
	body, err := clusterapi.IndicesPayloads.SearchParams.
		Marshal(vector, limit, filters, keywordRanking, sort, cursor, groupBy, additional)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal request payload: %w", err)
	}
	url := url.URL{
		Scheme: "http",
		Host:   host,
		Path:   fmt.Sprintf("/indices/%s/shards/%s/objects/_search", index, shard),
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url.String(), bytes.NewReader(body))
	if err != nil {
		return nil, nil, fmt.Errorf("create http request: %w", err)
	}
	clusterapi.IndicesPayloads.SearchParams.SetContentTypeHeaderReq(req)

	// send request
	resp := &searchShardResp{}
	err = c.doWithCustomMarshaller(c.timeoutUnit*20, req, body, resp.decode)
	return resp.Objects, resp.Distributions, err
}

type searchShardResp struct {
	Objects       []*storobj.Object
	Distributions []float32
}

func (r *searchShardResp) decode(data []byte) (err error) {
	r.Objects, r.Distributions, err = clusterapi.IndicesPayloads.SearchResults.Unmarshal(data)
	return
}

type aggregateResp struct {
	Result *aggregation.Result
}

func (r *aggregateResp) decode(data []byte) (err error) {
	r.Result, err = clusterapi.IndicesPayloads.AggregationResult.Unmarshal(data)
	return
}

func (c *RemoteIndex) Aggregate(ctx context.Context, hostName, index,
	shard string, params aggregation.Params,
) (*aggregation.Result, error) {
	// create new request
	body, err := clusterapi.IndicesPayloads.AggregationParams.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("marshal request payload: %w", err)
	}

	url := &url.URL{
		Scheme: "http",
		Host:   hostName,
		Path:   fmt.Sprintf("/indices/%s/shards/%s/objects/_aggregations", index, shard),
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url.String(), bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create http request: %w", err)
	}
	clusterapi.IndicesPayloads.AggregationParams.SetContentTypeHeaderReq(req)

	// send request
	resp := &aggregateResp{}
	err = c.doWithCustomMarshaller(c.timeoutUnit*20, req, body, resp.decode)
	return resp.Result, err
}

func (c *RemoteIndex) FindUUIDs(ctx context.Context, hostName, indexName,
	shardName string, filters *filters.LocalFilter,
) ([]strfmt.UUID, error) {
	paramsBytes, err := clusterapi.IndicesPayloads.FindUUIDsParams.Marshal(filters)
	if err != nil {
		return nil, errors.Wrap(err, "marshal request payload")
	}

	path := fmt.Sprintf("/indices/%s/shards/%s/objects/_find", indexName, shardName)
	method := http.MethodPost
	url := url.URL{Scheme: "http", Host: hostName, Path: path}

	req, err := http.NewRequestWithContext(ctx, method, url.String(),
		bytes.NewReader(paramsBytes))
	if err != nil {
		return nil, errors.Wrap(err, "open http request")
	}

	clusterapi.IndicesPayloads.FindUUIDsParams.SetContentTypeHeaderReq(req)
	res, err := c.client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send http request")
	}

	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(res.Body)
		return nil, errors.Errorf("unexpected status code %d (%s)", res.StatusCode,
			body)
	}

	resBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read body")
	}

	ct, ok := clusterapi.IndicesPayloads.FindUUIDsResults.CheckContentTypeHeader(res)
	if !ok {
		return nil, errors.Errorf("unexpected content type: %s", ct)
	}

	uuids, err := clusterapi.IndicesPayloads.FindUUIDsResults.Unmarshal(resBytes)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal body")
	}
	return uuids, nil
}

func (c *RemoteIndex) DeleteObjectBatch(ctx context.Context, hostName, indexName, shardName string,
	uuids []strfmt.UUID, dryRun bool,
) objects.BatchSimpleObjects {
	path := fmt.Sprintf("/indices/%s/shards/%s/objects", indexName, shardName)
	method := http.MethodDelete
	url := url.URL{Scheme: "http", Host: hostName, Path: path}

	marshalled, err := clusterapi.IndicesPayloads.BatchDeleteParams.Marshal(uuids, dryRun)
	if err != nil {
		err := errors.Wrap(err, "marshal payload")
		return objects.BatchSimpleObjects{objects.BatchSimpleObject{Err: err}}
	}

	req, err := http.NewRequestWithContext(ctx, method, url.String(),
		bytes.NewReader(marshalled))
	if err != nil {
		err := errors.Wrap(err, "open http request")
		return objects.BatchSimpleObjects{objects.BatchSimpleObject{Err: err}}
	}

	clusterapi.IndicesPayloads.BatchDeleteParams.SetContentTypeHeaderReq(req)

	res, err := c.client.Do(req)
	if err != nil {
		err := errors.Wrap(err, "send http request")
		return objects.BatchSimpleObjects{objects.BatchSimpleObject{Err: err}}
	}

	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(res.Body)
		err := errors.Errorf("unexpected status code %d (%s)", res.StatusCode, body)
		return objects.BatchSimpleObjects{objects.BatchSimpleObject{Err: err}}
	}

	if ct, ok := clusterapi.IndicesPayloads.BatchDeleteResults.
		CheckContentTypeHeader(res); !ok {
		err := errors.Errorf("unexpected content type: %s", ct)
		return objects.BatchSimpleObjects{objects.BatchSimpleObject{Err: err}}
	}

	resBytes, err := io.ReadAll(res.Body)
	if err != nil {
		err := errors.Wrap(err, "ready body")
		return objects.BatchSimpleObjects{objects.BatchSimpleObject{Err: err}}
	}

	batchDeleteResults, err := clusterapi.IndicesPayloads.BatchDeleteResults.Unmarshal(resBytes)
	if err != nil {
		err := errors.Wrap(err, "unmarshal body")
		return objects.BatchSimpleObjects{objects.BatchSimpleObject{Err: err}}
	}

	return batchDeleteResults
}

func (c *RemoteIndex) GetShardQueueSize(ctx context.Context,
	hostName, indexName, shardName string,
) (int64, error) {
	path := fmt.Sprintf("/indices/%s/shards/%s/queuesize", indexName, shardName)
	method := http.MethodGet
	url := url.URL{Scheme: "http", Host: hostName, Path: path}

	req, err := http.NewRequestWithContext(ctx, method, url.String(), nil)
	if err != nil {
		return 0, errors.Wrap(err, "open http request")
	}
	var size int64
	clusterapi.IndicesPayloads.GetShardQueueSizeParams.SetContentTypeHeaderReq(req)
	try := func(ctx context.Context) (bool, error) {
		res, err := c.client.Do(req)
		if err != nil {
			return ctx.Err() == nil, fmt.Errorf("connect: %w", err)
		}
		defer res.Body.Close()

		if code := res.StatusCode; code != http.StatusOK {
			body, _ := io.ReadAll(res.Body)
			return shouldRetry(code), fmt.Errorf("status code: %v body: (%s)", code, body)
		}
		resBytes, err := io.ReadAll(res.Body)
		if err != nil {
			return false, errors.Wrap(err, "read body")
		}

		ct, ok := clusterapi.IndicesPayloads.GetShardQueueSizeResults.CheckContentTypeHeader(res)
		if !ok {
			return false, errors.Errorf("unexpected content type: %s", ct)
		}

		size, err = clusterapi.IndicesPayloads.GetShardQueueSizeResults.Unmarshal(resBytes)
		if err != nil {
			return false, errors.Wrap(err, "unmarshal body")
		}
		return false, nil
	}
	return size, c.retry(ctx, 9, try)
}

func (c *RemoteIndex) GetShardStatus(ctx context.Context,
	hostName, indexName, shardName string,
) (string, error) {
	path := fmt.Sprintf("/indices/%s/shards/%s/status", indexName, shardName)
	method := http.MethodGet
	url := url.URL{Scheme: "http", Host: hostName, Path: path}

	req, err := http.NewRequestWithContext(ctx, method, url.String(), nil)
	if err != nil {
		return "", errors.Wrap(err, "open http request")
	}
	var status string
	clusterapi.IndicesPayloads.GetShardStatusParams.SetContentTypeHeaderReq(req)
	try := func(ctx context.Context) (bool, error) {
		res, err := c.client.Do(req)
		if err != nil {
			return ctx.Err() == nil, fmt.Errorf("connect: %w", err)
		}
		defer res.Body.Close()

		if code := res.StatusCode; code != http.StatusOK {
			body, _ := io.ReadAll(res.Body)
			return shouldRetry(code), fmt.Errorf("status code: %v body: (%s)", code, body)
		}
		resBytes, err := io.ReadAll(res.Body)
		if err != nil {
			return false, errors.Wrap(err, "read body")
		}

		ct, ok := clusterapi.IndicesPayloads.GetShardStatusResults.CheckContentTypeHeader(res)
		if !ok {
			return false, errors.Errorf("unexpected content type: %s", ct)
		}

		status, err = clusterapi.IndicesPayloads.GetShardStatusResults.Unmarshal(resBytes)
		if err != nil {
			return false, errors.Wrap(err, "unmarshal body")
		}
		return false, nil
	}
	return status, c.retry(ctx, 9, try)
}

func (c *RemoteIndex) UpdateShardStatus(ctx context.Context, hostName, indexName, shardName,
	targetStatus string,
) error {
	paramsBytes, err := clusterapi.IndicesPayloads.UpdateShardStatusParams.Marshal(targetStatus)
	if err != nil {
		return errors.Wrap(err, "marshal request payload")
	}
	path := fmt.Sprintf("/indices/%s/shards/%s/status", indexName, shardName)
	method := http.MethodPost
	url := url.URL{Scheme: "http", Host: hostName, Path: path}

	try := func(ctx context.Context) (bool, error) {
		req, err := http.NewRequestWithContext(ctx, method, url.String(),
			bytes.NewReader(paramsBytes))
		if err != nil {
			return false, fmt.Errorf("create http request: %w", err)
		}
		clusterapi.IndicesPayloads.UpdateShardStatusParams.SetContentTypeHeaderReq(req)

		res, err := c.client.Do(req)
		if err != nil {
			return ctx.Err() == nil, fmt.Errorf("connect: %w", err)
		}
		defer res.Body.Close()

		if code := res.StatusCode; code != http.StatusOK {
			body, _ := io.ReadAll(res.Body)
			return shouldRetry(code), fmt.Errorf("status code: %v body: (%s)", code, body)
		}

		return false, nil
	}

	return c.retry(ctx, 9, try)
}

func (c *RemoteIndex) PutFile(ctx context.Context, hostName, indexName,
	shardName, fileName string, payload io.ReadSeekCloser,
) error {
	defer payload.Close()
	path := fmt.Sprintf("/indices/%s/shards/%s/files/%s",
		indexName, shardName, fileName)

	method := http.MethodPost
	url := url.URL{Scheme: "http", Host: hostName, Path: path}
	try := func(ctx context.Context) (bool, error) {
		req, err := http.NewRequestWithContext(ctx, method, url.String(), payload)
		if err != nil {
			return false, fmt.Errorf("create http request: %w", err)
		}
		clusterapi.IndicesPayloads.ShardFiles.SetContentTypeHeaderReq(req)
		res, err := c.client.Do(req)
		if err != nil {
			return ctx.Err() == nil, fmt.Errorf("connect: %w", err)
		}
		defer res.Body.Close()

		if code := res.StatusCode; code != http.StatusNoContent {
			shouldRetry := shouldRetry(code)
			if shouldRetry {
				_, err := payload.Seek(0, 0)
				shouldRetry = (err == nil)
			}
			body, _ := io.ReadAll(res.Body)
			return shouldRetry, fmt.Errorf("status code: %v body: (%s)", code, body)
		}
		return false, nil
	}

	return c.retry(ctx, 12, try)
}

func (c *RemoteIndex) CreateShard(ctx context.Context,
	hostName, indexName, shardName string,
) error {
	path := fmt.Sprintf("/indices/%s/shards/%s", indexName, shardName)

	method := http.MethodPost
	url := url.URL{Scheme: "http", Host: hostName, Path: path}

	req, err := http.NewRequestWithContext(ctx, method, url.String(), nil)
	if err != nil {
		return fmt.Errorf("create http request: %w", err)
	}
	try := func(ctx context.Context) (bool, error) {
		res, err := c.client.Do(req)
		if err != nil {
			return ctx.Err() == nil, fmt.Errorf("connect: %w", err)
		}
		defer res.Body.Close()

		if code := res.StatusCode; code != http.StatusCreated {
			body, _ := io.ReadAll(res.Body)
			return shouldRetry(code), fmt.Errorf("status code: %v body: (%s)", code, body)
		}
		return false, nil
	}

	return c.retry(ctx, 9, try)
}

func (c *RemoteIndex) ReInitShard(ctx context.Context,
	hostName, indexName, shardName string,
) error {
	path := fmt.Sprintf("/indices/%s/shards/%s:reinit", indexName, shardName)

	method := http.MethodPut
	url := url.URL{Scheme: "http", Host: hostName, Path: path}

	req, err := http.NewRequestWithContext(ctx, method, url.String(), nil)
	if err != nil {
		return fmt.Errorf("create http request: %w", err)
	}
	try := func(ctx context.Context) (bool, error) {
		res, err := c.client.Do(req)
		if err != nil {
			return ctx.Err() == nil, fmt.Errorf("connect: %w", err)
		}
		defer res.Body.Close()

		if code := res.StatusCode; code != http.StatusNoContent {
			body, _ := io.ReadAll(res.Body)
			return shouldRetry(code), fmt.Errorf("status code: %v body: (%s)", code, body)

		}
		return false, nil
	}

	return c.retry(ctx, 9, try)
}

func (c *RemoteIndex) IncreaseReplicationFactor(ctx context.Context,
	hostName, indexName string, dist scaler.ShardDist,
) error {
	path := fmt.Sprintf("/replicas/indices/%s/replication-factor:increase", indexName)

	method := http.MethodPut
	url := url.URL{Scheme: "http", Host: hostName, Path: path}

	body, err := clusterapi.IndicesPayloads.IncreaseReplicationFactor.Marshall(dist)
	if err != nil {
		return err
	}
	try := func(ctx context.Context) (bool, error) {
		req, err := http.NewRequestWithContext(ctx, method, url.String(), bytes.NewReader(body))
		if err != nil {
			return false, fmt.Errorf("create http request: %w", err)
		}

		res, err := c.client.Do(req)
		if err != nil {
			return ctx.Err() == nil, fmt.Errorf("connect: %w", err)
		}
		defer res.Body.Close()

		if code := res.StatusCode; code != http.StatusNoContent {
			body, _ := io.ReadAll(res.Body)
			return shouldRetry(code), fmt.Errorf("status code: %v body: (%s)", code, body)
		}
		return false, nil
	}
	return c.retry(ctx, 34, try)
}
