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
	"strconv"
	"time"

	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/file"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
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

func (c *RemoteIndex) PutObject(ctx context.Context, host, index,
	shard string, obj *storobj.Object, schemaVersion uint64,
) error {
	value := []string{strconv.FormatUint(schemaVersion, 10)}

	body, err := clusterapi.IndicesPayloads.SingleObject.Marshal(obj)
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}

	req, err := setupRequest(ctx, http.MethodPost, host,
		fmt.Sprintf("/indices/%s/shards/%s/objects", index, shard),
		url.Values{replica.SchemaVersionKey: value}.Encode(),
		bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create http request: %w", err)
	}

	clusterapi.IndicesPayloads.SingleObject.SetContentTypeHeaderReq(req)
	_, err = c.do(c.timeoutUnit*60, req, body, nil, successCode)
	return err
}

func duplicateErr(in error, count int) []error {
	out := make([]error, count)
	for i := range out {
		out[i] = in
	}
	return out
}

func (c *RemoteIndex) BatchPutObjects(ctx context.Context, host, index,
	shard string, objs []*storobj.Object, _ *additional.ReplicationProperties, schemaVersion uint64,
) []error {
	value := []string{strconv.FormatUint(schemaVersion, 10)}
	body, err := clusterapi.IndicesPayloads.ObjectList.Marshal(objs)
	if err != nil {
		return duplicateErr(fmt.Errorf("encode request: %w", err), len(objs))
	}

	req, err := setupRequest(ctx, http.MethodPost, host,
		fmt.Sprintf("/indices/%s/shards/%s/objects", index, shard),
		url.Values{replica.SchemaVersionKey: value}.Encode(),
		bytes.NewReader(body))
	if err != nil {
		return duplicateErr(fmt.Errorf("create http request: %w", err), len(objs))
	}
	clusterapi.IndicesPayloads.ObjectList.SetContentTypeHeaderReq(req)

	var resp []error
	decode := func(data []byte) error {
		resp = clusterapi.IndicesPayloads.ErrorList.Unmarshal(data)
		return nil
	}

	if err = c.doWithCustomMarshaller(c.timeoutUnit*60, req, body, decode, successCode, 9); err != nil {
		return duplicateErr(err, len(objs))
	}

	return resp
}

func (c *RemoteIndex) BatchAddReferences(ctx context.Context, hostName, indexName,
	shardName string, refs objects.BatchReferences, schemaVersion uint64,
) []error {
	value := []string{strconv.FormatUint(schemaVersion, 10)}
	marshalled, err := clusterapi.IndicesPayloads.ReferenceList.Marshal(refs)
	if err != nil {
		return duplicateErr(errors.Wrap(err, "marshal payload"), len(refs))
	}

	req, err := setupRequest(ctx, http.MethodPost, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/references", indexName, shardName),
		url.Values{replica.SchemaVersionKey: value}.Encode(),
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

	req, err := setupRequest(ctx, http.MethodGet, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/objects/%s", indexName, shardName, id),
		url.Values{
			"additional":       []string{additionalEncoded},
			"selectProperties": []string{selectPropsEncoded},
		}.Encode(),
		nil)
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
	req, err := setupRequest(ctx, http.MethodGet, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/objects/%s", indexName, shardName, id),
		url.Values{"check_exists": []string{"true"}}.Encode(),
		nil)
	if err != nil {
		return false, fmt.Errorf("create http request: %w", err)
	}
	ok := func(code int) bool { return code == http.StatusNotFound || code == http.StatusNoContent }
	code, err := c.do(c.timeoutUnit*20, req, nil, nil, ok)
	return code != http.StatusNotFound, err
}

func (c *RemoteIndex) DeleteObject(ctx context.Context, hostName, indexName,
	shardName string, id strfmt.UUID, deletionTime time.Time, schemaVersion uint64,
) error {
	value := []string{strconv.FormatUint(schemaVersion, 10)}
	req, err := setupRequest(ctx, http.MethodDelete, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/objects/%s/%d", indexName, shardName, id, deletionTime.UnixMilli()),
		url.Values{replica.SchemaVersionKey: value}.Encode(),
		nil)
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
	shardName string, mergeDoc objects.MergeDocument, schemaVersion uint64,
) error {
	value := []string{strconv.FormatUint(schemaVersion, 10)}
	marshalled, err := clusterapi.IndicesPayloads.MergeDoc.Marshal(mergeDoc)
	if err != nil {
		return errors.Wrap(err, "marshal payload")
	}
	req, err := setupRequest(ctx, http.MethodPatch, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/objects/%s", indexName, shardName,
			mergeDoc.ID),
		url.Values{replica.SchemaVersionKey: value}.Encode(),
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
	req, err := setupRequest(ctx, http.MethodGet, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/objects", indexName, shardName),
		url.Values{"ids": []string{idsEncoded}}.Encode(),
		nil)
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
	vector []models.Vector,
	targetVector []string,
	distance float32,
	limit int,
	filters *filters.LocalFilter,
	keywordRanking *searchparams.KeywordRanking,
	sort []filters.Sort,
	cursor *filters.Cursor,
	groupBy *searchparams.GroupBy,
	additional additional.Properties,
	targetCombination *dto.TargetCombination,
	properties []string,
) ([]*storobj.Object, []float32, error) {
	// new request
	body, err := clusterapi.IndicesPayloads.SearchParams.
		Marshal(vector, targetVector, distance, limit, filters, keywordRanking, sort, cursor, groupBy, additional, targetCombination, properties)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal request payload: %w", err)
	}
	req, err := setupRequest(ctx, http.MethodPost, host,
		fmt.Sprintf("/indices/%s/shards/%s/objects/_search", index, shard),
		"", bytes.NewReader(body))
	if err != nil {
		return nil, nil, fmt.Errorf("create http request: %w", err)
	}
	clusterapi.IndicesPayloads.SearchParams.SetContentTypeHeaderReq(req)

	// send request
	resp := &searchShardResp{}
	err = c.doWithCustomMarshaller(c.timeoutUnit*20, req, body, resp.decode, successCode, 9)
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
	req, err := setupRequest(ctx, http.MethodPost, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/objects/_aggregations", index, shard),
		"", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create http request: %w", err)
	}
	clusterapi.IndicesPayloads.AggregationParams.SetContentTypeHeaderReq(req)

	// send request
	resp := &aggregateResp{}
	err = c.doWithCustomMarshaller(c.timeoutUnit*20, req, body, resp.decode, successCode, 9)
	return resp.Result, err
}

func (c *RemoteIndex) FindUUIDs(ctx context.Context, hostName, indexName,
	shardName string, filters *filters.LocalFilter,
) ([]strfmt.UUID, error) {
	paramsBytes, err := clusterapi.IndicesPayloads.FindUUIDsParams.Marshal(filters)
	if err != nil {
		return nil, errors.Wrap(err, "marshal request payload")
	}
	req, err := setupRequest(ctx, http.MethodPost, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/objects/_find", indexName, shardName),
		"", bytes.NewReader(paramsBytes))
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
	uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, schemaVersion uint64,
) objects.BatchSimpleObjects {
	value := []string{strconv.FormatUint(schemaVersion, 10)}
	marshalled, err := clusterapi.IndicesPayloads.BatchDeleteParams.Marshal(uuids, deletionTime, dryRun)
	if err != nil {
		err := errors.Wrap(err, "marshal payload")
		return objects.BatchSimpleObjects{objects.BatchSimpleObject{Err: err}}
	}
	req, err := setupRequest(ctx, http.MethodDelete, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/objects", indexName, shardName),
		url.Values{replica.SchemaVersionKey: value}.Encode(),
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
	req, err := setupRequest(ctx, http.MethodGet, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/queuesize", indexName, shardName),
		"", nil)
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
	req, err := setupRequest(ctx, http.MethodGet, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/status", indexName, shardName),
		"", nil)
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
	targetStatus string, schemaVersion uint64,
) error {
	paramsBytes, err := clusterapi.IndicesPayloads.UpdateShardStatusParams.Marshal(targetStatus)
	if err != nil {
		return errors.Wrap(err, "marshal request payload")
	}
	value := []string{strconv.FormatUint(schemaVersion, 10)}

	try := func(ctx context.Context) (bool, error) {
		req, err := setupRequest(ctx, http.MethodPost, hostName,
			fmt.Sprintf("/indices/%s/shards/%s/status", indexName, shardName),
			url.Values{replica.SchemaVersionKey: value}.Encode(),
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

	try := func(ctx context.Context) (bool, error) {
		req, err := setupRequest(ctx, http.MethodPost, hostName,
			fmt.Sprintf("/indices/%s/shards/%s/files/%s", indexName, shardName, fileName),
			"", payload)
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
	req, err := setupRequest(ctx, http.MethodPost, hostName,
		fmt.Sprintf("/indices/%s/shards/%s", indexName, shardName),
		"", nil)
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
	req, err := setupRequest(ctx, http.MethodPut, hostName,
		fmt.Sprintf("/indices/%s/shards/%s:reinit", indexName, shardName),
		"", nil)
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
	body, err := clusterapi.IndicesPayloads.IncreaseReplicationFactor.Marshall(dist)
	if err != nil {
		return err
	}
	try := func(ctx context.Context) (bool, error) {
		req, err := setupRequest(ctx, http.MethodPut, hostName,
			fmt.Sprintf("/replicas/indices/%s/replication-factor:increase", indexName),
			"", bytes.NewReader(body))
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

// PauseFileActivity pauses the collection's shard replica background processes on the specified
// host. You should explicitly resume the background processes once you're done with the
// files.
func (c *RemoteIndex) PauseFileActivity(ctx context.Context,
	hostName, indexName, shardName string, schemaVersion uint64,
) error {
	value := []string{strconv.FormatUint(schemaVersion, 10)}
	req, err := setupRequest(ctx, http.MethodPost, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/background:pause", indexName, shardName),
		url.Values{replica.SchemaVersionKey: value}.Encode(),
		nil,
	)
	if err != nil {
		return fmt.Errorf("create http request: %w", err)
	}

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
		return false, nil
	}
	return c.retry(ctx, 9, try)
}

// ResumeFileActivity resumes the collection's shard replica background processes on the specified host
func (c *RemoteIndex) ResumeFileActivity(ctx context.Context,
	hostName, indexName, shardName string,
) error {
	req, err := setupRequest(ctx, http.MethodPost, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/background:resume", indexName, shardName),
		"", nil)
	if err != nil {
		return fmt.Errorf("create http request: %w", err)
	}

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
		return false, nil
	}
	return c.retry(ctx, 9, try)
}

// ListFiles returns a list of files that can be used to get the shard data at the time the pause
// was requested. The returned relative file paths are relative to the shard's root directory.
// indexName is the collection name.
func (c *RemoteIndex) ListFiles(ctx context.Context,
	hostName, indexName, shardName string,
) ([]string, error) {
	req, err := setupRequest(ctx, http.MethodPost, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/background:list", indexName, shardName),
		"", nil)
	if err != nil {
		return []string{}, fmt.Errorf("create http request: %w", err)
	}

	var relativeFilePaths []string
	clusterapi.IndicesPayloads.ShardFilesResults.SetContentTypeHeaderReq(req)
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

		relativeFilePaths, err = clusterapi.IndicesPayloads.ShardFilesResults.Unmarshal(resBytes)
		if err != nil {
			return false, errors.Wrap(err, "unmarshal body")
		}
		return false, nil
	}
	return relativeFilePaths, c.retry(ctx, 9, try)
}

// GetFileMetadata returns file info to the file relative to the
// shard's root directory.
func (c *RemoteIndex) GetFileMetadata(ctx context.Context, hostName, indexName,
	shardName, relativeFilePath string,
) (file.FileMetadata, error) {
	req, err := setupRequest(ctx, http.MethodGet, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/files:metadata/%s", indexName, shardName, relativeFilePath),
		"", nil)
	if err != nil {
		return file.FileMetadata{}, fmt.Errorf("create http request: %w", err)
	}

	clusterapi.IndicesPayloads.ShardFiles.SetContentTypeHeaderReq(req)

	var md file.FileMetadata

	try := func(ctx context.Context) (bool, error) {
		res, err := c.client.Do(req)
		if err != nil {
			return ctx.Err() == nil, fmt.Errorf("connect: %w", err)
		}

		if res.StatusCode != http.StatusOK {
			defer res.Body.Close()
			body, _ := io.ReadAll(res.Body)
			return shouldRetry(res.StatusCode), fmt.Errorf(
				"unexpected status code %d (%s)", res.StatusCode, body)
		}

		resBytes, err := io.ReadAll(res.Body)
		if err != nil {
			return false, errors.Wrap(err, "read body")
		}

		md, err = clusterapi.IndicesPayloads.ShardFileMetadataResults.Unmarshal(resBytes)
		if err != nil {
			return false, errors.Wrap(err, "unmarshal body")
		}

		return false, nil
	}
	return md, c.retry(ctx, 9, try)
}

// GetFile caller must close the returned io.ReadCloser if no error is returned.
// indexName is the collection name. relativeFilePath is the path to the file relative to the
// shard's root directory.
func (c *RemoteIndex) GetFile(ctx context.Context, hostName, indexName,
	shardName, relativeFilePath string,
) (io.ReadCloser, error) {
	req, err := setupRequest(ctx, http.MethodGet, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/files/%s", indexName, shardName, relativeFilePath),
		"", nil)
	if err != nil {
		return nil, fmt.Errorf("create http request: %w", err)
	}
	clusterapi.IndicesPayloads.ShardFiles.SetContentTypeHeaderReq(req)
	var file io.ReadCloser
	try := func(ctx context.Context) (bool, error) {
		res, err := c.client.Do(req)
		if err != nil {
			return ctx.Err() == nil, fmt.Errorf("connect: %w", err)
		}

		if res.StatusCode != http.StatusOK {
			defer res.Body.Close()
			body, _ := io.ReadAll(res.Body)
			return shouldRetry(res.StatusCode), fmt.Errorf(
				"unexpected status code %d (%s)", res.StatusCode, body)
		}

		file = res.Body
		return false, nil
	}
	return file, c.retry(ctx, 9, try)
}

// AddAsyncReplicationTargetNode configures and starts async replication for the given
// host with the specified override.
func (c *RemoteIndex) AddAsyncReplicationTargetNode(
	ctx context.Context,
	hostName, indexName, shardName string,
	targetNodeOverride additional.AsyncReplicationTargetNodeOverride,
	schemaVersion uint64,
) error {
	body, err := clusterapi.IndicesPayloads.AsyncReplicationTargetNode.Marshal(targetNodeOverride)
	if err != nil {
		return fmt.Errorf("marshal target node override: %w", err)
	}
	value := []string{strconv.FormatUint(schemaVersion, 10)}
	req, err := setupRequest(ctx, http.MethodPost, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/async-replication-target-node", indexName, shardName),
		url.Values{replica.SchemaVersionKey: value}.Encode(),
		bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("create http request: %w", err)
	}
	clusterapi.IndicesPayloads.AsyncReplicationTargetNode.SetContentTypeHeaderReq(req)

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
		return false, nil
	}
	return c.retry(ctx, 9, try)
}

// RemoveAsyncReplicationTargetNode removes the given target node override for async replication.
func (c *RemoteIndex) RemoveAsyncReplicationTargetNode(
	ctx context.Context,
	hostName, indexName, shardName string,
	targetNodeOverride additional.AsyncReplicationTargetNodeOverride,
) error {
	body, err := clusterapi.IndicesPayloads.AsyncReplicationTargetNode.Marshal(targetNodeOverride)
	if err != nil {
		return fmt.Errorf("marshal target node override: %w", err)
	}

	req, err := setupRequest(ctx, http.MethodDelete, hostName,
		fmt.Sprintf("/indices/%s/shards/%s/async-replication-target-node", indexName, shardName),
		"", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create http request: %w", err)
	}
	clusterapi.IndicesPayloads.AsyncReplicationTargetNode.SetContentTypeHeaderReq(req)

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

// setupRequest is a simple helper to create a new http request with the given method, host, path,
// query, and body. Note that you can leave the query empty if you don't need it and the body can
// be nil. This does not send the request, just creates the request object.
func setupRequest(
	ctx context.Context,
	method, host, path, query string,
	body io.Reader,
) (*http.Request, error) {
	url := url.URL{Scheme: "http", Host: host, Path: path, RawQuery: query}
	req, err := http.NewRequestWithContext(ctx, method, url.String(), body)
	if err != nil {
		return nil, fmt.Errorf("create http request: %w", err)
	}
	return req, nil
}
