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
	"math/rand"
	"net/http"
	"net/url"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

// ReplicationClient is to coordinate operations among replicas

type replicationClient retryClient

func NewReplicationClient(httpClient *http.Client) replica.Client {
	return &replicationClient{
		client:  httpClient,
		retryer: newRetryer(),
	}
}

// FetchObject fetches one object it exits
func (c *replicationClient) FetchObject(ctx context.Context, host, index,
	shard string, id strfmt.UUID, selectProps search.SelectProperties,
	additional additional.Properties, numRetries int,
) (replica.Replica, error) {
	resp := replica.Replica{}
	req, err := newHttpReplicaRequest(ctx, http.MethodGet, host, index, shard, "", id.String(), nil, 0)
	if err != nil {
		return resp, fmt.Errorf("create http request: %w", err)
	}
	err = c.doCustomUnmarshal(c.timeoutUnit*20, req, nil, resp.UnmarshalBinary, numRetries)
	return resp, err
}

func (c *replicationClient) DigestObjects(ctx context.Context,
	host, index, shard string, ids []strfmt.UUID, numRetries int,
) (result []types.RepairResponse, err error) {
	var resp []types.RepairResponse
	body, err := json.Marshal(ids)
	if err != nil {
		return nil, fmt.Errorf("marshal digest objects input: %w", err)
	}
	req, err := newHttpReplicaRequest(
		ctx, http.MethodGet, host, index, shard,
		"", "_digest", bytes.NewReader(body), 0)
	if err != nil {
		return resp, fmt.Errorf("create http request: %w", err)
	}
	err = c.do(c.timeoutUnit*20, req, body, &resp, numRetries)
	return resp, err
}

func (c *replicationClient) DigestObjectsInRange(ctx context.Context,
	host, index, shard string, initialUUID, finalUUID strfmt.UUID, limit int,
) (result []types.RepairResponse, err error) {
	body, err := json.Marshal(replica.DigestObjectsInRangeReq{
		InitialUUID: initialUUID,
		FinalUUID:   finalUUID,
		Limit:       limit,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal digest objects in range input: %w", err)
	}

	req, err := newHttpReplicaRequest(
		ctx, http.MethodPost, host, index, shard,
		"", "digestsInRange", bytes.NewReader(body), 0)
	if err != nil {
		return nil, fmt.Errorf("create http request: %w", err)
	}

	var resp replica.DigestObjectsInRangeResp
	err = c.do(c.timeoutUnit*20, req, body, &resp, 9)
	return resp.Digests, err
}

func (c *replicationClient) HashTreeLevel(ctx context.Context,
	host, index, shard string, level int, discriminant *hashtree.Bitset,
) (digests []hashtree.Digest, err error) {
	var resp []hashtree.Digest
	body, err := discriminant.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshal hashtree level input: %w", err)
	}
	req, err := newHttpReplicaRequest(
		ctx, http.MethodPost, host, index, shard,
		"", fmt.Sprintf("hashtree/%d", level), bytes.NewReader(body), 0)
	if err != nil {
		return resp, fmt.Errorf("create http request: %w", err)
	}
	err = c.do(c.timeoutUnit*20, req, body, &resp, 9)
	return resp, err
}

func (c *replicationClient) OverwriteObjects(ctx context.Context,
	host, index, shard string, vobjects []*objects.VObject,
) ([]types.RepairResponse, error) {
	var resp []types.RepairResponse
	body, err := clusterapi.IndicesPayloads.VersionedObjectList.Marshal(vobjects)
	if err != nil {
		return nil, fmt.Errorf("encode request: %w", err)
	}
	req, err := newHttpReplicaRequest(
		ctx, http.MethodPut, host, index, shard,
		"", "_overwrite", bytes.NewReader(body), 0)
	if err != nil {
		return resp, fmt.Errorf("create http request: %w", err)
	}
	err = c.do(c.timeoutUnit*90, req, body, &resp, 9)
	return resp, err
}

func (c *replicationClient) FetchObjects(ctx context.Context, host,
	index, shard string, ids []strfmt.UUID,
) ([]replica.Replica, error) {
	resp := make(replica.Replicas, len(ids))
	idsBytes, err := json.Marshal(ids)
	if err != nil {
		return nil, fmt.Errorf("marshal ids: %w", err)
	}

	idsEncoded := base64.StdEncoding.EncodeToString(idsBytes)

	req, err := newHttpReplicaRequest(ctx, http.MethodGet, host, index, shard, "", "", nil, 0)
	if err != nil {
		return nil, fmt.Errorf("create http request: %w", err)
	}

	req.URL.RawQuery = url.Values{"ids": []string{idsEncoded}}.Encode()
	err = c.doCustomUnmarshal(c.timeoutUnit*90, req, nil, resp.UnmarshalBinary, 9)
	return resp, err
}

func (c *replicationClient) PutObject(ctx context.Context, host, index,
	shard, requestID string, obj *storobj.Object, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	var resp replica.SimpleResponse
	body, err := clusterapi.IndicesPayloads.SingleObject.Marshal(obj)
	if err != nil {
		return resp, fmt.Errorf("encode request: %w", err)
	}

	req, err := newHttpReplicaRequest(ctx, http.MethodPost, host, index, shard, requestID, "", nil, schemaVersion)
	if err != nil {
		return resp, fmt.Errorf("create http request: %w", err)
	}

	clusterapi.IndicesPayloads.SingleObject.SetContentTypeHeaderReq(req)
	err = c.do(c.timeoutUnit*90, req, body, &resp, 9)
	return resp, err
}

func (c *replicationClient) DeleteObject(ctx context.Context, host, index,
	shard, requestID string, uuid strfmt.UUID, deletionTime time.Time, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	var resp replica.SimpleResponse
	uuidTs := fmt.Sprintf("%s/%d", uuid.String(), deletionTime.UnixMilli())
	req, err := newHttpReplicaRequest(ctx, http.MethodDelete, host, index, shard, requestID, uuidTs, nil, schemaVersion)
	if err != nil {
		return resp, fmt.Errorf("create http request: %w", err)
	}

	err = c.do(c.timeoutUnit*90, req, nil, &resp, 9)
	return resp, err
}

func (c *replicationClient) PutObjects(ctx context.Context, host, index,
	shard, requestID string, objects []*storobj.Object, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	var resp replica.SimpleResponse
	body, err := clusterapi.IndicesPayloads.ObjectList.Marshal(objects)
	if err != nil {
		return resp, fmt.Errorf("encode request: %w", err)
	}
	req, err := newHttpReplicaRequest(ctx, http.MethodPost, host, index, shard, requestID, "", nil, schemaVersion)
	if err != nil {
		return resp, fmt.Errorf("create http request: %w", err)
	}

	clusterapi.IndicesPayloads.ObjectList.SetContentTypeHeaderReq(req)
	err = c.do(c.timeoutUnit*90, req, body, &resp, 9)
	return resp, err
}

func (c *replicationClient) MergeObject(ctx context.Context, host, index, shard, requestID string,
	doc *objects.MergeDocument, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	var resp replica.SimpleResponse
	body, err := clusterapi.IndicesPayloads.MergeDoc.Marshal(*doc)
	if err != nil {
		return resp, fmt.Errorf("encode request: %w", err)
	}

	req, err := newHttpReplicaRequest(ctx, http.MethodPatch, host, index, shard,
		requestID, doc.ID.String(), nil, schemaVersion)
	if err != nil {
		return resp, fmt.Errorf("create http request: %w", err)
	}

	clusterapi.IndicesPayloads.MergeDoc.SetContentTypeHeaderReq(req)
	err = c.do(c.timeoutUnit*90, req, body, &resp, 9)
	return resp, err
}

func (c *replicationClient) AddReferences(ctx context.Context, host, index,
	shard, requestID string, refs []objects.BatchReference, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	var resp replica.SimpleResponse
	body, err := clusterapi.IndicesPayloads.ReferenceList.Marshal(refs)
	if err != nil {
		return resp, fmt.Errorf("encode request: %w", err)
	}
	req, err := newHttpReplicaRequest(ctx, http.MethodPost, host, index, shard,
		requestID, "references", nil, schemaVersion)
	if err != nil {
		return resp, fmt.Errorf("create http request: %w", err)
	}

	clusterapi.IndicesPayloads.ReferenceList.SetContentTypeHeaderReq(req)
	err = c.do(c.timeoutUnit*90, req, body, &resp, 9)
	return resp, err
}

func (c *replicationClient) DeleteObjects(ctx context.Context, host, index, shard, requestID string,
	uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, schemaVersion uint64,
) (resp replica.SimpleResponse, err error) {
	body, err := clusterapi.IndicesPayloads.BatchDeleteParams.Marshal(uuids, deletionTime, dryRun)
	if err != nil {
		return resp, fmt.Errorf("encode request: %w", err)
	}
	req, err := newHttpReplicaRequest(ctx, http.MethodDelete, host, index, shard, requestID, "", nil, schemaVersion)
	if err != nil {
		return resp, fmt.Errorf("create http request: %w", err)
	}

	clusterapi.IndicesPayloads.BatchDeleteParams.SetContentTypeHeaderReq(req)
	err = c.do(c.timeoutUnit*90, req, body, &resp, 9)
	return resp, err
}

func (c *replicationClient) FindUUIDs(ctx context.Context, hostName, indexName,
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

// Commit asks a host to commit and stores the response in the value pointed to by resp
func (c *replicationClient) Commit(ctx context.Context, host, index, shard string, requestID string, resp interface{}) error {
	req, err := newHttpReplicaCMD(host, "commit", index, shard, requestID, nil)
	if err != nil {
		return fmt.Errorf("create http request: %w", err)
	}

	return c.do(c.timeoutUnit*90, req, nil, resp, 9)
}

func (c *replicationClient) Abort(ctx context.Context, host, index, shard, requestID string) (
	resp replica.SimpleResponse, err error,
) {
	req, err := newHttpReplicaCMD(host, "abort", index, shard, requestID, nil)
	if err != nil {
		return resp, fmt.Errorf("create http request: %w", err)
	}

	err = c.do(c.timeoutUnit*5, req, nil, &resp, 9)
	return resp, err
}

func newHttpReplicaRequest(ctx context.Context, method, host, index, shard, requestId, suffix string, body io.Reader, schemaVersion uint64) (*http.Request, error) {
	path := fmt.Sprintf("/replicas/indices/%s/shards/%s/objects", index, shard)
	if suffix != "" {
		path = fmt.Sprintf("%s/%s", path, suffix)
	}
	u := url.URL{
		Scheme: "http",
		Host:   host,
		Path:   path,
	}

	urlValues := url.Values{}
	urlValues[replica.SchemaVersionKey] = []string{fmt.Sprint(schemaVersion)}
	if requestId != "" {
		urlValues[replica.RequestKey] = []string{requestId}
	}
	u.RawQuery = urlValues.Encode()

	return http.NewRequestWithContext(ctx, method, u.String(), body)
}

func newHttpReplicaCMD(host, cmd, index, shard, requestId string, body io.Reader) (*http.Request, error) {
	path := fmt.Sprintf("/replicas/indices/%s/shards/%s:%s", index, shard, cmd)
	q := url.Values{replica.RequestKey: []string{requestId}}.Encode()
	url := url.URL{Scheme: "http", Host: host, Path: path, RawQuery: q}
	return http.NewRequest(http.MethodPost, url.String(), body)
}

func (c *replicationClient) do(timeout time.Duration, req *http.Request, body []byte, resp interface{}, numRetries int) (err error) {
	ctx, cancel := context.WithTimeout(req.Context(), timeout)
	defer cancel()
	req = req.WithContext(ctx)
	try := func(ctx context.Context) (bool, error) {
		if body != nil {
			req.Body = io.NopCloser(bytes.NewReader(body))
		}
		res, err := c.client.Do(req)
		if err != nil {
			return false, fmt.Errorf("connect: %w", err)
		}
		defer res.Body.Close()

		if code := res.StatusCode; code != http.StatusOK {
			b, _ := io.ReadAll(res.Body)
			return shouldRetry(code), fmt.Errorf("status code: %v, error: %s", code, b)
		}
		if err := json.NewDecoder(res.Body).Decode(resp); err != nil {
			return false, fmt.Errorf("decode response: %w", err)
		}
		return false, nil
	}
	return c.retry(ctx, numRetries, try)
}

func (c *replicationClient) doCustomUnmarshal(timeout time.Duration,
	req *http.Request, body []byte, decode func([]byte) error, numRetries int,
) (err error) {
	return (*retryClient)(c).doWithCustomMarshaller(timeout, req, body, decode, successCode, numRetries)
}

// backOff return a new random duration in the interval [d, 3d].
// It implements truncated exponential back-off with introduced jitter.
func backOff(d time.Duration) time.Duration {
	return time.Duration(float64(d.Nanoseconds()*2) * (0.5 + rand.Float64()))
}

func shouldRetry(code int) bool {
	return code == http.StatusInternalServerError ||
		code == http.StatusTooManyRequests ||
		code == http.StatusServiceUnavailable
}
