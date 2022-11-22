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

package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/clusterapi"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/semi-technologies/weaviate/usecases/objects"
	"github.com/semi-technologies/weaviate/usecases/replica"
)

var marshaller = clusterapi.IndicesPayloads

type ReplicationClient struct {
	client *http.Client
}

func NewReplicationClient(httpClient *http.Client) *ReplicationClient {
	return &ReplicationClient{client: httpClient}
}

func (c *ReplicationClient) PutObject(ctx context.Context, host, index,
	shard, requestID string, obj *storobj.Object,
) (replica.SimpleResponse, error) {
	var resp replica.SimpleResponse
	payload, err := marshaller.SingleObject.Marshal(obj)
	if err != nil {
		return resp, fmt.Errorf("encode request: %w", err)
	}

	req, err := newHttpRequest(ctx, http.MethodPost, host, index, shard, requestID, "", bytes.NewReader(payload))
	if err != nil {
		return resp, fmt.Errorf("create http request: %w", err)
	}

	marshaller.SingleObject.SetContentTypeHeaderReq(req)
	res, err := c.client.Do(req)
	if err != nil {
		return resp, fmt.Errorf("connect: %w", err)
	}

	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return resp, fmt.Errorf("status code: %v", res.StatusCode)
	}

	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		return resp, fmt.Errorf("decode response: %w", err)
	}

	return resp, nil
}

func (c *ReplicationClient) DeleteObject(ctx context.Context, host, index,
	shard, requestID string, uuid strfmt.UUID,
) (replica.SimpleResponse, error) {
	var resp replica.SimpleResponse
	req, err := newHttpRequest(ctx, http.MethodDelete, host, index, shard, requestID, uuid.String(), nil)
	if err != nil {
		return resp, fmt.Errorf("create http request: %w", err)
	}

	res, err := c.client.Do(req)
	if err != nil {
		return resp, fmt.Errorf("connect: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		// TODO: return and err and let the coordinator decided what it needs to be done
		return resp, nil
	}
	if res.StatusCode != http.StatusOK {
		return resp, fmt.Errorf("status code: %v", res.StatusCode)
	}
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		return resp, fmt.Errorf("decode response: %w", err)
	}
	return resp, nil
}

func (c *ReplicationClient) PutObjects(ctx context.Context, host, index,
	shard, requestID string, objects []*storobj.Object,
) (replica.SimpleResponse, error) {
	var resp replica.SimpleResponse
	body, err := clusterapi.IndicesPayloads.ObjectList.Marshal(objects)
	if err != nil {
		return resp, fmt.Errorf("encode request: %w", err)
	}
	req, err := newHttpRequest(ctx, http.MethodPost, host, index, shard, requestID, "", bytes.NewReader(body))
	if err != nil {
		return resp, fmt.Errorf("create http request: %w", err)
	}

	clusterapi.IndicesPayloads.ObjectList.SetContentTypeHeaderReq(req)
	res, err := c.client.Do(req)
	if err != nil {
		return resp, fmt.Errorf("connect: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return resp, fmt.Errorf("status code: %v", res.StatusCode)
	}
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		return resp, fmt.Errorf("decode response: %w", err)
	}
	return resp, nil
}

func (c *ReplicationClient) MergeObject(ctx context.Context, host, index, shard, requestID string,
	doc *objects.MergeDocument,
) (replica.SimpleResponse, error) {
	var resp replica.SimpleResponse
	body, err := clusterapi.IndicesPayloads.MergeDoc.Marshal(*doc)
	if err != nil {
		return resp, fmt.Errorf("encode request: %w", err)
	}

	req, err := newHttpRequest(ctx, http.MethodPatch, host, index, shard,
		requestID, doc.ID.String(), bytes.NewReader(body))
	if err != nil {
		return resp, fmt.Errorf("create http request: %w", err)
	}

	clusterapi.IndicesPayloads.MergeDoc.SetContentTypeHeaderReq(req)
	res, err := c.client.Do(req)
	if err != nil {
		return resp, fmt.Errorf("connect: %w", err)
	}

	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return resp, fmt.Errorf("status code: %v", res.StatusCode)
	}
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		return resp, fmt.Errorf("decode response: %w", err)
	}
	return resp, nil
}

func (c *ReplicationClient) AddReferences(ctx context.Context, host, index,
	shard, requestID string, refs []objects.BatchReference,
) (replica.SimpleResponse, error) {
	var resp replica.SimpleResponse
	body, err := clusterapi.IndicesPayloads.ReferenceList.Marshal(refs)
	if err != nil {
		return resp, fmt.Errorf("encode request: %w", err)
	}
	req, err := newHttpRequest(ctx, http.MethodPost, host, index, shard,
		requestID, "references",
		bytes.NewReader(body))
	if err != nil {
		return resp, fmt.Errorf("create http request: %w", err)
	}

	clusterapi.IndicesPayloads.ReferenceList.SetContentTypeHeaderReq(req)
	res, err := c.client.Do(req)
	if err != nil {
		return resp, fmt.Errorf("connect: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return resp, fmt.Errorf("status code: %v", res.StatusCode)
	}
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		return resp, fmt.Errorf("decode response: %w", err)
	}
	return resp, nil
}

func (c *ReplicationClient) DeleteObjects(ctx context.Context, host, index, shard, requestID string,
	docIDs []uint64, dryRun bool,
) (resp replica.SimpleResponse, err error) {
	body, err := clusterapi.IndicesPayloads.BatchDeleteParams.Marshal(docIDs, dryRun)
	if err != nil {
		return resp, fmt.Errorf("encode request: %w", err)
	}
	req, err := newHttpRequest(ctx, http.MethodDelete, host, index, shard, requestID, "", bytes.NewReader(body))
	if err != nil {
		return resp, fmt.Errorf("create http request: %w", err)
	}

	clusterapi.IndicesPayloads.BatchDeleteParams.SetContentTypeHeaderReq(req)
	res, err := c.client.Do(req)
	if err != nil {
		return resp, fmt.Errorf("connect: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return resp, fmt.Errorf("status code: %v", res.StatusCode)
	}
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		return resp, fmt.Errorf("decode response: %w", err)
	}
	return resp, nil
}

// Commit asks a host to commit and stores the response in the value pointed to by resp
func (c *ReplicationClient) Commit(ctx context.Context, host, index, shard string, requestID string, resp interface{}) error {
	req, err := newHttpCMD(ctx, host, "commit", index, shard, requestID, nil)
	if err != nil {
		return fmt.Errorf("create http request: %w", err)
	}

	res, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("status code: %v", res.StatusCode)
	}

	if err := json.NewDecoder(res.Body).Decode(resp); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

func (c *ReplicationClient) Abort(ctx context.Context, host, index, shard, requestID string) (
	resp replica.SimpleResponse, err error,
) {
	req, err := newHttpCMD(ctx, host, "abort", index, shard, requestID, nil)
	if err != nil {
		return resp, fmt.Errorf("create http request: %w", err)
	}

	res, err := c.client.Do(req)
	if err != nil {
		return resp, fmt.Errorf("connect: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return resp, fmt.Errorf("status code: %v", res.StatusCode)
	}
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		return resp, fmt.Errorf("decode response: %w", err)
	}
	return resp, nil
}

func newHttpRequest(ctx context.Context, method, host, index, shard, requestId, suffix string, body io.Reader) (*http.Request, error) {
	path := fmt.Sprintf("/replicas/indices/%s/shards/%s/objects", index, shard)
	if suffix != "" {
		path = fmt.Sprintf("%s/%s", path, suffix)
	}
	url := url.URL{
		Scheme:   "http",
		Host:     host,
		Path:     path,
		RawQuery: url.Values{replica.RequestKey: []string{requestId}}.Encode(),
	}

	return http.NewRequestWithContext(ctx, method, url.String(), body)
}

func newHttpCMD(ctx context.Context, host, cmd, index, shard, requestId string, body io.Reader) (*http.Request, error) {
	path := fmt.Sprintf("/replicas/indices/%s/shards/%s:%s", index, shard, cmd)
	q := url.Values{replica.RequestKey: []string{requestId}}.Encode()
	url := url.URL{Scheme: "http", Host: host, Path: path, RawQuery: q}
	return http.NewRequestWithContext(ctx, http.MethodPost, url.String(), body)
}
