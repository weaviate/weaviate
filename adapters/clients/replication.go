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
	"math/rand"
	"net/http"
	"net/url"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/clusterapi"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/semi-technologies/weaviate/usecases/objects"
	"github.com/semi-technologies/weaviate/usecases/replica"
)

// ReplicationClient is to coordinate operations among replicas

type replicationClient struct {
	client      *http.Client
	minBackOff  time.Duration
	maxBackOff  time.Duration
	timeoutUnit time.Duration
}

func NewReplicationClient(httpClient *http.Client) replica.Client {
	return &replicationClient{
		client:      httpClient,
		minBackOff:  time.Millisecond * 150,
		maxBackOff:  time.Second * 20,
		timeoutUnit: time.Second,
	}
}

func (c *replicationClient) PutObject(ctx context.Context, host, index,
	shard, requestID string, obj *storobj.Object,
) (replica.SimpleResponse, error) {
	var resp replica.SimpleResponse
	payload, err := clusterapi.IndicesPayloads.SingleObject.Marshal(obj)
	if err != nil {
		return resp, fmt.Errorf("encode request: %w", err)
	}

	req, err := newHttpRequest(ctx, http.MethodPost, host, index, shard, requestID, "", bytes.NewReader(payload))
	if err != nil {
		return resp, fmt.Errorf("create http request: %w", err)
	}

	clusterapi.IndicesPayloads.SingleObject.SetContentTypeHeaderReq(req)
	err = c.do(c.timeoutUnit*2, req, &resp)
	return resp, err
}

func (c *replicationClient) DeleteObject(ctx context.Context, host, index,
	shard, requestID string, uuid strfmt.UUID,
) (replica.SimpleResponse, error) {
	var resp replica.SimpleResponse
	req, err := newHttpRequest(ctx, http.MethodDelete, host, index, shard, requestID, uuid.String(), nil)
	if err != nil {
		return resp, fmt.Errorf("create http request: %w", err)
	}

	err = c.do(c.timeoutUnit*2, req, &resp)
	return resp, err
}

func (c *replicationClient) PutObjects(ctx context.Context, host, index,
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
	err = c.do(c.timeoutUnit*10, req, &resp)
	return resp, err
}

func (c *replicationClient) MergeObject(ctx context.Context, host, index, shard, requestID string,
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
	err = c.do(c.timeoutUnit*2, req, &resp)
	return resp, err
}

func (c *replicationClient) AddReferences(ctx context.Context, host, index,
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
	err = c.do(c.timeoutUnit*10, req, &resp)
	return resp, err
}

func (c *replicationClient) DeleteObjects(ctx context.Context, host, index, shard, requestID string,
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
	err = c.do(c.timeoutUnit*10, req, &resp)
	return resp, err
}

// Commit asks a host to commit and stores the response in the value pointed to by resp
func (c *replicationClient) Commit(ctx context.Context, host, index, shard string, requestID string, resp interface{}) error {
	req, err := newHttpCMD(ctx, host, "commit", index, shard, requestID, nil)
	if err != nil {
		return fmt.Errorf("create http request: %w", err)
	}

	return c.do(c.timeoutUnit*32, req, &resp)
}

func (c *replicationClient) Abort(ctx context.Context, host, index, shard, requestID string) (
	resp replica.SimpleResponse, err error,
) {
	req, err := newHttpCMD(ctx, host, "abort", index, shard, requestID, nil)
	if err != nil {
		return resp, fmt.Errorf("create http request: %w", err)
	}

	err = c.do(c.timeoutUnit, req, &resp)
	return resp, err
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

func (c *replicationClient) do(timeout time.Duration, req *http.Request, resp interface{}) (err error) {
	var (
		ctx, cancel = context.WithTimeout(req.Context(), timeout)
		errs        = make(chan error, 1)
		delay       = c.minBackOff
		maxBackOff  = c.maxBackOff
		shouldRetry = false
	)
	defer cancel()

	try := func(req *http.Request) (bool, error) {
		res, err := c.client.Do(req)
		if err != nil {
			return true, fmt.Errorf("connect: %w", err)
		}
		defer res.Body.Close()

		if code := res.StatusCode; code != http.StatusOK {
			shouldRetry := (code == http.StatusInternalServerError || code == http.StatusTooManyRequests)
			return shouldRetry, fmt.Errorf("status code: %v", code)
		}
		if err := json.NewDecoder(res.Body).Decode(resp); err != nil {
			return false, fmt.Errorf("decode response: %w", err)
		}
		return false, nil
	}

	for {
		go func() {
			if shouldRetry {
				time.Sleep(delay)
			}
			var err error
			shouldRetry, err = try(req)
			errs <- err
		}()

		select {
		case <-ctx.Done():
			return fmt.Errorf("%v: %w", err, ctx.Err())
		case err = <-errs:
			if err == nil || !shouldRetry {
				return err
			}
			if delay = backOff(delay); delay >= maxBackOff {
				return err
			}
		}
	}
}

// backOff return a new random duration in the interval [d, 3d].
// It implements truncated exponential back-off with introduced jitter.
func backOff(d time.Duration) time.Duration {
	return time.Duration(float64(d.Nanoseconds()*2) * (0.5 + rand.Float64()))
}
