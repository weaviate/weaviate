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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/weaviate/weaviate/usecases/backup"
)

const (
	pathCanCommit = "/backups/can-commit"
	pathCommit    = "/backups/commit"
	pathStatus    = "/backups/status"
	pathAbort     = "/backups/abort"
)

type ClusterBackups struct {
	client *http.Client
}

func NewClusterBackups(client *http.Client) *ClusterBackups {
	return &ClusterBackups{client: client}
}

func (c *ClusterBackups) CanCommit(ctx context.Context,
	host string, req *backup.Request,
) (*backup.CanCommitResponse, error) {
	url := url.URL{Scheme: "http", Host: host, Path: pathCanCommit}

	b, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal can-commit request: %w", err)
	}

	httpReq, err := http.NewRequest(http.MethodPost, url.String(), bytes.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("new can-commit request: %w", err)
	}

	respBody, statusCode, err := c.do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("can-commit request: %w", err)
	}

	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code %d (%s)",
			statusCode, respBody)
	}

	var resp backup.CanCommitResponse
	err = json.Unmarshal(respBody, &resp)
	if err != nil {
		return nil, fmt.Errorf("unmarshal can-commit response: %w", err)
	}

	return &resp, nil
}

func (c *ClusterBackups) Commit(ctx context.Context,
	host string, req *backup.StatusRequest,
) error {
	url := url.URL{Scheme: "http", Host: host, Path: pathCommit}

	b, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal commit request: %w", err)
	}

	httpReq, err := http.NewRequest(http.MethodPost, url.String(), bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("new commit request: %w", err)
	}

	respBody, statusCode, err := c.do(httpReq)
	if err != nil {
		return fmt.Errorf("commit request: %w", err)
	}

	if statusCode != http.StatusCreated {
		return fmt.Errorf("unexpected status code %d (%s)",
			statusCode, respBody)
	}

	return nil
}

func (c *ClusterBackups) Status(ctx context.Context,
	host string, req *backup.StatusRequest,
) (*backup.StatusResponse, error) {
	url := url.URL{Scheme: "http", Host: host, Path: pathStatus}

	b, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal status request: %w", err)
	}

	httpReq, err := http.NewRequest(http.MethodPost, url.String(), bytes.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("new status request: %w", err)
	}

	respBody, statusCode, err := c.do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("status request: %w", err)
	}

	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code %d (%s)",
			statusCode, respBody)
	}

	var resp backup.StatusResponse
	err = json.Unmarshal(respBody, &resp)
	if err != nil {
		return nil, fmt.Errorf("unmarshal status response: %w", err)
	}

	return &resp, nil
}

func (c *ClusterBackups) Abort(_ context.Context,
	host string, req *backup.AbortRequest,
) error {
	url := url.URL{Scheme: "http", Host: host, Path: pathAbort}

	b, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal abort request: %w", err)
	}

	httpReq, err := http.NewRequest(http.MethodPost, url.String(), bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("new abort request: %w", err)
	}

	respBody, statusCode, err := c.do(httpReq)
	if err != nil {
		return fmt.Errorf("abort request: %w", err)
	}

	if statusCode != http.StatusNoContent {
		return fmt.Errorf("unexpected status code %d (%s)",
			statusCode, respBody)
	}

	return nil
}

func (c *ClusterBackups) do(req *http.Request) (body []byte, statusCode int, err error) {
	httpResp, err := c.client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("make request: %w", err)
	}

	body, err = io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, httpResp.StatusCode, fmt.Errorf("read response: %w", err)
	}
	defer httpResp.Body.Close()

	return body, httpResp.StatusCode, nil
}
