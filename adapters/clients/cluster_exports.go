//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
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

	"github.com/weaviate/weaviate/usecases/export"
)

const (
	pathExportPrepare = "/exports/prepare"
	pathExportCommit  = "/exports/commit"
	pathExportAbort   = "/exports/abort"
	pathExportStatus  = "/exports/status"
)

// ClusterExports handles inter-node export communication.
type ClusterExports struct {
	client *http.Client
}

// NewClusterExports creates a new cluster exports client
func NewClusterExports(client *http.Client) *ClusterExports {
	return &ClusterExports{client: client}
}

// Prepare asks a participant to reserve its export slot.
func (c *ClusterExports) Prepare(ctx context.Context, host string, req *export.ExportRequest) error {
	u := url.URL{Scheme: "http", Host: host, Path: pathExportPrepare}

	b, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal prepare request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("new prepare request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	respBody, statusCode, err := c.do(httpReq)
	if err != nil {
		return fmt.Errorf("prepare request: %w", err)
	}

	if statusCode != http.StatusOK {
		return fmt.Errorf("prepare failed: status %d (%s)", statusCode, respBody)
	}

	return nil
}

// Commit tells a participant to start the export.
func (c *ClusterExports) Commit(ctx context.Context, host, exportID string) error {
	u := url.URL{Scheme: "http", Host: host, Path: pathExportCommit, RawQuery: "id=" + url.QueryEscape(exportID)}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), nil)
	if err != nil {
		return fmt.Errorf("new commit request: %w", err)
	}

	respBody, statusCode, err := c.do(httpReq)
	if err != nil {
		return fmt.Errorf("commit request: %w", err)
	}

	if statusCode != http.StatusOK {
		return fmt.Errorf("commit failed: status %d (%s)", statusCode, respBody)
	}

	return nil
}

// Abort tells a participant to release its reservation.
func (c *ClusterExports) Abort(ctx context.Context, host, exportID string) {
	u := url.URL{Scheme: "http", Host: host, Path: pathExportAbort, RawQuery: "id=" + url.QueryEscape(exportID)}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), nil)
	if err != nil {
		return
	}

	c.do(httpReq)
}

// IsRunning checks whether a participant node is still running the given export.
func (c *ClusterExports) IsRunning(ctx context.Context, host, exportID string) (bool, error) {
	u := url.URL{Scheme: "http", Host: host, Path: pathExportStatus, RawQuery: "id=" + url.QueryEscape(exportID)}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return false, fmt.Errorf("new status request: %w", err)
	}

	respBody, statusCode, err := c.do(httpReq)
	if err != nil {
		return false, fmt.Errorf("status request: %w", err)
	}

	if statusCode != http.StatusOK {
		return false, fmt.Errorf("unexpected status code %d (%s)", statusCode, respBody)
	}

	var result export.ExportStatusResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return false, fmt.Errorf("unmarshal status response: %w", err)
	}

	return result.Running, nil
}

func (c *ClusterExports) do(req *http.Request) (body []byte, statusCode int, err error) {
	httpResp, err := c.client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("make request: %w", err)
	}
	defer httpResp.Body.Close()

	body, err = io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, httpResp.StatusCode, fmt.Errorf("read response: %w", err)
	}

	return body, httpResp.StatusCode, nil
}
