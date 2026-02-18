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
	pathExportExecute = "/exports/execute"
	pathExportStatus  = "/exports/status"
)

// ClusterExports handles inter-node export communication.
// It sends fire-and-forget requests to participant nodes.
type ClusterExports struct {
	client *http.Client
}

// NewClusterExports creates a new cluster exports client
func NewClusterExports(client *http.Client) *ClusterExports {
	return &ClusterExports{client: client}
}

// Execute sends an export request to a participant node.
// This is fire-and-forget: the participant will export its shards asynchronously.
func (c *ClusterExports) Execute(ctx context.Context, host string, req *export.ExportRequest) error {
	u := url.URL{Scheme: "http", Host: host, Path: pathExportExecute}

	b, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal export request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("new export request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	respBody, statusCode, err := c.do(httpReq)
	if err != nil {
		return fmt.Errorf("export request: %w", err)
	}

	if statusCode != http.StatusAccepted {
		return fmt.Errorf("unexpected status code %d (%s)", statusCode, respBody)
	}

	return nil
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
