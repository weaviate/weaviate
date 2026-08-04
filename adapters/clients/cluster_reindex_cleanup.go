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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

const pathReindexCleanupActivity = "/reindex/cleanup-activity"

// ErrReindexCleanupUnsupported means the node runs a build without the route.
// Waiting on it would never succeed, so callers stop asking rather than burn
// their whole budget on a node that can never answer.
var ErrReindexCleanupUnsupported = errors.New("node does not serve the reindex cleanup-activity route")

type ClusterReindexCleanup struct {
	client   *http.Client
	resolver nodeResolver
}

func NewClusterReindexCleanup(client *http.Client, resolver nodeResolver) *ClusterReindexCleanup {
	return &ClusterReindexCleanup{client: client, resolver: resolver}
}

// CleanupInProgress asks one node whether it is still tearing down reindex
// sidecars for the collection.
func (c *ClusterReindexCleanup) CleanupInProgress(ctx context.Context, nodeName, collection string) (bool, error) {
	host, found := c.resolver.NodeHostname(nodeName)
	if !found {
		return false, fmt.Errorf("unable to resolve hostname for %q", nodeName)
	}
	u := url.URL{
		Scheme:   "http",
		Host:     host,
		Path:     pathReindexCleanupActivity,
		RawQuery: url.Values{"collection": []string{collection}}.Encode(),
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return false, fmt.Errorf("new reindex cleanup request: %w", err)
	}
	res, err := c.client.Do(req)
	if err != nil {
		return false, fmt.Errorf("reindex cleanup request: %w", err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return false, fmt.Errorf("read reindex cleanup response: %w", err)
	}
	if res.StatusCode == http.StatusNotFound {
		return false, ErrReindexCleanupUnsupported
	}
	if res.StatusCode != http.StatusOK {
		return false, fmt.Errorf("reindex cleanup: unexpected status code %d (%s)", res.StatusCode, body)
	}
	var activity struct {
		CleaningUp bool `json:"cleaningUp"`
	}
	if err := json.Unmarshal(body, &activity); err != nil {
		return false, fmt.Errorf("unmarshal reindex cleanup response: %w", err)
	}
	return activity.CleaningUp, nil
}
